//! Describing the item a delivery names, the subscriber that scrobbles it,
//! and the other direction: applying what a provider reports to a library.

use std::{
    collections::HashSet,
    sync::{Arc, LazyLock, Mutex},
};

use anyhow::{Context as _, Error, Result, anyhow, bail};
use async_trait::async_trait;
use chrono::{Datelike, Utc};
use serde::Serialize;
use tracing::{info, warn};
use uuid::Uuid;

use crate::{
    AppContext,
    addons::media_tracker::{
        MediaTrackerCapabilities, MediaTrackerCredentials, MediaTrackerCtx,
        MediaTrackerError, MediaTrackerEvent, MediaTrackerEventKind,
        MediaTrackerTarget, RemoteKind, RemoteWatch,
    },
    db,
    signals::{DeliveryMode, Event, EventType, Subscriber},
};

/// Simkl asks media servers to sync when a playback session ends. This keeps
/// that from turning a binge into a pull per episode.
const PLAYBACK_PULL_THROTTLE: chrono::Duration = chrono::Duration::minutes(15);

/// Connections with a pull in progress. One pull per connection at a time:
/// the hourly task, a "sync now" and a fresh connect can otherwise all read
/// the same lists at once.
static SYNCING: LazyLock<Mutex<HashSet<Uuid>>> = LazyLock::new(Default::default);

/// Holds a connection's place in [`SYNCING`] until dropped.
pub struct SyncGuard(Uuid);

impl SyncGuard {
    /// `None` when a pull for `tracker_id` is already running.
    pub fn try_begin(tracker_id: Uuid) -> Option<Self> {
        // The lock is released at the end of this statement, before a guard
        // exists: a guard's `Drop` takes the same lock, so building one while
        // it is held (as `then_some` would, only to drop it) deadlocks.
        let inserted = SYNCING
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(tracker_id);
        inserted.then(|| Self(tracker_id))
    }
}

impl Drop for SyncGuard {
    fn drop(&mut self) {
        SYNCING
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.0);
    }
}

pub fn is_syncing(tracker_id: Uuid) -> bool {
    SYNCING
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .contains(&tracker_id)
}

/// Whether a provider has anything to pull.
fn pulls(caps: &MediaTrackerCapabilities) -> bool {
    caps.history_import
        || caps
            .watch_state_sync
            .pulls()
        || caps
            .ratings
            .pulls()
        || caps
            .favorites
            .pulls()
}

fn describe(media: &db::Media, series: Option<&db::Media>) -> MediaTrackerTarget {
    // A season row's own index is the season number; an episode's is the
    // episode number and its parent's the season.
    let (season, episode) = match media.kind {
        db::MediaKind::Episode => (media.parent_idx, media.idx),
        db::MediaKind::Season => (media.idx, None),
        _ => (None, None),
    };
    MediaTrackerTarget {
        kind: media
            .kind
            .clone(),
        title: media
            .title
            .clone(),
        year: media
            .released_at
            .map(|d| d.year()),
        ids: media
            .external_ids
            .clone(),
        series: series.map(|s| Box::new(describe(s, None))),
        season,
        episode,
        runtime_seconds: media.runtime,
    }
}

/// The series an episode or season hangs off. The ancestor walk follows
/// `parent_id`, so an episode saved without a season row above it falls back
/// to the `grandparent_id` its row names, which `Media::validate` requires of
/// it.
async fn series_of(ctx: &AppContext, media: &db::Media) -> Result<Option<db::Media>> {
    if !matches!(media.kind, db::MediaKind::Episode | db::MediaKind::Season) {
        return Ok(None);
    }
    if let Some(series) = db::Media::get_ancestors(&ctx.db, &media.id)
        .await?
        .into_iter()
        .find(|m| m.kind == db::MediaKind::Series)
    {
        return Ok(Some(series));
    }
    let Some(series_id) = media
        .grandparent_id
        .or(media.parent_id)
    else {
        return Ok(None);
    };
    Ok(db::Media::get_by_id(&ctx.db, &series_id)
        .await?
        .filter(|m| m.kind == db::MediaKind::Series))
}

/// The item as a provider needs to see it, or `None` when nothing about it
/// carries an id one could match on. Episodes carry their series, because a
/// provider keys an episode on the show's ids plus season and episode.
pub async fn resolve_target(
    ctx: &AppContext,
    media: &mut db::Media,
) -> Result<Option<MediaTrackerTarget>> {
    let mut series = series_of(ctx, media).await?;

    // Opportunistic, and here so it reuses the series row loaded above: a TMDB
    // or Kitsu error must not hold up an event that was already deliverable, so
    // it is surfaced below only if it turns out to be why nothing matched.
    let mut completion_err: Option<Error> = None;
    if let Some(series) = series.as_mut() {
        if let Err(e) = crate::services::MediaResolveService::complete_episode_ids(
            media, series, ctx,
        )
        .await
        {
            completion_err = Some(e);
        }
    }

    let target = describe(media, series.as_ref());
    if !target.is_matchable() {
        if let Some(e) = completion_err {
            return Err(e);
        }
        return Ok(None);
    }
    if let Some(e) = completion_err {
        warn!(
            title = %media.title,
            error = %e,
            "failed to complete episode ids, delivering with what already matched"
        );
    }
    Ok(Some(target))
}

/// What one pull changed locally.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct SyncReport {
    /// Items the provider reported.
    pub received: usize,
    /// Of those, the ones the library has.
    pub matched: usize,
    pub unmatched: usize,
    /// Matched, but the library could not be updated. The watermark stays put
    /// so the next delta offers them again.
    pub failed: usize,
    pub marked_played: usize,
    pub rated: usize,
    pub favorites: usize,
}

/// The local row a remote item names, if the library has it. Movies and
/// series are looked up in their own kind only: TMDB movie and TV ids
/// overlap. Seasons and episodes hang off the series by number, since
/// providers key them on the show; their ids are minted from the series key,
/// with a positional lookup for rows imported another way.
pub async fn locate_remote(
    db: &sqlx::SqlitePool,
    watch: &RemoteWatch,
) -> Result<Option<db::Media>> {
    let (season, episode) = match (watch.kind, watch.season, watch.episode) {
        (RemoteKind::Movie, None, None) => {
            let Some(id) =
                db::Media::find_by_external_ids(db, &db::MediaKind::Movie, &watch.ids)
                    .await
            else {
                return Ok(None);
            };
            return Ok(db::Media::get_by_id(db, &id).await?);
        }
        // A movie has no seasons; an episode number without a season names
        // nothing.
        (RemoteKind::Movie, _, _) | (RemoteKind::Show, None, Some(_)) => {
            return Ok(None);
        }
        (RemoteKind::Show, None, None) => {
            let Some(id) =
                db::Media::find_by_external_ids(db, &db::MediaKind::Series, &watch.ids)
                    .await
            else {
                return Ok(None);
            };
            return Ok(db::Media::get_by_id(db, &id).await?);
        }
        (RemoteKind::Show, Some(season), episode) => (season, episode),
    };

    let Some(series_id) =
        db::Media::find_by_external_ids(db, &db::MediaKind::Series, &watch.ids).await
    else {
        return Ok(None);
    };
    let Some(series) = db::Media::get_by_id(db, &series_id).await? else {
        return Ok(None);
    };
    let key = series.series_canonical_key();
    let minted = match episode {
        Some(ep) => db::Media::episode_id(&key, season, ep),
        None => db::Media::season_id(&key, season),
    };
    if let Some(media) = db::Media::get_by_id(db, &minted).await? {
        return Ok(Some(media));
    }

    let filter = match episode {
        Some(ep) => db::MediaFilter {
            kind: Some(vec![db::MediaKind::Episode]),
            grandparent_id: Some(series.id),
            index_number: Some(ep),
            ..Default::default()
        },
        None => db::MediaFilter {
            kind: Some(vec![db::MediaKind::Season]),
            parent_id: Some(series.id),
            index_number: Some(season),
            ..Default::default()
        },
    };
    let rows = db::Media::get_by_filter(db, &filter)
        .await?
        .records;
    Ok(rows
        .into_iter()
        .find(|m| episode.is_none() || m.parent_idx == Some(season)))
}

/// Apply what a provider reported to one user's library. Positive only: a
/// remote list says what is in it, not what left it, so nothing here marks an
/// item unplayed or clears a rating. Clients hear about changes over the
/// websocket; no `Event` is emitted, or the change would be pushed straight
/// back to the provider it came from.
pub async fn apply_remote_watches(
    ctx: &AppContext,
    user: &db::User,
    watches: &[RemoteWatch],
) -> Result<SyncReport> {
    let release_threshold = db::Settings::get_config_or_default(&ctx.db)
        .await
        .release_date_threshold();
    let mut report = SyncReport {
        received: watches.len(),
        ..Default::default()
    };
    for watch in watches {
        let media = match locate_remote(&ctx.db, watch).await {
            Ok(Some(media)) => media,
            Ok(None) => {
                report.unmatched += 1;
                continue;
            }
            Err(e) => {
                warn!(ids = ?watch.ids, error = %e, "media tracker: lookup failed");
                report.unmatched += 1;
                continue;
            }
        };
        report.matched += 1;
        if let Err(e) =
            apply_remote_watch(ctx, user, &media, watch, release_threshold, &mut report)
                .await
        {
            report.failed += 1;
            warn!(
                media = %media.id,
                title = %media.title,
                error = %e,
                "media tracker: could not apply remote state"
            );
        }
    }
    Ok(report)
}

async fn apply_remote_watch(
    ctx: &AppContext,
    user: &db::User,
    media: &db::Media,
    watch: &RemoteWatch,
    release_threshold: Option<chrono::NaiveDateTime>,
    report: &mut SyncReport,
) -> Result<()> {
    let state = db::UserMediaState::get_or_new(&ctx.db, user, media).await?;
    let mut changed = false;

    let already_played = state.play_count > 0
        || state
            .played_at
            .is_some();
    if watch.watched && !already_played {
        let mut played = media
            .mark_played(&ctx.db, user, true, release_threshold)
            .await?;
        // Keep the provider's date: it is when the user actually watched.
        if let Some(at) = watch.watched_at {
            played.played_at = Some(at);
            played
                .save(&ctx.db)
                .await?;
        }
        report.marked_played += 1;
        changed = true;
    }

    // Providers keep whole-number ratings, so a local 8.4 comes back as 8;
    // only a rating that rounds differently is news.
    let same_rating = |remote: f64| {
        state
            .rating
            .is_some_and(|local| local.round() == remote.round())
    };
    if let Some(rating) = watch.rating {
        match db::UserRating::try_from(f64::from(rating)) {
            Ok(rating) if !same_rating(rating.value()) => {
                db::UserMediaState::set_rating(&ctx.db, user, media, Some(rating))
                    .await?;
                report.rated += 1;
                changed = true;
            }
            Ok(_) => {}
            Err(e) => warn!(
                media = %media.id,
                rating,
                error = %e,
                "media tracker: ignoring an out-of-range remote rating"
            ),
        }
    }

    match watch.favorite {
        Some(true) if !state.favorite => {
            media
                .mark_favorite(&ctx.db, user)
                .await?;
            report.favorites += 1;
            changed = true;
        }
        Some(false) if state.favorite => {
            media
                .unmark_favorite(&ctx.db, user)
                .await?;
            report.favorites += 1;
            changed = true;
        }
        _ => {}
    }

    if changed {
        let _ = ctx
            .ws_tx
            .send(crate::ws::WsEvent::UserDataChanged {
                user_id: user.id,
                item_id: media.id,
            });
    }
    Ok(())
}

fn tracker_ctx(ctx: &AppContext) -> MediaTrackerCtx {
    MediaTrackerCtx {
        config: Arc::new(
            ctx.config
                .clone(),
        ),
    }
}

/// Pull a provider's state for one connection and apply it. `full` ignores
/// the watermark and reads everything again. A provider failure is recorded
/// on the connection the way a failed delivery is. One pull per connection
/// runs at a time.
pub async fn sync_tracker(
    ctx: &AppContext,
    tracker: &db::UserMediaTracker,
    full: bool,
) -> Result<SyncReport> {
    if tracker.status != db::MediaTrackerStatus::Connected {
        bail!(
            "the connection is {} and needs reconnecting",
            tracker.status
        );
    }
    let addon = ctx
        .addons
        .media_tracker_for(tracker.addon_id)
        .ok_or_else(|| anyhow!("the tracker addon is disabled or gone"))?;
    let caps = addon.capabilities();
    if !pulls(&caps) {
        bail!("this provider does not report remote changes");
    }
    let Some(_running) = SyncGuard::try_begin(tracker.id) else {
        bail!("a pull for this connection is already running");
    };
    let user = db::User::get_by_id(&ctx.db, &tracker.user_id)
        .await?
        .context("user not found")?;

    let tctx = tracker_ctx(ctx);
    // Taken before the fetch so nothing that lands during it is skipped.
    let started_at = Utc::now().naive_utc();
    let since = if full { None } else { tracker.last_pull_at };
    let fetched = match since {
        None if caps.history_import => {
            addon
                .import_history(&tracker.credentials, &tctx)
                .await
        }
        _ => {
            addon
                .pull_changes(since, &tracker.credentials, &tctx)
                .await
        }
    };
    let watches = match fetched {
        Ok(watches) => watches,
        Err(e) => {
            if unchanged_since(ctx, tracker).await? {
                db::UserMediaTracker::mark_failure(&ctx.db, tracker.id, &e).await?;
            }
            return Err(anyhow!("{e}"));
        }
    };

    let report = apply_remote_watches(ctx, &user, &watches).await?;
    // A reconnect while this ran keeps the row id but is a different
    // connection; its state is not this pull's to write.
    if !unchanged_since(ctx, tracker).await? {
        warn!(tracker = %tracker.id, "connection changed during the pull; not recording it");
        return Ok(report);
    }
    if report.failed == 0 {
        db::UserMediaTracker::mark_pull_success(&ctx.db, tracker.id, started_at)
            .await?;
    } else {
        // The provider answered; the library did not take everything. Keep
        // the watermark so the next delta offers those items again.
        db::UserMediaTracker::mark_success(&ctx.db, tracker.id).await?;
    }
    Ok(report)
}

/// Whether the stored row still is the connection `tracker` was read as.
/// `upsert` bumps `updated_at` on a reconnect; a status change alone does
/// not make it a different connection.
async fn unchanged_since(
    ctx: &AppContext,
    tracker: &db::UserMediaTracker,
) -> Result<bool> {
    Ok(db::UserMediaTracker::get(&ctx.db, tracker.id)
        .await?
        .is_some_and(|now| now.updated_at == tracker.updated_at))
}

/// Run [`sync_tracker`] off the request path, logging the outcome. `false`
/// when a pull for this connection is already running, in which case nothing
/// is started.
pub fn spawn_sync(ctx: AppContext, tracker: db::UserMediaTracker, full: bool) -> bool {
    if is_syncing(tracker.id) {
        return false;
    }
    tokio::spawn(async move {
        match sync_tracker(&ctx, &tracker, full).await {
            Ok(report) => info!(
                tracker = %tracker.id,
                user = %tracker.user_id,
                received = report.received,
                matched = report.matched,
                failed = report.failed,
                marked_played = report.marked_played,
                rated = report.rated,
                "media tracker pull finished"
            ),
            Err(e) => warn!(
                tracker = %tracker.id,
                user = %tracker.user_id,
                error = %e,
                "media tracker pull failed"
            ),
        }
    });
    true
}

/// Store a connection, replacing an earlier one to the same addon, and start
/// the first import in the background when the provider offers one. With no
/// `event_filters` a reconnect keeps what the user had chosen and a first
/// connection gets the provider's default.
pub async fn connect_tracker(
    ctx: &AppContext,
    user_id: Uuid,
    addon_id: Uuid,
    credentials: MediaTrackerCredentials,
    event_filters: Option<Vec<MediaTrackerEventKind>>,
) -> Result<db::UserMediaTracker> {
    let addon = ctx
        .addons
        .media_tracker_for(addon_id)
        .ok_or_else(|| anyhow!("the tracker addon is disabled or gone"))?;
    let caps = addon.capabilities();
    let event_filters = match event_filters {
        Some(filters) => filters,
        None => {
            db::UserMediaTracker::get_for_user_and_addon(&ctx.db, user_id, addon_id)
                .await?
                .map(|existing| existing.event_filters)
                .unwrap_or(caps.default_event_filter)
        }
    };
    let mut tracker =
        db::UserMediaTracker::new(user_id, addon_id, credentials, event_filters);
    tracker.account_name = addon.account_label(&tracker.credentials);
    tracker
        .upsert(&ctx.db)
        .await?;
    // A reconnect keeps the earlier row's id: read back what is stored.
    let tracker =
        db::UserMediaTracker::get_for_user_and_addon(&ctx.db, user_id, addon_id)
            .await?
            .context("connection vanished after being stored")?;
    if caps.history_import {
        spawn_sync(ctx.clone(), tracker.clone(), true);
    }
    Ok(tracker)
}

pub struct MediaTrackerSubscriber {
    pub ctx: AppContext,
}

#[async_trait]
impl Subscriber for MediaTrackerSubscriber {
    fn key(&self) -> &'static str {
        "media_tracker"
    }

    fn events(&self) -> &[EventType] {
        &[
            EventType::PlaybackStarted,
            EventType::PlaybackProgress,
            EventType::PlaybackStopped,
            EventType::MarkPlayed,
            EventType::MarkUnplayed,
            EventType::MarkFavorite,
            EventType::UnmarkFavorite,
            EventType::Rating,
        ]
    }

    fn delivery_mode(&self) -> DeliveryMode {
        DeliveryMode::Persistent {
            max_retries: Some(12),
        }
    }

    async fn handle(&self, event: Event) -> anyhow::Result<()> {
        let (user_id, media_id, tracker_event) = match event {
            Event::PlaybackStarted(i) => (
                i.user_id,
                i.media_id,
                MediaTrackerEvent::PlaybackStart {
                    position_ticks: i.position_ticks,
                },
            ),
            Event::PlaybackProgress(i) => (
                i.user_id,
                i.media_id,
                MediaTrackerEvent::PlaybackProgress {
                    position_ticks: i.position_ticks,
                    is_paused: i.is_paused,
                },
            ),
            Event::PlaybackStopped(i) => (
                i.user_id,
                i.media_id,
                MediaTrackerEvent::PlaybackStop {
                    position_ticks: i.position_ticks,
                    played: i.played,
                },
            ),
            Event::MarkPlayed(i) => {
                (i.user_id, i.media_id, MediaTrackerEvent::MarkPlayed)
            }
            Event::MarkUnplayed(i) => {
                (i.user_id, i.media_id, MediaTrackerEvent::MarkUnplayed)
            }
            Event::MarkFavorite(i) => {
                (i.user_id, i.media_id, MediaTrackerEvent::MarkFavorite)
            }
            Event::UnmarkFavorite(i) => {
                (i.user_id, i.media_id, MediaTrackerEvent::UnmarkFavorite)
            }
            Event::Rating(i) => (
                i.user_id,
                i.media_id,
                MediaTrackerEvent::Rating { rating: i.rating },
            ),
            _ => return Ok(()),
        };

        if !self
            .ctx
            .addons
            .has_media_tracker()
        {
            return Ok(());
        }

        let kind = tracker_event.kind();
        let connected: Vec<db::UserMediaTracker> = db::UserMediaTracker::list_for_user(
            &self
                .ctx
                .db,
            user_id,
        )
        .await?
        .into_iter()
        .filter(|t| t.status == db::MediaTrackerStatus::Connected)
        .collect();

        // Simkl asks media servers to pull when a session ends, rather than
        // on a timer. Throttled, and never twice at once.
        if matches!(tracker_event, MediaTrackerEvent::PlaybackStop { .. }) {
            let now = Utc::now().naive_utc();
            for tracker in &connected {
                let due = tracker
                    .last_pull_at
                    .is_none_or(|at| now - at >= PLAYBACK_PULL_THROTTLE);
                let can_pull = self
                    .ctx
                    .addons
                    .media_tracker_for(tracker.addon_id)
                    .is_some_and(|a| pulls(&a.capabilities()));
                if due && can_pull {
                    spawn_sync(
                        self.ctx
                            .clone(),
                        tracker.clone(),
                        false,
                    );
                }
            }
        }

        let wanted: Vec<db::UserMediaTracker> = connected
            .into_iter()
            .filter(|t| t.wants(kind))
            .filter(|t| {
                self.ctx
                    .addons
                    .media_tracker_for(t.addon_id)
                    .is_some_and(|a| {
                        a.capabilities()
                            .supports(kind)
                    })
            })
            .collect();

        if wanted.is_empty() {
            return Ok(());
        }

        let Some(mut media) = db::Media::get_by_id(
            &self
                .ctx
                .db,
            &media_id,
        )
        .await?
        else {
            return Ok(());
        };

        let Some(target) = resolve_target(&self.ctx, &mut media).await? else {
            return Ok(());
        };

        let tctx = MediaTrackerCtx {
            config: Arc::new(
                self.ctx
                    .config
                    .clone(),
            ),
        };

        // Every failure lands on its connection, where the user sees it. Only
        // a retryable one is handed back: the dispatcher retries what it is
        // given, and a provider that has said no will keep saying no.
        let mut retry: Option<MediaTrackerError> = None;
        for tracker in &wanted {
            if let Some(addon) = self
                .ctx
                .addons
                .media_tracker_for(tracker.addon_id)
            {
                match addon
                    .on_event(&tracker_event, &target, &tracker.credentials, &tctx)
                    .await
                {
                    Ok(()) => {
                        let _ = db::UserMediaTracker::mark_success(
                            &self
                                .ctx
                                .db,
                            tracker.id,
                        )
                        .await;
                    }
                    Err(e) => {
                        if let Err(db_err) = db::UserMediaTracker::mark_failure(
                            &self
                                .ctx
                                .db,
                            tracker.id,
                            &e,
                        )
                        .await
                        {
                            warn!(tracker = %tracker.id, error = %db_err, "could not record a tracker failure");
                        }
                        if e.is_retryable() {
                            retry.get_or_insert(e);
                        } else {
                            warn!(
                                tracker = %tracker.id,
                                user = %user_id,
                                error = %e,
                                "media tracker refused an event"
                            );
                        }
                    }
                }
            }
        }

        match retry {
            Some(e) => Err(anyhow!("{e}")),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        addons::{
            Addon, AddonCapabilities, AddonKind, AddonPresetRef, AddonRuntime,
            media_tracker::{
                MediaTrackerAddon, MediaTrackerCapabilities, MediaTrackerCredentials,
                MediaTrackerError, MediaTrackerEventKind, MediaTrackerResult,
                SyncDirection,
            },
        },
        db::{MediaTrackerStatus, UserMediaTracker},
        integration_test::{new_test_server, seed_episode, seed_movie},
        signals::MarkPlayedInfo,
    };
    use async_trait::async_trait;
    use chrono::Utc;
    use std::sync::Arc;

    struct StubMediaTracker {
        events: Vec<MediaTrackerEventKind>,
        /// What `pull_changes` hands back.
        pull: Vec<RemoteWatch>,
        /// Make `pull_changes` fail as a rejected token would.
        fail_pull_with_reauth: bool,
        /// Make `on_event` fail this way.
        fail_events: Option<fn() -> MediaTrackerError>,
        /// The `since` each pull was asked for.
        pulls_seen: std::sync::Mutex<Vec<Option<chrono::NaiveDateTime>>>,
    }

    impl StubMediaTracker {
        fn everything() -> Self {
            Self {
                events: vec![
                    MediaTrackerEventKind::PlaybackStart,
                    MediaTrackerEventKind::PlaybackProgress,
                    MediaTrackerEventKind::PlaybackStop,
                    MediaTrackerEventKind::MarkPlayed,
                    MediaTrackerEventKind::MarkUnplayed,
                    MediaTrackerEventKind::MarkFavorite,
                    MediaTrackerEventKind::UnmarkFavorite,
                    MediaTrackerEventKind::Rating,
                ],
                pull: Vec::new(),
                fail_pull_with_reauth: false,
                fail_events: None,
                pulls_seen: Default::default(),
            }
        }

        fn pulling(pull: Vec<RemoteWatch>) -> Self {
            Self {
                pull,
                ..Self::everything()
            }
        }
    }

    impl AddonKind for StubMediaTracker {
        fn id(&self) -> &'static str {
            "scripted"
        }
    }

    #[async_trait]
    impl MediaTrackerAddon for StubMediaTracker {
        fn capabilities(&self) -> MediaTrackerCapabilities {
            MediaTrackerCapabilities {
                supported_events: self
                    .events
                    .clone(),
                watch_state_sync: SyncDirection::Both,
                ratings: SyncDirection::Both,
                ..Default::default()
            }
        }

        async fn on_event(
            &self,
            _event: &MediaTrackerEvent,
            _target: &MediaTrackerTarget,
            _creds: &MediaTrackerCredentials,
            _ctx: &MediaTrackerCtx,
        ) -> MediaTrackerResult<()> {
            match self.fail_events {
                Some(make) => Err(make()),
                None => Ok(()),
            }
        }

        async fn pull_changes(
            &self,
            since: Option<chrono::NaiveDateTime>,
            _creds: &MediaTrackerCredentials,
            _ctx: &MediaTrackerCtx,
        ) -> MediaTrackerResult<Vec<RemoteWatch>> {
            self.pulls_seen
                .lock()
                .unwrap()
                .push(since);
            if self.fail_pull_with_reauth {
                return Err(MediaTrackerError::reauth("401"));
            }
            Ok(self
                .pull
                .clone())
        }
    }

    async fn connect(
        ctx: &AppContext,
        name: &str,
        status: MediaTrackerStatus,
        filters: Vec<MediaTrackerEventKind>,
    ) -> Uuid {
        let addon = crate::integration_test::register_media_tracker(
            ctx,
            name,
            Arc::new(StubMediaTracker::everything()),
        )
        .await;
        let mut tracker = UserMediaTracker::new(
            user_id(ctx).await,
            addon.id,
            Default::default(),
            filters,
        );
        tracker.status = status;
        tracker
            .upsert(&ctx.db)
            .await
            .unwrap();
        tracker.id
    }

    async fn user_id(ctx: &AppContext) -> Uuid {
        db::User::get_by_username(&ctx.db, "test")
            .await
            .unwrap()
            .unwrap()
            .id
    }

    /// The walk to the series follows `parent_id`, which an episode saved
    /// without a season row above it does not have.
    #[tokio::test]
    async fn a_flat_episode_still_reaches_its_series() {
        let (_s, guard) = new_test_server()
            .await
            .unwrap();
        let ctx = &guard.0;

        let external_ids = db::ExternalIds {
            imdb: db::NonEmptyString::try_new("tt0306414".to_string()).ok(),
            tmdb: Some(1438),
            ..Default::default()
        };
        let mut series = db::Media {
            id: Uuid::from(&db::MediaIdRaw {
                kind: db::MediaKind::Series,
                external_ids: external_ids.clone(),
                season: None,
                episode: None,
            }),
            title: "The Wire".into(),
            kind: db::MediaKind::Series,
            external_ids,
            ..Default::default()
        };
        series
            .save(&ctx.db)
            .await
            .unwrap();

        let mut episode = db::Media {
            title: "The Target".into(),
            kind: db::MediaKind::Episode,
            grandparent_id: Some(series.id),
            idx: Some(1),
            parent_idx: Some(1),
            external_ids: db::ExternalIds {
                imdb: db::NonEmptyString::try_new("tt0749451".to_string()).ok(),
                tvdb: Some(299034),
                ..Default::default()
            },
            ..Default::default()
        };
        episode
            .save(&ctx.db)
            .await
            .unwrap();

        let target = resolve_target(ctx, &mut episode)
            .await
            .unwrap()
            .expect("an episode alone identifies nothing");

        assert_eq!(
            target
                .series
                .expect("no season row is not no series")
                .ids
                .tmdb,
            Some(1438)
        );
    }

    #[tokio::test]
    async fn subscriber_delivers_to_connected_trackers() {
        let (_s, guard) = new_test_server()
            .await
            .unwrap();
        let ctx = &guard.0;
        let _tracker = connect(
            ctx,
            "a",
            MediaTrackerStatus::Connected,
            vec![MediaTrackerEventKind::MarkPlayed],
        )
        .await;
        let media = crate::integration_test::seed_movie(ctx).await;
        let uid = user_id(ctx).await;

        let sub = MediaTrackerSubscriber { ctx: ctx.clone() };
        let result = sub
            .handle(Event::MarkPlayed(MarkPlayedInfo {
                user_id: uid,
                media_id: media.id,
            }))
            .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn subscriber_ignores_playback_progress_for_mark_played_tracker() {
        let (_s, guard) = new_test_server()
            .await
            .unwrap();
        let ctx = &guard.0;
        let _tracker = connect(
            ctx,
            "a",
            MediaTrackerStatus::Connected,
            vec![MediaTrackerEventKind::MarkPlayed],
        )
        .await;
        let media = crate::integration_test::seed_movie(ctx).await;
        let uid = user_id(ctx).await;

        let sub = MediaTrackerSubscriber { ctx: ctx.clone() };
        // PlaybackProgress is not in the subscriber's event list at all — emit
        // returns immediately without calling handle, but we can test that
        // handle on an unrelated event returns Ok.
        let result = sub
            .handle(Event::PlaybackProgress(crate::signals::PlaybackContext {
                user_id: uid,
                media_id: media.id,
                position_ticks: 1000,
                is_paused: false,
                ..Default::default()
            }))
            .await;

        assert!(result.is_ok());
    }
    fn imdb(id: &str) -> Option<db::NonEmptyString> {
        db::NonEmptyString::try_new(id.to_string()).ok()
    }

    fn watched_at() -> chrono::NaiveDateTime {
        chrono::NaiveDate::from_ymd_opt(2024, 5, 1)
            .unwrap()
            .and_hms_opt(18, 0, 0)
            .unwrap()
    }

    fn remote(
        ids: db::ExternalIds,
        season: Option<i64>,
        episode: Option<i64>,
    ) -> RemoteWatch {
        RemoteWatch {
            kind: if season.is_some() {
                RemoteKind::Show
            } else {
                RemoteKind::Movie
            },
            ids,
            season,
            episode,
            watched: true,
            position_ticks: None,
            watched_at: Some(watched_at()),
            favorite: None,
            rating: None,
        }
    }

    async fn test_user(ctx: &AppContext) -> db::User {
        db::User::get_by_username(&ctx.db, "test")
            .await
            .unwrap()
            .unwrap()
    }

    async fn state_of(
        ctx: &AppContext,
        user: &db::User,
        media: &db::Media,
    ) -> db::UserMediaState {
        db::UserMediaState::get_or_new(&ctx.db, user, media)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn a_remote_watch_marks_a_movie_played_once_and_carries_its_rating() {
        let (_s, guard) = new_test_server()
            .await
            .unwrap();
        let ctx = &guard.0;
        let movie = seed_movie(ctx).await;
        let user = test_user(ctx).await;

        let mut watch = remote(
            db::ExternalIds {
                tmdb: Some(949),
                ..Default::default()
            },
            None,
            None,
        );
        watch.rating = Some(8.0);

        let report = apply_remote_watches(ctx, &user, std::slice::from_ref(&watch))
            .await
            .unwrap();
        assert_eq!(
            report,
            SyncReport {
                received: 1,
                matched: 1,
                unmatched: 0,
                failed: 0,
                marked_played: 1,
                rated: 1,
                favorites: 0,
            }
        );
        let state = state_of(ctx, &user, &movie).await;
        assert!(state.play_count > 0);
        assert_eq!(
            state.played_at,
            Some(watched_at()),
            "the provider's date wins"
        );
        assert_eq!(state.rating, Some(8.0));

        // The same report again is a no-op, not a second play.
        let again = apply_remote_watches(ctx, &user, &[watch])
            .await
            .unwrap();
        assert_eq!(again.matched, 1);
        assert_eq!(again.marked_played, 0);
        assert_eq!(again.rated, 0);
        assert_eq!(
            state_of(ctx, &user, &movie)
                .await
                .play_count,
            1
        );
    }

    /// A list says what is in it, not what left it.
    #[tokio::test]
    async fn a_remote_list_never_unmarks_or_unrates_locally() {
        let (_s, guard) = new_test_server()
            .await
            .unwrap();
        let ctx = &guard.0;
        let movie = seed_movie(ctx).await;
        let user = test_user(ctx).await;
        movie
            .mark_played(&ctx.db, &user, false, None)
            .await
            .unwrap();
        db::UserMediaState::set_rating(
            &ctx.db,
            &user,
            &movie,
            Some(db::UserRating::try_from(6.0).unwrap()),
        )
        .await
        .unwrap();

        let mut watch = remote(
            db::ExternalIds {
                imdb: imdb("tt0113277"),
                ..Default::default()
            },
            None,
            None,
        );
        watch.watched = false;
        watch.rating = None;
        let report = apply_remote_watches(ctx, &user, &[watch])
            .await
            .unwrap();
        assert_eq!(report.matched, 1);
        assert_eq!(report.marked_played, 0);

        let state = state_of(ctx, &user, &movie).await;
        assert!(state.play_count > 0, "still played");
        assert_eq!(state.rating, Some(6.0), "still rated");
    }

    #[tokio::test]
    async fn an_episode_is_located_through_its_series_and_the_rest_is_unmatched() {
        let (_s, guard) = new_test_server()
            .await
            .unwrap();
        let ctx = &guard.0;
        let episode = seed_episode(ctx).await;
        let user = test_user(ctx).await;
        let series_ids = db::ExternalIds {
            imdb: imdb("tt0306414"),
            ..Default::default()
        };

        let report = apply_remote_watches(
            ctx,
            &user,
            &[
                remote(series_ids.clone(), Some(1), Some(1)),
                // An episode the library does not have.
                remote(series_ids.clone(), Some(9), Some(9)),
                // A season number alone names the season row.
                remote(series_ids.clone(), Some(1), None),
                // A show nobody has.
                remote(
                    db::ExternalIds {
                        tmdb: Some(1),
                        ..Default::default()
                    },
                    None,
                    None,
                ),
            ],
        )
        .await
        .unwrap();
        assert_eq!(report.received, 4);
        assert_eq!(report.matched, 2);
        assert_eq!(report.unmatched, 2);

        assert!(
            state_of(ctx, &user, &episode)
                .await
                .play_count
                > 0
        );
        let season = db::Media::get_by_id(
            &ctx.db,
            &episode
                .parent_id
                .unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(
            state_of(ctx, &user, &season)
                .await
                .play_count
                > 0,
            "its only episode was watched, so the season is too"
        );

        let located = locate_remote(&ctx.db, &remote(series_ids, Some(1), Some(1)))
            .await
            .unwrap()
            .expect("the episode is found by position");
        assert_eq!(located.id, episode.id);
    }

    #[tokio::test]
    async fn a_sync_moves_the_watermark_and_a_rejected_token_disconnects() {
        let (_s, guard) = new_test_server()
            .await
            .unwrap();
        let ctx = &guard.0;
        let movie = seed_movie(ctx).await;
        let user = test_user(ctx).await;

        let stub = Arc::new(StubMediaTracker::pulling(vec![remote(
            db::ExternalIds {
                tmdb: Some(949),
                ..Default::default()
            },
            None,
            None,
        )]));
        let addon =
            crate::integration_test::register_media_tracker(ctx, "pulls", stub.clone())
                .await;
        let tracker =
            UserMediaTracker::new(user.id, addon.id, Default::default(), vec![]);
        tracker
            .upsert(&ctx.db)
            .await
            .unwrap();

        let report = sync_tracker(ctx, &tracker, false)
            .await
            .unwrap();
        assert_eq!(report.marked_played, 1);
        assert!(
            state_of(ctx, &user, &movie)
                .await
                .play_count
                > 0
        );
        let after = UserMediaTracker::get(&ctx.db, tracker.id)
            .await
            .unwrap()
            .unwrap();
        assert!(
            after
                .last_pull_at
                .is_some(),
            "the watermark moves"
        );
        assert!(
            after
                .last_success_at
                .is_some()
        );
        assert_eq!(after.status, MediaTrackerStatus::Connected);

        // The next pull is a delta from the watermark; a forced one is not.
        sync_tracker(ctx, &after, false)
            .await
            .unwrap();
        sync_tracker(ctx, &after, true)
            .await
            .unwrap();
        let seen = stub
            .pulls_seen
            .lock()
            .unwrap()
            .clone();
        assert_eq!(seen[0], None, "nothing pulled yet: everything");
        assert_eq!(seen[1], after.last_pull_at);
        assert_eq!(seen[2], None, "full re-read");
    }

    #[tokio::test]
    async fn a_failed_pull_is_recorded_on_the_connection() {
        let (_s, guard) = new_test_server()
            .await
            .unwrap();
        let ctx = &guard.0;
        let user = test_user(ctx).await;
        let stub = Arc::new(StubMediaTracker {
            fail_pull_with_reauth: true,
            ..StubMediaTracker::everything()
        });
        let addon =
            crate::integration_test::register_media_tracker(ctx, "broken", stub).await;
        let tracker =
            UserMediaTracker::new(user.id, addon.id, Default::default(), vec![]);
        tracker
            .upsert(&ctx.db)
            .await
            .unwrap();

        assert!(
            sync_tracker(ctx, &tracker, false)
                .await
                .is_err()
        );
        let after = UserMediaTracker::get(&ctx.db, tracker.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after.status, MediaTrackerStatus::AuthExpired);
        assert!(
            after
                .last_error
                .is_some()
        );
        assert_eq!(after.last_pull_at, None);

        // Once disconnected, a sync is refused before the provider is asked.
        assert!(
            sync_tracker(ctx, &after, false)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn connecting_labels_the_account_and_keeps_one_row_per_addon() {
        let (_s, guard) = new_test_server()
            .await
            .unwrap();
        let ctx = &guard.0;
        let user = test_user(ctx).await;
        let addon = crate::integration_test::register_media_tracker(
            ctx,
            "labelled",
            Arc::new(StubMediaTracker::everything()),
        )
        .await;

        let first = connect_tracker(
            ctx,
            user.id,
            addon.id,
            MediaTrackerCredentials::new(serde_json::json!({"token": "a"})),
            Some(vec![MediaTrackerEventKind::MarkPlayed]),
        )
        .await
        .unwrap();
        let second = connect_tracker(
            ctx,
            user.id,
            addon.id,
            MediaTrackerCredentials::new(serde_json::json!({"token": "b"})),
            Some(vec![MediaTrackerEventKind::Rating]),
        )
        .await
        .unwrap();
        assert_eq!(first.id, second.id, "a reconnect keeps the row");
        assert_eq!(second.event_filters, vec![MediaTrackerEventKind::Rating]);
        assert_eq!(
            second
                .credentials
                .get_str("token"),
            Some("b")
        );
        // A reconnect that says nothing about filters keeps the user's.
        let third = connect_tracker(
            ctx,
            user.id,
            addon.id,
            MediaTrackerCredentials::new(serde_json::json!({"token": "c"})),
            None,
        )
        .await
        .unwrap();
        assert_eq!(third.event_filters, vec![MediaTrackerEventKind::Rating]);
        assert_eq!(
            third
                .credentials
                .get_str("token"),
            Some("c")
        );
        assert_eq!(
            UserMediaTracker::list_for_user(&ctx.db, user.id)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    /// TMDB numbers movies and shows separately, so a show's id can equal a
    /// movie's. Each kind must only ever find its own.
    #[tokio::test]
    async fn a_remote_show_never_lands_on_a_movie_sharing_its_tmdb_number() {
        let (_s, guard) = new_test_server()
            .await
            .unwrap();
        let ctx = &guard.0;
        let movie = seed_movie(ctx).await; // tmdb 949
        // A series must carry an imdb id; the tmdb number is what collides.
        let episode = crate::integration_test::seed_episode_with(
            ctx,
            db::ExternalIds {
                imdb: imdb("tt9999999"),
                tmdb: Some(949),
                ..Default::default()
            },
        )
        .await;
        let user = test_user(ctx).await;
        let ids = db::ExternalIds {
            tmdb: Some(949),
            ..Default::default()
        };

        let mut show = remote(ids.clone(), None, None);
        show.kind = RemoteKind::Show;
        let series = locate_remote(&ctx.db, &show)
            .await
            .unwrap()
            .expect("the series is found");
        assert_eq!(series.kind, db::MediaKind::Series);
        assert_eq!(
            series.id,
            episode
                .grandparent_id
                .unwrap()
        );

        let as_movie = remote(ids.clone(), None, None);
        assert_eq!(
            locate_remote(&ctx.db, &as_movie)
                .await
                .unwrap()
                .unwrap()
                .id,
            movie.id
        );

        // Applying the show's watch leaves the movie alone.
        apply_remote_watches(ctx, &user, &[show])
            .await
            .unwrap();
        assert_eq!(
            state_of(ctx, &user, &movie)
                .await
                .play_count,
            0
        );
    }

    /// A pushed 8.4 becomes 8 on the provider; pulling it back must not turn
    /// the user's 8.4 into 8.0.
    #[tokio::test]
    async fn a_pulled_rating_that_rounds_the_same_is_not_applied() {
        let (_s, guard) = new_test_server()
            .await
            .unwrap();
        let ctx = &guard.0;
        let movie = seed_movie(ctx).await;
        let user = test_user(ctx).await;
        db::UserMediaState::set_rating(
            &ctx.db,
            &user,
            &movie,
            Some(db::UserRating::try_from(8.4).unwrap()),
        )
        .await
        .unwrap();

        let mut same = remote(
            db::ExternalIds {
                tmdb: Some(949),
                ..Default::default()
            },
            None,
            None,
        );
        same.watched = false;
        same.rating = Some(8.0);
        let report = apply_remote_watches(ctx, &user, std::slice::from_ref(&same))
            .await
            .unwrap();
        assert_eq!(report.rated, 0);
        assert_eq!(
            state_of(ctx, &user, &movie)
                .await
                .rating,
            Some(8.4)
        );

        same.rating = Some(6.0);
        let report = apply_remote_watches(ctx, &user, &[same])
            .await
            .unwrap();
        assert_eq!(report.rated, 1);
        assert_eq!(
            state_of(ctx, &user, &movie)
                .await
                .rating,
            Some(6.0)
        );
    }

    #[test]
    fn one_pull_per_connection_at_a_time() {
        let id = Uuid::from_u128(77);
        let first = SyncGuard::try_begin(id).expect("free");
        assert!(is_syncing(id));
        assert!(SyncGuard::try_begin(id).is_none(), "already running");
        drop(first);
        assert!(!is_syncing(id));
        assert!(SyncGuard::try_begin(id).is_some());
    }

    /// A provider that rejects an event must say so on the connection, and a
    /// rejected token must switch it to needing a reconnect. Neither is worth
    /// retrying, so the dispatcher is told all is well.
    #[tokio::test]
    async fn a_refused_delivery_is_recorded_and_not_retried() {
        let (_s, guard) = new_test_server()
            .await
            .unwrap();
        let ctx = &guard.0;
        let media = crate::integration_test::seed_movie(ctx).await;
        let uid = user_id(ctx).await;
        let addon = crate::integration_test::register_media_tracker(
            ctx,
            "refusing",
            Arc::new(StubMediaTracker {
                fail_events: Some(|| MediaTrackerError::reauth("401")),
                ..StubMediaTracker::everything()
            }),
        )
        .await;
        let tracker = UserMediaTracker::new(
            uid,
            addon.id,
            Default::default(),
            vec![MediaTrackerEventKind::MarkPlayed],
        );
        tracker
            .upsert(&ctx.db)
            .await
            .unwrap();

        let sub = MediaTrackerSubscriber { ctx: ctx.clone() };
        let result = sub
            .handle(Event::MarkPlayed(MarkPlayedInfo {
                user_id: uid,
                media_id: media.id,
            }))
            .await;
        assert!(result.is_ok(), "a permanent refusal is not retried");

        let after = UserMediaTracker::get(&ctx.db, tracker.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after.status, MediaTrackerStatus::AuthExpired);
        assert_eq!(
            after
                .last_error
                .as_deref(),
            Some("401 (reconnect required)")
        );
    }

    #[tokio::test]
    async fn a_retryable_delivery_failure_is_handed_back_to_the_dispatcher() {
        let (_s, guard) = new_test_server()
            .await
            .unwrap();
        let ctx = &guard.0;
        let media = crate::integration_test::seed_movie(ctx).await;
        let uid = user_id(ctx).await;
        let addon = crate::integration_test::register_media_tracker(
            ctx,
            "flaky",
            Arc::new(StubMediaTracker {
                fail_events: Some(|| MediaTrackerError::retryable("503")),
                ..StubMediaTracker::everything()
            }),
        )
        .await;
        let tracker = UserMediaTracker::new(
            uid,
            addon.id,
            Default::default(),
            vec![MediaTrackerEventKind::MarkPlayed],
        );
        tracker
            .upsert(&ctx.db)
            .await
            .unwrap();

        let sub = MediaTrackerSubscriber { ctx: ctx.clone() };
        assert!(
            sub.handle(Event::MarkPlayed(MarkPlayedInfo {
                user_id: uid,
                media_id: media.id,
            }))
            .await
            .is_err()
        );
        let after = UserMediaTracker::get(&ctx.db, tracker.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            after.status,
            MediaTrackerStatus::Connected,
            "a blip is not a disconnect"
        );
        assert_eq!(
            after
                .last_error
                .as_deref(),
            Some("503")
        );
    }

    /// The hourly task works from a snapshot. A reconnect in the meantime is
    /// a different connection with the same row id, and must not inherit the
    /// old pull's verdict.
    #[tokio::test]
    async fn a_pull_from_a_stale_snapshot_does_not_write_over_a_reconnect() {
        let (_s, guard) = new_test_server()
            .await
            .unwrap();
        let ctx = &guard.0;
        let user = test_user(ctx).await;
        let addon = crate::integration_test::register_media_tracker(
            ctx,
            "snapshot",
            Arc::new(StubMediaTracker::pulling(vec![])),
        )
        .await;
        let snapshot =
            UserMediaTracker::new(user.id, addon.id, Default::default(), vec![]);
        snapshot
            .upsert(&ctx.db)
            .await
            .unwrap();

        // Someone reconnects before the pull gets to write.
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        let mut reconnected =
            UserMediaTracker::new(user.id, addon.id, Default::default(), vec![]);
        reconnected.account_name = Some("new account".into());
        reconnected
            .upsert(&ctx.db)
            .await
            .unwrap();

        sync_tracker(ctx, &snapshot, false)
            .await
            .unwrap();
        let after = UserMediaTracker::get(&ctx.db, snapshot.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after.last_pull_at, None, "the stale pull left no watermark");
        assert_eq!(
            after
                .account_name
                .as_deref(),
            Some("new account")
        );
    }
}

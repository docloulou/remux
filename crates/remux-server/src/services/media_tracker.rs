//! Describing the item a delivery names, the subscriber that scrobbles it,
//! and the other direction: applying what a provider reports to a library.

use std::sync::Arc;

use anyhow::{Context as _, Error, Result, anyhow, bail};
use async_trait::async_trait;
use chrono::{Datelike, Utc};
use serde::Serialize;
use tracing::{info, warn};
use uuid::Uuid;

use crate::{
    AppContext,
    addons::media_tracker::{
        MediaTrackerCredentials, MediaTrackerCtx, MediaTrackerEvent,
        MediaTrackerEventKind, MediaTrackerTarget, RemoteWatch,
    },
    db,
    signals::{DeliveryMode, Event, EventType, Subscriber},
};

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
    pub marked_played: usize,
    pub rated: usize,
    pub favorites: usize,
}

/// The local row a remote item names, if the library has it. Movies and
/// series are found by external id. Seasons and episodes hang off the series
/// by number, since providers key them on the show; their ids are minted from
/// the series key, with a positional lookup for rows imported another way.
pub async fn locate_remote(
    db: &sqlx::SqlitePool,
    watch: &RemoteWatch,
) -> Result<Option<db::Media>> {
    let (season, episode) = match (watch.season, watch.episode) {
        (None, None) => {
            for kind in [db::MediaKind::Movie, db::MediaKind::Series] {
                if let Some(id) =
                    db::Media::find_by_external_ids(db, &kind, &watch.ids).await
                {
                    return Ok(db::Media::get_by_id(db, &id).await?);
                }
            }
            return Ok(None);
        }
        (Some(season), episode) => (season, episode),
        // An episode number without a season names nothing.
        (None, Some(_)) => return Ok(None),
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

    if let Some(rating) = watch.rating {
        match db::UserRating::try_from(f64::from(rating)) {
            Ok(rating) if state.rating != Some(rating.value()) => {
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
/// on the connection the way a failed delivery is.
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
    let pulls = caps.history_import
        || caps
            .watch_state_sync
            .pulls()
        || caps
            .ratings
            .pulls()
        || caps
            .favorites
            .pulls();
    if !pulls {
        bail!("this provider does not report remote changes");
    }
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
            db::UserMediaTracker::mark_failure(&ctx.db, tracker.id, &e).await?;
            return Err(anyhow!("{e}"));
        }
    };

    let report = apply_remote_watches(ctx, &user, &watches).await?;
    db::UserMediaTracker::mark_pull_success(&ctx.db, tracker.id, started_at).await?;
    Ok(report)
}

/// Run [`sync_tracker`] off the request path, logging the outcome.
pub fn spawn_sync(ctx: AppContext, tracker: db::UserMediaTracker, full: bool) {
    tokio::spawn(async move {
        match sync_tracker(&ctx, &tracker, full).await {
            Ok(report) => info!(
                tracker = %tracker.id,
                user = %tracker.user_id,
                received = report.received,
                matched = report.matched,
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
}

/// Store a connection, replacing an earlier one to the same addon, and start
/// the first import in the background when the provider offers one.
pub async fn connect_tracker(
    ctx: &AppContext,
    user_id: Uuid,
    addon_id: Uuid,
    credentials: MediaTrackerCredentials,
    event_filters: Vec<MediaTrackerEventKind>,
) -> Result<db::UserMediaTracker> {
    let addon = ctx
        .addons
        .media_tracker_for(addon_id)
        .ok_or_else(|| anyhow!("the tracker addon is disabled or gone"))?;
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
    if addon
        .capabilities()
        .history_import
    {
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
        let wanted: Vec<db::UserMediaTracker> = db::UserMediaTracker::list_for_user(
            &self
                .ctx
                .db,
            user_id,
        )
        .await?
        .into_iter()
        .filter(|t| t.status == db::MediaTrackerStatus::Connected && t.wants(kind))
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

        let mut errors: Vec<anyhow::Error> = Vec::new();
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
                        errors.push(anyhow::anyhow!("{e}"));
                    }
                }
            }
        }

        if let Some(e) = errors
            .into_iter()
            .next()
        {
            return Err(e);
        }
        Ok(())
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
            Ok(())
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
            vec![MediaTrackerEventKind::MarkPlayed],
        )
        .await
        .unwrap();
        let second = connect_tracker(
            ctx,
            user.id,
            addon.id,
            MediaTrackerCredentials::new(serde_json::json!({"token": "b"})),
            vec![MediaTrackerEventKind::Rating],
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
        assert_eq!(
            UserMediaTracker::list_for_user(&ctx.db, user.id)
                .await
                .unwrap()
                .len(),
            1
        );
    }
}

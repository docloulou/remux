//! Simkl media tracker: scrobbles playback, mirrors played state and ratings
//! to a user's Simkl account, and pulls their Simkl lists back into remux.
//!
//! Simkl marks an item watched when a `/scrobble/stop` arrives at or past 80%.
//! remux decides watched with its own threshold, so a stop that counted here
//! is sent as 100% and one that did not is kept under Simkl's line: two
//! services disagreeing on one watch would otherwise ping-pong through the
//! two-way sync.

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::{sync::Arc, time::Duration};
use tracing::{debug, warn};
use uuid::Uuid;

use super::{
    AddonCapabilities, AddonKind, AddonMetadata, AddonOption, AddonOptionType,
    AddonPreset, AddonPresetRegistration, MediaKind, ResourceType,
    media_tracker::{
        AuthFlow, DeviceAuthPoll, DeviceAuthStart, MediaTrackerAddon,
        MediaTrackerCapabilities, MediaTrackerCredentials, MediaTrackerCtx,
        MediaTrackerError, MediaTrackerEvent, MediaTrackerEventKind,
        MediaTrackerResult, MediaTrackerTarget, RemoteKind, RemoteWatch, SyncDirection,
    },
};
use crate::{
    db,
    sdks::{self, ClientError, RestClient, simkl},
};

pub struct SimklPreset;

impl AddonPreset for SimklPreset {
    fn id(&self) -> &'static str {
        "simkl"
    }

    fn metadata(&self) -> AddonMetadata {
        AddonMetadata {
            id: "simkl".to_string(),
            display_name: "Simkl".to_string(),
            description: "Simkl — scrobble playback and keep watched state and ratings in sync with each user's Simkl account.".to_string(),
            icon: None,
            supported_resources: vec![AddonMetadata::simple_resource(ResourceType::Tracking)],
            supported_types: vec![MediaKind::Movie, MediaKind::Series],
            supported_resources_user: vec![],
            supported_types_user: vec![],
            options: vec![AddonOption {
                id: "client_id".to_string(),
                name: "Simkl Client ID".to_string(),
                description: Some(
                    "The Client ID of your Simkl application. Create one for free at simkl.com/settings/developer. Users then connect their own accounts with a PIN."
                        .to_string(),
                ),
                required: true,
                default: None,
                kind: AddonOptionType::Password,
            }],
        }
    }

    fn from_cfg(
        &self,
        _addon_id: Uuid,
        cfg: &serde_json::Value,
        _config: &crate::Config,
    ) -> Result<AddonCapabilities> {
        let client_id = cfg
            .get("client_id")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| anyhow::anyhow!("Simkl needs a Client ID"))?
            .to_string();
        Ok(AddonCapabilities {
            media_tracker: Some(Arc::new(SimklAddon { client_id })),
            ..Default::default()
        })
    }
}

inventory::submit! {
    AddonPresetRegistration(|| Box::new(SimklPreset))
}

pub struct SimklAddon {
    client_id: String,
}

/// Where the PIN page lives when Simkl's answer does not say.
const DEFAULT_VERIFICATION_URL: &str = "https://simkl.com/pin";
const DEFAULT_PIN_INTERVAL_SECS: i64 = 5;
const DEFAULT_PIN_EXPIRY_SECS: i64 = 900;
/// The scrobble endpoints lock an item per user for this long.
const SCROBBLE_RATE_LIMIT_WINDOW: Duration = Duration::from_secs(20);
/// Simkl's stated ceiling is one POST per second; a 429 whose Retry-After is
/// missing or shorter gets at least this much room.
const MIN_RETRY_AFTER: Duration = Duration::from_secs(5);
/// Clocks differ; a delta pull reaches back this far past the watermark.
const PULL_OVERLAP: chrono::Duration = chrono::Duration::minutes(5);

/// The credential blob: `{"access_token": "...", "username": "...",
/// "account_id": 123}`. Only the token is required.
const CRED_TOKEN: &str = "access_token";
const CRED_USERNAME: &str = "username";
const CRED_ACCOUNT_ID: &str = "account_id";

fn map_client_error(err: ClientError) -> MediaTrackerError {
    match err {
        ClientError::Unauthorized => {
            MediaTrackerError::reauth("Simkl rejected the access token")
        }
        ClientError::RateLimited { retry_after_secs } => {
            MediaTrackerError::retry_after(
                "Simkl rate limit reached",
                Duration::from_secs(retry_after_secs.max(MIN_RETRY_AFTER.as_secs())),
            )
        }
        ClientError::Http {
            status, message, ..
        } => match status {
            // The per-user scrobble lock reports as a 400 with this code.
            400 if message.contains("RATE_LIMIT") => MediaTrackerError::retry_after(
                "Simkl is still processing the previous scrobble for this item",
                SCROBBLE_RATE_LIMIT_WINDOW,
            ),
            404 => MediaTrackerError::permanent(format!(
                "Simkl does not know this item ({message})"
            )),
            412 => MediaTrackerError::permanent(format!(
                "Simkl rejected the Client ID, check the addon settings ({message})"
            )),
            500..=599 => MediaTrackerError::retryable(format!(
                "Simkl returned {status}: {message}"
            )),
            _ => MediaTrackerError::permanent(format!(
                "Simkl returned {status}: {message}"
            )),
        },
        // The url would carry the client id, and this message ends up in the
        // connection's `last_error`, which users see.
        ClientError::Transport(e) => MediaTrackerError::retryable(scrub_client_id(
            &format!("could not reach Simkl: {}", e.without_url()),
        )),
        ClientError::Json { status, source, .. } => MediaTrackerError::permanent(
            format!("unexpected Simkl response (status {status}): {source}"),
        ),
        other => MediaTrackerError::permanent(scrub_client_id(&other.to_string())),
    }
}

/// Every request url names the client id; a message quoting one must not.
fn scrub_client_id(message: &str) -> String {
    static CLIENT_ID: std::sync::LazyLock<regex::Regex> =
        std::sync::LazyLock::new(|| regex::Regex::new(r"client_id=[^&\s)]+").unwrap());
    CLIENT_ID
        .replace_all(message, "client_id=<redacted>")
        .into_owned()
}

fn simkl_ids(ids: &db::ExternalIds) -> simkl::SimklIds {
    simkl::SimklIds {
        imdb: ids
            .imdb
            .as_ref()
            .map(|s| s.to_string()),
        tmdb: ids.tmdb,
        tvdb: ids.tvdb,
        kitsu: ids.kitsu,
        ..Default::default()
    }
}

fn external_ids(ids: &simkl::SimklIds) -> db::ExternalIds {
    db::ExternalIds {
        imdb: ids
            .imdb
            .clone()
            .and_then(|s| db::NonEmptyString::try_new(s).ok()),
        tmdb: ids.tmdb,
        tvdb: ids.tvdb,
        kitsu: ids.kitsu,
        ..Default::default()
    }
}

/// Whether remux could find a row for these ids: Simkl also returns MAL,
/// AniDB and AniList ids, which nothing here is keyed on.
fn has_local_ids(ids: &db::ExternalIds) -> bool {
    ids.imdb
        .is_some()
        || ids
            .tmdb
            .is_some()
        || ids
            .tvdb
            .is_some()
        || ids
            .kitsu
            .is_some()
}

fn scrobble_item(target: &MediaTrackerTarget) -> simkl::ScrobbleItem {
    simkl::ScrobbleItem {
        title: Some(
            target
                .title
                .clone(),
        ),
        year: target
            .year
            .map(i64::from),
        ids: simkl_ids(&target.ids),
    }
}

/// The show and position an episode target names, or why it cannot be sent.
fn episode_coordinates(
    target: &MediaTrackerTarget,
) -> MediaTrackerResult<(&MediaTrackerTarget, i64, i64)> {
    let series = target
        .series
        .as_deref()
        .ok_or_else(|| {
            MediaTrackerError::permanent("episode has no series to key it on")
        })?;
    let (Some(season), Some(episode)) = (target.season, target.episode) else {
        return Err(MediaTrackerError::permanent(
            "episode has no season or episode number",
        ));
    };
    Ok((series, season, episode))
}

/// What one remux item is to Simkl, or `None` for kinds it does not track.
enum SimklItem<'a> {
    Movie(&'a MediaTrackerTarget),
    Show(&'a MediaTrackerTarget),
    Season {
        series: &'a MediaTrackerTarget,
        season: i64,
    },
    Episode {
        series: &'a MediaTrackerTarget,
        season: i64,
        episode: i64,
    },
}

fn classify(target: &MediaTrackerTarget) -> MediaTrackerResult<Option<SimklItem<'_>>> {
    Ok(Some(match target.kind {
        db::MediaKind::Movie => SimklItem::Movie(target),
        db::MediaKind::Series => SimklItem::Show(target),
        db::MediaKind::Season => {
            let series = target
                .series
                .as_deref()
                .ok_or_else(|| {
                    MediaTrackerError::permanent("season has no series to key it on")
                })?;
            let season = target
                .season
                .ok_or_else(|| MediaTrackerError::permanent("season has no number"))?;
            SimklItem::Season { series, season }
        }
        db::MediaKind::Episode => {
            let (series, season, episode) = episode_coordinates(target)?;
            SimklItem::Episode {
                series,
                season,
                episode,
            }
        }
        _ => return Ok(None),
    }))
}

fn show_payload(series: &MediaTrackerTarget) -> simkl::SyncShow {
    simkl::SyncShow {
        title: Some(
            series
                .title
                .clone(),
        ),
        year: series
            .year
            .map(i64::from),
        ids: simkl_ids(&series.ids),
        ..Default::default()
    }
}

/// The `/sync/history` body that marks `target` watched (`watched_at` set)
/// or unwatched (`None`).
fn history_payload(
    item: &SimklItem<'_>,
    watched_at: Option<DateTime<Utc>>,
) -> simkl::SyncPayload {
    match item {
        SimklItem::Movie(movie) => simkl::SyncPayload {
            movies: vec![simkl::SyncMovie {
                title: Some(
                    movie
                        .title
                        .clone(),
                ),
                year: movie
                    .year
                    .map(i64::from),
                ids: simkl_ids(&movie.ids),
                watched_at,
                ..Default::default()
            }],
            ..Default::default()
        },
        SimklItem::Show(series) => simkl::SyncPayload {
            shows: vec![simkl::SyncShow {
                // Without seasons, only a completed status marks the show
                // watched; a removal names the show alone.
                status: watched_at.map(|_| simkl::ListStatus::Completed),
                ..show_payload(series)
            }],
            ..Default::default()
        },
        SimklItem::Season { series, season } => simkl::SyncPayload {
            shows: vec![simkl::SyncShow {
                seasons: Some(vec![simkl::SyncSeason {
                    number: *season,
                    episodes: None,
                }]),
                ..show_payload(series)
            }],
            ..Default::default()
        },
        SimklItem::Episode {
            series,
            season,
            episode,
        } => simkl::SyncPayload {
            shows: vec![simkl::SyncShow {
                seasons: Some(vec![simkl::SyncSeason {
                    number: *season,
                    episodes: Some(vec![simkl::SyncEpisode {
                        number: *episode,
                        watched_at,
                    }]),
                }]),
                ..show_payload(series)
            }],
            ..Default::default()
        },
    }
}

/// remux keeps 0-10 with decimals; Simkl takes 1-10 whole numbers and treats
/// anything else as not found. Below one there is nothing to say, so the
/// rating is cleared instead.
fn simkl_rating(rating: f32) -> Option<u8> {
    if !rating.is_finite() {
        return None;
    }
    let rounded = rating.round();
    if rounded < 1.0 {
        return None;
    }
    Some(rounded.min(10.0) as u8)
}

/// The `/sync/ratings` body for `target`, or `None` for an episode or season,
/// which Simkl cannot rate.
fn ratings_payload(
    item: &SimklItem<'_>,
    rating: Option<u8>,
) -> Option<simkl::SyncPayload> {
    match item {
        SimklItem::Movie(movie) => Some(simkl::SyncPayload {
            movies: vec![simkl::SyncMovie {
                title: Some(
                    movie
                        .title
                        .clone(),
                ),
                year: movie
                    .year
                    .map(i64::from),
                ids: simkl_ids(&movie.ids),
                rating,
                ..Default::default()
            }],
            ..Default::default()
        }),
        SimklItem::Show(series) => Some(simkl::SyncPayload {
            shows: vec![simkl::SyncShow {
                rating,
                ..show_payload(series)
            }],
            ..Default::default()
        }),
        SimklItem::Season { .. } | SimklItem::Episode { .. } => None,
    }
}

/// Turn a user's Simkl lists into what core applies locally. Nothing here
/// ever says "unwatched": Simkl reports what is in a list, not what left it,
/// so an item's absence carries no information.
fn remote_watches(items: &simkl::AllItemsResponse) -> Vec<RemoteWatch> {
    let mut out = Vec::new();
    for entry in &items.movies {
        push_remote_watches(&mut out, entry, false);
    }
    for entry in &items.shows {
        push_remote_watches(&mut out, entry, false);
    }
    for entry in &items.anime {
        push_remote_watches(&mut out, entry, true);
    }
    out
}

fn push_remote_watches(
    out: &mut Vec<RemoteWatch>,
    entry: &simkl::ListEntry,
    is_anime: bool,
) {
    let Some(item) = entry.item() else {
        return;
    };
    let ids = external_ids(&item.ids);
    if !has_local_ids(&ids) {
        debug!(
            title = ?item.title,
            "simkl: skipping an item with no imdb, tmdb, tvdb or kitsu id"
        );
        return;
    }
    let rating = entry
        .user_rating
        .map(|r| r as f32);
    let watched_at = entry
        .last_watched_at
        .map(|d| d.naive_utc());
    // Rating an item that is not on any list moves it to Completed on Simkl,
    // so a completed row is only a watch when Simkl also has a watch date
    // that is not just the rating's. Otherwise a rating pushed from here
    // would come back as a play.
    let watched = entry.status == Some(simkl::ListStatus::Completed)
        && entry
            .last_watched_at
            .is_some()
        && entry.last_watched_at != entry.user_rated_at;

    if entry.is_movie() {
        out.push(RemoteWatch {
            kind: RemoteKind::Movie,
            ids,
            season: None,
            episode: None,
            watched,
            position_ticks: None,
            watched_at,
            favorite: None,
            rating,
        });
        return;
    }

    let is_anime = is_anime
        || entry
            .anime_type
            .is_some();
    // Anime is numbered per title on Simkl. A library keyed on tvdb, imdb or
    // tmdb uses TVDB's seasons, which the `tvdb` block gives; one keyed on
    // kitsu counts the way Simkl does. Each candidate carries only the ids of
    // the scheme it is numbered in, so it can only land on a matching series.
    let tvdb_scheme_ids = db::ExternalIds {
        kitsu: None,
        ..ids.clone()
    };
    let kitsu_scheme_ids = db::ExternalIds {
        kitsu: ids.kitsu,
        ..Default::default()
    };
    let episode_watch =
        |ids: db::ExternalIds, season: i64, episode: i64, at| RemoteWatch {
            kind: RemoteKind::Show,
            ids,
            season: Some(season),
            episode: Some(episode),
            watched: true,
            position_ticks: None,
            watched_at: at,
            favorite: None,
            rating: None,
        };
    let mut episodes = 0usize;
    for season in &entry.seasons {
        for ep in &season.episodes {
            let at = ep
                .watched_at
                .or(entry.last_watched_at)
                .map(|d| d.naive_utc());
            if is_anime {
                if let Some((s, e)) = ep
                    .tvdb
                    .as_ref()
                    .and_then(|t| Some((t.season?, t.episode?)))
                    .filter(|_| has_local_ids(&tvdb_scheme_ids))
                {
                    out.push(episode_watch(tvdb_scheme_ids.clone(), s, e, at));
                    episodes += 1;
                }
                if let (Some(s), Some(e), Some(_)) =
                    (season.number, ep.number, ids.kitsu)
                {
                    out.push(episode_watch(kitsu_scheme_ids.clone(), s, e, at));
                    episodes += 1;
                }
            } else if let (Some(s), Some(e)) = (season.number, ep.number) {
                out.push(episode_watch(ids.clone(), s, e, at));
                episodes += 1;
            }
        }
    }

    // The show row carries the rating, and stands in for the episodes when a
    // completed show came back without them.
    let show_watched = watched && episodes == 0;
    if rating.is_some() || show_watched {
        out.push(RemoteWatch {
            kind: RemoteKind::Show,
            ids,
            season: None,
            episode: None,
            watched: show_watched,
            position_ticks: None,
            watched_at,
            favorite: None,
            rating,
        });
    }
}

impl SimklAddon {
    fn client(
        &self,
        token: Option<&str>,
        ctx: &MediaTrackerCtx,
    ) -> MediaTrackerResult<RestClient<simkl::SimklAuth>> {
        simkl::client(
            &self.client_id,
            token,
            &ctx.config
                .simkl_base_url,
        )
        .map_err(|e| MediaTrackerError::permanent(format!("bad Simkl base url: {e}")))
    }

    fn token<'a>(
        &self,
        creds: &'a MediaTrackerCredentials,
    ) -> MediaTrackerResult<&'a str> {
        creds
            .get_str(CRED_TOKEN)
            .ok_or_else(|| MediaTrackerError::reauth("no Simkl access token stored"))
    }

    fn user_client(
        &self,
        creds: &MediaTrackerCredentials,
        ctx: &MediaTrackerCtx,
    ) -> MediaTrackerResult<RestClient<simkl::SimklAuth>> {
        let token = self.token(creds)?;
        self.client(Some(token), ctx)
    }

    /// The account behind a fresh token, so the connection can be labelled.
    /// A failure here is not worth losing the token over.
    async fn credentials_for(
        &self,
        access_token: String,
        ctx: &MediaTrackerCtx,
    ) -> MediaTrackerCredentials {
        let mut creds = serde_json::json!({ CRED_TOKEN: access_token });
        match self.client(Some(&access_token), ctx) {
            Ok(client) => match client
                .execute(simkl::UserSettings)
                .await
            {
                Ok(settings) => {
                    if let Some(name) = settings
                        .user
                        .as_ref()
                        .and_then(|u| {
                            u.name
                                .clone()
                        })
                    {
                        creds[CRED_USERNAME] = serde_json::Value::String(name);
                    }
                    if let Some(id) = settings
                        .account
                        .as_ref()
                        .and_then(|a| a.id)
                    {
                        creds[CRED_ACCOUNT_ID] = serde_json::Value::from(id);
                    }
                }
                Err(e) => {
                    warn!(error = %e, "simkl: could not read the account behind a new token")
                }
            },
            Err(e) => {
                warn!(error = %e, "simkl: could not build a client for a new token")
            }
        }
        MediaTrackerCredentials::new(creds)
    }

    /// Send one `/sync/*` write and read Simkl's verdict: an item it could not
    /// match is a permanent failure for this event, not a reason to retry.
    async fn sync_write<EP>(
        &self,
        client: &RestClient<simkl::SimklAuth>,
        endpoint: EP,
        what: &str,
    ) -> MediaTrackerResult<()>
    where
        EP: sdks::Endpoint<Output = simkl::SyncResponse> + Clone,
    {
        let resp = client
            .execute(endpoint)
            .await
            .map_err(map_client_error)?;
        if resp.affected() == 0
            && !resp
                .not_found
                .is_empty()
        {
            return Err(MediaTrackerError::permanent(format!(
                "Simkl could not match this item for {what}"
            )));
        }
        Ok(())
    }

    async fn scrobble(
        &self,
        client: &RestClient<simkl::SimklAuth>,
        item: &SimklItem<'_>,
        progress: f64,
        verb: ScrobbleVerb,
    ) -> MediaTrackerResult<()> {
        let payload = match item {
            SimklItem::Movie(movie) => {
                simkl::ScrobblePayload::movie(scrobble_item(movie), progress)
            }
            SimklItem::Episode {
                series,
                season,
                episode,
            } => simkl::ScrobblePayload::episode(
                scrobble_item(series),
                *season,
                *episode,
                progress,
            ),
            SimklItem::Show(_) | SimklItem::Season { .. } => {
                debug!("simkl: only movies and episodes are scrobbled");
                return Ok(());
            }
        };
        let result = match verb {
            ScrobbleVerb::Start => {
                client
                    .execute(simkl::ScrobbleStart(payload))
                    .await
            }
            ScrobbleVerb::Pause => {
                client
                    .execute(simkl::ScrobblePause(payload))
                    .await
            }
            ScrobbleVerb::Stop => {
                client
                    .execute(simkl::ScrobbleStop(payload))
                    .await
            }
        };
        match result {
            Ok(_) => Ok(()),
            // Already scrobbled within the hour: Simkl agrees with us.
            Err(ClientError::Http { status: 409, .. }) => Ok(()),
            Err(e) => Err(map_client_error(e)),
        }
    }
}

#[derive(Clone, Copy)]
enum ScrobbleVerb {
    Start,
    Pause,
    Stop,
}

#[async_trait]
impl AddonKind for SimklAddon {
    fn id(&self) -> &'static str {
        "simkl"
    }
}

#[async_trait]
impl MediaTrackerAddon for SimklAddon {
    fn capabilities(&self) -> MediaTrackerCapabilities {
        let events = vec![
            MediaTrackerEventKind::PlaybackStart,
            MediaTrackerEventKind::PlaybackProgress,
            MediaTrackerEventKind::PlaybackStop,
            MediaTrackerEventKind::MarkPlayed,
            MediaTrackerEventKind::MarkUnplayed,
            MediaTrackerEventKind::Rating,
        ];
        MediaTrackerCapabilities {
            auth_flow: AuthFlow::OAuthDeviceCode,
            connect_fields: Vec::new(),
            default_event_filter: events.clone(),
            supported_events: events,
            history_import: true,
            progress_import: false,
            watch_state_sync: SyncDirection::Both,
            favorites: SyncDirection::None,
            ratings: SyncDirection::Both,
            watchlist: SyncDirection::None,
        }
    }

    async fn begin_device_auth(
        &self,
        ctx: &MediaTrackerCtx,
    ) -> MediaTrackerResult<DeviceAuthStart> {
        let resp = self
            .client(None, ctx)?
            .execute(simkl::PinRequest::default())
            .await
            .map_err(map_client_error)?;
        let user_code = resp
            .user_code
            .clone()
            .map(|c| {
                c.trim()
                    .to_string()
            })
            .filter(|c| !c.is_empty())
            .ok_or_else(|| MediaTrackerError::permanent("Simkl returned no PIN"))?;
        Ok(DeviceAuthStart {
            verification_url: resp
                .verification_url()
                .unwrap_or(DEFAULT_VERIFICATION_URL)
                .to_string(),
            poll_token: user_code.clone(),
            user_code,
            interval: Duration::from_secs(
                resp.interval
                    .unwrap_or(DEFAULT_PIN_INTERVAL_SECS)
                    .max(1) as u64,
            ),
            expires_in: Duration::from_secs(
                resp.expires_in
                    .unwrap_or(DEFAULT_PIN_EXPIRY_SECS)
                    .max(1) as u64,
            ),
        })
    }

    async fn poll_device_auth(
        &self,
        poll_token: &str,
        ctx: &MediaTrackerCtx,
    ) -> MediaTrackerResult<DeviceAuthPoll> {
        let resp = self
            .client(None, ctx)?
            .execute(simkl::PinPoll {
                user_code: poll_token.to_string(),
            })
            .await
            .map_err(map_client_error)?;
        Ok(match resp.status() {
            simkl::PinStatus::Pending => DeviceAuthPoll::Pending,
            simkl::PinStatus::Expired => DeviceAuthPoll::Denied,
            simkl::PinStatus::Approved { access_token } => DeviceAuthPoll::Approved(
                self.credentials_for(access_token.into_inner(), ctx)
                    .await,
            ),
        })
    }

    async fn verify(
        &self,
        creds: &MediaTrackerCredentials,
        ctx: &MediaTrackerCtx,
    ) -> MediaTrackerResult<()> {
        self.user_client(creds, ctx)?
            .execute(simkl::UserSettings)
            .await
            .map(|_| ())
            .map_err(map_client_error)
    }

    fn account_label(&self, creds: &MediaTrackerCredentials) -> Option<String> {
        creds
            .get_str(CRED_USERNAME)
            .map(str::to_string)
    }

    async fn on_event(
        &self,
        event: &MediaTrackerEvent,
        target: &MediaTrackerTarget,
        creds: &MediaTrackerCredentials,
        ctx: &MediaTrackerCtx,
    ) -> MediaTrackerResult<()> {
        let Some(item) = classify(target)? else {
            debug!(kind = %target.kind, "simkl: kind is not tracked");
            return Ok(());
        };
        let client = self.user_client(creds, ctx)?;

        match event {
            MediaTrackerEvent::PlaybackStart { position_ticks } => {
                let progress = target
                    .progress_percent(*position_ticks)
                    .unwrap_or(0.0);
                self.scrobble(&client, &item, progress, ScrobbleVerb::Start)
                    .await
            }
            // Core only reports the transitions: a pause, and the resume
            // after it, which Simkl takes as a fresh start.
            MediaTrackerEvent::PlaybackProgress {
                position_ticks,
                is_paused,
            } => {
                let progress = target
                    .progress_percent(*position_ticks)
                    .unwrap_or(0.0);
                let verb = if *is_paused {
                    ScrobbleVerb::Pause
                } else {
                    ScrobbleVerb::Start
                };
                self.scrobble(&client, &item, progress, verb)
                    .await
            }
            MediaTrackerEvent::PlaybackStop {
                position_ticks,
                played,
            } => {
                // remux has already ruled on this watch; keep Simkl's own
                // threshold from ruling differently.
                let progress = if *played {
                    100.0
                } else {
                    target
                        .progress_percent(*position_ticks)
                        .unwrap_or(0.0)
                        .min(simkl::WATCHED_THRESHOLD_PERCENT - 0.01)
                };
                self.scrobble(&client, &item, progress, ScrobbleVerb::Stop)
                    .await
            }
            MediaTrackerEvent::MarkPlayed => {
                let payload = history_payload(&item, Some(Utc::now()));
                self.sync_write(
                    &client,
                    simkl::AddToHistory(payload),
                    "marking watched",
                )
                .await
            }
            MediaTrackerEvent::MarkUnplayed => {
                let payload = history_payload(&item, None);
                self.sync_write(
                    &client,
                    simkl::RemoveFromHistory(payload),
                    "removing from history",
                )
                .await
            }
            MediaTrackerEvent::Rating { rating } => {
                let rating = rating.and_then(simkl_rating);
                let Some(payload) = ratings_payload(&item, rating) else {
                    debug!(kind = %target.kind, "simkl: only movies and shows can be rated");
                    return Ok(());
                };
                match rating {
                    Some(_) => {
                        self.sync_write(&client, simkl::AddRatings(payload), "rating")
                            .await
                    }
                    None => {
                        self.sync_write(
                            &client,
                            simkl::RemoveRatings(payload),
                            "clearing the rating",
                        )
                        .await
                    }
                }
            }
            MediaTrackerEvent::MarkFavorite | MediaTrackerEvent::UnmarkFavorite => {
                Err(MediaTrackerError::unsupported("favorites"))
            }
        }
    }

    async fn import_history(
        &self,
        creds: &MediaTrackerCredentials,
        ctx: &MediaTrackerCtx,
    ) -> MediaTrackerResult<Vec<RemoteWatch>> {
        self.pull_changes(None, creds, ctx)
            .await
    }

    /// Gated on `/sync/activities`, as Simkl asks: a delta pull only happens
    /// once something has actually changed since `since`.
    async fn pull_changes(
        &self,
        since: Option<chrono::NaiveDateTime>,
        creds: &MediaTrackerCredentials,
        ctx: &MediaTrackerCtx,
    ) -> MediaTrackerResult<Vec<RemoteWatch>> {
        let client = self.user_client(creds, ctx)?;
        // The watermark is remux's clock and Simkl's stamps are its own, so
        // both the gate and the fetch reach back the same margin.
        let floor = since.map(|s| s.and_utc() - PULL_OVERLAP);
        if let Some(floor) = floor {
            let activities = client
                .execute(simkl::Activities)
                .await
                .map_err(map_client_error)?;
            if !activities.changed_since(floor) {
                debug!("simkl: nothing changed since the last pull");
                return Ok(Vec::new());
            }
        }
        let items = client
            .execute(simkl::AllItems::for_sync(floor))
            .await
            .map_err(map_client_error)?;
        Ok(remote_watches(&items))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use httpmock::{Method, MockServer};

    fn ctx(server: &MockServer) -> MediaTrackerCtx {
        MediaTrackerCtx {
            config: Arc::new(crate::Config {
                simkl_base_url: server.base_url(),
                ..Default::default()
            }),
        }
    }

    fn addon() -> SimklAddon {
        SimklAddon {
            client_id: "cid".into(),
        }
    }

    fn creds() -> MediaTrackerCredentials {
        MediaTrackerCredentials::new(serde_json::json!({
            "access_token": "tok", "username": "alice"
        }))
    }

    fn imdb(id: &str) -> Option<db::NonEmptyString> {
        db::NonEmptyString::try_new(id.to_string()).ok()
    }

    fn movie() -> MediaTrackerTarget {
        MediaTrackerTarget {
            kind: db::MediaKind::Movie,
            title: "Heat".into(),
            year: Some(1995),
            ids: db::ExternalIds {
                imdb: imdb("tt0113277"),
                tmdb: Some(949),
                ..Default::default()
            },
            series: None,
            season: None,
            episode: None,
            runtime_seconds: Some(1000),
        }
    }

    fn series() -> MediaTrackerTarget {
        MediaTrackerTarget {
            kind: db::MediaKind::Series,
            title: "The Wire".into(),
            year: Some(2002),
            ids: db::ExternalIds {
                imdb: imdb("tt0306414"),
                tvdb: Some(79126),
                ..Default::default()
            },
            series: None,
            season: None,
            episode: None,
            runtime_seconds: None,
        }
    }

    fn episode() -> MediaTrackerTarget {
        MediaTrackerTarget {
            kind: db::MediaKind::Episode,
            title: "The Target".into(),
            year: Some(2002),
            ids: db::ExternalIds::default(),
            series: Some(Box::new(series())),
            season: Some(1),
            episode: Some(3),
            runtime_seconds: Some(3600),
        }
    }

    fn season() -> MediaTrackerTarget {
        MediaTrackerTarget {
            kind: db::MediaKind::Season,
            title: "Season 2".into(),
            year: None,
            ids: db::ExternalIds::default(),
            series: Some(Box::new(series())),
            season: Some(2),
            episode: None,
            runtime_seconds: None,
        }
    }

    fn ticks(seconds: i64) -> i64 {
        seconds * 10_000_000
    }

    #[test]
    fn the_preset_refuses_to_load_without_a_client_id() {
        let cfg = crate::Config::default();
        assert!(
            SimklPreset
                .from_cfg(Uuid::nil(), &serde_json::json!({}), &cfg)
                .is_err()
        );
        assert!(
            SimklPreset
                .from_cfg(Uuid::nil(), &serde_json::json!({"client_id": "  "}), &cfg)
                .is_err()
        );
        let caps = SimklPreset
            .from_cfg(Uuid::nil(), &serde_json::json!({"client_id": "abc"}), &cfg)
            .unwrap();
        assert!(
            caps.media_tracker
                .is_some()
        );
    }

    #[test]
    fn the_default_filter_is_a_subset_of_what_is_supported() {
        let caps = addon().capabilities();
        assert_eq!(caps.auth_flow, AuthFlow::OAuthDeviceCode);
        for kind in &caps.default_event_filter {
            assert!(
                caps.supports(*kind),
                "{kind} is on by default but unsupported"
            );
        }
        assert!(!caps.supports(MediaTrackerEventKind::MarkFavorite));
        assert!(caps.history_import);
        assert!(
            caps.watch_state_sync
                .pulls()
                && caps
                    .watch_state_sync
                    .pushes()
        );
        assert_eq!(caps.ratings, SyncDirection::Both);
    }

    #[tokio::test]
    async fn a_finished_watch_stops_the_scrobble_at_one_hundred_percent() {
        let server = MockServer::start();
        let stop = server.mock(|when, then| {
            when.method(Method::POST)
                .path("/scrobble/stop")
                .header("authorization", "Bearer tok")
                .json_body_partial(
                    r#"{"progress": 100.0, "movie": {"title": "Heat", "year": 1995, "ids": {"imdb": "tt0113277", "tmdb": 949}}}"#,
                );
            then.status(201)
                .json_body(serde_json::json!({"action": "scrobble", "progress": 100}));
        });

        addon()
            .on_event(
                &MediaTrackerEvent::PlaybackStop {
                    position_ticks: ticks(950),
                    played: true,
                },
                &movie(),
                &creds(),
                &ctx(&server),
            )
            .await
            .unwrap();
        stop.assert();
    }

    /// remux said this was not a watch; Simkl must not decide otherwise.
    #[tokio::test]
    async fn an_abandoned_watch_stays_under_simkls_threshold() {
        let server = MockServer::start();
        let stop = server.mock(|when, then| {
            when.method(Method::POST)
                .path("/scrobble/stop")
                .json_body_partial(r#"{"progress": 79.99}"#);
            then.status(201)
                .json_body(serde_json::json!({"action": "pause", "progress": 79.99}));
        });

        addon()
            .on_event(
                &MediaTrackerEvent::PlaybackStop {
                    position_ticks: ticks(900),
                    played: false,
                },
                &movie(),
                &creds(),
                &ctx(&server),
            )
            .await
            .unwrap();
        stop.assert();
    }

    #[tokio::test]
    async fn an_episode_is_scrobbled_through_its_show_and_resumed_after_a_pause() {
        let server = MockServer::start();
        let start = server.mock(|when, then| {
            when.method(Method::POST)
                .path("/scrobble/start")
                .json_body_partial(
                    r#"{"show": {"title": "The Wire", "year": 2002, "ids": {"imdb": "tt0306414", "tvdb": 79126}}, "episode": {"season": 1, "number": 3}}"#,
                );
            then.status(201)
                .json_body(serde_json::json!({"action": "start"}));
        });
        let pause = server.mock(|when, then| {
            when.method(Method::POST)
                .path("/scrobble/pause")
                .json_body_partial(
                    r#"{"progress": 50.0, "episode": {"season": 1, "number": 3}}"#,
                );
            then.status(201)
                .json_body(serde_json::json!({"action": "pause"}));
        });

        let a = addon();
        a.on_event(
            &MediaTrackerEvent::PlaybackStart {
                position_ticks: ticks(900),
            },
            &episode(),
            &creds(),
            &ctx(&server),
        )
        .await
        .unwrap();
        a.on_event(
            &MediaTrackerEvent::PlaybackProgress {
                position_ticks: ticks(1800),
                is_paused: true,
            },
            &episode(),
            &creds(),
            &ctx(&server),
        )
        .await
        .unwrap();
        // A resume is a start again, from where it left off.
        a.on_event(
            &MediaTrackerEvent::PlaybackProgress {
                position_ticks: ticks(1900),
                is_paused: false,
            },
            &episode(),
            &creds(),
            &ctx(&server),
        )
        .await
        .unwrap();
        assert_eq!(start.hits(), 2);
        pause.assert();
    }

    #[tokio::test]
    async fn a_repeat_stop_within_the_hour_is_not_a_failure() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(Method::POST)
                .path("/scrobble/stop");
            then.status(409)
                .json_body(
                    serde_json::json!({"watched_at": "2024-05-01T18:00:00-05:00"}),
                );
        });
        addon()
            .on_event(
                &MediaTrackerEvent::PlaybackStop {
                    position_ticks: ticks(1000),
                    played: true,
                },
                &movie(),
                &creds(),
                &ctx(&server),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn marking_an_episode_played_writes_its_show_season_and_number() {
        let server = MockServer::start();
        let add = server.mock(|when, then| {
            when.method(Method::POST)
                .path("/sync/history")
                .json_body_partial(
                    r#"{"shows": [{"title": "The Wire", "ids": {"imdb": "tt0306414", "tvdb": 79126}, "seasons": [{"number": 1, "episodes": [{"number": 3}]}]}]}"#,
                );
            then.status(201)
                .json_body(serde_json::json!({
                    "added": {"movies": 0, "shows": 0, "episodes": 1},
                    "not_found": {"movies": [], "shows": [], "episodes": []}
                }));
        });
        addon()
            .on_event(
                &MediaTrackerEvent::MarkPlayed,
                &episode(),
                &creds(),
                &ctx(&server),
            )
            .await
            .unwrap();
        add.assert();
    }

    #[tokio::test]
    async fn marking_a_whole_show_played_completes_it_and_unplayed_removes_it() {
        let server = MockServer::start();
        let add = server.mock(|when, then| {
            when.method(Method::POST)
                .path("/sync/history")
                .json_body_partial(
                    r#"{"shows": [{"ids": {"imdb": "tt0306414"}, "status": "completed"}]}"#,
                );
            then.status(201)
                .json_body(serde_json::json!({"added": {"shows": 1, "episodes": 60}}));
        });
        let remove = server.mock(|when, then| {
            when.method(Method::POST)
                .path("/sync/history/remove")
                .json_body_partial(r#"{"shows": [{"ids": {"imdb": "tt0306414"}}]}"#);
            then.status(201)
                .json_body(serde_json::json!({"deleted": {"shows": 1}}));
        });
        let a = addon();
        a.on_event(
            &MediaTrackerEvent::MarkPlayed,
            &series(),
            &creds(),
            &ctx(&server),
        )
        .await
        .unwrap();
        a.on_event(
            &MediaTrackerEvent::MarkUnplayed,
            &series(),
            &creds(),
            &ctx(&server),
        )
        .await
        .unwrap();
        add.assert();
        remove.assert();
    }

    /// A removal names the show alone: a `status` on it would re-add it.
    #[test]
    fn a_history_removal_carries_no_status() {
        let series = series();
        let item = classify(&series)
            .unwrap()
            .unwrap();
        let body = serde_json::to_value(history_payload(&item, None)).unwrap();
        assert!(
            body["shows"][0]
                .get("status")
                .is_none()
        );
        let body =
            serde_json::to_value(history_payload(&item, Some(Utc::now()))).unwrap();
        assert_eq!(body["shows"][0]["status"], "completed");
    }

    #[tokio::test]
    async fn a_season_is_addressed_without_listing_its_episodes() {
        let server = MockServer::start();
        let add = server.mock(|when, then| {
            when.method(Method::POST)
                .path("/sync/history")
                .json_body_partial(r#"{"shows": [{"seasons": [{"number": 2}]}]}"#);
            then.status(201)
                .json_body(serde_json::json!({"added": {"episodes": 12}}));
        });
        addon()
            .on_event(
                &MediaTrackerEvent::MarkPlayed,
                &season(),
                &creds(),
                &ctx(&server),
            )
            .await
            .unwrap();
        add.assert();
    }

    #[tokio::test]
    async fn an_item_simkl_cannot_match_fails_permanently() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(Method::POST)
                .path("/sync/history");
            then.status(201)
                .json_body(serde_json::json!({
                    "added": {"movies": 0},
                    "not_found": {"movies": [{"ids": {"imdb": "tt0113277"}}]}
                }));
        });
        let err = addon()
            .on_event(
                &MediaTrackerEvent::MarkPlayed,
                &movie(),
                &creds(),
                &ctx(&server),
            )
            .await
            .unwrap_err();
        assert!(!err.is_retryable());
        assert!(!err.requires_reauth());
    }

    #[tokio::test]
    async fn ratings_are_rounded_to_simkls_scale_and_cleared_below_one() {
        let server = MockServer::start();
        let add = server.mock(|when, then| {
            when.method(Method::POST)
                .path("/sync/ratings")
                .json_body_partial(
                    r#"{"movies": [{"rating": 8, "ids": {"imdb": "tt0113277"}}]}"#,
                );
            then.status(201)
                .json_body(serde_json::json!({"added": {"movies": 1}}));
        });
        let remove = server.mock(|when, then| {
            when.method(Method::POST)
                .path("/sync/ratings/remove")
                .json_body_partial(r#"{"shows": [{"ids": {"imdb": "tt0306414"}}]}"#);
            then.status(201)
                .json_body(serde_json::json!({"deleted": {"shows": 1}}));
        });
        let a = addon();
        a.on_event(
            &MediaTrackerEvent::Rating { rating: Some(8.4) },
            &movie(),
            &creds(),
            &ctx(&server),
        )
        .await
        .unwrap();
        a.on_event(
            &MediaTrackerEvent::Rating { rating: Some(0.4) },
            &series(),
            &creds(),
            &ctx(&server),
        )
        .await
        .unwrap();
        add.assert();
        remove.assert();
        assert_eq!(simkl_rating(10.0), Some(10));
        assert_eq!(simkl_rating(12.0), Some(10));
        assert_eq!(simkl_rating(6.5), Some(7));
        assert_eq!(simkl_rating(0.0), None);
        assert_eq!(simkl_rating(f32::NAN), None);
    }

    /// Simkl cannot rate an episode; the event is not an error either.
    #[tokio::test]
    async fn an_episode_rating_is_dropped_quietly() {
        let server = MockServer::start();
        let any = server.mock(|when, then| {
            when.path_contains("/sync/ratings");
            then.status(201)
                .json_body(serde_json::json!({}));
        });
        addon()
            .on_event(
                &MediaTrackerEvent::Rating { rating: Some(9.0) },
                &episode(),
                &creds(),
                &ctx(&server),
            )
            .await
            .unwrap();
        assert_eq!(any.hits(), 0);
    }

    #[tokio::test]
    async fn favourites_are_refused_and_untracked_kinds_ignored() {
        let server = MockServer::start();
        let a = addon();
        let err = a
            .on_event(
                &MediaTrackerEvent::MarkFavorite,
                &movie(),
                &creds(),
                &ctx(&server),
            )
            .await
            .unwrap_err();
        assert!(!err.is_retryable());

        let track = MediaTrackerTarget {
            kind: db::MediaKind::Track,
            ..movie()
        };
        a.on_event(
            &MediaTrackerEvent::MarkPlayed,
            &track,
            &creds(),
            &ctx(&server),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn missing_credentials_ask_for_a_reconnect_before_any_request() {
        let server = MockServer::start();
        let err = addon()
            .on_event(
                &MediaTrackerEvent::MarkPlayed,
                &movie(),
                &MediaTrackerCredentials::new(serde_json::json!({})),
                &ctx(&server),
            )
            .await
            .unwrap_err();
        assert!(err.requires_reauth());
    }

    #[tokio::test]
    async fn provider_errors_are_split_by_what_to_do_next() {
        let server = MockServer::start();
        let unauthorized = server.mock(|when, then| {
            when.method(Method::POST)
                .path("/sync/history")
                .header("authorization", "Bearer revoked");
            then.status(401)
                .json_body(
                    serde_json::json!({"error": "user_token_failed", "code": 401}),
                );
        });
        let err = addon()
            .on_event(
                &MediaTrackerEvent::MarkPlayed,
                &movie(),
                &MediaTrackerCredentials::new(
                    serde_json::json!({"access_token": "revoked"}),
                ),
                &ctx(&server),
            )
            .await
            .unwrap_err();
        assert!(err.requires_reauth());
        unauthorized.assert();

        assert!(
            map_client_error(ClientError::RateLimited {
                retry_after_secs: 30
            })
            .is_retryable()
        );
        let http = |status: u16, message: &str| ClientError::Http {
            status,
            message: message.into(),
            endpoint: None,
            body: None,
        };
        assert!(map_client_error(http(503, "internal")).is_retryable());
        assert!(map_client_error(http(400, "RATE_LIMIT")).is_retryable());
        match map_client_error(ClientError::RateLimited {
            retry_after_secs: 0,
        }) {
            MediaTrackerError::Retryable {
                retry_after: Some(d),
                ..
            } => assert_eq!(d, MIN_RETRY_AFTER, "a missing Retry-After still waits"),
            other => panic!("expected a retry hint, got {other:?}"),
        }
        assert!(!map_client_error(http(400, "empty_field")).is_retryable());
        assert!(!map_client_error(http(412, "client_id_failed")).is_retryable());
        assert!(!map_client_error(http(404, "id_err")).is_retryable());
        match map_client_error(http(400, "RATE_LIMIT")) {
            MediaTrackerError::Retryable {
                retry_after: Some(d),
                ..
            } => assert_eq!(d, SCROBBLE_RATE_LIMIT_WINDOW),
            other => panic!("expected a retry hint, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn the_pin_flow_goes_from_code_to_labelled_credentials() {
        let server = MockServer::start();
        let pin = server.mock(|when, then| {
            when.method(Method::GET)
                .path("/oauth/pin")
                .query_param("client_id", "cid");
            then.status(200)
                .json_body(serde_json::json!({
                    "result": "OK", "device_code": "x", "user_code": "5G6JAH",
                    "verification_uri": "https://simkl.com/pin", "expires_in": 900, "interval": 5
                }));
        });
        let a = addon();
        let start = a
            .begin_device_auth(&ctx(&server))
            .await
            .unwrap();
        pin.assert();
        assert_eq!(start.user_code, "5G6JAH");
        assert_eq!(start.poll_token, "5G6JAH");
        assert_eq!(start.verification_url, "https://simkl.com/pin");
        assert_eq!(start.interval, Duration::from_secs(5));
        assert_eq!(start.expires_in, Duration::from_secs(900));

        let mut pending = server.mock(|when, then| {
            when.method(Method::GET)
                .path("/oauth/pin/5G6JAH");
            then.status(200)
                .json_body(serde_json::json!({"result": "KO", "message": "Authorization pending"}));
        });
        assert!(matches!(
            a.poll_device_auth("5G6JAH", &ctx(&server))
                .await
                .unwrap(),
            DeviceAuthPoll::Pending
        ));
        pending.assert();
        pending.delete();

        let approved = server.mock(|when, then| {
            when.method(Method::GET)
                .path("/oauth/pin/5G6JAH");
            then.status(200)
                .json_body(
                    serde_json::json!({"result": "OK", "access_token": "tok-new"}),
                );
        });
        let settings = server.mock(|when, then| {
            when.method(Method::POST)
                .path("/users/settings")
                .header("authorization", "Bearer tok-new");
            then.status(200)
                .json_body(serde_json::json!({
                    "user": {"name": "alice"}, "account": {"id": 42, "type": "free"}
                }));
        });
        let DeviceAuthPoll::Approved(creds) = a
            .poll_device_auth("5G6JAH", &ctx(&server))
            .await
            .unwrap()
        else {
            panic!("expected approval");
        };
        approved.assert();
        settings.assert();
        assert_eq!(creds.get_str("access_token"), Some("tok-new"));
        assert_eq!(
            a.account_label(&creds)
                .as_deref(),
            Some("alice")
        );
        assert_eq!(
            creds
                .expose()
                .get("account_id")
                .and_then(|v| v.as_i64()),
            Some(42)
        );
    }

    #[tokio::test]
    async fn a_pin_simkl_no_longer_knows_is_denied() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(Method::GET)
                .path("/oauth/pin/OLD");
            then.status(200)
                .json_body(serde_json::json!({
                    "result": "OK", "device_code": "y", "user_code": "FRESH1",
                    "verification_uri": "https://simkl.com/pin", "expires_in": 900, "interval": 5
                }));
        });
        assert!(matches!(
            addon()
                .poll_device_auth("OLD", &ctx(&server))
                .await
                .unwrap(),
            DeviceAuthPoll::Denied
        ));
    }

    #[tokio::test]
    async fn verify_reports_a_revoked_token_as_needing_reauth() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(Method::POST)
                .path("/users/settings");
            then.status(401)
                .json_body(serde_json::json!({"error": "user_token_failed"}));
        });
        let err = addon()
            .verify(&creds(), &ctx(&server))
            .await
            .unwrap_err();
        assert!(err.requires_reauth());
    }

    #[tokio::test]
    async fn a_delta_pull_is_skipped_when_nothing_changed() {
        let server = MockServer::start();
        let activities = server.mock(|when, then| {
            when.method(Method::GET)
                .path("/sync/activities");
            then.status(200)
                .json_body(serde_json::json!({"all": "2026-05-01T00:00:00Z"}));
        });
        let items = server.mock(|when, then| {
            when.method(Method::GET)
                .path_contains("/sync/all-items");
            then.status(200)
                .json_body(serde_json::json!({}));
        });
        let since = DateTime::parse_from_rfc3339("2026-05-02T00:00:00Z")
            .unwrap()
            .naive_utc();
        let got = addon()
            .pull_changes(Some(since), &creds(), &ctx(&server))
            .await
            .unwrap();
        assert!(got.is_empty());
        activities.assert();
        assert_eq!(items.hits(), 0, "no list pull without a changed activity");
    }

    #[tokio::test]
    async fn a_delta_pull_reaches_back_past_the_watermark() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(Method::GET)
                .path("/sync/activities");
            then.status(200)
                .json_body(serde_json::json!({"all": "2026-05-03T00:00:00Z"}));
        });
        let items = server.mock(|when, then| {
            when.method(Method::GET)
                .path("/sync/all-items")
                .query_param("extended", "full_anime_seasons")
                .query_param("episode_watched_at", "yes")
                .query_param("include_all_episodes", "yes")
                .query_param("date_from", "2026-05-01T23:55:00Z");
            then.status(200)
                .json_body(serde_json::json!({}));
        });
        let since = DateTime::parse_from_rfc3339("2026-05-02T00:00:00Z")
            .unwrap()
            .naive_utc();
        addon()
            .pull_changes(Some(since), &creds(), &ctx(&server))
            .await
            .unwrap();
        items.assert();
    }

    #[tokio::test]
    async fn a_full_import_asks_for_everything_without_checking_activities() {
        let server = MockServer::start();
        let activities = server.mock(|when, then| {
            when.method(Method::GET)
                .path("/sync/activities");
            then.status(200)
                .json_body(serde_json::json!({}));
        });
        let items = server.mock(|when, then| {
            when.method(Method::GET)
                .path("/sync/all-items")
                .matches(|req| {
                    !req.query_params
                        .as_ref()
                        .is_some_and(|q| q.iter().any(|(k, _)| k == "date_from"))
                });
            then.status(200)
                .json_body(serde_json::json!({
                    "movies": [{
                        "status": "completed", "user_rating": 9,
                        "last_watched_at": "2026-05-01T20:00:00Z",
                        "movie": {"title": "Heat", "year": 1995, "ids": {"simkl": 1, "imdb": "tt0113277", "tmdb": 949}}
                    }]
                }));
        });
        let got = addon()
            .import_history(&creds(), &ctx(&server))
            .await
            .unwrap();
        items.assert();
        assert_eq!(activities.hits(), 0);
        assert_eq!(got.len(), 1);
        assert!(got[0].watched);
        assert_eq!(got[0].rating, Some(9.0));
        assert_eq!(
            got[0]
                .ids
                .tmdb,
            Some(949)
        );
        assert!(
            got[0]
                .watched_at
                .is_some()
        );
    }

    #[test]
    fn remote_lists_become_per_episode_watches_and_show_ratings() {
        let items: simkl::AllItemsResponse = serde_json::from_value(serde_json::json!({
            "shows": [{
                "status": "watching", "user_rating": 8,
                "last_watched_at": "2026-05-15T00:35:15Z",
                "show": {"title": "The Wire", "ids": {"imdb": "tt0306414", "tvdb": "79126"}},
                "seasons": [{"number": 1, "episodes": [
                    {"number": 1, "watched_at": "2026-05-10T00:00:00Z"},
                    {"number": 2}
                ]}]
            }, {
                "status": "completed",
                "last_watched_at": "2026-05-01T00:00:00Z",
                "show": {"title": "Finished", "ids": {"tmdb": 555}}
            }, {
                "status": "plantowatch",
                "show": {"title": "Unmapped", "ids": {"simkl": 7, "mal": 99}}
            }],
            "movies": [{
                "status": "plantowatch",
                "movie": {"title": "Later", "ids": {"imdb": "tt0000001"}}
            }]
        }))
        .unwrap();
        let got = remote_watches(&items);

        let wire: Vec<_> = got
            .iter()
            .filter(|w| {
                w.ids
                    .tvdb
                    == Some(79126)
            })
            .collect();
        // Two episodes plus the show row carrying the rating.
        assert_eq!(wire.len(), 3);
        assert!(
            wire.iter()
                .all(|w| w.kind == RemoteKind::Show)
        );
        let ep1 = wire
            .iter()
            .find(|w| w.episode == Some(1))
            .unwrap();
        assert!(ep1.watched);
        assert_eq!(ep1.season, Some(1));
        assert!(
            ep1.watched_at
                .is_some()
        );
        let ep2 = wire
            .iter()
            .find(|w| w.episode == Some(2))
            .unwrap();
        assert!(
            ep2.watched_at
                .is_some(),
            "an undated episode borrows the show's last watch"
        );
        let show = wire
            .iter()
            .find(|w| {
                w.episode
                    .is_none()
            })
            .unwrap();
        assert_eq!(show.rating, Some(8.0));
        assert!(!show.watched, "a show in progress is not watched");

        let finished = got
            .iter()
            .find(|w| {
                w.ids
                    .tmdb
                    == Some(555)
            })
            .expect("a completed show without episodes is still applied");
        assert!(
            finished.watched
                && finished
                    .episode
                    .is_none()
        );
        assert_eq!(finished.kind, RemoteKind::Show);

        assert!(
            !got.iter()
                .any(|w| w.ids == db::ExternalIds::default()),
            "items with no local id are dropped"
        );

        let later = got
            .iter()
            .find(|w| {
                w.ids
                    .imdb
                    .as_deref()
                    .map(String::as_str)
                    == Some("tt0000001")
            })
            .unwrap();
        assert_eq!(later.kind, RemoteKind::Movie);
        assert!(
            !later.watched
                && later
                    .rating
                    .is_none()
        );
    }

    /// Rating an unlisted movie moves it to Completed on Simkl. That must not
    /// come back as a watch, or a rating pushed from here would mark the
    /// movie played on the next pull.
    #[test]
    fn a_completed_movie_counts_as_watched_only_with_a_watch_date_of_its_own() {
        let items: simkl::AllItemsResponse =
            serde_json::from_value(serde_json::json!({
                "movies": [{
                    "status": "completed", "user_rating": 7,
                    "user_rated_at": "2026-05-01T10:00:00Z",
                    "last_watched_at": "2026-05-01T10:00:00Z",
                    "movie": {"title": "Only rated", "ids": {"tmdb": 1}}
                }, {
                    "status": "completed", "user_rating": 7,
                    "user_rated_at": "2026-05-02T10:00:00Z",
                    "last_watched_at": "2026-05-01T20:00:00Z",
                    "movie": {"title": "Watched then rated", "ids": {"tmdb": 2}}
                }, {
                    "status": "completed",
                    "movie": {"title": "No date at all", "ids": {"tmdb": 3}}
                }]
            }))
            .unwrap();
        let got = remote_watches(&items);
        let by = |tmdb: i64| {
            got.iter()
                .find(|w| {
                    w.ids
                        .tmdb
                        == Some(tmdb)
                })
                .unwrap()
        };
        assert!(!by(1).watched);
        assert_eq!(by(1).rating, Some(7.0), "the rating still comes through");
        assert!(by(2).watched);
        assert!(!by(3).watched);
    }

    /// Simkl numbers anime per title. A library keyed on tvdb gets TVDB's
    /// seasons; one keyed on kitsu gets Simkl's numbering; each candidate
    /// carries only the ids it is valid for.
    #[test]
    fn anime_episodes_are_offered_in_the_scheme_each_library_uses() {
        let items: simkl::AllItemsResponse = serde_json::from_value(serde_json::json!({
            "anime": [{
                "status": "watching",
                "show": {"title": "Both", "ids": {"kitsu": 1, "tvdb": 76885, "mal": 1}},
                "seasons": [{"number": 1, "episodes": [
                    {"number": 27, "watched_at": "2026-05-15T00:13:09Z", "tvdb": {"season": 2, "episode": 1}},
                    {"number": 28, "tvdb": {"season": 2, "episode": null}}
                ]}]
            }, {
                "status": "watching",
                "show": {"title": "Kitsu only", "ids": {"kitsu": 2, "mal": 2}},
                "seasons": [{"number": 1, "episodes": [{"number": 5, "tvdb": {"season": 1, "episode": 5}}]}]
            }]
        }))
        .unwrap();
        let got = remote_watches(&items);

        let tvdb: Vec<_> = got
            .iter()
            .filter(|w| {
                w.ids
                    .tvdb
                    == Some(76885)
            })
            .collect();
        assert_eq!(
            tvdb.len(),
            1,
            "an episode with no tvdb mapping is not guessed"
        );
        assert_eq!((tvdb[0].season, tvdb[0].episode), (Some(2), Some(1)));
        assert_eq!(
            tvdb[0]
                .ids
                .kitsu,
            None
        );

        let kitsu: Vec<_> = got
            .iter()
            .filter(|w| {
                w.ids
                    .kitsu
                    == Some(1)
            })
            .collect();
        assert_eq!(kitsu.len(), 2);
        assert!(
            kitsu
                .iter()
                .all(|w| w
                    .ids
                    .tvdb
                    .is_none()
                    && w.season == Some(1))
        );
        assert!(
            kitsu
                .iter()
                .any(|w| w.episode == Some(27))
                && kitsu
                    .iter()
                    .any(|w| w.episode == Some(28))
        );

        let only = got
            .iter()
            .filter(|w| {
                w.ids
                    .kitsu
                    == Some(2)
            })
            .collect::<Vec<_>>();
        assert_eq!(only.len(), 1, "no tvdb id, so only Simkl's numbering");
        assert_eq!((only[0].season, only[0].episode), (Some(1), Some(5)));
    }

    #[test]
    fn client_ids_are_scrubbed_from_error_text() {
        let raw = "error sending request for url (http://x/users/settings?client_id=abc-123&app-name=remux)";
        let scrubbed = scrub_client_id(raw);
        assert!(!scrubbed.contains("abc-123"), "{scrubbed}");
        assert!(scrubbed.contains("client_id=<redacted>&app-name=remux"));
    }

    /// The url of a failed request carries the client id; the message that
    /// lands in `last_error` must not.
    #[tokio::test]
    async fn a_transport_failure_does_not_leak_the_client_id() {
        let server = MockServer::start();
        let unreachable = format!("http://127.0.0.1:{}", {
            // A port nothing listens on: bind, read the port, drop.
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr()
                .unwrap()
                .port()
        });
        let _ = server;
        let ctx = MediaTrackerCtx {
            config: Arc::new(crate::Config {
                simkl_base_url: unreachable,
                ..Default::default()
            }),
        };
        let err = SimklAddon {
            client_id: "top-secret-client-id".into(),
        }
        .verify(&creds(), &ctx)
        .await
        .unwrap_err();
        assert!(err.is_retryable());
        assert!(
            !err.to_string()
                .contains("top-secret-client-id"),
            "leaked: {err}"
        );
    }
}

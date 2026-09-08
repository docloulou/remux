//! Simkl API client (<https://api.simkl.org>).
//!
//! Every request carries `client_id`, `app-name` and `app-version` as query
//! parameters plus a `User-Agent`, which the API requires. User-scoped calls
//! add a bearer token. Simkl publishes a limit of 10 GET/s and 1 POST/s per
//! client and user; callers are expected to gate list pulls on
//! [`Activities`] rather than poll [`AllItems`] blindly.

use crate::{Auth, Body, ClientError, Endpoint, RestClient};
use chrono::{DateTime, NaiveDateTime, Utc};
use http::Method;
use remux_utils::Secret;
use serde::{Deserialize, Deserializer, Serialize};
use serde_with::skip_serializing_none;

pub const DEFAULT_BASE_URL: &str = "https://api.simkl.com";
/// Simkl asks for a short lowercase identifier and a version on every call.
pub const APP_NAME: &str = "remux";
pub const APP_VERSION: &str = "1.0";
const USER_AGENT: &str = "remux/1.0";

/// Progress at or above which `/scrobble/stop` marks an item watched.
pub const WATCHED_THRESHOLD_PERCENT: f64 = 80.0;

#[derive(Clone)]
pub struct SimklAuth {
    pub client_id: String,
    /// `None` for the unauthenticated endpoints (PIN flow, search).
    pub access_token: Option<String>,
}

impl std::fmt::Debug for SimklAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SimklAuth")
            .field("client_id", &"<redacted>")
            .field(
                "access_token",
                &self
                    .access_token
                    .as_ref()
                    .map(|_| "<redacted>"),
            )
            .finish()
    }
}

impl Auth for SimklAuth {
    fn apply(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        let mut req = req
            .header("simkl-api-key", &self.client_id)
            .header("Accept", "application/json")
            .header("User-Agent", USER_AGENT)
            .query(&[
                (
                    "client_id",
                    self.client_id
                        .as_str(),
                ),
                ("app-name", APP_NAME),
                ("app-version", APP_VERSION),
            ]);
        if let Some(token) = &self.access_token {
            req = req.bearer_auth(token);
        }
        req
    }
}

/// Simkl error bodies are `{"error": "...", "code": 412, "message": "..."}`.
/// The docs say to branch on `error`, so it leads the message. 401 and 429
/// never reach here: `RestClient` turns them into `Unauthorized` and
/// `RateLimited` before consulting the mapper.
fn simkl_error_mapper(status: u16, endpoint: &str, body: &str) -> ClientError {
    #[derive(Deserialize)]
    struct ErrorBody {
        error: Option<String>,
        message: Option<String>,
    }
    let message = serde_json::from_str::<ErrorBody>(body)
        .ok()
        .and_then(|e| match (e.error, e.message) {
            (Some(code), Some(msg)) if !msg.is_empty() => {
                Some(format!("{code}: {msg}"))
            }
            (Some(code), _) => Some(code),
            (None, Some(msg)) if !msg.is_empty() => Some(msg),
            _ => None,
        })
        .unwrap_or_else(|| match status {
            412 => "client_id_failed".to_string(),
            _ => "http error".to_string(),
        });
    ClientError::Http {
        status,
        message,
        endpoint: Some(remux_utils::Secret::new(endpoint.to_string())),
        body: Some(remux_utils::Secret::new(body.to_string())),
    }
}

pub fn client(
    client_id: &str,
    access_token: Option<&str>,
    base_url: &str,
) -> Result<RestClient<SimklAuth>, url::ParseError> {
    Ok(RestClient::new(base_url)?
        .with_auth(SimklAuth {
            client_id: client_id.to_string(),
            access_token: access_token.map(str::to_string),
        })
        .with_error_mapper(simkl_error_mapper)
        .with_retry(crate::ExponentialBackoff::builder().build_with_max_retries(3)))
}

// ---------------------------------------------------------------------------
// Lenient field decoders. Simkl mixes `"tvdb": "153021"` with `"tmdb": 1399`
// in the same object, and a sync must not fail on one odd value.
// ---------------------------------------------------------------------------

fn flexible_i64<'de, D: Deserializer<'de>>(d: D) -> Result<Option<i64>, D::Error> {
    let v = Option::<serde_json::Value>::deserialize(d)?;
    Ok(match v {
        Some(serde_json::Value::Number(n)) => n
            .as_i64()
            .or_else(|| {
                n.as_f64()
                    .map(|f| f as i64)
            }),
        Some(serde_json::Value::String(s)) => s
            .trim()
            .parse::<i64>()
            .ok(),
        _ => None,
    })
}

fn flexible_f64<'de, D: Deserializer<'de>>(d: D) -> Result<Option<f64>, D::Error> {
    let v = Option::<serde_json::Value>::deserialize(d)?;
    Ok(match v {
        Some(serde_json::Value::Number(n)) => n.as_f64(),
        Some(serde_json::Value::String(s)) => s
            .trim()
            .parse::<f64>()
            .ok(),
        _ => None,
    })
}

fn flexible_string<'de, D: Deserializer<'de>>(
    d: D,
) -> Result<Option<String>, D::Error> {
    let v = Option::<serde_json::Value>::deserialize(d)?;
    Ok(match v {
        Some(serde_json::Value::String(s))
            if !s
                .trim()
                .is_empty() =>
        {
            Some(s)
        }
        Some(serde_json::Value::Number(n)) => Some(n.to_string()),
        _ => None,
    })
}

pub fn parse_datetime(s: &str) -> Option<DateTime<Utc>> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Some(dt.with_timezone(&Utc));
    }
    for fmt in [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%.f",
    ] {
        if let Ok(naive) = NaiveDateTime::parse_from_str(s, fmt) {
            return Some(naive.and_utc());
        }
    }
    None
}

fn flexible_datetime<'de, D: Deserializer<'de>>(
    d: D,
) -> Result<Option<DateTime<Utc>>, D::Error> {
    let v = Option::<serde_json::Value>::deserialize(d)?;
    Ok(match v {
        Some(serde_json::Value::String(s)) => parse_datetime(&s),
        _ => None,
    })
}

/// A count that some endpoints report as a number and others (add-to-list)
/// as the array of items acted on.
fn flexible_count<'de, D: Deserializer<'de>>(d: D) -> Result<Option<i64>, D::Error> {
    let v = Option::<serde_json::Value>::deserialize(d)?;
    Ok(match v {
        Some(serde_json::Value::Number(n)) => n.as_i64(),
        Some(serde_json::Value::Array(items)) => Some(items.len() as i64),
        Some(serde_json::Value::String(s)) => s
            .trim()
            .parse::<i64>()
            .ok(),
        _ => None,
    })
}

/// Whole seconds, UTC, `Z` suffix: the shape every documented example uses.
fn rfc3339_secs<S: serde::Serializer>(
    v: &Option<DateTime<Utc>>,
    s: S,
) -> Result<S::Ok, S::Error> {
    match v {
        Some(d) => {
            s.serialize_str(&d.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
        }
        None => s.serialize_none(),
    }
}

// ---------------------------------------------------------------------------
// Shared identity types
// ---------------------------------------------------------------------------

/// Every id Simkl can match on. Send all you have: it resolves `simkl` first,
/// then the external ids, then title and year.
#[skip_serializing_none]
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SimklIds {
    #[serde(default, deserialize_with = "flexible_i64")]
    pub simkl: Option<i64>,
    #[serde(default, deserialize_with = "flexible_string")]
    pub slug: Option<String>,
    #[serde(default, deserialize_with = "flexible_string")]
    pub imdb: Option<String>,
    #[serde(default, deserialize_with = "flexible_i64")]
    pub tmdb: Option<i64>,
    #[serde(default, deserialize_with = "flexible_i64")]
    pub tvdb: Option<i64>,
    #[serde(default, deserialize_with = "flexible_i64")]
    pub mal: Option<i64>,
    #[serde(default, deserialize_with = "flexible_i64")]
    pub anidb: Option<i64>,
    #[serde(default, deserialize_with = "flexible_i64")]
    pub kitsu: Option<i64>,
    #[serde(default, deserialize_with = "flexible_i64")]
    pub anilist: Option<i64>,
}

impl SimklIds {
    pub fn is_empty(&self) -> bool {
        self.simkl
            .is_none()
            && self
                .imdb
                .is_none()
            && self
                .tmdb
                .is_none()
            && self
                .tvdb
                .is_none()
            && self
                .mal
                .is_none()
            && self
                .anidb
                .is_none()
            && self
                .kitsu
                .is_none()
            && self
                .anilist
                .is_none()
    }
}

/// A watchlist bucket. `Other` keeps a sync alive when Simkl adds one.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    strum_macros::Display,
    strum_macros::EnumString,
)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
pub enum ListStatus {
    Watching,
    Plantowatch,
    Hold,
    Completed,
    Dropped,
    #[serde(other)]
    Other,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    strum_macros::Display,
    strum_macros::EnumString,
)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
pub enum ItemType {
    Shows,
    Movies,
    Anime,
}

// ---------------------------------------------------------------------------
// OAuth: PIN (device) flow and code exchange
// ---------------------------------------------------------------------------

/// Step 1 of the PIN flow: `GET /oauth/pin`.
#[derive(Debug, Clone, Default, Serialize)]
pub struct PinRequest {
    /// Where simkl.com/pin sends the user after approving.
    pub redirect: Option<String>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PinResponse {
    #[serde(default)]
    pub result: String,
    pub device_code: Option<String>,
    pub user_code: Option<String>,
    pub verification_url: Option<String>,
    pub verification_uri: Option<String>,
    #[serde(default, deserialize_with = "flexible_i64")]
    pub expires_in: Option<i64>,
    #[serde(default, deserialize_with = "flexible_i64")]
    pub interval: Option<i64>,
}

impl PinResponse {
    /// `verification_url` is the legacy alias of `verification_uri`; either
    /// may be the one populated.
    pub fn verification_url(&self) -> Option<&str> {
        self.verification_url
            .as_deref()
            .or(self
                .verification_uri
                .as_deref())
    }
}

impl Endpoint for PinRequest {
    type Output = PinResponse;

    fn path(&self) -> String {
        "oauth/pin".to_string()
    }

    fn query_params(&self) -> impl serde::Serialize + '_ {
        self
    }
}

/// Step 3 of the PIN flow: `GET /oauth/pin/{user_code}`.
#[derive(Debug, Clone)]
pub struct PinPoll {
    pub user_code: String,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PinPollResponse {
    #[serde(default)]
    pub result: String,
    pub access_token: Option<Secret<String>>,
    pub message: Option<String>,
    /// Present when Simkl no longer knows the code: it answers with a fresh
    /// initialisation instead of an error.
    pub device_code: Option<String>,
    pub user_code: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PinStatus {
    Pending,
    Approved {
        access_token: Secret<String>,
    },
    /// The code expired, was already consumed, or was never issued.
    Expired,
}

impl PinPollResponse {
    pub fn status(&self) -> PinStatus {
        if self
            .result
            .eq_ignore_ascii_case("OK")
        {
            if let Some(token) = self
                .access_token
                .as_ref()
                .filter(|t| {
                    !t.expose()
                        .is_empty()
                })
            {
                return PinStatus::Approved {
                    access_token: token.clone(),
                };
            }
        }
        if self
            .device_code
            .is_some()
            || self
                .user_code
                .is_some()
        {
            return PinStatus::Expired;
        }
        PinStatus::Pending
    }
}

impl Endpoint for PinPoll {
    type Output = PinPollResponse;

    fn path(&self) -> String {
        format!("oauth/pin/{}", self.user_code)
    }
}

/// `POST /oauth/token` for the redirect flow.
#[derive(Debug, Clone, Serialize)]
pub struct TokenExchange {
    pub code: String,
    pub client_id: String,
    pub client_secret: String,
    pub redirect_uri: String,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TokenResponse {
    pub access_token: Secret<String>,
    pub token_type: Option<String>,
    pub scope: Option<String>,
    #[serde(default, deserialize_with = "flexible_i64")]
    pub expires_in: Option<i64>,
}

impl Endpoint for TokenExchange {
    type Output = TokenResponse;

    fn path(&self) -> String {
        "oauth/token".to_string()
    }

    fn method(&self) -> Method {
        Method::POST
    }

    fn body(&self) -> Body {
        Body::Json(serde_json::json!({
            "code": self.code,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "redirect_uri": self.redirect_uri,
            "grant_type": "authorization_code",
        }))
    }
}

// ---------------------------------------------------------------------------
// Users
// ---------------------------------------------------------------------------

/// `POST /users/settings` — POST for historical reasons, no body. The
/// cheapest authenticated call, so it doubles as the token check.
#[derive(Debug, Clone, Default)]
pub struct UserSettings;

#[skip_serializing_none]
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct UserProfile {
    #[serde(default, deserialize_with = "flexible_string")]
    pub name: Option<String>,
    #[serde(default, deserialize_with = "flexible_datetime")]
    pub joined_at: Option<DateTime<Utc>>,
    #[serde(default, deserialize_with = "flexible_string")]
    pub avatar: Option<String>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct UserAccount {
    #[serde(default, deserialize_with = "flexible_i64")]
    pub id: Option<i64>,
    #[serde(default, deserialize_with = "flexible_string")]
    pub timezone: Option<String>,
    #[serde(rename = "type", default, deserialize_with = "flexible_string")]
    pub kind: Option<String>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct UserSettingsResponse {
    pub user: Option<UserProfile>,
    pub account: Option<UserAccount>,
}

impl Endpoint for UserSettings {
    type Output = UserSettingsResponse;

    fn path(&self) -> String {
        "users/settings".to_string()
    }

    fn method(&self) -> Method {
        Method::POST
    }
}

// ---------------------------------------------------------------------------
// Sync: activities and list reads
// ---------------------------------------------------------------------------

/// `GET /sync/activities`. `all` moving is the signal that a list pull is
/// worth making.
#[derive(Debug, Clone, Default)]
pub struct Activities;

#[skip_serializing_none]
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DomainActivities {
    #[serde(default, deserialize_with = "flexible_datetime")]
    pub all: Option<DateTime<Utc>>,
    #[serde(default, deserialize_with = "flexible_datetime")]
    pub rated_at: Option<DateTime<Utc>>,
    #[serde(default, deserialize_with = "flexible_datetime")]
    pub playback: Option<DateTime<Utc>>,
    #[serde(default, deserialize_with = "flexible_datetime")]
    pub plantowatch: Option<DateTime<Utc>>,
    #[serde(default, deserialize_with = "flexible_datetime")]
    pub watching: Option<DateTime<Utc>>,
    #[serde(default, deserialize_with = "flexible_datetime")]
    pub completed: Option<DateTime<Utc>>,
    #[serde(default, deserialize_with = "flexible_datetime")]
    pub hold: Option<DateTime<Utc>>,
    #[serde(default, deserialize_with = "flexible_datetime")]
    pub dropped: Option<DateTime<Utc>>,
    #[serde(default, deserialize_with = "flexible_datetime")]
    pub removed_from_list: Option<DateTime<Utc>>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ActivitiesResponse {
    #[serde(default, deserialize_with = "flexible_datetime")]
    pub all: Option<DateTime<Utc>>,
    pub tv_shows: Option<DomainActivities>,
    pub anime: Option<DomainActivities>,
    pub movies: Option<DomainActivities>,
}

impl ActivitiesResponse {
    /// Whether anything changed after `since`. Unknown (`all` missing) counts
    /// as changed, so a pull is never skipped on a parse gap.
    pub fn changed_since(&self, since: DateTime<Utc>) -> bool {
        self.all
            .map_or(true, |all| all > since)
    }
}

impl Endpoint for Activities {
    type Output = ActivitiesResponse;

    fn path(&self) -> String {
        "sync/activities".to_string()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Extended {
    Full,
    /// `full` plus, on every anime episode, where it sits in TVDB's
    /// season/episode scheme.
    FullAnimeSeasons,
    IdsOnly,
    SimklIdsOnly,
}

/// `GET /sync/all-items[/{type}[/{status}]]`.
#[derive(Debug, Clone, Default)]
pub struct AllItems {
    pub kind: Option<ItemType>,
    pub status: Option<ListStatus>,
    /// Only items modified after this instant.
    pub date_from: Option<DateTime<Utc>>,
    pub extended: Option<Extended>,
    /// Per-episode `watched_at`; needs `extended=full`.
    pub episode_watched_at: bool,
    /// Episode arrays for completed and dropped shows too; needs
    /// `extended=full`.
    pub include_all_episodes: bool,
}

impl AllItems {
    /// Everything a two-way sync needs: full entries with dated episodes,
    /// including the shows already finished, and anime episodes mapped onto
    /// TVDB seasons.
    pub fn for_sync(date_from: Option<DateTime<Utc>>) -> Self {
        Self {
            kind: None,
            status: None,
            date_from,
            extended: Some(Extended::FullAnimeSeasons),
            episode_watched_at: true,
            include_all_episodes: true,
        }
    }
}

#[skip_serializing_none]
#[derive(Serialize)]
struct AllItemsQuery<'a> {
    date_from: Option<String>,
    extended: Option<&'a Extended>,
    episode_watched_at: Option<&'static str>,
    include_all_episodes: Option<&'static str>,
}

impl Endpoint for AllItems {
    type Output = AllItemsResponse;

    fn path(&self) -> String {
        match (self.kind, self.status) {
            (Some(kind), Some(status)) => format!("sync/all-items/{kind}/{status}"),
            (Some(kind), None) => format!("sync/all-items/{kind}"),
            (None, Some(status)) => format!("sync/all-items/all/{status}"),
            (None, None) => "sync/all-items".to_string(),
        }
    }

    fn query_params(&self) -> impl serde::Serialize + '_ {
        AllItemsQuery {
            date_from: self
                .date_from
                .map(|d| d.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)),
            extended: self
                .extended
                .as_ref(),
            episode_watched_at: self
                .episode_watched_at
                .then_some("yes"),
            include_all_episodes: self
                .include_all_episodes
                .then_some("yes"),
        }
    }
}

#[skip_serializing_none]
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ItemInfo {
    #[serde(default, deserialize_with = "flexible_string")]
    pub title: Option<String>,
    #[serde(default, deserialize_with = "flexible_i64")]
    pub year: Option<i64>,
    /// Minutes.
    #[serde(default, deserialize_with = "flexible_i64")]
    pub runtime: Option<i64>,
    #[serde(default)]
    pub ids: SimklIds,
}

/// Where an anime episode sits in TVDB's season/episode scheme. Anime-native
/// catalogues count episodes per title from 1, so this is the mapping a
/// series/season/episode library needs.
#[skip_serializing_none]
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TvdbEpisodeRef {
    #[serde(default, deserialize_with = "flexible_i64")]
    pub season: Option<i64>,
    #[serde(default, deserialize_with = "flexible_i64")]
    pub episode: Option<i64>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct EpisodeEntry {
    #[serde(default, deserialize_with = "flexible_i64")]
    pub number: Option<i64>,
    #[serde(default, deserialize_with = "flexible_datetime")]
    pub watched_at: Option<DateTime<Utc>>,
    /// Anime only.
    pub tvdb: Option<TvdbEpisodeRef>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SeasonEntry {
    #[serde(default, deserialize_with = "flexible_i64")]
    pub number: Option<i64>,
    #[serde(default)]
    pub episodes: Vec<EpisodeEntry>,
}

/// One row of a user's list. Exactly one of `show` and `movie` is set; anime
/// entries use `show`.
#[skip_serializing_none]
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ListEntry {
    #[serde(default, deserialize_with = "flexible_datetime")]
    pub added_to_watchlist_at: Option<DateTime<Utc>>,
    #[serde(default, deserialize_with = "flexible_datetime")]
    pub last_watched_at: Option<DateTime<Utc>>,
    #[serde(default, deserialize_with = "flexible_datetime")]
    pub user_rated_at: Option<DateTime<Utc>>,
    #[serde(default, deserialize_with = "flexible_f64")]
    pub user_rating: Option<f64>,
    pub status: Option<ListStatus>,
    #[serde(default, deserialize_with = "flexible_i64")]
    pub watched_episodes_count: Option<i64>,
    #[serde(default, deserialize_with = "flexible_i64")]
    pub total_episodes_count: Option<i64>,
    #[serde(default, deserialize_with = "flexible_i64")]
    pub not_aired_episodes_count: Option<i64>,
    pub show: Option<ItemInfo>,
    pub movie: Option<ItemInfo>,
    #[serde(default)]
    pub seasons: Vec<SeasonEntry>,
    pub anime_type: Option<String>,
}

impl ListEntry {
    pub fn item(&self) -> Option<&ItemInfo> {
        self.movie
            .as_ref()
            .or(self
                .show
                .as_ref())
    }

    pub fn is_movie(&self) -> bool {
        self.movie
            .is_some()
    }
}

/// Keyed by type; a key is absent when that bucket is empty, and an empty
/// library is `{}`. Older responses were a bare array, which is accepted too.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct AllItemsResponse {
    #[serde(default)]
    pub shows: Vec<ListEntry>,
    #[serde(default)]
    pub movies: Vec<ListEntry>,
    #[serde(default)]
    pub anime: Vec<ListEntry>,
}

impl AllItemsResponse {
    pub fn entries(&self) -> impl Iterator<Item = &ListEntry> {
        self.movies
            .iter()
            .chain(
                self.shows
                    .iter(),
            )
            .chain(
                self.anime
                    .iter(),
            )
    }

    pub fn len(&self) -> usize {
        self.movies
            .len()
            + self
                .shows
                .len()
            + self
                .anime
                .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<'de> Deserialize<'de> for AllItemsResponse {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize, Default)]
        struct Keyed {
            #[serde(default)]
            shows: Vec<ListEntry>,
            #[serde(default)]
            movies: Vec<ListEntry>,
            #[serde(default)]
            anime: Vec<ListEntry>,
        }
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            Keyed(Keyed),
            List(Vec<ListEntry>),
            Null(()),
        }
        Ok(match Raw::deserialize(d)? {
            Raw::Keyed(k) => Self {
                shows: k.shows,
                movies: k.movies,
                anime: k.anime,
            },
            Raw::List(entries) => {
                let (movies, shows): (Vec<_>, Vec<_>) = entries
                    .into_iter()
                    .partition(ListEntry::is_movie);
                Self {
                    shows,
                    movies,
                    anime: Vec::new(),
                }
            }
            Raw::Null(()) => Self::default(),
        })
    }
}

// ---------------------------------------------------------------------------
// Sync: writes (history, ratings, lists)
// ---------------------------------------------------------------------------

#[skip_serializing_none]
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SyncEpisode {
    pub number: i64,
    #[serde(default, serialize_with = "rfc3339_secs")]
    pub watched_at: Option<DateTime<Utc>>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SyncSeason {
    pub number: i64,
    /// `None` addresses the whole season.
    pub episodes: Option<Vec<SyncEpisode>>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SyncMovie {
    pub title: Option<String>,
    pub year: Option<i64>,
    #[serde(default)]
    pub ids: SimklIds,
    #[serde(default, serialize_with = "rfc3339_secs")]
    pub watched_at: Option<DateTime<Utc>>,
    /// 1-10.
    pub rating: Option<u8>,
    #[serde(default, serialize_with = "rfc3339_secs")]
    pub rated_at: Option<DateTime<Utc>>,
    /// `add-to-list` only: the bucket this item goes to.
    pub to: Option<ListStatus>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SyncShow {
    pub title: Option<String>,
    pub year: Option<i64>,
    #[serde(default)]
    pub ids: SimklIds,
    /// Without seasons, `status: completed` marks the whole show watched.
    pub status: Option<ListStatus>,
    pub seasons: Option<Vec<SyncSeason>>,
    /// 1-10.
    pub rating: Option<u8>,
    #[serde(default, serialize_with = "rfc3339_secs")]
    pub rated_at: Option<DateTime<Utc>>,
    /// `add-to-list` only: the bucket this item goes to.
    pub to: Option<ListStatus>,
}

/// Body shared by every `/sync/*` write. Anime goes under `shows`; Simkl
/// resolves the type from the ids.
#[skip_serializing_none]
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SyncPayload {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub movies: Vec<SyncMovie>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub shows: Vec<SyncShow>,
}

impl SyncPayload {
    pub fn is_empty(&self) -> bool {
        self.movies
            .is_empty()
            && self
                .shows
                .is_empty()
    }
}

#[skip_serializing_none]
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SyncCounts {
    #[serde(default, deserialize_with = "flexible_count")]
    pub movies: Option<i64>,
    #[serde(default, deserialize_with = "flexible_count")]
    pub shows: Option<i64>,
    #[serde(default, deserialize_with = "flexible_count")]
    pub episodes: Option<i64>,
}

impl SyncCounts {
    pub fn total(&self) -> i64 {
        self.movies
            .unwrap_or(0)
            + self
                .shows
                .unwrap_or(0)
            + self
                .episodes
                .unwrap_or(0)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct NotFound {
    #[serde(default)]
    pub movies: Vec<serde_json::Value>,
    #[serde(default)]
    pub shows: Vec<serde_json::Value>,
    #[serde(default)]
    pub episodes: Vec<serde_json::Value>,
}

impl NotFound {
    pub fn len(&self) -> usize {
        self.movies
            .len()
            + self
                .shows
                .len()
            + self
                .episodes
                .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[skip_serializing_none]
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SyncResponse {
    pub added: Option<SyncCounts>,
    pub deleted: Option<SyncCounts>,
    #[serde(default)]
    pub not_found: NotFound,
}

impl SyncResponse {
    /// Items Simkl acted on, whichever verb the endpoint used.
    pub fn affected(&self) -> i64 {
        self.added
            .as_ref()
            .map(SyncCounts::total)
            .unwrap_or(0)
            + self
                .deleted
                .as_ref()
                .map(SyncCounts::total)
                .unwrap_or(0)
    }
}

macro_rules! sync_write_endpoint {
    ($name:ident, $path:literal, $doc:literal) => {
        #[doc = $doc]
        #[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
        pub struct $name(pub SyncPayload);

        impl Endpoint for $name {
            type Output = SyncResponse;

            fn path(&self) -> String {
                $path.to_string()
            }

            fn method(&self) -> Method {
                Method::POST
            }

            fn body(&self) -> Body {
                Body::Json(serde_json::to_value(&self.0).unwrap_or_default())
            }
        }
    };
}

sync_write_endpoint!(
    AddToHistory,
    "sync/history",
    "`POST /sync/history`: record watches. A show with `status: completed` and no seasons marks the whole show; seasons without episodes mark whole seasons."
);
sync_write_endpoint!(
    RemoveFromHistory,
    "sync/history/remove",
    "`POST /sync/history/remove`: same shape as the add; omitting seasons removes the whole item."
);
sync_write_endpoint!(
    AddRatings,
    "sync/ratings",
    "`POST /sync/ratings`: 1-10 integer ratings on movies and shows. Episodes cannot be rated."
);
sync_write_endpoint!(
    RemoveRatings,
    "sync/ratings/remove",
    "`POST /sync/ratings/remove`: same shape as the add, without `rating`."
);
sync_write_endpoint!(
    AddToList,
    "sync/add-to-list",
    "`POST /sync/add-to-list`: move each item into the bucket its own `to` names. The response lists the items instead of counting them."
);
sync_write_endpoint!(
    RemoveFromList,
    "sync/remove-from-list",
    "`POST /sync/remove-from-list`: drop items from the user's lists."
);

// ---------------------------------------------------------------------------
// Scrobble
// ---------------------------------------------------------------------------

#[skip_serializing_none]
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ScrobbleItem {
    pub title: Option<String>,
    pub year: Option<i64>,
    #[serde(default)]
    pub ids: SimklIds,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ScrobbleEpisode {
    pub season: i64,
    pub number: i64,
}

/// Exactly one of `movie` or `show` plus `episode`. Progress is 0-100 with
/// two decimals; `stop` at or above [`WATCHED_THRESHOLD_PERCENT`] marks the
/// item watched, below it saves a resumable playback.
#[skip_serializing_none]
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ScrobblePayload {
    pub progress: f64,
    pub movie: Option<ScrobbleItem>,
    pub show: Option<ScrobbleItem>,
    pub episode: Option<ScrobbleEpisode>,
}

impl ScrobblePayload {
    pub fn movie(item: ScrobbleItem, progress: f64) -> Self {
        Self {
            progress: clamp_progress(progress),
            movie: Some(item),
            show: None,
            episode: None,
        }
    }

    pub fn episode(
        show: ScrobbleItem,
        season: i64,
        number: i64,
        progress: f64,
    ) -> Self {
        Self {
            progress: clamp_progress(progress),
            movie: None,
            show: Some(show),
            episode: Some(ScrobbleEpisode { season, number }),
        }
    }
}

/// Two decimals, inside 0-100, as the API requires.
pub fn clamp_progress(progress: f64) -> f64 {
    let p = if progress.is_finite() { progress } else { 0.0 };
    (p.clamp(0.0, 100.0) * 100.0).round() / 100.0
}

#[skip_serializing_none]
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ScrobbleResponse {
    /// `start`, `pause`, `scrobble` or `checkin`.
    pub action: Option<String>,
    #[serde(default, deserialize_with = "flexible_f64")]
    pub progress: Option<f64>,
    pub movie: Option<ScrobbleItem>,
    pub show: Option<ScrobbleItem>,
}

macro_rules! scrobble_endpoint {
    ($name:ident, $path:literal, $doc:literal) => {
        #[doc = $doc]
        #[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
        pub struct $name(pub ScrobblePayload);

        impl Endpoint for $name {
            type Output = ScrobbleResponse;

            fn path(&self) -> String {
                $path.to_string()
            }

            fn method(&self) -> Method {
                Method::POST
            }

            fn body(&self) -> Body {
                Body::Json(serde_json::to_value(&self.0).unwrap_or_default())
            }
        }
    };
}

scrobble_endpoint!(
    ScrobbleStart,
    "scrobble/start",
    "`POST /scrobble/start`: playback began or resumed; the item shows under Watching now."
);
scrobble_endpoint!(
    ScrobblePause,
    "scrobble/pause",
    "`POST /scrobble/pause`: save progress; resumable from any device."
);
scrobble_endpoint!(
    ScrobbleStop,
    "scrobble/stop",
    "`POST /scrobble/stop`: end playback. A 409 means this item was already scrobbled within the last hour."
);

// ---------------------------------------------------------------------------
// Search
// ---------------------------------------------------------------------------

/// `GET /search/id`: find the Simkl entry for an external id.
#[skip_serializing_none]
#[derive(Debug, Clone, Default, Serialize)]
pub struct SearchById {
    pub imdb: Option<String>,
    pub tmdb: Option<i64>,
    pub tvdb: Option<i64>,
    pub mal: Option<i64>,
    pub anidb: Option<i64>,
    /// `show`, `movie`, `anime` or `tv`.
    #[serde(rename = "type")]
    pub kind: Option<String>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SearchResult {
    #[serde(rename = "type", default, deserialize_with = "flexible_string")]
    pub kind: Option<String>,
    #[serde(default, deserialize_with = "flexible_string")]
    pub title: Option<String>,
    #[serde(default, deserialize_with = "flexible_i64")]
    pub year: Option<i64>,
    #[serde(default)]
    pub ids: SimklIds,
}

impl Endpoint for SearchById {
    type Output = Vec<SearchResult>;

    fn path(&self) -> String {
        "search/id".to_string()
    }

    fn query_params(&self) -> impl serde::Serialize + '_ {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use httpmock::MockServer;

    fn test_client(server: &MockServer, token: Option<&str>) -> RestClient<SimklAuth> {
        client("cid-123", token, &server.base_url()).unwrap()
    }

    #[tokio::test]
    async fn every_request_carries_the_client_identity() {
        let server = MockServer::start();
        let pin = server.mock(|when, then| {
            when.method("GET")
                .path("/oauth/pin")
                .query_param("client_id", "cid-123")
                .query_param("app-name", APP_NAME)
                .query_param("app-version", APP_VERSION)
                .header("simkl-api-key", "cid-123")
                .header_exists("user-agent");
            then.status(200)
                .json_body(serde_json::json!({
                    "result": "OK",
                    "device_code": "ignored",
                    "user_code": "5G6JAH",
                    "verification_uri": "https://simkl.com/pin",
                    "expires_in": 900,
                    "interval": 5
                }));
        });

        let got = test_client(&server, None)
            .execute(PinRequest::default())
            .await
            .unwrap();
        pin.assert();
        assert_eq!(
            got.user_code
                .as_deref(),
            Some("5G6JAH")
        );
        assert_eq!(got.verification_url(), Some("https://simkl.com/pin"));
        assert_eq!(got.interval, Some(5));
    }

    #[tokio::test]
    async fn user_scoped_calls_add_a_bearer_token() {
        let server = MockServer::start();
        let settings = server.mock(|when, then| {
            when.method("POST")
                .path("/users/settings")
                .header("authorization", "Bearer tok-1");
            then.status(200)
                .json_body(serde_json::json!({
                    "user": {"name": "alice", "joined_at": "2018-01-15T00:00:00Z"},
                    "account": {"id": 12345, "timezone": "Europe/Madrid", "type": "free"}
                }));
        });

        let got = test_client(&server, Some("tok-1"))
            .execute(UserSettings)
            .await
            .unwrap();
        settings.assert();
        assert_eq!(
            got.user
                .and_then(|u| u.name)
                .as_deref(),
            Some("alice")
        );
        assert_eq!(
            got.account
                .and_then(|a| a.id),
            Some(12345)
        );
    }

    #[test]
    fn pin_poll_distinguishes_pending_approved_and_expired() {
        let pending: PinPollResponse = serde_json::from_value(serde_json::json!({
            "result": "KO", "message": "Authorization pending"
        }))
        .unwrap();
        assert_eq!(pending.status(), PinStatus::Pending);

        let slow: PinPollResponse = serde_json::from_value(serde_json::json!({
            "result": "KO", "message": "Slow down"
        }))
        .unwrap();
        assert_eq!(slow.status(), PinStatus::Pending);

        let approved: PinPollResponse = serde_json::from_value(serde_json::json!({
            "result": "OK", "access_token": "s3cr3t"
        }))
        .unwrap();
        assert_eq!(
            approved.status(),
            PinStatus::Approved {
                access_token: Secret::new("s3cr3t".into())
            }
        );
        assert!(
            !format!("{:?}", approved.status()).contains("s3cr3t"),
            "a token must not be printable"
        );

        // A fresh initialisation is how Simkl says the old code is gone.
        let expired: PinPollResponse = serde_json::from_value(serde_json::json!({
            "result": "OK", "device_code": "x", "user_code": "NEW123",
            "verification_uri": "https://simkl.com/pin", "expires_in": 900, "interval": 5
        }))
        .unwrap();
        assert_eq!(expired.status(), PinStatus::Expired);
    }

    #[test]
    fn ids_accept_numbers_and_numeric_strings() {
        let ids: SimklIds = serde_json::from_value(serde_json::json!({
            "simkl": 2090, "slug": "the-walking-dead", "imdb": "tt1520211",
            "tvdb": "153021", "tmdb": 1399, "mal": null
        }))
        .unwrap();
        assert_eq!(ids.tvdb, Some(153021));
        assert_eq!(ids.tmdb, Some(1399));
        assert_eq!(
            ids.imdb
                .as_deref(),
            Some("tt1520211")
        );
        assert_eq!(ids.mal, None);
        assert!(!ids.is_empty());
        assert!(SimklIds::default().is_empty());
    }

    #[test]
    fn all_items_parses_the_keyed_shape_with_episodes() {
        let body = serde_json::json!({
            "shows": [{
                "last_watched_at": "2026-05-15T00:35:15Z",
                "user_rating": 8,
                "status": "watching",
                "last_watched": "S01E01",
                "watched_episodes_count": 2,
                "show": {
                    "title": "The Walking Dead", "year": 2010, "runtime": 43,
                    "ids": {"simkl": 2090, "imdb": "tt1520211", "tvdb": "153021", "tmdb": 1399}
                },
                "seasons": [{"number": 1, "episodes": [
                    {"number": 2, "watched_at": "2026-05-15T00:32:20Z"},
                    {"number": 3, "watched_at": "2026-05-15T00:32:21Z"}
                ]}]
            }],
            "anime": [{
                "status": "completed",
                "show": {"title": "Cowboy Bebop", "year": 1998, "ids": {"simkl": 37089, "mal": 1, "kitsu": 1}},
                "anime_type": "tv",
                "seasons": [{"number": 1, "episodes": [{"number": 1, "watched_at": "2026-05-15T00:13:09Z", "tvdb": {"season": 1, "episode": 1}}]}]
            }],
            "movies": [{
                "last_watched_at": "1994-09-01T16:00:00Z",
                "status": "completed",
                "user_rating": null,
                "movie": {"title": "The Godfather", "year": 1972, "ids": {"simkl": 53434, "imdb": "tt0068646", "tmdb": 238, "tvdb": "275"}}
            }]
        });
        let got: AllItemsResponse = serde_json::from_value(body).unwrap();
        assert_eq!(got.len(), 3);
        let show = &got.shows[0];
        assert_eq!(show.status, Some(ListStatus::Watching));
        assert_eq!(show.user_rating, Some(8.0));
        assert_eq!(
            show.seasons[0]
                .episodes
                .len(),
            2
        );
        assert_eq!(show.seasons[0].episodes[1].number, Some(3));
        assert!(
            show.seasons[0].episodes[0]
                .watched_at
                .is_some()
        );
        assert_eq!(
            got.anime[0]
                .item()
                .unwrap()
                .ids
                .kitsu,
            Some(1)
        );
        assert_eq!(
            got.anime[0].seasons[0].episodes[0]
                .tvdb
                .as_ref()
                .and_then(|t| t.episode),
            Some(1)
        );
        let movie = &got.movies[0];
        assert!(movie.is_movie());
        assert_eq!(movie.status, Some(ListStatus::Completed));
        assert_eq!(
            movie
                .item()
                .unwrap()
                .ids
                .tvdb,
            Some(275)
        );
        assert_eq!(
            movie
                .last_watched_at
                .unwrap()
                .to_rfc3339(),
            "1994-09-01T16:00:00+00:00"
        );
    }

    #[test]
    fn all_items_accepts_an_empty_library_and_a_bare_list() {
        let empty: AllItemsResponse = serde_json::from_str("{}").unwrap();
        assert!(empty.is_empty());
        let null: AllItemsResponse = serde_json::from_str("null").unwrap();
        assert!(null.is_empty());

        let list: AllItemsResponse = serde_json::from_value(serde_json::json!([
            {"status": "completed", "movie": {"title": "Heat", "ids": {"imdb": "tt0113277"}}},
            {"status": "watching", "show": {"title": "The Wire", "ids": {"imdb": "tt0306414"}}}
        ]))
        .unwrap();
        assert_eq!(
            list.movies
                .len(),
            1
        );
        assert_eq!(
            list.shows
                .len(),
            1
        );
    }

    #[test]
    fn an_unknown_list_status_does_not_fail_the_whole_pull() {
        let got: AllItemsResponse = serde_json::from_value(serde_json::json!({
            "movies": [{"status": "rewatching", "movie": {"ids": {"simkl": 1}}}]
        }))
        .unwrap();
        assert_eq!(got.movies[0].status, Some(ListStatus::Other));
    }

    #[test]
    fn all_items_query_uses_the_documented_parameters() {
        let since = DateTime::parse_from_rfc3339("2026-05-14T06:50:38Z")
            .unwrap()
            .with_timezone(&Utc);
        let ep = AllItems::for_sync(Some(since));
        assert_eq!(ep.path(), "sync/all-items");
        let q = ep.query();
        assert!(q.contains(&("date_from".into(), "2026-05-14T06%3A50%3A38Z".into())));
        assert!(q.contains(&("extended".into(), "full_anime_seasons".into())));
        assert!(q.contains(&("episode_watched_at".into(), "yes".into())));
        assert!(q.contains(&("include_all_episodes".into(), "yes".into())));

        let typed = AllItems {
            kind: Some(ItemType::Movies),
            status: Some(ListStatus::Plantowatch),
            ..Default::default()
        };
        assert_eq!(typed.path(), "sync/all-items/movies/plantowatch");
        assert!(
            typed
                .query()
                .is_empty()
        );
        let status_only = AllItems {
            status: Some(ListStatus::Completed),
            ..Default::default()
        };
        assert_eq!(status_only.path(), "sync/all-items/all/completed");
    }

    #[test]
    fn activities_gate_on_the_top_level_timestamp() {
        let acts: ActivitiesResponse = serde_json::from_value(serde_json::json!({
            "all": "2026-05-14T06:50:38Z",
            "tv_shows": {"all": "2026-05-14T06:49:56Z", "hold": null},
            "movies": {"all": "2026-05-14T06:50:38Z"}
        }))
        .unwrap();
        let before = DateTime::parse_from_rfc3339("2026-05-14T06:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let after = DateTime::parse_from_rfc3339("2026-05-14T07:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert!(acts.changed_since(before));
        assert!(!acts.changed_since(after));
        assert!(ActivitiesResponse::default().changed_since(after));
    }

    #[test]
    fn history_payload_omits_what_was_not_set() {
        let payload = SyncPayload {
            movies: vec![SyncMovie {
                title: Some("Heat".into()),
                year: Some(1995),
                ids: SimklIds {
                    imdb: Some("tt0113277".into()),
                    tmdb: Some(949),
                    ..Default::default()
                },
                watched_at: None,
                rating: None,
                rated_at: None,
                to: None,
            }],
            shows: vec![SyncShow {
                title: None,
                year: None,
                ids: SimklIds {
                    tvdb: Some(79126),
                    ..Default::default()
                },
                status: None,
                seasons: Some(vec![SyncSeason {
                    number: 1,
                    episodes: Some(vec![SyncEpisode {
                        number: 1,
                        watched_at: None,
                    }]),
                }]),
                rating: None,
                rated_at: None,
                to: None,
            }],
        };
        let json = serde_json::to_value(&payload).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "movies": [{"title": "Heat", "year": 1995, "ids": {"imdb": "tt0113277", "tmdb": 949}}],
                "shows": [{"ids": {"tvdb": 79126}, "seasons": [{"number": 1, "episodes": [{"number": 1}]}]}]
            })
        );

        let whole_season = SyncShow {
            seasons: Some(vec![SyncSeason {
                number: 2,
                episodes: None,
            }]),
            ..Default::default()
        };
        assert_eq!(
            serde_json::to_value(&whole_season).unwrap(),
            serde_json::json!({"ids": {}, "seasons": [{"number": 2}]})
        );
    }

    /// Simkl rejects a top-level `to`; each item names its own bucket.
    #[test]
    fn add_to_list_names_each_items_bucket() {
        let json = serde_json::to_value(&AddToList(SyncPayload {
            movies: vec![SyncMovie {
                to: Some(ListStatus::Plantowatch),
                ..Default::default()
            }],
            shows: vec![],
        }))
        .unwrap();
        assert_eq!(json["movies"][0]["to"], "plantowatch");
        assert!(
            json.get("to")
                .is_none()
        );
        let listed: SyncResponse = serde_json::from_value(serde_json::json!({
            "added": {"movies": [{"to": "plantowatch", "ids": {"simkl": 1}}], "shows": []},
            "not_found": {"movies": [], "shows": []}
        }))
        .unwrap();
        assert_eq!(listed.affected(), 1);
        assert_eq!(AddToList::default().path(), "sync/add-to-list");
        assert_eq!(RemoveFromList::default().path(), "sync/remove-from-list");
        assert_eq!(AddRatings::default().path(), "sync/ratings");
        assert_eq!(RemoveRatings::default().path(), "sync/ratings/remove");
        assert_eq!(AddToHistory::default().path(), "sync/history");
        assert_eq!(RemoveFromHistory::default().path(), "sync/history/remove");
    }

    #[tokio::test]
    async fn a_history_write_reports_what_was_added_and_what_was_not_found() {
        let server = MockServer::start();
        let add = server.mock(|when, then| {
            when.method("POST")
                .path("/sync/history")
                .header("content-type", "application/json")
                .json_body_partial(r#"{"movies": [{"ids": {"imdb": "tt0113277"}}]}"#);
            then.status(201)
                .json_body(serde_json::json!({
                    "added": {"movies": 1, "shows": 0, "episodes": 0},
                    "not_found": {"movies": [], "shows": [{"ids": {"tvdb": 1}}]}
                }));
        });

        let got = test_client(&server, Some("tok"))
            .execute(AddToHistory(SyncPayload {
                movies: vec![SyncMovie {
                    ids: SimklIds {
                        imdb: Some("tt0113277".into()),
                        ..Default::default()
                    },
                    ..Default::default()
                }],
                ..Default::default()
            }))
            .await
            .unwrap();
        add.assert();
        assert_eq!(got.affected(), 1);
        assert_eq!(
            got.not_found
                .len(),
            1
        );
    }

    #[test]
    fn scrobble_payloads_carry_exactly_one_item_and_a_bounded_progress() {
        let movie = ScrobblePayload::movie(
            ScrobbleItem {
                title: Some("Inception".into()),
                year: Some(2010),
                ids: SimklIds {
                    imdb: Some("tt1375666".into()),
                    ..Default::default()
                },
            },
            133.7,
        );
        assert_eq!(
            serde_json::to_value(&movie).unwrap(),
            serde_json::json!({
                "progress": 100.0,
                "movie": {"title": "Inception", "year": 2010, "ids": {"imdb": "tt1375666"}}
            })
        );

        let episode = ScrobblePayload::episode(
            ScrobbleItem {
                ids: SimklIds {
                    tvdb: Some(305288),
                    ..Default::default()
                },
                ..Default::default()
            },
            1,
            3,
            42.123,
        );
        assert_eq!(
            serde_json::to_value(&episode).unwrap(),
            serde_json::json!({
                "progress": 42.12,
                "show": {"ids": {"tvdb": 305288}},
                "episode": {"season": 1, "number": 3}
            })
        );
        assert_eq!(clamp_progress(-3.0), 0.0);
        assert_eq!(clamp_progress(f64::NAN), 0.0);
        assert_eq!(ScrobbleStart::default().path(), "scrobble/start");
        assert_eq!(ScrobblePause::default().path(), "scrobble/pause");
        assert_eq!(ScrobbleStop::default().path(), "scrobble/stop");
    }

    #[tokio::test]
    async fn errors_surface_simkls_machine_readable_code() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method("POST")
                .path("/scrobble/stop");
            then.status(412)
                .json_body(serde_json::json!({
                    "error": "client_id_failed", "code": 412, "message": "Invalid client_id"
                }));
        });
        let err = test_client(&server, Some("tok"))
            .execute(ScrobbleStop::default())
            .await
            .unwrap_err();
        match err {
            ClientError::Http {
                status, message, ..
            } => {
                assert_eq!(status, 412);
                assert_eq!(message, "client_id_failed: Invalid client_id");
            }
            other => panic!("expected an http error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_revoked_token_is_unauthorized() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method("GET")
                .path("/sync/activities");
            then.status(401)
                .json_body(
                    serde_json::json!({"error": "user_token_failed", "code": 401}),
                );
        });
        let err = test_client(&server, Some("revoked"))
            .execute(Activities)
            .await
            .unwrap_err();
        assert!(matches!(err, ClientError::Unauthorized));
    }

    #[test]
    fn search_by_id_serialises_its_filters() {
        let ep = SearchById {
            imdb: Some("tt0113277".into()),
            kind: Some("movie".into()),
            ..Default::default()
        };
        let q = ep.query();
        assert!(q.contains(&("imdb".into(), "tt0113277".into())));
        assert!(q.contains(&("type".into(), "movie".into())));
        assert!(
            !q.iter()
                .any(|(k, _)| k == "tmdb")
        );
    }

    #[test]
    fn dates_without_an_offset_still_parse() {
        assert!(parse_datetime("2026-05-14T06:50:38Z").is_some());
        assert!(parse_datetime("2024-05-01T18:00:00-05:00").is_some());
        assert!(parse_datetime("2026-05-14T06:50:38").is_some());
        assert!(parse_datetime("2026-05-14 06:50:38").is_some());
        assert!(parse_datetime("").is_none());
        assert!(parse_datetime("yesterday").is_none());
    }

    /// Dates go out as whole seconds with a `Z`, the way the docs show them.
    #[test]
    fn write_timestamps_carry_no_fractional_seconds() {
        let at = DateTime::parse_from_rfc3339("2026-09-07T10:11:12.482913554Z")
            .unwrap()
            .with_timezone(&Utc);
        let movie = serde_json::to_value(SyncMovie {
            watched_at: Some(at),
            rated_at: Some(at),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(movie["watched_at"], "2026-09-07T10:11:12Z");
        assert_eq!(movie["rated_at"], "2026-09-07T10:11:12Z");
        let episode = serde_json::to_value(SyncEpisode {
            number: 1,
            watched_at: Some(at),
        })
        .unwrap();
        assert_eq!(episode["watched_at"], "2026-09-07T10:11:12Z");
        // And still read back.
        let back: SyncMovie = serde_json::from_value(movie).unwrap();
        assert_eq!(
            back.watched_at
                .unwrap()
                .timestamp(),
            at.timestamp()
        );
    }
}

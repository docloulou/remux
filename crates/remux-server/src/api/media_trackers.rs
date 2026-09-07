//! A user's connections to media trackers (Simkl, …). Jellyfin has nothing
//! like it, so everything lives under `/remux`. A user manages their own
//! connections; an admin can manage anyone's, which is how the dashboard
//! connects an account on a user's behalf.

use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use remux_macros::{delete, get, post, put};
use std::{str::FromStr, sync::Arc};
use tracing::warn;
use uuid::Uuid;

use crate::{
    AppState, IntoApiError, OptionExt, ResultExt,
    addons::{
        AddonRuntime,
        media_tracker::{
            AuthFlow, DeviceAuthPoll, MediaTrackerAddon, MediaTrackerCapabilities,
            MediaTrackerCtx, MediaTrackerError, MediaTrackerEventKind,
        },
    },
    db::{self, auth},
    services::media_tracker as service,
};
use axum_anyhow::ApiResult as Result;
use remux_sdks::remux::{
    MediaTrackerDeviceAuthDto, MediaTrackerDevicePollDto,
    MediaTrackerDevicePollRequest, MediaTrackerProviderDto, MediaTrackerSyncRequest,
    MediaTrackerSyncStartedDto, MediaTrackerTokenConnectRequest,
    UpdateMediaTrackerRequest, UserMediaTrackerDto, UserMediaTrackersDto,
};

fn provider_dto(
    runtime: &AddonRuntime,
    caps: &MediaTrackerCapabilities,
) -> MediaTrackerProviderDto {
    MediaTrackerProviderDto {
        addon_id: runtime
            .row
            .id,
        name: runtime
            .row
            .name
            .clone(),
        kind: runtime
            .row
            .preset
            .kind
            .clone(),
        auth_flow: caps
            .auth_flow
            .to_string(),
        connect_fields: caps
            .connect_fields
            .clone(),
        supported_events: caps
            .supported_events
            .iter()
            .map(ToString::to_string)
            .collect(),
        default_event_filter: caps
            .default_event_filter
            .iter()
            .map(ToString::to_string)
            .collect(),
        history_import: caps.history_import,
        pulls_changes: caps
            .watch_state_sync
            .pulls()
            || caps
                .ratings
                .pulls()
            || caps
                .favorites
                .pulls(),
    }
}

fn tracker_dto(t: db::UserMediaTracker) -> UserMediaTrackerDto {
    UserMediaTrackerDto {
        id: t.id,
        addon_id: t.addon_id,
        user_id: t.user_id,
        status: t
            .status
            .to_string(),
        event_filters: t
            .event_filters
            .iter()
            .map(ToString::to_string)
            .collect(),
        account_name: t.account_name,
        last_success_at: t.last_success_at,
        last_error_at: t.last_error_at,
        last_error: t.last_error,
        last_error_kind: t
            .last_error_kind
            .map(|k| k.to_string()),
        last_pull_at: t.last_pull_at,
        created_at: t.created_at,
        updated_at: t.updated_at,
    }
}

/// A provider failure as an API error: something worth retrying is a 502,
/// anything else a 400 carrying the provider's reason.
fn provider_error(e: MediaTrackerError) -> axum_anyhow::ApiError {
    let detail = e.to_string();
    let err = anyhow::anyhow!(detail.clone());
    if e.is_retryable() {
        err.context_bad_gateway(&detail)
    } else {
        err.context_bad_request(&detail)
    }
}

fn tracker_ctx(state: &AppState) -> MediaTrackerCtx {
    MediaTrackerCtx {
        config: Arc::new(
            state
                .ctx
                .config
                .clone(),
        ),
    }
}

/// The enabled tracker addon `addon_id` names.
fn provider(
    state: &AppState,
    addon_id: Uuid,
) -> Result<(Arc<dyn MediaTrackerAddon>, MediaTrackerCapabilities)> {
    let addon = state
        .ctx
        .addons
        .media_tracker_for(addon_id)
        .context_not_found("No media tracker addon with that id")?;
    let caps = addon.capabilities();
    Ok((addon, caps))
}

/// The requested event filter checked against what the provider supports, or
/// the provider's default when the caller left it out.
fn parse_filters(
    caps: &MediaTrackerCapabilities,
    filters: Option<Vec<String>>,
) -> Result<Vec<MediaTrackerEventKind>> {
    let Some(filters) = filters else {
        return Ok(caps
            .default_event_filter
            .clone());
    };
    let mut out = Vec::with_capacity(filters.len());
    for raw in filters {
        let kind = MediaTrackerEventKind::from_str(raw.trim())
            .context_bad_request(&format!("Unknown event kind: {raw}"))?;
        if !caps.supports(kind) {
            return Err(anyhow::anyhow!("unsupported event kind {kind}")
                .context_bad_request(&format!(
                    "This provider does not support {kind} events"
                )));
        }
        if !out.contains(&kind) {
            out.push(kind);
        }
    }
    Ok(out)
}

/// A connection belonging to `user`, or 404: another user's connection is
/// not distinguishable from a missing one.
async fn own_connection(
    state: &AppState,
    user: &db::User,
    id: Uuid,
) -> Result<db::UserMediaTracker> {
    db::UserMediaTracker::get(
        &state
            .ctx
            .db,
        id,
    )
    .await?
    .filter(|t| t.user_id == user.id)
    .context_not_found("No such media tracker connection")
}

/// Every tracker a user could connect, and the connections they have.
#[get("/remux/users/{user_id}/mediatrackers")]
pub async fn list(
    State(state): State<AppState>,
    auth::TargetUser(user): auth::TargetUser,
    Path(_user_id): Path<Uuid>,
) -> Result<Json<UserMediaTrackersDto>> {
    let providers: Vec<MediaTrackerProviderDto> = state
        .ctx
        .addons
        .list()
        .iter()
        .filter(|r| {
            r.row
                .enabled
        })
        .filter_map(|r| {
            r.caps
                .media_tracker
                .as_ref()
                .map(|a| provider_dto(r, &a.capabilities()))
        })
        .collect();
    let connections = db::UserMediaTracker::list_for_user(
        &state
            .ctx
            .db,
        user.id,
    )
    .await?
    .into_iter()
    .map(tracker_dto)
    .collect();
    Ok(Json(UserMediaTrackersDto {
        providers,
        connections,
    }))
}

/// Start a device-code login: the response carries the code to show the user
/// and where they enter it.
#[post("/remux/users/{user_id}/mediatrackers/providers/{addon_id}/device")]
pub async fn begin_device_auth(
    State(state): State<AppState>,
    auth::TargetUser(_user): auth::TargetUser,
    Path((_, addon_id)): Path<(Uuid, Uuid)>,
) -> Result<Json<MediaTrackerDeviceAuthDto>> {
    let (addon, caps) = provider(&state, addon_id)?;
    if caps.auth_flow != AuthFlow::OAuthDeviceCode {
        return Err(anyhow::anyhow!("wrong auth flow")
            .context_bad_request("This provider does not use a device code"));
    }
    let start = addon
        .begin_device_auth(&tracker_ctx(&state))
        .await
        .map_err(provider_error)?;
    Ok(Json(MediaTrackerDeviceAuthDto {
        verification_url: start.verification_url,
        user_code: start.user_code,
        poll_token: start.poll_token,
        interval_secs: start
            .interval
            .as_secs(),
        expires_in_secs: start
            .expires_in
            .as_secs(),
    }))
}

/// Ask whether the user has approved the code yet. Approval stores the
/// connection and starts the first import.
#[post("/remux/users/{user_id}/mediatrackers/providers/{addon_id}/device/poll")]
pub async fn poll_device_auth(
    State(state): State<AppState>,
    auth::TargetUser(user): auth::TargetUser,
    Path((_, addon_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<MediaTrackerDevicePollRequest>,
) -> Result<Json<MediaTrackerDevicePollDto>> {
    let (addon, caps) = provider(&state, addon_id)?;
    if req
        .poll_token
        .trim()
        .is_empty()
    {
        return Err(anyhow::anyhow!("empty poll token")
            .context_bad_request("pollToken is required"));
    }
    // Checked before polling so a bad filter never wastes an approval.
    let filters = parse_filters(&caps, req.event_filters)?;
    let poll = addon
        .poll_device_auth(&req.poll_token, &tracker_ctx(&state))
        .await
        .map_err(provider_error)?;
    let (status, connection) = match poll {
        DeviceAuthPoll::Pending => ("pending", None),
        DeviceAuthPoll::Denied => ("denied", None),
        DeviceAuthPoll::Approved(creds) => {
            let tracker =
                service::connect_tracker(&state.ctx, user.id, addon_id, creds, filters)
                    .await?;
            ("approved", Some(tracker_dto(tracker)))
        }
    };
    Ok(Json(MediaTrackerDevicePollDto {
        status: status.to_string(),
        connection,
    }))
}

/// Connect with a pasted token or key, for providers that work that way.
#[post("/remux/users/{user_id}/mediatrackers/providers/{addon_id}/token")]
pub async fn connect_with_token(
    State(state): State<AppState>,
    auth::TargetUser(user): auth::TargetUser,
    Path((_, addon_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<MediaTrackerTokenConnectRequest>,
) -> Result<(StatusCode, Json<UserMediaTrackerDto>)> {
    let (addon, caps) = provider(&state, addon_id)?;
    if caps.auth_flow != AuthFlow::Token {
        return Err(anyhow::anyhow!("wrong auth flow")
            .context_bad_request("This provider does not connect with a token"));
    }
    let filters = parse_filters(&caps, req.event_filters)?;
    let creds = addon
        .connect_with_token(&req.fields, &tracker_ctx(&state))
        .await
        .map_err(provider_error)?;
    let tracker =
        service::connect_tracker(&state.ctx, user.id, addon_id, creds, filters).await?;
    Ok((StatusCode::CREATED, Json(tracker_dto(tracker))))
}

/// Change what is synced. Credentials are untouched, so this never needs a
/// reconnect.
#[put("/remux/users/{user_id}/mediatrackers/connections/{id}")]
pub async fn update(
    State(state): State<AppState>,
    auth::TargetUser(user): auth::TargetUser,
    Path((_, id)): Path<(Uuid, Uuid)>,
    Json(req): Json<UpdateMediaTrackerRequest>,
) -> Result<Json<UserMediaTrackerDto>> {
    let tracker = own_connection(&state, &user, id).await?;
    let (_, caps) = provider(&state, tracker.addon_id)?;
    let filters = parse_filters(&caps, Some(req.event_filters))?;
    db::UserMediaTracker::set_event_filters(
        &state
            .ctx
            .db,
        tracker.id,
        &filters,
    )
    .await?;
    let tracker = own_connection(&state, &user, id).await?;
    Ok(Json(tracker_dto(tracker)))
}

/// Forget the connection. The provider is told when it can be, and the row
/// goes either way: disconnecting must work while the provider is down.
#[delete("/remux/users/{user_id}/mediatrackers/connections/{id}")]
pub async fn disconnect(
    State(state): State<AppState>,
    auth::TargetUser(user): auth::TargetUser,
    Path((_, id)): Path<(Uuid, Uuid)>,
) -> Result<StatusCode> {
    let tracker = own_connection(&state, &user, id).await?;
    if let Some(addon) = state
        .ctx
        .addons
        .media_tracker_for(tracker.addon_id)
    {
        if let Err(e) = addon
            .disconnect(&tracker.credentials, &tracker_ctx(&state))
            .await
        {
            warn!(tracker = %tracker.id, error = %e, "provider disconnect failed; removing locally");
        }
    }
    db::UserMediaTracker::delete(
        &state
            .ctx
            .db,
        tracker.id,
    )
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Pull the provider's state now, in the background. The connection's
/// `lastPullAt` and `lastError` show how it went.
#[post("/remux/users/{user_id}/mediatrackers/connections/{id}/sync")]
pub async fn sync_now(
    State(state): State<AppState>,
    auth::TargetUser(user): auth::TargetUser,
    Path((_, id)): Path<(Uuid, Uuid)>,
    body: Option<Json<MediaTrackerSyncRequest>>,
) -> Result<(StatusCode, Json<MediaTrackerSyncStartedDto>)> {
    let tracker = own_connection(&state, &user, id).await?;
    if tracker.status != db::MediaTrackerStatus::Connected {
        return Err(anyhow::anyhow!("connection is {}", tracker.status)
            .context_bad_request("Reconnect this tracker before syncing"));
    }
    let (_, caps) = provider(&state, tracker.addon_id)?;
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
        return Err(anyhow::anyhow!("provider does not pull")
            .context_bad_request("This provider does not report remote changes"));
    }
    let full = body.map_or(false, |Json(b)| b.full);
    service::spawn_sync(
        state
            .ctx
            .clone(),
        tracker,
        full,
    );
    Ok((
        StatusCode::ACCEPTED,
        Json(MediaTrackerSyncStartedDto { started: true }),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Config,
        integration_test::{
            AUTH_HEADER, auth_header_with_token, new_test_server_with_config,
        },
    };
    use axum_test::TestServer;
    use http::header::{HeaderName, HeaderValue};
    use httpmock::MockServer;
    use serde_json::json;

    fn auth(token: &str) -> (HeaderName, HeaderValue) {
        (
            http::header::AUTHORIZATION,
            HeaderValue::from_str(&auth_header_with_token(token)).unwrap(),
        )
    }

    async fn login(server: &TestServer, name: &str, pw: &str) -> (String, Uuid) {
        let body: serde_json::Value = server
            .post("/users/authenticatebyname")
            .add_header(
                http::header::AUTHORIZATION,
                HeaderValue::from_static(AUTH_HEADER),
            )
            .json(&json!({ "Username": name, "Pw": pw }))
            .await
            .json();
        (
            body["AccessToken"]
                .as_str()
                .unwrap()
                .to_string(),
            body["User"]["Id"]
                .as_str()
                .unwrap()
                .parse()
                .unwrap(),
        )
    }

    /// A server whose Simkl addon talks to `mock`, logged in as the admin.
    async fn simkl_server() -> (
        TestServer,
        crate::integration_test::TestGuard,
        MockServer,
        String,
        Uuid,
        Uuid,
    ) {
        let mock = MockServer::start();
        let (server, guard) = new_test_server_with_config(Config {
            database_url: Some("sqlite::memory:".into()),
            torrent_http_port: None,
            disable_dht: true,
            simkl_base_url: mock.base_url(),
            ..Default::default()
        })
        .await
        .unwrap();
        let (token, user_id) = login(&server, "test", "test").await;
        let (h, v) = auth(&token);
        let created: serde_json::Value = server
            .post("/addons")
            .add_header(h, v)
            .json(&json!({
                "preset": { "kind": "simkl", "config": { "client_id": "cid" } },
                "name": "Simkl",
                "resources": [],
            }))
            .await
            .json();
        let addon_id: Uuid = created["id"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap();
        (server, guard, mock, token, user_id, addon_id)
    }

    #[tokio::test]
    async fn a_user_connects_through_the_pin_flow_and_can_tune_and_drop_it() {
        let (server, _guard, mock, token, user_id, addon_id) = simkl_server().await;
        let (h, v) = auth(&token);
        let base = format!("/remux/users/{user_id}/mediatrackers");

        let listed: UserMediaTrackersDto = server
            .get(&base)
            .add_header(h.clone(), v.clone())
            .await
            .json();
        let provider = listed
            .providers
            .iter()
            .find(|p| p.addon_id == addon_id)
            .expect("the simkl addon is offered");
        assert_eq!(provider.kind, "simkl");
        assert_eq!(provider.auth_flow, "oauth_device_code");
        assert!(provider.history_import && provider.pulls_changes);
        assert!(
            provider
                .supported_events
                .contains(&"playback_stop".to_string())
        );
        assert!(
            listed
                .connections
                .is_empty()
        );

        // Step 1: a code to show.
        mock.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/oauth/pin")
                .query_param("client_id", "cid");
            then.status(200)
                .json_body(json!({
                    "result": "OK", "device_code": "x", "user_code": "5G6JAH",
                    "verification_uri": "https://simkl.com/pin", "expires_in": 900, "interval": 5
                }));
        });
        let start: MediaTrackerDeviceAuthDto = server
            .post(&format!("{base}/providers/{addon_id}/device"))
            .add_header(h.clone(), v.clone())
            .await
            .json();
        assert_eq!(start.user_code, "5G6JAH");
        assert_eq!(start.verification_url, "https://simkl.com/pin");
        assert_eq!(start.interval_secs, 5);

        // Step 2: not yet.
        let mut pending = mock.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/oauth/pin/5G6JAH");
            then.status(200)
                .json_body(json!({"result": "KO", "message": "Authorization pending"}));
        });
        let poll: MediaTrackerDevicePollDto = server
            .post(&format!("{base}/providers/{addon_id}/device/poll"))
            .add_header(h.clone(), v.clone())
            .json(&json!({ "pollToken": start.poll_token }))
            .await
            .json();
        assert_eq!(poll.status, "pending");
        assert!(
            poll.connection
                .is_none()
        );
        pending.delete();

        // A filter the provider cannot honour is refused before polling.
        server
            .post(&format!("{base}/providers/{addon_id}/device/poll"))
            .add_header(h.clone(), v.clone())
            .json(&json!({ "pollToken": start.poll_token, "eventFilters": ["mark_favorite"] }))
            .expect_failure()
            .await
            .assert_status(StatusCode::BAD_REQUEST);

        // Step 3: approved. The first import runs in the background.
        mock.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/oauth/pin/5G6JAH");
            then.status(200)
                .json_body(json!({"result": "OK", "access_token": "tok-1"}));
        });
        mock.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/users/settings")
                .header("authorization", "Bearer tok-1");
            then.status(200)
                .json_body(json!({"user": {"name": "alice"}, "account": {"id": 7}}));
        });
        mock.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/sync/all-items");
            then.status(200)
                .json_body(json!({}));
        });
        let poll: MediaTrackerDevicePollDto = server
            .post(&format!("{base}/providers/{addon_id}/device/poll"))
            .add_header(h.clone(), v.clone())
            .json(&json!({ "pollToken": start.poll_token }))
            .await
            .json();
        assert_eq!(poll.status, "approved");
        let conn = poll
            .connection
            .expect("an approved poll returns the connection");
        assert_eq!(conn.status, "connected");
        assert_eq!(
            conn.account_name
                .as_deref(),
            Some("alice")
        );
        assert_eq!(conn.event_filters, provider.default_event_filter);
        assert_eq!(conn.user_id, user_id);

        let listed: UserMediaTrackersDto = server
            .get(&base)
            .add_header(h.clone(), v.clone())
            .await
            .json();
        assert_eq!(
            listed
                .connections
                .len(),
            1
        );
        assert!(
            listed
                .connections
                .iter()
                .all(|c| c.id == conn.id),
            "reconnecting must not create a second row"
        );

        // Tuning the filter keeps the token.
        let updated: UserMediaTrackerDto = server
            .put(&format!("{base}/connections/{}", conn.id))
            .add_header(h.clone(), v.clone())
            .json(&json!({ "eventFilters": ["mark_played", "mark_played", "rating"] }))
            .await
            .json();
        assert_eq!(updated.event_filters, vec!["mark_played", "rating"]);
        server
            .put(&format!("{base}/connections/{}", conn.id))
            .add_header(h.clone(), v.clone())
            .json(&json!({ "eventFilters": ["teleport"] }))
            .expect_failure()
            .await
            .assert_status(StatusCode::BAD_REQUEST);

        // A pull can be asked for.
        let started = server
            .post(&format!("{base}/connections/{}/sync", conn.id))
            .add_header(h.clone(), v.clone())
            .json(&json!({ "full": true }))
            .await;
        started.assert_status(StatusCode::ACCEPTED);
        let started: MediaTrackerSyncStartedDto = started.json();
        assert!(started.started);

        // And the connection dropped.
        server
            .delete(&format!("{base}/connections/{}", conn.id))
            .add_header(h.clone(), v.clone())
            .await
            .assert_status(StatusCode::NO_CONTENT);
        let listed: UserMediaTrackersDto = server
            .get(&base)
            .add_header(h.clone(), v.clone())
            .await
            .json();
        assert!(
            listed
                .connections
                .is_empty()
        );
        server
            .delete(&format!("{base}/connections/{}", conn.id))
            .add_header(h, v)
            .expect_failure()
            .await
            .assert_status(StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn a_user_only_sees_and_touches_their_own_connections() {
        let (server, _guard, _mock, admin_token, admin_id, addon_id) =
            simkl_server().await;
        let (h, v) = auth(&admin_token);
        server
            .post("/users/new")
            .add_header(h.clone(), v.clone())
            .json(&json!({ "Name": "bob", "Password": "pw" }))
            .await;
        let (bob_token, bob_id) = login(&server, "bob", "pw").await;
        let (bh, bv) = auth(&bob_token);

        // Bob can list his own trackers, which offers the provider too.
        let mine: UserMediaTrackersDto = server
            .get(&format!("/remux/users/{bob_id}/mediatrackers"))
            .add_header(bh.clone(), bv.clone())
            .await
            .json();
        assert!(
            mine.providers
                .iter()
                .any(|p| p.addon_id == addon_id)
        );

        // But not the admin's.
        server
            .get(&format!("/remux/users/{admin_id}/mediatrackers"))
            .add_header(bh.clone(), bv.clone())
            .expect_failure()
            .await
            .assert_status(StatusCode::FORBIDDEN);

        // A connection stored for the admin is invisible to bob by id.
        let creds = crate::addons::media_tracker::MediaTrackerCredentials::new(
            json!({"access_token": "t"}),
        );
        let tracker = service::connect_tracker(
            &_guard.0,
            admin_id,
            addon_id,
            creds,
            vec![MediaTrackerEventKind::MarkPlayed],
        )
        .await
        .unwrap();
        server
            .delete(&format!(
                "/remux/users/{bob_id}/mediatrackers/connections/{}",
                tracker.id
            ))
            .add_header(bh, bv)
            .expect_failure()
            .await
            .assert_status(StatusCode::NOT_FOUND);

        // The admin can, on bob's behalf, and gets bob's list.
        let bobs: UserMediaTrackersDto = server
            .get(&format!("/remux/users/{bob_id}/mediatrackers"))
            .add_header(h.clone(), v.clone())
            .await
            .json();
        assert!(
            bobs.connections
                .is_empty()
        );
        server
            .post(&format!(
                "/remux/users/{bob_id}/mediatrackers/providers/{}/token",
                Uuid::nil()
            ))
            .add_header(h, v)
            .json(&json!({ "fields": {} }))
            .expect_failure()
            .await
            .assert_status(StatusCode::NOT_FOUND);
    }
}

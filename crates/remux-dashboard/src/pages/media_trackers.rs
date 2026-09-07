//! A user's media tracker connections (Simkl, …), shown inside the user
//! editor. Connecting goes through the provider's device code: the code is
//! shown here, the user enters it on the provider's site, and this panel
//! polls until it is approved.

use crate::{
    components::{ErrorAlert, LoadingText, Switch},
    state::AppState,
};
use dioxus::prelude::*;
use remux_sdks::remux::{
    AddonOptionType, BeginMediaTrackerDeviceAuth, ConnectMediaTrackerWithToken,
    DisconnectMediaTracker, GetUserMediaTrackers, MediaTrackerDeviceAuthDto,
    MediaTrackerDevicePollRequest, MediaTrackerProviderDto,
    MediaTrackerTokenConnectRequest, PollMediaTrackerDeviceAuth, SyncMediaTracker,
    UpdateMediaTracker, UpdateMediaTrackerRequest, UserMediaTrackerDto,
    UserMediaTrackersDto,
};
use std::{collections::HashMap, time::Duration};
use uuid::Uuid;

/// What each event kind means to a person, in the order the toggles show.
const EVENT_LABELS: &[(&str, &str)] = &[
    ("playback_start", "Scrobble when playback starts"),
    ("playback_progress", "Scrobble pauses"),
    (
        "playback_stop",
        "Scrobble when playback stops (marks watched)",
    ),
    ("mark_played", "Marking items watched"),
    ("mark_unplayed", "Marking items unwatched"),
    ("mark_favorite", "Adding favourites"),
    ("unmark_favorite", "Removing favourites"),
    ("rating", "Ratings"),
];

fn event_label(kind: &str) -> String {
    EVENT_LABELS
        .iter()
        .find(|(k, _)| *k == kind)
        .map(|(_, label)| (*label).to_string())
        .unwrap_or_else(|| kind.replace('_', " "))
}

fn short_time(dt: impl std::fmt::Display) -> String {
    crate::state::fmt_datetime(dt)
}

/// A device-code login in flight for one provider.
#[derive(Clone, PartialEq)]
struct PendingAuth {
    addon_id: Uuid,
    auth: MediaTrackerDeviceAuthDto,
}

#[component]
pub fn MediaTrackersPanel(app_state: AppState, user_id: Uuid) -> Element {
    let mut data: Signal<Option<UserMediaTrackersDto>> = use_signal(|| None);
    let mut error = use_signal(|| Option::<String>::None);
    let mut refresh = use_signal(|| 0_u32);
    let mut pending: Signal<Option<PendingAuth>> = use_signal(|| None);
    let mut auth_notice = use_signal(|| Option::<String>::None);

    let load_client = app_state.clone();
    use_effect(move || {
        let _r = *refresh.read();
        let client = load_client.clone();
        spawn(async move {
            match client
                .execute(GetUserMediaTrackers { user_id })
                .await
            {
                Ok(d) => {
                    data.set(Some(d));
                    error.set(None);
                }
                Err(e) => error.set(Some(e.user_message())),
            }
        });
    });

    // Poll the provider at its interval while a code is outstanding. The
    // loop ends when the code is approved, declined, expired, or the dialog
    // was closed (the pending value changed underneath it).
    let poll_client = app_state.clone();
    use_effect(move || {
        let Some(current) = pending
            .read()
            .clone()
        else {
            return;
        };
        let client = poll_client.clone();
        spawn(async move {
            let interval = current
                .auth
                .interval_secs
                .max(1);
            let attempts = current
                .auth
                .expires_in_secs
                / interval
                + 1;
            for _ in 0..attempts {
                gloo_timers::future::sleep(Duration::from_secs(interval)).await;
                if pending
                    .peek()
                    .as_ref()
                    != Some(&current)
                {
                    return;
                }
                let result = client
                    .execute(PollMediaTrackerDeviceAuth {
                        user_id,
                        addon_id: current.addon_id,
                        request: MediaTrackerDevicePollRequest {
                            poll_token: current
                                .auth
                                .poll_token
                                .clone(),
                            event_filters: None,
                        },
                    })
                    .await;
                match result {
                    Ok(r) if r.status == "approved" => {
                        pending.set(None);
                        auth_notice.set(None);
                        let v = *refresh.peek() + 1;
                        refresh.set(v);
                        return;
                    }
                    Ok(r) if r.status == "denied" => {
                        pending.set(None);
                        auth_notice.set(Some(
                            "The code expired or was declined. Start again.".into(),
                        ));
                        return;
                    }
                    Ok(_) => {}
                    Err(e) => {
                        pending.set(None);
                        auth_notice.set(Some(e.user_message()));
                        return;
                    }
                }
            }
            pending.set(None);
            auth_notice.set(Some(
                "The code expired before it was entered. Start again.".into(),
            ));
        });
    });

    let on_changed = move |_| {
        let v = *refresh.peek() + 1;
        refresh.set(v);
    };

    rsx! {
        div { class: "field",
            label { class: "field-label", "Media Trackers" }
            span { class: "field-hint",
                "Scrobble playback and keep watched state and ratings in sync with this user's account on a tracking service. Changes made on the service are pulled back hourly."
            }
            if let Some(err) = error.read().as_ref() {
                ErrorAlert { message: err.clone() }
            }
            if let Some(notice) = auth_notice.read().as_ref() {
                ErrorAlert { message: notice.clone() }
            }
            if data.read().is_none() {
                LoadingText {}
            } else if data.read().as_ref().map(|d| d.providers.is_empty()).unwrap_or(true) {
                span { class: "field-hint",
                    "No media tracker addon is installed. Add one (for example Simkl) on the Addons page first."
                }
            } else {
                div { style: "display:flex;flex-direction:column;gap:8px;margin-top:8px",
                    for provider in data.read().as_ref().map(|d| d.providers.clone()).unwrap_or_default() {
                        {
                            let connection = data.read().as_ref()
                                .and_then(|d| d.connections.iter().find(|c| c.addon_id == provider.addon_id).cloned());
                            let addon_id = provider.addon_id;
                            rsx! {
                                ProviderRow {
                                    key: "{addon_id}",
                                    app_state: app_state.clone(),
                                    user_id,
                                    provider,
                                    connection,
                                    on_changed,
                                    on_device_auth: move |auth: MediaTrackerDeviceAuthDto| {
                                        auth_notice.set(None);
                                        pending.set(Some(PendingAuth { addon_id, auth }));
                                    },
                                }
                            }
                        }
                    }
                }
            }
        }

        if let Some(current) = pending.read().clone() {
            div { class: "modal-backdrop",
                div { class: "modal", style: "max-width:420px;padding:24px", onclick: move |e| e.stop_propagation(),
                    p { class: "modal-title", "Connect your account" }
                    p { style: "margin:0 0 12px;font-size:.85rem",
                        "Open "
                        a { href: "{current.auth.verification_url}", target: "_blank", rel: "noopener", "{current.auth.verification_url}" }
                        " and enter this code:"
                    }
                    div { style: "font-size:2rem;font-weight:700;letter-spacing:.3em;text-align:center;margin:12px 0;font-family:monospace",
                        "{current.auth.user_code}"
                    }
                    p { class: "field-hint", style: "text-align:center",
                        "Waiting for approval… this dialog closes by itself once the code is accepted."
                    }
                    div { style: "display:flex;gap:8px;justify-content:flex-end;margin-top:16px",
                        button {
                            r#type: "button",
                            class: "btn btn-ghost",
                            style: "height:32px;font-size:.75rem",
                            onclick: move |_| pending.set(None),
                            "Cancel"
                        }
                    }
                }
            }
        }
    }
}

#[component]
fn ProviderRow(
    app_state: AppState,
    user_id: Uuid,
    provider: MediaTrackerProviderDto,
    connection: Option<UserMediaTrackerDto>,
    on_changed: EventHandler,
    on_device_auth: EventHandler<MediaTrackerDeviceAuthDto>,
) -> Element {
    let mut busy = use_signal(|| false);
    let mut row_error = use_signal(|| Option::<String>::None);
    let mut token_fields: Signal<HashMap<String, String>> = use_signal(HashMap::new);
    let mut sync_started = use_signal(|| false);

    let addon_id = provider.addon_id;
    let is_device = provider.auth_flow == "oauth_device_code";
    let is_token = provider.auth_flow == "token";
    let connected = connection
        .as_ref()
        .is_some_and(|c| c.status == "connected");
    let needs_reauth = connection
        .as_ref()
        .is_some_and(|c| c.status == "auth_expired");

    let status_text = match connection.as_ref() {
        None => "Not connected".to_string(),
        Some(c) => {
            let who = c
                .account_name
                .as_deref()
                .map(|n| format!(" as {n}"))
                .unwrap_or_default();
            match c
                .status
                .as_str()
            {
                "connected" => format!("Connected{who}"),
                "auth_expired" => format!("Reconnect needed{who}"),
                other => format!("{other}{who}"),
            }
        }
    };
    let status_color = match connection
        .as_ref()
        .map(|c| {
            c.status
                .as_str()
        }) {
        Some("connected") => "var(--success, #3c9)",
        Some(_) => "var(--error)",
        None => "var(--text-dim)",
    };

    let connect_device = {
        let client = app_state.clone();
        move |_| {
            if *busy.peek() {
                return;
            }
            busy.set(true);
            row_error.set(None);
            let client = client.clone();
            spawn(async move {
                match client
                    .execute(BeginMediaTrackerDeviceAuth { user_id, addon_id })
                    .await
                {
                    Ok(auth) => on_device_auth.call(auth),
                    Err(e) => row_error.set(Some(e.user_message())),
                }
                busy.set(false);
            });
        }
    };

    let connect_token = {
        let client = app_state.clone();
        move |_| {
            if *busy.peek() {
                return;
            }
            busy.set(true);
            row_error.set(None);
            let client = client.clone();
            let fields = serde_json::to_value(
                token_fields
                    .peek()
                    .clone(),
            )
            .unwrap_or_default();
            spawn(async move {
                match client
                    .execute(ConnectMediaTrackerWithToken {
                        user_id,
                        addon_id,
                        request: MediaTrackerTokenConnectRequest {
                            fields,
                            event_filters: None,
                        },
                    })
                    .await
                {
                    Ok(_) => {
                        token_fields.set(HashMap::new());
                        on_changed.call(());
                    }
                    Err(e) => row_error.set(Some(e.user_message())),
                }
                busy.set(false);
            });
        }
    };

    let connection_id = connection
        .as_ref()
        .map(|c| c.id);

    let disconnect = {
        let client = app_state.clone();
        move |_| {
            let Some(id) = connection_id else {
                return;
            };
            if *busy.peek() {
                return;
            }
            busy.set(true);
            row_error.set(None);
            let client = client.clone();
            spawn(async move {
                match client
                    .execute(DisconnectMediaTracker { user_id, id })
                    .await
                {
                    Ok(_) => on_changed.call(()),
                    Err(e) => row_error.set(Some(e.user_message())),
                }
                busy.set(false);
            });
        }
    };

    let sync_now = {
        let client = app_state.clone();
        move |_| {
            let Some(id) = connection_id else {
                return;
            };
            if *busy.peek() {
                return;
            }
            busy.set(true);
            row_error.set(None);
            let client = client.clone();
            spawn(async move {
                match client
                    .execute(SyncMediaTracker {
                        user_id,
                        id,
                        full: false,
                    })
                    .await
                {
                    Ok(_) => sync_started.set(true),
                    Err(e) => row_error.set(Some(e.user_message())),
                }
                busy.set(false);
            });
        }
    };

    let current_filters: Vec<String> = connection
        .as_ref()
        .map(|c| {
            c.event_filters
                .clone()
        })
        .unwrap_or_default();

    let toggle_event = {
        let client = app_state.clone();
        let filters = current_filters.clone();
        move |(kind, enabled): (String, bool)| {
            let Some(id) = connection_id else {
                return;
            };
            let mut next = filters.clone();
            next.retain(|k| *k != kind);
            if enabled {
                next.push(kind);
            }
            let client = client.clone();
            row_error.set(None);
            spawn(async move {
                match client
                    .execute(UpdateMediaTracker {
                        user_id,
                        id,
                        request: UpdateMediaTrackerRequest {
                            event_filters: next,
                        },
                    })
                    .await
                {
                    Ok(_) => on_changed.call(()),
                    Err(e) => row_error.set(Some(e.user_message())),
                }
            });
        }
    };

    rsx! {
        div { style: "border:1px solid var(--border);border-radius:8px;padding:10px 12px;display:flex;flex-direction:column;gap:8px",
            div { style: "display:flex;align-items:center;gap:8px;flex-wrap:wrap",
                span { style: "font-size:.85rem;font-weight:600", "{provider.name}" }
                span { class: "addon-card-kind", style: "font-size:.6rem", "{provider.kind}" }
                span { style: "font-size:.72rem;color:{status_color};margin-left:auto", "{status_text}" }
            }

            if let Some(c) = connection.as_ref() {
                div { style: "display:flex;gap:12px;flex-wrap:wrap;font-size:.68rem;color:var(--text-dim)",
                    if let Some(at) = c.last_pull_at {
                        span { "Last pull: {short_time(at)}" }
                    } else {
                        span { "Not pulled yet" }
                    }
                    if let Some(at) = c.last_success_at {
                        span { "Last success: {short_time(at)}" }
                    }
                    if let Some(err) = c.last_error.as_ref() {
                        span { style: "color:var(--error)", "Last error: {err}" }
                    }
                }
            }

            if is_token && !connected {
                div { style: "display:flex;flex-direction:column;gap:6px",
                    for field in provider.connect_fields.clone() {
                        {
                            let fid = field.id.clone();
                            let input_type = match field.kind {
                                AddonOptionType::Password => "password",
                                _ => "text",
                            };
                            let value = token_fields.read().get(&fid).cloned().unwrap_or_default();
                            rsx! {
                                div { class: "field", key: "{fid}",
                                    label { class: "field-label", "{field.name}" }
                                    input {
                                        r#type: input_type,
                                        class: "field-input",
                                        value: "{value}",
                                        oninput: move |e| {
                                            token_fields.write().insert(fid.clone(), e.value());
                                        },
                                    }
                                    if let Some(desc) = field.description.as_ref() {
                                        span { class: "field-hint", "{desc}" }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            if connected {
                div { style: "display:flex;flex-direction:column;gap:2px",
                    for kind in provider.supported_events.clone() {
                        {
                            let enabled = current_filters.contains(&kind);
                            let kind_for_toggle = kind.clone();
                            let mut toggle = toggle_event.clone();
                            rsx! {
                                div { class: "toggle-row", key: "{kind}",
                                    div { class: "toggle-row-text",
                                        span { class: "toggle-label", style: "font-size:.78rem", "{event_label(&kind)}" }
                                    }
                                    Switch {
                                        checked: enabled,
                                        on_change: move |v: bool| toggle((kind_for_toggle.clone(), v)),
                                    }
                                }
                            }
                        }
                    }
                }
            }

            if let Some(err) = row_error.read().as_ref() {
                ErrorAlert { message: err.clone() }
            }
            if *sync_started.read() {
                span { class: "field-hint", "Pull started. Refresh in a minute to see the result." }
            }

            div { style: "display:flex;gap:8px;justify-content:flex-end;flex-wrap:wrap",
                if connected && provider.pulls_changes {
                    button {
                        r#type: "button",
                        class: "btn btn-ghost",
                        style: "height:30px;font-size:.68rem;padding:0 10px",
                        disabled: *busy.read(),
                        onclick: sync_now,
                        "Sync now"
                    }
                }
                if is_device {
                    button {
                        r#type: "button",
                        class: if connected { "btn btn-ghost" } else { "btn btn-primary" },
                        style: "height:30px;font-size:.68rem;padding:0 10px",
                        disabled: *busy.read(),
                        onclick: connect_device,
                        if needs_reauth { "Reconnect" } else if connected { "Connect another account" } else { "Connect" }
                    }
                }
                if is_token && !connected {
                    button {
                        r#type: "button",
                        class: "btn btn-primary",
                        style: "height:30px;font-size:.68rem;padding:0 10px",
                        disabled: *busy.read(),
                        onclick: connect_token,
                        if needs_reauth { "Reconnect" } else { "Connect" }
                    }
                }
                if connection.is_some() {
                    button {
                        r#type: "button",
                        class: "btn btn-ghost",
                        style: "height:30px;font-size:.68rem;padding:0 10px;color:var(--error);border-color:var(--error)",
                        disabled: *busy.read(),
                        onclick: disconnect,
                        "Disconnect"
                    }
                }
            }
        }
    }
}

//! The one component tree: the header, the account bar, the skeleton and the
//! shared bits of the panes. It is platform-free, and reaches the platform
//! only through the contexts the platform's app provides.

mod notes;
mod orders;
mod photos;
mod retention;
mod your_data;

#[cfg(all(feature = "peer", not(target_arch = "wasm32")))]
pub(crate) mod peer;
#[cfg(all(feature = "peer", not(target_arch = "wasm32")))]
pub(crate) use peer::PeerPanel;

pub use notes::NotesPane;
pub use orders::OrdersPane;
pub use photos::PhotosPane;
pub use retention::RetentionPane;
pub use your_data::YourDataPane;

use connetto_client::AccountChoice;
use connetto_demo_support as support;
use dioxus::prelude::*;
use dioxus_free_icons::Icon;
use dioxus_free_icons::icons::fa_solid_icons::{
    FaArrowRightArrowLeft, FaCheck, FaCircleUser, FaFingerprint, FaRightFromBracket,
    FaRightToBracket, FaTrashCan, FaUserPlus, FaXmark,
};

use crate::platform::{Client, DemoContent, Ops};

/// The app's own status line, set by the platform's event loop.
#[derive(Clone, Copy)]
pub struct Status(pub Signal<String>);

/// The signed-in user id the bar shows and every write names as owner.
#[derive(Clone, Copy)]
pub struct UserId(pub Signal<Option<String>>);

/// The credential-store key of the account signed in, or `None` while the
/// session is landing.
#[derive(Clone, Copy)]
pub struct AccountKey(pub Signal<Option<String>>);

/// Every stored account as (key, decoded label, signed in right now).
#[derive(Clone, Copy)]
pub struct Accounts(pub Signal<Vec<(String, String, bool)>>);

/// Whether the account picker is waiting for a click.
#[derive(Clone, Copy)]
pub struct PickerActive(pub Signal<bool>);

/// The choice the picker made; consumed by the platform.
#[derive(Clone, Copy)]
pub struct AccountAnswer(pub Signal<Option<AccountChoice>>);

/// The one line the header shows about key custody, absent until the platform
/// knows.
#[derive(Clone, Copy)]
pub struct CustodyLine(pub Signal<Option<String>>);

/// The header's gate line, the standing of the gate that guards the secrets,
/// absent until the platform knows it.
#[derive(Clone, Copy)]
pub struct GateLine(pub Signal<Option<String>>);

/// The warning shown while the session nears expiry with local work unsent.
#[derive(Clone, Copy)]
pub struct ExpiryWarn(pub Signal<Option<String>>);

/// The client the panes drive, set once the platform's boot has attached.
#[derive(Clone, Copy)]
pub struct ClientSlot(pub Signal<Option<Client>>);

/// The content lane the photos pane drives, set with the client.
#[derive(Clone, Copy)]
pub struct ContentSlot(pub Signal<Option<DemoContent>>);

/// The replica's page footprint, (total pages, free pages).
#[derive(Clone, Copy)]
pub struct Footprint(pub Signal<(i64, i64)>);

/// Whether the sign-in button can start a login right now.
#[derive(Clone, Copy)]
pub struct SignInReady(pub Signal<bool>);

/// The stored-secrets gate standing, which the header shows beside the
/// status.
#[derive(Clone, Copy)]
pub struct Gate(pub Signal<&'static str>);

/// The line a clock outside the server's window earns, replacing the device
/// line while it holds.
#[derive(Clone, Copy)]
pub struct Clock(pub Signal<Option<&'static str>>);

/// The device line, the identity and certificate standing on a native build
/// and the no-device-identity statement on the web.
#[derive(Clone, Copy)]
pub struct DeviceLine(pub Signal<String>);

/// The app name, and the status, gate, device, custody and expiry lines,
/// each as the platform's own functions report them.
#[component]
fn Header() -> Element {
    let Status(status) = use_context::<Status>();
    let GateLine(gate_line) = use_context::<GateLine>();
    let DeviceLine(device) = use_context::<DeviceLine>();
    let CustodyLine(custody) = use_context::<CustodyLine>();
    let ExpiryWarn(expiry) = use_context::<ExpiryWarn>();
    let Gate(gate) = use_context::<Gate>();
    let Clock(clock) = use_context::<Clock>();
    let ops = use_context::<Ops>();

    let device_now = match clock() {
        Some(clock) => clock.to_owned(),
        None => device().clone(),
    };

    let unlock: Option<Element> = if ops.offers_unlock() && gate() != "open" {
        let ops = ops.clone();
        Some(rsx! {
            button {
                onclick: move |_| {
                    let ops = ops.clone();
                    spawn(async move { ops.unlock().await });
                },
                Icon { icon: FaFingerprint, width: 14, height: 14 }
                "Unlock"
            }
        })
    } else {
        None
    };

    rsx! {
        header {
            h1 { "connetto demo" }
            p { class: "status", "status: " {status.read().clone()} }
            if let Some(line) = gate_line.read().clone() {
                p { class: "status", {line} }
            }
            p { class: "status", "device: {device_now}" }
            if let Some(extra) = unlock {
                {extra}
            }
            if let Some(line) = custody.read().clone() {
                p { class: "status", {line} }
            }
            if let Some(warn) = expiry.read().clone() {
                p { class: "warn", {warn} }
            }
        }
    }
}

/// The signed-in user, the account picker, the switch and add account
/// buttons, the passkey enrol where the platform offers it, and the logout
/// controls.
#[component]
fn AccountBar() -> Element {
    let UserId(user_id) = use_context::<UserId>();
    let AccountKey(account_key) = use_context::<AccountKey>();
    let Accounts(accounts) = use_context::<Accounts>();
    let PickerActive(picker_active) = use_context::<PickerActive>();
    let AccountAnswer(account_answer) = use_context::<AccountAnswer>();
    let ops = use_context::<Ops>();
    let switch_ops = ops.clone();
    let add_ops = ops.clone();

    let SignInReady(sign_in_ready) = use_context::<SignInReady>();
    let EnrolAvailable(offerable) = use_context::<EnrolAvailable>();
    let EnrolMsg(enrol_msg) = use_context::<EnrolMsg>();
    let sign_in_ops = ops.clone();

    let uid = user_id.read().clone().unwrap_or_default();
    let signed_in = !uid.is_empty();
    let account = account_key.read().clone();
    let dup = account
        .as_deref()
        .map(support::account_label)
        .filter(|label| *label == uid);
    let accounts_now = accounts.read().clone();
    let account_tag: Option<String> = account
        .as_deref()
        .filter(|_| dup.is_none())
        .map(support::account_label);
    let picker_now = *picker_active.read();

    let picker_html: Element = if picker_now {
        rsx! {
            div {
                p { "Choose an account to sign in as:" }
                div { class: "row",
                    for (acc_key, acc_label, is_current) in accounts_now.clone() {
                        button {
                            key: "{acc_key}",
                            onclick: {
                                let mut answer = account_answer;
                                move |_| {
                                    answer.set(Some(AccountChoice::Account(acc_key.clone())));
                                }
                            },
                            Icon { icon: FaRightToBracket, width: 14, height: 14 }
                            "Sign in as {acc_label}"
                            if is_current { " (current)" }
                        }
                    }
                    button {
                        onclick: {
                            let mut answer = account_answer;
                            move |_| answer.set(Some(AccountChoice::New))
                        },
                        Icon { icon: FaUserPlus, width: 14, height: 14 }
                        "Sign in as a new account"
                    }
                }
            }
        }
    } else {
        rsx! {}
    };
    let enrol_html: Option<Element> = if *offerable.read() {
        let msg = enrol_msg.read().clone();
        let enrol_ops = sign_in_ops.clone();
        Some(rsx! {
            button {
                onclick: {
                    let ops = enrol_ops.clone();
                    move |_| {
                        let ops = ops.clone();
                        spawn(async move { ops.enrol_gate().await });
                    }
                },
                Icon { icon: FaFingerprint, width: 14, height: 14 }
                "Enrol passkey gate"
            }
            if let Some(msg) = msg {
                p { class: "error", {msg} }
            }
        })
    } else {
        None
    };

    if signed_in {
        rsx! {
            section { class: "auth-banner",
                h2 {
                    Icon { icon: FaCircleUser, width: 14, height: 14 }
                    "Account"
                }
                p {
                    "Signed in as "
                    code { class: "id", title: uid.clone(), {support::short_id(&uid)} }
                    if let Some(label) = account_tag.clone() {
                        " (account: "
                        code { class: "id", title: label.clone(), {support::short_id(&label)} }
                        ")"
                    }
                }
                div { class: "row",
                    for (acc_key, acc_label, _) in accounts_now.iter().filter(|(_, _, current)| !*current) {
                        button {
                            key: "{acc_key}",
                            onclick: {
                                let ops = switch_ops.clone();
                                let key = acc_key.clone();
                                move |_| {
                                    let ops = ops.clone();
                                    let key = key.clone();
                                    spawn(async move { ops.switch_account(&key).await });
                                }
                            },
                            Icon { icon: FaArrowRightArrowLeft, width: 14, height: 14 }
                            "Switch to {acc_label}"
                        }
                    }
                    button {
                        onclick: {
                            let ops = add_ops.clone();
                            move |_| {
                                let ops = ops.clone();
                                spawn(async move { ops.add_account().await });
                            }
                        },
                        Icon { icon: FaUserPlus, width: 14, height: 14 }
                        "Add another account"
                    }
                    if let Some(extra) = enrol_html {
                        {extra}
                    }
                }
                LogoutControls {}
                { picker_html }
            }
        }
    } else {
        let ready = *sign_in_ready.read();
        let sign_ops = sign_in_ops.clone();
        rsx! {
            section { class: "auth-banner",
                h2 {
                    Icon { icon: FaCircleUser, width: 14, height: 14 }
                    "Account"
                }
                p { "Sign in to begin." }
                div { class: "row",
                    button {
                        class: "primary",
                        disabled: !ready,
                        onclick: {
                            let ops = sign_ops.clone();
                            move |_| {
                                let ops = ops.clone();
                                spawn(async move { ops.sign_in().await });
                            }
                        },
                        Icon { icon: FaRightToBracket, width: 14, height: 14 }
                        "Sign in"
                    }
                }
            }
        }
    }
}

/// The logout controls, one set on every platform. Keeping the local data
/// revokes the session and leaves the replica and its key, so a returning
/// user resumes from the persisted cursor. Deleting wipes the replica and
/// asks for confirmation while local writes are still unsent.
#[component]
fn LogoutControls() -> Element {
    let ops = use_context::<Ops>();
    let keep_ops = ops.clone();
    let delete_ops = ops.clone();
    let confirm_ops = ops.clone();
    let cancel_ops = ops.clone();
    let dismiss_ops = ops.clone();
    let state = ops.logout_state();
    let refused = ops.refused_pending();

    let on_keep = move |_| {
        let ops = keep_ops.clone();
        spawn(async move { ops.logout_keep().await });
    };
    let on_delete = move |_| {
        let ops = delete_ops.clone();
        spawn(async move { ops.logout_delete().await });
    };
    let on_confirm = move |_| {
        let ops = confirm_ops.clone();
        spawn(async move { ops.logout_delete_confirmed().await });
    };
    let on_cancel = move |_| {
        let ops = cancel_ops.clone();
        spawn(async move { ops.dismiss().await });
    };
    let on_dismiss = move |_| {
        let ops = dismiss_ops.clone();
        spawn(async move { ops.dismiss().await });
    };

    if let Some(pending) = *refused.read() {
        rsx! {
            div { class: "confirm",
                p { {support::logout_refused_line(pending)} }
                div { class: "row",
                    button {
                        class: "danger",
                        onclick: on_confirm,
                        Icon { icon: FaTrashCan, width: 14, height: 14 }
                        "Force delete"
                    }
                    button {
                        onclick: on_dismiss,
                        Icon { icon: FaXmark, width: 14, height: 14 }
                        "Cancel"
                    }
                }
            }
        }
    } else {
        match state.read().clone() {
            support::LogoutState::Idle => rsx! {
                div { class: "row",
                    button {
                        onclick: on_keep,
                        Icon { icon: FaRightFromBracket, width: 14, height: 14 }
                        "Log out, keep local data"
                    }
                    button {
                        class: "danger",
                        onclick: on_delete,
                        Icon { icon: FaTrashCan, width: 14, height: 14 }
                        "Delete local data and log out"
                    }
                }
            },
            support::LogoutState::ConfirmDelete { unsynced_count } => rsx! {
                div { class: "confirm",
                    p { {support::logout_confirm_line(unsynced_count)} }
                    div { class: "row",
                        button {
                            class: "danger",
                            onclick: on_confirm,
                            Icon { icon: FaCheck, width: 14, height: 14 }
                            "Confirm: delete and log out"
                        }
                        button {
                            onclick: on_cancel,
                            Icon { icon: FaXmark, width: 14, height: 14 }
                            "Cancel"
                        }
                    }
                }
            },
            support::LogoutState::Working => rsx! { p { "Working..." } },
            support::LogoutState::Error(msg) => rsx! {
                p { class: "error", "Error: {msg}" }
                button {
                    onclick: on_dismiss,
                    Icon { icon: FaXmark, width: 14, height: 14 }
                    "Dismiss"
                }
            },
        }
    }
}

/// The enrol passkey gate's offerability and outcome message.
#[derive(Clone, Copy)]
pub struct EnrolAvailable(pub(crate) Signal<bool>);

#[derive(Clone, Copy)]
pub struct EnrolMsg(pub(crate) Signal<Option<String>>);

/// The pane layout shown while a session is still landing; the panes replace
/// it once the client attaches.
#[component]
fn PanesSkeleton() -> Element {
    rsx! {
        div { class: "panes",
            div { class: "pane",
                h2 { {support::ORDERS_PANE_TITLE} " " span { class: "badge synced", {support::ORDERS_BADGE} } }
                p { {support::orders_readout(0, 0)} }
                table {
                    thead { tr { th { "id" } th { "quantity" } } }
                    tbody {}
                }
            }
            div { class: "pane",
                h2 { {support::PHOTOS_PANE_TITLE} " " span { class: "badge synced", {support::PHOTOS_BADGE} } }
                p { {support::photos_readout(0)} }
                table {
                    thead { tr { th { "id" } th { "state" } th { "image" } } }
                    tbody {}
                }
            }
            div { class: "pane",
                h2 { {support::NOTES_PANE_TITLE} " " span { class: "badge local", {support::NOTES_BADGE} } }
                p { {support::notes_readout(0)} }
                table {
                    thead { tr { th { "id" } th { "body" } } }
                    tbody {}
                }
            }
            div { class: "pane",
                h2 { {support::RETENTION_PANE_TITLE} " " span { class: "badge trim", {support::RETENTION_BADGE} } }
                p { {support::retention_readout(0, 0, 0)} }
                p { {support::RETENTION_EXPLAINER} }
            }
            div { class: "pane",
                h2 { {support::YOUR_DATA_PANE_TITLE} " " span { class: "badge local", {support::YOUR_DATA_BADGE} } }
                p { {support::YOUR_DATA_INTRO} }
            }
        }
    }
}

/// The five panes, mounted once the client is ready.
#[component]
fn Panes() -> Element {
    rsx! {
        div { class: "panes",
            OrdersPane {}
            PhotosPane {}
            NotesPane {}
            RetentionPane {}
            YourDataPane {}
        }
    }
}

/// The whole page: the stylesheet, the header, the account bar and the panes
/// or their skeleton.
#[component]
pub fn Page(ready: bool, restoring: bool, peer: Option<Element>) -> Element {
    rsx! {
        style { {support::CSS} }
        div { class: "wrap",
            Header {}
            AccountBar {}
            if let Some(peer) = peer {
                {peer}
            }
            if ready {
                Panes {}
            } else if restoring {
                PanesSkeleton {}
            } else {
                p { class: "status", "Connecting to the connetto stack..." }
            }
        }
    }
}

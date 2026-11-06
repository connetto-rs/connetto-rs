//! The screen. Every component reads the one view signal and sends its
//! control through [`act`]. None of them touches the connetto client.

use connetto_demo_support as support;
use leptos::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::{JsFuture, spawn_local};
use web_sys::HtmlInputElement;

use crate::icons::{Fa, Icon};
use crate::platform::act;
use connetto_demo_support::view::{Action, LogoutState, Session, View};

/// The whole page with the stylesheet, the header, the account bar, and the
/// panes or their skeleton.
#[component]
pub fn App(view: RwSignal<View>) -> impl IntoView {
    view! {
        <style>{support::CSS}</style>
        <div class="wrap">
            <Header view=view />
            <AccountBar view=view />
            {move || view.with(|v| v.nearby.is_some()).then(|| view! { <NearbyPane view=view /> })}
            {move || view.with(|v| v.team.is_some()).then(|| view! { <TeamNotesPane view=view /> })}
            <Dashboard view=view />
        </div>
    }
}

/// A status paragraph for an optional line, nothing while it is absent.
fn status_line(line: Option<String>) -> Option<impl IntoView> {
    line.map(|line| view! { <p class="status">{line}</p> })
}

/// A short id in a `code` tag, the full value in its tooltip.
fn short_code(id: String) -> impl IntoView {
    let short = support::short_id(&id).to_owned();
    view! { <code class="id" title=id>{short}</code> }
}

/// The app's name in its header, the same `connetto demo` the native dioxus
/// demo shows in the Tauri app.
#[cfg(feature = "tauri")]
const APP_TITLE: &str = "connetto demo";
/// The app's name in its header.
#[cfg(not(feature = "tauri"))]
const APP_TITLE: &str = "connetto web demo (Leptos)";

/// The app name, and the status, gate, device, storage, custody and expiry
/// lines.
#[component]
fn Header(view: RwSignal<View>) -> impl IntoView {
    view! {
        <header>
            <h1>{APP_TITLE}</h1>
            <p class="status">"status: " {move || view.with(|v| v.status.clone())}</p>
            {move || status_line(view.with(|v| v.gate_line.clone()))}
            <p class="status">"device: " {move || view.with(|v| v.device_line.clone())}</p>
            <p class="status">{move || view.with(|v| v.storage.clone())}</p>
            <Show when=move || view.with(|v| v.offers_unlock)>
                <button on:click=move |_| act(Action::Unlock)>
                    <Fa icon=Icon::Fingerprint />
                    "Unlock"
                </button>
            </Show>
            {move || status_line(view.with(|v| v.custody_line.clone()))}
            {move || {
                view.with(|v| v.expiry_warning.clone())
                    .map(|warn| view! { <p class="warn">{warn}</p> })
            }}
        </header>
    }
}

/// The account bar with the signed-in controls, the sign-in banner, the
/// restore line while a stored session lands, or the account picker.
#[component]
fn AccountBar(view: RwSignal<View>) -> impl IntoView {
    let session = move || view.with(|v| v.session);
    view! {
        <section class="auth-banner">
            <h2>
                <Fa icon=Icon::CircleUser />
                "Account"
            </h2>
            {move || match session() {
                Session::SignedIn => view! { <SignedIn view=view /> }.into_any(),
                Session::Restoring => view! { <Restoring view=view /> }.into_any(),
                Session::PickingAccount => view! { <Picker view=view /> }.into_any(),
                Session::Connecting | Session::SignInOffered => {
                    view! { <SignIn view=view /> }.into_any()
                }
            }}
        </section>
    }
}

/// The sign-in banner, its button inert until the worker parks a login URL.
#[component]
fn SignIn(view: RwSignal<View>) -> impl IntoView {
    let offered = move || view.with(|v| v.session == Session::SignInOffered);
    view! {
        <p>"Sign in to begin."</p>
        <div class="row">
            <button class="primary" disabled=move || !offered() on:click=move |_| act(Action::SignIn)>
                <Fa icon=Icon::RightToBracket />
                "Sign in"
            </button>
        </div>
    }
}

/// The stored account a restoring page is signing back in as.
#[component]
fn Restoring(view: RwSignal<View>) -> impl IntoView {
    let label = move || {
        view.with(|v| v.stored_account.as_ref().map(|entry| entry.label.clone()))
            .unwrap_or_default()
    };
    view! {
        <p>"Signed in as " {move || short_code(label())}</p>
    }
}

/// The account picker the worker parks while it awaits a choice.
#[component]
fn Picker(view: RwSignal<View>) -> impl IntoView {
    let accounts = move || view.with(|v| v.picking.clone().unwrap_or_default());
    view! {
        <div>
            <p>"Choose an account to sign in as:"</p>
            <div class="row">
                {move || {
                    accounts()
                        .into_iter()
                        .map(|entry| {
                            let (key, label) = (entry.key, entry.label);
                            view! {
                                <button on:click=move |_| {
                                    act(Action::ChooseAccount {
                                        account: Some(key.clone()),
                                    })
                                }>
                                    <Fa icon=Icon::RightToBracket />
                                    "Sign in as " {label}
                                </button>
                            }
                        })
                        .collect_view()
                }}
                <button on:click=move |_| act(Action::ChooseAccount { account: None })>
                    <Fa icon=Icon::UserPlus />
                    "Sign in as a new account"
                </button>
            </div>
        </div>
    }
}

/// The signed-in identity, the account switches, the enrol offer and the
/// logout controls.
#[component]
fn SignedIn(view: RwSignal<View>) -> impl IntoView {
    let uid = move || view.with(|v| v.identity.clone().unwrap_or_default());
    let account_tag = move || {
        view.with(|v| {
            v.account
                .as_ref()
                .map(|entry| entry.label.clone())
                .filter(|label| Some(label) != v.identity.as_ref())
        })
    };
    let switches = move || {
        view.with(|v| {
            v.known_accounts
                .iter()
                .filter(|entry| v.account.as_ref().map(|live| &live.key) != Some(&entry.key))
                .map(|entry| (entry.key.clone(), entry.label.clone()))
                .collect::<Vec<_>>()
        })
    };
    view! {
        <p>
            "Signed in as "
            {move || short_code(uid())}
            {move || {
                account_tag().map(|label| view! { " (account: " {short_code(label)} ")" })
            }}
        </p>
        <div class="row">
            {move || {
                switches()
                    .into_iter()
                    .map(|(key, label)| {
                        view! {
                            <button on:click=move |_| {
                                act(Action::SwitchAccount {
                                    account: key.clone(),
                                })
                            }>
                                <Fa icon=Icon::ArrowRightArrowLeft />
                                "Switch to " {label}
                            </button>
                        }
                    })
                    .collect_view()
            }}
            <button on:click=move |_| act(Action::AddAccount)>
                <Fa icon=Icon::UserPlus />
                "Add another account"
            </button>
            <Show when=move || view.with(|v| v.enrol_offer)>
                <button on:click=move |_| act(Action::EnrolGate)>
                    <Fa icon=Icon::Fingerprint />
                    "Enrol passkey gate"
                </button>
            </Show>
        </div>
        <LogoutControls view=view />
    }
}

/// The logout controls. Keeping the local data revokes the session and
/// leaves the replica and its key. Deleting wipes the replica, and asks for
/// confirmation while local writes are still unsent or after the worker
/// refused because writes arrived meanwhile.
#[component]
fn LogoutControls(view: RwSignal<View>) -> impl IntoView {
    move || {
        let (logout, refused) = view.with(|v| (v.logout.clone(), v.refused_pending));
        if let Some(pending) = refused {
            return view! {
                <div class="confirm">
                    <p>{support::logout_refused_line(pending)}</p>
                    <div class="row">
                        <button class="danger" on:click=move |_| act(Action::ForceDelete)>
                            <Fa icon=Icon::TrashCan />
                            "Force delete"
                        </button>
                        <button on:click=move |_| act(Action::CancelLogout)>
                            <Fa icon=Icon::Xmark />
                            "Cancel"
                        </button>
                    </div>
                </div>
            }
            .into_any();
        }
        match logout {
            LogoutState::Idle => view! {
                <div class="row">
                    <button on:click=move |_| act(Action::LogOutKeep)>
                        <Fa icon=Icon::RightFromBracket />
                        "Log out, keep local data"
                    </button>
                    <button class="danger" on:click=move |_| act(Action::LogOutDelete)>
                        <Fa icon=Icon::TrashCan />
                        "Delete local data and log out"
                    </button>
                </div>
            }
            .into_any(),
            LogoutState::ConfirmDelete { unsynced_count } => view! {
                <div class="confirm">
                    <p>{support::logout_confirm_line(unsynced_count)}</p>
                    <div class="row">
                        <button class="danger" on:click=move |_| act(Action::ConfirmDelete)>
                            <Fa icon=Icon::Check />
                            "Confirm: delete and log out"
                        </button>
                        <button on:click=move |_| act(Action::CancelLogout)>
                            <Fa icon=Icon::Xmark />
                            "Cancel"
                        </button>
                    </div>
                </div>
            }
            .into_any(),
            LogoutState::Working => view! { <p>"Working..."</p> }.into_any(),
            LogoutState::Error(message) => view! {
                <p class="error">"Error: " {message}</p>
                <button on:click=move |_| act(Action::CancelLogout)>
                    <Fa icon=Icon::Xmark />
                    "Dismiss"
                </button>
            }
            .into_any(),
        }
    }
}

/// The six panes once the tab attached, the skeleton while a stored session
/// lands, or the connecting line before either.
#[component]
fn Dashboard(view: RwSignal<View>) -> impl IntoView {
    move || {
        let (attached, restoring) = view.with(|v| (v.attached, v.stored_account.is_some()));
        if attached {
            view! {
                <div class="panes">
                    <OrdersPane view=view />
                    <PhotosPane view=view />
                    <ProductsPane view=view />
                    <NotesPane view=view />
                    <RetentionPane view=view />
                    <YourDataPane view=view />
                </div>
            }
            .into_any()
        } else if restoring {
            view! { <PanesSkeleton /> }.into_any()
        } else {
            view! { <p class="status">"Connecting to the connetto stack..."</p> }.into_any()
        }
    }
}

/// A pane heading with its icon, its title and its badge.
fn pane_heading(
    icon: Icon,
    title: &'static str,
    badge_class: &'static str,
    badge: &'static str,
) -> impl IntoView {
    view! {
        <h2>
            <Fa icon=icon />
            {title}
            " "
            <span class=badge_class>{badge}</span>
        </h2>
    }
}

/// A pane footer with the info icon and the pane's purpose line.
#[component]
fn PaneInfo(text: &'static str) -> impl IntoView {
    view! {
        <p class="pane-info">
            <Fa icon=Icon::CircleInfo size=12 />
            <span>{text}</span>
        </p>
    }
}

/// The pane layout shown while a stored session is still landing.
#[component]
fn PanesSkeleton() -> impl IntoView {
    view! {
        <div class="panes">
            <div class="pane toned tone-orders">
                {pane_heading(Icon::CartShopping, support::ORDERS_PANE_TITLE, "badge synced", support::ORDERS_BADGE)}
                <p>{support::orders_readout(0, 0)}</p>
                <table>
                    <thead><tr><th>"id"</th><th>"quantity"</th></tr></thead>
                    <tbody></tbody>
                </table>
                <PaneInfo text=support::ORDERS_INFO />
            </div>
            <div class="pane toned tone-photos">
                {pane_heading(Icon::Images, support::PHOTOS_PANE_TITLE, "badge synced", support::PHOTOS_BADGE)}
                <p>{support::photos_readout(0)}</p>
                <table>
                    <thead><tr><th>"id"</th><th>"state"</th><th>"image"</th></tr></thead>
                    <tbody></tbody>
                </table>
                <PaneInfo text=support::PHOTOS_INFO />
            </div>
            <div class="pane toned tone-catalogue">
                {pane_heading(Icon::Tags, support::CATALOGUE_HEADING, "badge shared", "shared")}
                <p>{support::CATALOGUE_INTRO}</p>
                <table>
                    <thead><tr><th>"name"</th><th>"price"</th></tr></thead>
                    <tbody></tbody>
                </table>
                <PaneInfo text=support::CATALOGUE_INFO />
            </div>
            <div class="pane toned tone-notes">
                {pane_heading(Icon::NoteSticky, support::NOTES_PANE_TITLE, "badge local", support::NOTES_BADGE)}
                <p>{support::notes_readout(0)}</p>
                <table>
                    <thead><tr><th>"id"</th><th>"body"</th></tr></thead>
                    <tbody></tbody>
                </table>
                <PaneInfo text=support::NOTES_INFO />
            </div>
            <div class="pane toned tone-retention">
                {pane_heading(Icon::Broom, support::RETENTION_PANE_TITLE, "badge trim", support::RETENTION_BADGE)}
                <p>{support::retention_readout(0, 0, 0)}</p>
                <p>{support::RETENTION_EXPLAINER}</p>
                <PaneInfo text=support::RETENTION_INFO />
            </div>
            <div class="pane toned tone-your-data">
                {pane_heading(Icon::Database, support::YOUR_DATA_PANE_TITLE, "badge local", support::YOUR_DATA_BADGE)}
                <p>{support::YOUR_DATA_INTRO}</p>
                <PaneInfo text=support::YOUR_DATA_INFO />
            </div>
        </div>
    }
}

/// The orders pane with the rows newest first, the server's pushed counts,
/// and the pane's writes.
#[component]
fn OrdersPane(view: RwSignal<View>) -> impl IntoView {
    let no_identity = move || view.with(|v| v.identity.as_deref().is_none_or(str::is_empty));
    view! {
        <div class="pane toned tone-orders">
            {pane_heading(Icon::CartShopping, support::ORDERS_PANE_TITLE, "badge synced", support::ORDERS_BADGE)}
            <p>
                {move || {
                    view.with(|v| {
                        support::orders_readout(
                            v.orders.len(),
                            v.orders.iter().map(|row| row.quantity).sum(),
                        )
                    })
                }}
            </p>
            <p>
                {support::ORDERS_COUNT_PREFIX}
                <strong>{move || view.with(|v| support::orders_count_value(v.server_count))}</strong>
            </p>
            <p>
                {support::ORDERS_GROUPED_PREFIX}
                <strong>{move || view.with(|v| v.grouped.clone())}</strong>
            </p>
            <div class="row">
                <button class="primary" disabled=no_identity on:click=move |_| act(Action::AddOrder)>
                    <Fa icon=Icon::Plus />
                    "Add order"
                </button>
                <Show when=move || view.with(|v| !v.orders.is_empty())>
                    <button class="danger" on:click=move |_| act(Action::RemoveNewest)>
                        <Fa icon=Icon::Minus />
                        "Remove newest"
                    </button>
                </Show>
                <Show when=move || view.with(|v| v.backend_writer)>
                    <button on:click=move |_| act(Action::InsertBackend)>
                        <Fa icon=Icon::Database />
                        "Insert via Postgres (backend writer)"
                    </button>
                </Show>
            </div>
            <table>
                <thead><tr><th>"id"</th><th>"quantity"</th></tr></thead>
                <tbody>
                    {move || {
                        view.with(|v| {
                            v.orders
                                .iter()
                                .map(|row| {
                                    let quantity = row.quantity.to_string();
                                    view! {
                                        <tr>
                                            <td>{short_code(row.id.clone())}</td>
                                            <td>{quantity}</td>
                                        </tr>
                                    }
                                })
                                .collect_view()
                        })
                    }}
                </tbody>
            </table>
            <PaneInfo text=support::ORDERS_INFO />
        </div>
    }
}

/// Read the first file an input holds and hand its name and bytes to
/// `send`, then clear the input so picking the same file again fires.
fn on_pick(input: HtmlInputElement, send: fn(String, Vec<u8>) -> Action) {
    let file = input.files().and_then(|files| files.get(0));
    input.set_value("");
    let Some(file) = file else {
        return;
    };
    spawn_local(async move {
        if let Some((name, bytes)) = read_picked_file(file).await {
            act(send(name, bytes));
        }
    });
}

/// A picked file's name and bytes, read through the browser's Blob API.
async fn read_picked_file(file: web_sys::File) -> Option<(String, Vec<u8>)> {
    let buf = JsFuture::from(file.array_buffer()).await.ok()?;
    let buf = buf.dyn_into::<js_sys::ArrayBuffer>().ok()?;
    Some((file.name(), js_sys::Uint8Array::new(&buf).to_vec()))
}

/// The photos pane with the rows and their display sources, the file pick
/// that stages a photo, the storage controls, and the refused and lost files.
#[component]
fn PhotosPane(view: RwSignal<View>) -> impl IntoView {
    let no_identity = move || view.with(|v| v.identity.as_deref().is_none_or(str::is_empty));
    view! {
        <div class="pane toned tone-photos">
            {pane_heading(Icon::Images, support::PHOTOS_PANE_TITLE, "badge synced", support::PHOTOS_BADGE)}
            <p>{move || view.with(|v| support::photos_readout(v.photos.len()))}</p>
            <div class="row">
                <input
                    id="photo-file-input"
                    type="file"
                    accept="image/*"
                    disabled=no_identity
                    on:change=move |ev| {
                        on_pick(event_target(&ev), |name, bytes| Action::StagePhoto { name, bytes })
                    }
                />
            </div>
            {move || status_line(view.with(|v| v.stage_line.clone()))}
            <table>
                <thead><tr><th>"id"</th><th>"state"</th><th>"image"</th></tr></thead>
                <tbody>
                    {move || {
                        view.with(|v| {
                            v.photos
                                .iter()
                                .map(|row| {
                                    let state = row.state.clone();
                                    let thumb = v
                                        .photo_thumbs
                                        .get(&row.id)
                                        .cloned()
                                        .map(|src| view! { <img class="thumb" src=src /> });
                                    view! {
                                        <tr>
                                            <td>{short_code(row.id.clone())}</td>
                                            <td>{state}</td>
                                            <td>{thumb}</td>
                                        </tr>
                                    }
                                })
                                .collect_view()
                        })
                    }}
                </tbody>
            </table>
            <div class="row">
                <button on:click=move |_| act(Action::Pin)>
                    <Fa icon=Icon::Thumbtack />
                    "Pin all photos"
                </button>
                <button on:click=move |_| act(Action::Unpin)>
                    <Fa icon=Icon::LinkSlash />
                    "Unpin photos"
                </button>
                <button on:click=move |_| act(Action::FetchPinned)>
                    <Fa icon=Icon::Download />
                    "Fetch pinned"
                </button>
                <button on:click=move |_| act(Action::TidyContent)>
                    <Fa icon=Icon::Broom />
                    "Free up content storage"
                </button>
            </div>
            {move || status_line(view.with(|v| v.content_line.clone()))}
            <RefusedUploads view=view />
            <LostBytes view=view />
            <PaneInfo text=support::PHOTOS_INFO />
        </div>
    }
}

/// The refused uploads, each with a retry that clears its refusal.
#[component]
fn RefusedUploads(view: RwSignal<View>) -> impl IntoView {
    move || {
        let entries = view.with(|v| v.refused_uploads.clone());
        (!entries.is_empty()).then(|| {
            let rows = entries
                .into_iter()
                .map(|(hex, detail)| {
                    let short = hex.chars().take(8).collect::<String>();
                    view! {
                        <div>
                            <p class="status">{format!("{short}: {detail}")}</p>
                            <button on:click=move |_| {
                                act(Action::RetryUpload {
                                    file_id: hex.clone(),
                                })
                            }>
                                <Fa icon=Icon::RotateRight />
                                "Retry"
                            </button>
                        </div>
                    }
                })
                .collect_view();
            view! {
                <div class="confirm">
                    <p>"Refused uploads"</p>
                    {rows}
                </div>
            }
        })
    }
}

/// The files whose unsent bytes were lost, acknowledged with a forget.
#[component]
fn LostBytes(view: RwSignal<View>) -> impl IntoView {
    move || {
        let ids = view.with(|v| v.lost_bytes.clone());
        (!ids.is_empty()).then(|| {
            let rows = ids
                .into_iter()
                .map(|hex| {
                    let short = hex.chars().take(8).collect::<String>();
                    view! { <p class="status">{short}</p> }
                })
                .collect_view();
            view! {
                <div class="confirm">
                    <p>
                        "Bytes lost. These files' chunks are unreadable and their rows still exist. Acknowledge to clear the record."
                    </p>
                    {rows}
                    <button on:click=move |_| act(Action::AcknowledgeLost)>
                        <Fa icon=Icon::Eraser />
                        "Acknowledge all"
                    </button>
                </div>
            }
        })
    }
}

/// The catalogue pane with the shared rows in name order, read from the one
/// copy the shared store keeps on this device.
#[component]
fn ProductsPane(view: RwSignal<View>) -> impl IntoView {
    view! {
        <div class="pane toned tone-catalogue">
            {pane_heading(Icon::Tags, support::CATALOGUE_HEADING, "badge shared", "shared")}
            <p>{support::CATALOGUE_INTRO}</p>
            <table>
                <thead><tr><th>"name"</th><th>"price"</th></tr></thead>
                <tbody>
                    {move || {
                        view.with(|v| {
                            v.products
                                .iter()
                                .map(|row| {
                                    let name = row.name.clone();
                                    let price = row.price.clone();
                                    view! {
                                        <tr>
                                            <td>{name}</td>
                                            <td>{price}</td>
                                        </tr>
                                    }
                                })
                                .collect_view()
                        })
                    }}
                </tbody>
            </table>
            <PaneInfo text=support::CATALOGUE_INFO />
        </div>
    }
}

/// The notes pane with the device-private rows and the save write. The draft
/// clears once the platform reports the note saved, so a failed save keeps
/// the text.
#[component]
fn NotesPane(view: RwSignal<View>) -> impl IntoView {
    let draft = RwSignal::new(String::new());
    let saved = move || view.with(|v| v.notes_saved);
    Effect::watch(saved, move |_, _, _| draft.set(String::new()), false);
    view! {
        <div class="pane toned tone-notes">
            {pane_heading(Icon::NoteSticky, support::NOTES_PANE_TITLE, "badge local", support::NOTES_BADGE)}
            <p>{move || view.with(|v| support::notes_readout(v.notes.len()))}</p>
            <div class="row">
                <input
                    type="text"
                    placeholder=support::NOTES_PLACEHOLDER
                    prop:value=move || draft.get()
                    on:input=move |ev| draft.set(event_target_value(&ev))
                />
                <button
                    class="primary"
                    on:click=move |_| {
                        let body = draft.get_untracked();
                        if !body.is_empty() {
                            act(Action::SaveNote { body });
                        }
                    }
                >
                    <Fa icon=Icon::FloppyDisk />
                    "Save note"
                </button>
            </div>
            <table>
                <thead><tr><th>"id"</th><th>"body"</th></tr></thead>
                <tbody>
                    {move || {
                        view.with(|v| {
                            v.notes
                                .iter()
                                .map(|row| {
                                    view! {
                                        <tr>
                                            <td>{row.id.clone()}</td>
                                            <td>{row.body.clone()}</td>
                                        </tr>
                                    }
                                })
                                .collect_view()
                        })
                    }}
                </tbody>
            </table>
            <PaneInfo text=support::NOTES_INFO />
        </div>
    }
}

/// The retention pane with the replica mirror's footprint and the trim pass.
#[component]
fn RetentionPane(view: RwSignal<View>) -> impl IntoView {
    view! {
        <div class="pane toned tone-retention">
            {pane_heading(Icon::Broom, support::RETENTION_PANE_TITLE, "badge trim", support::RETENTION_BADGE)}
            <p>
                {move || {
                    view.with(|v| support::retention_readout(v.pages, v.free_pages, v.covered_rows))
                }}
            </p>
            <p>{support::RETENTION_EXPLAINER}</p>
            <div class="row">
                <button on:click=move |_| act(Action::FreeUpSpace)>
                    <Fa icon=Icon::Broom />
                    "Free up space"
                </button>
            </div>
            <PaneInfo text=support::RETENTION_INFO />
        </div>
    }
}

/// The your-data pane with the local export and the archive import.
#[component]
fn YourDataPane(view: RwSignal<View>) -> impl IntoView {
    view! {
        <div class="pane toned tone-your-data">
            {pane_heading(Icon::Database, support::YOUR_DATA_PANE_TITLE, "badge local", support::YOUR_DATA_BADGE)}
            <p>{support::YOUR_DATA_INTRO}</p>
            <div class="row">
                <button on:click=move |_| act(Action::Export)>
                    <Fa icon=Icon::FileExport />
                    "Export local data"
                </button>
            </div>
            {move || status_line(view.with(|v| v.export_line.clone()))}
            <p>{support::YOUR_DATA_IMPORT_INTRO}</p>
            <div class="row">
                <input
                    id="import-file-input"
                    type="file"
                    accept=".zip"
                    on:change=move |ev| {
                        on_pick(event_target(&ev), |name, bytes| Action::Import { name, bytes })
                    }
                />
            </div>
            {move || status_line(view.with(|v| v.import_line.clone()))}
            <PaneInfo text=support::YOUR_DATA_INFO />
        </div>
    }
}

/// The scoped style for the nearby panel, its sections and their forms.
const PEER_PANEL_STYLE: &str = r#".peer section + section { border-top: 1px solid var(--border); }
.peer section { padding: 6px 0; }
.peer h4 { display: flex; align-items: center; gap: 6px; margin: 4px 0; font-size: 0.95em; }
.peer p { margin: 2px 0; font-size: 0.9em; }
.peer input { display: block; width: 100%; max-width: 28em; box-sizing: border-box; margin: 4px 0; }"#;

/// The nearby panel (R76): the peer link, the hotspot this device hosts with
/// its offer as plain lines, and the forms that join a hotspot another
/// device hosted. It stands beside the panes while the build carries the
/// peer link.
#[component]
fn NearbyPane(view: RwSignal<View>) -> impl IntoView {
    // The join forms, the typed values the controls keep locally and send
    // with their actions.
    let join_ssid = RwSignal::new(String::new());
    let join_passphrase = RwSignal::new(String::new());
    let join_port = RwSignal::new(String::new());
    let join_payload = RwSignal::new(String::new());

    view! {
        <div class="peer panel toned tone-nearby">
            <style>{PEER_PANEL_STYLE}</style>
            <h2>
                <Fa icon=Icon::SatelliteDish />
                "nearby "
                <span class="badge peering">"peer"</span>
            </h2>
            {move || {
                let near = view.with(|v| v.nearby.clone().unwrap_or_default());
                let support::view::NearbyView {
                    peer_line,
                    bluetooth_line,
                    beacon_line,
                    linked,
                    nearby: nearby_lines,
                    offer,
                    outcome,
                    vpn_line,
                    joined,
                    can_host,
                    links_off,
                } = near;
                let hosting = offer.is_some();
                view! {
                    <section>
                        <h4>
                            <Fa icon=Icon::CircleInfo />
                            "Status"
                        </h4>
                        <p>{peer_line}</p>
                        <p>{bluetooth_line.clone()}</p>
                        <p>{beacon_line}</p>
                        <p>{support::view::links_line(links_off)}</p>
                        {linked.into_iter().map(|line| view! { <p>{line}</p> }).collect_view()}
                        {nearby_lines.into_iter().map(|line| view! { <p>{line}</p> }).collect_view()}
                        {offer.map(|offer| {
                            view! {
                                <p>"hotspot ssid: " {offer.ssid}</p>
                                <p>"hotspot passphrase: " {offer.passphrase}</p>
                                <p>"hotspot port: " {offer.port}</p>
                                <p>"hotspot payload: " {offer.payload}</p>
                                {offer.qr_svg.map(|svg| view! { <div inner_html=svg /> })}
                            }
                        })}
                        <p>"hotspot: " {outcome}</p>
                        {vpn_line.map(|line| view! { <p>{line}</p> })}
                        <div class="row">
                            {support::view::offers_bluetooth_on(&bluetooth_line).then(|| view! {
                                <button on:click=move |_| act(Action::EnableBluetooth)>
                                    <Fa icon=Icon::PowerOff />
                                    "Turn on Bluetooth"
                                </button>
                            })}
                            <button on:click=move |_| act(Action::SetPeerLinks { on: links_off })>
                                <Fa icon=Icon::SatelliteDish />
                                {if links_off { "Resume nearby links" } else { "Pause nearby links" }}
                            </button>
                        </div>
                    </section>
                    {(can_host && !joined).then(|| {
                        view! {
                            <section>
                                <h4>
                                    <Fa icon=Icon::TowerBroadcast />
                                    "Host"
                                </h4>
                                <div class="row">
                                    {(!hosting).then(|| {
                                        view! {
                                            <button on:click=move |_| act(Action::HostHotspot)>
                                                <Fa icon=Icon::TowerBroadcast />
                                                "Host a hotspot"
                                            </button>
                                        }
                                    })}
                                    {hosting.then(|| {
                                        view! {
                                            <button on:click=move |_| act(Action::StopHotspot)>
                                                <Fa icon=Icon::Stop />
                                                "Stop the hotspot"
                                            </button>
                                        }
                                    })}
                                </div>
                            </section>
                        }
                    })}
                    {(!hosting && !joined).then(|| {
                        view! {
                            <section>
                                <h4>
                                    <Fa icon=Icon::Wifi />
                                    "Join by the offer"
                                </h4>
                                <input
                                    type="text"
                                    name="ssid"
                                    placeholder="the offer's ssid"
                                    style="font-family: monospace;"
                                    prop:value=move || join_ssid.get()
                                    on:input=move |ev| join_ssid.set(event_target_value(&ev))
                                />
                                <input
                                    type="text"
                                    name="passphrase"
                                    placeholder="the offer's passphrase"
                                    style="font-family: monospace;"
                                    prop:value=move || join_passphrase.get()
                                    on:input=move |ev| join_passphrase.set(event_target_value(&ev))
                                />
                                <input
                                    type="text"
                                    name="port"
                                    placeholder="the offer's port"
                                    style="font-family: monospace;"
                                    prop:value=move || join_port.get()
                                    on:input=move |ev| join_port.set(event_target_value(&ev))
                                />
                                <button on:click=move |_| {
                                    let ssid = join_ssid.get_untracked().trim().to_owned();
                                    let passphrase = join_passphrase.get_untracked().clone();
                                    let port = join_port.get_untracked().trim().to_owned();
                                    act(Action::JoinHotspot { ssid, passphrase, port });
                                }>
                                    <Fa icon=Icon::Wifi />
                                    "Join the hotspot"
                                </button>
                            </section>
                            <section>
                                <h4>
                                    <Fa icon=Icon::Qrcode />
                                    "Join by the payload"
                                </h4>
                                <input
                                    type="text"
                                    name="payload"
                                    placeholder="WIFI: payload"
                                    style="font-family: monospace;"
                                    prop:value=move || join_payload.get()
                                    on:input=move |ev| join_payload.set(event_target_value(&ev))
                                />
                                <button on:click=move |_| {
                                    let payload = join_payload.get_untracked().trim().to_owned();
                                    act(Action::JoinPayload { payload });
                                }>
                                    <Fa icon=Icon::Qrcode />
                                    "Join from the payload"
                                </button>
                            </section>
                        }
                    })}
                    {(!hosting).then(|| {
                        view! {
                            <section>
                                <h4>
                                    <Fa icon=Icon::SatelliteDish />
                                    "Join nearby, or leave"
                                </h4>
                                <div class="row">
                                    {joined.then(|| {
                                        view! {
                                            <button on:click=move |_| act(Action::LeaveHotspot)>
                                                <Fa icon=Icon::RightFromBracket />
                                                "Leave the hotspot"
                                            </button>
                                        }
                                    })}
                                    {(!joined).then(|| {
                                        view! {
                                            <button on:click=move |_| act(Action::JoinNearby)>
                                                <Fa icon=Icon::SatelliteDish />
                                                "Join the nearby host"
                                            </button>
                                        }
                                    })}
                                </div>
                            </section>
                        }
                    })}
                }
            }}
            <PaneInfo text=support::NEARBY_INFO />
        </div>
    }
}

/// The team notes panel (R77): the team the roster names, the note's field
/// and write, the team's notes, one line per peer write, and the tidy pass.
/// It stands beside the nearby panel and syncs through the server.
#[component]
fn TeamNotesPane(view: RwSignal<View>) -> impl IntoView {
    // The team picker and the note's field, the typed values the controls
    // keep locally and send with their actions.
    let team_input = RwSignal::new(String::new());
    let note_body = RwSignal::new(String::new());

    // Fill the team picker from the view's picked while it stands empty, so a
    // roster landing after the panel mounts still names its first team.
    Effect::watch(
        move || {
            view.with(|v| {
                v.team
                    .as_ref()
                    .map(|t| t.picked.clone())
                    .unwrap_or_default()
            })
        },
        move |picked, _, _| {
            if team_input.get_untracked().is_empty() {
                team_input.set(picked.clone());
            }
        },
        true,
    );

    view! {
        <div class="panel toned tone-team">
            <h2>
                <Fa icon=Icon::Users />
                "team notes "
                <span class="badge synced">"synced + peer"</span>
            </h2>
            <p>{move || view.with(|v| v.team.as_ref().map(|t| t.team_line()).unwrap_or_else(|| "team: none".to_owned()))}</p>
            <p>{move || view.with(|v| v.team.as_ref().map(|t| t.teams_line()).unwrap_or_else(|| "teams: none".to_owned()))}</p>
            <p>{move || view.with(|v| support::view::members_line(v.team.as_ref().map(|t| t.members.as_slice()).unwrap_or_default()))}</p>
            <input
                type="text"
                name="team"
                placeholder="the team to read and write"
                prop:value=move || team_input.get()
                on:input=move |ev| {
                    let value = event_target_value(&ev);
                    team_input.set(value.clone());
                    act(Action::PickTeam { team: value });
                }
                on:change=move |ev| act(Action::PickTeam { team: event_target_value(&ev) })
            />
            {move || {
                let (has_pick, notes, outcome) = view.with(|v| {
                    v.team
                        .as_ref()
                        .map(|t| (!t.picked.is_empty(), t.notes.clone(), t.outcome.clone()))
                        .unwrap_or((false, Vec::new(), String::new()))
                });
                has_pick.then(|| {
                    let empty = notes.is_empty();
                    view! {
                        <div class="lines">
                            {empty.then(|| view! { <p class="status">"no notes in this team yet"</p> })}
                            {notes.into_iter().map(|line| view! { <p>{line}</p> }).collect_view()}
                        </div>
                        <div class="row">
                            <input
                                type="text"
                                name="note-body"
                                placeholder="a note for the team"
                                prop:value=move || note_body.get()
                                on:input=move |ev| note_body.set(event_target_value(&ev))
                            />
                            <button class="primary" on:click=move |_| {
                                let body = note_body.get_untracked().trim().to_owned();
                                note_body.set(String::new());
                                act(Action::AddTeamNote { body });
                            }>
                                <Fa icon=Icon::Plus />
                                "Add note"
                            </button>
                        </div>
                        <p class="status">{outcome}</p>
                    }
                })
            }}
            {move || {
                let events = view.with(|v| v.team.as_ref().map(|t| t.events.clone()).unwrap_or_default());
                (!events.is_empty()).then(|| {
                    view! {
                        <h3>
                            <Fa icon=Icon::ClockRotateLeft size=12 />
                            "peer activity"
                        </h3>
                        <div class="lines">
                            {events.into_iter().map(|line| view! { <p>{line}</p> }).collect_view()}
                        </div>
                    }
                })
            }}
            {move || {
                view.with(|v| v.team.as_ref().map(|t| t.peer).unwrap_or(false)).then(|| {
                    view! {
                        <div class="row">
                            <button on:click=move |_| act(Action::TidyTeam)>
                                <Fa icon=Icon::Broom />
                                "Tidy"
                            </button>
                        </div>
                    }
                })
            }}
            {move || {
                view.with(|v| v.team.as_ref().and_then(|t| t.error.clone()))
                    .map(|err| view! { <p class="error">{err}</p> })
            }}
            <PaneInfo text=support::TEAM_INFO />
        </div>
    }
}

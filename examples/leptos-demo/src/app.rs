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

/// The app name, and the status, gate, device, custody and expiry lines.
#[component]
fn Header(view: RwSignal<View>) -> impl IntoView {
    view! {
        <header>
            <h1>"connetto web demo (Leptos)"</h1>
            <p class="status">"status: " {move || view.with(|v| v.status.clone())}</p>
            {move || status_line(view.with(|v| v.gate_line.clone()))}
            <p class="status">"device: " {move || view.with(|v| v.device_line.clone())}</p>
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
        view.with(|v| v.stored_account.as_deref().map(support::account_label))
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
                        .map(|key| {
                            let label = support::account_label(&key);
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
                .as_deref()
                .map(support::account_label)
                .filter(|label| Some(label) != v.identity.as_ref())
        })
    };
    let switches = move || {
        view.with(|v| {
            v.known_accounts
                .iter()
                .filter(|key| v.account.as_deref() != Some(key.as_str()))
                .map(|key| (key.clone(), support::account_label(key)))
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

/// The five panes once the tab attached, the skeleton while a stored session
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

/// The pane layout shown while a stored session is still landing.
#[component]
fn PanesSkeleton() -> impl IntoView {
    view! {
        <div class="panes">
            <div class="pane">
                {pane_heading(Icon::CartShopping, support::ORDERS_PANE_TITLE, "badge synced", support::ORDERS_BADGE)}
                <p>{support::orders_readout(0, 0)}</p>
                <table>
                    <thead><tr><th>"id"</th><th>"quantity"</th></tr></thead>
                    <tbody></tbody>
                </table>
            </div>
            <div class="pane">
                {pane_heading(Icon::Images, support::PHOTOS_PANE_TITLE, "badge synced", support::PHOTOS_BADGE)}
                <p>{support::photos_readout(0)}</p>
                <table>
                    <thead><tr><th>"id"</th><th>"state"</th><th>"image"</th></tr></thead>
                    <tbody></tbody>
                </table>
            </div>
            <div class="pane">
                {pane_heading(Icon::NoteSticky, support::NOTES_PANE_TITLE, "badge local", support::NOTES_BADGE)}
                <p>{support::notes_readout(0)}</p>
                <table>
                    <thead><tr><th>"id"</th><th>"body"</th></tr></thead>
                    <tbody></tbody>
                </table>
            </div>
            <div class="pane">
                {pane_heading(Icon::Broom, support::RETENTION_PANE_TITLE, "badge trim", support::RETENTION_BADGE)}
                <p>{support::retention_readout(0, 0, 0)}</p>
                <p>{support::RETENTION_EXPLAINER}</p>
            </div>
            <div class="pane">
                {pane_heading(Icon::Database, support::YOUR_DATA_PANE_TITLE, "badge local", support::YOUR_DATA_BADGE)}
                <p>{support::YOUR_DATA_INTRO}</p>
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
        <div class="pane">
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
        <div class="pane">
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

/// The notes pane with the device-private rows and the save write. The draft
/// clears once the platform reports the note saved, so a failed save keeps
/// the text.
#[component]
fn NotesPane(view: RwSignal<View>) -> impl IntoView {
    let draft = RwSignal::new(String::new());
    let saved = move || view.with(|v| v.notes_saved);
    Effect::watch(saved, move |_, _, _| draft.set(String::new()), false);
    view! {
        <div class="pane">
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
        </div>
    }
}

/// The retention pane with the replica mirror's footprint and the trim pass.
#[component]
fn RetentionPane(view: RwSignal<View>) -> impl IntoView {
    view! {
        <div class="pane">
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
        </div>
    }
}

/// The your-data pane with the local export and the archive import.
#[component]
fn YourDataPane(view: RwSignal<View>) -> impl IntoView {
    view! {
        <div class="pane">
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
        </div>
    }
}

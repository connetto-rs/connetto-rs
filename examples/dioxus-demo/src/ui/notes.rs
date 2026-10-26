//! The notes pane: the device-private rows, which never leave the device.

use connetto_demo_support as support;
use connetto_dioxus::use_live;
use dioxus::prelude::*;
use dioxus_free_icons::Icon;
use dioxus_free_icons::icons::fa_solid_icons::{FaFloppyDisk, FaNoteSticky};

use super::ClientSlot;

/// The notes pane.
#[component]
pub fn NotesPane() -> Element {
    let ClientSlot(client_slot) = use_context::<ClientSlot>();
    let client = client_slot.read().clone().expect("mounted with a client");

    let notes_live = use_live::<_, _, support::Note>(&client, support::notes_rows());
    let rows: Vec<support::Note> = notes_live.value().read().iter().cloned().collect();
    let notes_error = notes_live.error().read().clone();

    let note_text = use_signal(String::new);
    let save_client = client.clone();
    let on_note_input = {
        move |event: Event<FormData>| {
            let mut note_text = note_text;
            note_text.set(event.value());
        }
    };

    rsx! {
        div { class: "pane",
            h2 {
                Icon { icon: FaNoteSticky, width: 14, height: 14 }
                {support::NOTES_PANE_TITLE} " "
                span { class: "badge local", {support::NOTES_BADGE} }
            }
            p { {support::notes_readout(rows.len())} }
            div { class: "row",
                input {
                    r#type: "text",
                    placeholder: support::NOTES_PLACEHOLDER,
                    value: note_text.read().clone(),
                    oninput: on_note_input,
                }
                button {
                    class: "primary",
                    onclick: move |_| {
                        let body = note_text.read().clone();
                        if body.is_empty() {
                            return;
                        }
                        let client = save_client.clone();
                        let note_text = note_text;
                        spawn(async move {
                            let mut note_text = note_text;
                            match support::save_note(&client, &body).await {
                                Ok(_) => note_text.set(String::new()),
                                Err(err) => tracing::error!(error = %err, "note save failed"),
                            }
                        });
                    },
                    Icon { icon: FaFloppyDisk, width: 14, height: 14 }
                    "Save note"
                }
            }
            if let Some(err) = notes_error {
                p { class: "error", "notes error: {err}" }
            }
            table {
                thead { tr { th { "id" } th { "body" } } }
                tbody {
                    for row in rows {
                        tr { key: "{row.id}",
                            td { {row.id.to_string()} }
                            td { {row.body} }
                        }
                    }
                }
            }
        }
    }
}

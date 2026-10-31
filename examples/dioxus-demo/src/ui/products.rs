//! The catalogue pane: the shared store's products, by name, every account
//! on the device reading the one copy.

use connetto_demo_support as support;
use connetto_dioxus::use_live;
use dioxus::prelude::*;
use dioxus_free_icons::Icon;
use dioxus_free_icons::icons::fa_solid_icons::FaTags;

use super::ClientSlot;

/// The catalogue pane.
#[component]
pub fn ProductsPane() -> Element {
    let ClientSlot(client_slot) = use_context::<ClientSlot>();
    let client = client_slot.read().clone().expect("mounted with a client");

    let products_live = use_live::<_, _, support::Product>(&client, support::products_by_name());
    let rows: Vec<support::Product> = products_live.value().read().iter().cloned().collect();
    let rows_error = products_live.error().read().clone();

    rsx! {
        div { class: "pane",
            h2 {
                Icon { icon: FaTags, width: 14, height: 14 }
                {support::CATALOGUE_HEADING} " "
                span { class: "badge shared", "shared" }
            }
            p { {support::CATALOGUE_INTRO} }
            if let Some(err) = rows_error {
                p { class: "error", "catalogue error: {err}" }
            }
            table {
                thead { tr { th { "name" } th { "price" } } }
                tbody {
                    for (id, name, price) in rows.iter().map(|product| {
                        (
                            product.id.to_string(),
                            product.name.clone(),
                            support::price_label(product.price_cents),
                        )
                    }).collect::<Vec<_>>() {
                        tr { key: "{id}",
                            td { {name} }
                            td { {price} }
                        }
                    }
                }
            }
        }
    }
}

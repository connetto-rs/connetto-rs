//! The orders pane: the visible rows newest first, the count the server
//! pushes, the add and remove buttons.

use connetto_demo_support as support;
use connetto_dioxus::use_live;
use dioxus::prelude::*;
use dioxus_free_icons::Icon;
use dioxus_free_icons::icons::fa_solid_icons::{FaCartShopping, FaMinus, FaPlus};

use super::{ClientSlot, UserId};

/// The orders pane.
#[component]
pub fn OrdersPane() -> Element {
    let ClientSlot(client_slot) = use_context::<ClientSlot>();
    let client = client_slot.read().clone().expect("mounted with a client");
    let UserId(user_id) = use_context::<UserId>();
    let uid = user_id.read().clone().unwrap_or_default();

    let orders_live = use_live::<_, _, support::Order>(&client, support::orders_newest_first());
    let count = use_live(&client, support::orders_count());
    let counts_by_quantity = use_live(&client, support::orders_counts_by_quantity());

    let rows: Vec<support::Order> = orders_live.value().read().iter().cloned().collect();
    let sum: i64 = rows.iter().map(|row| row.quantity).sum();
    let count_text = support::orders_count_value(*count.value().read());
    let rows_error = orders_live.error().read().clone();
    let count_error = count.error().read().clone();
    let grouped_text = support::grouped_label(&counts_by_quantity.value().read());
    let grouped_error = counts_by_quantity.error().read().clone();
    let newest_id = rows.first().map(|row| row.id);

    let add_uid = uid.clone();
    let add_client = client.clone();
    let remove_client = client.clone();

    rsx! {
        div { class: "pane",
            h2 {
                Icon { icon: FaCartShopping, width: 14, height: 14 }
                {support::ORDERS_PANE_TITLE} " "
                span { class: "badge synced", {support::ORDERS_BADGE} }
            }
            p { {support::orders_readout(rows.len(), sum)} }
            p { {support::ORDERS_COUNT_PREFIX} strong { {count_text} } }
            p { {support::ORDERS_GROUPED_PREFIX} strong { {grouped_text} } }
            div { class: "row",
                button {
                    class: "primary",
                    disabled: uid.is_empty(),
                    onclick: move |_| {
                        let client = add_client.clone();
                        let identity = add_uid.clone();
                        spawn(async move {
                            let quantity = support::fresh_quantity();
                            if let Err(err) = support::add_order(&client, &identity, quantity).await {
                                tracing::error!(error = %err, "order insert failed");
                            }
                        });
                    },
                    Icon { icon: FaPlus, width: 14, height: 14 }
                    "Add order"
                }
                if newest_id.is_some() {
                    button {
                        class: "danger",
                        onclick: move |_| {
                            let Some(id) = newest_id else {
                                return;
                            };
                            let client = remove_client.clone();
                            spawn(async move {
                                if let Err(err) = support::remove_newest_order(&client, id).await {
                                    tracing::error!(error = %err, "order remove failed");
                                }
                            });
                        },
                        Icon { icon: FaMinus, width: 14, height: 14 }
                        "Remove newest"
                    }
                }
            }
            if let Some(err) = rows_error {
                p { class: "error", "orders error: {err}" }
            }
            if let Some(err) = count_error {
                p { class: "error", "count error: {err}" }
            }
            if let Some(err) = grouped_error {
                p { class: "error", "grouped subscription error: {err}" }
            }
            table {
                thead { tr { th { "id" } th { "quantity" } } }
                tbody {
                    for (id, quantity) in rows
                        .iter()
                        .map(|row| (row.id.to_string(), row.quantity))
                        .collect::<Vec<_>>()
                    {
                        tr { key: "{id}",
                            td { code { class: "id", title: id.clone(), {support::short_id(&id)} } }
                            td { "{quantity}" }
                        }
                    }
                }
            }
        }
    }
}

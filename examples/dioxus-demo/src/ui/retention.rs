//! The retention pane: the replica mirror's footprint and the trim pass.

use connetto_demo_support as support;
use connetto_dioxus::use_live;
use dioxus::prelude::*;
use dioxus_free_icons::Icon;
use dioxus_free_icons::icons::fa_solid_icons::FaBroom;

use super::{ClientSlot, Footprint};

/// The retention pane.
#[component]
pub fn RetentionPane() -> Element {
    let ClientSlot(client_slot) = use_context::<ClientSlot>();
    let client = client_slot.read().clone().expect("mounted with a client");
    let Footprint(footprint) = use_context::<Footprint>();
    let (pages, free) = *footprint.read();
    let tidy_client = client.clone();
    // The rows the live subscriptions still cover, which the trim pass spares.
    let orders_live = use_live(&client, support::orders_newest_first());
    let notes_live = use_live(&client, support::notes_rows());
    let photos_live = use_live(&client, support::photos_rows());
    let covered = orders_live.value().read().len()
        + notes_live.value().read().len()
        + photos_live.value().read().len();

    rsx! {
        div { class: "pane",
            h2 {
                Icon { icon: FaBroom, width: 14, height: 14 }
                {support::RETENTION_PANE_TITLE} " "
                span { class: "badge trim", {support::RETENTION_BADGE} }
            }
            p { {support::retention_readout(pages, free, covered)} }
            p { {support::RETENTION_EXPLAINER} }
            div { class: "row",
                button {
                    onclick: move |_| {
                        let client = tidy_client.clone();
                        let footprint = footprint;
                        spawn(async move {
                            let mut footprint = footprint;
                            if let Err(err) = client.tidy().await {
                                tracing::error!(error = %err, "free up space failed");
                            }
                            footprint.set(support::replica_footprint(&client).await);
                        });
                    },
                    Icon { icon: FaBroom, width: 14, height: 14 }
                    "Free up space"
                }
            }
        }
    }
}

//! The peer and hotspot panel (R76): the device's peer listener, its linked
//! peers, the hotspot it hosts with the offer shown as plain lines, and the
//! forms that join a hotspot another device hosted.
//!
//! The panel is native only, since a browser has no Bluetooth or hotspot
//! control, and sits behind the `peer` feature like the link itself.

use dioxus::prelude::*;

use crate::platform::native::SessionParts;
use connetto_client::ClientEvent;
use connetto_demo_support::nearby;
use connetto_demo_support::view::OfferView;

/// The peer and hotspot panel (R76 slice 4): the device's peer listener, its
/// linked peers, the hotspot it hosts with the offer shown as plain lines,
/// and the form that joins a hotspot another device hosted.
#[cfg(feature = "peer")]
#[component]
pub(crate) fn PeerPanel(parts: SessionParts) -> Element {
    use connetto_client::{HotspotError, HotspotOffer};
    use dioxus_free_icons::Icon;
    use dioxus_free_icons::icons::fa_solid_icons::{
        FaCircleInfo, FaPowerOff, FaQrcode, FaRightFromBracket, FaSatelliteDish, FaStop,
        FaTowerBroadcast, FaWifi,
    };

    // The listener's port, read again every second, since the listener opens
    // beside the pump the way enrolment does.
    let mut peer_port: Signal<Option<u16>> = use_signal(|| None);
    // One line per linked peer, read off the node on the same pace, so a
    // replaced link never leaves its old address on the page.
    let mut peers: Signal<Vec<String>> = use_signal(Vec::new);
    // The Bluetooth standing and the host's beacon, read on the same pace.
    let mut bluetooth_line: Signal<String> = use_signal(|| "bluetooth: unsupported".to_owned());
    let mut beacon_line: Signal<String> =
        use_signal(|| "beacon: off (no bluetooth on this system)".to_owned());
    {
        let parts = parts.clone();
        use_hook(move || {
            spawn(async move {
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    let port = parts.0.native.peer_address().map(|address| address.port());
                    if *peer_port.peek() != port {
                        peer_port.set(port);
                    }
                    let state = format!("bluetooth: {}", parts.0.native.bluetooth_state());
                    if *bluetooth_line.peek() != state {
                        bluetooth_line.set(state);
                    }
                    let line = nearby::beacon_line(parts.0.native.beacon_state());
                    if *beacon_line.peek() != line {
                        beacon_line.set(line);
                    }
                    let mut lines: Vec<String> = parts
                        .0
                        .native
                        .linked_peers()
                        .iter()
                        .map(|link| nearby::linked_line(&link.peer, link.remote))
                        .collect();
                    lines.sort();
                    if *peers.peek() != lines {
                        peers.set(lines);
                    }
                }
            })
        });
    }

    // The offer this device hosts, as its plain lines, until the hotspot
    // stops.
    let offer: Signal<Option<OfferView>> = use_signal(|| None);
    // The last hotspot or join outcome, `idle` before the first.
    let outcome: Signal<String> = use_signal(|| "idle".to_owned());
    // Whether this device stands joined to a host's hotspot, which hides the
    // joins and the hosting until it leaves.
    let joined: Signal<bool> = use_signal(|| false);
    // Whether a VPN refused the joined hotspot's dial, shown on its own line
    // so a join's outcome landing later never hides it.
    let vpn_blocked: Signal<bool> = use_signal(|| false);
    // A permission a host or join named as missing, which the panel asks for
    // on the UI thread, where the platform's prompt wants to be asked from.
    let missing: Signal<Option<String>> = use_signal(|| None);
    // The hosts whose beacons this device sees, with their prefixes and
    // signals, as the scan reports them.
    let nearby: Signal<Vec<connetto_client::NearbyHost>> = use_signal(Vec::new);
    // The join form, the host's offer read off its lines or typed over.
    let mut join_ssid: Signal<String> = use_signal(String::new);
    let mut join_passphrase: Signal<String> = use_signal(String::new);
    let mut join_port: Signal<String> = use_signal(String::new);
    // A pasted or scanned `WIFI:` payload (R76 decision 24).
    let mut join_payload: Signal<String> = use_signal(String::new);

    // The link's and the hotspot's events, which the panel owns.
    let client = parts.0.client.clone();
    let event_rx = client.events();
    {
        let mut offer = offer;
        let mut outcome = outcome;
        let mut nearby = nearby;
        let mut joined = joined;
        let mut vpn_blocked = vpn_blocked;
        use_hook(move || {
            spawn(async move {
                let mut rx = event_rx;
                while let Ok(event) = rx.recv().await {
                    match event {
                        ClientEvent::HotspotStopped => {
                            offer.set(None);
                            outcome.set("stopped by the system or the user".to_owned());
                        }
                        ClientEvent::HotspotLeft => {
                            joined.set(false);
                            vpn_blocked.set(false);
                            outcome.set("the joined network was lost".to_owned());
                        }
                        ClientEvent::HostNearby { host, prefix, rssi } => {
                            let mut held = nearby.peek().clone();
                            held.retain(|known| known.host != host);
                            held.push(connetto_client::NearbyHost { host, prefix, rssi });
                            nearby.set(held);
                        }
                        ClientEvent::HostGone { host } => {
                            let mut held = nearby.peek().clone();
                            held.retain(|known| known.host != host);
                            nearby.set(held);
                        }
                        ClientEvent::JoinedNetworkBlockedByVpn => {
                            vpn_blocked.set(true);
                        }
                        _ => {}
                    }
                }
            })
        });
    }

    // The platform's permission prompt wants the UI thread, so the panel
    // fires it there when a host or join named a missing permission.
    let mut missing_effect = missing;
    #[cfg(target_os = "android")]
    let permission_parts = parts.clone();
    use_effect(move || {
        if missing_effect().is_some() {
            #[cfg(target_os = "android")]
            if let Err(err) = permission_parts.0.native.request_peer_permissions() {
                tracing::warn!(error = %err, "the peer permissions could not be asked");
            }
            #[cfg(target_os = "macos")]
            connetto_peer_apple::hotspot::request_location_access();
            missing_effect.set(None);
        }
    });

    let host_parts = parts.clone();
    let host_offer = offer;
    let host_outcome = outcome;
    let host_missing = missing;
    let stop_parts = parts.clone();
    let stop_offer = offer;
    let stop_outcome = outcome;
    let join_parts = parts.clone();
    let mut join_outcome = outcome;
    let join_missing = missing;
    let leave_parts = parts.clone();
    let payload_parts = parts.clone();
    let payload_outcome = outcome;
    let payload_missing = missing;
    let nearby_missing = missing;
    let leave_outcome = outcome;
    let bt_parts = parts.clone();
    let bt_outcome = outcome;
    let nearby_parts = parts.clone();
    let nearby_outcome = outcome;
    let nearby_hosts = nearby;
    let join_joined = joined;
    let payload_joined = joined;
    let nearby_joined = joined;
    let leave_joined = joined;
    let leave_vpn_blocked = vpn_blocked;
    // The runtime peer-link switch's standing, read back from the client
    // after each switch.
    let links_parts = parts.clone();
    let mut links_off: Signal<bool> = use_signal(|| false);

    let certificate = parts.0.native.device_certificate();
    let peer_line = nearby::peer_line(
        peer_port(),
        certificate
            .as_ref()
            .map(|certificate| certificate.identity()),
    );
    let peers = peers();
    let nearby_lines: Vec<String> = nearby().iter().map(nearby::nearby_line).collect();
    let offer = offer();
    let outcome = outcome();
    let hosting = offer.is_some();
    let joined_now = joined();
    let vpn_line = (joined() && vpn_blocked()).then_some(nearby::VPN_LINE);

    rsx! {
        div { class: "peer panel toned tone-nearby",
            style {
                ".peer section + section {{ border-top: 1px solid var(--border); }}
                .peer section {{ padding: 6px 0; }}
                .peer h4 {{ display: flex; align-items: center; gap: 6px; margin: 4px 0; font-size: 0.95em; }}
                .peer p {{ margin: 2px 0; font-size: 0.9em; }}
                .peer input {{ display: block; width: 100%; max-width: 28em; box-sizing: border-box; margin: 4px 0; }}"
            }
            h2 {
                Icon { icon: FaSatelliteDish, width: 14, height: 14 }
                "nearby "
                span { class: "badge peering", "peer" }
            }
            section {
                h4 {
                    Icon { icon: FaCircleInfo, width: 14, height: 14 }
                    "Status"
                }
                p { {peer_line} }
                p { {bluetooth_line()} }
                p { {beacon_line()} }
                p { {connetto_demo_support::view::links_line(links_off())} }
                for line in peers.iter().cloned() {
                    p { {line} }
                }
                for line in nearby_lines {
                    p { {line} }
                }
                if let Some(offer) = offer {
                    p {
                        "hotspot ssid: "
                        {offer.ssid.clone()}
                    }
                    p {
                        "hotspot passphrase: "
                        {offer.passphrase.clone()}
                    }
                    p {
                        "hotspot port: "
                        {offer.port.clone()}
                    }
                    p {
                        "hotspot payload: "
                        {offer.payload.clone()}
                    }
                    if let Some(svg) = offer.qr_svg.clone() {
                        div { dangerous_inner_html: svg }
                    }
                }
                p {
                    "hotspot: "
                    {outcome}
                }
                if let Some(line) = vpn_line {
                    p { {line} }
                }
                div { class: "row",
                    if connetto_demo_support::view::offers_bluetooth_on(&bluetooth_line()) {
                        button {
                            onclick: move |_| {
                                let parts = bt_parts.clone();
                                let mut outcome = bt_outcome;
                                spawn(async move {
                                    outcome.set("turning on bluetooth".to_owned());
                                    match parts.0.native.enable_bluetooth().await {
                                        Ok(()) => outcome.set("bluetooth is on".to_owned()),
                                        Err(err) => outcome.set(err.to_string()),
                                    }
                                });
                            },
                            Icon { icon: FaPowerOff, width: 14, height: 14 }
                            "Turn on Bluetooth"
                        }
                    }
                    button {
                        onclick: move |_| {
                            links_parts.0.native.set_peer_links(links_off());
                            links_off.set(!links_parts.0.native.peer_links_on());
                        },
                        Icon { icon: FaSatelliteDish, width: 14, height: 14 }
                        if links_off() {
                            "Resume nearby links"
                        } else {
                            "Pause nearby links"
                        }
                    }
                }
            }
            // Apple devices only join (R77 decision 23).
            if !cfg!(any(target_os = "ios", target_os = "macos")) && !joined_now {
                section {
                    h4 {
                        Icon { icon: FaTowerBroadcast, width: 14, height: 14 }
                        "Host"
                    }
                    div { class: "row",
                        if !hosting {
                            button {
                                onclick: move |_| {
                                    let parts = host_parts.clone();
                                    let mut offer = host_offer;
                                    let mut outcome = host_outcome;
                                    let mut missing = host_missing;
                                    spawn(async move {
                                        outcome.set("asking for the hotspot".to_owned());
                                        let result = parts.0.native.host_hotspot().await;
                                        match result {
                                            Ok(hosted) => {
                                                offer.set(Some(nearby::offer_view(&hosted.offer)));
                                                outcome.set(nearby::hosted_outcome(&hosted.beacon));
                                            }
                                            Err(err) => {
                                                if let HotspotError::MissingPermission(name) = &err {
                                                    missing.set(Some(name.clone()));
                                                }
                                                outcome.set(err.to_string());
                                            }
                                        }
                                    });
                                },
                                Icon {
                                    icon: FaTowerBroadcast,
                                    width: 14,
                                    height: 14,
                                }
                                "Host a hotspot"
                            }
                        }
                        if hosting {
                            button {
                                onclick: move |_| {
                                    let parts = stop_parts.clone();
                                    let mut offer = stop_offer;
                                    let mut outcome = stop_outcome;
                                    spawn(async move {
                                        parts.0.native.stop_hotspot().await;
                                        offer.set(None);
                                        outcome.set("stopped".to_owned());
                                    });
                                },
                                Icon { icon: FaStop, width: 14, height: 14 }
                                "Stop the hotspot"
                            }
                        }
                    }

                }
            }
            if !hosting && !joined_now {
                section {
                    h4 {
                        Icon { icon: FaWifi, width: 14, height: 14 }
                        "Join by the offer"
                    }
                    input {
                        r#type: "text",
                        name: "ssid",
                        placeholder: "the offer's ssid",
                        value: "{join_ssid}",
                        oninput: move |event| join_ssid.set(event.value()),
                        style: "font-family: monospace;",
                    }
                    input {
                        r#type: "text",
                        name: "passphrase",
                        placeholder: "the offer's passphrase",
                        value: "{join_passphrase}",
                        oninput: move |event| join_passphrase.set(event.value()),
                        style: "font-family: monospace;",
                    }
                    input {
                        r#type: "text",
                        name: "port",
                        placeholder: "the offer's port",
                        value: "{join_port}",
                        oninput: move |event| join_port.set(event.value()),
                        style: "font-family: monospace;",
                    }
                    button {
                        onclick: move |_| {
                            let offer = match nearby::typed_offer(
                                &join_ssid.peek(),
                                &join_passphrase.peek(),
                                &join_port.peek(),
                            ) {
                                Ok(offer) => offer,
                                Err(outcome) => {
                                    join_outcome.set(outcome.to_owned());
                                    return;
                                }
                            };
                            spawn(join_offer(
                                join_parts.clone(),
                                offer,
                                join_outcome,
                                join_joined,
                                join_missing,
                            ));
                        },
                        Icon { icon: FaWifi, width: 14, height: 14 }
                        "Join the hotspot"
                    }
                }
                section {
                    h4 {
                        Icon { icon: FaQrcode, width: 14, height: 14 }
                        "Join by the payload"
                    }
                    input {
                        name: "payload",
                        placeholder: "WIFI: payload",
                        value: "{join_payload}",
                        oninput: move |event| join_payload.set(event.value()),
                        style: "font-family: monospace;",
                    }
                    button {
                        onclick: move |_| {
                            let mut outcome = payload_outcome;
                            let offer = match HotspotOffer::from_wifi_payload(join_payload.peek().trim()) {
                                Ok(offer) => offer,
                                Err(err) => {
                                    outcome.set(err.to_string());
                                    return;
                                }
                            };
                            spawn(join_offer(
                                payload_parts.clone(),
                                offer,
                                outcome,
                                payload_joined,
                                payload_missing,
                            ));
                        },
                        Icon { icon: FaQrcode, width: 14, height: 14 }
                        "Join from the payload"
                    }
                }
            }
            if !hosting {
                section {
                    h4 {
                        Icon { icon: FaSatelliteDish, width: 14, height: 14 }
                        "Join nearby, or leave"
                    }
                    div { class: "row",
                        if joined_now {
                            button {
                                onclick: move |_| {
                                    let parts = leave_parts.clone();
                                    let mut outcome = leave_outcome;
                                    let mut joined = leave_joined;
                                    let mut vpn_blocked = leave_vpn_blocked;
                                    spawn(async move {
                                        parts.0.native.leave_hotspot().await;
                                        joined.set(false);
                                        vpn_blocked.set(false);
                                        outcome.set("left".to_owned());
                                    });
                                },
                                Icon {
                                    icon: FaRightFromBracket,
                                    width: 14,
                                    height: 14,
                                }
                                "Leave the hotspot"
                            }
                        }
                        if !joined_now {
                            button {
                                onclick: move |_| {
                                    let strongest = nearby::strongest(&nearby_hosts.peek());
                                    let mut outcome = nearby_outcome;
                                    let mut joined = nearby_joined;
                                    // One nearby join at a time, so its outcome is the one shown.
                                    if *outcome.peek() == nearby::JOINING_NEARBY {
                                        return;
                                    }
                                    let Some(host) = strongest else {
                                        outcome.set("no host is nearby".to_owned());
                                        return;
                                    };
                                    let parts = nearby_parts.clone();
                                    let mut missing = nearby_missing;
                                    spawn(async move {
                                        outcome.set(nearby::JOINING_NEARBY.to_owned());
                                        match parts.0.native.join_nearby(&host).await {
                                            Ok((identity, gateway)) => {
                                                joined.set(true);
                                                outcome.set(nearby::joined_nearby_outcome(gateway, &identity));
                                            }
                                            Err(err) => {
                                                if let Some(name) = nearby::nearby_permission(&err) {
                                                    missing.set(Some(name.to_owned()));
                                                }
                                                outcome.set(err.to_string());
                                            }
                                        }
                                    });
                                },
                                Icon {
                                    icon: FaSatelliteDish,
                                    width: 14,
                                    height: 14,
                                }
                                "Join the nearby host"
                            }
                        }
                    }
                }
            }
            super::PaneInfo { text: connetto_demo_support::NEARBY_INFO }
        }
    }
}

/// Join the hotspot `offer` names, showing the join's outcome and the
/// permission it misses.
#[cfg(feature = "peer")]
async fn join_offer(
    parts: SessionParts,
    offer: connetto_client::HotspotOffer,
    mut outcome: Signal<String>,
    mut joined: Signal<bool>,
    mut missing: Signal<Option<String>>,
) {
    outcome.set(nearby::JOINING_HOTSPOT.to_owned());
    match parts.0.native.join_hotspot(&offer).await {
        Ok(gateway) => {
            joined.set(true);
            outcome.set(nearby::joined_outcome(gateway));
        }
        Err(err) => {
            if let connetto_client::JoinError::MissingPermission(name) = &err {
                missing.set(Some(name.clone()));
            }
            outcome.set(err.to_string());
        }
    }
}

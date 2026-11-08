//! The nearby panel's lines (R76), the one form every native demo shows and
//! the phone runners read off the page, `linked:`, `nearby:`, `beacon:` and
//! the hotspot's outcome among them.

use std::fmt::Write as _;
use std::net::{IpAddr, SocketAddr};

use connetto_client::{
    BeaconState, HostId, HotspotOffer, HotspotSecurity, JoinError, JoinNearbyError, NearbyHost,
};
use connetto_core::device_cert::DeviceIdentity;

use crate::view::OfferView;

/// The line a VPN refusing the joined hotspot's dial shows.
pub const VPN_LINE: &str = "link: a VPN on this device blocks its own connections over the joined hotspot, only peers that connect to it can link";

/// The outcome a nearby join shows while it runs, the guard against a
/// second one.
pub const JOINING_NEARBY: &str = "joining the nearby host";

/// The outcome a hotspot join by its typed lines or its payload shows while
/// it runs.
pub const JOINING_HOTSPOT: &str = "joining the hotspot";

/// A device identity as `account/key prefix`, what the panel names devices
/// by, its own included.
#[must_use]
pub fn short_identity(identity: &DeviceIdentity) -> String {
    let key = identity.key().to_string();
    let prefix = key.get(..8).unwrap_or_default();
    format!("{}/{prefix}", identity.account())
}

/// One linked peer's line, with the address it is linked through where
/// known.
#[must_use]
pub fn linked_line(peer: &DeviceIdentity, remote: Option<SocketAddr>) -> String {
    match remote {
        Some(remote) => format!("linked: {} via {remote}", short_identity(peer)),
        None => format!("linked: {}", short_identity(peer)),
    }
}

/// The device's own peer line, the port it listens on and the identity it
/// links as, or `not serving` while it lacks either.
#[must_use]
pub fn peer_line(port: Option<u16>, me: Option<&DeviceIdentity>) -> String {
    match (port, me) {
        (Some(port), Some(me)) => format!("peer: listening on {port} as {}", short_identity(me)),
        _ => "peer: not serving".to_owned(),
    }
}

/// The host's beacon standing as the panel's line.
#[must_use]
pub fn beacon_line(state: BeaconState) -> String {
    match state {
        BeaconState::Advertising { prefix } => {
            let mut line = String::from("beacon: advertising ");
            for byte in prefix {
                let _ = write!(line, "{byte:02x}");
            }
            line
        }
        BeaconState::Off { reason } => format!("beacon: off ({reason})"),
    }
}

/// The offer's port line, `none` while the device serves no peer port.
#[must_use]
pub fn port_line(port: Option<u16>) -> String {
    port.map_or_else(|| "none".to_owned(), |port| port.to_string())
}

/// The payload as an SVG QR code, `None` where it does not fit one.
#[must_use]
pub fn payload_qr(payload: &str) -> Option<String> {
    let code = qrcode::QrCode::new(payload.as_bytes()).ok()?;
    Some(
        code.render::<qrcode::render::svg::Color<'_>>()
            .min_dimensions(200, 200)
            .build(),
    )
}

/// The offer this device hosts as the panel shows it, the passphrase copied
/// out before the offer zeroizes its own.
#[must_use]
pub fn offer_view(offer: &HotspotOffer) -> OfferView {
    let payload = offer.to_wifi_payload().to_string();
    OfferView {
        ssid: offer.ssid.clone(),
        passphrase: offer.passphrase.clone(),
        port: port_line(offer.port),
        qr_svg: payload_qr(&payload),
        payload,
    }
}

/// The outcome a started hotspot shows, naming a beacon that failed to
/// start.
#[must_use]
pub fn hosted_outcome<E: std::fmt::Display>(beacon: &Result<(), E>) -> String {
    match beacon {
        Ok(()) => "hosting".to_owned(),
        Err(err) => format!("hosting, the beacon is off: {err}"),
    }
}

/// The outcome a joined hotspot shows, the gateway the join reached.
#[must_use]
pub fn joined_outcome(gateway: IpAddr) -> String {
    format!("joined through the gateway {gateway}")
}

/// The offer the join form's typed lines name, a WPA2 network with the port
/// where one is typed.
///
/// # Errors
///
/// The outcome line naming what is wrong, an empty network name or
/// passphrase, or a port that is no port number.
pub fn typed_offer(ssid: &str, passphrase: &str, port: &str) -> Result<HotspotOffer, &'static str> {
    let ssid = ssid.trim();
    if ssid.is_empty() || passphrase.is_empty() {
        return Err("the ssid and the passphrase need filling");
    }
    let port = match port.trim() {
        "" => None,
        text => Some(
            text.parse::<u16>()
                .map_err(|_| "the port is not a number")?,
        ),
    };
    Ok(HotspotOffer::new(
        ssid,
        passphrase,
        HotspotSecurity::Wpa2,
        port,
    ))
}

/// The outcome a nearby join shows, the gateway and the host it reached.
#[must_use]
pub fn joined_nearby_outcome(gateway: IpAddr, host: &DeviceIdentity) -> String {
    format!(
        "joined through the gateway {gateway} as {}",
        short_identity(host)
    )
}

/// The permission a refused nearby join misses, which the panel asks for.
#[must_use]
pub fn nearby_permission(err: &JoinNearbyError) -> Option<&str> {
    match err {
        JoinNearbyError::Join(JoinError::MissingPermission(name)) => Some(name),
        _ => None,
    }
}

/// A nearby host's line, `nearby: <prefix>`, with the signal where known.
#[must_use]
pub fn nearby_line(host: &NearbyHost) -> String {
    let mut line = String::from("nearby: ");
    for byte in host.prefix {
        let _ = write!(line, "{byte:02x}");
    }
    if let Some(rssi) = host.rssi {
        let _ = write!(line, " at {rssi} dBm");
    }
    line
}

/// The host a nearby join picks, the strongest signal, ties broken by the
/// host.
#[must_use]
pub fn strongest(hosts: &[NearbyHost]) -> Option<HostId> {
    hosts
        .iter()
        .max_by_key(|known| (known.rssi.unwrap_or(i16::MIN), known.host))
        .map(|known| known.host)
}

#[cfg(test)]
mod tests {
    use super::{nearby_line, nearby_permission, strongest, typed_offer};
    use connetto_client::{BluetoothError, JoinError, JoinNearbyError, NearbyHost};

    /// The typed lines name a WPA2 offer, the network name trimmed and the
    /// port optional, and each missing or malformed line says what is wrong.
    #[test]
    fn the_typed_lines_name_an_offer_or_what_is_wrong() {
        let offer = typed_offer("  camp  ", "opensesame", " 4431 ").expect("an offer");
        assert_eq!(offer.ssid, "camp");
        assert_eq!(offer.passphrase, "opensesame");
        assert_eq!(offer.port, Some(4431));
        assert_eq!(
            typed_offer("camp", "opensesame", "")
                .expect("an offer")
                .port,
            None
        );
        let fill = "the ssid and the passphrase need filling";
        assert_eq!(typed_offer("   ", "opensesame", "").map(drop), Err(fill));
        assert_eq!(typed_offer("camp", "", "").map(drop), Err(fill));
        let port = "the port is not a number";
        assert_eq!(
            typed_offer("camp", "opensesame", "port").map(drop),
            Err(port)
        );
        assert_eq!(
            typed_offer("camp", "opensesame", "70000").map(drop),
            Err(port)
        );
    }

    /// A nearby join refused for a missing permission names it, so the
    /// panel asks for it as the typed and payload joins do.
    #[test]
    fn a_nearby_join_names_the_permission_it_misses() {
        let missing =
            JoinNearbyError::Join(JoinError::MissingPermission("Location Services".to_owned()));
        assert_eq!(nearby_permission(&missing), Some("Location Services"));
        let unsupported = JoinNearbyError::Bluetooth(BluetoothError::Unsupported);
        assert_eq!(nearby_permission(&unsupported), None);
    }

    /// The nearby join takes the strongest beacon, and a host the scan gave
    /// no signal for loses to any that has one.
    #[test]
    fn the_nearby_join_takes_the_strongest_beacon() {
        let host = |host: u64, rssi: Option<i16>| NearbyHost {
            host,
            prefix: [0; 8],
            rssi,
        };
        let hosts = [host(1, Some(-80)), host(2, Some(-50)), host(3, None)];
        assert_eq!(strongest(&hosts), Some(2));
        assert_eq!(strongest(&[]), None);
    }

    /// A beacon's line names its prefix in hex with its signal, the form the
    /// runners match.
    #[test]
    fn a_nearby_line_names_the_prefix_and_signal() {
        let with = NearbyHost {
            host: 1,
            prefix: [0x6d, 0x57, 0xb8, 0xa7, 0x88, 0x74, 0x78, 0x36],
            rssi: Some(-66),
        };
        assert_eq!(nearby_line(&with), "nearby: 6d57b8a788747836 at -66 dBm");
        let without_signal = NearbyHost {
            host: 2,
            prefix: [0xab; 8],
            rssi: None,
        };
        assert_eq!(nearby_line(&without_signal), "nearby: abababababababab");
    }
}

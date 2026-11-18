//! The one view the screen renders and the one action set the controls send.
//!
//! The view is every line the screen shows that no live query answers, and
//! the action every control the user has. The panes follow their live
//! queries themselves. Both derive `serde`, so the Tauri layer sends the same
//! two types between the page and the core unchanged, and file bytes travel
//! in the action as `Vec<u8>` with the file name, so the web and Tauri layers
//! take the same input.

pub use crate::LogoutState;
use serde::{Deserialize, Serialize};

/// The standing of this page's session, which part of the screen owns it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Session {
    /// The tab has not attached to the DB worker yet.
    #[default]
    Connecting,
    /// The worker parked a login URL, so the sign-in banner shows.
    SignInOffered,
    /// The worker parked the account picker.
    PickingAccount,
    /// A stored account is landing, so the pane skeletons show.
    Restoring,
    /// A session is live and the dashboard shows.
    SignedIn,
}

/// One catalogue row as the pane shows it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProductRow {
    /// The product's name.
    pub name: String,
    /// The price, as [`price_label`](crate::price_label) words it.
    pub price: String,
}

/// One stored account, its credential-store key and the label the account
/// bar shows for it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountEntry {
    /// The credential-store key.
    pub key: String,
    /// The decoded identity, or the raw key when the key does not decode.
    pub label: String,
}

/// The team notes panel's standing (R77).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamView {
    /// One line per applied, confirmed, rolled-back or refused peer write
    /// and per tidy pass, newest last.
    pub events: Vec<String>,
    /// How many provisional events arrived, which the notes' provisional
    /// accounts are read again on.
    pub provisional_events: u64,
    /// The last note write's outcome, `idle` before the first.
    pub outcome: String,
    /// Whether this build carries the peer link, which offers the tidy pass.
    pub peer: bool,
}

/// `team: <team>`, or `team: none` for an empty one.
#[must_use]
pub fn team_line(team: &str) -> String {
    if team.is_empty() {
        "team: none".to_owned()
    } else {
        format!("team: {team}")
    }
}

/// `teams: <a>, <b>` in sorted order, or `teams: none` for an empty roster.
#[must_use]
pub fn teams_line(teams: &[String]) -> String {
    let mut sorted = teams.to_vec();
    sorted.sort();
    sorted.dedup();
    if sorted.is_empty() {
        "teams: none".to_owned()
    } else {
        format!("teams: {}", sorted.join(", "))
    }
}

/// `members: <count>`, the picked team's synced roster.
#[must_use]
pub fn members_line(members: &[String]) -> String {
    format!("members: {}", members.len())
}

#[cfg(feature = "client")]
impl AccountEntry {
    /// The entry for `key`, labelled with its decoded identity.
    #[must_use]
    pub fn new(key: String) -> Self {
        Self {
            label: crate::account_label(&key),
            key,
        }
    }
}

/// The hotspot this device hosts, as the nearby panel shows its offer.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OfferView {
    /// The hotspot's network name.
    pub ssid: String,
    /// The hotspot's passphrase.
    pub passphrase: String,
    /// The device's peer port, `none` while it serves none.
    pub port: String,
    /// The offer as one `WIFI:` payload (R76 decision 23).
    pub payload: String,
    /// The payload drawn as an SVG QR code, absent where it fits none.
    pub qr_svg: Option<String>,
}

/// The nearby panel's standing, the peer link and the hotspot (R76).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NearbyView {
    /// `peer: listening on <port> as <identity>` or `peer: not serving`.
    pub peer_line: String,
    /// `bluetooth: <state>`.
    pub bluetooth_line: String,
    /// `beacon: advertising <prefix>` or `beacon: off (<reason>)`.
    pub beacon_line: String,
    /// One `linked: <identity> via <address>` line per linked peer, sorted.
    pub linked: Vec<String>,
    /// One `nearby: <prefix> at <rssi> dBm` line per host whose beacon shows.
    pub nearby: Vec<String>,
    /// The offer this device hosts, until the hotspot stops.
    pub offer: Option<OfferView>,
    /// The last hotspot or join outcome, `idle` before the first.
    pub outcome: String,
    /// The line a VPN refusing the joined hotspot's dial shows.
    pub vpn_line: Option<String>,
    /// Whether this device stands joined to another's hotspot.
    pub joined: bool,
    /// Whether this system hosts a hotspot from the app, never on the
    /// Apple devices, which only join (R77 decision 23).
    pub can_host: bool,
    /// Whether the runtime peer-link switch stands off, so no nearby phone
    /// links (R77).
    pub links_off: bool,
}

/// `links: on`, or `links: off` while the runtime peer-link switch refuses
/// every nearby link.
#[must_use]
pub fn links_line(off: bool) -> &'static str {
    if off { "links: off" } else { "links: on" }
}

/// Whether the nearby panel offers its Bluetooth button for the panel's
/// `bluetooth: <state>` line, only while the user can turn the radio on or
/// grant its permission, so a ready or absent Bluetooth shows no button.
#[must_use]
pub fn offers_bluetooth_on(bluetooth_line: &str) -> bool {
    matches!(
        bluetooth_line,
        "bluetooth: off" | "bluetooth: not permitted"
    )
}

/// Every line the screen shows.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct View {
    // The header.
    /// The status line, the client events' words.
    pub status: String,
    /// The gate line, shown once the worker reports the custody level.
    pub gate_line: Option<String>,
    /// The custody line, shown once the worker reports the custody level.
    pub custody_line: Option<String>,
    /// The device line, the identity and certificate standing on a native
    /// build and the no-device-identity statement in a browser.
    pub device_line: String,
    /// Whether the header offers the unlock, a native gate that is locked.
    pub offers_unlock: bool,

    // The account bar.
    /// The page's session standing.
    pub session: Session,
    /// The signed-in identity, absent while none is live.
    pub identity: Option<String>,
    /// The live account.
    pub account: Option<AccountEntry>,
    /// Every account the worker has offered.
    pub known_accounts: Vec<AccountEntry>,
    /// The picker's list, parked while the worker awaits a choice.
    pub picking: Option<Vec<AccountEntry>>,
    /// The account the last successful sign-in used, for the restore line.
    pub stored_account: Option<AccountEntry>,
    /// The session expiry warning line.
    pub expiry_warning: Option<String>,
    /// Whether the passkey gate can be offered from this window.
    pub enrol_offer: bool,
    /// The standing of the logout controls.
    pub logout: LogoutState,
    /// The pending count a refused delete held, the second-refusal standing.
    pub refused_pending: Option<u64>,

    // The orders pane.
    /// Whether the pane offers the insert through the backend writer, the
    /// phone proofs' Postgres path on the native build.
    pub backend_writer: bool,

    // The photos pane.
    /// How many pinned fetches finished, which the pane resolves its photos'
    /// display sources again on, since a fetch changes no row.
    pub pinned_fetches: u64,
    /// The refused uploads, each as an id and its refusal detail.
    pub refused_uploads: Vec<(String, String)>,
    /// The lost files, one id each, whose loss is unacknowledged.
    pub lost_bytes: Vec<String>,
    /// The last content-lane line.
    pub content_line: Option<String>,
    /// The last staging line.
    pub stage_line: Option<String>,

    // The header.
    /// The storage line, what this account and the shared store take.
    pub storage: String,

    // The notes pane.
    /// How many notes this window saved, which clears the draft on each.
    pub notes_saved: u64,

    // The retention pane.
    /// The replica mirror's total pages.
    pub pages: i64,
    /// The pages a trim can reclaim.
    pub free_pages: i64,

    // The your-data pane.
    /// The last export line.
    pub export_line: Option<String>,
    /// The last import line.
    pub import_line: Option<String>,

    /// The nearby panel, absent in a build without the peer link.
    pub nearby: Option<NearbyView>,
    /// The team notes panel, absent until a session stands.
    pub team: Option<TeamView>,

    /// Whether this window's tab client attached, the dashboard's gate.
    pub attached: bool,
}

impl View {
    /// The view before anything attaches, the page's first paint.
    #[must_use]
    pub fn initial() -> Self {
        Self {
            status: "connecting to the connetto stack".to_owned(),
            device_line: "no device identity in this build".to_owned(),
            storage: crate::STORAGE_UNREAD.to_owned(),
            ..Self::default()
        }
    }
}

/// Every control the user has.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Action {
    /// Open the login popup at the parked URL.
    SignIn,
    /// Run the native gate's unlock prompt.
    Unlock,
    /// Answer the parked account picker, `None` picking the new-account entry.
    ChooseAccount {
        /// The stored account key, or `None` for the new-account entry.
        account: Option<String>,
    },
    /// Add a new account to the worker's credential store.
    AddAccount,
    /// Insert one order through the backend writer, straight into Postgres.
    InsertBackend,
    /// Reboot the worker into another stored account.
    SwitchAccount {
        /// The stored account key.
        account: String,
    },
    /// Run the passkey gate enrolment ceremony.
    EnrolGate,
    /// Log out keeping the local data.
    LogOutKeep,
    /// Delete the local data and log out.
    LogOutDelete,
    /// The confirmed delete.
    ConfirmDelete,
    /// The forced delete after a refusal.
    ForceDelete,
    /// Abandon the logout flow.
    CancelLogout,
    /// Insert one new order.
    AddOrder,
    /// Remove one order row, the one the pane shows newest.
    RemoveOrder {
        /// The row's id, as its `Uuid` displays.
        id: String,
    },
    /// Stage one picked image file as a photo.
    StagePhoto {
        /// The file's name, which the mime guess reads.
        name: String,
        /// The file's bytes.
        bytes: Vec<u8>,
    },
    /// Pin every photos row's content.
    Pin,
    /// End the photos pin.
    Unpin,
    /// Fetch every pinned file the local store is missing.
    FetchPinned,
    /// Evict the unpinned, unreferenced content files.
    TidyContent,
    /// Clear one refusal so the next upload walk retries the file.
    RetryUpload {
        /// The refused file's id, hex.
        file_id: String,
    },
    /// Acknowledge every lost file.
    AcknowledgeLost,
    /// Save one device-private note.
    SaveNote {
        /// The note's body.
        body: String,
    },
    /// Run the replica's trim pass.
    FreeUpSpace,
    /// Read the replica mirror's footprint and the storage line again, which
    /// the panes ask for whenever the rows they show change.
    RereadStorage,
    /// Export this device's local data.
    Export,
    /// Restore one archive.
    Import {
        /// The archive's name.
        name: String,
        /// The archive's bytes.
        bytes: Vec<u8>,
    },
    /// Turn Bluetooth on, through the system's prompt where it asks.
    EnableBluetooth,
    /// Host a hotspot from the app.
    HostHotspot,
    /// Stop the hotspot this device hosts.
    StopHotspot,
    /// Join a hotspot by its offer's typed lines.
    JoinHotspot {
        /// The offer's network name.
        ssid: String,
        /// The offer's passphrase.
        passphrase: String,
        /// The offer's peer port, empty where it names none.
        port: String,
    },
    /// Join a hotspot by its `WIFI:` payload.
    JoinPayload {
        /// The pasted or scanned payload.
        payload: String,
    },
    /// Join the strongest host whose beacon shows.
    JoinNearby,
    /// Leave the joined hotspot.
    LeaveHotspot,
    /// Switch every nearby link on or off for the running app.
    SetPeerLinks {
        /// Whether nearby phones may link.
        on: bool,
    },
    /// Write one note into a team.
    AddTeamNote {
        /// The team's id.
        team: String,
        /// The note's body.
        body: String,
    },
    /// Run the eviction pass, which spares provisional rows.
    TidyTeam,
}

#[cfg(test)]
mod tests {
    use super::offers_bluetooth_on;

    /// The Bluetooth button stands only while the user can turn the radio on
    /// or grant its permission, and goes once Bluetooth is ready.
    #[test]
    fn the_bluetooth_button_stands_only_while_bluetooth_is_not_ready() {
        assert!(offers_bluetooth_on("bluetooth: off"));
        assert!(offers_bluetooth_on("bluetooth: not permitted"));
        assert!(!offers_bluetooth_on("bluetooth: ready"));
        assert!(!offers_bluetooth_on("bluetooth: unsupported"));
    }
}

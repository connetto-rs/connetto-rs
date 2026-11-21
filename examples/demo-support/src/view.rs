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

/// One stored account, its credential-store key, the label the account bar
/// shows for it and the login name this device last saw for it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountEntry {
    /// The credential-store key.
    pub key: String,
    /// The decoded identity, or the raw key when the key does not decode.
    pub label: String,
    /// The account's login name from the device's [`StoredNames`], absent
    /// until the account was live once with its `users` row synced.
    pub name: Option<String>,
}

/// The login name of each account this device stores, keyed by the account
/// id, so an account that is not live shows its name. The demo writes an
/// account's name whenever its own `users` row arrives while it is live.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StoredNames(std::collections::BTreeMap<String, String>);

impl StoredNames {
    /// The login name stored for `account`.
    #[must_use]
    pub fn name(&self, account: &str) -> Option<&str> {
        self.0.get(account).map(String::as_str)
    }

    /// Store `name` for `account`, answering whether the map changed, so a
    /// caller writes the map back only then.
    pub fn remember(&mut self, account: &str, name: &str) -> bool {
        if self.name(account) == Some(name) {
            return false;
        }
        self.0.insert(account.to_owned(), name.to_owned());
        true
    }

    /// Drop the name stored for `account`, answering whether the map changed.
    pub fn forget(&mut self, account: &str) -> bool {
        self.0.remove(account).is_some()
    }
}

/// A device on the peer link, named by its account and the first eight
/// characters of its device key.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PeerId {
    /// The account the device is signed in as.
    pub account: String,
    /// The first eight characters of the device key.
    pub key_prefix: String,
}

impl std::fmt::Display for PeerId {
    /// `account/key prefix`, what the nearby panel names devices by.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}/{}", self.account, self.key_prefix)
    }
}

/// One linked peer, with the address it is linked through where known.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct LinkedPeer {
    /// The linked device.
    pub peer: PeerId,
    /// The address the link runs over, where known.
    pub remote: Option<String>,
}

impl LinkedPeer {
    /// `linked: <peer> via <address>`, or `linked: <peer>` with no address.
    #[must_use]
    pub fn line(&self) -> String {
        match &self.remote {
            Some(remote) => format!("linked: {} via {remote}", self.peer),
            None => format!("linked: {}", self.peer),
        }
    }
}

/// The peer port this device listens on and the identity it links as.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Listening {
    /// The peer port.
    pub port: u16,
    /// This device.
    pub me: PeerId,
}

/// `peer: listening on <port> as <me>`, or `peer: not serving` while the
/// device lacks a port or an identity.
#[must_use]
pub fn peer_line(listening: Option<&Listening>) -> String {
    match listening {
        Some(Listening { port, me }) => format!("peer: listening on {port} as {me}"),
        None => "peer: not serving".to_owned(),
    }
}

/// One line of the team panel's peer activity, with the account that wrote
/// the row where the event names one.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamEvent {
    /// The event without its author, `provisional: applied <table> <key>`.
    pub line: String,
    /// The account the applied row came from.
    pub by: Option<String>,
}

impl TeamEvent {
    /// An event that names no account.
    #[must_use]
    pub fn plain(line: String) -> Self {
        Self { line, by: None }
    }

    /// The line as the panel prints it, `<line> by <account>` where the
    /// event names one.
    #[must_use]
    pub fn text(&self) -> String {
        match &self.by {
            Some(account) => format!("{} by {account}", self.line),
            None => self.line.clone(),
        }
    }
}

/// The team notes panel's standing (R77).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamView {
    /// One event per applied, confirmed, rolled-back or refused peer write
    /// and per tidy pass, newest last.
    pub events: Vec<TeamEvent>,
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
    /// The entry for `key`, labelled with its decoded identity and named
    /// from `names`.
    #[must_use]
    pub fn new(key: String, names: &StoredNames) -> Self {
        let label = crate::account_label(&key);
        Self {
            name: names.name(&label).map(str::to_owned),
            label,
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
    /// The port and identity this device links as, absent while it lacks
    /// either, shown through [`peer_line`].
    pub listening: Option<Listening>,
    /// `bluetooth: <state>`.
    pub bluetooth_line: String,
    /// `beacon: advertising <prefix>` or `beacon: off (<reason>)`.
    pub beacon_line: String,
    /// Every linked peer, sorted.
    pub linked: Vec<LinkedPeer>,
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

/// The text exposed by an account badge.
#[must_use]
pub fn badge_text(account: &str, name: Option<&str>) -> String {
    let short = crate::short_id(account);
    name.map_or_else(|| short.to_owned(), |name| format!("{name} {short}"))
}

/// The stored account whose deletion the screen is handling.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredDelete {
    /// The credential-store key.
    pub account: String,
    /// The deletion's standing.
    pub state: StoredDeleteState,
}

/// The controls offered for a stored account deletion.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StoredDeleteState {
    /// Ask permission to discard pending writes.
    Confirm {
        /// The number of pending writes.
        unsynced_count: usize,
    },
    /// Ask permission to delete a replica that could not be read, and why.
    Unreadable(String),
    /// A deletion is running.
    Working,
    /// The deletion's failure.
    Error(String),
}

/// A platform-independent stored deletion result.
#[derive(Debug, PartialEq, Eq)]
pub enum StoredDeleteOutcome {
    /// Remove the account, retaining an incomplete teardown report if present.
    Done(Option<String>),
    /// Keep the account pending permission to discard writes.
    Pending(usize),
    /// Keep an unreadable account pending explicit permission, with the reason.
    Unreadable(String),
    /// Keep the account and show the failure.
    Error(String),
}

#[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
impl From<Result<(), connetto_client::teardown::ForgetError>> for StoredDeleteOutcome {
    fn from(result: Result<(), connetto_client::teardown::ForgetError>) -> Self {
        use connetto_client::teardown::{ForgetError, ForgetRefusal};
        match result {
            Ok(()) => Self::Done(None),
            Err(ForgetError::Incomplete(report)) => Self::Done(Some(report.to_string())),
            Err(ForgetError::Refused(ForgetRefusal::Unsynced(writes))) => {
                Self::Pending(writes.len())
            }
            Err(ForgetError::Refused(ForgetRefusal::Unreadable(error))) => {
                Self::Unreadable(error.to_string())
            }
            Err(ForgetError::Refused(refusal)) => Self::Error(refusal.to_string()),
        }
    }
}

#[cfg(all(target_arch = "wasm32", feature = "web"))]
impl From<Result<connetto_web::auth::StoredForgetOutcome, connetto_web::auth::AuthError>>
    for StoredDeleteOutcome
{
    fn from(
        result: Result<connetto_web::auth::StoredForgetOutcome, connetto_web::auth::AuthError>,
    ) -> Self {
        use connetto_web::auth::{StoredForgetOutcome, StoredForgetRefusal};
        match result {
            Ok(StoredForgetOutcome::Done { failed }) => {
                Self::Done((!failed.is_empty()).then(|| format!("Incomplete deletion: {failed:?}")))
            }
            Ok(StoredForgetOutcome::Refused(StoredForgetRefusal::Pending(pending))) => {
                Self::Pending(usize::try_from(pending.len()).unwrap_or(usize::MAX))
            }
            Ok(StoredForgetOutcome::Refused(StoredForgetRefusal::Unreadable(reason))) => {
                Self::Unreadable(reason)
            }
            Ok(StoredForgetOutcome::Refused(refusal)) => Self::Error(format!("{refusal:?}")),
            Err(error) => Self::Error(error.to_string()),
        }
    }
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
    /// The stored account deletion, independent of live logout.
    pub stored_delete: Option<StoredDelete>,

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
    /// Whether a stored deletion prevents another action.
    #[must_use]
    pub fn stored_delete_working(&self) -> bool {
        matches!(
            self.stored_delete,
            Some(StoredDelete {
                state: StoredDeleteState::Working,
                ..
            })
        )
    }

    /// Apply a deletion result while leaving the live account untouched.
    pub fn finish_stored_delete(&mut self, account: &str, outcome: StoredDeleteOutcome) {
        let state = match outcome {
            StoredDeleteOutcome::Done(error) => {
                self.known_accounts.retain(|entry| entry.key != account);
                if let Some(entries) = &mut self.picking {
                    entries.retain(|entry| entry.key != account);
                }
                if self
                    .stored_account
                    .as_ref()
                    .is_some_and(|entry| entry.key == account)
                {
                    self.stored_account = None;
                }
                error.map(StoredDeleteState::Error)
            }
            StoredDeleteOutcome::Pending(unsynced_count) => {
                Some(StoredDeleteState::Confirm { unsynced_count })
            }
            StoredDeleteOutcome::Unreadable(reason) => Some(StoredDeleteState::Unreadable(reason)),
            StoredDeleteOutcome::Error(error) => Some(StoredDeleteState::Error(error)),
        };
        self.stored_delete = state.map(|state| StoredDelete {
            account: account.to_owned(),
            state,
        });
    }

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

    /// Give every entry of `account` the login name `name`, the live one,
    /// the stored ones and the picker's.
    pub fn name_account(&mut self, account: &str, name: &str) {
        let entries = self
            .account
            .iter_mut()
            .chain(self.stored_account.iter_mut())
            .chain(self.known_accounts.iter_mut())
            .chain(self.picking.iter_mut().flatten());
        for entry in entries.filter(|entry| entry.label == account) {
            entry.name = Some(name.to_owned());
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
    /// Delete a stored account without signing out the live account.
    DeleteStoredAccount {
        /// The credential-store key.
        account: String,
        /// Whether discarding unreadable or pending writes is permitted.
        force: bool,
    },
    /// Dismiss the stored deletion controls.
    CancelStoredDelete,
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
    use super::{
        LinkedPeer, Listening, PeerId, StoredNames, TeamEvent, offers_bluetooth_on, peer_line,
    };

    #[test]
    fn stored_delete_outcomes_preserve_or_remove_the_entry() {
        use super::{AccountEntry, StoredDeleteOutcome, StoredDeleteState, View};
        for (outcome, removed, state) in [
            (StoredDeleteOutcome::Done(None), true, None),
            (
                StoredDeleteOutcome::Done(Some("storage failed".into())),
                true,
                Some(StoredDeleteState::Error("storage failed".into())),
            ),
            (
                StoredDeleteOutcome::Pending(3),
                false,
                Some(StoredDeleteState::Confirm { unsynced_count: 3 }),
            ),
            (
                StoredDeleteOutcome::Unreadable("no replica key".into()),
                false,
                Some(StoredDeleteState::Unreadable("no replica key".into())),
            ),
            (
                StoredDeleteOutcome::Error("shared".into()),
                false,
                Some(StoredDeleteState::Error("shared".into())),
            ),
        ] {
            let entry = AccountEntry {
                key: "key".into(),
                label: "identity".into(),
                name: Some("name".into()),
            };
            let mut view = View {
                known_accounts: vec![entry.clone()],
                picking: Some(vec![entry.clone()]),
                stored_account: Some(entry),
                ..View::default()
            };
            view.finish_stored_delete("key", outcome);
            assert_eq!(view.known_accounts.is_empty(), removed);
            assert_eq!(view.picking.as_ref().unwrap().is_empty(), removed);
            assert_eq!(view.stored_account.is_none(), removed);
            assert_eq!(view.stored_delete.map(|delete| delete.state), state);
        }
    }

    #[test]
    fn badge_text_has_only_name_and_short_identity() {
        assert_eq!(
            super::badge_text("d5e24135-1234", Some("alice")),
            "alice d5e24135"
        );
        assert_eq!(super::badge_text("d5e24135-1234", None), "d5e24135");
    }

    fn alice_phone() -> PeerId {
        PeerId {
            account: "3f2a9c1e-0000-5000-8000-000000000001".to_owned(),
            key_prefix: "a1b2c3d4".to_owned(),
        }
    }

    /// The nearby lines the phone proof drivers read keep their words, so
    /// the structured peers change nothing on the page.
    #[test]
    fn the_nearby_lines_keep_the_words_the_proof_drivers_read() {
        let me = alice_phone();
        assert_eq!(
            peer_line(Some(&Listening {
                port: 7000,
                me: me.clone()
            })),
            "peer: listening on 7000 as 3f2a9c1e-0000-5000-8000-000000000001/a1b2c3d4"
        );
        assert_eq!(peer_line(None), "peer: not serving");
        let linked = LinkedPeer {
            peer: me.clone(),
            remote: Some("192.168.49.1:7000".to_owned()),
        };
        assert_eq!(
            linked.line(),
            "linked: 3f2a9c1e-0000-5000-8000-000000000001/a1b2c3d4 via 192.168.49.1:7000"
        );
        let linked = LinkedPeer {
            peer: me,
            remote: None,
        };
        assert_eq!(
            linked.line(),
            "linked: 3f2a9c1e-0000-5000-8000-000000000001/a1b2c3d4"
        );
    }

    /// A team event prints its author last, as the applied line always has.
    #[test]
    fn a_team_event_prints_its_author_last() {
        let applied = TeamEvent {
            line: "provisional: applied team_notes 9f1c".to_owned(),
            by: Some("bob".to_owned()),
        };
        assert_eq!(
            applied.text(),
            "provisional: applied team_notes 9f1c by bob"
        );
        let tidy = TeamEvent::plain("tidy: 0 provisional row(s) remain".to_owned());
        assert_eq!(tidy.text(), "tidy: 0 provisional row(s) remain");
    }

    /// The stored names change only on a new or different name, so the
    /// demo writes its file or `localStorage` entry only then.
    #[test]
    fn the_stored_names_change_only_on_a_new_name() {
        let mut names = StoredNames::default();
        assert!(names.remember("alice-id", "alice"));
        assert!(!names.remember("alice-id", "alice"));
        assert!(names.remember("alice-id", "alice2"));
        assert_eq!(names.name("alice-id"), Some("alice2"));
        assert_eq!(names.name("bob-id"), None);
        assert!(names.forget("alice-id"));
        assert!(!names.forget("alice-id"));
    }

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

#[cfg(all(test, not(target_arch = "wasm32"), feature = "native"))]
mod native_delete_tests {
    use super::StoredDeleteOutcome;
    use connetto_client::ClientError;
    use connetto_client::teardown::{ForgetError, ForgetIncomplete, ForgetRefusal};

    #[test]
    fn native_outcomes_cover_each_refusal_and_incomplete_teardown() {
        assert_eq!(
            StoredDeleteOutcome::from(Ok(())),
            StoredDeleteOutcome::Done(None)
        );
        assert_eq!(
            StoredDeleteOutcome::from(Err(ForgetError::Refused(ForgetRefusal::Unsynced(
                Vec::new()
            )))),
            StoredDeleteOutcome::Pending(0)
        );
        assert_eq!(
            StoredDeleteOutcome::from(Err(ForgetError::Refused(ForgetRefusal::Unreadable(
                ClientError::Session("unreadable".into())
            )))),
            StoredDeleteOutcome::Unreadable(ClientError::Session("unreadable".into()).to_string())
        );
        for refusal in [
            ForgetRefusal::NoReplica,
            ForgetRefusal::LiveAccount,
            ForgetRefusal::UnknownAccount,
            ForgetRefusal::Shared(ClientError::Session("shared".into())),
        ] {
            let message = refusal.to_string();
            assert_eq!(
                StoredDeleteOutcome::from(Err(ForgetError::Refused(refusal))),
                StoredDeleteOutcome::Error(message)
            );
        }
        let mut report = ForgetIncomplete::default();
        report.revoke = Some(ClientError::Transport("offline".into()));
        let message = report.to_string();
        assert!(message.contains("revoking the session"));
        assert_eq!(
            StoredDeleteOutcome::from(Err(ForgetError::Incomplete(Box::new(report)))),
            StoredDeleteOutcome::Done(Some(message))
        );
    }
}

#[cfg(all(test, target_arch = "wasm32", feature = "web"))]
mod browser_delete_tests {
    use super::StoredDeleteOutcome;
    use connetto_web::auth::{
        AuthError, PendingWork, StoredForgetFailure, StoredForgetOutcome, StoredForgetRefusal,
    };

    #[test]
    fn browser_outcomes_cover_each_refusal_failure_and_timeout() {
        assert_eq!(
            StoredDeleteOutcome::from(Ok(StoredForgetOutcome::Done { failed: Vec::new() })),
            StoredDeleteOutcome::Done(None)
        );
        let pending = PendingWork {
            content_files: 2,
            held_files: 1,
            ..PendingWork::default()
        };
        assert_eq!(
            StoredDeleteOutcome::from(Ok(StoredForgetOutcome::Refused(
                StoredForgetRefusal::Pending(pending)
            ))),
            StoredDeleteOutcome::Pending(3)
        );
        assert_eq!(
            StoredDeleteOutcome::from(Ok(StoredForgetOutcome::Refused(
                StoredForgetRefusal::Unreadable("unreadable".into())
            ))),
            StoredDeleteOutcome::Unreadable("unreadable".into())
        );
        for refusal in [
            StoredForgetRefusal::LiveAccount,
            StoredForgetRefusal::UnknownAccount,
            StoredForgetRefusal::Shared("shared".into()),
        ] {
            let message = format!("{refusal:?}");
            assert_eq!(
                StoredDeleteOutcome::from(Ok(StoredForgetOutcome::Refused(refusal))),
                StoredDeleteOutcome::Error(message)
            );
        }
        let failed = vec![
            StoredForgetFailure::Revoke("offline".into()),
            StoredForgetFailure::Storage("busy".into()),
        ];
        let message = format!("Incomplete deletion: {failed:?}");
        assert_eq!(
            StoredDeleteOutcome::from(Ok(StoredForgetOutcome::Done { failed })),
            StoredDeleteOutcome::Done(Some(message))
        );
        for error in [
            AuthError::Store("store".into()),
            AuthError::Request("request".into()),
            AuthError::Transient("offline".into()),
            AuthError::Decode,
            AuthError::StateMismatch,
            AuthError::Cancelled,
            AuthError::Context("context".into()),
            AuthError::Undecryptable("key".into()),
            AuthError::Locked {
                detail: "locked".into(),
            },
        ] {
            let message = error.to_string();
            assert_eq!(
                StoredDeleteOutcome::from(Err(error)),
                StoredDeleteOutcome::Error(message)
            );
        }
    }
}

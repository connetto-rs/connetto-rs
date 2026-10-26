//! The one view the screen renders and the one action set the controls send.
//!
//! The view is every line the screen shows and the action every control the
//! user has. Both derive `serde`, so the Tauri layer sends the same two types
//! over invoke unchanged, and file bytes travel in the action as `Vec<u8>`
//! with the file name, so the web and Tauri layers take the same input.

use std::collections::HashMap;

pub use connetto_demo_support::LogoutState;
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

/// One orders row as the pane shows it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderRow {
    /// The row's key.
    pub id: String,
    /// The row's quantity.
    pub quantity: i64,
}

/// One photos row as the pane shows it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhotoRow {
    /// The row's key.
    pub id: String,
    /// The content state the server reports, `pending` before it does.
    pub state: String,
}

/// One notes row as the pane shows it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoteRow {
    /// The row's key.
    pub id: i64,
    /// The note's text.
    pub body: String,
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

    // The account bar.
    /// The page's session standing.
    pub session: Session,
    /// The signed-in identity, absent while none is live.
    pub identity: Option<String>,
    /// The decoded label of the live account key.
    pub account: Option<String>,
    /// Every account key the worker has offered.
    pub known_accounts: Vec<String>,
    /// The picker's list, parked while the worker awaits a choice.
    pub picking: Option<Vec<String>>,
    /// The account the last successful sign-in used, for the restore line.
    pub stored_account: Option<String>,
    /// The session expiry warning line.
    pub expiry_warning: Option<String>,
    /// Whether the passkey gate can be offered from this window.
    pub enrol_offer: bool,
    /// The standing of the logout controls.
    pub logout: LogoutState,
    /// The pending count a refused delete held, the second-refusal standing.
    pub refused_pending: Option<u64>,

    // The orders pane.
    /// The visible orders rows, newest first.
    pub orders: Vec<OrderRow>,
    /// The server's `COUNT(*)`, absent until its first push.
    pub server_count: Option<i64>,
    /// The server's grouped `COUNT(*)` line.
    pub grouped: String,

    // The photos pane.
    /// The photos rows, oldest first.
    pub photos: Vec<PhotoRow>,
    /// Each row's display source by row key, a server URL or a local `data:`
    /// URL.
    pub photo_thumbs: HashMap<String, String>,
    /// The refused uploads, each as an id and its refusal detail.
    pub refused_uploads: Vec<(String, String)>,
    /// The lost files, one id each, whose loss is unacknowledged.
    pub lost_bytes: Vec<String>,
    /// The last content-lane line.
    pub content_line: Option<String>,
    /// The last staging line.
    pub stage_line: Option<String>,

    // The notes pane.
    /// The device-private notes rows, oldest first.
    pub notes: Vec<NoteRow>,
    /// How many notes this window saved, which clears the draft on each.
    pub notes_saved: u64,

    // The retention pane.
    /// The replica mirror's total pages.
    pub pages: i64,
    /// The pages a trim can reclaim.
    pub free_pages: i64,
    /// The rows the live subscriptions cover.
    pub covered_rows: usize,

    // The your-data pane.
    /// The last export line.
    pub export_line: Option<String>,
    /// The last import line.
    pub import_line: Option<String>,

    /// Whether this window's tab client attached, the dashboard's gate.
    pub attached: bool,
}

impl View {
    /// The view before anything attaches, the page's first paint.
    #[must_use]
    pub fn initial() -> Self {
        Self {
            status: "connecting to the connetto stack".to_owned(),
            grouped: connetto_demo_support::grouped_label(&HashMap::new()),
            ..Self::default()
        }
    }
}

/// Every control the user has.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Action {
    /// Open the login popup at the parked URL.
    SignIn,
    /// Answer the parked account picker, `None` picking the new-account entry.
    ChooseAccount {
        /// The stored account key, or `None` for the new-account entry.
        account: Option<String>,
    },
    /// Add a new account to the worker's credential store.
    AddAccount,
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
    /// Remove the newest order row.
    RemoveNewest,
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
    /// Export this device's local data.
    Export,
    /// Restore one archive.
    Import {
        /// The archive's name.
        name: String,
        /// The archive's bytes.
        bytes: Vec<u8>,
    },
}

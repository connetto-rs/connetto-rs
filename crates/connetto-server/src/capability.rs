//! Share keys: the deployment's key type, and the authorized mint call.
//!
//! A capability is a connetto-signed token asserting the bearer is a named
//! subject and nothing more. The permission attached to that name is an
//! ordinary row in the application's own table, gated by an ordinary policy, so
//! withdrawing a share is deleting that row and nothing here is consulted at
//! use time beyond the signature. See
//! `docs/architecture/12-identity-session-capability.md`.
//!
//! [`CapabilityKey`] is the one seam: the deployment's key type implements it,
//! naming how a fresh key is minted and how the keys a caller holds are packed
//! into the single Postgres setting a policy compares against. `String`
//! implements it, and is the default.
//!
//! [`CapabilityIssuer`] is the library call an application makes from its own
//! handler. connetto gains no endpoint for it.

use core::fmt::Display;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use connetto_core::auth::{
    CapabilityKey, CapabilitySubject, ContentCaller, DEFAULT_USER_SETTING, Principal, UnsetSettings,
};
use connetto_core::{KindRoles, SessionId};
use diesel::sql_types::{Bool, Text};
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use subql::ColumnId;
use subql::backend::{Postgres, Value};
use subql::visibility::{RowWrite, Verdict, VisibilityPolicy, WriteOp};

use crate::authn::token::{AuthConfig, TokenAuthority, TokenError};
use crate::reserve::{ReaderGate, ReaderPermit};
use crate::row_view::ValuesRow;
use crate::snapshot::RowSource;
use crate::throttle::Tier;

/// How a deployment makes a fresh share key.
///
/// Separate from [`CapabilityKey`] because only the issuing side mints one.
/// A replica needs the packing to answer the policies it holds and has no
/// business creating a bearer secret.
pub trait MintCapabilityKey: CapabilityKey {
    /// Mint a fresh key. It is a bearer secret, so it must be unguessable.
    fn mint() -> Self;
}

/// The default share-key: `key:` followed by a version 4 UUID. 122 bits of
/// randomness, and no rendering of it can contain the separator.
impl MintCapabilityKey for String {
    fn mint() -> Self {
        format!("key:{}", uuid::Uuid::new_v4())
    }
}

diesel::define_sql_function! {
    /// Postgres `set_config`, the only way to hand a value to a policy. The
    /// query DSL has no other spelling for a session setting, so it is declared
    /// here as a typed function rather than written as a raw string.
    fn set_config(name: Text, value: Text, is_local: Bool) -> Text;
}

/// Bind a write's writer and sequence under `W`'s settings for the rest of
/// the transaction, so the deployment stamps the rows it writes (R105
/// decision 6).
pub(crate) async fn bind_writer<W: crate::writer_schema::ConnettoWriterSchema>(
    conn: &mut AsyncPgConnection,
    writer: i64,
    seq: i64,
) -> diesel::QueryResult<()> {
    diesel::select((
        set_config(W::WRITER_SETTING, writer.to_string(), true),
        set_config(W::SEQ_SETTING, seq.to_string(), true),
    ))
    .execute(conn)
    .await
    .map(drop)
}

/// Render the caller for a transaction or a ticket: the identity, the login's
/// session, the share keys packed by the deployment's [`CapabilityKey`], and
/// the bot list the caller's connection binds.
///
/// The one renderer of a caller's values. A half the caller does not hold
/// stays absent, never `""`, so an owner comparison is NULL and hides the row
/// while a public predicate still returns its own.
pub(crate) fn rendered_caller<Id: Display, Key: CapabilityKey>(
    caller: &Principal<Id, Key>,
) -> ContentCaller {
    ContentCaller::new(
        caller
            .identity()
            .map(|identity| identity.user_id.to_string()),
        login_session(caller),
        Key::subjects(caller.capabilities()),
        bot_list_for(caller),
    )
}

/// The session of the login the caller's identity came from, the one the auth
/// store keeps live, or `None` for a caller holding no login.
///
/// An identity resolves only from a login, whose handle the auth store minted
/// and keeps. A share key's run has a handle minted for it and never stored,
/// so it names none.
pub(crate) fn login_session<Id, Key>(caller: &Principal<Id, Key>) -> Option<SessionId> {
    caller.identity().map(|_| caller.session_id())
}

/// The bot list one caller's connections carry in `app.bot_list`, the value
/// every path that runs SQL as a caller and the watcher's request answer
/// both bind, so Postgres and the local gate hold the caller to one list.
///
/// The wildcard for a caller holding nothing, and the list's own text for the
/// login that resolved one, read from the caller the one place it lives.
pub(crate) fn bot_list_for<Id: Display, Key: CapabilityKey>(caller: &Principal<Id, Key>) -> String {
    caller.bot_list().map_or_else(
        || connetto_core::BOT_LIST_WILDCARD.to_owned(),
        connetto_core::bots::BotList::text,
    )
}

/// The entries the cap policy's `string_to_array` reads off a bound bot list, none for the empty text.
pub(crate) fn bot_list_entries(text: &str) -> impl Iterator<Item = &str> {
    (!text.is_empty())
        .then(|| text.split(connetto_core::BOT_LIST_SEPARATOR))
        .into_iter()
        .flatten()
}

/// The bucket a per-caller meter charges: whatever names this caller, else
/// the run's own handle.
///
/// Namespaced by [`ContentCaller::storage_key`], so an identity and a share
/// key that render alike meter separately. A caller holding nothing has no
/// name to carry across runs, so it meters per run rather than sharing one
/// bucket with every other anonymous caller.
///
/// The one keying for every meter. Keying on the identity alone would hand a
/// key-only caller a fresh allowance on every reconnect, because the fallback
/// is per run while the grants it presents are not.
pub(crate) fn meter_key<Id: Display, Key: CapabilityKey>(
    caller: &Principal<Id, Key>,
    run: impl Display,
) -> String {
    rendered_caller(caller)
        .storage_key(Key::SEPARATOR)
        .unwrap_or_else(|| run.to_string())
}

/// How a deployment's policies read the caller: the setting naming the
/// identity, which of the identity and the subjects may be left unset, and
/// the role each kind of caller acts as.
///
/// One value for every path that binds a caller, so the snapshot, the write
/// and the visibility check cannot disagree on any of them. A source reading
/// on the owner pool, past every policy, names no kind roles and stays the
/// role it logged in as.
#[derive(Debug, Clone)]
pub(crate) struct BindingSettings {
    user: Arc<str>,
    unset: UnsetSettings,
    kind_roles: Option<KindRoles>,
}

impl Default for BindingSettings {
    fn default() -> Self {
        Self {
            user: DEFAULT_USER_SETTING.into(),
            unset: UnsetSettings::ALWAYS_BOUND,
            kind_roles: None,
        }
    }
}

impl BindingSettings {
    /// The same settings, reading the identity from `user`.
    pub(crate) fn with_user(mut self, user: Arc<str>) -> Self {
        self.user = user;
        self
    }

    /// The same settings, `unset` naming the ones a caller may leave unset.
    pub(crate) const fn with_unset(mut self, unset: UnsetSettings) -> Self {
        self.unset = unset;
        self
    }

    /// The same settings, each kind acting as the role `kind_roles` names.
    pub(crate) fn with_kind_roles(mut self, kind_roles: KindRoles) -> Self {
        self.kind_roles = Some(kind_roles);
        self
    }
}

/// The caller rendered for one RLS transaction, under the setting names this
/// deployment binds them to and the role its kind acts as.
///
/// Built outside the transaction (the values are owned so the apply future
/// stays `Send`) and applied as its first statement. Every path that runs SQL
/// as a caller goes through this, so the snapshot, the write, and the per-row
/// visibility check cannot answer differently about what the caller holds.
pub(crate) struct CallerBinding {
    caller: ContentCaller,
    settings: BindingSettings,
    setting: &'static str,
    separator: char,
    bot_list: String,
    /// The caller's kind role, `None` on a source that names no kind roles.
    role: Option<Arc<str>>,
}

/// The value of PostgreSQL's `role` setting that returns a transaction to
/// its login role, which `RESET ROLE` sets.
const NO_ROLE: &str = "none";

impl CallerBinding {
    /// Render `caller` under the deployment's key binding and `settings`.
    pub(crate) fn of<Id: Display, Key: CapabilityKey>(
        caller: &Principal<Id, Key>,
        settings: &BindingSettings,
    ) -> Self {
        Self {
            caller: rendered_caller(caller),
            settings: settings.clone(),
            setting: Key::SETTING,
            separator: Key::SEPARATOR,
            bot_list: bot_list_for(caller),
            role: settings
                .kind_roles
                .as_ref()
                .map(|roles| roles.shared_role(caller.kind())),
        }
    }

    /// The role the transaction acts as, the login role when the source
    /// names no kind roles.
    fn role(&self) -> &str {
        self.role.as_deref().unwrap_or(NO_ROLE)
    }

    /// The identity and the packed subjects to bind, a half the caller does
    /// not hold taking the value its declaration reads as absent.
    fn values(&self) -> (String, String) {
        let unset = self.settings.unset;
        let user = self
            .caller
            .identity()
            .unwrap_or_else(|| unset.absent_user())
            .to_owned();
        let subjects = self
            .caller
            .packed_subjects(self.separator)
            .unwrap_or_else(|| unset.absent_subjects().to_owned());
        (user, subjects)
    }

    /// Bind all three values and switch to the caller's kind role for the
    /// rest of the transaction, in one statement.
    ///
    /// All the settings are always bound. A half the caller does not hold
    /// takes `''` when the deployment declared it may be unset, and the
    /// absent marker otherwise. The bot list is the one value
    /// [`bot_list_for`] reads for the caller. Leaving one unbound would read
    /// as `''` to the next caller on a pooled connection, which a policy not
    /// declared to expect it must never see. The role is set as `SET LOCAL
    /// ROLE` sets it, so PostgreSQL holds every later statement to the kind's
    /// grants and policies until the transaction ends or
    /// [`reset_role`] returns it to the reader.
    pub(crate) async fn apply(self, conn: &mut AsyncPgConnection) -> diesel::QueryResult<()> {
        let (user, subjects) = self.values();
        let role = self.role().to_owned();
        diesel::select((
            set_config(self.settings.user.to_string(), user, true),
            set_config(self.setting, subjects, true),
            set_config(connetto_core::BOT_LIST_SETTING, self.bot_list, true),
            set_config("role", role, true),
        ))
        .execute(conn)
        .await
        .map(drop)
    }

    /// The same binding as [`apply`](Self::apply), rendered as SQL statement
    /// strings for a path that cannot run typed queries: the re-execution
    /// connector takes its per-transaction setup as plain statements
    /// (`SessionSetup`), so a per-viewer read binds its caller through these
    /// (R85). Values ride as quoted literals with their quotes doubled, the
    /// one escape single-quoted Postgres literals need.
    pub(crate) fn setup_statements(&self) -> Vec<String> {
        fn quoted(value: &str) -> String {
            value.replace('\'', "''")
        }
        let (user, subjects) = self.values();
        [
            (&*self.settings.user, user.as_str()),
            (self.setting, subjects.as_str()),
            (connetto_core::BOT_LIST_SETTING, self.bot_list.as_str()),
            ("role", self.role()),
        ]
        .into_iter()
        .map(|(setting, value)| {
            format!(
                "SELECT set_config('{}', '{}', true)",
                quoted(setting),
                quoted(value)
            )
        })
        .collect()
    }

    /// Whether this caller holds anything to be read as.
    pub(crate) fn binds_a_caller(&self) -> bool {
        self.caller.identity().is_some() || !self.caller.subjects().is_empty()
    }
}

/// Return the transaction to the reader role it logged in as, the role
/// connetto's bookkeeping is granted to, keeping the caller's settings bound.
pub(crate) async fn reset_role(conn: &mut AsyncPgConnection) -> diesel::QueryResult<()> {
    diesel::select(set_config("role", NO_ROLE, true))
        .execute(conn)
        .await
        .map(drop)
}

/// The write verbs a share certifies, beside the reading every share certifies.
///
/// The application is the only party that knows what its own permission row
/// grants, so it names the verbs and connetto certifies exactly those. A read
/// share names none, which is [`ShareLevel::read`].
///
/// # Why creating is not among them
///
/// A share names a table and one row's key, and a write question is judged on
/// the row versions its verb needs. Creating is judged on the row being
/// created, which a mint does not hold and cannot: whatever the bearer later
/// inserts is a different row under a different key, so a question asked now
/// about the shared row says nothing about it. Handing the row the mint read
/// over as the row being created asks whether the caller may create a row that
/// already exists, which is a different question wearing the right name.
///
/// ```
/// use connetto_server::ShareLevel;
///
/// // A share that lets the bearer edit the row but never remove it.
/// let level = ShareLevel::read().with_update();
/// assert!(!level.is_read_only());
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct ShareLevel {
    update: bool,
    delete: bool,
}

impl ShareLevel {
    /// Certify reading and nothing else.
    #[must_use]
    pub const fn read() -> Self {
        Self {
            update: false,
            delete: false,
        }
    }

    /// Also certify replacing the row's values.
    #[must_use]
    pub const fn with_update(mut self) -> Self {
        self.update = true;
        self
    }

    /// Also certify removing the row.
    #[must_use]
    pub const fn with_delete(mut self) -> Self {
        self.delete = true;
        self
    }

    /// Whether this share certifies no write at all.
    #[must_use]
    pub const fn is_read_only(self) -> bool {
        !self.update && !self.delete
    }

    /// The named verbs as questions about `row`, in a fixed order, so a mint
    /// asks the same questions in the same sequence however the caller built
    /// the level.
    ///
    /// A mint holds one version of the row, so a replacement is asked as
    /// [`RowWrite::UpdateUsing`], which is subql's question for exactly this
    /// case: whether the row as it stands may be replaced at all. It is the
    /// weaker half of the full replacement question, and it is the half a
    /// delegated permission over a row needs. `read` is what both verbs read,
    /// the key a shared write names the row by.
    fn writes<'a, R: ?Sized>(
        self,
        row: &'a R,
        read: &'a [ColumnId],
    ) -> impl Iterator<Item = RowWrite<'a, R>> {
        [
            (self.update, RowWrite::UpdateUsing { old: row, read }),
            (self.delete, RowWrite::Delete { old: row, read }),
        ]
        .into_iter()
        .filter_map(|(named, write)| named.then_some(write))
    }

    /// The named verbs, in the order the mint asks them, so an
    /// application can read back what a share certified.
    ///
    /// The unit stands in for a row because [`RowWrite::op`] reads the variant
    /// and never the row, which keeps one ordered list rather than two that
    /// can drift.
    pub fn verbs(self) -> impl Iterator<Item = WriteOp> {
        self.writes(&(), &[]).map(|write| write.op())
    }
}

/// One write verb as prose, for a refusal a person reads.
const fn verb(op: WriteOp) -> &'static str {
    match op {
        WriteOp::Insert => "insert into",
        WriteOp::Update => "update",
        WriteOp::Delete => "delete from",
    }
}

/// A share key that was minted, and the token proving the bearer holds it.
///
/// The application writes the row granting this key access, so the two agree on
/// the name by construction. What the application must not skip is the policy
/// on that table: connetto checked the caller may read the resource it named,
/// and only a `WITH CHECK` on the grant row makes the row that actually lands
/// answer to the same rule.
#[derive(Debug, Clone)]
pub struct IssuedCapability<Key> {
    /// The minted key, for the row the application writes.
    pub key: CapabilitySubject<Key>,
    /// The signed token, for the application to deliver however it likes.
    pub token: String,
    /// When the token stops checking out.
    pub expires_at: SystemTime,
    /// What connetto certified the caller holds, which is what the permission
    /// row must not exceed. It travels here rather than in the token, because a
    /// permission inside the token would split authorization between the
    /// token's contents and the model.
    pub level: ShareLevel,
}

/// A share could not be minted.
#[derive(Debug, thiserror::Error)]
pub enum ShareError {
    /// The caller may not read the resource it asked to share.
    #[error("the caller may not read {table}, so it may not share it")]
    Unauthorized {
        /// The table the caller named.
        table: String,
    },
    /// The caller may read the row but may not perform a verb it asked to
    /// share, so it may not hand that verb on.
    #[error("the caller may not {} {table}, so it may not share that", verb(*op))]
    NotWritable {
        /// The table the caller named.
        table: String,
        /// The first named verb the policy denied.
        op: WriteOp,
    },
    /// The policy could not decide whether the caller may perform a verb it
    /// asked to share, so nothing is certified.
    ///
    /// Distinct from [`NotWritable`](Self::NotWritable) on purpose: that one
    /// says the caller may not perform the verb, and this one says the question
    /// was not answerable, which is a different thing to tell an operator.
    #[error("the policy cannot decide whether the caller may {} {table}, so it certifies nothing: {detail}", verb(*op))]
    WriteUndecidable {
        /// The table the caller named.
        table: String,
        /// The verb the policy could not decide.
        op: WriteOp,
        /// What the policy reported about why.
        detail: String,
    },
    /// The row could not be reached at all, which is not the same as the
    /// caller not being allowed to share it.
    #[error("reading the row to share failed: {0}")]
    Read(String),
    /// The authorization check itself failed.
    #[error("authorization check failed: {0}")]
    Policy(String),
    /// The requested lifetime exceeds the deployment's ceiling.
    #[error("requested lifetime {requested:?} exceeds the ceiling {ceiling:?}")]
    TtlTooLong {
        /// What the caller asked for.
        requested: Duration,
        /// The configured maximum.
        ceiling: Duration,
    },
    /// The minted key's rendering contains the packing separator, so binding it
    /// would grant a neighbouring key's access.
    #[error("the minted key contains the packing separator {separator:?}")]
    SeparatorInKey {
        /// The separator the binding uses.
        separator: char,
    },
    /// Signing failed.
    #[error(transparent)]
    Mint(#[from] TokenError),
    /// The unreserved reader share stayed full for the whole queue window, so
    /// the reads behind the mint were refused rather than served on capacity
    /// R39 reserves for identified callers.
    #[error("no reader capacity for an unidentified caller, retry after {retry_after:?}")]
    RateLimited {
        /// How long to wait before asking again.
        retry_after: Duration,
    },
}

/// Mints share keys, having checked the caller may read what it is sharing.
///
/// Held beside [`AuthService`](crate::AuthService) at startup and called from
/// the application's own handler, so the application keeps its routing, its
/// request shape and its rate limits.
pub struct CapabilityIssuer<P, R, Id> {
    authority: Arc<TokenAuthority>,
    policy: Arc<P>,
    rows: Arc<R>,
    ttl: Duration,
    max_ttl: Duration,
    /// Sink for the durable record of a successful mint, which ending the
    /// minting session reads back (R105 decision 5). It also fixes `Id`, so
    /// no separate marker is needed.
    audit: crate::audit::AuditHook<Id>,
    /// The reader-pool gate the mint's reads take a share permit from. `None`
    /// leaves them ungated, the pre-R39 behaviour.
    reader: Option<ReaderGate>,
}

impl<P, R, Id> CapabilityIssuer<P, R, Id> {
    /// Build over the token authority, the visibility policy, the row source
    /// the shared row is read through, the lifetimes the deployment
    /// configured, and the audit sink every successful mint is recorded in,
    /// usually [`pg_audit_hook`](crate::audit::pg_audit_hook).
    ///
    /// Unlike the revocation paths, a mint's row names its user: the mint
    /// runs as the caller and so already holds the identity, with no extra
    /// round trip.
    #[must_use]
    pub fn new(
        authority: Arc<TokenAuthority>,
        policy: Arc<P>,
        rows: Arc<R>,
        config: &AuthConfig,
        audit: crate::audit::AuditHook<Id>,
    ) -> Self {
        Self {
            authority,
            policy,
            rows,
            ttl: config.capability_ttl(),
            max_ttl: config.capability_max_ttl(),
            audit,
            reader: None,
        }
    }

    /// Bound this issuer's reads by the reader pool's reserved split (R39).
    ///
    /// The mint reads the shared row and asks the visibility question on the
    /// reader pool, so without this an unidentified caller could occupy
    /// connections past the reserve. Clone the same [`ReaderGate`] the
    /// [`RequestGuard`](crate::RequestGuard) holds: one split per pool.
    #[must_use]
    pub fn with_reader_gate(mut self, gate: ReaderGate) -> Self {
        self.reader = Some(gate);
        self
    }

    /// Mint a share key over the row `key` names in `table`, on behalf of
    /// `caller`, certifying `level`.
    ///
    /// `ttl` overrides the configured default and is refused rather than
    /// quietly shortened when it exceeds the ceiling, so an application's own
    /// statement of when a link dies cannot be a lie.
    ///
    /// The row is read before the question is asked, because the question is
    /// about the row and the caller named only its key. That read runs as the
    /// caller, so a row it may not see and a row that does not exist are the
    /// same refusal and minting cannot be turned into a probe. The question
    /// itself is the same visibility seam every other path goes through,
    /// because a caller must not share what it cannot read.
    ///
    /// A share certifying a write asks the seam's write question once per verb
    /// [`ShareLevel`] names, about the row as it stands, and all of them must
    /// allow (R34). Against the row-level-security policy every write is
    /// allowed, so the refusal waits on an engine that can answer.
    ///
    /// # Errors
    ///
    /// [`ShareError`] when the caller may not read the row or may not perform
    /// a verb it named, the row cannot be reached, the lifetime exceeds the
    /// ceiling, signing fails, or an unidentified caller finds the unreserved
    /// reader share full.
    pub async fn issue<Key>(
        &self,
        caller: &Principal<Id, Key>,
        table: &str,
        key: &[Value<Postgres>],
        level: ShareLevel,
        ttl: Option<Duration>,
    ) -> Result<IssuedCapability<Key>, ShareError>
    where
        Id: Clone,
        Key: MintCapabilityKey,
        P: VisibilityPolicy<Watcher = Arc<Principal<Id, Key>>, Backend = Postgres>,
        P::Error: Display,
        R: RowSource<Id, Key>,
    {
        let ttl = match ttl {
            Some(requested) if requested > self.max_ttl => {
                return Err(ShareError::TtlTooLong {
                    requested,
                    ceiling: self.max_ttl,
                });
            }
            Some(requested) => requested,
            None => self.ttl,
        };
        // The row read and the visibility question below both check out
        // reader connections, one at a time, so one share permit spans the
        // mint (R39).
        let _reader_permit = match &self.reader {
            Some(gate) => gate
                .acquire(Tier::of(caller))
                .await
                .map_err(|retry_after| ShareError::RateLimited { retry_after })?,
            None => ReaderPermit::none(),
        };
        let row = self
            .rows
            .read_row(caller, table, key)
            .await
            .map_err(|err| ShareError::Read(err.to_string()))?;
        let unauthorized = || ShareError::Unauthorized {
            table: table.to_owned(),
        };
        let row = row.ok_or_else(unauthorized)?;
        let view = ValuesRow::new(self.rows.catalog(), row.table_id, &row.values);
        let watchers = [Arc::new(caller.clone())];
        let mut verdicts = Vec::new();
        Verdict::reset(&mut verdicts, watchers.len());
        self.policy
            .may_see(&view, &watchers, &mut verdicts)
            .await
            .map_err(|err| ShareError::Policy(err.to_string()))?;
        if !matches!(verdicts.as_slice(), [Verdict::Allow, ..]) {
            return Err(unauthorized());
        }
        // One question per verb the caller named, about the row as it stands,
        // which is the only version a mint holds. All must allow: a share must
        // not hand on a verb the sharer does not hold itself.
        //
        // A question that fails to answer refuses too, and says so in its own
        // words: no database write follows a mint, so this loop is the only gate
        // there is, and reporting an unanswered question as a denial would tell
        // an operator the row is unwritable when the truth is that nothing knew.
        for write in level.writes(&view, &row.key) {
            let op = write.op();
            let verdict = self
                .policy
                .may_write(write, &watchers[0])
                .await
                .map_err(|err| ShareError::WriteUndecidable {
                    table: table.to_owned(),
                    op,
                    detail: err.to_string(),
                })?;
            if !verdict.allowed() {
                return Err(ShareError::NotWritable {
                    table: table.to_owned(),
                    op: write.op(),
                });
            }
        }
        // Captured here because `key` is shadowed by the minted subject on
        // the next line. The values travel as read: what type the audit table
        // stores a row key as is the application's, through
        // `ConnettoAuditSchema::row_key`.
        let shared_row = key.to_vec();
        let key = CapabilitySubject::<Key>::new(Key::mint());
        if key.key().to_string().contains(Key::SEPARATOR) {
            return Err(ShareError::SeparatorInKey {
                separator: Key::SEPARATOR,
            });
        }
        let issued_at = SystemTime::now();
        let token = self.authority.mint_capability(&key, issued_at, ttl)?;
        // After the mint, so a refused request records nothing: this table
        // holds what happened, and denials go to the log.
        (self.audit)(
            crate::audit::AuthEvent::new(
                caller.session_id(),
                caller.identity().map(|ctx| ctx.user_id.clone()),
                crate::audit::AuthOp::CapabilityMinted,
            )
            .about_row(table, shared_row)
            .about_key(key.key().to_string()),
        );
        Ok(IssuedCapability {
            key,
            token,
            expires_at: issued_at + ttl,
            level,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nothing_held_leaves_the_setting_unbound() {
        assert_eq!(String::subjects(&[]), Vec::<String>::new());
        assert!(
            ContentCaller::new(
                None,
                None,
                String::subjects(&[]),
                connetto_core::BOT_LIST_WILDCARD.to_owned()
            )
            .packed_subjects(',')
            .is_none()
        );
    }

    #[test]
    fn held_keys_join_in_order_under_one_separator() {
        let keys = [
            CapabilitySubject::new("key:b"),
            CapabilitySubject::new("key:a"),
            CapabilitySubject::new("key:a"),
        ];
        let caller = ContentCaller::new(
            None,
            None,
            String::subjects(&keys),
            connetto_core::BOT_LIST_WILDCARD.to_owned(),
        );
        assert_eq!(
            caller.packed_subjects(',').as_deref(),
            Some("key:a,key:b"),
            "sorted, deduped, and joined by the deployment's separator"
        );
        assert_eq!(
            caller.attributions(),
            vec!["key:a", "key:b"],
            "and a commit is attributed to each key rather than to the list"
        );
    }

    #[test]
    fn a_minted_key_never_contains_the_separator() {
        for _ in 0..64 {
            assert!(!String::mint().contains(<String as CapabilityKey>::SEPARATOR));
        }
    }

    #[test]
    fn two_mints_never_collide() {
        assert_ne!(String::mint(), <String as MintCapabilityKey>::mint());
    }

    /// A key-only caller meters under its keys, so reconnecting does not hand
    /// it a fresh allowance, and a caller holding nothing meters per run.
    #[test]
    fn a_meter_bucket_follows_the_caller_across_runs() {
        use connetto_core::SessionId;
        use connetto_core::auth::{AuthContext, Subject, VerifiedSession};

        fn handle(byte: u8) -> SessionId {
            SessionId::from_uuid(uuid::Uuid::from_bytes([byte; 16]))
        }

        fn key_only(run: u8) -> Principal {
            let mut principal = Principal::unidentified(handle(run));
            principal
                .accept(Subject::Capability(CapabilitySubject::new("key:k1")))
                .expect("a capability never conflicts");
            principal
        }

        let first = key_only(1);
        let second = key_only(2);
        assert_eq!(
            meter_key(&first, first.session_id()),
            meter_key(&second, second.session_id()),
            "the same key holder meters in one bucket across two runs"
        );

        let nothing_held: Principal = Principal::unidentified(handle(3));
        let other_run: Principal = Principal::unidentified(handle(4));
        assert_ne!(
            meter_key(&nothing_held, nothing_held.session_id()),
            meter_key(&other_run, other_run.session_id()),
            "a caller holding nothing meters per run rather than sharing one bucket"
        );

        let mut identified: Principal = Principal::unidentified(handle(5));
        identified
            .accept(Subject::Identity(VerifiedSession {
                context: AuthContext::new("key:k1"),
                session_id: handle(5),
                bot_list: None,
            }))
            .expect("one identity is accepted");
        assert_ne!(
            meter_key(&identified, identified.session_id()),
            meter_key(&first, first.session_id()),
            "an identity spelled like the key meters apart from the key holder"
        );
    }

    /// The binding reads the wildcard for a person and the list's own text for a login that resolved one.
    #[test]
    fn the_binding_reads_the_list_from_the_caller() {
        use connetto_core::SessionId;
        use connetto_core::auth::{AuthContext, Subject, VerifiedSession};
        use connetto_core::bots::{BotList, Verb};

        fn handle(byte: u8) -> SessionId {
            SessionId::from_uuid(uuid::Uuid::from_bytes([byte; 16]))
        }

        let person: Principal = Principal::unidentified(handle(1));
        assert_eq!(
            bot_list_for(&person),
            connetto_core::BOT_LIST_WILDCARD,
            "a caller holding no list carries the wildcard"
        );

        let mut bot: Principal = Principal::unidentified(handle(2));
        bot.accept(Subject::Identity(VerifiedSession {
            context: AuthContext::new("bot-a"),
            session_id: handle(2),
            bot_list: Some(BotList::new([
                ("t".to_owned(), Verb::Read),
                ("t".to_owned(), Verb::Insert),
            ])),
        }))
        .expect("one identity is accepted");
        assert_eq!(
            bot_list_for(&bot),
            "t:read,t:insert",
            "a caller holding a list carries its own text"
        );
    }

    /// A caller holding nothing binds `''` for exactly the halves the
    /// deployment declared may be unset, and the absent marker for the rest.
    #[test]
    fn an_absent_half_binds_as_its_declaration_reads_it() {
        let anonymous: Principal =
            Principal::unidentified(SessionId::from_uuid(uuid::Uuid::from_bytes([7; 16])));
        let marker = connetto_core::auth::absent_marker();
        let bound = |unset: UnsetSettings| {
            CallerBinding::of(&anonymous, &BindingSettings::default().with_unset(unset))
                .setup_statements()
        };
        let statement =
            |setting: &str, value: &str| format!("SELECT set_config('{setting}', '{value}', true)");
        for (unset, user, subjects) in [
            (UnsetSettings::ALWAYS_BOUND, marker, marker),
            (UnsetSettings::ALWAYS_BOUND.with_user(), "", marker),
            (UnsetSettings::ALWAYS_BOUND.with_subjects(), marker, ""),
            (
                UnsetSettings::ALWAYS_BOUND.with_user().with_subjects(),
                "",
                "",
            ),
        ] {
            assert_eq!(
                bound(unset),
                [
                    statement(DEFAULT_USER_SETTING, user),
                    statement(<String as CapabilityKey>::SETTING, subjects),
                    statement(
                        connetto_core::BOT_LIST_SETTING,
                        connetto_core::BOT_LIST_WILDCARD
                    ),
                    statement("role", NO_ROLE),
                ],
                "{unset:?}"
            );
        }
    }

    /// A caller is bound as its kind's role, anonymous until a login
    /// resolves, and a source without kind roles stays the role it logged in
    /// as.
    #[test]
    fn the_binding_acts_as_the_callers_kind_role() {
        use connetto_core::CallerKind;
        use connetto_core::auth::{AuthContext, Subject, VerifiedSession};

        let session = SessionId::from_uuid(uuid::Uuid::from_bytes([9; 16]));
        let anonymous: Principal = Principal::unidentified(session);
        let mut signed_in: Principal = Principal::unidentified(session);
        signed_in
            .accept(Subject::Identity(VerifiedSession {
                context: AuthContext::new("alice"),
                session_id: session,
                bot_list: None,
            }))
            .expect("one identity is accepted");
        let renamed = KindRoles::defaults()
            .clone()
            .with_role(CallerKind::Authenticated, "app_signed_in");
        let role = |caller: &Principal, settings: &BindingSettings| {
            CallerBinding::of(caller, settings)
                .setup_statements()
                .last()
                .cloned()
                .expect("the role is bound last")
        };
        let kinds = BindingSettings::default().with_kind_roles(renamed);
        assert_eq!(
            role(&anonymous, &kinds),
            "SELECT set_config('role', 'anon', true)"
        );
        assert_eq!(
            role(&signed_in, &kinds),
            "SELECT set_config('role', 'app_signed_in', true)"
        );
        assert_eq!(
            role(&signed_in, &BindingSettings::default()),
            "SELECT set_config('role', 'none', true)",
            "an owner-pool source binds no kind"
        );
    }
}

//! Startup checks against the deployment's Postgres.
//!
//! connetto emits no server DDL on any path a deployment runs, so everything it
//! reads from must already be there. A missing piece is refused at startup,
//! naming which one, because the alternatives are all worse: a replication
//! stream against an absent slot becomes a retry loop that never succeeds and
//! never says why, and an absent oplog table becomes a failure on the first
//! change rather than on the boot that introduced it.
//!
//! One entry point rather than a check per caller, so a later requirement is a
//! variant and a list entry instead of a seventh hand-rolled refusal. Not every
//! requirement is a thing that exists or does not: a table can be there and
//! still be unfit to stream, which is why a probe reports three outcomes rather
//! than a bool.

use diesel::prelude::*;
use diesel::sql_types::{Bool, Text};
use diesel::{QueryableByName, sql_query};
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::pooled_connection::bb8::Pool;

/// Something the deployment provisions and connetto only reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Artifact<'a> {
    /// A logical replication slot, by `slot_name`.
    ReplicationSlot(&'a str),
    /// A publication, by `pubname`.
    Publication(&'a str),
    /// A table, by name, resolved through the connection's own `search_path`.
    Table(&'a str),
    /// A table the change stream must carry, by publication and table name.
    ///
    /// Distinct from [`Table`](Self::Table), which asks only whether the table
    /// exists. A policy reading a table the publication leaves out learns
    /// nothing when a grant is given or taken away, so the store goes stale
    /// and then answers confidently and wrongly, which is silent rather than
    /// loud.
    PublishedTable {
        /// The publication the change stream reads.
        publication: &'a str,
        /// Resolved through the connection's `search_path` unless schema-qualified.
        table: &'a str,
    },
    /// Every table the publication carries records the row as it was, which
    /// Postgres does only under `REPLICA IDENTITY FULL`.
    ///
    /// Not a thing the deployment creates but a property of the tables it
    /// already streams, so its refusal names the tables to fix rather than
    /// offering to have one provisioned.
    ///
    /// Audit and bookkeeping tables carried by the publication need the same previous images.
    PreviousImages {
        /// The publication whose tables must all record their previous image.
        publication: &'a str,
    },
    /// A synced table, by name, resolved through the connection's own
    /// `search_path`, that must have row-level security on and the four
    /// `AS RESTRICTIVE` cap policies the deployment applied from the
    /// generated cap document.
    ///
    /// A restrictive policy bites only on a table with row-level security,
    /// and a table with it on and no permissive policy admits nobody, so
    /// the two halves travel together and a table missing either is not
    /// held to a bot list at all.
    CappedTable(&'a str),
    /// A column a table must carry, by table and column name.
    ///
    /// The table existing says nothing about its shape, and connetto reads
    /// whatever it names from the table the deployment provisions, so a
    /// column the table lacks is a boot that looks healthy and then fails
    /// every statement that names it.
    TableColumn {
        /// The table the column must live in.
        table: &'a str,
        /// The column connetto reads from it.
        column: &'a str,
    },
    /// A table the connecting role must read in full, by name, resolved
    /// through the connection's own `search_path`.
    ///
    /// The owner pool reads the facts behind the authorization store and the
    /// rows withdrawn from callers for everyone at once, binding none of the
    /// settings a policy reads. Postgres applies a table's policies to its
    /// owner under `FORCE ROW LEVEL SECURITY` and to any other role that is
    /// neither a superuser nor `BYPASSRLS`, so such a read would come back
    /// short and the rows it missed would be lost without an error.
    ReadInFull(&'a str),
    /// The role every connection must both log in as and act as, compared
    /// with `session_user` and `current_user`.
    ///
    /// A role setting (`ALTER ROLE ... SET role`) switches `current_user` on
    /// every connection of the login role, and the policies and grants that
    /// bind a statement are those of `current_user`, so a connection matching
    /// on its login role alone could still run under another role's rules.
    ActingRole(&'a str),
    /// A role a kind of caller acts as, probed from a connection logged in as
    /// the reader role, which switches to it for each caller's transaction.
    ///
    /// The kind role must exist, must not log in, be a superuser, bypass
    /// row-level security or belong to `pg_read_all_data` or
    /// `pg_write_all_data`, and the reader must hold it with `SET` and without
    /// inheriting it, since Postgres applies a policy's `TO` clause to every
    /// role whose privileges the acting role holds.
    KindRole(&'a str),
    /// A table, by name, resolved through the connection's own `search_path`,
    /// whose policies and grants must bind `role`.
    ///
    /// Postgres skips a table's policies for its owner unless the table forces
    /// row-level security, and the owner holds every privilege on the table,
    /// so a role that owns the table or inherits its owner's privileges
    /// (`pg_has_role(role, owner, 'USAGE')`) reads and writes around the rules
    /// both translators model.
    PolicyBoundTable {
        /// The role the table's policies must bind.
        role: &'a str,
        /// The table whose policies must bind it.
        table: &'a str,
    },
}

/// Why a kind role cannot be acted as, each named as the deployment would fix
/// it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KindRoleFault {
    /// The role can log in, so a client could reach Postgres as the kind
    /// without connetto.
    LogsIn,
    /// The role is a superuser, which no policy or grant binds.
    Superuser,
    /// The role bypasses row-level security.
    BypassesRls,
    /// The role belongs to `pg_read_all_data`, which reads every table.
    ReadsAllData,
    /// The role belongs to `pg_write_all_data`, which writes every table.
    WritesAllData,
    /// The reader inherits the role's privileges, so the kind's policies would
    /// apply to every caller before any switch.
    InheritedByReader,
    /// The reader cannot `SET ROLE` to the role, so no caller of the kind
    /// could be served.
    NotSettable,
}

impl core::fmt::Display for KindRoleFault {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(match self {
            Self::LogsIn => "it can log in (ALTER ROLE ... NOLOGIN)",
            Self::Superuser => "it is a superuser (ALTER ROLE ... NOSUPERUSER)",
            Self::BypassesRls => "it bypasses row-level security (ALTER ROLE ... NOBYPASSRLS)",
            Self::ReadsAllData => "it belongs to pg_read_all_data",
            Self::WritesAllData => "it belongs to pg_write_all_data",
            Self::InheritedByReader => {
                "the reader role inherits it (GRANT ... TO the reader WITH INHERIT FALSE)"
            }
            Self::NotSettable => {
                "the reader role cannot switch to it (GRANT ... TO the reader WITH SET TRUE)"
            }
        })
    }
}

/// What a probe found.
enum Found {
    /// There, and fit to serve.
    Fit,
    /// Absent. The deployment provisions it.
    Absent,
    /// There and unfit, naming what has to be fixed.
    Unfit(Vec<String>),
    /// There, but row-level security or one of the four cap policies is
    /// missing, naming each piece that is.
    Uncapped(Vec<String>),
    /// The table is there but lacks a column connetto reads from it, naming
    /// both.
    MissingColumn { table: String, column: String },
    /// The table is there, and row-level security filters it for the
    /// connecting role.
    Filtered(String),
    /// The connecting role lacks schema usage or table read permission.
    ReadDenied(String),
    /// The connection logs in or acts as another role than the one required.
    OtherRole {
        session_user: String,
        current_user: String,
    },
    /// The role owns the table or inherits its owner's privileges.
    OwnerPrivileges { role: String, owner: String },
    /// The kind role is there and unfit to act as.
    KindUnfit(Vec<KindRoleFault>),
}

impl Artifact<'_> {
    /// How the refusal names this, in the words a deployment would use.
    const fn noun(&self) -> &'static str {
        match self {
            Self::ReplicationSlot(_) => "replication slot",
            Self::Publication(_) | Self::PreviousImages { .. } => "publication",
            Self::Table(_) | Self::ReadInFull(_) | Self::PolicyBoundTable { .. } => "table",
            Self::ActingRole(_) => "role",
            Self::KindRole(_) => "kind role",
            Self::PublishedTable { .. } => "replicated table",
            Self::CappedTable(_) => "synced table",
            Self::TableColumn { .. } => "column",
        }
    }

    /// The name the deployment gave it.
    const fn name(&self) -> &str {
        match self {
            Self::ReplicationSlot(name)
            | Self::Publication(name)
            | Self::Table(name)
            | Self::ActingRole(name)
            | Self::KindRole(name) => name,
            Self::PublishedTable { table, .. }
            | Self::CappedTable(table)
            | Self::ReadInFull(table)
            | Self::PolicyBoundTable { table, .. }
            | Self::TableColumn { table, .. } => table,
            Self::PreviousImages { publication } => publication,
        }
    }

    /// What `conn` says about this artifact.
    ///
    /// The name is bound rather than interpolated in every case, including the
    /// table, where `to_regclass` takes it as text and so needs no identifier
    /// quoting of its own.
    ///
    /// **The slot probe filters on the database and that is load-bearing.**
    /// `pg_replication_slots` lists the whole cluster, and a logical slot name
    /// is unique cluster-wide rather than per database, so a slot of the same
    /// name bound to a neighbouring database satisfied a bare name match while
    /// being unusable from here. Found by running this against a cluster that
    /// had one. The same clause rejects a physical slot, whose `database` is
    /// null, which is equally unusable for logical decoding.
    ///
    /// Not checked: that the slot's output plugin is `pgoutput`. A slot on the
    /// wrong plugin fails at stream time rather than here.
    ///
    /// The catalog probes are raw statements because they read `pg_catalog`
    /// views that exist to be queried this way, and each binds only its own
    /// name. The publication membership check needs two binds and gets a
    /// modelled view instead. The replica-identity probe joins the view to
    /// `pg_class` on schema and name, so it reads three catalogs and stays a
    /// statement of its own.
    #[expect(clippy::too_many_lines, reason = "one probe arm per artifact variant")]
    async fn probe(&self, conn: &mut AsyncPgConnection) -> Result<Found, diesel::result::Error> {
        let probe = match self {
            Self::PublishedTable { publication, table } => {
                let present: bool = diesel::select(diesel::dsl::exists(
                    pg_publication_tables::table
                        .inner_join(
                            pg_namespace::table
                                .on(pg_namespace::nspname.eq(pg_publication_tables::schemaname)),
                        )
                        .inner_join(
                            pg_class::table.on(pg_class::relnamespace
                                .eq(pg_namespace::oid)
                                .and(pg_class::relname.eq(pg_publication_tables::tablename))),
                        )
                        .filter(pg_publication_tables::pubname.eq(publication))
                        .filter(pg_class::oid.nullable().eq(to_regclass(table))),
                ))
                .get_result(conn)
                .await?;
                return Ok(if present { Found::Fit } else { Found::Absent });
            }
            Self::PreviousImages { publication } => {
                let deficient: Vec<Named> = sql_query(
                    "SELECT c.relname AS name FROM pg_publication_tables p \
                     JOIN pg_namespace n ON n.nspname = p.schemaname \
                     JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = p.tablename \
                     WHERE p.pubname = $1 AND c.relreplident <> 'f' \
                     ORDER BY c.relname",
                )
                .bind::<Text, _>(publication)
                .load(conn)
                .await?;
                return Ok(if deficient.is_empty() {
                    Found::Fit
                } else {
                    Found::Unfit(deficient.into_iter().map(|row| row.name).collect())
                });
            }
            Self::CappedTable(_) => {
                // A catalog read of `pg_policies` beside `pg_class`, which
                // the DSL does not model, so a statement of its own, one
                // bound name.
                let capped: Vec<Capped> = sql_query(
                    "SELECT bool_or(c.relrowsecurity) AS rls, \
                     COALESCE(bool_or(p.policyname = ($1 || '_connetto_cap_read')), false) AS read_cap, \
                     COALESCE(bool_or(p.policyname = ($1 || '_connetto_cap_insert')), false) AS insert_cap, \
                     COALESCE(bool_or(p.policyname = ($1 || '_connetto_cap_update')), false) AS update_cap, \
                     COALESCE(bool_or(p.policyname = ($1 || '_connetto_cap_delete')), false) AS delete_cap \
                     FROM pg_class c \
                     LEFT JOIN pg_policies p \
                       ON p.schemaname = (SELECT n.nspname FROM pg_namespace n WHERE n.oid = c.relnamespace) \
                      AND p.tablename = c.relname \
                     WHERE c.oid = to_regclass($1)",
                )
                .bind::<Text, _>(self.name())
                .load(conn)
                .await?;
                let Some(capped) = capped.into_iter().next() else {
                    return Ok(Found::Absent);
                };
                let mut missing = Vec::new();
                if !capped.rls {
                    missing.push("row-level security".to_owned());
                }
                for (piece, held) in [
                    ("read", capped.read_cap),
                    ("insert", capped.insert_cap),
                    ("update", capped.update_cap),
                    ("delete", capped.delete_cap),
                ] {
                    if !held {
                        missing.push(format!("{}:{}", self.name(), piece));
                    }
                }
                return Ok(if missing.is_empty() {
                    Found::Fit
                } else {
                    Found::Uncapped(missing)
                });
            }
            Self::TableColumn { table, column } => {
                // A catalogue read the DSL does not model, beside to_regclass, so a statement of its own.
                let shaped: Vec<Shaped> = sql_query(
                    "SELECT to_regclass($1) IS NOT NULL AS present, \
                     COALESCE(to_regclass($1) IS NOT NULL AND EXISTS ( \
                         SELECT 1 FROM pg_attribute a \
                         WHERE a.attrelid = to_regclass($1) AND a.attname = $2 \
                           AND a.attnum > 0 AND NOT a.attisdropped), false) AS column_present",
                )
                .bind::<Text, _>(table)
                .bind::<Text, _>(column)
                .load(conn)
                .await?;
                let Some(shaped) = shaped.into_iter().next() else {
                    return Ok(Found::Absent);
                };
                return Ok(if shaped.column_present {
                    Found::Fit
                } else if shaped.present {
                    Found::MissingColumn {
                        table: table.to_string(),
                        column: column.to_string(),
                    }
                } else {
                    Found::Absent
                });
            }
            Self::ReadInFull(table) => {
                // Diesel has no constructor for the `CURRENT_USER` value expression.
                let role = diesel::dsl::sql::<Text>("CURRENT_USER::text");
                let access = pg_class::table
                    .filter(pg_class::oid.nullable().eq(to_regclass(table)))
                    .select((
                        row_security_active(pg_class::oid),
                        has_table_privilege(pg_class::oid, "SELECT")
                            .and(has_schema_privilege(pg_class::relnamespace, "USAGE")),
                        role,
                    ))
                    .get_result::<ReadAccess>(conn)
                    .await
                    .optional()?;
                return Ok(match access {
                    None => Found::Absent,
                    Some(ReadAccess {
                        readable: false,
                        role,
                        ..
                    }) => Found::ReadDenied(role),
                    Some(ReadAccess {
                        filtered: true,
                        role,
                        ..
                    }) => Found::Filtered(role),
                    Some(_) => Found::Fit,
                });
            }
            Self::ActingRole(expected) => {
                // Diesel has no constructor for the `SESSION_USER` and
                // `CURRENT_USER` value expressions.
                let (session_user, current_user) = diesel::select((
                    diesel::dsl::sql::<Text>("SESSION_USER::text"),
                    diesel::dsl::sql::<Text>("CURRENT_USER::text"),
                ))
                .get_result::<(String, String)>(conn)
                .await?;
                return Ok(if session_user == *expected && current_user == *expected {
                    Found::Fit
                } else {
                    Found::OtherRole {
                        session_user,
                        current_user,
                    }
                });
            }
            Self::KindRole(kind) => {
                let attributes: Option<KindAttributes> = sql_query(
                    "SELECT r.rolcanlogin AS logs_in, r.rolsuper AS superuser, \
                     r.rolbypassrls AS bypasses_rls, \
                     pg_has_role(r.oid, 'pg_read_all_data', 'MEMBER') AS reads_all_data, \
                     pg_has_role(r.oid, 'pg_write_all_data', 'MEMBER') AS writes_all_data, \
                     pg_has_role(CURRENT_USER, r.oid, 'USAGE') AS inherited_by_reader, \
                     pg_has_role(CURRENT_USER, r.oid, 'SET') AS settable \
                     FROM pg_roles r WHERE r.rolname = $1",
                )
                .bind::<Text, _>(*kind)
                .get_result(conn)
                .await
                .optional()?;
                return Ok(attributes.map_or(Found::Absent, |attributes| {
                    let faults = attributes.faults();
                    if faults.is_empty() {
                        Found::Fit
                    } else {
                        Found::KindUnfit(faults)
                    }
                }));
            }
            Self::PolicyBoundTable { role, table } => {
                let held = pg_class::table
                    .filter(pg_class::oid.nullable().eq(to_regclass(table)))
                    .select((
                        role_holds(*role, pg_class::relowner, "USAGE"),
                        pg_get_userbyid(pg_class::relowner),
                    ))
                    .get_result::<(bool, String)>(conn)
                    .await
                    .optional()?;
                return Ok(match held {
                    None => Found::Absent,
                    Some((true, owner)) => Found::OwnerPrivileges {
                        role: (*role).to_owned(),
                        owner,
                    },
                    Some((false, _)) => Found::Fit,
                });
            }
            Self::ReplicationSlot(_) => {
                "SELECT EXISTS (SELECT 1 FROM pg_replication_slots \
                 WHERE slot_name = $1 AND database = current_database()) AS present"
            }
            Self::Publication(_) => {
                "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1) AS present"
            }
            Self::Table(_) => "SELECT to_regclass($1) IS NOT NULL AS present",
        };
        let rows: Vec<Present> = sql_query(probe)
            .bind::<Text, _>(self.name())
            .load(conn)
            .await?;
        Ok(if rows.into_iter().next().is_some_and(|row| row.present) {
            Found::Fit
        } else {
            Found::Absent
        })
    }
}

diesel::table! {
    /// The publication's expanded table membership.
    pg_publication_tables (pubname) {
        /// The publication's name.
        pubname -> Text,
        /// One table it carries, unqualified.
        tablename -> Text,
        /// The table's schema.
        schemaname -> Text,
    }
}

diesel::table! {
    /// Database relation identities.
    pg_class (oid) {
        /// Relation identity.
        oid -> Oid,
        /// Stored relation name.
        relname -> Text,
        /// Namespace identity.
        relnamespace -> Oid,
        /// Owning role identity.
        relowner -> Oid,
    }
}

diesel::table! {
    /// Database namespace identities.
    pg_namespace (oid) {
        /// Namespace identity.
        oid -> Oid,
        /// Stored namespace name.
        nspname -> Text,
    }
}

diesel::allow_tables_to_appear_in_same_query!(pg_publication_tables, pg_class, pg_namespace,);

diesel::define_sql_function! {
    /// Resolve a relation through the connecting role's search path.
    fn to_regclass(name: Text) -> diesel::sql_types::Nullable<diesel::sql_types::Oid>;
}

diesel::define_sql_function! {
    /// Whether row-level security filters the connecting role.
    fn row_security_active(relation: diesel::sql_types::Oid) -> Bool;
}

diesel::define_sql_function! {
    /// Whether the connecting role holds a table privilege.
    fn has_table_privilege(relation: diesel::sql_types::Oid, privilege: Text) -> Bool;
}

diesel::define_sql_function! {
    /// Whether the connecting role holds a schema privilege.
    fn has_schema_privilege(schema: diesel::sql_types::Oid, privilege: Text) -> Bool;
}

diesel::define_sql_function! {
    /// Whether `role` holds another role's privileges, as `USAGE` asks.
    #[sql_name = "pg_has_role"]
    fn role_holds(role: Text, held: diesel::sql_types::Oid, privilege: Text) -> Bool;
}

diesel::define_sql_function! {
    /// The name of a role.
    fn pg_get_userbyid(role: diesel::sql_types::Oid) -> Text;
}

/// Why the server refused to start.
#[derive(Debug, thiserror::Error)]
pub enum PreflightError {
    /// The connection pool could not hand out a connection.
    #[error("preflight pool error: {0}")]
    Pool(String),
    /// The probe itself failed, so whether the artifact exists is unknown.
    #[error("checking for {noun} {name}: {source}")]
    Probe {
        /// What was being looked for.
        noun: &'static str,
        /// Its name.
        name: String,
        /// The underlying failure.
        source: diesel::result::Error,
    },
    /// The artifact is absent. The deployment owns creating it.
    #[error(
        "{noun} {name} does not exist. connetto creates no server objects, so the \
         deployment must provision it before the server will start"
    )]
    Missing {
        /// What was being looked for.
        noun: &'static str,
        /// Its name.
        name: String,
    },
    /// Every table the publication carries is there, and some of them do not
    /// record the row as it was.
    ///
    /// Its own variant because [`Missing`](Self::Missing) offers to have the
    /// deployment provision the artifact, which is the wrong instruction for a
    /// table that exists and is configured the wrong way.
    #[error(
        "publication {publication} carries tables that do not record the row as it \
         was before a change: {tables}. connetto has to know that to take a row \
         back from a caller who may no longer see it, and Postgres records it only \
         under REPLICA IDENTITY FULL, so run ALTER TABLE <name> REPLICA IDENTITY \
         FULL on each before the server will start"
    )]
    PreviousImage {
        /// The publication whose tables were audited.
        publication: String,
        /// The tables to fix, comma separated.
        tables: String,
    },
    /// The table exists but is not held to the bot cap.
    ///
    /// Its own variant because the fix is to apply the generated cap
    /// document, not to provision anything that is missing.
    #[error(
        "synced table {table} is not held to the bot cap: missing {missing}. \
         connetto generates the cap rules beside the deployment's schema and \
         policies, so apply the generated document before the server will start"
    )]
    Uncapped {
        /// The table that is not capped.
        table: String,
        /// The missing pieces, comma separated.
        missing: String,
    },
    /// The table exists but lacks a column connetto reads from it.
    ///
    /// Its own variant because the fix is adding a column to a table the
    /// deployment already provisions, not creating a new object.
    #[error(
        "table {table} has no {column} column. connetto reads the column and \
         creates no server objects, so the deployment must add it before the \
         server will start"
    )]
    MissingColumn {
        /// The table that lacks the column.
        table: String,
        /// The column connetto reads from it.
        column: String,
    },
    /// Row-level security filters a role that must read the table in full.
    #[error("row-level security filters role {role} on table {table}, which must be read in full")]
    ReadFiltered {
        /// The table the role cannot read in full.
        table: String,
        /// The connecting database role.
        role: String,
    },
    /// The connecting role cannot select from the table.
    #[error("role {role} requires SELECT and schema USAGE for table {table}")]
    ReadDenied {
        /// The table the role cannot read.
        table: String,
        /// The connecting database role.
        role: String,
    },
    /// A connection logs in or acts as another role than the required one.
    #[error(
        "the connection logs in as role {session_user} and acts as role {current_user}, \
         but must log in and act as role {expected}"
    )]
    ActingAs {
        /// The role the connection must log in and act as.
        expected: String,
        /// The role the connection logged in as.
        session_user: String,
        /// The role the connection acts as.
        current_user: String,
    },
    /// A kind role exists and cannot be acted as, naming every fault.
    #[error(
        "kind role {role} cannot be acted as: {}",
        faults.iter().map(ToString::to_string).collect::<Vec<_>>().join(", ")
    )]
    KindRole {
        /// The kind role.
        role: String,
        /// Every reason it cannot be acted as.
        faults: Vec<KindRoleFault>,
    },
    /// The connecting role owns a table or inherits its owner's privileges,
    /// so the table's policies and grants do not bind it.
    #[error(
        "role {role} owns table {table} or inherits the privileges of its owner {owner}, \
         so the table's policies and grants do not bind it"
    )]
    OwnerPrivileges {
        /// The table whose policies must bind the role.
        table: String,
        /// The connecting database role.
        role: String,
        /// The table's owner.
        owner: String,
    },
}

#[derive(QueryableByName)]
struct Present {
    #[diesel(sql_type = Bool)]
    present: bool,
}

/// Whether a table is present and carries the column a probe asks for.
#[derive(QueryableByName)]
struct Shaped {
    #[diesel(sql_type = Bool)]
    present: bool,
    #[diesel(sql_type = Bool)]
    column_present: bool,
}

/// The cap state of one synced table, as the probe reads it.
#[expect(
    clippy::struct_excessive_bools,
    reason = "one field per probe column: RLS plus the four verb caps"
)]
#[derive(QueryableByName)]
struct Capped {
    #[diesel(sql_type = Bool)]
    rls: bool,
    #[diesel(sql_type = Bool)]
    read_cap: bool,
    #[diesel(sql_type = Bool)]
    insert_cap: bool,
    #[diesel(sql_type = Bool)]
    update_cap: bool,
    #[diesel(sql_type = Bool)]
    delete_cap: bool,
}

/// One name a probe reported.
#[derive(QueryableByName)]
struct Named {
    #[diesel(sql_type = Text)]
    name: String,
}

/// The connecting role's effective read access to a relation.
#[derive(Queryable)]
struct ReadAccess {
    filtered: bool,
    readable: bool,
    role: String,
}

/// The attributes of a kind role and the reader's hold on it.
#[expect(
    clippy::struct_excessive_bools,
    reason = "one field per probe column: each is a separate fault"
)]
#[derive(QueryableByName)]
struct KindAttributes {
    #[diesel(sql_type = Bool)]
    logs_in: bool,
    #[diesel(sql_type = Bool)]
    superuser: bool,
    #[diesel(sql_type = Bool)]
    bypasses_rls: bool,
    #[diesel(sql_type = Bool)]
    reads_all_data: bool,
    #[diesel(sql_type = Bool)]
    writes_all_data: bool,
    #[diesel(sql_type = Bool)]
    inherited_by_reader: bool,
    #[diesel(sql_type = Bool)]
    settable: bool,
}

impl KindAttributes {
    /// Every fault the attributes show, in declaration order.
    fn faults(&self) -> Vec<KindRoleFault> {
        [
            (self.logs_in, KindRoleFault::LogsIn),
            (self.superuser, KindRoleFault::Superuser),
            (self.bypasses_rls, KindRoleFault::BypassesRls),
            (self.reads_all_data, KindRoleFault::ReadsAllData),
            (self.writes_all_data, KindRoleFault::WritesAllData),
            (self.inherited_by_reader, KindRoleFault::InheritedByReader),
            (!self.settable, KindRoleFault::NotSettable),
        ]
        .into_iter()
        .filter_map(|(faulty, fault)| faulty.then_some(fault))
        .collect()
    }
}

/// Refuse unless every artifact is there and fit to serve, naming the first
/// that is not.
///
/// Checked in the order given, so a caller lists the piece a reader would want
/// named first when several are absent at once, which on a fresh database is
/// all of them.
///
/// # Errors
///
/// [`PreflightError::Missing`] naming the absent artifact,
/// [`PreflightError::MissingColumn`] naming a table that lacks a column
/// connetto reads, [`PreflightError::Uncapped`] naming the synced table the
/// cap rules leave out, [`PreflightError::PreviousImage`] naming the
/// replicated tables that cannot report an old row,
/// [`PreflightError::ReadFiltered`] naming a table row-level security filters
/// for the connecting role,
/// [`PreflightError::ReadDenied`] naming a table the connecting role cannot select,
/// [`PreflightError::ActingAs`] naming the roles a connection logs in and acts
/// as when either is not the required one,
/// [`PreflightError::OwnerPrivileges`] naming a table a role owns or whose
/// owner's privileges it inherits,
/// [`PreflightError::KindRole`] naming a kind role and why it cannot be acted
/// as,
/// [`PreflightError::Probe`] when the check could not be answered, and
/// [`PreflightError::Pool`] when no connection was available.
pub async fn require(
    pool: &Pool<AsyncPgConnection>,
    artifacts: &[Artifact<'_>],
) -> Result<(), PreflightError> {
    let mut conn = pool
        .get()
        .await
        .map_err(|err| PreflightError::Pool(err.to_string()))?;
    for artifact in artifacts {
        let found = artifact
            .probe(&mut conn)
            .await
            .map_err(|source| PreflightError::Probe {
                noun: artifact.noun(),
                name: artifact.name().to_owned(),
                source,
            })?;
        match found {
            Found::Fit => {}
            Found::Absent => {
                return Err(PreflightError::Missing {
                    noun: artifact.noun(),
                    name: artifact.name().to_owned(),
                });
            }
            Found::Unfit(tables) => {
                return Err(PreflightError::PreviousImage {
                    publication: artifact.name().to_owned(),
                    tables: tables.join(", "),
                });
            }
            Found::Uncapped(missing) => {
                return Err(PreflightError::Uncapped {
                    table: artifact.name().to_owned(),
                    missing: missing.join(", "),
                });
            }
            Found::MissingColumn { table, column } => {
                return Err(PreflightError::MissingColumn { table, column });
            }
            Found::Filtered(role) => {
                return Err(PreflightError::ReadFiltered {
                    table: artifact.name().to_owned(),
                    role,
                });
            }
            Found::ReadDenied(role) => {
                return Err(PreflightError::ReadDenied {
                    table: artifact.name().to_owned(),
                    role,
                });
            }
            Found::OtherRole {
                session_user,
                current_user,
            } => {
                return Err(PreflightError::ActingAs {
                    expected: artifact.name().to_owned(),
                    session_user,
                    current_user,
                });
            }
            Found::OwnerPrivileges { role, owner } => {
                return Err(PreflightError::OwnerPrivileges {
                    table: artifact.name().to_owned(),
                    role,
                    owner,
                });
            }
            Found::KindUnfit(faults) => {
                return Err(PreflightError::KindRole {
                    role: artifact.name().to_owned(),
                    faults,
                });
            }
        }
    }
    Ok(())
}

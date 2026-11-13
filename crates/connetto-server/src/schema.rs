//! One contract for every deployment-owned table connetto reads through a
//! trait (R98).
//!
//! [`ConnettoSchema`] names one member per table set, each tied to the same
//! `Id`, so a deployment that omits one does not compile and no two disagree
//! on the identity type. [`connetto_schema!`](crate::connetto_schema) emits
//! the default tables and the impl. A deployment with its own tables
//! implements the member traits by hand and names them here.
//!
//! ```
//! connetto_server::connetto_schema! {
//!     pub struct AppSchema;
//!     id: String => diesel::sql_types::Text,
//!     audit_row_key: uuid::Uuid => diesel::sql_types::Uuid,
//!     audit_ddl: connetto_server::connetto_audit_ddl!("TEXT", "UUID"),
//! }
//! ```
//!
//! A deployment with device identity maps its own descriptor type, defined
//! once where its client and its server both see it, onto the enrolment
//! table's columns:
//!
//! ```
//! #[derive(Clone, serde::Serialize, serde::Deserialize)]
//! pub struct AppDevice {
//!     pub name: String,
//!     pub model: Option<String>,
//! }
//!
//! connetto_server::connetto_schema! {
//!     pub struct AppSchema;
//!     id: String => diesel::sql_types::Text,
//!     audit_row_key: uuid::Uuid => diesel::sql_types::Uuid,
//!     audit_ddl: connetto_server::connetto_audit_ddl!("TEXT", "UUID"),
//!     device_descriptor: AppDevice {
//!         name: diesel::sql_types::Text,
//!         model: diesel::sql_types::Nullable<diesel::sql_types::Text>,
//!     },
//! }
//! ```
//!
//! A session carries the application's own description of where its sign-in
//! came from, which the server's describe callback returns and the session
//! table holds one column per field:
//!
//! ```
//! #[derive(Clone)]
//! pub struct AppSession {
//!     pub place: String,
//! }
//!
//! connetto_server::connetto_schema! {
//!     pub struct AppSchema;
//!     id: String => diesel::sql_types::Text,
//!     audit_row_key: uuid::Uuid => diesel::sql_types::Uuid,
//!     audit_ddl: connetto_server::connetto_audit_ddl!("TEXT", "UUID"),
//!     session_descriptor: AppSession { place: diesel::sql_types::Text },
//! }
//! ```
//!
//! A field list that disagrees with the type does not compile:
//!
//! ```compile_fail
//! #[derive(Clone, serde::Serialize, serde::Deserialize)]
//! pub struct AppDevice {
//!     pub name: String,
//! }
//!
//! connetto_server::connetto_schema! {
//!     pub struct AppSchema;
//!     id: String => diesel::sql_types::Text,
//!     audit_row_key: uuid::Uuid => diesel::sql_types::Uuid,
//!     audit_ddl: connetto_server::connetto_audit_ddl!("TEXT", "UUID"),
//!     device_descriptor: AppDevice { nickname: diesel::sql_types::Text },
//! }
//! ```
//!
//! Every member must be named, so a schema missing one is refused:
//!
//! ```compile_fail
//! use connetto_server::defaults::{
//!     ConnettoAudit, ConnettoAuthSchema, ConnettoEnrolments, ConnettoWriters,
//! };
//!
//! struct NoBans;
//!
//! impl connetto_server::schema::ConnettoSchema for NoBans {
//!     type Id = String;
//!     type Auth = ConnettoAuthSchema;
//!     type Writers = ConnettoWriters;
//!     type Audit = ConnettoAudit;
//!     type Enrolments = ConnettoEnrolments;
//! #   #[cfg(feature = "content")]
//! #   type Files = connetto_file_server::DefaultFileSchema;
//! }
//! ```
//!
//! Naming it compiles:
//!
//! ```
//! use connetto_server::defaults::{
//!     ConnettoAudit, ConnettoAuthSchema, ConnettoBans, ConnettoBots, ConnettoEnrolments,
//!     ConnettoWriters,
//! };
//!
//! struct Complete;
//!
//! impl connetto_server::schema::ConnettoSchema for Complete {
//!     type Id = String;
//!     type Auth = ConnettoAuthSchema;
//!     type Writers = ConnettoWriters;
//!     type Audit = ConnettoAudit;
//!     type Bans = ConnettoBans;
//!     type Enrolments = ConnettoEnrolments;
//!     type Bots = ConnettoBots;
//! #   #[cfg(feature = "content")]
//! #   type Files = connetto_file_server::DefaultFileSchema;
//! }
//! ```

use crate::audit::ConnettoAuditSchema;
use crate::authn::ConnettoStoreSchema;
use crate::ban::ConnettoBanSchema;
use crate::bots::ConnettoBotSchema;
use crate::device_cert::ConnettoEnrolmentSchema;
use crate::writer_schema::ConnettoWriterSchema;

/// Every deployment-owned table set connetto reads through a trait, under one
/// identity type.
pub trait ConnettoSchema: Send + Sync + 'static {
    /// The deployment's typed user id, the one every member carries. It
    /// parses from the text a deployment's SQL functions answer with, such as
    /// a file's owner (R79 decision 17).
    type Id: serde::Serialize
        + serde::de::DeserializeOwned
        + Clone
        + core::fmt::Display
        + core::str::FromStr
        + Send
        + Sync
        + 'static;
    /// The login sessions and provider tokens.
    type Auth: ConnettoStoreSchema<Id = Self::Id>;
    /// The writer's applied frontier and its settled entries (R75).
    type Writers: ConnettoWriterSchema<Id = Self::Id>;
    /// The audit log of access changes.
    type Audit: ConnettoAuditSchema<Id = Self::Id>;
    /// The ban list.
    type Bans: ConnettoBanSchema<Id = Self::Id>;
    /// The device enrolments, their certificates and the revocation-list
    /// numbers (R74).
    type Enrolments: ConnettoEnrolmentSchema<Id = Self::Id>;
    /// The bot registry: the registration codes, the bots, their list
    /// versions and the installations (R91).
    type Bots: ConnettoBotSchema<Id = Self::Id>;
    /// The file server's manifests and chunk registry.
    #[cfg(feature = "content")]
    type Files: connetto_file_server::ConnettoFileSchema;
}

/// Composes the application, registry, bookkeeping and private audit catalog.
#[must_use]
pub fn engine_ddl<D: ConnettoSchema>(pg_ddl: &str, devices: bool) -> String {
    let mut ddl = connetto_schema::with_bookkeeping::<<D::Bots as ConnettoBotSchema>::IdSql>(
        &connetto_schema::with_registry::<<D::Bots as ConnettoBotSchema>::IdSql>(pg_ddl),
        devices,
    );
    ddl.push('\n');
    ddl.push_str(D::Audit::CATALOG_DDL);
    if !ddl.trim_end().ends_with(';') {
        ddl.push_str("\n;");
    }
    ddl
}

/// Generates the deployment schema from identity types and `audit_ddl`, with optional device and session descriptors.
///
/// Invoked at module scope with `diesel` and `diesel_async` in scope. The
/// member structs it emits are `ConnettoAuthSchema`, `ConnettoWriters`,
/// `ConnettoAudit`, `ConnettoBans`, `ConnettoEnrolments` and `ConnettoBots`,
/// and the file member is the file server's `_cfs_` tables. Each descriptor is
/// the application's own type, destructured field by field, so a field list
/// that disagrees with it does not compile, and without one it is `()`.
#[macro_export]
macro_rules! connetto_schema {
    (
        $vis:vis struct $name:ident;
        id: $id:ty => $id_sql:ty,
        audit_row_key: $pk:ty => $pk_sql:ty,
        audit_ddl: $ddl:expr
        $(, device_descriptor: $($desc:ident)::+ { $($field:ident : $field_sql:ty),* $(,)? })?
        $(, session_descriptor: $($sdesc:ident)::+ { $($sfield:ident : $sfield_sql:ty),* $(,)? })?
        $(,)?
    ) => {
        $crate::connetto_auth_tables!($id, $id_sql; $($($sdesc)::+ { $($sfield : $sfield_sql),* })?);
        $crate::connetto_writers_table!($id, $id_sql);
        $crate::connetto_audit_table!($id, $id_sql, $pk, $pk_sql, $ddl);
        $crate::connetto_ban_table!($id, $id_sql);
        $crate::connetto_enrolment_tables!($id, $id_sql; $($($desc)::+ { $($field : $field_sql),* })?);
        $crate::connetto_bots_tables!($id, $id_sql);

        /// The deployment's tables, every member named.
        #[derive(Debug, Clone, Copy, Default)]
        $vis struct $name;

        impl $crate::schema::ConnettoSchema for $name {
            type Id = $id;
            type Auth = ConnettoAuthSchema;
            type Writers = ConnettoWriters;
            type Audit = ConnettoAudit;
            type Bans = ConnettoBans;
            type Enrolments = ConnettoEnrolments;
            type Bots = ConnettoBots;
            $crate::__connetto_schema_files!();
        }
    };
}

/// The file member of [`connetto_schema!`], present when this crate serves files.
#[cfg(feature = "content")]
#[doc(hidden)]
#[macro_export]
macro_rules! __connetto_schema_files {
    () => {
        type Files = $crate::__files::DefaultFileSchema;
    };
}

/// The file member of [`connetto_schema!`], absent when this crate serves no files.
#[cfg(not(feature = "content"))]
#[doc(hidden)]
#[macro_export]
macro_rules! __connetto_schema_files {
    () => {};
}

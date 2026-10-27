//! The primary-key equality filter every caller-scoped Postgres read builds.
//!
//! The table and its key columns come from the deployment's own runtime DDL, so
//! there is no `table!` schema to express these reads against and they are raw
//! SQL. This is the one place that renders the predicate and binds the values,
//! so the visibility check and the row the minting path reads cannot drift
//! apart. Identifiers come from the parsed catalog and every value binds
//! positionally with the scalar type the catalog gave it, so the row's own
//! primary-key index answers the read. A date or time key reaches the filter
//! as text, the form the replica carries it in, so its placeholder casts to
//! the column's declared type and Postgres compares it as that type.

use core::fmt::Write as _;

use connetto_core::quote_ident;
use diesel::pg::Pg;
use diesel::query_builder::{BoxedSqlQuery, SqlQuery};
use diesel::sql_types::{BigInt, Binary, Bool, Date, Double, Text, Time, Timestamp, Timestamptz};
use subql::backend::{Postgres, ScalarFamily, Value};
use subql::{
    ColumnId, ColumnLike as _, DatabaseLike, TableId, TableLike as _, ValueError, catalog_helpers,
};

/// A key column could not be read, or carried a type the filter cannot bind.
#[derive(Debug, thiserror::Error)]
pub(crate) enum KeyError {
    /// A primary-key cell of the row could not be decoded.
    #[error(transparent)]
    Value(#[from] ValueError),
    /// A primary-key column has a type the filter cannot bind yet.
    #[error("unsupported primary-key type {kind} on table {table}")]
    Unsupported {
        /// Table whose key could not be bound.
        table: String,
        /// The scalar kind that is not yet bindable.
        kind: String,
    },
}

/// One primary-key value in a shape the filter can bind.
///
/// The bindable set is exactly this enum, so an unsupported key type is
/// reported once while the filter is built rather than rediscovered per use.
enum KeyBind {
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
    Bytes(Vec<u8>),
    Uuid(uuid::Uuid),
    Date(chrono::NaiveDate),
    Time(chrono::NaiveTime),
    Timestamp(chrono::NaiveDateTime),
    TimestampTz(chrono::DateTime<chrono::Utc>),
}

/// One key column: its name, and the declared type its placeholder casts to
/// when the column is a date or time.
struct KeyColumn {
    name: String,
    cast: Option<String>,
}

impl KeyColumn {
    /// The placeholder `$place`, cast to the column's type where it has one.
    fn placeholder(&self, place: usize) -> String {
        match &self.cast {
            Some(cast) => format!("${place}::{cast}"),
            None => format!("${place}"),
        }
    }
}

/// One row's key columns, in key order, with the value each binds.
type CollectedKey = (Vec<KeyColumn>, Vec<KeyBind>);

/// A predicate over a table's primary key, plus the values it binds.
///
/// Two shapes, both key-shaped and both answered by the primary-key index:
/// [`build`](Self::build) renders `"col" = $1 AND "col2" = $2` to name one
/// row, and [`after`](Self::after) renders `("col", "col2") > ($1, $2)` to
/// resume a snapshot page past the last row it delivered (R58).
pub(crate) struct KeyFilter {
    predicate: String,
    binds: Vec<KeyBind>,
}

impl KeyFilter {
    /// Build the filter for `table`'s primary key, reading each key cell
    /// through `value_at`.
    ///
    /// [`None`] where no row can be named at all: a table with no primary key,
    /// or a key cell carrying no value. Every caller treats that as a denial,
    /// which is what asking the database would have returned.
    pub(crate) fn build<DB, F>(
        catalog: &DB,
        table_id: TableId,
        table: &str,
        value_at: F,
    ) -> Result<Option<Self>, KeyError>
    where
        DB: DatabaseLike,
        F: FnMut(usize, ColumnId) -> Result<Value<Postgres>, ValueError>,
    {
        let Some((columns, binds)) = Self::collect(catalog, table_id, table, value_at)? else {
            return Ok(None);
        };
        let mut predicate = String::new();
        for (position, column) in columns.iter().enumerate() {
            if position > 0 {
                predicate.push_str(" AND ");
            }
            let _ = write!(
                predicate,
                "{} = {}",
                quote_ident(&column.name),
                column.placeholder(position + 1)
            );
        }
        Ok(Some(Self { predicate, binds }))
    }

    /// The keyset predicate resuming past one row: a row-value comparison
    /// over the whole primary key, with placeholders starting at `first` so
    /// the subscription's own binds keep the numbers they were translated
    /// with.
    ///
    /// A row-value comparison is lexicographic, which is the order a
    /// composite key's own index is in, so a page resumes at one index
    /// descent whatever the key's arity.
    ///
    /// [`None`] on the same terms as [`build`](Self::build).
    pub(crate) fn after<DB, F>(
        catalog: &DB,
        table_id: TableId,
        table: &str,
        first: usize,
        value_at: F,
    ) -> Result<Option<Self>, KeyError>
    where
        DB: DatabaseLike,
        F: FnMut(usize, ColumnId) -> Result<Value<Postgres>, ValueError>,
    {
        let Some((columns, binds)) = Self::collect(catalog, table_id, table, value_at)? else {
            return Ok(None);
        };
        let names = columns
            .iter()
            .map(|column| quote_ident(&column.name))
            .collect::<Vec<_>>()
            .join(", ");
        let places = columns
            .iter()
            .enumerate()
            .map(|(offset, column)| column.placeholder(first + offset))
            .collect::<Vec<_>>()
            .join(", ");
        Ok(Some(Self {
            predicate: format!("({names}) > ({places})"),
            binds,
        }))
    }

    /// Read every key cell of the row through `value_at`, in key order.
    ///
    /// [`None`] where no row can be named at all: a table with no primary
    /// key, a key cell carrying no value, or a key column the catalog cannot
    /// name.
    fn collect<DB, F>(
        catalog: &DB,
        table_id: TableId,
        table: &str,
        mut value_at: F,
    ) -> Result<Option<CollectedKey>, KeyError>
    where
        DB: DatabaseLike,
        F: FnMut(usize, ColumnId) -> Result<Value<Postgres>, ValueError>,
    {
        let columns = catalog_helpers::primary_key_columns(catalog, table_id).unwrap_or_default();
        if columns.is_empty() {
            return Ok(None);
        }
        let mut binds = Vec::with_capacity(columns.len());
        let mut keys = Vec::with_capacity(columns.len());
        for (position, column) in columns.into_iter().enumerate() {
            let bind = match value_at(position, column)? {
                Value::Bool(value) => KeyBind::Bool(value),
                Value::Int(value) => KeyBind::Int(value),
                Value::Float(value) => KeyBind::Float(value),
                Value::String(value) => KeyBind::Text(value),
                Value::Bytes(value) => KeyBind::Bytes(value),
                Value::Uuid(value) => KeyBind::Uuid(value),
                Value::Date(value) => KeyBind::Date(value),
                Value::Time(value) => KeyBind::Time(value),
                Value::Timestamp(value) => KeyBind::Timestamp(value),
                Value::TimestampTz(value) => KeyBind::TimestampTz(value),
                Value::Null | Value::Missing => return Ok(None),
                other => {
                    return Err(KeyError::Unsupported {
                        table: table.to_owned(),
                        kind: format!("{:?}", other.scalar_kind()),
                    });
                }
            };
            let Some(name) = catalog_helpers::column_name(catalog, table_id, column) else {
                return Ok(None);
            };
            keys.push(KeyColumn {
                name,
                cast: temporal_type(catalog, table_id, column),
            });
            binds.push(bind);
        }
        Ok(Some((keys, binds)))
    }

    /// The rendered `WHERE` body, with `$n` placeholders in bind order.
    pub(crate) fn predicate(&self) -> &str {
        &self.predicate
    }

    /// Attach the key values to `query`, in placeholder order.
    pub(crate) fn bind<'a>(
        &self,
        query: BoxedSqlQuery<'a, Pg, SqlQuery>,
    ) -> BoxedSqlQuery<'a, Pg, SqlQuery> {
        self.binds.iter().fold(query, |query, value| match value {
            KeyBind::Bool(value) => query.bind::<Bool, _>(*value),
            KeyBind::Int(value) => query.bind::<BigInt, _>(*value),
            KeyBind::Float(value) => query.bind::<Double, _>(*value),
            KeyBind::Text(value) => query.bind::<Text, _>(value.clone()),
            KeyBind::Bytes(value) => query.bind::<Binary, _>(value.clone()),
            KeyBind::Uuid(value) => query.bind::<diesel::sql_types::Uuid, _>(*value),
            KeyBind::Date(value) => query.bind::<Date, _>(*value),
            KeyBind::Time(value) => query.bind::<Time, _>(*value),
            KeyBind::Timestamp(value) => query.bind::<Timestamp, _>(*value),
            KeyBind::TimestampTz(value) => query.bind::<Timestamptz, _>(*value),
        })
    }
}

/// The declared type of `column` when it is a date or time, which the
/// placeholder casts to, and [`None`] for every other type, whose bind
/// already carries it.
fn temporal_type<DB: DatabaseLike>(
    catalog: &DB,
    table_id: TableId,
    column: ColumnId,
) -> Option<String> {
    match catalog_helpers::column_scalar_family(catalog, table_id, column)? {
        ScalarFamily::Date
        | ScalarFamily::Time
        | ScalarFamily::Timestamp
        | ScalarFamily::TimestampTz => {
            let table = catalog.table_by_id(usize::try_from(table_id).ok()?)?;
            let declared = table.column_by_id(usize::from(column), catalog).ok()??;
            Some(declared.data_type(catalog).into_owned())
        }
        _ => None,
    }
}

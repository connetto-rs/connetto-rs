//! Every catalog table's shape, resolved once when a catalog is loaded.

use std::sync::Arc;

use connetto_core::quote_ident;
use sqlite_diff_rs::SimpleTable;
use subql::backend::Postgres;
use subql::{CatalogError, DatabaseLike, TableId, catalog_helpers};

use crate::materializer::MaterializerError;

/// Every table of one catalog, by table id: its column names and primary key
/// in catalog order, and the `SELECT` list a read of it projects.
///
/// A catalog that cannot name one of a table's columns refuses here, when it
/// is loaded, so a read site holding an id of the same catalog finds the
/// table's columns without a lookup that can fail.
#[derive(Debug)]
pub(crate) struct CatalogShapes {
    tables: Box<[TableShape]>,
}

/// One table's shape and the `SELECT` list naming its catalog columns.
#[derive(Debug)]
struct TableShape {
    shape: SimpleTable,
    /// The quoted column names, comma separated, which a read projects so a
    /// column the live table gained under the running server never reaches
    /// the client. Shared, so a planned write carries it without a copy.
    select_list: Arc<str>,
}

impl CatalogShapes {
    /// Resolve every table of `catalog`.
    ///
    /// # Errors
    ///
    /// [`CatalogError`] when the catalog cannot name one of a table's columns.
    pub(crate) fn of<DB: DatabaseLike>(catalog: &DB) -> Result<Self, CatalogError> {
        let tables = (0..)
            .zip(catalog.tables())
            .map(|(table_id, _)| {
                let shape = catalog_helpers::simple_table(catalog, table_id)?;
                let select_list = shape
                    .column_names()
                    .iter()
                    .map(|name| quote_ident(name.as_str()))
                    .collect::<Vec<_>>()
                    .join(", ")
                    .into();
                Ok(TableShape { shape, select_list })
            })
            .collect::<Result<_, CatalogError>>()?;
        Ok(Self { tables })
    }

    /// The column names and primary key of `table_id`.
    ///
    /// # Panics
    ///
    /// When `table_id` is not an id of the catalog these shapes were built
    /// from.
    pub(crate) fn shape(&self, table_id: TableId) -> &SimpleTable {
        &self.table(table_id).shape
    }

    /// The `SELECT` list naming the catalog columns of `table_id`.
    ///
    /// # Panics
    ///
    /// When `table_id` is not an id of the catalog these shapes were built
    /// from.
    pub(crate) fn select_list(&self, table_id: TableId) -> &Arc<str> {
        &self.table(table_id).select_list
    }

    /// The `SELECT` list of the table `catalog` names `table`.
    ///
    /// For a name that did not come out of this catalog by id, such as a
    /// stored record's table, which may name a table a later deployment
    /// dropped.
    ///
    /// # Errors
    ///
    /// [`MaterializerError::SchemaMismatch`] when the catalog holds no such
    /// table.
    pub(crate) fn select_list_named<DB: DatabaseLike>(
        &self,
        catalog: &DB,
        table: &str,
    ) -> Result<&Arc<str>, MaterializerError> {
        catalog_helpers::table_id::<Postgres, _>(catalog, table)
            .map(|table_id| self.select_list(table_id))
            .ok_or_else(|| MaterializerError::SchemaMismatch(table.to_owned()))
    }

    fn table(&self, table_id: TableId) -> &TableShape {
        let index = usize::try_from(table_id).expect("a table id fits in a usize");
        &self.tables[index]
    }
}

/// A table of 65,537 columns, one past the last ordinal a column id holds,
/// which parses as a catalog whose columns cannot all be named.
#[cfg(test)]
pub(crate) fn unnameable_columns_ddl() -> String {
    let columns = (0..=usize::from(u16::MAX) + 1)
        .map(|ordinal| format!("c{ordinal} INT"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("CREATE TABLE wide ({columns});")
}

/// The cause a catalog of [`unnameable_columns_ddl`] refuses to load with,
/// rendered: the first ordinal its one table cannot name.
#[cfg(test)]
pub(crate) fn unnameable_column() -> String {
    CatalogError::UnknownColumn {
        table_id: 0,
        ordinal: usize::from(u16::MAX) + 1,
    }
    .to_string()
}

/// A pool whose every connection attempt fails within 100 ms, for the
/// refusals a catalog makes before any connection is taken.
#[cfg(test)]
pub(crate) fn unconnected_pool()
-> diesel_async::pooled_connection::bb8::Pool<diesel_async::AsyncPgConnection> {
    let manager = diesel_async::pooled_connection::AsyncDieselConnectionManager::<
        diesel_async::AsyncPgConnection,
    >::new("postgres://nobody@127.0.0.1:9/none");
    diesel_async::pooled_connection::bb8::Pool::builder()
        .connection_timeout(core::time::Duration::from_millis(100))
        .build_unchecked(manager)
}

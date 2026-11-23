//! A row view over values connetto already holds.
//!
//! subql views a replication event through its own `EventRow`, which covers the
//! change and the catchup paths. The write path holds an uploaded op and the
//! minting path a row it read back, and neither is an event, so both view their
//! values through [`ValuesRow`].

use sql_traits::prelude::DatabaseLike;
use subql::backend::{ArrayCell, Postgres, Value};
use subql::visibility::RowView;
use subql::{ColumnId, ParserDB, TableId, ValueError};

/// One row's column values in catalog column order, with the catalog that
/// declares its columns.
///
/// Reading a column clones its value, which is what
/// [`RowView::value_at`] returns, so nothing is copied for a column nobody
/// asks about. An array column holds the replica's JSON array text, which
/// [`RowView::list_at`] reads against the column's declared element type.
pub struct ValuesRow<'a, DB = ParserDB> {
    catalog: &'a DB,
    table: TableId,
    values: &'a [Value<Postgres>],
}

impl<'a, DB> ValuesRow<'a, DB> {
    /// View `values` as the row of `table` in `catalog`, indexed by column
    /// ordinal.
    pub const fn new(catalog: &'a DB, table: TableId, values: &'a [Value<Postgres>]) -> Self {
        Self {
            catalog,
            table,
            values,
        }
    }
}

impl<DB: DatabaseLike> RowView for ValuesRow<'_, DB> {
    type Backend = Postgres;

    fn table_id(&self) -> TableId {
        self.table
    }

    fn value_at(&self, col: ColumnId) -> Result<Value<Postgres>, ValueError> {
        Ok(self
            .values
            .get(usize::from(col))
            .cloned()
            .unwrap_or(Value::Missing))
    }

    fn list_at(&self, column: ColumnId) -> Result<ArrayCell<Postgres>, ValueError> {
        let Some(element) = subql::catalog_helpers::column_array_element_kind::<Postgres, _>(
            self.catalog,
            self.table,
            column,
        ) else {
            return Ok(ArrayCell::Missing);
        };
        match self.values.get(usize::from(column)) {
            None | Some(Value::Missing) => Ok(ArrayCell::Missing),
            Some(Value::Null) => Ok(ArrayCell::Null),
            Some(Value::String(text)) => subql::patchset::replica_array_cell(column, element, text),
            Some(_) => Err(ValueError::Array { column }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::PostgreSqlDialect;

    #[test]
    fn array_columns_read_the_replica_text_as_typed_elements() {
        let catalog = ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE papers (id INT PRIMARY KEY, viewers TEXT[], scores INT[]);",
        )
        .expect("catalog");
        let papers =
            subql::catalog_helpers::table_id::<Postgres, _>(&catalog, "papers").expect("papers");
        let values = [
            Value::Int(3),
            Value::String(r#"["alice",null]"#.to_owned()),
            Value::String("[1,2]".to_owned()),
        ];
        let row = ValuesRow::new(&catalog, papers, &values);
        assert_eq!(
            row.list_at(1),
            Ok(ArrayCell::Elements(vec![
                Value::String("alice".to_owned()),
                Value::Null
            ]))
        );
        assert_eq!(
            row.list_at(2),
            Ok(ArrayCell::Elements(vec![Value::Int(1), Value::Int(2)]))
        );
        assert_eq!(row.list_at(0), Ok(ArrayCell::Missing));
        assert_eq!(row.list_at(3), Ok(ArrayCell::Missing));
        let nulls = [Value::Int(4), Value::Null, Value::Int(5)];
        let row = ValuesRow::new(&catalog, papers, &nulls);
        assert_eq!(row.list_at(1), Ok(ArrayCell::Null));
        assert_eq!(row.list_at(2), Err(ValueError::Array { column: 2 }));
        let malformed = [Value::Int(6), Value::String("alice".to_owned())];
        let row = ValuesRow::new(&catalog, papers, &malformed);
        assert_eq!(row.list_at(1), Err(ValueError::Array { column: 1 }));
    }
}

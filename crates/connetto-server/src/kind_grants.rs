//! The table and column privileges each caller kind's role holds on every
//! served table, read from the live database (R108).
//!
//! A subscription or a write is checked here before the database sees it,
//! against exactly what `PostgreSQL` checks, columns included: a read needs
//! `SELECT` on every column the statement names, or on any column when it
//! names none, an `INSERT` needs `INSERT` on the columns it assigns, an
//! `UPDATE` needs `UPDATE` on the columns it assigns and `SELECT` on those
//! it reads, and a `DELETE` needs `DELETE` and `SELECT` on what it reads.
//!
//! A refusal names the table and the kind only when some kind may read the
//! table, since every application binary carries every kind's client copy
//! and so already shows such a table. A table no kind may read is refused
//! without a name, so a caller cannot learn that it exists.

use std::collections::HashMap;

use connetto_core::{CallerKind, KindRoles};
use sql_traits::prelude::{ColumnLike, ColumnPrivilege, DatabaseLike, RoleLike, TableLike};
use sql_traits::structs::{ActingRole, TargetName};
use subql::backend::{Backend, Postgres, Value};
use subql::{ColumnId, TableId, catalog_helpers};

use crate::materializer::PlannedWrite;

/// Why a kind may not run a command on a table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NotGranted {
    /// Some kind may read the table, so the refusal names it and the kind.
    Named {
        /// The table, by its catalog name.
        table: String,
        /// The kind refused.
        kind: CallerKind,
    },
    /// No kind may read the table, so the refusal names nothing.
    Hidden {
        /// The table, by its catalog name, for the log alone.
        table: String,
    },
}

/// Where one privilege holds.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Columns {
    /// On the table, and so on every column.
    Table,
    /// On these columns alone, sorted, none for a privilege not held.
    Some(Vec<ColumnId>),
}

impl Columns {
    fn of<Live: DatabaseLike, Served: DatabaseLike>(
        held: &ColumnPrivilege<'_, Live::Column>,
        served: &Served,
        table: TableId,
    ) -> Self {
        match held {
            ColumnPrivilege::Table => Self::Table,
            ColumnPrivilege::Columns(columns) => {
                let mut ids: Vec<ColumnId> = columns
                    .iter()
                    .filter_map(|column| {
                        catalog_helpers::column_id_for_name::<Postgres, _>(
                            served,
                            table,
                            column.column_name(),
                            column.column_name_is_quoted(),
                        )
                    })
                    .collect();
                ids.sort_unstable();
                Self::Some(ids)
            }
        }
    }

    fn on(&self, column: ColumnId) -> bool {
        match self {
            Self::Table => true,
            Self::Some(ids) => ids.binary_search(&column).is_ok(),
        }
    }

    fn on_all(&self, columns: &[ColumnId]) -> bool {
        columns.iter().all(|column| self.on(*column))
    }

    fn on_any(&self) -> bool {
        match self {
            Self::Table => true,
            Self::Some(ids) => !ids.is_empty(),
        }
    }
}

/// What one kind's role holds on one table.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Held {
    select: Columns,
    insert: Columns,
    update: Columns,
    delete: bool,
}

impl Held {
    const NOTHING: Self = Self {
        select: Columns::Some(Vec::new()),
        insert: Columns::Some(Vec::new()),
        update: Columns::Some(Vec::new()),
        delete: false,
    };
}

/// What every kind holds on one served table.
#[derive(Debug, Clone, PartialEq, Eq)]
struct TableGrants {
    name: String,
    /// The served table's column count, which a read of every column covers.
    columns: u16,
    kinds: [Held; 5],
}

impl TableGrants {
    fn refusal(&self, kind: CallerKind) -> NotGranted {
        if self.kinds.iter().any(|held| held.select.on_any()) {
            NotGranted::Named {
                table: self.name.clone(),
                kind,
            }
        } else {
            NotGranted::Hidden {
                table: self.name.clone(),
            }
        }
    }

    fn reads(&self, kind: CallerKind, columns: Option<&[ColumnId]>) -> bool {
        let select = &self.kinds[kind.index()].select;
        match columns {
            None => {
                matches!(select, Columns::Table)
                    || (0..self.columns).all(|column| select.on(column))
            }
            Some([]) => select.on_any(),
            Some(columns) => select.on_all(columns),
        }
    }

    fn writes(&self, kind: CallerKind, write: &PlannedWrite) -> bool {
        let held = &self.kinds[kind.index()];
        match write {
            PlannedWrite::Insert { assigned, .. } => held.insert.on_all(assigned),
            PlannedWrite::Update { assigned, read, .. } => {
                held.update.on_all(assigned) && held.select.on_all(read)
            }
            PlannedWrite::Delete { read, .. } => held.delete && held.select.on_all(read),
        }
    }
}

/// The privileges every kind holds on every served table.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct KindGrants {
    tables: HashMap<TableId, TableGrants>,
}

impl KindGrants {
    /// What each kind's role in `roles` holds in `live` on every table of
    /// `served`. A served table `live` lacks, or a role it does not declare,
    /// holds nothing.
    pub(crate) fn of<Served: DatabaseLike, Live: DatabaseLike>(
        served: &Served,
        live: &Live,
        roles: &KindRoles,
    ) -> Self {
        let tables = served
            .tables()
            .filter_map(|table| {
                let id = TableId::try_from(served.table_id(table)?).ok()?;
                let columns = u16::try_from(table.columns(served).ok()?.count()).ok()?;
                let target = TargetName::new(table.table_name(), table.table_name_is_quoted());
                let target = match table.table_schema() {
                    Some(schema) => target.with_schema(schema, table.table_schema_is_quoted()),
                    None => target,
                };
                let found = live
                    .resolve_target_table(target, <Postgres as Backend>::WRITTEN_TABLE_NAME_CASE)
                    .ok()
                    .flatten();
                let kinds = CallerKind::ALL.map(|kind| {
                    found
                        .and_then(|found| held(found, live, roles.role(kind), served, id))
                        .unwrap_or(Held::NOTHING)
                });
                Some((
                    id,
                    TableGrants {
                        name: table.table_name().to_owned(),
                        columns,
                        kinds,
                    },
                ))
            })
            .collect();
        Self { tables }
    }

    /// Whether `kind` may read `table` through `columns`, every column when
    /// [`None`] and any column when empty.
    pub(crate) fn read(
        &self,
        kind: CallerKind,
        table: TableId,
        columns: Option<&[ColumnId]>,
    ) -> Result<(), NotGranted> {
        match self.tables.get(&table) {
            Some(grants) if grants.reads(kind, columns) => Ok(()),
            Some(grants) => Err(grants.refusal(kind)),
            None => Ok(()),
        }
    }

    /// Whether `kind` may make `write` on `table`.
    pub(crate) fn write(
        &self,
        kind: CallerKind,
        table: TableId,
        write: &PlannedWrite,
    ) -> Result<(), NotGranted> {
        match self.tables.get(&table) {
            Some(grants) if grants.writes(kind, write) => Ok(()),
            Some(grants) => Err(grants.refusal(kind)),
            None => Ok(()),
        }
    }

    /// Leave out of `write`, when it is an `INSERT` from `kind` on `table`,
    /// every column holding `NULL` that the kind holds no `INSERT` on. Such a
    /// `NULL` assigns nothing, so the row takes the column's default, as the
    /// kind's replica copy stores it (R108 decision 8).
    pub(crate) fn unassign_ungranted_nulls(
        &self,
        kind: CallerKind,
        table: TableId,
        write: &mut PlannedWrite,
    ) {
        let (PlannedWrite::Insert { new, assigned }, Some(grants)) =
            (write, self.tables.get(&table))
        else {
            return;
        };
        let insert = &grants.kinds[kind.index()].insert;
        assigned.retain(|column| {
            insert.on(*column) || !new.get(usize::from(*column)).is_some_and(Value::is_null)
        });
    }
}

/// What `role` holds on `table`, as `PostgreSQL` answers through the role
/// and the roles it inherits, or what `PUBLIC` holds when `live` declares no
/// such role.
fn held<Live: DatabaseLike, Served: DatabaseLike>(
    table: &Live::Table,
    live: &Live,
    role: &str,
    served: &Served,
    id: TableId,
) -> Option<Held> {
    let acting = live
        .roles()
        .find(|declared| declared.stored_name() == role)
        .map_or(ActingRole::Public, ActingRole::Declared);
    let privileges = table.privileges(acting, live).ok()?;
    Some(Held {
        select: Columns::of::<Live, _>(&privileges.select, served, id),
        insert: Columns::of::<Live, _>(&privileges.insert, served, id),
        update: Columns::of::<Live, _>(&privileges.update, served, id),
        delete: privileges.delete,
    })
}

#[cfg(test)]
mod tests {
    use sqlparser::dialect::PostgreSqlDialect;
    use subql::ParserDB;

    use super::*;

    const TABLE: &str =
        "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT, quantity INT, status TEXT);";

    /// The grants of every kind on `orders` once `grants` ran after the table.
    fn grants(grants: &str) -> (KindGrants, TableId) {
        let served = ParserDB::parse::<PostgreSqlDialect>(TABLE).expect("served");
        let live = ParserDB::parse::<PostgreSqlDialect>(&format!(
            "{TABLE}\nCREATE ROLE anon NOLOGIN;\nCREATE ROLE authenticated NOLOGIN;\n{grants}"
        ))
        .expect("live");
        let id = catalog_helpers::table_id::<Postgres, _>(&served, "orders").expect("orders");
        (KindGrants::of(&served, &live, KindRoles::defaults()), id)
    }

    #[test]
    fn a_column_grant_covers_only_its_columns() {
        let (grants, orders) = grants(
            "GRANT SELECT ON orders TO authenticated;\nGRANT SELECT (id, quantity) ON orders TO anon;",
        );
        assert_eq!(
            grants.read(CallerKind::Anon, orders, Some(&[0, 1, 2, 3])),
            Err(NotGranted::Named {
                table: "orders".to_owned(),
                kind: CallerKind::Anon,
            }),
        );
        assert_eq!(grants.read(CallerKind::Anon, orders, Some(&[0, 2])), Ok(()));
        assert_eq!(grants.read(CallerKind::Authenticated, orders, None), Ok(()));
    }
}

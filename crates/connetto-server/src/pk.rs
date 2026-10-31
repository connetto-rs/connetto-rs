//! Canonical primary-key codec.
//!
//! The oplog stores each retained change under a stable identity for its row,
//! and that identity has to be the same byte string whether the key arrived as
//! a CDC event's `Value<Postgres>` cells or as an uploaded sqlite-diff row
//! image. Both encode the key columns, in key order, into one self-describing
//! byte string. Encoding is `MessagePack` over `Vec<Value<Postgres>>`.
//!
//! The wire mapping is directed by the catalog because the read path is: it
//! lifts each column to its declared type, so a `UUID` becomes
//! [`Value::Uuid`] rather than raw bytes. An upload carries that same key as a
//! sixteen-byte blob, so it has to lift the blob the same way. Otherwise the
//! two spellings of one row name it differently, and every identity comparison
//! spanning both (the authorization seam names a row by rendering its key)
//! misses. A `NUMERIC(p,s)` column is the other such type. The wire carries it
//! as the replica's integer count of minor units, and the read path as the
//! decimal.

use bigdecimal::BigDecimal;
use sqlite_diff_rs::Value as WireValue;
use subql::backend::{DeclaredType, Postgres, ScalarKind, ScalarKindOf, Value};
use subql::{ColumnId, DatabaseLike, TableId, catalog_helpers};

/// The sqlite-diff value shape carried by an uploaded row image.
type Wire = WireValue<String, Vec<u8>>;

/// What the catalog declares about one column that decides how its wire cell
/// lifts.
#[derive(Clone, Copy, Default)]
pub(crate) struct WireColumn {
    /// The declared scalar kind, [`None`] past the table's columns.
    kind: Option<ScalarKindOf<Postgres>>,
    /// The scale a `NUMERIC(p,s)` column's minor units travel at.
    minor_unit_scale: Option<u32>,
}

/// Map one uploaded sqlite-diff cell to the canonical [`Value<Postgres>`],
/// directed by its `column`.
///
/// sqlite-diff carries only SQLite's four storage classes plus NULL. Integers,
/// reals, text, and blobs map straight across and NULL stays NULL, which is
/// exactly right whenever the storage class already matches the Postgres type.
/// The exceptions are the columns whose type has no matching storage class. A
/// `UUID` is a sixteen-byte blob the read path decodes to [`Value::Uuid`], and
/// a blob of any other length keeps the plain storage mapping. A
/// `NUMERIC(p,s)` is an integer of minor units, which lifts to the
/// [`Value::Decimal`] it counts.
pub(crate) fn from_wire(value: &Wire, column: WireColumn) -> Value<Postgres> {
    match (value, column.kind, column.minor_unit_scale) {
        (WireValue::Blob(blob), Some(ScalarKind::Builtin(DeclaredType::Uuid)), _) => {
            uuid_from_blob(blob)
        }
        (WireValue::Integer(units), _, Some(scale)) => {
            Value::Decimal(BigDecimal::new((*units).into(), i64::from(scale)))
        }
        (WireValue::Null, ..) => Value::Null,
        (WireValue::Integer(int), ..) => Value::Int(*int),
        (WireValue::Real(real), ..) => Value::Float(*real),
        (WireValue::Text(text), ..) => Value::String(text.clone()),
        (WireValue::Blob(blob), ..) => Value::Bytes(blob.clone()),
    }
}

/// Lift a sixteen-byte blob to [`Value::Uuid`], matching the read path. A blob
/// of any other length is not a UUID image, so it stays [`Value::Bytes`] rather
/// than being forced into a shape it is not.
fn uuid_from_blob(blob: &[u8]) -> Value<Postgres> {
    <[u8; 16]>::try_from(blob).map_or_else(
        |_| Value::Bytes(blob.to_vec()),
        |bytes| Value::Uuid(uuid::Uuid::from_bytes(bytes)),
    )
}

/// What the catalog declares about the column at `ordinal` in `table_id`,
/// nothing when the ordinal is past the table's columns.
pub(crate) fn wire_column<DB: DatabaseLike>(
    db: &DB,
    table_id: TableId,
    ordinal: usize,
) -> WireColumn {
    let Ok(column_id) = ColumnId::try_from(ordinal) else {
        return WireColumn::default();
    };
    let kind = catalog_helpers::column_scalar_kind::<Postgres, _>(db, table_id, column_id);
    let minor_unit_scale = match kind {
        Some(ScalarKind::Builtin(DeclaredType::Decimal)) => {
            catalog_helpers::column(db, table_id, column_id)
                .and_then(|column| catalog_helpers::minor_unit_scale(db, column))
        }
        _ => None,
    };
    WireColumn {
        kind,
        minor_unit_scale,
    }
}

/// Map a full uploaded row image to canonical values, directing each cell by
/// its column's catalog scalar kind. A cell's position is its column ordinal.
pub(crate) fn row_from_wire<DB: DatabaseLike>(
    db: &DB,
    table_id: TableId,
    values: &[Wire],
) -> Vec<Value<Postgres>> {
    values
        .iter()
        .enumerate()
        .map(|(ordinal, value)| from_wire(value, wire_column(db, table_id, ordinal)))
        .collect()
}

/// Encode primary-key values observed on the read path.
///
/// Serialization of `Value<Postgres>` into an in-memory buffer cannot fail, so
/// this returns the bytes directly.
///
/// # Panics
///
/// Does not panic in practice. Serializing `Vec<Value<Postgres>>` into a `Vec<u8>` with `rmp_serde` has no I/O path and every `Value` variant maps cleanly to a `MessagePack` type.
#[must_use]
pub fn encode(values: &[Value<Postgres>]) -> Vec<u8> {
    rmp_serde::to_vec(values).expect("encoding Vec<Value<Postgres>> into a Vec cannot fail")
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::str::FromStr as _;
    use sqlparser::dialect::PostgreSqlDialect;
    use subql::ParserDB;

    /// How each column of a table holding one column of each mapping lifts,
    /// plus one ordinal past its last column.
    fn columns() -> Vec<WireColumn> {
        let db = ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE t (id BIGINT PRIMARY KEY, name TEXT, ref UUID, \
             amount NUMERIC(10,2), bare NUMERIC);",
        )
        .expect("the DDL parses");
        let table = catalog_helpers::table_id::<Postgres, _>(&db, "t").expect("t resolves");
        (0..6)
            .map(|ordinal| wire_column(&db, table, ordinal))
            .collect()
    }

    #[test]
    fn a_composite_key_encodes_to_a_stable_string() {
        let key = vec![
            Value::<Postgres>::Int(42),
            Value::<Postgres>::String("tenant-a".to_owned()),
        ];
        assert_eq!(encode(&key), encode(&key.clone()));
        assert_ne!(encode(&key), encode(&key[..1]));
    }

    /// A UUID travels as a sixteen-byte blob and a `NUMERIC(10,2)` as minor
    /// units, while the read path types both from the catalog, so a row named
    /// from either spelling encodes to the same identity.
    #[test]
    fn wire_and_read_paths_agree() {
        let id = uuid::Uuid::from_u128(0xab4a_f609_f3d2_5ebd_b482_fbe8_b7db_00c6);
        let wire = [
            Wire::Integer(7),
            Wire::Text("x".to_owned()),
            Wire::Blob(id.as_bytes().to_vec()),
            Wire::Integer(1250),
            Wire::Text("3.125".to_owned()),
        ];
        let read = [
            Value::<Postgres>::Int(7),
            Value::<Postgres>::String("x".to_owned()),
            Value::<Postgres>::Uuid(id),
            Value::<Postgres>::Decimal(BigDecimal::from_str("12.50").expect("a decimal")),
            Value::<Postgres>::String("3.125".to_owned()),
        ];
        let mapped: Vec<Value<Postgres>> = wire
            .iter()
            .zip(columns())
            .map(|(value, column)| from_wire(value, column))
            .collect();
        assert_eq!(mapped, read);
        assert_eq!(encode(&mapped), encode(&read));
    }

    /// Minor units lift at the column's own scale, negative counts included,
    /// and NULL stays NULL.
    #[test]
    fn minor_units_lift_to_the_decimal_they_count() {
        let amount = columns()[3];
        for (units, decimal) in [(14, "0.14"), (700, "7.00"), (-350, "-3.50"), (0, "0.00")] {
            assert_eq!(
                from_wire(&Wire::Integer(units), amount),
                Value::Decimal(BigDecimal::from_str(decimal).expect("a decimal")),
                "{units} minor units"
            );
        }
        assert_eq!(from_wire(&Wire::Null, amount), Value::<Postgres>::Null);
    }

    #[test]
    fn a_blob_under_a_uuid_column_lifts_to_a_uuid() {
        let uuid = columns()[2];
        let id = uuid::Uuid::from_u128(1);
        assert_eq!(
            from_wire(&Wire::Blob(id.as_bytes().to_vec()), uuid),
            Value::<Postgres>::Uuid(id),
        );
        // A blob that is not sixteen bytes is not a UUID image, so it stays
        // bytes rather than being forced into one.
        assert_eq!(
            from_wire(&Wire::Blob(vec![0, 1, 2]), uuid),
            Value::<Postgres>::Bytes(vec![0, 1, 2]),
        );
    }

    #[test]
    fn past_the_columns_a_cell_keeps_its_storage_class() {
        let past = columns()[5];
        assert_eq!(
            from_wire(&Wire::Blob(vec![0, 1, 2, 255]), past),
            Value::<Postgres>::Bytes(vec![0, 1, 2, 255]),
        );
        assert_eq!(
            from_wire(&Wire::Integer(1250), past),
            Value::<Postgres>::Int(1250)
        );
        assert_eq!(from_wire(&Wire::Null, past), Value::<Postgres>::Null);
    }
}

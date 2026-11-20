//! The `orders` table the conflict, credits, and resync suites feed to the
//! worker through a scripted upstream.

#![cfg(target_arch = "wasm32")]

use sqlite_diff_rs::{DiffOps, Insert, PatchSet, SimpleTable, Value};

/// The table, keyed on a 16-byte UUID.
pub const DDL: &str = "CREATE TABLE orders (id BLOB PRIMARY KEY DEFAULT (uuidv4()) CHECK (length(id) = 16) NOT NULL, quantity INTEGER) STRICT;";

/// The live query the worker and the tab subscribe.
pub const QUERY: &str = "SELECT * FROM orders WHERE quantity > 0";

/// The compressed insert-patchset the wire carries for `rows`, each an id and
/// its quantity.
pub fn orders_payload(rows: &[(rosetta_uuid::Uuid, i64)]) -> Vec<u8> {
    let mut patchset = PatchSet::<SimpleTable, String, Vec<u8>>::new();
    for (id, quantity) in rows {
        let table = SimpleTable::new("orders", &[("id", ""), ("quantity", "")], &[0]);
        // The id travels as its raw 16 bytes in a SQLite BLOB, the encoding
        // the server and the changeset apply expect.
        let insert = Insert::<_, String, Vec<u8>>::from(table)
            .set(0, Value::Blob(<[u8; 16]>::from(*id).to_vec()))
            .expect("set id")
            .set(1, Value::Integer(*quantity))
            .expect("set quantity");
        patchset = patchset.insert(insert);
    }
    zstd::encode_all(patchset.build().as_slice(), 3).expect("compress patchset")
}

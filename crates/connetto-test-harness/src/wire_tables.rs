//! The wire-table shapes the client and server tests build their changesets
//! and patchsets against.

use sqlite_diff_rs::SimpleTable;

/// The `orders (id, price, quantity, status)` table keyed on `id`, the shape
/// the snapshot sources and mutation uploads in the loop tests carry.
#[must_use]
pub fn orders_table() -> SimpleTable {
    SimpleTable::new(
        "orders",
        &[("id", ""), ("price", ""), ("quantity", ""), ("status", "")],
        &[0],
    )
}

//! The wire-table shapes the client and server tests build their own
//! changesets against, so a table shape the many tests share lives in one
//! place instead of one copy per test.

use sqlite_diff_rs::SimpleTable;

/// The `orders` shape the resync and loop tests carry: four untyped columns keyed on `id`.
#[must_use]
pub fn orders_table() -> SimpleTable {
    SimpleTable::new(
        "orders",
        &[("id", ""), ("price", ""), ("quantity", ""), ("status", "")],
        &[0],
    )
}

/// The `notes` shape the write-path tests sync: three untyped columns keyed on `id`.
#[must_use]
pub fn notes_table() -> SimpleTable {
    SimpleTable::new(
        "notes",
        &[("id", ""), ("body", ""), ("edited_at", "")],
        &[0],
    )
}

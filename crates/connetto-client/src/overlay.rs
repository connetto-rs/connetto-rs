use diesel::connection::SimpleConnection;
use diesel::{ExpressionMethods, QueryDsl, RunQueryDsl, SqliteConnection};

use crate::{ClientError, quote_ident};

diesel::table! {
    _connetto_overlays (logical) {
        logical -> Text,
        physical -> Text,
        overlay -> Text,
    }
}

diesel::table! {
    _connetto_overlay_tags (tbl, key) {
        tbl -> Text,
        key -> Text,
        writer -> Nullable<Text>,
        seq -> Nullable<BigInt>,
        dirty -> Bool,
    }
}

pub(crate) const DDL: &str = "CREATE TABLE IF NOT EXISTS _connetto_overlays \
    (logical TEXT PRIMARY KEY NOT NULL, physical TEXT NOT NULL, overlay TEXT NOT NULL UNIQUE); \
    CREATE TABLE IF NOT EXISTS _connetto_overlay_tags \
    (tbl TEXT NOT NULL, key TEXT NOT NULL, writer TEXT, seq INTEGER, dirty INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(tbl, key))";

pub(crate) fn install(
    database: &mut SqliteConnection,
    layout: Option<&connetto_core::schema::SharedLayout>,
) -> Result<(), ClientError> {
    database.batch_execute(DDL)?;
    diesel::delete(_connetto_overlays::table).execute(database)?;
    if let Some(layout) = layout {
        for entry in layout.overlays() {
            diesel::insert_into(_connetto_overlays::table)
                .values((
                    _connetto_overlays::logical.eq(entry.logical()),
                    _connetto_overlays::physical.eq(entry.physical()),
                    _connetto_overlays::overlay.eq(entry.overlay()),
                ))
                .execute(database)?;
        }
    }
    Ok(())
}

pub(crate) fn names(database: &mut SqliteConnection) -> Result<Vec<String>, ClientError> {
    Ok(_connetto_overlays::table
        .select(_connetto_overlays::overlay)
        .load(database)?)
}

pub(crate) fn assign_pending(
    database: &mut SqliteConnection,
    writer: &str,
    sequence: i64,
) -> Result<(), ClientError> {
    prune_dirty(database)?;
    diesel::update(_connetto_overlay_tags::table.filter(_connetto_overlay_tags::dirty))
        .set((
            _connetto_overlay_tags::writer.eq(writer),
            _connetto_overlay_tags::seq.eq(sequence),
            _connetto_overlay_tags::dirty.eq(false),
        ))
        .execute(database)?;
    Ok(())
}

fn delete_rows(
    database: &mut SqliteConnection,
    rows: Vec<(String, String)>,
) -> Result<(), ClientError> {
    for (table, key) in rows {
        database.batch_execute(&format!(
            "DELETE FROM main.{} WHERE {key}",
            quote_ident(&table)
        ))?;
        diesel::delete(
            _connetto_overlay_tags::table
                .filter(_connetto_overlay_tags::tbl.eq(table))
                .filter(_connetto_overlay_tags::key.eq(key)),
        )
        .execute(database)?;
    }
    Ok(())
}

pub(crate) fn drop_through(
    database: &mut SqliteConnection,
    writer: &str,
    sequence: i64,
) -> Result<(), ClientError> {
    let rows = _connetto_overlay_tags::table
        .filter(_connetto_overlay_tags::writer.eq(writer))
        .filter(_connetto_overlay_tags::seq.le(sequence))
        .filter(_connetto_overlay_tags::dirty.eq(false))
        .select((_connetto_overlay_tags::tbl, _connetto_overlay_tags::key))
        .load(database)?;
    diesel::update(
        _connetto_overlay_tags::table
            .filter(_connetto_overlay_tags::writer.eq(writer))
            .filter(_connetto_overlay_tags::seq.le(sequence))
            .filter(_connetto_overlay_tags::dirty),
    )
    .set((
        _connetto_overlay_tags::writer.eq(None::<&str>),
        _connetto_overlay_tags::seq.eq(None::<i64>),
    ))
    .execute(database)?;
    delete_rows(database, rows)
}

pub(crate) fn drop_entry(
    database: &mut SqliteConnection,
    writer: &str,
    sequence: i64,
) -> Result<(), ClientError> {
    let rows = _connetto_overlay_tags::table
        .filter(_connetto_overlay_tags::writer.eq(writer))
        .filter(_connetto_overlay_tags::seq.eq(sequence))
        .filter(_connetto_overlay_tags::dirty.eq(false))
        .select((_connetto_overlay_tags::tbl, _connetto_overlay_tags::key))
        .load(database)?;
    diesel::update(
        _connetto_overlay_tags::table
            .filter(_connetto_overlay_tags::writer.eq(writer))
            .filter(_connetto_overlay_tags::seq.eq(sequence))
            .filter(_connetto_overlay_tags::dirty),
    )
    .set((
        _connetto_overlay_tags::writer.eq(None::<&str>),
        _connetto_overlay_tags::seq.eq(None::<i64>),
    ))
    .execute(database)?;
    delete_rows(database, rows)
}

pub(crate) fn drop_unqueued(database: &mut SqliteConnection) -> Result<(), ClientError> {
    let rows = _connetto_overlay_tags::table
        .filter(_connetto_overlay_tags::writer.is_null())
        .select((_connetto_overlay_tags::tbl, _connetto_overlay_tags::key))
        .load(database)?;
    delete_rows(database, rows)?;
    diesel::update(_connetto_overlay_tags::table.filter(_connetto_overlay_tags::dirty))
        .set(_connetto_overlay_tags::dirty.eq(false))
        .execute(database)?;
    Ok(())
}

pub(crate) fn drop_writer(
    database: &mut SqliteConnection,
    writer: &str,
) -> Result<(), ClientError> {
    let rows = _connetto_overlay_tags::table
        .filter(_connetto_overlay_tags::writer.eq(writer))
        .filter(_connetto_overlay_tags::dirty.eq(false))
        .select((_connetto_overlay_tags::tbl, _connetto_overlay_tags::key))
        .load(database)?;
    diesel::update(
        _connetto_overlay_tags::table
            .filter(_connetto_overlay_tags::writer.eq(writer))
            .filter(_connetto_overlay_tags::dirty),
    )
    .set((
        _connetto_overlay_tags::writer.eq(None::<&str>),
        _connetto_overlay_tags::seq.eq(None::<i64>),
    ))
    .execute(database)?;
    delete_rows(database, rows)
}

pub(crate) fn rekey(
    database: &mut SqliteConnection,
    from: &str,
    to: &str,
) -> Result<(), ClientError> {
    diesel::update(_connetto_overlay_tags::table.filter(_connetto_overlay_tags::writer.eq(from)))
        .set(_connetto_overlay_tags::writer.eq(to))
        .execute(database)?;
    Ok(())
}

pub(crate) fn move_entry(
    database: &mut SqliteConnection,
    from: &str,
    original: i64,
    to: &str,
    sequence: i64,
) -> Result<(), ClientError> {
    diesel::update(
        _connetto_overlay_tags::table
            .filter(_connetto_overlay_tags::writer.eq(from))
            .filter(_connetto_overlay_tags::seq.eq(original)),
    )
    .set((
        _connetto_overlay_tags::writer.eq(to),
        _connetto_overlay_tags::seq.eq(sequence),
    ))
    .execute(database)?;
    Ok(())
}

pub(crate) fn clear(database: &mut SqliteConnection) -> Result<(), ClientError> {
    for name in names(database)? {
        database.batch_execute(&format!("DELETE FROM main.{}", quote_ident(&name)))?;
    }
    diesel::delete(_connetto_overlay_tags::table).execute(database)?;
    Ok(())
}

fn prune_dirty(database: &mut SqliteConnection) -> Result<(), ClientError> {
    #[derive(diesel::QueryableByName)]
    struct Present {
        #[diesel(sql_type = diesel::sql_types::Bool)]
        present: bool,
    }
    let rows: Vec<(String, String)> = _connetto_overlay_tags::table
        .filter(_connetto_overlay_tags::dirty)
        .select((_connetto_overlay_tags::tbl, _connetto_overlay_tags::key))
        .load(database)?;
    for (table, key) in rows {
        let exists = diesel::sql_query(format!(
            "SELECT EXISTS(SELECT 1 FROM main.{} WHERE {key}) AS present",
            quote_ident(&table)
        ))
        .get_result::<Present>(database)?
        .present;
        if !exists {
            diesel::delete(
                _connetto_overlay_tags::table
                    .filter(_connetto_overlay_tags::tbl.eq(table))
                    .filter(_connetto_overlay_tags::key.eq(key)),
            )
            .execute(database)?;
        }
    }
    Ok(())
}

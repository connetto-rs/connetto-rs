-- The reader role's grants on connetto's own bookkeeping, which no caller
-- kind is ever granted. Kept apart from roles.sql, which the translations
-- read, because these tables are connetto's rather than the schema's. Apply
-- after roles.sql and after creating the _connetto_writers,
-- _connetto_settled, _connetto_session_writers and _connetto_courier_notices
-- writer tables.
GRANT SELECT, INSERT, UPDATE, DELETE ON _connetto_writers, _connetto_settled, _connetto_session_writers, _connetto_courier_notices TO connetto_reader;
-- A couriered entry writes its notice row in the transaction that settles it.
GRANT USAGE ON SEQUENCE _connetto_courier_notices_id_seq TO connetto_reader;

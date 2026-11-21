-- Apply after the reader role and the writer and audit tables exist.
GRANT SELECT, INSERT, UPDATE, DELETE ON _connetto_writers, _connetto_settled, _connetto_session_writers, _connetto_courier_notices TO connetto_reader;
-- A couriered entry writes its notice row in the transaction that settles it.
GRANT USAGE ON SEQUENCE _connetto_courier_notices_id_seq TO connetto_reader;
GRANT SELECT ON auth_events TO connetto_reader;

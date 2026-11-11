-- The roles the deployment runs connetto as and their grants on the synced
-- tables. The server and every client build read this file beside schema.sql
-- and policies.sql, so it holds plain statements over the tables schema.sql
-- declares and the bot registry tables connetto adds to it. Every client's
-- replica holds a table to the reader's grants, leaving out one the reader
-- cannot read. connetto's own bookkeeping tables are granted in
-- bookkeeping.sql, which no translation reads. Apply after schema.sql and the
-- registry tables. A migration runs it once, since PostgreSQL has no CREATE
-- ROLE IF NOT EXISTS.

-- The non-owner role the connetto-server runs reads and writes as, named by
-- CONNETTO_READER_URL and CONNETTO_READER_ROLE.
CREATE ROLE connetto_reader LOGIN PASSWORD 'connetto_reader';
GRANT USAGE ON SCHEMA public TO connetto_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON orders TO connetto_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON order_lines TO connetto_reader;
-- Photo rows travel the same path as the orders, and the demo write path
-- inserts them as connetto_reader, so the writer grant sits with the others.
GRANT SELECT, INSERT, UPDATE, DELETE ON photos TO connetto_reader;
-- The catalogue carries no policy of its own, so the read is public. The
-- writer grants stay off it, because the demo never writes its rows.
GRANT SELECT ON products TO connetto_reader;

-- The team tables the demo's accounts share through the membership rows, the
-- rows the reader role only reads and the notes it writes like the orders.
GRANT SELECT ON team_members TO connetto_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON team_notes TO connetto_reader;

-- The bot registry. A person's app writes registration codes and the
-- installations of the bots it adds, and reads the bots and their lists.
GRANT SELECT ON connetto_bots, connetto_bot_lists TO connetto_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON connetto_registration_codes, connetto_installations,
    connetto_installation_optional_tables TO connetto_reader;

-- The roles the deployment runs connetto as and their grants on the synced
-- tables. The server and every client build read this file beside schema.sql
-- and policies.sql, so it holds plain statements over the tables schema.sql
-- declares and the bot registry tables connetto adds to it. Each kind of
-- caller acts as its own role, and its client copy holds a table to that
-- role's grants, leaving out one the role cannot read. connetto's own
-- bookkeeping tables are granted to the reader in bookkeeping.sql, which no
-- translation reads. Apply after schema.sql and the registry tables. A
-- migration runs it once, since PostgreSQL has no CREATE ROLE IF NOT EXISTS.

-- The non-owner role the connetto-server logs in as, named by
-- CONNETTO_READER_URL and CONNETTO_READER_ROLE. It holds no table of its own
-- and switches to the caller's kind for every caller statement.
CREATE ROLE connetto_reader LOGIN PASSWORD 'connetto_reader';
GRANT USAGE ON SCHEMA public TO connetto_reader;

-- The kind roles, one per kind of caller. The reader switches to each with
-- SET ROLE and inherits none of them, so no kind's policies apply to another.
CREATE ROLE anon NOLOGIN;
CREATE ROLE authenticated NOLOGIN;
CREATE ROLE bot_installation NOLOGIN;
CREATE ROLE bot NOLOGIN;
CREATE ROLE delegated NOLOGIN;
GRANT anon, authenticated, bot_installation, bot, delegated TO connetto_reader
    WITH INHERIT FALSE, SET TRUE;
GRANT USAGE ON SCHEMA public TO anon, authenticated, bot_installation, bot, delegated;

-- The demo's tables, which anonymous and signed-in callers reach alike, the
-- policies telling them apart. The bot kinds reach them from R99 on.
GRANT SELECT, INSERT, UPDATE, DELETE ON orders, order_lines TO anon, authenticated;
-- Photo rows travel the same path as the orders, and the demo write path
-- inserts them as the caller's kind, so the writer grant sits with the others.
GRANT SELECT, INSERT, UPDATE, DELETE ON photos TO anon, authenticated;
-- The catalogue carries no policy of its own, so the read is public. The
-- writer grants stay off it, because the demo never writes its rows.
GRANT SELECT ON products TO anon, authenticated;

-- The team tables the demo's accounts share through the membership rows, the
-- rows the kinds only read and the notes they write like the orders.
GRANT SELECT ON teams, team_members TO anon, authenticated;
-- The teams and their membership rows decide what the team_notes, photos and
-- users policies let through, so no client role may change them, whatever
-- default privileges a database grants.
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON teams, team_members
    FROM PUBLIC, connetto_reader, anon, authenticated, bot_installation, bot, delegated;
GRANT SELECT, INSERT, UPDATE, DELETE ON team_notes TO anon, authenticated;

-- The account names, which only the server's sign-in writes, as the owner.
GRANT SELECT ON users TO anon, authenticated;

-- The bot registry, granted per kind. A person's app writes registration
-- codes and the installations of the bots it adds, and reads the bots and
-- their lists. A bot, an installation and an agent read their own rows, which
-- the registry's policies narrow to.
GRANT SELECT, INSERT ON connetto_registration_codes TO authenticated;
GRANT SELECT ON connetto_bots, connetto_bot_lists
    TO authenticated, bot_installation, bot, delegated;
GRANT SELECT, INSERT, UPDATE, DELETE ON connetto_installations,
    connetto_installation_optional_tables TO authenticated;
GRANT SELECT ON connetto_installations, connetto_installation_optional_tables
    TO bot_installation, bot, delegated;

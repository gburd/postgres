--
-- contrib/sepgsql/sepgsql.sql
--
-- [Step to install]
--
-- 1. Run initdb
--    to set up a new database cluster.
--
-- 2. Edit $PGDATA/postgresql.conf
--    to add '$libdir/sepgsql' to shared_preload_libraries.
--
--    Example)
--        shared_preload_libraries = '$libdir/sepgsql'
--
-- 3. Run this script for each databases
--    This script installs corresponding functions, and assigns initial
--    security labels on target database objects.
--    It can be run both single-user mode and multi-user mode, according
--    to your preference.
--
--    Example)
--      $ for DBNAME in template0 template1 postgres;     \
--        do                                              \
--          postgres --single -F -c exit_on_error=true -D $PGDATA $DBNAME \
--            < /path/to/script/sepgsql.sql > /dev/null   \
--        done
--
-- 4. Start postmaster,
--    if you initialized the database in single-user mode.
--
LOAD '$libdir/sepgsql';
CREATE OR REPLACE FUNCTION pg_catalog.sepgsql_getcon() RETURNS text AS '$libdir/sepgsql', 'sepgsql_getcon' LANGUAGE C;
CREATE OR REPLACE FUNCTION pg_catalog.sepgsql_setcon(text) RETURNS bool AS '$libdir/sepgsql', 'sepgsql_setcon' LANGUAGE C;
CREATE OR REPLACE FUNCTION pg_catalog.sepgsql_mcstrans_in(text) RETURNS text AS '$libdir/sepgsql', 'sepgsql_mcstrans_in' LANGUAGE C STRICT;
CREATE OR REPLACE FUNCTION pg_catalog.sepgsql_mcstrans_out(text) RETURNS text AS '$libdir/sepgsql', 'sepgsql_mcstrans_out' LANGUAGE C STRICT;
CREATE OR REPLACE FUNCTION pg_catalog.sepgsql_restorecon(text) RETURNS bool AS '$libdir/sepgsql', 'sepgsql_restorecon' LANGUAGE C;
SELECT sepgsql_restorecon(NULL);

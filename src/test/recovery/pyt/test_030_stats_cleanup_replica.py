# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/recovery/t/030_stats_cleanup_replica.pl.

Standbys drop stats when the drop records are replayed, persist stats across
graceful restarts, and discard stats after an immediate/crash restart.
"""


def _populate_standby_stats(primary, standby, connect_db, schema):
    primary.safe_psql(
        "CREATE TABLE {}.drop_tab_test1 AS SELECT generate_series(1,100) AS a".format(
            schema
        ),
        dbname=connect_db,
    )
    primary.safe_psql(
        "CREATE FUNCTION {}.drop_func_test1() RETURNS VOID AS 'select 2;' "
        "LANGUAGE SQL IMMUTABLE".format(schema),
        dbname=connect_db,
    )
    primary.wait_for_catchup(standby)

    dboid = standby.safe_psql(
        "SELECT oid FROM pg_database WHERE datname = '{}'".format(connect_db),
        dbname=connect_db,
    )
    tableoid = standby.safe_psql(
        "SELECT '{}.drop_tab_test1'::regclass::oid".format(schema), dbname=connect_db
    )
    funcoid = standby.safe_psql(
        "SELECT '{}.drop_func_test1()'::regprocedure::oid".format(schema),
        dbname=connect_db,
    )

    # Generate stats on the standby.
    standby.safe_psql(
        "SELECT * FROM {}.drop_tab_test1".format(schema), dbname=connect_db
    )
    standby.safe_psql("SELECT {}.drop_func_test1()".format(schema), dbname=connect_db)
    return dboid, tableoid, funcoid


def _drop_function_by_oid(primary, connect_db, funcoid):
    name = primary.safe_psql(
        "SELECT '{}'::regprocedure".format(funcoid), dbname=connect_db
    )
    primary.safe_psql("DROP FUNCTION {}".format(name), dbname=connect_db)


def _drop_table_by_oid(primary, connect_db, tableoid):
    name = primary.safe_psql(
        "SELECT '{}'::regclass".format(tableoid), dbname=connect_db
    )
    primary.safe_psql("DROP TABLE {}".format(name), dbname=connect_db)


def _func_tab_status(standby, connect_db, oids, present, sect):
    dboid, tableoid, funcoid = oids
    rel = standby.safe_psql(
        "SELECT pg_stat_have_stats('relation', {}, {})".format(dboid, tableoid),
        dbname=connect_db,
    )
    func = standby.safe_psql(
        "SELECT pg_stat_have_stats('function', {}, {})".format(dboid, funcoid),
        dbname=connect_db,
    )
    assert rel == present and func == present, "{}: standby stats as expected".format(
        sect
    )


def _db_status(standby, connect_db, dboid, present, sect):
    assert (
        standby.safe_psql(
            "SELECT pg_stat_have_stats('database', {}, 0)".format(dboid),
            dbname=connect_db,
        )
        == present
    ), "{}: standby db stats as expected".format(sect)


def test_stats_cleanup_replica(create_pg):
    """Standby stats track drops, survive graceful restart, vanish on crash."""
    primary = create_pg("primary", allows_streaming=True, start=False)
    primary.append_conf("track_functions = 'all'")
    primary.start()
    primary.backup("my_backup")
    standby = create_pg(
        "standby", from_backup=(primary, "my_backup"), has_streaming=True, start=False
    )
    standby.start()

    # Drop directly.
    oids = _populate_standby_stats(primary, standby, "postgres", "public")
    _func_tab_status(standby, "postgres", oids, "t", "initial")
    _drop_table_by_oid(primary, "postgres", oids[1])
    _drop_function_by_oid(primary, "postgres", oids[2])
    primary.wait_for_catchup(standby)
    _func_tab_status(standby, "postgres", oids, "f", "post drop")

    # Drop indirectly via schema.
    primary.safe_psql("CREATE SCHEMA drop_schema_test1")
    primary.wait_for_catchup(standby)
    oids = _populate_standby_stats(primary, standby, "postgres", "drop_schema_test1")
    _func_tab_status(standby, "postgres", oids, "t", "schema creation")
    primary.safe_psql("DROP SCHEMA drop_schema_test1 CASCADE")
    primary.wait_for_catchup(standby)
    _func_tab_status(standby, "postgres", oids, "f", "post schema drop")

    # Drop the database.
    primary.safe_psql("CREATE DATABASE test")
    primary.wait_for_catchup(standby)
    oids = _populate_standby_stats(primary, standby, "test", "public")
    _func_tab_status(standby, "test", oids, "t", "createdb")
    _db_status(standby, "test", oids[0], "t", "createdb")
    primary.safe_psql("DROP DATABASE test")
    primary.wait_for_catchup(standby)
    _func_tab_status(standby, "postgres", oids, "f", "post dropdb")
    _db_status(standby, "postgres", oids[0], "f", "post dropdb")

    # Stats persist across a graceful restart.
    oids = _populate_standby_stats(primary, standby, "postgres", "public")
    _func_tab_status(standby, "postgres", oids, "t", "pre restart")
    standby.restart()
    _func_tab_status(standby, "postgres", oids, "t", "post non-immediate")

    # But are discarded after an immediate restart.
    standby.stop("immediate")
    standby.start()
    _func_tab_status(standby, "postgres", oids, "f", "post immediate restart")

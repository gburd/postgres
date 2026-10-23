# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/recovery/t/032_relfilenode_reuse.pl.

When a database OID (and thus relfilenode paths) is reused after DROP/CREATE
DATABASE, a hot standby must not lose or misapply buffered changes: forcing
buffer eviction (pg_prewarm) on both nodes around each update, across template
reuse, VACUUM FULL, and tablespace moves, the standby's contents always match
the primary, and both shut down cleanly.
"""


def _send_wait(session, query, pattern):
    session.query_until(pattern, query + "\n")


def _cause_eviction(psql_primary, psql_standby):
    query = (
        "SELECT SUM(pg_prewarm(oid)) warmed_buffers FROM pg_class "
        "WHERE pg_relation_filenode(oid) != 0;"
    )
    _send_wait(psql_primary, query, r"warmed_buffers")
    _send_wait(psql_standby, query, r"warmed_buffers")


def _verify(primary, standby, counter, message):
    query = "SELECT datab, count(*) FROM large GROUP BY 1 ORDER BY 1 LIMIT 10"
    assert primary.safe_psql(query, dbname="conflict_db") == "{}|4000".format(
        counter
    ), "primary: {}".format(message)
    primary.wait_for_catchup(standby)
    assert standby.safe_psql(query, dbname="conflict_db") == "{}|4000".format(
        counter
    ), "standby: {}".format(message)


def test_032_relfilenode_reuse(create_pg, pg_bin):
    """Reused database OID / relfilenode replays correctly on a hot standby."""
    primary = create_pg("primary", allows_streaming=True, start=False)
    primary.append_conf(
        "\nallow_in_place_tablespaces = true\nlog_connections=receipt\n"
        "full_page_writes=off\nlog_min_messages=debug2\nshared_buffers=1MB\n"
    )
    primary.start()
    backup_name = "my_backup"
    primary.backup(backup_name)
    standby = create_pg(
        "standby", from_backup=(primary, backup_name), has_streaming=True, start=False
    )
    standby.start()
    psql_primary = primary.background_psql(
        "postgres", on_error_stop=False, tuples_only=False, quiet=False
    )
    psql_standby = standby.background_psql(
        "postgres", on_error_stop=False, tuples_only=False, quiet=False
    )
    primary.safe_psql("CREATE DATABASE conflict_db_template OID = 50000;")
    primary.safe_psql(
        "CREATE TABLE large(id serial primary key, dataa text, datab text);\n"
        "INSERT INTO large(dataa, datab) SELECT g.i::text, 1 "
        "FROM generate_series(1, 4000) g(i);",
        dbname="conflict_db_template",
    )
    primary.safe_psql(
        "CREATE DATABASE conflict_db TEMPLATE conflict_db_template OID = 50001;"
    )
    primary.safe_psql(
        "CREATE EXTENSION pg_prewarm;\n"
        "CREATE TABLE replace_sb(data text);\n"
        "INSERT INTO replace_sb(data) SELECT random()::text "
        "FROM generate_series(1, 15000);"
    )
    primary.wait_for_catchup(standby)
    _send_wait(psql_primary, "BEGIN;", r"BEGIN")
    _send_wait(psql_standby, "BEGIN;", r"BEGIN")
    primary.safe_psql("UPDATE large SET datab = 1;", dbname="conflict_db")
    _cause_eviction(psql_primary, psql_standby)
    primary.safe_psql("DROP DATABASE conflict_db;")
    primary.safe_psql(
        "CREATE DATABASE conflict_db TEMPLATE conflict_db_template OID = 50001;"
    )
    _verify(primary, standby, 1, "initial contents as expected")
    primary.safe_psql("UPDATE large SET datab = 2;", dbname="conflict_db")
    _cause_eviction(psql_primary, psql_standby)
    _verify(
        primary,
        standby,
        2,
        "update to reused relfilenode (due to DB oid conflict) is not lost",
    )
    primary.safe_psql("VACUUM FULL large;", dbname="conflict_db")
    primary.safe_psql("UPDATE large SET datab = 3;", dbname="conflict_db")
    _verify(primary, standby, 3, "restored contents as expected")
    primary.safe_psql("CREATE TABLESPACE test_tablespace LOCATION ''")
    primary.safe_psql("UPDATE large SET datab = 4;", dbname="conflict_db")
    _cause_eviction(psql_primary, psql_standby)
    primary.safe_psql("ALTER DATABASE conflict_db SET TABLESPACE test_tablespace")
    primary.safe_psql("ALTER DATABASE conflict_db SET TABLESPACE pg_default")
    primary.safe_psql("UPDATE large SET datab = 5;", dbname="conflict_db")
    _cause_eviction(psql_primary, psql_standby)
    _verify(primary, standby, 5, "post move contents as expected")
    primary.safe_psql("ALTER DATABASE conflict_db SET TABLESPACE test_tablespace")
    primary.safe_psql("UPDATE large SET datab = 7;", dbname="conflict_db")
    _cause_eviction(psql_primary, psql_standby)
    primary.safe_psql("UPDATE large SET datab = 8;", dbname="conflict_db")
    primary.safe_psql("DROP DATABASE conflict_db")
    primary.safe_psql("DROP TABLESPACE test_tablespace")
    primary.safe_psql("REINDEX TABLE pg_database")
    psql_primary.quit()
    psql_standby.quit()
    primary.stop()
    standby.stop()
    pg_bin.command_like(
        ["pg_controldata", primary.datadir],
        r"Database cluster state:\s+shut down\n",
        "primary shut down ok",
    )
    pg_bin.command_like(
        ["pg_controldata", standby.datadir],
        r"Database cluster state:\s+shut down in recovery\n",
        "standby shut down ok",
    )

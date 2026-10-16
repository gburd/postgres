# Copyright (c) 2024-2026, PostgreSQL Global Development Group

"""Port of src/test/recovery/t/048_vacuum_horizon_floor.pl.

Reproduces the vacuum-horizon-floor scenario: a VACUUM on the primary blocks in
BufferCleanup while a cursor pins a heap page; meanwhile the standby's
hot_standby_feedback horizon is toggled (by detaching/reattaching the walreceiver
via primary_conninfo) so the computed vacuum horizon could regress. The pinned
tuple (value 7) must still be readable across the cursor's second FETCH, and the
VACUUM must complete without corrupting visibility.
"""

import re


def test_048_vacuum_horizon_floor(create_pg):
    """VACUUM under a moving standby horizon preserves pinned-tuple visibility."""
    primary = create_pg("primary", allows_streaming="physical", start=False)
    primary.append_conf(
        "\nhot_standby_feedback = on\nautovacuum = off\nlog_min_messages = INFO\n"
        "maintenance_work_mem = 64\nio_combine_limit = 1\n"
    )
    primary.start()
    primary.backup("my_backup")
    replica = create_pg(
        "standby",
        from_backup=(primary, "my_backup"),
        has_streaming=True,
        start=False,
    )
    replica.start()
    test_db = "test_db"
    primary.safe_psql("CREATE DATABASE {}".format(test_db))
    orig_conninfo = primary.connstr()
    table1 = "vac_horizon_floor_table"
    psql_a = primary.background_psql(test_db, on_error_stop=True)
    psql_b = primary.background_psql(test_db, on_error_stop=True)
    nrows = 2000
    primary.safe_psql(
        "CREATE TABLE {t}(col1 int)\n"
        "    WITH (autovacuum_enabled=false, fillfactor=10);\n"
        "INSERT INTO {t} VALUES(7);\n"
        "INSERT INTO {t} SELECT generate_series(1, {n}) % 3;\n"
        "CREATE INDEX on {t}(col1);\n"
        "DELETE FROM {t} WHERE col1 = 0;\n"
        "INSERT INTO {t} VALUES(7);".format(t=table1, n=nrows),
        dbname=test_db,
    )
    primary_lsn = primary.lsn("flush")
    primary.wait_for_catchup(replica, "replay", primary_lsn)
    assert replica.poll_query_until(
        "SELECT EXISTS (SELECT * FROM pg_stat_wal_receiver);", "t", dbname=test_db
    )
    replica.safe_psql(
        "ALTER SYSTEM SET primary_conninfo = '';\nSELECT pg_reload_conf();",
        dbname=test_db,
    )
    assert replica.poll_query_until(
        "SELECT EXISTS (SELECT * FROM pg_stat_wal_receiver);", "f", dbname=test_db
    )
    res = psql_a.query_safe(
        "INSERT INTO {t} VALUES (99);\n"
        "UPDATE {t} SET col1 = 100 WHERE col1 = 99;\n"
        "SELECT 'after_update';".format(t=table1)
    )
    assert re.search(
        r"^after_update$", res, re.M
    ), "UPDATE occurred on primary session A"
    cursor1 = "vac_horizon_floor_cursor1"
    res = psql_b.query_safe(
        "BEGIN;\nSET enable_bitmapscan = off;\nSET enable_indexscan = off;\n"
        "SET enable_indexonlyscan = off;\n"
        "DECLARE {c} CURSOR FOR SELECT * FROM {t} WHERE col1 = 7;\n"
        "FETCH {c};".format(c=cursor1, t=table1)
    )
    assert res == "7", "Cursor query returned {}. Expected value 7.".format(res)
    vacuum_pid = psql_a.query_safe("SELECT pg_backend_pid();")
    psql_a.send(
        "SET maintenance_io_concurrency = 0;\n"
        "VACUUM (VERBOSE, FREEZE, PARALLEL 0) {t};\n"
        "\\echo VACUUM\n".format(t=table1)
    )
    assert primary.poll_query_until(
        "SELECT count(*) >= 1 FROM pg_stat_activity\n"
        "    WHERE pid = {pid}\n"
        "    AND wait_event = 'BufferCleanup';".format(pid=vacuum_pid),
        "t",
        dbname=test_db,
    )
    assert replica.poll_query_until(
        "SELECT EXISTS (SELECT * FROM pg_stat_wal_receiver);", "f", dbname=test_db
    )
    replica.safe_psql(
        "ALTER SYSTEM SET primary_conninfo = '{}';\n"
        "SELECT pg_reload_conf();".format(orig_conninfo),
        dbname=test_db,
    )
    assert replica.poll_query_until(
        "SELECT EXISTS (SELECT * FROM pg_stat_wal_receiver);", "t", dbname=test_db
    )
    assert primary.poll_query_until(
        "SELECT EXISTS (SELECT * FROM pg_stat_replication);", "t", dbname=test_db
    )
    res = psql_b.query_safe("FETCH {}".format(cursor1))
    assert (
        res == "7"
    ), "Cursor query returned {} from second fetch. Expected value 7.".format(res)
    assert primary.poll_query_until(
        "SELECT index_vacuum_count > 0\n"
        "FROM pg_stat_progress_vacuum\n"
        "WHERE datname='{db}' AND relid::regclass = '{t}'::regclass;".format(
            db=test_db, t=table1
        ),
        "t",
        dbname=test_db,
    )
    psql_b.query_until(re.compile(r"^commit$", re.M), "COMMIT;\n\\echo commit\n")
    assert primary.poll_query_until(
        "SELECT vacuum_count > 0\n"
        "FROM pg_stat_all_tables WHERE relname = '{t}';".format(t=table1),
        "t",
        dbname=test_db,
    )
    primary_lsn = primary.lsn("flush")
    primary.safe_psql("INSERT INTO {t} VALUES (1);".format(t=table1), dbname=test_db)
    primary.wait_for_catchup(replica, "replay", primary_lsn)
    psql_a.quit()
    psql_b.quit()
    replica.stop()
    primary.stop()

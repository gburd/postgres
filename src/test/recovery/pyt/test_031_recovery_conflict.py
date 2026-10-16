# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/recovery/t/031_recovery_conflict.pl.

Exercises each recovery-conflict type on a hot-standby: buffer-pin, snapshot,
lock, tablespace, startup-deadlock, and database conflicts. For each, a standby
session is made to conflict with replayed primary activity; the standby logs the
expected "cancelled due to recovery conflict" reason and bumps the matching
pg_stat_database_conflicts counter. The total is checked against
pg_stat_database.conflicts.
"""

import re

import pypg


class _Conflicts:
    """Mutable state shared with the nested check helpers."""

    def __init__(self, standby, test_db):
        self.standby = standby
        self.test_db = test_db
        self.log_location = standby.current_log_position()
        self.sect = ""

    def check_log(self, message):
        old = self.log_location
        self.log_location = self.standby.wait_for_log(re.escape(message), old)
        assert self.log_location > old, (
            "{}: logfile contains terminated connection due to recovery "
            "conflict".format(self.sect)
        )

    def check_stat(self, conflict_type):
        count = self.standby.safe_psql(
            "SELECT confl_{} FROM pg_stat_database_conflicts WHERE "
            "datname='{}';".format(conflict_type, self.test_db),
            dbname=self.test_db,
        )
        assert count == "1", "{}: stats show conflict on standby".format(self.sect)


def test_031_recovery_conflict(create_pg):
    """Each recovery-conflict type is logged and counted on the standby."""
    tablespace1 = "test_recovery_conflict_tblspc"
    primary = create_pg("primary", allows_streaming=True, start=False)
    primary.append_conf(
        "\nallow_in_place_tablespaces = on\nlog_temp_files = 0\n"
        "max_prepared_transactions = 10\nmax_standby_streaming_delay = 50ms\n"
        "temp_tablespaces = {}\nlog_recovery_conflict_waits = on\n"
        "deadlock_timeout = 10ms\n".format(tablespace1)
    )
    primary.start()
    backup_name = "my_backup"
    primary.safe_psql("CREATE TABLESPACE {} LOCATION ''".format(tablespace1))
    primary.backup(backup_name)
    standby = create_pg(
        "standby",
        from_backup=(primary, backup_name),
        has_streaming=True,
        start=False,
    )
    standby.start()
    test_db = "test_db"
    primary.safe_psql("CREATE DATABASE {}".format(test_db))
    table1 = "test_recovery_conflict_table1"
    table2 = "test_recovery_conflict_table2"
    primary.safe_psql(
        "CREATE TABLE {t1}(a int, b int);\n"
        "INSERT INTO {t1} SELECT i % 3, 0 FROM generate_series(1,20) i;\n"
        "CREATE TABLE {t2}(a int, b int);".format(t1=table1, t2=table2),
        dbname=test_db,
    )
    primary.wait_for_replay_catchup(standby)
    psql = standby.background_psql(test_db, on_error_stop=False)
    state = _Conflicts(standby, test_db)
    cursor1 = "test_recovery_conflict_cursor"
    expected = 0
    expected += _buffer_pin(primary, standby, psql, state, table1, cursor1, test_db)
    expected += _snapshot(primary, standby, psql, state, table1, cursor1, test_db)
    expected += _lock(primary, standby, psql, state, table1, cursor1, test_db)
    expected += _tablespace(
        primary, standby, psql, state, tablespace1, cursor1, test_db
    )
    expected += _startup_deadlock(
        primary, standby, psql, state, table1, table2, cursor1, test_db
    )
    assert standby.safe_psql(
        "SELECT conflicts FROM pg_stat_database WHERE datname='{}';".format(test_db),
        dbname=test_db,
    ) == str(expected), "{} recovery conflicts shown in pg_stat_database".format(
        expected
    )
    state.sect = "database conflict"
    primary.safe_psql("DROP DATABASE {};".format(test_db))
    primary.wait_for_replay_catchup(standby)
    state.check_log("User was connected to a database that must be dropped")
    psql.quit()
    standby.stop()
    primary.stop()


def _buffer_pin(primary, standby, psql, state, table1, cursor1, test_db):
    state.sect = "buffer pin conflict"
    primary.safe_psql(
        "BEGIN;\nINSERT INTO {t} VALUES (1,0);\nROLLBACK;\n"
        "BEGIN; LOCK {t}; COMMIT;".format(t=table1),
        dbname=test_db,
    )
    primary.wait_for_replay_catchup(standby)
    res = psql.query_safe(
        "BEGIN;\nDECLARE {c} CURSOR FOR SELECT b FROM {t};\n"
        "FETCH FORWARD FROM {c};".format(c=cursor1, t=table1)
    )
    assert re.search(
        r"^0$", res, re.M
    ), "{}: cursor with conflicting pin established".format(state.sect)
    state.log_location = standby.current_log_position()
    primary.safe_psql("VACUUM FREEZE {};".format(table1), dbname=test_db)
    primary.wait_for_replay_catchup(standby)
    state.check_log("User was holding shared buffer pin for too long")
    psql.reconnect_and_clear()
    state.check_stat("bufferpin")
    return 1


def _snapshot(primary, standby, psql, state, table1, cursor1, test_db):
    state.sect = "snapshot conflict"
    primary.safe_psql(
        "INSERT INTO {} SELECT i, 0 FROM generate_series(1,20) i".format(table1),
        dbname=test_db,
    )
    primary.wait_for_replay_catchup(standby)
    res = psql.query_safe(
        "BEGIN;\nDECLARE {c} CURSOR FOR SELECT b FROM {t};\n"
        "FETCH FORWARD FROM {c};".format(c=cursor1, t=table1)
    )
    assert re.search(
        r"^0$", res, re.M
    ), "{}: cursor with conflicting snapshot established".format(state.sect)
    primary.safe_psql(
        "UPDATE {} SET a = a + 1 WHERE a > 2;".format(table1), dbname=test_db
    )
    primary.safe_psql("VACUUM FREEZE {};".format(table1), dbname=test_db)
    primary.wait_for_replay_catchup(standby)
    state.check_log(
        "User query might have needed to see row versions that must be removed"
    )
    psql.reconnect_and_clear()
    state.check_stat("snapshot")
    return 1


def _lock(primary, standby, psql, state, table1, _cursor1, test_db):
    state.sect = "lock conflict"
    res = psql.query_safe(
        "BEGIN;\nLOCK TABLE {} IN ACCESS SHARE MODE;\nSELECT 1;".format(table1)
    )
    assert re.search(r"^1$", res, re.M), "{}: conflicting lock acquired".format(
        state.sect
    )
    primary.safe_psql("DROP TABLE {};".format(table1), dbname=test_db)
    primary.wait_for_replay_catchup(standby)
    state.check_log("User was holding a relation lock for too long")
    psql.reconnect_and_clear()
    state.check_stat("lock")
    return 1


def _tablespace(primary, standby, psql, state, tablespace1, cursor1, test_db):
    state.sect = "tablespace conflict"
    res = psql.query_safe(
        "BEGIN;\nSET work_mem = '64kB';\nDECLARE {c} CURSOR FOR\n"
        "  SELECT count(*) FROM generate_series(1,6000);\n"
        "FETCH FORWARD FROM {c};".format(c=cursor1)
    )
    assert re.search(
        r"^6000$", res, re.M
    ), "{}: cursor with conflicting temp file established".format(state.sect)
    primary.safe_psql("DROP TABLESPACE {};".format(tablespace1), dbname=test_db)
    primary.wait_for_replay_catchup(standby)
    state.check_log("User was or might have been using tablespace that must be dropped")
    psql.reconnect_and_clear()
    state.check_stat("tablespace")
    return 1


def _startup_deadlock(primary, standby, psql, state, table1, table2, cursor1, test_db):
    state.sect = "startup deadlock"
    standby.adjust_conf(
        "max_standby_streaming_delay",
        "{}s".format(pypg.test_timeout_default()),
    )
    standby.restart()
    psql.reconnect_and_clear()
    primary.safe_psql(
        "CREATE TABLE {t1}(a int, b int);\nINSERT INTO {t1} VALUES (1);\n"
        "BEGIN;\nINSERT INTO {t1}(a) SELECT generate_series(1, 100) i;\n"
        "ROLLBACK;\nBEGIN;\nLOCK TABLE {t2};\nPREPARE TRANSACTION 'lock';\n"
        "INSERT INTO {t1}(a) VALUES (170);\nSELECT txid_current();".format(
            t1=table1, t2=table2
        ),
        dbname=test_db,
    )
    primary.wait_for_replay_catchup(standby)
    psql.query_until(
        re.compile(r"^1$", re.M),
        "BEGIN;\nDECLARE {c} CURSOR FOR SELECT a FROM {t1};\n"
        "FETCH FORWARD FROM {c};\nSELECT * FROM {t2};\n".format(
            c=cursor1, t1=table1, t2=table2
        ),
    )
    assert standby.poll_query_until(
        "SELECT 'waiting' FROM pg_locks WHERE locktype = 'relation' AND NOT "
        "granted;",
        "waiting",
    ), "{}: lock acquisition is waiting".format(state.sect)
    primary.safe_psql("VACUUM FREEZE {};".format(table1), dbname=test_db)
    primary.wait_for_replay_catchup(standby)
    state.check_log("User transaction caused buffer deadlock with recovery.")
    psql.reconnect_and_clear()
    state.check_stat("deadlock")
    primary.safe_psql("ROLLBACK PREPARED 'lock';", dbname=test_db)
    standby.adjust_conf("max_standby_streaming_delay", "50ms")
    standby.restart()
    psql.reconnect_and_clear()
    return 1

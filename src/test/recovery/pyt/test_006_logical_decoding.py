# Copyright (c) 2017-2026, PostgreSQL Global Development Group

"""Port of src/test/recovery/t/006_logical_decoding.pl.

Core logical decoding behaviour on a single node: replication-command error
paths, SQL and pg_recvlogical decoding producing identical output, decoding
state surviving a fast restart, cross-database slot-use failures, a database
with an active logical slot refusing to drop (and succeeding once inactive),
logical slot advance persisting across restarts, and pg_stat_replication_slots
statistics/reset semantics.
"""

import platform
import subprocess

import pypg

_EXPECTED = (
    "BEGIN\n"
    "table public.decoding_test: INSERT: x[integer]:1 y[text]:'1'\n"
    "table public.decoding_test: INSERT: x[integer]:2 y[text]:'2'\n"
    "table public.decoding_test: INSERT: x[integer]:3 y[text]:'3'\n"
    "table public.decoding_test: INSERT: x[integer]:4 y[text]:'4'\n"
    "COMMIT"
)


def _expect_stderr(node, query, pattern, msg, replication=None):
    res = node.psql_capture(query, dbname="template1", replication=replication)
    assert pattern in res.stderr, "{}: {!r} not in {!r}".format(
        msg, pattern, res.stderr
    )


def test_006_logical_decoding(create_pg):
    """Logical decoding error paths, output, restart, drop, advance and stats."""
    node = create_pg("primary", allows_streaming=True, start=False)
    node.append_conf("\nwal_level = logical\n")
    node.start()
    node.safe_psql("CREATE TABLE decoding_test(x integer, y text);")
    node.safe_psql(
        "SELECT pg_create_logical_replication_slot('test_slot', 'test_decoding');"
    )
    _expect_stderr(
        node,
        "START_REPLICATION SLOT test_slot LOGICAL 0/0",
        'replication slot "test_slot" was not created in this database',
        "Logical decoding correctly fails to start",
        replication="database",
    )
    _expect_stderr(
        node,
        "READ_REPLICATION_SLOT test_slot;",
        "cannot use READ_REPLICATION_SLOT with a logical replication slot",
        "READ_REPLICATION_SLOT not supported for logical slots",
        replication="database",
    )
    _expect_stderr(
        node,
        "START_REPLICATION SLOT s1 LOGICAL 0/1",
        "ERROR:  logical decoding requires a database connection",
        "Logical decoding fails on non-database connection",
        replication="true",
    )
    node.safe_psql(
        "INSERT INTO decoding_test(x,y) SELECT s, s::text "
        "FROM generate_series(1,10) s;"
    )
    result = node.safe_psql(
        "SELECT pg_logical_slot_get_changes('test_slot', NULL, NULL);"
    )
    assert len(result.split("\n")) == 12, "Decoding produced 12 rows inc BEGIN/COMMIT"
    node.restart()
    result = node.safe_psql(
        "SELECT pg_logical_slot_get_changes('test_slot', NULL, NULL);"
    )
    assert result == "", "Decoding after fast restart repeats no rows"
    node.safe_psql(
        "INSERT INTO decoding_test(x,y) SELECT s, s::text "
        "FROM generate_series(1,4) s;"
    )
    stdout_sql = node.safe_psql(
        "SELECT data FROM pg_logical_slot_peek_changes('test_slot', NULL, NULL, "
        "'include-xids', '0', 'skip-empty-xacts', '1');"
    )
    assert stdout_sql == _EXPECTED, "got expected output from SQL decoding session"
    endpos = node.safe_psql(
        "SELECT lsn FROM pg_logical_slot_peek_changes('test_slot', NULL, NULL) "
        "ORDER BY lsn DESC LIMIT 1;"
    )
    node.safe_psql(
        "INSERT INTO decoding_test(x,y) SELECT s, s::text "
        "FROM generate_series(5,50) s;"
    )
    opts = {"include-xids": "0", "skip-empty-xacts": "1"}
    stdout_recv = node.pg_recvlogical_upto(
        "postgres", "test_slot", endpos, pypg.test_timeout_default(), options=opts
    )
    assert (
        stdout_recv.rstrip("\n") == _EXPECTED
    ), "got same expected output from pg_recvlogical decoding session"
    assert node.poll_query_until(
        "SELECT EXISTS (SELECT 1 FROM pg_replication_slots "
        "WHERE slot_name = 'test_slot' AND active_pid IS NULL)"
    ), "slot never became inactive"
    stdout_recv = node.pg_recvlogical_upto(
        "postgres", "test_slot", endpos, pypg.test_timeout_default(), options=opts
    )
    assert stdout_recv.rstrip("\n") == "", "pg_recvlogical acknowledged changes"
    _otherdb_phase(node)
    _advance_and_stats_phase(node)
    node.stop()


def _otherdb_phase(node):
    """A DB with an active logical slot refuses to drop; succeeds once inactive."""
    node.safe_psql("CREATE DATABASE otherdb")
    assert (
        node.psql_capture(
            "SELECT lsn FROM pg_logical_slot_peek_changes('test_slot', NULL, NULL) "
            "ORDER BY lsn DESC LIMIT 1;",
            dbname="otherdb",
        ).rc
        == 3
    ), "replaying logical slot from another database fails"
    node.safe_psql(
        "SELECT pg_create_logical_replication_slot('otherdb_slot', 'test_decoding');",
        dbname="otherdb",
    )
    if platform.system() != "Windows":
        recv = subprocess.Popen(  # pylint: disable=consider-using-with
            [
                "pg_recvlogical",
                "--dbname",
                node.connstr("otherdb"),
                "--slot",
                "otherdb_slot",
                "--file",
                "-",
                "--start",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            assert node.poll_query_until(
                "SELECT EXISTS (SELECT 1 FROM pg_replication_slots "
                "WHERE slot_name = 'otherdb_slot' AND active_pid IS NOT NULL)",
                dbname="otherdb",
            ), "slot never became active"
            assert (
                node.psql_capture("DROP DATABASE otherdb").rc == 3
            ), "dropping a DB with active logical slots fails"
        finally:
            recv.terminate()
            recv.wait()
        assert (
            node.slot("otherdb_slot")["plugin"] == "test_decoding"
        ), "logical slot still exists"
    assert node.poll_query_until(
        "SELECT EXISTS (SELECT 1 FROM pg_replication_slots "
        "WHERE slot_name = 'otherdb_slot' AND active_pid IS NULL)",
        dbname="otherdb",
    ), "slot never became inactive"
    assert (
        node.psql_capture("DROP DATABASE otherdb").rc == 0
    ), "dropping a DB with inactive logical slots succeeds"
    assert (
        node.slot("otherdb_slot")["plugin"] == ""
    ), "logical slot was actually dropped with DB"


def _advance_and_stats_phase(node):
    """Logical slot advance persists across restart; stats/reset semantics."""
    logical_slot = "logical_slot"
    node.safe_psql(
        "SELECT pg_create_logical_replication_slot('{}', 'test_decoding', "
        "false, false, true);".format(logical_slot)
    )
    node.safe_psql(
        "CREATE TABLE tab_logical_slot (a int);\n"
        "INSERT INTO tab_logical_slot VALUES (generate_series(1,10));"
    )
    current_lsn = node.safe_psql("SELECT pg_current_wal_lsn();")
    assert (
        node.psql_capture(
            "SELECT pg_replication_slot_advance('{}', '{}'::pg_lsn);".format(
                logical_slot, current_lsn
            )
        ).rc
        == 0
    ), "slot advancing with logical slot"
    pre = node.safe_psql(
        "SELECT restart_lsn from pg_replication_slots "
        "WHERE slot_name = '{}';".format(logical_slot)
    )
    node.restart()
    post = node.safe_psql(
        "SELECT restart_lsn from pg_replication_slots "
        "WHERE slot_name = '{}';".format(logical_slot)
    )
    assert pre == post, "logical slot advance persists across restarts"
    assert (
        node.safe_psql(
            "SELECT total_bytes > 0, stats_reset IS NULL "
            "FROM pg_stat_replication_slots WHERE slot_name = 'test_slot'"
        )
        == "t|t"
    ), "Total bytes is > 0 and stats_reset is NULL for slot 'test_slot'."
    node.safe_psql("SELECT pg_stat_reset_replication_slot('test_slot')")
    reset1 = node.safe_psql(
        "SELECT stats_reset FROM pg_stat_replication_slots "
        "WHERE slot_name = 'test_slot'"
    )
    node.safe_psql("SELECT pg_stat_reset_replication_slot('test_slot')")
    assert (
        node.safe_psql(
            "SELECT stats_reset > '{}'::timestamptz, total_bytes = 0 "
            "FROM pg_stat_replication_slots WHERE slot_name = 'test_slot'".format(
                reset1
            )
        )
        == "t|t"
    ), "reset timestamp later after second reset and total_bytes 0"
    assert (
        node.safe_psql(
            "SELECT stats_reset IS NULL FROM pg_stat_replication_slots "
            "WHERE slot_name = 'logical_slot'"
        )
        == "t"
    ), "Stats_reset is NULL for slot 'logical_slot' before reset."
    reset1 = node.safe_psql(
        "SELECT stats_reset FROM pg_stat_replication_slots "
        "WHERE slot_name = 'test_slot'"
    )
    node.safe_psql("SELECT pg_stat_reset_replication_slot(NULL)")
    assert (
        node.safe_psql(
            "SELECT stats_reset IS NOT NULL FROM pg_stat_replication_slots "
            "WHERE slot_name = 'logical_slot'"
        )
        == "t"
    ), "Stats_reset is not NULL for slot 'logical_slot' after reset all."
    assert (
        node.safe_psql(
            "SELECT stats_reset > '{}'::timestamptz FROM pg_stat_replication_slots "
            "WHERE slot_name = 'test_slot'".format(reset1)
        )
        == "t"
    ), "reset timestamp later after resetting stats again."

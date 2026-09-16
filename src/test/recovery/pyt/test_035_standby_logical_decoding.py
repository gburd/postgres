# Copyright (c) 2023-2026, PostgreSQL Global Development Group

"""Port of src/test/recovery/t/035_standby_logical_decoding.pl.

Logical decoding on a hot standby and its conflict-with-recovery behaviour.
A primary (wal_level=logical) feeds a standby that holds logical replication
slots; primary-side actions that advance the catalog xid horizon (VACUUM /
VACUUM FULL on pg_class and pg_authid, on-access pruning of a
user_catalog_table, lowering wal_level, DROP DATABASE) must invalidate the
standby's logical slots with the expected invalidation_reason, log
"invalidating obsolete replication slot", and bump
pg_stat_database_conflicts.confl_active_logicalslot.  Also covers: a standby
refusing to start with a pre-existing logical slot while hot_standby = off,
basic SQL and pg_recvlogical decoding on the standby, subscribing on the
standby to a primary publication, and decoding pre/post-promotion rows on the
promoted standby and its cascading standby.  An injection point
'skip-log-running-xacts' (attached as 'error') prevents xl_running_xacts from
advancing the active slot's catalog_xmin during the conflict scenarios.

Requires an injection-points build.
"""

import os
import re
import subprocess
import threading
import time
from typing import cast

import pytest

import pypg
from pypg import PostgresServer

# Name for the physical slot on primary
_PRIMARY_SLOTNAME = "primary_physical"
_STANDBY_PHYSICAL_SLOTNAME = "standby_physical"

_EXPECTED_BASIC = (
    "BEGIN\n"
    "table public.decoding_test: INSERT: x[integer]:1 y[text]:'1'\n"
    "table public.decoding_test: INSERT: x[integer]:2 y[text]:'2'\n"
    "table public.decoding_test: INSERT: x[integer]:3 y[text]:'3'\n"
    "table public.decoding_test: INSERT: x[integer]:4 y[text]:'4'\n"
    "COMMIT"
)

_EXPECTED_PROMOTION = (
    "BEGIN\n"
    "table public.decoding_test: INSERT: x[integer]:1 y[text]:'1'\n"
    "table public.decoding_test: INSERT: x[integer]:2 y[text]:'2'\n"
    "table public.decoding_test: INSERT: x[integer]:3 y[text]:'3'\n"
    "table public.decoding_test: INSERT: x[integer]:4 y[text]:'4'\n"
    "COMMIT\n"
    "BEGIN\n"
    "table public.decoding_test: INSERT: x[integer]:5 y[text]:'5'\n"
    "table public.decoding_test: INSERT: x[integer]:6 y[text]:'6'\n"
    "table public.decoding_test: INSERT: x[integer]:7 y[text]:'7'\n"
    "COMMIT"
)


_SQL_DROP_CONFLICT_TABLE = (
    "CREATE TABLE conflict_test(x integer, y text);\nDROP TABLE conflict_test;"
)


class _RecvLogical:
    """A background ``pg_recvlogical --start`` process for a standby slot.

    The Python analogue of the ``IPC::Run::start`` handle the Perl test keeps in
    ``$handle``: stdout/stderr are captured by reader threads so the test can
    poll the accumulated stdout for a pattern (mirroring ``pump_until``) and, on
    ``finish``, inspect the exit code and stderr (mirroring
    ``check_pg_recvlogical_stderr``).
    """

    def __init__(self, bindir, slot_name, connstr, env, timeout):
        self._cmd = [
            str(bindir / "pg_recvlogical"),
            "--dbname",
            connstr,
            "--slot",
            slot_name,
            "--option",
            "include-xids=0",
            "--option",
            "skip-empty-xacts=1",
            "--file",
            "-",
            "--no-loop",
            "--start",
        ]
        self._timeout = timeout
        self._lock = threading.Lock()
        self._stdout = ""
        self._stderr = ""
        # pylint: disable=consider-using-with  # long-lived; closed in finish()
        self._proc = subprocess.Popen(
            self._cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )
        self._threads = [
            threading.Thread(target=self._reader, args=("out",), daemon=True),
            threading.Thread(target=self._reader, args=("err",), daemon=True),
        ]
        for thread in self._threads:
            thread.start()

    def _reader(self, which):
        stream = self._proc.stdout if which == "out" else self._proc.stderr
        assert stream is not None
        for line in iter(stream.readline, ""):
            with self._lock:
                if which == "out":
                    self._stdout += line
                else:
                    self._stderr += line

    @property
    def stdout(self):
        """The accumulated stdout captured so far."""
        with self._lock:
            return self._stdout

    @property
    def stderr(self):
        """The accumulated stderr captured so far."""
        with self._lock:
            return self._stderr

    def pump_until(self, pattern):
        """Poll captured stdout until pattern matches; raise on timeout.

        Mirrors ``PostgreSQL::Test::Utils::pump_until`` over the recvlogical
        handle: the regex is applied (DOTALL) to all stdout seen so far.
        """
        regex = re.compile(pattern, re.DOTALL)
        deadline = time.monotonic() + self._timeout
        while True:
            with self._lock:
                if regex.search(self._stdout):
                    return
            if self._proc.poll() is not None:
                with self._lock:
                    if regex.search(self._stdout):
                        return
                raise AssertionError(
                    "pg_recvlogical exited before producing expected output;"
                    " stdout:\n{}\nstderr:\n{}".format(self._stdout, self._stderr)
                )
            if time.monotonic() > deadline:
                raise TimeoutError(
                    "timed out waiting for pg_recvlogical stdout to match {!r};"
                    " stdout:\n{}\nstderr:\n{}".format(
                        pattern, self._stdout, self._stderr
                    )
                )
            time.sleep(0.05)

    def finish(self):
        """Wait for the process to exit and return its exit code.

        Mirrors ``$handle->finish``: closes stdout/stderr after the child is
        done so the captured buffers are complete. Only valid once the server
        has terminated the client (e.g. after a slot conflict); a still-
        streaming ``--no-loop --start`` would never exit on its own.
        """
        returncode = self._proc.wait(timeout=self._timeout)
        for thread in self._threads:
            thread.join(timeout=1)
        for stream in (self._proc.stdout, self._proc.stderr):
            if stream is not None:
                stream.close()
        return returncode

    def terminate(self):
        """Kill the background process if it is still running and reap it.

        Used at test cleanup to mirror the Perl harness tearing down a cluster
        with a still-attached pg_recvlogical (which never exits on its own).
        """
        if self._proc.poll() is None:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=self._timeout)
            except subprocess.TimeoutExpired:
                self._proc.kill()
                self._proc.wait(timeout=self._timeout)
        for thread in self._threads:
            thread.join(timeout=1)
        for stream in (self._proc.stdout, self._proc.stderr):
            if stream is not None and not stream.closed:
                stream.close()


class _Nodes:
    """Holds the four cluster nodes and shared per-run state."""

    def __init__(self, bindir, timeout):
        self.bindir = bindir
        self.timeout = timeout
        self.handles: list["_RecvLogical"] = []
        # Assigned in stages by the _init_*/setup helpers below. They are typed
        # non-optional (and cast from None) so the type checker tracks the
        # server API; every helper sets a node before any other reads it.
        self.primary = cast(PostgresServer, None)
        self.standby = cast(PostgresServer, None)
        self.cascading_standby = cast(PostgresServer, None)
        self.subscriber = cast(PostgresServer, None)
        self.handle = cast("_RecvLogical", None)


def _wait_for_xmins(node, slotname, check_expr):
    """Wait until the slot's xmin columns satisfy check_expr."""
    assert node.poll_query_until(
        "SELECT {expr}\n"
        "FROM pg_catalog.pg_replication_slots\n"
        "WHERE slot_name = '{slot}';".format(expr=check_expr, slot=slotname)
    ), "Timed out waiting for slot xmins to advance"


def _create_logical_slots(ctx, node, slot_prefix):
    """Create the required logical slots on a standby (active + inactive)."""
    node.create_logical_slot_on_standby(
        ctx.primary, slot_prefix + "inactiveslot", "testdb"
    )
    node.create_logical_slot_on_standby(
        ctx.primary, slot_prefix + "activeslot", "testdb"
    )


def _drop_logical_slots(ctx, slot_prefix):
    """Drop the logical slots on the standby."""
    ctx.standby.psql_capture(
        "SELECT pg_drop_replication_slot('{}inactiveslot')".format(slot_prefix)
    )
    ctx.standby.psql_capture(
        "SELECT pg_drop_replication_slot('{}activeslot')".format(slot_prefix)
    )


def _make_slot_active(ctx, node, slot_prefix, wait):
    """Acquire a standby 'activeslot' via background pg_recvlogical.

    With wait=True, poll until the slot has a non-NULL active_pid (mirrors the
    Perl helper's success path); otherwise this is a known-failure scenario.
    """
    active_slot = slot_prefix + "activeslot"
    handle = _RecvLogical(
        ctx.bindir,
        active_slot,
        node.connstr("testdb"),
        node._connenv(),  # pylint: disable=protected-access
        ctx.timeout,
    )
    ctx.handles.append(handle)
    if wait:
        assert node.poll_query_until(
            "SELECT EXISTS (SELECT 1 FROM pg_replication_slots "
            "WHERE slot_name = '{}' AND active_pid IS NOT NULL)".format(active_slot),
            dbname="testdb",
        ), "slot never became active"
    return handle


def _check_pg_recvlogical_stderr(handle, check_stderr):
    """Assert the recvlogical client exited non-zero and stderr matches."""
    returncode = handle.finish()
    assert returncode != 0, "pg_recvlogical exited non-zero"
    assert re.search(check_stderr, handle.stderr), "slot has been invalidated"


def _check_slots_dropped(ctx, slot_prefix, handle):
    """Assert both standby slots were dropped and the client conflicted."""
    assert (
        ctx.standby.slot(slot_prefix + "inactiveslot")["slot_type"] == ""
    ), "inactiveslot on standby dropped"
    assert (
        ctx.standby.slot(slot_prefix + "activeslot")["slot_type"] == ""
    ), "activeslot on standby dropped"
    _check_pg_recvlogical_stderr(handle, "conflict with recovery")


def _change_hsf_and_wait_for_xmins(ctx, hsf, invalidated):
    """Set hot_standby_feedback and wait for the expected xmin state."""
    ctx.standby.append_conf("\nhot_standby_feedback = {}\n".format(hsf))
    ctx.standby.reload()
    if hsf and invalidated:
        _wait_for_xmins(
            ctx.primary,
            _PRIMARY_SLOTNAME,
            "xmin IS NOT NULL AND catalog_xmin IS NULL",
        )
    elif hsf:
        _wait_for_xmins(
            ctx.primary,
            _PRIMARY_SLOTNAME,
            "xmin IS NOT NULL AND catalog_xmin IS NOT NULL",
        )
    else:
        _wait_for_xmins(
            ctx.primary,
            _PRIMARY_SLOTNAME,
            "xmin IS NULL AND catalog_xmin IS NULL",
        )


def _check_slots_conflict_reason(ctx, slot_prefix, reason):
    """Assert invalidation_reason of both conflicting slots equals reason."""
    for kind in ("activeslot", "inactiveslot"):
        slot = slot_prefix + kind
        res = ctx.standby.safe_psql(
            "select invalidation_reason from pg_replication_slots "
            "where slot_name = '{}' and conflicting;".format(slot)
        )
        assert res == reason, "{} reason for conflict is {}".format(slot, reason)


def _reactive_slots_change_hfs_and_wait_for_xmins(
    ctx, previous_slot_prefix, slot_prefix, hsf, invalidated
):
    """Re-create slots under a new prefix, set hsf, activate, reset stats."""
    _drop_logical_slots(ctx, previous_slot_prefix)
    _create_logical_slots(ctx, ctx.standby, slot_prefix)
    _change_hsf_and_wait_for_xmins(ctx, hsf, invalidated)
    ctx.handle = _make_slot_active(ctx, ctx.standby, slot_prefix, True)
    # reset stat: easier to check confl_active_logicalslot
    ctx.standby.psql_capture("select pg_stat_reset();", dbname="testdb")


def _check_for_invalidation(ctx, slot_prefix, log_start, test_name):
    """Assert invalidation is logged for both slots and conflict stat bumped."""
    active_slot = slot_prefix + "activeslot"
    inactive_slot = slot_prefix + "inactiveslot"
    assert ctx.standby.log_matches(
        'invalidating obsolete replication slot "{}"'.format(inactive_slot),
        log_start,
    ), "inactiveslot slot invalidation is logged {}".format(test_name)
    assert ctx.standby.log_matches(
        'invalidating obsolete replication slot "{}"'.format(active_slot),
        log_start,
    ), "activeslot slot invalidation is logged {}".format(test_name)
    assert ctx.standby.poll_query_until(
        "select (confl_active_logicalslot = 1) from pg_stat_database_conflicts "
        "where datname = 'testdb'"
    ), "Timed out waiting confl_active_logicalslot to be updated"


def _wait_until_vacuum_can_remove(ctx, vac_option, sql, to_vac):
    """Advance the xid horizon then VACUUM, guarding xl_running_xacts.

    The injection point keeps xl_running_xacts from advancing the active slot's
    catalog_xmin, which would otherwise prevent the expected conflict.
    """
    ctx.primary.safe_psql(
        "SELECT injection_points_attach('skip-log-running-xacts', 'error');",
        dbname="testdb",
    )
    xid_horizon = ctx.primary.safe_psql(
        "select pg_snapshot_xmin(pg_current_snapshot());", dbname="testdb"
    )
    ctx.primary.safe_psql(sql, dbname="testdb")
    assert ctx.primary.poll_query_until(
        "SELECT (select pg_snapshot_xmin(pg_current_snapshot())::text::int "
        "- {}) > 0".format(xid_horizon),
        dbname="testdb",
    ), "new snapshot does not have a newer horizon"
    ctx.primary.safe_psql(
        "VACUUM {} verbose {};\n"
        "INSERT INTO flush_wal DEFAULT VALUES;".format(vac_option, to_vac),
        dbname="testdb",
    )
    ctx.primary.wait_for_replay_catchup(ctx.standby)
    ctx.primary.safe_psql(
        "SELECT injection_points_detach('skip-log-running-xacts');",
        dbname="testdb",
    )


def _init_primary(ctx, create_pg):
    """Initialize the primary node and the b1 backup; return the backup name."""
    primary = create_pg(
        "primary", allows_streaming=True, has_archiving=True, start=False
    )
    ctx.primary = primary
    primary.append_conf(
        "\nwal_level = 'logical'\nmax_replication_slots = 4\n"
        "max_wal_senders = 4\nautovacuum = off\n"
    )
    primary.dump_info()
    primary.start()
    if not primary.check_extension("injection_points"):
        pytest.skip("Extension injection_points not installed")
    primary.psql_capture("CREATE DATABASE testdb")
    primary.safe_psql(
        "SELECT * FROM pg_create_physical_replication_slot('{}');".format(
            _PRIMARY_SLOTNAME
        ),
        dbname="testdb",
    )
    assert (
        primary.safe_psql(
            "SELECT conflicting is null FROM pg_replication_slots "
            "where slot_name = '{}';".format(_PRIMARY_SLOTNAME)
        )
        == "t"
    ), "Physical slot reports conflicting as NULL"
    backup_name = "b1"
    primary.backup(backup_name)
    # flush_wal lets us force a WAL flush after a VACUUM, which does not flush.
    primary.psql_capture("CREATE TABLE flush_wal();", dbname="testdb")
    return backup_name


def _init_standby(ctx, create_pg, backup_name):
    """Initialize the streaming+restoring standby and wait for catchup."""
    standby = create_pg(
        "standby",
        from_backup=(ctx.primary, backup_name),
        has_streaming=True,
        has_restoring=True,
        start=False,
    )
    ctx.standby = standby
    standby.append_conf(
        "primary_slot_name = '{}'\nmax_replication_slots = 5".format(_PRIMARY_SLOTNAME)
    )
    standby.start()
    ctx.primary.wait_for_replay_catchup(standby)


def _test_hot_standby_off_refusal(ctx):
    """A pre-existing logical slot makes the standby refuse hot_standby = off."""
    ctx.standby.create_logical_slot_on_standby(ctx.primary, "restart_test", "postgres")
    ctx.standby.stop()
    ctx.standby.append_conf("hot_standby = off")
    # The server is expected to fail during startup, so do not use start().
    ctx.standby.bin.run_command(
        [
            "pg_ctl",
            "--pgdata",
            str(ctx.standby.datadir),
            "--log",
            str(ctx.standby.log),
            "start",
        ]
    )
    pidfile = ctx.standby.datadir / "postmaster.pid"
    deadline = time.monotonic() + ctx.timeout
    while pidfile.exists() and time.monotonic() < deadline:
        time.sleep(0.1)
    logfile = pypg.slurp_file(ctx.standby.log)
    assert re.search(
        r'FATAL: .* logical replication slot ".*" exists on the standby, '
        r'but "hot_standby" = "off"',
        logfile,
    ), "the standby ends with an error during startup because hot_standby disabled"
    ctx.standby.adjust_conf("hot_standby", "on")
    ctx.standby.start()
    ctx.standby.safe_psql("SELECT pg_drop_replication_slot('restart_test')")


def _test_basic_decoding(ctx):
    """Basic SQL and pg_recvlogical decoding work on the standby."""
    ctx.primary.safe_psql(
        "CREATE TABLE decoding_test(x integer, y text);", dbname="testdb"
    )
    ctx.primary.safe_psql(
        "INSERT INTO decoding_test(x,y) SELECT s, s::text "
        "FROM generate_series(1,10) s;",
        dbname="testdb",
    )
    ctx.primary.wait_for_replay_catchup(ctx.standby)
    result = ctx.standby.safe_psql(
        "SELECT pg_logical_slot_get_changes('behaves_ok_activeslot', NULL, NULL);",
        dbname="testdb",
    )
    assert (
        len(result.split("\n")) == 14
    ), "Decoding produced 14 rows (2 BEGIN/COMMIT and 10 rows)"
    ctx.primary.safe_psql(
        "INSERT INTO decoding_test(x,y) SELECT s, s::text "
        "FROM generate_series(1,4) s;",
        dbname="testdb",
    )
    ctx.primary.wait_for_replay_catchup(ctx.standby)
    stdout_sql = ctx.standby.safe_psql(
        "SELECT data FROM pg_logical_slot_peek_changes('behaves_ok_activeslot', "
        "NULL, NULL, 'include-xids', '0', 'skip-empty-xacts', '1');",
        dbname="testdb",
    )
    assert stdout_sql == _EXPECTED_BASIC, "got expected output from SQL decoding"
    endpos = ctx.standby.safe_psql(
        "SELECT lsn FROM pg_logical_slot_peek_changes('behaves_ok_activeslot', "
        "NULL, NULL) ORDER BY lsn DESC LIMIT 1;",
        dbname="testdb",
    )
    ctx.primary.safe_psql(
        "INSERT INTO decoding_test(x,y) SELECT s, s::text "
        "FROM generate_series(5,50) s;",
        dbname="testdb",
    )
    ctx.primary.wait_for_replay_catchup(ctx.standby)
    opts = {"include-xids": "0", "skip-empty-xacts": "1"}
    stdout_recv = ctx.standby.pg_recvlogical_upto(
        "testdb", "behaves_ok_activeslot", endpos, ctx.timeout, options=opts
    )
    assert (
        stdout_recv.rstrip("\n") == _EXPECTED_BASIC
    ), "got same expected output from pg_recvlogical decoding session"
    assert ctx.standby.poll_query_until(
        "SELECT EXISTS (SELECT 1 FROM pg_replication_slots "
        "WHERE slot_name = 'behaves_ok_activeslot' AND active_pid IS NULL)",
        dbname="testdb",
    ), "slot never became inactive"
    stdout_recv = ctx.standby.pg_recvlogical_upto(
        "testdb", "behaves_ok_activeslot", endpos, ctx.timeout, options=opts
    )
    assert stdout_recv.rstrip("\n") == "", "pg_recvlogical acknowledged changes"
    ctx.primary.safe_psql("CREATE DATABASE otherdb")
    ctx.primary.wait_for_replay_catchup(ctx.standby)
    res = ctx.standby.psql_capture(
        "SELECT lsn FROM pg_logical_slot_peek_changes('behaves_ok_activeslot', "
        "NULL, NULL) ORDER BY lsn DESC LIMIT 1;",
        dbname="otherdb",
    )
    assert re.search(
        r'replication slot "behaves_ok_activeslot" was not created in this database',
        res.stderr,
    ), "replaying logical slot from another database fails"


def _test_subscribe_on_standby(ctx):
    """Subscribe on the standby to a primary publication and verify replication."""
    ctx.primary.safe_psql("CREATE TABLE tab_rep (a int primary key)")
    ctx.subscriber.safe_psql("CREATE TABLE tab_rep (a int primary key)")
    ctx.primary.safe_psql("CREATE PUBLICATION tap_pub for table tab_rep")
    ctx.primary.wait_for_replay_catchup(ctx.standby)
    standby_connstr = ctx.standby.connstr() + " dbname=postgres"
    # Use a background psql so we can run pg_log_standby_snapshot() on the
    # primary while CREATE SUBSCRIPTION is still waiting.
    sub_psql = ctx.subscriber.background_psql()
    sub_psql.send(
        "CREATE SUBSCRIPTION tap_sub\n"
        "     CONNECTION '{}'\n"
        "     PUBLICATION tap_pub\n"
        "     WITH (copy_data = off);\n".format(standby_connstr)
    )
    ctx.primary.log_standby_snapshot(ctx.standby, "tap_sub")
    sub_psql.quit()
    ctx.subscriber.wait_for_subscription_sync(ctx.standby, "tap_sub")
    ctx.primary.safe_psql("INSERT INTO tab_rep select generate_series(1,10);")
    ctx.primary.wait_for_replay_catchup(ctx.standby)
    ctx.standby.wait_for_catchup("tap_sub")
    assert (
        ctx.subscriber.safe_psql("SELECT count(*) FROM tab_rep") == "10"
    ), "check replicated inserts after subscription on standby"
    ctx.subscriber.safe_psql("DROP SUBSCRIPTION tap_sub")
    ctx.subscriber.stop()


def _test_vacuum_full_scenario(ctx):
    """Scenario 1: hot_standby_feedback off and VACUUM FULL on pg_class."""
    _reactive_slots_change_hfs_and_wait_for_xmins(
        ctx, "behaves_ok_", "vacuum_full_", 0, 1
    )
    ctx.primary.safe_psql(
        "INSERT INTO decoding_test(x,y) SELECT 100,'100';", dbname="testdb"
    )
    assert ctx.standby.poll_query_until(
        "SELECT total_txns > 0 FROM pg_stat_replication_slots "
        "WHERE slot_name = 'vacuum_full_activeslot'",
        dbname="testdb",
    ), "replication slot stats of vacuum_full_activeslot not updated"
    _wait_until_vacuum_can_remove(
        ctx,
        "full",
        _SQL_DROP_CONFLICT_TABLE,
        "pg_class",
    )
    _check_for_invalidation(ctx, "vacuum_full_", 1, "with vacuum FULL on pg_class")
    _check_slots_conflict_reason(ctx, "vacuum_full_", "rows_removed")
    res = ctx.standby.psql_capture(
        "ALTER_REPLICATION_SLOT vacuum_full_inactiveslot (failover);",
        replication="database",
    )
    assert re.search(
        r'ERROR:  can no longer access replication slot "vacuum_full_inactiveslot"',
        res.stderr,
    ) and re.search(
        r'DETAIL:  This replication slot has been invalidated due to "rows_removed".',
        res.stderr,
    ), "invalidated slot cannot be altered"
    assert (
        ctx.standby.safe_psql(
            "SELECT total_txns > 0 FROM pg_stat_replication_slots "
            "WHERE slot_name = 'vacuum_full_activeslot'",
            dbname="testdb",
        )
        == "t"
    ), "replication slot stats not removed after invalidation"
    ctx.handle = _make_slot_active(ctx, ctx.standby, "vacuum_full_", False)
    _check_pg_recvlogical_stderr(
        ctx.handle,
        'can no longer access replication slot "vacuum_full_activeslot"',
    )
    res = ctx.standby.psql_capture(
        "select pg_copy_logical_replication_slot('vacuum_full_inactiveslot', "
        "'vacuum_full_inactiveslot_copy');",
        replication="database",
    )
    assert re.search(
        r"ERROR:  cannot copy invalidated replication slot "
        r'"vacuum_full_inactiveslot"',
        res.stderr,
    ), "invalidated slot cannot be copied"
    _change_hsf_and_wait_for_xmins(ctx, 1, 1)


def _test_invalidation_survives_restart_and_no_wal(ctx):
    """Invalidated slots stay invalidated across restart and free WAL."""
    ctx.standby.restart()
    _check_slots_conflict_reason(ctx, "vacuum_full_", "rows_removed")
    restart_lsn = ctx.standby.safe_psql(
        "SELECT restart_lsn FROM pg_replication_slots\n"
        "    WHERE slot_name = 'vacuum_full_activeslot' AND conflicting;"
    )
    walfile_name = ctx.primary.safe_psql(
        "SELECT pg_walfile_name('{}')".format(restart_lsn)
    )
    ctx.primary.advance_wal(1)
    ctx.primary.safe_psql("checkpoint;")
    ctx.primary.wait_for_replay_catchup(ctx.standby)
    ctx.standby.safe_psql("checkpoint;")
    standby_walfile = ctx.standby.datadir / "pg_wal" / walfile_name
    assert (
        not standby_walfile.is_file()
    ), "invalidated logical slots do not lead to retaining WAL"


def _test_row_removal_scenario(ctx):
    """Scenario 2: conflict due to row removal (VACUUM on pg_class)."""
    logstart = ctx.standby.current_log_position()
    _reactive_slots_change_hfs_and_wait_for_xmins(
        ctx, "vacuum_full_", "row_removal_", 0, 1
    )
    _wait_until_vacuum_can_remove(
        ctx,
        "",
        _SQL_DROP_CONFLICT_TABLE,
        "pg_class",
    )
    _check_for_invalidation(ctx, "row_removal_", logstart, "with vacuum on pg_class")
    _check_slots_conflict_reason(ctx, "row_removal_", "rows_removed")
    ctx.handle = _make_slot_active(ctx, ctx.standby, "row_removal_", False)
    _check_pg_recvlogical_stderr(
        ctx.handle,
        'can no longer access replication slot "row_removal_activeslot"',
    )


def _test_shared_row_removal_scenario(ctx):
    """Scenario 3: conflict due to row removal on a shared catalog (pg_authid)."""
    logstart = ctx.standby.current_log_position()
    _reactive_slots_change_hfs_and_wait_for_xmins(
        ctx, "row_removal_", "shared_row_removal_", 0, 1
    )
    _wait_until_vacuum_can_remove(
        ctx,
        "",
        "CREATE ROLE create_trash;\nDROP ROLE create_trash;",
        "pg_authid",
    )
    _check_for_invalidation(
        ctx, "shared_row_removal_", logstart, "with vacuum on pg_authid"
    )
    _check_slots_conflict_reason(ctx, "shared_row_removal_", "rows_removed")
    ctx.handle = _make_slot_active(ctx, ctx.standby, "shared_row_removal_", False)
    _check_pg_recvlogical_stderr(
        ctx.handle,
        'can no longer access replication slot "shared_row_removal_activeslot"',
    )


def _test_no_conflict_scenario(ctx):
    """Scenario 4: VACUUM on a non-catalog table; no conflict expected."""
    logstart = ctx.standby.current_log_position()
    _reactive_slots_change_hfs_and_wait_for_xmins(
        ctx, "shared_row_removal_", "no_conflict_", 0, 1
    )
    _wait_until_vacuum_can_remove(
        ctx,
        "",
        "CREATE TABLE conflict_test(x integer, y text);\n"
        "INSERT INTO conflict_test(x,y) SELECT s, s::text "
        "FROM generate_series(1,4) s;\n"
        "UPDATE conflict_test set x=1, y=1;",
        "conflict_test",
    )
    assert not ctx.standby.log_matches(
        'invalidating obsolete replication slot "no_conflict_inactiveslot"',
        logstart,
    ), "inactiveslot slot invalidation is not logged with vacuum on conflict_test"
    assert not ctx.standby.log_matches(
        'invalidating obsolete replication slot "no_conflict_activeslot"',
        logstart,
    ), "activeslot slot invalidation is not logged with vacuum on conflict_test"
    assert ctx.standby.poll_query_until(
        "select (confl_active_logicalslot = 0) from pg_stat_database_conflicts "
        "where datname = 'testdb'"
    ), "Timed out waiting confl_active_logicalslot to be updated"
    assert (
        ctx.standby.safe_psql(
            "select bool_or(conflicting) from\n"
            "  (select conflicting from pg_replication_slots\n"
            "    where slot_type = 'logical')"
        )
        == "f"
    ), "Logical slots are reported as non conflicting"
    _change_hsf_and_wait_for_xmins(ctx, 1, 0)
    ctx.standby.restart()


def _test_pruning_scenario(ctx):
    """Scenario 5: conflict due to on-access pruning of a user_catalog_table."""
    logstart = ctx.standby.current_log_position()
    _reactive_slots_change_hfs_and_wait_for_xmins(ctx, "no_conflict_", "pruning_", 0, 0)
    ctx.primary.safe_psql(
        "SELECT injection_points_attach('skip-log-running-xacts', 'error');",
        dbname="testdb",
    )
    ctx.primary.safe_psql(
        "CREATE TABLE prun(id integer, s char(2000)) "
        "WITH (fillfactor = 75, user_catalog_table = true);",
        dbname="testdb",
    )
    ctx.primary.safe_psql("INSERT INTO prun VALUES (1, 'A');", dbname="testdb")
    for letter in ("B", "C", "D", "E"):
        ctx.primary.safe_psql(
            "UPDATE prun SET s = '{}';".format(letter), dbname="testdb"
        )
    ctx.primary.wait_for_replay_catchup(ctx.standby)
    ctx.primary.safe_psql(
        "SELECT injection_points_detach('skip-log-running-xacts');",
        dbname="testdb",
    )
    _check_for_invalidation(ctx, "pruning_", logstart, "with on-access pruning")
    _check_slots_conflict_reason(ctx, "pruning_", "rows_removed")
    ctx.handle = _make_slot_active(ctx, ctx.standby, "pruning_", False)
    _check_pg_recvlogical_stderr(
        ctx.handle,
        'can no longer access replication slot "pruning_activeslot"',
    )
    _change_hsf_and_wait_for_xmins(ctx, 1, 1)


def _test_wal_level_scenario(ctx):
    """Scenario 6: lowering primary wal_level invalidates the slots."""
    logstart = ctx.standby.current_log_position()
    _drop_logical_slots(ctx, "pruning_")
    _create_logical_slots(ctx, ctx.standby, "wal_level_")
    ctx.handle = _make_slot_active(ctx, ctx.standby, "wal_level_", True)
    ctx.standby.psql_capture("select pg_stat_reset();", dbname="testdb")
    ctx.primary.append_conf("\nwal_level = 'replica'\n")
    ctx.primary.restart()
    ctx.primary.wait_for_replay_catchup(ctx.standby)
    _check_for_invalidation(ctx, "wal_level_", logstart, "due to wal_level")
    _check_slots_conflict_reason(ctx, "wal_level_", "wal_level_insufficient")
    ctx.handle = _make_slot_active(ctx, ctx.standby, "wal_level_", False)
    _check_pg_recvlogical_stderr(
        ctx.handle,
        'logical decoding on standby requires "effective_wal_level" >= '
        '"logical" on the primary',
    )
    ctx.primary.append_conf("\nwal_level = 'logical'\n")
    ctx.primary.restart()
    ctx.primary.wait_for_replay_catchup(ctx.standby)
    ctx.handle = _make_slot_active(ctx, ctx.standby, "wal_level_", False)
    _check_pg_recvlogical_stderr(
        ctx.handle,
        'can no longer access replication slot "wal_level_activeslot"',
    )


def _test_drop_database_scenario(ctx):
    """DROP DATABASE drops its standby slots, including active ones."""
    _drop_logical_slots(ctx, "wal_level_")
    _create_logical_slots(ctx, ctx.standby, "drop_db_")
    ctx.handle = _make_slot_active(ctx, ctx.standby, "drop_db_", True)
    ctx.standby.create_logical_slot_on_standby(ctx.primary, "otherslot", "postgres")
    ctx.primary.safe_psql("DROP DATABASE testdb")
    ctx.primary.wait_for_replay_catchup(ctx.standby)
    assert (
        ctx.standby.safe_psql(
            "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = 'testdb')"
        )
        == "f"
    ), "database dropped on standby"
    _check_slots_dropped(ctx, "drop_db", ctx.handle)
    assert (
        ctx.standby.slot("otherslot")["slot_type"] == "logical"
    ), "otherslot on standby not dropped"
    ctx.standby.psql_capture("SELECT pg_drop_replication_slot('otherslot')")


def _setup_promotion(ctx, create_pg, backup_name):
    """Recreate testdb, build a cascading standby, and activate promotion slots.

    Returns the cascading-standby recvlogical handle.
    """
    ctx.standby.reload()
    ctx.primary.psql_capture("CREATE DATABASE testdb")
    ctx.primary.safe_psql(
        "CREATE TABLE decoding_test(x integer, y text);", dbname="testdb"
    )
    ctx.primary.wait_for_replay_catchup(ctx.standby)
    ctx.standby.safe_psql(
        "SELECT * FROM pg_create_physical_replication_slot('{}');".format(
            _STANDBY_PHYSICAL_SLOTNAME
        ),
        dbname="testdb",
    )
    ctx.standby.backup(backup_name)
    cascading = create_pg(
        "cascading_standby",
        from_backup=(ctx.standby, backup_name),
        has_streaming=True,
        has_restoring=True,
        start=False,
    )
    ctx.cascading_standby = cascading
    cascading.append_conf(
        "primary_slot_name = '{}'\nhot_standby_feedback = on".format(
            _STANDBY_PHYSICAL_SLOTNAME
        )
    )
    cascading.start()
    _create_logical_slots(ctx, ctx.standby, "promotion_")
    ctx.standby.wait_for_replay_catchup(cascading, ctx.primary)
    _create_logical_slots(ctx, cascading, "promotion_")
    ctx.handle = _make_slot_active(ctx, ctx.standby, "promotion_", True)
    cascading_handle = _make_slot_active(ctx, cascading, "promotion_", True)
    return cascading_handle


def _test_promotion_scenario(ctx, create_pg, backup_name):
    """Promote the standby and verify decoding of pre/post-promotion rows."""
    cascading_handle = _setup_promotion(ctx, create_pg, backup_name)
    ctx.primary.safe_psql(
        "INSERT INTO decoding_test(x,y) SELECT s, s::text "
        "FROM generate_series(1,4) s;",
        dbname="testdb",
    )
    ctx.primary.wait_for_replay_catchup(ctx.standby)
    ctx.standby.wait_for_replay_catchup(ctx.cascading_standby, ctx.primary)
    ctx.standby.promote()
    ctx.standby.safe_psql(
        "INSERT INTO decoding_test(x,y) SELECT s, s::text "
        "FROM generate_series(5,7) s;",
        dbname="testdb",
    )
    ctx.standby.wait_for_replay_catchup(ctx.cascading_standby)
    stdout_sql = ctx.standby.safe_psql(
        "SELECT data FROM pg_logical_slot_peek_changes('promotion_inactiveslot', "
        "NULL, NULL, 'include-xids', '0', 'skip-empty-xacts', '1');",
        dbname="testdb",
    )
    assert (
        stdout_sql == _EXPECTED_PROMOTION
    ), "got expected output from SQL decoding session on promoted standby"
    ctx.handle.pump_until(r"^.*COMMIT.*COMMIT$")
    assert (
        ctx.handle.stdout.rstrip("\n") == _EXPECTED_PROMOTION
    ), "got same expected output from pg_recvlogical decoding session"
    stdout_sql = ctx.cascading_standby.safe_psql(
        "SELECT data FROM pg_logical_slot_peek_changes('promotion_inactiveslot', "
        "NULL, NULL, 'include-xids', '0', 'skip-empty-xacts', '1');",
        dbname="testdb",
    )
    assert (
        stdout_sql == _EXPECTED_PROMOTION
    ), "got expected output from SQL decoding session on cascading standby"
    cascading_handle.pump_until(r"^.*COMMIT.*COMMIT$")
    assert (
        cascading_handle.stdout.rstrip("\n") == _EXPECTED_PROMOTION
    ), "got same expected output from pg_recvlogical on cascading standby"


def test_035_standby_logical_decoding(create_pg, bindir):
    """Logical decoding on a standby and recovery-conflict invalidation."""
    if os.environ.get("enable_injection_points") != "yes":
        pytest.skip("Injection points not supported by this build")
    ctx = _Nodes(bindir, pypg.test_timeout_default())
    try:
        backup_name = _init_primary(ctx, create_pg)
        _init_standby(ctx, create_pg, backup_name)
        ctx.subscriber = create_pg("subscriber")
        _test_hot_standby_off_refusal(ctx)
        _create_logical_slots(ctx, ctx.standby, "behaves_ok_")
        _test_basic_decoding(ctx)
        _test_subscribe_on_standby(ctx)
        ctx.primary.safe_psql("CREATE EXTENSION injection_points;", dbname="testdb")
        _test_vacuum_full_scenario(ctx)
        _test_invalidation_survives_restart_and_no_wal(ctx)
        _test_row_removal_scenario(ctx)
        _test_shared_row_removal_scenario(ctx)
        _test_no_conflict_scenario(ctx)
        _test_pruning_scenario(ctx)
        _test_wal_level_scenario(ctx)
        _test_drop_database_scenario(ctx)
        _test_promotion_scenario(ctx, create_pg, backup_name)
    finally:
        # The still-streaming promotion pg_recvlogical clients never exit on
        # their own; reap every background handle so nothing is left running.
        for handle in ctx.handles:
            handle.terminate()

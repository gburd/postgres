# Copyright (c) 2020-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long
"""Port of src/test/recovery/t/019_replslot_limit.pl.

Exercises max_slot_wal_keep_size and the replication-slot wal_status lifecycle:
reserved -> extended -> unreserved -> lost as WAL accumulates beyond the limit,
the interaction with wal_keep_size, slot invalidation (logged on both primary and
standby), checkpoint non-blocking, walsender termination to release a slot under
WAL pressure (SIGSTOP/SIGCONT on the sender/receiver), and inactive_since
tracking for physical and logical slots.
"""

import os
import signal
import time

import pypg


def _slot_status(node, slot, cols="wal_status"):
    return node.safe_psql(
        "SELECT {} FROM pg_replication_slots WHERE slot_name = '{}'".format(cols, slot)
    )


def test_019_replslot_limit(create_pg):
    """Slot wal_status transitions and invalidation behave per the WAL limits."""
    _scenario_status_lifecycle(create_pg)
    _scenario_checkpoint_not_blocked(create_pg)
    if os.name == "nt":
        return
    _scenario_walsender_termination(create_pg)
    _scenario_inactive_since(create_pg)


def _scenario_status_lifecycle(create_pg):
    primary = create_pg(
        "primary", allows_streaming=True, extra=["--wal-segsize=1"], start=False
    )
    primary.append_conf(
        "\nmin_wal_size = 2MB\nmax_wal_size = 4MB\nlog_checkpoints = yes\n"
    )
    primary.start()
    primary.safe_psql("SELECT pg_create_physical_replication_slot('rep1')")
    assert (
        _slot_status(
            primary,
            "rep1",
            "restart_lsn IS NULL, wal_status is NULL, safe_wal_size is NULL",
        )
        == "t|t|t"
    ), 'check the state of non-reserved slot is "unknown"'
    primary.backup("my_backup")
    standby = create_pg(
        "standby_1", from_backup=(primary, "my_backup"), has_streaming=True, start=False
    )
    standby.append_conf("primary_slot_name = 'rep1'")
    standby.start()
    primary.wait_for_slot_catchup("rep1", "restart", primary.lsn("write"))
    standby.stop()
    assert (
        _slot_status(primary, "rep1", "wal_status, safe_wal_size IS NULL")
        == "reserved|t"
    )
    for n in (1, 4):
        primary.advance_wal(n)
        primary.safe_psql("CHECKPOINT;")
        assert (
            _slot_status(primary, "rep1", "wal_status, safe_wal_size IS NULL")
            == "reserved|t"
        )
    standby.start()
    primary.wait_for_slot_catchup("rep1", "restart", primary.lsn("write"))
    standby.stop()
    primary.append_conf("\nmax_slot_wal_keep_size = 6MB\n")
    primary.reload()
    assert _slot_status(primary, "rep1") == "reserved", "max_slot_wal_keep_size working"
    primary.advance_wal(2)
    primary.safe_psql("CHECKPOINT;")
    assert _slot_status(primary, "rep1") == "reserved", "slot remains reserved"
    _scenario_extended_unreserved_lost(primary, standby)


def _scenario_extended_unreserved_lost(primary, standby):
    standby.start()
    primary.wait_for_slot_catchup("rep1", "restart", primary.lsn("write"))
    standby.stop()
    primary.safe_psql(
        "ALTER SYSTEM SET wal_keep_size to '8MB'; SELECT pg_reload_conf();"
    )
    primary.advance_wal(6)
    assert _slot_status(primary, "rep1") == "extended", "wal_keep_size overrides limit"
    primary.safe_psql("ALTER SYSTEM SET wal_keep_size to 0; SELECT pg_reload_conf();")
    standby.start()
    primary.wait_for_slot_catchup("rep1", "restart", primary.lsn("write"))
    standby.stop()
    primary.advance_wal(6)
    assert _slot_status(primary, "rep1") == "extended", 'state changes to "extended"'
    primary.safe_psql("CHECKPOINT;")
    primary.advance_wal(1)
    assert (
        _slot_status(primary, "rep1", "wal_status, safe_wal_size <= 0")
        == "unreserved|t"
    ), 'state "unreserved"'
    standby.start()
    primary.wait_for_slot_catchup("rep1", "restart", primary.lsn("write"))
    standby.stop()
    assert not standby.log_matches(
        "requested WAL segment [0-9A-F]+ has already been removed"
    ), "required WAL segments still available"
    _scenario_invalidation(primary, standby)


def _scenario_invalidation(primary, standby):
    primary.safe_psql("CHECKPOINT;")
    primary.safe_psql("ALTER SYSTEM SET max_wal_size='40MB'; SELECT pg_reload_conf()")
    logstart = primary.current_log_position()
    primary.advance_wal(7)
    primary.safe_psql("ALTER SYSTEM RESET max_wal_size; SELECT pg_reload_conf()")
    primary.safe_psql("CHECKPOINT;")
    assert _wait_log(
        primary, 'invalidating obsolete replication slot "rep1"', logstart
    ), "slot invalidation logged"
    assert (
        primary.safe_psql(
            "SELECT slot_name, active, restart_lsn IS NULL, wal_status, safe_wal_size\n"
            "FROM pg_replication_slots WHERE slot_name = 'rep1'"
        )
        == "rep1|f|t|lost|"
    ), 'slot inactive and "lost" persists'
    assert _wait_log(primary, "checkpoint complete: ", logstart), "checkpoint ended"
    redoseg = primary.safe_psql(
        "SELECT pg_walfile_name(lsn) FROM pg_create_physical_replication_slot('s2', true)"
    )
    oldestseg = primary.safe_psql(
        "SELECT pg_ls_dir AS f FROM pg_ls_dir('pg_wal') WHERE pg_ls_dir ~ "
        "'^[0-9A-F]{24}$' ORDER BY 1 LIMIT 1"
    )
    primary.safe_psql("SELECT pg_drop_replication_slot('s2')")
    assert oldestseg == redoseg, "segments have been removed"
    sb_logstart = standby.current_log_position()
    standby.start()
    assert _wait_log(
        standby,
        'This replication slot has been invalidated due to "wal_removed".',
        sb_logstart,
    ), "replication has been broken"
    primary.stop()
    standby.stop()


def _scenario_checkpoint_not_blocked(create_pg):
    primary = create_pg("primary2", allows_streaming=True, start=False)
    primary.append_conf(
        "\nmin_wal_size = 32MB\nmax_wal_size = 32MB\nlog_checkpoints = yes\n"
    )
    primary.start()
    primary.safe_psql("SELECT pg_create_physical_replication_slot('rep1')")
    primary.backup("my_backup2")
    primary.stop()
    primary.append_conf("\nmax_slot_wal_keep_size = 0\n")
    primary.start()
    standby = create_pg(
        "standby_2",
        from_backup=(primary, "my_backup2"),
        has_streaming=True,
        start=False,
    )
    standby.append_conf("primary_slot_name = 'rep1'")
    standby.start()
    primary.advance_wal(1)
    assert (
        primary.safe_psql(
            "CHECKPOINT; SELECT 'finished';", timeout=pypg.test_timeout_default()
        )
        == "finished"
    ), "checkpoint command is not blocked"
    primary.stop()
    standby.stop()


def _scenario_walsender_termination(create_pg):
    primary = create_pg(
        "primary3", allows_streaming=True, extra=["--wal-segsize=1"], start=False
    )
    primary.append_conf(
        "\nmin_wal_size = 2MB\nmax_wal_size = 2MB\nlog_checkpoints = yes\n"
        "max_slot_wal_keep_size = 1MB\n"
    )
    primary.start()
    primary.safe_psql("SELECT pg_create_physical_replication_slot('rep3')")
    primary.backup("my_backup")
    standby = create_pg(
        "standby_3", from_backup=(primary, "my_backup"), has_streaming=True, start=False
    )
    standby.append_conf("primary_slot_name = 'rep3'")
    standby.start()
    primary.wait_for_catchup(standby)
    senderpid = _wait_single_pid(primary, "walsender")
    receiverpid = int(
        standby.safe_psql(
            "SELECT pid FROM pg_stat_activity WHERE backend_type = 'walreceiver'"
        )
    )
    logstart = primary.current_log_position()
    os.kill(senderpid, signal.SIGSTOP)
    os.kill(receiverpid, signal.SIGSTOP)
    primary.advance_wal(2)
    assert _wait_log(
        primary,
        'terminating process {} to release replication slot "rep3"'.format(senderpid),
        logstart,
        secs=True,
    ), "walsender termination logged"
    os.kill(senderpid, signal.SIGCONT)
    assert primary.poll_query_until(
        "SELECT wal_status FROM pg_replication_slots WHERE slot_name = 'rep3'", "lost"
    ), "timed out waiting for slot to be lost"
    assert _wait_log(
        primary, 'invalidating obsolete replication slot "rep3"', logstart, secs=True
    ), "slot invalidation logged"
    os.kill(receiverpid, signal.SIGCONT)
    primary.stop()
    standby.stop()


def _scenario_inactive_since(create_pg):
    primary = create_pg("primary4", allows_streaming="logical", start=False)
    primary.start()
    primary.backup("my_backup4")
    standby = create_pg(
        "standby4", from_backup=(primary, "my_backup4"), has_streaming=True, start=False
    )
    sb_slot = "sb4_slot"
    standby.append_conf("primary_slot_name = '{}'".format(sb_slot))
    creation_time = primary.safe_psql("SELECT current_timestamp;")
    primary.safe_psql(
        "SELECT pg_create_physical_replication_slot(slot_name := '{}');".format(sb_slot)
    )
    inactive_since = primary.validate_slot_inactive_since(sb_slot, creation_time)
    standby.start()
    primary.wait_for_catchup(standby)
    assert (
        _slot_status(primary, sb_slot, "inactive_since IS NULL") == "t"
    ), "active physical slot inactive_since is NULL"
    standby.stop()
    primary.restart()
    assert (
        primary.safe_psql(
            "SELECT inactive_since > '{}'::timestamptz FROM pg_replication_slots "
            "WHERE slot_name = '{}' AND inactive_since IS NOT NULL;".format(
                inactive_since, sb_slot
            )
        )
        == "t"
    ), "inactive physical slot inactive_since updated"
    _scenario_inactive_since_logical(create_pg, primary)


def _scenario_inactive_since_logical(create_pg, publisher):
    subscriber = create_pg("subscriber4", start=False)
    subscriber.start()
    connstr = publisher.connstr() + " dbname=postgres"
    publisher.safe_psql("CREATE PUBLICATION pub FOR ALL TABLES")
    creation_time = publisher.safe_psql("SELECT current_timestamp;")
    lsub_slot = "lsub4_slot"
    publisher.safe_psql(
        "SELECT pg_create_logical_replication_slot(slot_name := '{}', "
        "plugin := 'pgoutput');".format(lsub_slot)
    )
    inactive_since = publisher.validate_slot_inactive_since(lsub_slot, creation_time)
    subscriber.safe_psql(
        "CREATE SUBSCRIPTION sub CONNECTION '{}' PUBLICATION pub WITH "
        "(slot_name = '{}', create_slot = false)".format(connstr, lsub_slot)
    )
    subscriber.wait_for_subscription_sync(publisher, "sub")
    assert (
        _slot_status(publisher, lsub_slot, "inactive_since IS NULL") == "t"
    ), "active logical slot inactive_since is NULL"
    subscriber.stop()
    publisher.restart()
    assert (
        publisher.safe_psql(
            "SELECT inactive_since > '{}'::timestamptz FROM pg_replication_slots "
            "WHERE slot_name = '{}' AND inactive_since IS NOT NULL;".format(
                inactive_since, lsub_slot
            )
        )
        == "t"
    ), "inactive logical slot inactive_since updated"
    publisher.stop()
    subscriber.stop()


def _wait_log(node, pattern, offset, secs=False):
    attempts = pypg.test_timeout_default() if secs else 10 * pypg.test_timeout_default()
    for _ in range(attempts + 1):
        if node.log_matches(pattern, offset):
            return True
        time.sleep(1 if secs else 0.1)
    return False


def _wait_single_pid(node, backend_type):
    for _ in range(10 * pypg.test_timeout_default() + 1):
        pid = node.safe_psql(
            "SELECT pid FROM pg_stat_activity WHERE backend_type = '{}'".format(
                backend_type
            )
        )
        if pid.isdigit():
            return int(pid)
        time.sleep(0.1)
    raise RuntimeError("could not determine single {} pid".format(backend_type))

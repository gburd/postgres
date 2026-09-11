# Copyright (c) 2024-2026, PostgreSQL Global Development Group

"""Port of src/test/recovery/t/046_checkpoint_logical_slot.pl.

A logical slot must remain valid when a checkpoint that advances WAL removal
races with a logical-decoding segment advance (injection points coordinate the
race), surviving an immediate crash and restart. Then a synced failover slot on
a standby must not be wrongly invalidated when a restartpoint races with slot
synchronization. Requires injection points.
"""

import os

import pytest


def test_046_checkpoint_logical_slot(create_pg):
    """Logical and synced-failover slots stay valid across checkpoint races."""
    if os.environ.get("enable_injection_points") != "yes":
        pytest.skip("Injection points not supported by this build")
    node = create_pg("mike", allows_streaming="logical", start=False)
    node.start()
    if not node.check_extension("injection_points"):
        pytest.skip("Extension injection_points not installed")
    node.safe_psql("CREATE EXTENSION injection_points")
    node.safe_psql(
        "select pg_create_logical_replication_slot('slot_logical', 'test_decoding')"
    )
    node.safe_psql("select pg_create_physical_replication_slot('slot_physical', true)")
    node.safe_psql(
        "select count(*) from pg_logical_slot_get_changes('slot_logical', null, null)"
    )
    node.safe_psql(
        "select pg_replication_slot_advance('slot_physical', pg_current_wal_lsn())"
    )
    node.safe_psql("checkpoint")
    xacts = node.background_psql("postgres")
    xacts.query_until(r"run_xacts", "\\echo run_xacts\nSELECT 1 \\watch 0.1\n\\q\n")
    node.advance_wal(20)
    node.safe_psql("checkpoint")
    node.advance_wal(20)
    checkpoint = node.background_psql("postgres")
    checkpoint.query_safe(
        "select injection_points_attach('checkpoint-before-old-wal-removal','wait')"
    )
    checkpoint.query_until(
        r"starting_checkpoint", "\\echo starting_checkpoint\ncheckpoint;\n\\q\n"
    )
    node.wait_for_event("checkpointer", "checkpoint-before-old-wal-removal")
    logical = node.background_psql("postgres")
    logical.query_safe(
        "select injection_points_attach("
        "'logical-replication-slot-advance-segment','wait');"
    )
    logical.query_until(
        r"get_changes",
        "\n\\echo get_changes\n"
        "select count(*) from pg_logical_slot_get_changes('slot_logical', null, "
        "null) \\watch 1\n\\q\n",
    )
    node.wait_for_event("client backend", "logical-replication-slot-advance-segment")
    node.safe_psql(
        "select pg_replication_slot_advance('slot_physical', pg_current_wal_lsn())"
    )
    node.safe_psql(
        "select pg_logical_emit_message(false, '', repeat('123456789', 1000))"
    )
    log_offset = node.current_log_position()
    node.safe_psql(
        "select injection_points_wakeup('checkpoint-before-old-wal-removal')"
    )
    node.wait_for_log(r"checkpoint complete", log_offset)
    node.stop("immediate")
    node.start()
    node.safe_psql(
        "select count(*) from pg_logical_slot_get_changes('slot_logical', null, null);"
    )
    xacts.quit()
    checkpoint.quit()
    logical.quit()
    _failover_slot_phase(create_pg, node)


def _failover_slot_phase(create_pg, primary):
    """A synced failover slot on a standby is not invalidated by a restartpoint."""
    primary.append_conf("autovacuum = off")
    primary.reload()
    backup_name = "backup"
    primary.backup(backup_name)
    standby = create_pg(
        "standby",
        from_backup=(primary, backup_name),
        has_streaming=True,
        start=False,
    )
    connstr_1 = primary.connstr()
    standby.append_conf(
        "\nhot_standby_feedback = on\n"
        "primary_slot_name = 'phys_slot'\n"
        "primary_conninfo = '{} dbname=postgres'\n".format(connstr_1)
    )
    primary.safe_psql(
        "SELECT pg_create_logical_replication_slot("
        "'failover_slot', 'test_decoding', false, false, true);\n"
        "SELECT pg_create_physical_replication_slot('phys_slot');"
    )
    standby.start()
    primary.advance_wal(1)
    primary.safe_psql("CHECKPOINT")
    primary.wait_for_replay_catchup(standby)
    checkpoint = standby.background_psql("postgres")
    checkpoint.query_safe(
        "select injection_points_attach("
        "'restartpoint-before-slot-invalidation','wait')"
    )
    checkpoint.query_until(
        r"starting_checkpoint", "\\echo starting_checkpoint\ncheckpoint;\n"
    )
    standby.wait_for_event("checkpointer", "restartpoint-before-slot-invalidation")
    standby.append_conf("sync_replication_slots = on")
    standby.reload()
    standby.poll_query_until(
        "SELECT COUNT(*) > 0 FROM pg_replication_slots "
        "WHERE slot_name = 'failover_slot'"
    )
    standby.safe_psql(
        "select injection_points_wakeup('restartpoint-before-slot-invalidation');\n"
        "select injection_points_detach('restartpoint-before-slot-invalidation')"
    )
    checkpoint.quit()
    assert (
        standby.safe_psql(
            "SELECT invalidation_reason IS NULL AND synced FROM pg_replication_slots "
            "WHERE slot_name = 'failover_slot';"
        )
        == "t"
    ), "logical slot is not invalidated"

# Copyright (c) 2024-2026, PostgreSQL Global Development Group

"""Port of src/test/recovery/t/047_checkpoint_physical_slot.pl.

A physical replication slot's restart_lsn advanced during an in-progress
checkpoint (paused before old-WAL removal via an injection point) must still
reference a WAL segment that exists after an immediate crash and restart.
Requires injection points.
"""

import os

import pytest


def test_047_checkpoint_physical_slot(create_pg):
    """The slot's required WAL segment survives a checkpoint-time advance + crash."""
    if os.environ.get("enable_injection_points") != "yes":
        pytest.skip("Injection points not supported by this build")
    node = create_pg("mike", start=False)
    node.append_conf("wal_level = 'replica'")
    node.start()
    if not node.check_extension("injection_points"):
        pytest.skip("Extension injection_points not installed")
    node.safe_psql("CREATE EXTENSION injection_points")
    node.safe_psql("select pg_create_physical_replication_slot('slot_physical', true)")
    node.safe_psql(
        "select pg_replication_slot_advance('slot_physical', pg_current_wal_lsn())"
    )
    node.safe_psql("checkpoint")
    node.advance_wal(20)
    node.safe_psql(
        "select pg_replication_slot_advance('slot_physical', pg_current_wal_lsn())"
    )
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
    node.safe_psql(
        "select pg_replication_slot_advance('slot_physical', pg_current_wal_lsn())"
    )
    log_offset = node.current_log_position()
    node.safe_psql(
        "select injection_points_wakeup('checkpoint-before-old-wal-removal')"
    )
    node.wait_for_log(r"checkpoint complete", log_offset)
    node.stop("immediate")
    node.start()
    restart_lsn = node.safe_psql(
        "select restart_lsn from pg_replication_slots "
        "where slot_name = 'slot_physical'"
    )
    restart_lsn_segment = node.safe_psql(
        "SELECT pg_walfile_name('{}'::pg_lsn)".format(restart_lsn)
    )
    assert os.path.isfile(
        "{}/pg_wal/{}".format(node.datadir, restart_lsn_segment)
    ), "WAL segment {} for physical slot's restart_lsn {} exists".format(
        restart_lsn_segment, restart_lsn
    )

# Copyright (c) 2023-2026, PostgreSQL Global Development Group

"""Port of src/test/modules/test_misc/t/008_replslot_single_user.pl.

Replication-slot operations work in single-user mode (postgres --single):
creating logical/physical/temporary slots, logical decoding, advancing, copying,
and dropping slots all succeed when run as single-user SQL against a stopped
cluster's data directory.
"""

import os
import subprocess
import sys

import pytest


def _single_mode(node, queries, test_name):
    result = subprocess.run(
        [
            os.path.join(str(node.bin_dir), "postgres"),
            "--single",
            "-F",
            "-c",
            "exit_on_error=true",
            "-D",
            str(node.datadir),
            "postgres",
        ],
        input=queries,
        encoding="utf-8",
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=node.connenv,
        check=False,
    )
    assert result.returncode == 0, test_name


@pytest.mark.skipif(sys.platform == "win32", reason="not supported on Windows")
def test_008_replslot_single_user(create_pg):
    """Replication-slot operations succeed in single-user mode."""
    slot_logical = "slot_logical"
    slot_physical = "slot_physical"
    node = create_pg("node", allows_streaming="logical")
    node.safe_psql("CREATE TABLE foo (id int)")
    node.stop()
    _single_mode(
        node,
        "SELECT pg_create_logical_replication_slot('{}', 'test_decoding')".format(
            slot_logical
        ),
        "logical slot creation",
    )
    _single_mode(
        node,
        "SELECT pg_create_physical_replication_slot('{}', true)".format(slot_physical),
        "physical slot creation",
    )
    _single_mode(
        node,
        "SELECT pg_create_physical_replication_slot('slot_tmp', true, true)",
        "temporary physical slot creation",
    )
    _single_mode(
        node,
        "INSERT INTO foo VALUES (1);\n"
        "SELECT pg_logical_slot_get_changes('{}', NULL, NULL);\n".format(slot_logical),
        "logical decoding",
    )
    _single_mode(
        node,
        "SELECT pg_replication_slot_advance('{}', pg_current_wal_lsn())".format(
            slot_logical
        ),
        "logical slot advance",
    )
    _single_mode(
        node,
        "SELECT pg_replication_slot_advance('{}', pg_current_wal_lsn())".format(
            slot_physical
        ),
        "physical slot advance",
    )
    _single_mode(
        node,
        "SELECT pg_copy_logical_replication_slot('{}', 'slot_log_copy')".format(
            slot_logical
        ),
        "logical slot copy",
    )
    _single_mode(
        node,
        "SELECT pg_copy_physical_replication_slot('{}', 'slot_phy_copy')".format(
            slot_physical
        ),
        "physical slot copy",
    )
    _single_mode(
        node,
        "SELECT pg_drop_replication_slot('{}')".format(slot_logical),
        "logical slot drop",
    )
    _single_mode(
        node,
        "SELECT pg_drop_replication_slot('{}')".format(slot_physical),
        "physical slot drop",
    )

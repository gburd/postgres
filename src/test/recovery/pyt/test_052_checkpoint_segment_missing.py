# Copyright (c) 2026, PostgreSQL Global Development Group

"""Port of src/test/recovery/t/052_checkpoint_segment_missing.pl.

Verify crash recovery behavior when the WAL segment containing the checkpoint
record referenced by pg_controldata is missing and there is no backup_label
file: the startup process should fail with a FATAL about the missing
checkpoint record.
"""

import re

from pypg import slurp_file


def test_checkpoint_segment_missing(pg_bin, create_pg):
    """Recovery FATALs when the checkpoint WAL segment is gone (no backup_label)."""
    node = create_pg("testnode", start=False)
    node.append_conf("log_checkpoints = on")
    node.start()

    # Force a checkpoint so pg_controldata points to a record we can target.
    node.safe_psql("CHECKPOINT;")

    checkpoint_walfile = node.safe_psql(
        "SELECT pg_walfile_name(checkpoint_lsn) FROM pg_control_checkpoint()"
    )
    assert checkpoint_walfile != "", "derived checkpoint WAL file name"

    node.stop("immediate")

    walpath = node.datadir / "pg_wal" / checkpoint_walfile
    assert walpath.is_file(), "checkpoint WAL file exists before deletion"
    walpath.unlink()
    assert not walpath.exists(), "checkpoint WAL file removed"

    # Use pg_ctl directly (not node.start) because recovery is expected to fail.
    pg_bin.result(
        ["pg_ctl", "--pgdata", str(node.datadir), "--log", str(node.log), "start"]
    )

    assert re.search(
        r"FATAL: .* could not locate a valid checkpoint record at .*",
        slurp_file(node.log),
    ), "FATAL logged for missing checkpoint record (no backup_label path)"

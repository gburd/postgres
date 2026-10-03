# Copyright (c) 2024-2026, PostgreSQL Global Development Group

"""Port of src/test/recovery/t/050_redo_segment_missing.pl.

When the WAL segment holding a checkpoint's redo location is missing at startup,
the server must FATAL rather than start. Injection points pause checkpointing so
a WAL switch lands the redo and checkpoint records in different segments; the
redo segment is then deleted before a restart. Requires injection points.
"""

import os
import re

import pytest

import pypg


def test_050_redo_segment_missing(create_pg, pg_bin):
    """A missing redo WAL segment makes startup FATAL, not succeed."""
    if os.environ.get("enable_injection_points") != "yes":
        pytest.skip("Injection points not supported by this build")
    node = create_pg("testnode", start=False)
    node.append_conf("log_checkpoints = on")
    node.start()
    if not node.check_extension("injection_points"):
        pytest.skip("Extension injection_points not installed")
    node.safe_psql("CREATE EXTENSION injection_points")
    node.safe_psql(
        "select injection_points_attach('create-checkpoint-initial', 'wait')"
    )
    node.safe_psql("select injection_points_attach('create-checkpoint-run', 'wait')")
    checkpoint = node.background_psql("postgres")
    checkpoint.query_until(
        r"starting_checkpoint", "\\echo starting_checkpoint\ncheckpoint;\n"
    )
    node.wait_for_event("checkpointer", "create-checkpoint-initial")
    node.safe_psql("select injection_points_wakeup('create-checkpoint-initial')")
    node.wait_for_event("checkpointer", "create-checkpoint-run")
    node.safe_psql("SELECT pg_switch_wal()")
    log_offset = node.current_log_position()
    node.safe_psql("select injection_points_wakeup('create-checkpoint-run')")
    node.wait_for_log(r"checkpoint complete", log_offset)
    checkpoint.quit()
    redo_lsn = node.safe_psql("SELECT redo_lsn FROM pg_control_checkpoint()")
    redo_walfile_name = node.safe_psql("SELECT pg_walfile_name('{}')".format(redo_lsn))
    checkpoint_lsn = node.safe_psql(
        "SELECT checkpoint_lsn FROM pg_control_checkpoint()"
    )
    checkpoint_walfile_name = node.safe_psql(
        "SELECT pg_walfile_name('{}')".format(checkpoint_lsn)
    )
    assert (
        redo_walfile_name != checkpoint_walfile_name
    ), "redo and checkpoint records on different segments"
    os.unlink("{}/pg_wal/{}".format(node.datadir, redo_walfile_name))
    node.stop("immediate")
    pg_bin.run_command(
        ["pg_ctl", "--pgdata", str(node.datadir), "--log", str(node.log), "start"]
    )
    logfile = pypg.slurp_file(node.log)
    assert re.search(
        r"FATAL: .* could not find redo location .* referenced by checkpoint "
        r"record at .*",
        logfile,
    ), "ends with FATAL because it could not find redo location"

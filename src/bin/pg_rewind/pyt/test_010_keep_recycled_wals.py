# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/bin/pg_rewind/t/010_keep_recycled_wals.pl.

pg_rewind must not delete WAL segments that are still required for recovery on
the target. With archiving wedged (a failing archive_command) so segments are
retained, a rewind of the diverged old primary from the promoted standby reports
"Not removing file ... because it is required for recovery" for the kept files.
"""

import re


def test_010_keep_recycled_wals(rewind_test, pg_bin):
    """pg_rewind keeps WAL segments still required for recovery on the target."""
    rewind_test.setup_cluster()
    primary = rewind_test.primary
    primary.enable_archiving()
    rewind_test.start_primary()
    rewind_test.create_standby()
    standby = rewind_test.standby
    standby.enable_restoring(primary, standby=False)
    standby.reload()
    rewind_test.primary_psql("CHECKPOINT")  # last common checkpoint
    primary.append_conf("\narchive_command = 'false'\n")
    primary.reload()
    rewind_test.primary_psql("CREATE TABLE t(a int)")
    rewind_test.primary_psql("INSERT INTO t VALUES(0)")
    rewind_test.primary_psql("SELECT pg_switch_wal()")
    rewind_test.promote_standby()
    rewind_test.standby_psql("INSERT INTO t values(0)")
    rewind_test.standby_psql("SELECT pg_switch_wal()")
    standby.stop()
    primary.stop()
    result = pg_bin.run_command(
        [
            "pg_rewind",
            "--debug",
            "--source-pgdata",
            str(standby.datadir),
            "--target-pgdata",
            str(primary.datadir),
            "--no-sync",
        ]
    )
    assert re.search(
        r"Not removing file .* because it is required for recovery", result.stderr
    ), "some WAL files were skipped"

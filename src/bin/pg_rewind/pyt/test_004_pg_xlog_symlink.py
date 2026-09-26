# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/bin/pg_rewind/t/004_pg_xlog_symlink.pl.

pg_rewind works when the target's pg_wal is a symlink to an out-of-tree
directory: after divergence and rewind, the rewound primary's table content
matches the promoted standby. Exercised for 'local' and 'remote' source modes.
"""

import os
import shutil


def _run_test(rt, tmp_path, test_mode):
    xlogdir = str(tmp_path / ("xlog_primary_" + test_mode))
    if os.path.exists(xlogdir):
        shutil.rmtree(xlogdir)
    rt.setup_cluster(test_mode)
    pg_wal = os.path.join(str(rt.primary.datadir), "pg_wal")
    # Turn pg_wal into a symlink to an out-of-tree directory.
    shutil.move(pg_wal, xlogdir)
    os.symlink(xlogdir, pg_wal)
    rt.start_primary()
    rt.primary_psql("CREATE TABLE tbl1 (d text)")
    rt.primary_psql("INSERT INTO tbl1 VALUES ('in primary')")
    rt.primary_psql("CHECKPOINT")
    rt.create_standby(test_mode)
    rt.primary_psql("INSERT INTO tbl1 values ('in primary, before promotion')")
    rt.primary_psql("CHECKPOINT")
    rt.promote_standby()
    # Diverge the old primary and the promoted standby.
    rt.primary_psql("INSERT INTO tbl1 VALUES ('in primary, after promotion')")
    rt.standby_psql("INSERT INTO tbl1 VALUES ('in standby, after promotion')")
    rt.run_pg_rewind(test_mode)
    rt.check_query(
        "SELECT * FROM tbl1",
        "in primary\nin primary, before promotion\nin standby, after promotion",
        "table content",
    )
    rt.clean_rewind_test()


def test_004_pg_xlog_symlink(rewind_test, tmp_path):
    """pg_rewind with pg_wal as an out-of-tree symlink (local and remote)."""
    _run_test(rewind_test, tmp_path, "local")
    _run_test(rewind_test, tmp_path, "remote")

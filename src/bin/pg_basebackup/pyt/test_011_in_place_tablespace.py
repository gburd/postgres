# Copyright (c) 2020-2026, PostgreSQL Global Development Group

"""Port of src/bin/pg_basebackup/t/011_in_place_tablespace.pl.

A tar-format base backup of a cluster with an in-place tablespace produces both
the main base.tar and exactly one numbered tablespace tar.
"""

import glob
import os
import tempfile


def test_011_in_place_tablespace(create_pg):
    """tar-format backup emits base.tar plus one tablespace tar."""
    tempdir = tempfile.mkdtemp(prefix="ipts_")
    node = create_pg("main", allows_streaming=True)
    node.safe_psql(
        "SET allow_in_place_tablespaces = on;\n"
        "CREATE TABLESPACE inplace LOCATION '';"
    )
    backupdir = tempdir + "/backup"
    node.command_ok(
        [
            "pg_basebackup",
            "--no-sync",
            "--checkpoint",
            "fast",
            "--pgdata",
            backupdir,
            "--format",
            "tar",
            "--wal-method",
            "none",
        ],
        "pg_basebackup runs",
    )
    assert os.path.isfile("{}/base.tar".format(backupdir)), "backup tar was created"
    tblspc_tars = glob.glob("{}/[0-9]*.tar".format(backupdir))
    assert len(tblspc_tars) == 1, "one tablespace tar was created"

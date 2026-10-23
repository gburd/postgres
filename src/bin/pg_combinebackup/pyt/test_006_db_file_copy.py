# Copyright (c) 2021-2026, PostgreSQL Global Development Group

# pylint: disable=implicit-str-concat
"""Port of src/bin/pg_combinebackup/t/006_db_file_copy.pl.

When a database is dropped and recreated with the same OID (via FILE_COPY
strategy) between a full and an incremental backup, pg_combinebackup must
reconstruct the *recreated* (empty) database, not the original: the table that
existed in the pre-drop database is absent after combining and restoring.
"""

import os
import re


def test_006_db_file_copy(create_pg):
    """A reused-OID FILE_COPY database is reconstructed empty after combine."""
    mode = os.environ.get("PG_TEST_PG_COMBINEBACKUP_MODE") or "--copy"
    primary = create_pg(
        "primary", has_archiving=True, allows_streaming=True, start=False
    )
    primary.append_conf("summarize_wal = on")
    primary.start()
    primary.safe_psql("CREATE DATABASE lakh OID = 100000 STRATEGY = FILE_COPY")
    primary.safe_psql("CREATE TABLE t1 (a int)", dbname="lakh")
    backup1path = "{}/backup1".format(primary.backup_dir)
    primary.command_ok(
        ["pg_basebackup", "--pgdata", backup1path, "--no-sync", "--checkpoint", "fast"],
        "full backup",
    )
    primary.safe_psql(
        "DROP DATABASE lakh;\n" "CREATE DATABASE lakh OID = 100000 STRATEGY = FILE_COPY"
    )
    backup2path = "{}/backup2".format(primary.backup_dir)
    primary.command_ok(
        [
            "pg_basebackup",
            "--pgdata",
            backup2path,
            "--no-sync",
            "--checkpoint",
            "fast",
            "--incremental",
            backup1path + "/backup_manifest",
        ],
        "incremental backup",
    )
    restore = create_pg(
        "restore",
        from_backup=(primary, "backup2"),
        combine_with_prior=["backup1"],
        combine_mode=mode,
        start=False,
    )
    restore.start()
    res = restore.psql_capture("SELECT * FROM t1", dbname="lakh")
    assert res.stdout == "", "SELECT * FROM t1: no stdout"
    assert re.search(
        r'relation "t1" does not exist', res.stderr
    ), "SELECT * FROM t1: stderr missing table"

# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/bin/pg_combinebackup/t/003_timeline.pl.

A chain of full + incremental backups taken across a branch (a second node
restored from the combined chain and written to) combines correctly: a third
node restored from full+backup2+backup3 sees exactly the rows that belong on its
branch, and every backup in the chain verifies.
"""

import os


def test_003_timeline(create_pg):
    """pg_combinebackup correctly follows a full+incremental backup chain."""
    mode = os.environ.get("PG_TEST_PG_COMBINEBACKUP_MODE") or "--copy"
    node1 = create_pg("node1", has_archiving=True, allows_streaming=True, start=False)
    node1.append_conf("summarize_wal = on")
    node1.start()
    node1.safe_psql(
        "CREATE TABLE mytable (a int, b text);\n"
        "INSERT INTO mytable VALUES (1, 'aardvark');"
    )
    backup1path = "{}/backup1".format(node1.backup_dir)
    node1.command_ok(
        ["pg_basebackup", "--pgdata", backup1path, "--no-sync", "--checkpoint", "fast"],
        "full backup from node1",
    )
    node1.safe_psql("INSERT INTO mytable VALUES (2, 'beetle');")
    backup2path = "{}/backup2".format(node1.backup_dir)
    node1.command_ok(
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
        "incremental backup from node1",
    )
    node2 = create_pg(
        "node2",
        from_backup=(node1, "backup2"),
        combine_with_prior=["backup1"],
        start=False,
    )
    node2.start()
    node1.safe_psql("INSERT INTO mytable VALUES (3, 'crab');")
    node2.safe_psql("INSERT INTO mytable VALUES (4, 'dingo');")
    backup3path = "{}/backup3".format(node1.backup_dir)
    node2.command_ok(
        [
            "pg_basebackup",
            "--pgdata",
            backup3path,
            "--no-sync",
            "--checkpoint",
            "fast",
            "--incremental",
            backup2path + "/backup_manifest",
        ],
        "incremental backup from node2",
    )
    node3 = create_pg(
        "node3",
        from_backup=(node1, "backup3"),
        combine_with_prior=["backup1", "backup2"],
        combine_mode=mode,
        start=False,
    )
    node3.start()
    node3.safe_psql("INSERT INTO mytable VALUES (5, 'elephant');")
    result = node3.safe_psql(
        "select string_agg(a::text, ':'), string_agg(b, ':') from mytable;"
    )
    assert result == "1:2:4:5|aardvark:beetle:dingo:elephant"
    for backup_name in ("backup1", "backup2", "backup3"):
        node1.command_ok(
            ["pg_verifybackup", "{}/{}".format(node1.backup_dir, backup_name)],
            "verify backup {}".format(backup_name),
        )
    node3.stop()
    node2.stop()
    node1.stop()

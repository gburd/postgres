# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/bin/pg_combinebackup/t/007_wal_level_minimal.pl.

An incremental backup cannot be taken across a window where the server ran at
wal_level=minimal (with WAL summarization off): the WAL summaries needed since
the full backup are incomplete, so pg_basebackup --incremental fails.
"""


def test_007_wal_level_minimal(create_pg):
    """Incremental backup fails when WAL summaries are incomplete (minimal level)."""
    node1 = create_pg("node1", allows_streaming=True, start=False)
    node1.append_conf("summarize_wal = on\nwal_keep_size = '1GB'\n")
    node1.start()
    node1.safe_psql(
        "CREATE TABLE mytable (a int, b text);\n"
        "INSERT INTO mytable VALUES (1, 'finch');"
    )
    backup1path = "{}/backup1".format(node1.backup_dir)
    node1.command_ok(
        ["pg_basebackup", "--pgdata", backup1path, "--no-sync", "--checkpoint", "fast"],
        "full backup",
    )
    node1.safe_psql(
        "ALTER SYSTEM SET wal_level = minimal;\n"
        "ALTER SYSTEM SET max_wal_senders = 0;\n"
        "ALTER SYSTEM SET summarize_wal = off;"
    )
    node1.restart()
    node1.safe_psql("INSERT INTO mytable VALUES (2, 'gerbil');")
    node1.safe_psql(
        "ALTER SYSTEM RESET wal_level;\n"
        "ALTER SYSTEM RESET max_wal_senders;\n"
        "ALTER SYSTEM RESET summarize_wal;"
    )
    node1.restart()
    backup2path = "{}/backup2".format(node1.backup_dir)
    node1.command_fails_like(
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
        r"WAL summaries are required on timeline 1 from.*are incomplete",
        "incremental backup fails",
    )

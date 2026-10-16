# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/bin/pg_combinebackup/t/008_promote.pl.

An incremental backup can be taken from a promoted standby (on a new timeline)
relative to a full backup from the original primary, and pg_combinebackup can
reconstruct a usable data directory from that full + incremental chain.
"""


def test_008_promote(create_pg):
    """Incremental backup from a promoted standby combines into a usable cluster."""
    node1 = create_pg("node1", has_archiving=True, allows_streaming=True, start=False)
    node1.append_conf("summarize_wal = on")
    node1.append_conf("log_min_messages = debug1")
    node1.start()
    node1.safe_psql(
        "CREATE TABLE mytable (a int, b text);\n"
        "INSERT INTO mytable VALUES (1, 'avocado');"
    )
    backup1path = "{}/backup1".format(node1.backup_dir)
    node1.command_ok(
        ["pg_basebackup", "--pgdata", backup1path, "--no-sync", "--checkpoint", "fast"],
        "full backup from node1",
    )
    node1.safe_psql("CHECKPOINT")
    lsn = node1.safe_psql("SELECT pg_current_wal_insert_lsn()")
    node1.safe_psql("INSERT INTO mytable VALUES (2, 'beetle');")
    node2 = create_pg(
        "node2", from_backup=(node1, "backup1"), has_streaming=True, start=False
    )
    node2.append_conf(
        "recovery_target_lsn = '{}'\nrecovery_target_action = 'pause'\n".format(lsn)
    )
    node2.start()
    node2.poll_query_until("SELECT pg_get_wal_replay_pause_state() = 'paused';")
    node2.safe_psql("SELECT pg_promote()")
    node2.poll_query_until("SELECT pg_is_in_recovery() = 'f';")
    node2.safe_psql("INSERT INTO mytable VALUES (2, 'blackberry');")
    backup2path = "{}/backup2".format(node1.backup_dir)
    node2.command_ok(
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
        "incremental backup from node2",
    )
    node3 = create_pg(
        "node3",
        from_backup=(node1, "backup2"),
        combine_with_prior=["backup1"],
        start=False,
    )
    node3.start()
    node3.stop()

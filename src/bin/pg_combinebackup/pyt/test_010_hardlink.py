# Copyright (c) 2025-2026, PostgreSQL Global Development Group

"""Port of src/bin/pg_combinebackup/t/010_hardlink.pl.

pg_combinebackup --link reconstructs a data directory by hard-linking unchanged
relation file segments from the prior backup: every segment except the last of
a relation has 2 hard links (shared with the backup), and the last segment's
link count reflects whether that segment was rewritten in the incremental.
"""

import os


def _hard_link_count(path):
    return os.stat(path).st_nlink


def _check_data_file(data_file, last_segment_nlinks):
    segments = [data_file]
    n = 1
    while os.path.isfile("{}.{}".format(data_file, n)):
        segments.append("{}.{}".format(data_file, n))
        n += 1
    last = segments.pop()
    for segment in segments:
        assert _hard_link_count(segment) == 2, "File '{}' has 2 hard links".format(
            segment
        )
    assert (
        _hard_link_count(last) == last_segment_nlinks
    ), "File '{}' has {} hard link(s)".format(last, last_segment_nlinks)


def test_010_hardlink(create_pg):
    """pg_combinebackup --link hard-links unchanged relation segments."""
    create_query = (
        "CREATE TABLE test_{0} AS\n"
        "    SELECT x.id::bigint,\n"
        "           repeat('a', 1600) AS value\n"
        "    FROM generate_series(1, 100) AS x(id);"
    )
    path_query = (
        "SELECT pg_relation_filepath(oid) FROM pg_class WHERE relname = 'test_{0}';"
    )
    primary = create_pg(
        "primary", has_archiving=True, allows_streaming=True, start=False
    )
    primary.append_conf("summarize_wal = on")
    primary.append_conf("autovacuum = off")
    primary.start()
    primary.safe_psql(create_query.format("1"))
    primary.safe_psql(create_query.format("2"))
    test_1_path = primary.safe_psql(path_query.format("1"))
    test_2_path = primary.safe_psql(path_query.format("2"))
    backup1path = "{}/backup1".format(primary.backup_dir)
    primary.command_ok(
        [
            "pg_basebackup",
            "--pgdata",
            backup1path,
            "--no-sync",
            "--checkpoint",
            "fast",
            "--wal-method",
            "none",
        ],
        "full backup",
    )
    primary.safe_psql("INSERT INTO test_2 (id, value) VALUES (101, repeat('a', 1600));")
    backup2path = "{}/backup2".format(primary.backup_dir)
    primary.command_ok(
        [
            "pg_basebackup",
            "--pgdata",
            backup2path,
            "--no-sync",
            "--checkpoint",
            "fast",
            "--wal-method",
            "none",
            "--incremental",
            backup1path + "/backup_manifest",
        ],
        "incremental backup",
    )
    restore = create_pg(
        "restore",
        from_backup=(primary, "backup2"),
        combine_with_prior=["backup1"],
        combine_mode="--link",
        start=False,
    )
    _check_data_file("{}/{}".format(restore.datadir, test_1_path), 2)
    _check_data_file("{}/{}".format(restore.datadir, test_2_path), 1)
    primary.stop()

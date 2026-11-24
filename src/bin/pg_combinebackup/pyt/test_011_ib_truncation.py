# Copyright (c) 2022-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long,too-many-statements
"""Port of src/bin/pg_combinebackup/t/011_ib_truncation.pl.

Incremental backup across a relation truncation: pg_combinebackup correctly reconstructs a table that was truncated between the full and incremental backups (post-truncation block/row counts are preserved in the restored cluster).
Generated from the Perl original via .agent/gen_golden.py.
"""


def test_011_ib_truncation(create_pg):
    """Incremental backup reconstructs a relation truncated between backups."""
    primary = create_pg(
        "primary", allows_streaming=True, has_archiving=True, start=False
    )
    primary.append_conf("summarize_wal = on")
    primary.start()
    backup_path = primary.backup_dir
    full_backup = str(backup_path) + "/full"
    target_blocks = 6
    block_size = primary.safe_psql("SELECT current_setting('block_size')::int;")
    target_rows = int(target_blocks + 2)
    rows_after_truncation = int(target_rows - 1)
    primary.safe_psql(
        "CREATE TABLE t (\n        id int,\n        data text STORAGE PLAIN\n    ) WITH (autovacuum_enabled = false);"
    )
    primary.safe_psql(
        "INSERT INTO t\n        SELECT i,\n            repeat('0123456789ABCDEF0123456789ABCDEF', ("
        + str(block_size)
        + " / (2*32)))\n    FROM generate_series(1, "
        + str(target_rows)
        + ") i;"
    )
    primary.safe_psql("VACUUM t;")
    t_blocks = primary.safe_psql(
        "SELECT pg_relation_size('t') / current_setting('block_size')::int;"
    )
    assert int(t_blocks) > int(target_blocks), "target block size exceeded"
    primary.backup("full")
    primary.safe_psql("DELETE FROM t WHERE id > (" + str(rows_after_truncation) + ");")
    primary.safe_psql("VACUUM (TRUNCATE) t;")
    t_blocks = primary.safe_psql(
        "SELECT pg_relation_size('t') / current_setting('block_size')::int;"
    )
    assert t_blocks == str(
        rows_after_truncation
    ), "post-truncation row count as expected"
    assert int(t_blocks) > int(target_blocks), "post-truncation block count as expected"
    primary.backup(
        "incr", backup_options=["--incremental", str(full_backup) + "/backup_manifest"]
    )
    relfilenode = primary.safe_psql("SELECT pg_relation_filenode('t');")
    vm_limits = primary.safe_psql(
        "SELECT string_agg(relblocknumber::text, ',')\n\t   FROM pg_available_wal_summaries() s,\n\t        pg_wal_summary_contents(s.tli, s.start_lsn, s.end_lsn) c\n\t  WHERE c.relfilenode = "
        + str(relfilenode)
        + "\n\t    AND c.relforknumber = 2\n\t    AND c.is_limit_block;"
    )
    assert vm_limits == "1", "WAL summary has correct VM fork truncation limit"
    restored = create_pg(
        "node2", from_backup=(primary, "incr"), combine_with_prior=["full"], start=False
    )
    restored.start()
    restored_count = restored.safe_psql("SELECT count(*) FROM t;")
    assert restored_count == str(
        rows_after_truncation
    ), "Restored backup has correct row count"
    primary.stop()
    restored.stop()

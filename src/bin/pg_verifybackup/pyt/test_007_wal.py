# Copyright (c) 2020-2026, PostgreSQL Global Development Group

"""Port of src/bin/pg_verifybackup/t/007_wal.pl.

pg_verifybackup's WAL handling: a missing pg_wal fails WAL parsing (unless
--no-parse-wal, or --wal-path points at the relocated WAL), a corrupt WAL file
fails parsing, and backups taken on a timeline > 1 and in tar format (separate
pg_wal.tar) verify successfully.
"""

import os

import pypg


def test_007_wal(create_pg):
    """pg_verifybackup WAL parsing: missing/relocated/corrupt/timeline/tar."""
    primary = create_pg("primary", allows_streaming=True)
    backup_path = "{}/test_wal".format(primary.backup_dir)
    primary.command_ok(
        ["pg_basebackup", "--pgdata", backup_path, "--no-sync", "--checkpoint", "fast"],
        "base backup ok",
    )
    original_pg_wal = "{}/pg_wal".format(backup_path)
    relocated_pg_wal = "{}/relocated_pg_wal".format(primary.backup_dir)
    os.rename(original_pg_wal, relocated_pg_wal)
    primary.command_fails_like(
        ["pg_verifybackup", backup_path],
        r"WAL parsing failed for timeline 1",
        "missing pg_wal causes failure",
    )
    primary.command_ok(
        ["pg_verifybackup", "--no-parse-wal", backup_path],
        "missing pg_wal OK if not verifying WAL",
    )
    primary.command_ok(
        ["pg_verifybackup", "--wal-path", relocated_pg_wal, backup_path],
        "--wal-path can be used to specify WAL directory",
    )
    os.rename(relocated_pg_wal, original_pg_wal)
    walfiles = [
        f
        for f in pypg.slurp_dir(original_pg_wal)
        if len(f) == 24 and all(c in "0123456789ABCDEF" for c in f)
    ]
    target = "{}/{}".format(original_pg_wal, walfiles[0])
    wal_size = os.path.getsize(target)
    with open(target, "w", encoding="utf-8") as fh:
        fh.write("w" * wal_size)
    primary.command_fails_like(
        ["pg_verifybackup", backup_path],
        r"WAL parsing failed for timeline 1",
        "corrupt WAL file causes failure",
    )
    primary.stop()
    primary.append_conf("", "standby.signal")
    primary.start()
    primary.promote()
    primary.safe_psql("SELECT pg_switch_wal()")
    backup_path2 = "{}/test_tli".format(primary.backup_dir)
    primary.command_ok(
        [
            "pg_basebackup",
            "--pgdata",
            backup_path2,
            "--no-sync",
            "--checkpoint",
            "fast",
        ],
        "base backup 2 ok",
    )
    primary.command_ok(
        ["pg_verifybackup", backup_path2], "valid base backup with timeline > 1"
    )
    backup_path3 = "{}/test_tar_wal".format(primary.backup_dir)
    primary.command_ok(
        [
            "pg_basebackup",
            "--pgdata",
            backup_path3,
            "--no-sync",
            "--format",
            "tar",
            "--checkpoint",
            "fast",
        ],
        "tar backup with separate pg_wal.tar",
    )
    primary.command_ok(
        ["pg_verifybackup", backup_path3],
        "WAL verification succeeds with separate pg_wal.tar",
    )

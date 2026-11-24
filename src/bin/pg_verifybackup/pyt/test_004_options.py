# Copyright (c) 2020-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long
"""Port of src/bin/pg_verifybackup/t/004_options.pl.

pg_verifybackup option handling: --quiet is silent on success, --format
plain/tar validation, mutually-exclusive --progress/--quiet, checksum-mismatch
detection, --skip-checksums, --ignore (single/multiple/insufficient),
multiple-problem and --exit-on-error reporting, and a nonexistent directory.
"""

import re
import shutil

import pypg


def test_004_options(create_pg):
    """pg_verifybackup option/error handling across many scenarios."""
    primary = create_pg("primary", allows_streaming=True)
    backup_path = "{}/test_options".format(primary.backup_dir)
    primary.command_ok(
        ["pg_basebackup", "--pgdata", backup_path, "--no-sync", "--checkpoint", "fast"],
        "base backup ok",
    )
    res = primary.bin.result(["pg_verifybackup", "--quiet", backup_path])
    assert res.rc == 0, "--quiet succeeds: exit code 0"
    assert res.stdout == "", "--quiet succeeds: no stdout"
    assert res.stderr == "", "--quiet succeeds: no stderr"
    primary.command_ok(
        ["pg_verifybackup", "--format", "plain", backup_path],
        "verifies with --format=plain",
    )
    primary.command_fails_like(
        ["pg_verifybackup", "--format", "y", backup_path],
        r'invalid backup format "y", must be "plain" or "tar"',
        "does not verify with --format=y",
    )
    primary.command_fails_like(
        ["pg_verifybackup", "--format", "tar", "--no-parse-wal", backup_path],
        r'"pg_multixact" is not a regular file',
        "does not verify with --format=tar --no-parse-wal",
    )
    primary.command_fails_like(
        ["pg_verifybackup", "--progress", "--quiet", backup_path],
        r"cannot specify both -P/--progress and -q/--quiet",
        "cannot use --progress and --quiet at the same time",
    )
    version_pathname = "{}/PG_VERSION".format(backup_path)
    version_contents = pypg.slurp_file(version_pathname)
    with open(version_pathname, "w", encoding="utf-8") as fh:
        fh.write("q" * len(version_contents))
    primary.command_fails_like(
        ["pg_verifybackup", "--quiet", backup_path],
        r'checksum mismatch for file "PG_VERSION"',
        "--quiet checksum mismatch",
    )
    primary.command_like(
        ["pg_verifybackup", "--skip-checksums", backup_path],
        r"backup successfully verified",
        "--skip-checksums skips checksumming",
    )
    primary.command_checks_all(
        ["pg_verifybackup", "--progress", "--ignore", "PG_VERSION", backup_path],
        0,
        [r"backup successfully verified"],
        [r"(\d+/\d+ kB \(\d+%\) verified)+"],
        "--ignore ignores problem file",
    )
    shutil.rmtree("{}/pg_xact".format(backup_path))
    primary.command_fails_like(
        ["pg_verifybackup", "--ignore", "PG_VERSION", backup_path],
        r"pg_xact.*is present in the manifest but not on disk",
        "--ignore does not ignore all problems",
    )
    primary.command_like(
        [
            "pg_verifybackup",
            "--ignore",
            "PG_VERSION",
            "--ignore",
            "pg_xact",
            backup_path,
        ],
        r"backup successfully verified",
        "multiple --ignore options work",
    )
    res = primary.bin.result(["pg_verifybackup", backup_path])
    assert res.rc != 0, "multiple problems: fails"
    assert re.search(
        r"pg_xact.*is present in the manifest but not on disk", res.stderr
    ), "multiple problems: missing files reported"
    assert re.search(
        r'checksum mismatch for file "PG_VERSION"', res.stderr
    ), "multiple problems: checksum mismatch reported"
    res = primary.bin.result(["pg_verifybackup", "--exit-on-error", backup_path])
    assert res.rc != 0, "--exit-on-error reports 1 error: fails"
    assert re.search(
        r"pg_xact.*is present in the manifest but not on disk", res.stderr
    ), "--exit-on-error reports 1 error: missing files reported"
    assert not re.search(
        r'checksum mismatch for file "PG_VERSION"', res.stderr
    ), "--exit-on-error reports 1 error: checksum mismatch not reported"
    primary.command_fails_like(
        [
            "pg_verifybackup",
            "--manifest-path",
            "{}/backup_manifest".format(backup_path),
            "{}/fake".format(backup_path),
        ],
        r"could not open directory",
        "nonexistent backup directory",
    )

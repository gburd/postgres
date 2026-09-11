# Copyright (c) 2022-2026, PostgreSQL Global Development Group

"""Port of contrib/basebackup_to_shell/t/001_basic.pl.

The basebackup_to_shell module streams a base backup through a configured shell
command. The command must be configured; a target detail is permitted only when
the command template includes %d (and required when it does); an optional
required_role gates access. Successful backups produce gzip'd files that
decompress, untar, and verify with pg_verifybackup. Skips without gzip.
"""

import os
import subprocess

import pytest

_PG_BASEBACKUP_CMD = [
    "pg_basebackup",
    "--no-sync",
    "--checkpoint",
    "fast",
    "--username",
    "backupuser",
    "--wal-method",
    "fetch",
]


def _verify_backup(node, gzip, prefix, backup_dir, test_name, tmp_path):
    """Assert the gzip'd manifest/tar exist, then decompress/untar/verify."""
    assert os.path.isfile(
        "{}/{}backup_manifest.gz".format(backup_dir, prefix)
    ), "{}: backup_manifest.gz was created".format(test_name)
    assert os.path.isfile(
        "{}/{}base.tar.gz".format(backup_dir, prefix)
    ), "{}: base.tar.gz was created".format(test_name)
    tar = os.environ.get("TAR")
    if not tar:
        return
    subprocess.run(
        [gzip, "-d", "{}/{}backup_manifest.gz".format(backup_dir, prefix)],
        check=True,
    )
    subprocess.run(
        [gzip, "-d", "{}/{}base.tar.gz".format(backup_dir, prefix)], check=True
    )
    extract_path = tmp_path / "extract_{}".format(prefix or "nodetail")
    extract_path.mkdir()
    subprocess.run(
        [
            tar,
            "xf",
            "{}/{}base.tar".format(backup_dir, prefix),
            "-C",
            str(extract_path),
        ],
        check=True,
    )
    node.command_ok(
        [
            "pg_verifybackup",
            "--no-parse-wal",
            "--manifest-path",
            "{}/{}backup_manifest".format(backup_dir, prefix),
            "--exit-on-error",
            str(extract_path),
        ],
        "{}: backup verifies ok".format(test_name),
    )


def test_001_basic(create_pg, tmp_path):
    """basebackup_to_shell config gating and gzip'd backup verification."""
    gzip = os.environ.get("GZIP_PROGRAM")
    if not gzip:
        pytest.skip("gzip not available")
    node = create_pg(
        "primary",
        allows_streaming=True,
        auth_extra=["--create-role", "backupuser"],
        start=False,
    )
    node.append_conf("shared_preload_libraries = 'basebackup_to_shell'")
    node.start()
    node.safe_psql("CREATE USER backupuser REPLICATION")
    node.safe_psql("CREATE ROLE trustworthy")
    node.command_fails_like(
        _PG_BASEBACKUP_CMD + ["--target", "shell"],
        r"shell command for backup is not configured",
        "fails if basebackup_to_shell.command is not set",
    )
    backup_path = tmp_path / "backup"
    backup_path.mkdir()
    shell_command = '"{}" --fast > "{}/%f.gz"'.format(gzip, backup_path)
    node.append_conf("basebackup_to_shell.command='{}'".format(shell_command))
    node.reload()
    node.command_ok(
        _PG_BASEBACKUP_CMD + ["--target", "shell"],
        "backup with no detail: pg_basebackup",
    )
    _verify_backup(node, gzip, "", str(backup_path), "backup with no detail", tmp_path)
    node.command_fails_like(
        _PG_BASEBACKUP_CMD + ["--target", "shell:foo"],
        r"a target detail is not permitted because the configured command "
        r"does not include %d",
        "fails if detail provided without %d",
    )
    shell_command = '"{}" --fast > "{}/%d.%f.gz"'.format(gzip, backup_path)
    node.append_conf("basebackup_to_shell.command='{}'".format(shell_command))
    node.append_conf("basebackup_to_shell.required_role='trustworthy'")
    node.reload()
    node.command_fails_like(
        _PG_BASEBACKUP_CMD + ["--target", "shell"],
        r"permission denied to use basebackup_to_shell",
        "fails if required_role not granted",
    )
    node.safe_psql("GRANT trustworthy TO backupuser")
    node.command_fails_like(
        _PG_BASEBACKUP_CMD + ["--target", "shell"],
        r"a target detail is required because the configured command includes %d",
        "fails if %d is present and detail not given",
    )
    node.command_ok(
        _PG_BASEBACKUP_CMD + ["--target", "shell:bar"],
        "backup with detail: pg_basebackup",
    )
    _verify_backup(node, gzip, "bar.", str(backup_path), "backup with detail", tmp_path)

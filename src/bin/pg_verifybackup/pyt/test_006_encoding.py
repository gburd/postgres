# Copyright (c) 2020-2026, PostgreSQL Global Development Group

"""Port of src/bin/pg_verifybackup/t/006_encoding.pl.

pg_basebackup --manifest-force-encode hex-encodes every path in the backup
manifest; the manifest then contains many Encoded-Path entries and still
verifies successfully.
"""

import re

import pypg


def test_006_encoding(create_pg):
    """A force-encoded manifest has many Encoded-Path entries and verifies."""
    primary = create_pg("primary", allows_streaming=True)
    backup_path = "{}/test_encoding".format(primary.backup_dir)
    primary.command_ok(
        [
            "pg_basebackup",
            "--pgdata",
            backup_path,
            "--no-sync",
            "--checkpoint",
            "fast",
            "--manifest-force-encode",
        ],
        "backup ok with forced hex encoding",
    )
    manifest = pypg.slurp_file("{}/backup_manifest".format(backup_path))
    count = len(re.findall(r"Encoded-Path", manifest, re.IGNORECASE))
    assert count > 100, "many paths are encoded in the manifest"
    primary.command_like(
        ["pg_verifybackup", "--skip-checksums", backup_path],
        r"backup successfully verified",
        "backup with forced encoding verified",
    )

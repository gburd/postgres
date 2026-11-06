# Copyright (c) 2020-2026, PostgreSQL Global Development Group

"""Port of src/bin/pg_verifybackup/t/002_algorithm.pl.

pg_basebackup honors --manifest-checksums for every supported algorithm in both
plain and tar formats: a bogus algorithm is rejected, a valid one is recorded
throughout the manifest (or just produces a manifest for 'none'), and the
resulting backup verifies.
"""

import os
import shutil

import pypg


def _test_checksums(primary, fmt, algorithm):
    backup_path = "{}/{}/{}".format(primary.backup_dir, fmt, algorithm)
    backup = [
        "pg_basebackup",
        "--pgdata",
        backup_path,
        "--manifest-checksums",
        algorithm,
        "--no-sync",
        "--checkpoint",
        "fast",
    ]
    verify = ["pg_verifybackup", "--exit-on-error", backup_path]
    if fmt == "tar":
        backup += ["--format", "tar"]
    if algorithm == "bogus":
        primary.command_fails(
            backup, '{} format backup fails with algorithm "{}"'.format(fmt, algorithm)
        )
        return
    primary.command_ok(
        backup, '{} format backup ok with algorithm "{}"'.format(fmt, algorithm)
    )
    if algorithm == "none":
        assert os.path.isfile(
            "{}/backup_manifest".format(backup_path)
        ), "{} format backup manifest exists".format(fmt)
    else:
        manifest = pypg.slurp_file("{}/backup_manifest".format(backup_path))
        count = manifest.lower().count(algorithm.lower())
        assert count > 100, "{} is mentioned many times in the manifest".format(
            algorithm
        )
    primary.command_ok(
        verify, 'verify {} format backup with algorithm "{}"'.format(fmt, algorithm)
    )
    shutil.rmtree(backup_path)


def test_002_algorithm(create_pg):
    """pg_basebackup --manifest-checksums across formats and algorithms."""
    primary = create_pg("primary", allows_streaming=True)
    for fmt in ("plain", "tar"):
        for algorithm in (
            "bogus",
            "none",
            "crc32c",
            "sha224",
            "sha256",
            "sha384",
            "sha512",
        ):
            _test_checksums(primary, fmt, algorithm)

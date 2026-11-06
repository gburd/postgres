# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/bin/pg_combinebackup/t/004_manifest.pl.

pg_combinebackup manifest options: --no-manifest produces an unverifiable
backup; --manifest-checksums=NONE/SHA224 control the manifest's checksum
algorithm (SHA224 appears throughout, none omits Checksum-Algorithm), and each
combined backup verifies as expected.
"""

import os
import re

import pypg


def _combine_and_test(node, original, backup_name, failure_pattern, extra):
    revised = "{}/{}".format(node.backup_dir, backup_name)
    node.command_ok(
        ["pg_combinebackup", original, "--output", revised, "--no-sync"] + extra,
        "pg_combinebackup with {}".format(" ".join(extra)),
    )
    if failure_pattern is not None:
        node.command_fails_like(
            ["pg_verifybackup", revised],
            failure_pattern,
            "unable to verify backup {}".format(backup_name),
        )
    else:
        node.command_ok(
            ["pg_verifybackup", revised], "verify backup {}".format(backup_name)
        )


def test_004_manifest(create_pg):
    """pg_combinebackup --no-manifest and --manifest-checksums behavior."""
    mode = os.environ.get("PG_TEST_PG_COMBINEBACKUP_MODE") or "--copy"
    node = create_pg("node", has_archiving=True, allows_streaming=True)
    original = "{}/original".format(node.backup_dir)
    node.command_ok(
        ["pg_basebackup", "--pgdata", original, "--no-sync", "--checkpoint", "fast"],
        "full backup",
    )
    node.command_ok(["pg_verifybackup", original], "verify original backup")
    _combine_and_test(
        node,
        original,
        "nomanifest",
        r"could not open file.*backup_manifest",
        ["--no-manifest"],
    )
    _combine_and_test(
        node, original, "csum_none", None, ["--manifest-checksums=NONE", mode]
    )
    _combine_and_test(
        node, original, "csum_sha224", None, ["--manifest-checksums=SHA224", mode]
    )
    sha224 = pypg.slurp_file("{}/csum_sha224/backup_manifest".format(node.backup_dir))
    assert (
        len(re.findall(r"SHA224", sha224, re.IGNORECASE)) > 100
    ), "SHA224 is mentioned many times in SHA224 manifest"
    nocsum = pypg.slurp_file("{}/csum_none/backup_manifest".format(node.backup_dir))
    assert (
        len(re.findall(r"Checksum-Algorithm", nocsum, re.IGNORECASE)) == 0
    ), "Checksum-Algorithm is not mentioned in no-checksum manifest"

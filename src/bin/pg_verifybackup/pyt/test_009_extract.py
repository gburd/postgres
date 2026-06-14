# Copyright (c) 2020-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long
"""Port of src/bin/pg_verifybackup/t/009_extract.pl.

A server-compressed plain-format base backup, taken with each compression
method the build supports (none/gzip/lz4/zstd/parallel-zstd), extracts and
verifies successfully. Compression methods the build lacks are skipped.
"""

import re
import shutil

import pypg

_CONFIG = [
    {"method": "none", "flags": [], "enabled": True},
    {
        "method": "gzip",
        "flags": ["--compress", "server-gzip:5"],
        "enabled": pypg.check_pg_config(r"#define HAVE_LIBZ 1"),
    },
    {
        "method": "lz4",
        "flags": ["--compress", "server-lz4:5"],
        "enabled": pypg.check_pg_config(r"#define USE_LZ4 1"),
    },
    {
        "method": "zstd",
        "flags": ["--compress", "server-zstd:5"],
        "enabled": pypg.check_pg_config(r"#define USE_ZSTD 1"),
    },
    {
        "method": "parallel zstd",
        "flags": ["--compress", "server-zstd:workers=3"],
        "enabled": pypg.check_pg_config(r"#define USE_ZSTD 1"),
        "possibly_unsupported": r"could not set compression worker count to 3: Unsupported parameter",
    },
]


def test_009_extract(create_pg):
    """Server-compressed backups extract and verify for each supported method."""
    primary = create_pg("primary", allows_streaming=True)
    for tc in _CONFIG:
        backup_path = "{}/extract_backup".format(primary.backup_dir)
        method = tc["method"]
        if not tc["enabled"]:
            continue
        result = primary.bin.run_command(
            [
                "pg_basebackup",
                "--pgdata",
                backup_path,
                "--wal-method",
                "fetch",
                "--no-sync",
                "--checkpoint",
                "fast",
                "--format",
                "plain",
            ]
            + tc["flags"]
        )
        unsupported = tc.get("possibly_unsupported")
        if result.rc != 0 and unsupported and re.search(unsupported, result.stderr):
            continue
        assert result.rc == 0, "backup done, compression {}".format(method)
        primary.command_ok(
            ["pg_verifybackup", "--exit-on-error", backup_path],
            'backup verified, compression method "{}"'.format(method),
        )
        shutil.rmtree(backup_path)

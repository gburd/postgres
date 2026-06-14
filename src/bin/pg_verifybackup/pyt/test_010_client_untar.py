# Copyright (c) 2020-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long
"""Port of src/bin/pg_verifybackup/t/010_client_untar.pl.

Client-side tar-format base backups (--format tar) with each supported
compression method produce the expected base.tar[.ext] archive plus the
manifest and verify. Methods the build lacks (or a parallel-zstd worker count it
cannot honor) are skipped.
"""

import re
import shutil

import pypg


def _configs():
    z = pypg.check_pg_config(r"#define HAVE_LIBZ 1")
    lz4 = pypg.check_pg_config(r"#define USE_LZ4 1")
    zstd = pypg.check_pg_config(r"#define USE_ZSTD 1")
    return [
        ("none", [], "base.tar", True, None),
        ("gzip", ["--compress", "client-gzip:5"], "base.tar.gz", z, None),
        ("lz4", ["--compress", "client-lz4:5"], "base.tar.lz4", lz4, None),
        ("lz4", ["--compress", "client-lz4:1"], "base.tar.lz4", lz4, None),
        ("zstd", ["--compress", "client-zstd:5"], "base.tar.zst", zstd, None),
        (
            "zstd",
            ["--compress", "client-zstd:level=1,long"],
            "base.tar.zst",
            zstd,
            None,
        ),
        (
            "parallel zstd",
            ["--compress", "client-zstd:workers=3"],
            "base.tar.zst",
            zstd,
            r"could not set compression worker count to 3: Unsupported parameter",
        ),
    ]


def test_010_client_untar(create_pg):
    """Client-side tar backups produce expected archives and verify."""
    primary = create_pg("primary", allows_streaming=True)
    junk_data = primary.safe_psql(
        "SELECT string_agg(encode(sha256(i::bytea), 'hex'), '') "
        "FROM generate_series(1, 10240) s(i);"
    )
    with open("{}/junk".format(primary.datadir), "w", encoding="utf-8") as jf:
        jf.write(junk_data)
    backup_path = "{}/client-backup".format(primary.backup_dir)
    for method, flags, archive, enabled, unsupported in _configs():
        if not enabled:
            continue
        result = primary.bin.run_command(
            [
                "pg_basebackup",
                "--no-sync",
                "--pgdata",
                backup_path,
                "--wal-method",
                "fetch",
                "--checkpoint",
                "fast",
                "--format",
                "tar",
            ]
            + flags
        )
        if result.rc != 0 and unsupported and re.search(unsupported, result.stderr):
            continue
        assert result.rc == 0, "client side backup, compression {}".format(method)
        backup_files = ",".join(
            sorted(f for f in pypg.slurp_dir(backup_path) if f not in (".", ".."))
        )
        expected = ",".join(sorted(["backup_manifest", archive]))
        assert (
            backup_files == expected
        ), "found expected backup files, compression {}".format(method)
        primary.command_ok(
            ["pg_verifybackup", "--exit-on-error", backup_path],
            "verify backup, compression {}".format(method),
        )
        shutil.rmtree(backup_path)

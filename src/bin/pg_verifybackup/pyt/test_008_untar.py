# Copyright (c) 2020-2026, PostgreSQL Global Development Group

"""Port of src/bin/pg_verifybackup/t/008_untar.pl.

Server-side tar-format base backups (--target server:...) with each supported
compression method produce exactly the expected archive files (base.tar[.ext]
plus one per tablespace) alongside the manifest, and verify successfully.
Methods the build lacks are skipped.
"""

import shutil
import tempfile

import pypg


def _configs(tsoid):
    z = pypg.check_pg_config(r"#define HAVE_LIBZ 1")
    lz4 = pypg.check_pg_config(r"#define USE_LZ4 1")
    zstd = pypg.check_pg_config(r"#define USE_ZSTD 1")
    return [
        ("none", [], ["base.tar", "{}.tar".format(tsoid)], True),
        (
            "gzip",
            ["--compress", "server-gzip"],
            ["base.tar.gz", "{}.tar.gz".format(tsoid)],
            z,
        ),
        (
            "lz4",
            ["--compress", "server-lz4"],
            ["base.tar.lz4", "{}.tar.lz4".format(tsoid)],
            lz4,
        ),
        (
            "lz4",
            ["--compress", "server-lz4:5"],
            ["base.tar.lz4", "{}.tar.lz4".format(tsoid)],
            lz4,
        ),
        (
            "zstd",
            ["--compress", "server-zstd"],
            ["base.tar.zst", "{}.tar.zst".format(tsoid)],
            zstd,
        ),
        (
            "zstd",
            ["--compress", "server-zstd:level=1,long"],
            ["base.tar.zst", "{}.tar.zst".format(tsoid)],
            zstd,
        ),
    ]


def test_008_untar(create_pg):
    """Server-side tar backups produce expected archives and verify."""
    primary = create_pg("primary", allows_streaming=True)
    junk_data = primary.safe_psql(
        "SELECT string_agg(encode(sha256(i::bytea), 'hex'), '') "
        "FROM generate_series(1, 10240) s(i);"
    )
    with open("{}/junk".format(primary.datadir), "w", encoding="utf-8") as jf:
        jf.write(junk_data)
    source_ts_path = tempfile.mkdtemp(prefix="ts_")
    primary.safe_psql(
        "CREATE TABLESPACE regress_ts1 LOCATION '{}';\n"
        "CREATE TABLE regress_tbl1(i int) TABLESPACE regress_ts1;\n"
        "INSERT INTO regress_tbl1 VALUES(generate_series(1,5));".format(source_ts_path)
    )
    tsoid = primary.safe_psql(
        "SELECT oid FROM pg_tablespace WHERE spcname = 'regress_ts1'"
    )
    backup_path = "{}/server-backup".format(primary.backup_dir)
    for method, flags, archives, enabled in _configs(tsoid):
        if not enabled:
            continue
        primary.command_ok(
            [
                "pg_basebackup",
                "--no-sync",
                "--checkpoint",
                "fast",
                "--target",
                "server:{}".format(backup_path),
                "--wal-method",
                "fetch",
            ]
            + flags,
            "server side backup, compression {}".format(method),
        )
        backup_files = ",".join(
            sorted(f for f in pypg.slurp_dir(backup_path) if f not in (".", ".."))
        )
        expected = ",".join(sorted(["backup_manifest"] + archives))
        assert (
            backup_files == expected
        ), "found expected backup files, compression {}".format(method)
        primary.command_ok(
            ["pg_verifybackup", "--exit-on-error", backup_path],
            "verify backup, compression {}".format(method),
        )
        shutil.rmtree(backup_path)

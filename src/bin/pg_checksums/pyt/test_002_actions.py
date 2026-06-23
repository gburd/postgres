# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/bin/pg_checksums/t/002_actions.pl.

Basic sanity checks for pg_checksums using an initialized cluster.
"""

import platform

import pypg

darwin = platform.system() == "Darwin"

# Empty files that are correctly named, so pg_checksums passes them through.
_EMPTY_OK_FILES = [
    "99999",
    "99999.123",
    "99999_fsm",
    "99999_init",
    "99999_vm",
    "99999_init.123",
    "99999_fsm.123",
    "99999_vm.123",
]

# Correctly-named relation files which, when filled with junk, must be flagged.
_FAIL_CORRUPT_FILES = [
    "99990",
    "99990.123",
    "99990_fsm",
    "99990_init",
    "99990_vm",
    "99990_init.123",
    "99990_fsm.123",
    "99990_vm.123",
]


def _check_relation_corruption(pg_bin, node, table, tablespace):
    """Corrupt a relation's checksum on a tablespace and check detection."""
    pgdata = node.datadir

    node.safe_psql(
        "CREATE TABLE {0} AS SELECT a FROM generate_series(1,10000) AS a;"
        " ALTER TABLE {0} SET (autovacuum_enabled=false);".format(table)
    )
    node.safe_psql("ALTER TABLE {} SET TABLESPACE {};".format(table, tablespace))

    file_corrupted = node.safe_psql("SELECT pg_relation_filepath('{}');".format(table))
    relfilenode_corrupted = node.safe_psql(
        "SELECT relfilenode FROM pg_class WHERE relname = '{}';".format(table)
    )

    node.stop()

    # Checksums are correct for the single relfilenode (not corrupted yet).
    pg_bin.command_ok(
        [
            "pg_checksums",
            "--check",
            "--pgdata",
            pgdata,
            "--filenode",
            relfilenode_corrupted,
        ],
        "succeeds for single relfilenode on tablespace {} with offline cluster".format(
            tablespace
        ),
    )

    node.corrupt_page_checksum(file_corrupted, 0)

    node.command_checks_all(
        [
            "pg_checksums",
            "--check",
            "--pgdata",
            pgdata,
            "--filenode",
            relfilenode_corrupted,
        ],
        1,
        [r"Bad checksums:.*1"],
        [r"checksum verification failed"],
        "fails with corrupted data for single relfilenode on tablespace {}".format(
            tablespace
        ),
    )

    node.command_checks_all(
        ["pg_checksums", "--check", "--pgdata", pgdata],
        1,
        [r"Bad checksums:.*1"],
        [r"checksum verification failed"],
        "fails with corrupted data on tablespace {}".format(tablespace),
    )

    node.start()
    node.safe_psql("DROP TABLE {};".format(table))
    node.stop()
    node.command_ok(
        ["pg_checksums", "--check", "--pgdata", pgdata],
        "succeeds again after table drop on tablespace {}".format(tablespace),
    )
    node.start()


def _fail_corrupt(node, file):
    """pg_checksums must flag a correctly-named relation file full of junk."""
    pgdata = node.datadir
    file_name = pgdata / "global" / file
    pypg.append_to_file(file_name, "foo")

    node.command_checks_all(
        ["pg_checksums", "--check", "--pgdata", pgdata],
        1,
        [r"^$"],
        [r"could not read block 0 in file.*" + file + r'":'],
        "fails for corrupted data in {}".format(file),
    )

    file_name.unlink()


def _setup_dummy_files(pgdata):
    for name in _EMPTY_OK_FILES:
        pypg.append_to_file(pgdata / "global" / name, "")

    # Temporary files/folders with dummy contents, ignored by the scan.
    pypg.append_to_file(pgdata / "global" / "pgsql_tmp_123", "foo")
    (pgdata / "global" / "pgsql_tmp").mkdir()
    pypg.append_to_file(pgdata / "global" / "pgsql_tmp" / "1.1", "foo")
    pypg.append_to_file(pgdata / "global" / "pg_internal.init", "foo")
    pypg.append_to_file(pgdata / "global" / "pg_internal.init.123", "foo")

    # Non-postgres macOS file, ignored by the scan (skip creating it on macOS).
    if not darwin:
        pypg.append_to_file(pgdata / "global" / ".DS_Store", "foo")


def test_pg_checksums_actions(pg_bin, create_pg):
    """Enable/disable/verify checksums and detect corruption."""
    node = create_pg("node_checksum", start=False, extra=["--no-data-checksums"])
    pgdata = node.datadir

    pg_bin.command_like(
        ["pg_controldata", pgdata],
        r"Data page checksum version:.*0",
        "checksums disabled in control file",
    )

    _setup_dummy_files(pgdata)

    pg_bin.command_ok(
        ["pg_checksums", "--enable", "--no-sync", "--pgdata", pgdata],
        "checksums successfully enabled in cluster",
    )
    pg_bin.command_fails(
        ["pg_checksums", "--enable", "--no-sync", "--pgdata", pgdata],
        "enabling checksums fails if already enabled",
    )
    pg_bin.command_like(
        ["pg_controldata", pgdata],
        r"Data page checksum version:.*1",
        "checksums enabled in control file",
    )

    pg_bin.command_ok(
        ["pg_checksums", "--disable", "--pgdata", pgdata],
        "checksums successfully disabled in cluster",
    )
    pg_bin.command_fails(
        ["pg_checksums", "--disable", "--no-sync", "--pgdata", pgdata],
        "disabling checksums fails if already disabled",
    )
    pg_bin.command_like(
        ["pg_controldata", pgdata],
        r"Data page checksum version:.*0",
        "checksums disabled in control file",
    )

    pg_bin.command_ok(
        ["pg_checksums", "--enable", "--no-sync", "--pgdata", pgdata],
        "checksums successfully enabled in cluster",
    )
    pg_bin.command_like(
        ["pg_controldata", pgdata],
        r"Data page checksum version:.*1",
        "checksums enabled in control file",
    )

    pg_bin.command_ok(
        ["pg_checksums", "--check", "--pgdata", pgdata], "succeeds with offline cluster"
    )
    pg_bin.command_ok(
        ["pg_checksums", "--pgdata", pgdata], "verifies checksums as default action"
    )

    pg_bin.command_fails(
        ["pg_checksums", "--disable", "--filenode", "1234", "--pgdata", pgdata],
        "fails when relfilenodes are requested and action is --disable",
    )
    pg_bin.command_fails(
        ["pg_checksums", "--enable", "--filenode", "1234", "--pgdata", pgdata],
        "fails when relfilenodes are requested and action is --enable",
    )

    # postgres -C for an offline cluster (reports the GUC; server won't start).
    pg_bin.command_checks_all(
        [
            "pg_ctl",
            "start",
            "--silent",
            "--pgdata",
            pgdata,
            "-o",
            "-C data_checksums -c log_min_messages=fatal",
        ],
        1,
        [r"^on$"],
        [r"could not start server"],
        "data_checksums=on is reported on an offline cluster",
    )

    node.start()
    pg_bin.command_fails(
        ["pg_checksums", "--check", "--pgdata", pgdata], "fails with online cluster"
    )

    _check_relation_corruption(pg_bin, node, "corrupt1", "pg_default")

    tablespace_dir = node.basedir / "ts_corrupt_dir"
    tablespace_dir.mkdir()
    node.safe_psql("CREATE TABLESPACE ts_corrupt LOCATION '{}';".format(tablespace_dir))
    _check_relation_corruption(pg_bin, node, "corrupt2", "ts_corrupt")

    node.stop()

    # A foreign tablespace location must not be scanned.
    (tablespace_dir / "PG_99_999999991").mkdir()
    pypg.append_to_file(tablespace_dir / "PG_99_999999991" / "foo", "123")
    pg_bin.command_ok(
        ["pg_checksums", "--check", "--pgdata", pgdata],
        "succeeds with foreign tablespace",
    )

    for file in _FAIL_CORRUPT_FILES:
        _fail_corrupt(node, file)

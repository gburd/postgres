# Copyright (c) 2017-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long,too-many-statements
"""Port of src/bin/pg_basebackup/t/020_pg_receivewal.pl.

pg_receivewal option/usage handling, slot create/drop, synchronous streaming to
.partial then completed segments, optional gzip/lz4/zstd compression (skipped
when the build lacks them), streaming from a slot's restart_lsn, stream-dir
permissions, and resuming across a standby promotion / timeline jump.
"""

import glob
import os
import re

import pypg


def _glob1(pattern, msg):
    matches = glob.glob(pattern)
    assert len(matches) == 1, msg
    return matches[0]


def test_020_pg_receivewal(create_pg, pg_bin):
    """pg_receivewal usage, slots, compression, restart_lsn, and timeline jump."""
    os.umask(0o077)
    pg_bin.program_help_ok("pg_receivewal")
    pg_bin.program_version_ok("pg_receivewal")
    pg_bin.program_options_handling_ok("pg_receivewal")
    primary = create_pg(
        "primary", allows_streaming=True, extra=["--wal-segsize=1"], start=False
    )
    primary.start()
    stream_dir = "{}/archive_wal".format(primary.basedir)
    os.mkdir(stream_dir)
    primary.command_fails(
        ["pg_receivewal"], "pg_receivewal needs target directory specified"
    )
    primary.command_fails(
        ["pg_receivewal", "--directory", stream_dir, "--create-slot", "--drop-slot"],
        "failure if both --create-slot and --drop-slot specified",
    )
    primary.command_fails(
        ["pg_receivewal", "--directory", stream_dir, "--create-slot"],
        "failure if --create-slot specified without --slot",
    )
    primary.command_fails(
        ["pg_receivewal", "--directory", stream_dir, "--synchronous", "--no-sync"],
        "failure if --synchronous specified with --no-sync",
    )
    primary.command_fails_like(
        ["pg_receivewal", "--directory", stream_dir, "--compress", "none:1"],
        r'pg_receivewal: error: invalid compression specification: compression algorithm "none" does not accept a compression level',
        "failure if --compress none:N (where N > 0)",
    )
    slot_name = "test"
    primary.command_ok(
        ["pg_receivewal", "--slot", slot_name, "--create-slot"],
        "creating a replication slot",
    )
    slot = primary.slot(slot_name)
    assert slot["slot_type"] == "physical", "physical replication slot was created"
    assert slot["restart_lsn"] == "", "restart LSN of new slot is null"
    primary.command_ok(
        ["pg_receivewal", "--slot", slot_name, "--drop-slot"],
        "dropping a replication slot",
    )
    assert primary.slot(slot_name)["slot_type"] == "", "replication slot was removed"
    primary.psql_capture("CREATE TABLE test_table(x integer PRIMARY KEY);")
    primary.psql_capture("SELECT pg_switch_wal();")
    nextlsn = primary.safe_psql("SELECT pg_current_wal_insert_lsn();")
    primary.psql_capture("INSERT INTO test_table VALUES (1);")
    primary.command_ok(
        [
            "pg_receivewal",
            "--directory",
            stream_dir,
            "--verbose",
            "--endpos",
            nextlsn,
            "--synchronous",
            "--no-loop",
        ],
        "streaming some WAL with --synchronous",
    )
    partial_wal = _glob1(
        "{}/*.partial".format(stream_dir), "one partial WAL segment was created"
    )
    partial_wal = _compression_blocks(primary, stream_dir, partial_wal)
    primary.psql_capture("SELECT pg_switch_wal();")
    nextlsn = primary.safe_psql("SELECT pg_current_wal_insert_lsn();")
    primary.psql_capture("INSERT INTO test_table VALUES (4);")
    primary.command_ok(
        [
            "pg_receivewal",
            "--directory",
            stream_dir,
            "--verbose",
            "--endpos",
            nextlsn,
            "--no-loop",
        ],
        "streaming some WAL",
    )
    completed = re.sub(r"(\.gz|\.lz4)?\.partial", "", partial_wal)
    assert os.path.exists(
        completed
    ), "check that previously partial WAL is now complete"
    assert pypg.check_mode_recursive(
        stream_dir, 0o700, 0o600
    ), "check stream dir permissions"
    _slot_restart_lsn_streaming(primary)
    _timeline_jump(create_pg, primary)


def _compression_blocks(primary, stream_dir, partial_wal):
    """Run the gzip/lz4/zstd streaming sub-tests the build supports."""
    if pypg.check_pg_config(r"#define HAVE_LIBZ 1"):
        primary.psql_capture("SELECT pg_switch_wal();")
        nextlsn = primary.safe_psql("SELECT pg_current_wal_insert_lsn();")
        primary.psql_capture("INSERT INTO test_table VALUES (2);")
        primary.command_ok(
            [
                "pg_receivewal",
                "--directory",
                stream_dir,
                "--verbose",
                "--endpos",
                nextlsn,
                "--compress",
                "gzip:1",
                "--no-loop",
            ],
            "streaming some WAL using ZLIB compression",
        )
        zlib_wal = _glob1(
            "{}/*.gz".format(stream_dir),
            "one WAL segment compressed with ZLIB was created",
        )
        partial_wal = _glob1(
            "{}/*.gz.partial".format(stream_dir),
            "one partial WAL segment compressed with ZLIB was created",
        )
        gzip = os.environ.get("GZIP_PROGRAM")
        if gzip:
            assert (
                primary.bin.run_command([gzip, "--test", zlib_wal]).rc == 0
            ), "gzip verified the integrity of compressed WAL segments"
    if pypg.check_pg_config(r"#define USE_LZ4 1"):
        primary.psql_capture("SELECT pg_switch_wal();")
        nextlsn = primary.safe_psql("SELECT pg_current_wal_insert_lsn();")
        primary.psql_capture("INSERT INTO test_table VALUES (3);")
        primary.command_ok(
            [
                "pg_receivewal",
                "--directory",
                stream_dir,
                "--verbose",
                "--endpos",
                nextlsn,
                "--no-loop",
                "--compress",
                "lz4",
            ],
            "streaming some WAL using --compress=lz4",
        )
        lz4_wal = _glob1(
            "{}/*.lz4".format(stream_dir),
            "one WAL segment compressed with LZ4 was created",
        )
        partial_wal = _glob1(
            "{}/*.lz4.partial".format(stream_dir),
            "one partial WAL segment compressed with LZ4 was created",
        )
        lz4 = os.environ.get("LZ4")
        if lz4:
            assert (
                primary.bin.run_command([lz4, "-t", lz4_wal]).rc == 0
            ), "lz4 verified the integrity of compressed WAL segments"
    return partial_wal


def _slot_restart_lsn_streaming(primary):
    """Stream WAL starting from a slot's restart_lsn into a dedicated dir."""
    slot_dir = "{}/slot_wal".format(primary.basedir)
    os.mkdir(slot_dir)
    slot_name = "archive_slot"
    primary.psql_capture("checkpoint;")
    primary.psql_capture(
        "SELECT pg_create_physical_replication_slot('{}', true);".format(slot_name)
    )
    walfile_streamed = primary.safe_psql(
        "SELECT pg_walfile_name(restart_lsn) FROM pg_replication_slots "
        "WHERE slot_name = '{}';".format(slot_name)
    )
    primary.psql_capture("INSERT INTO test_table VALUES (5);")
    primary.psql_capture("SELECT pg_switch_wal();")
    nextlsn = primary.safe_psql("SELECT pg_current_wal_insert_lsn();")
    primary.psql_capture("INSERT INTO test_table VALUES (6);")
    primary.command_fails_like(
        [
            "pg_receivewal",
            "--directory",
            slot_dir,
            "--slot",
            "nonexistentslot",
            "--no-loop",
            "--no-sync",
            "--verbose",
            "--endpos",
            nextlsn,
        ],
        r'pg_receivewal: error: replication slot "nonexistentslot" does not exist',
        "pg_receivewal fails with non-existing slot",
    )
    primary.command_ok(
        [
            "pg_receivewal",
            "--directory",
            slot_dir,
            "--slot",
            slot_name,
            "--no-loop",
            "--no-sync",
            "--verbose",
            "--endpos",
            nextlsn,
        ],
        "WAL streamed from the slot's restart_lsn",
    )
    assert os.path.exists(
        "{}/{}".format(slot_dir, walfile_streamed)
    ), "WAL from the slot's restart_lsn has been archived"


def _timeline_jump(create_pg, primary):
    """After a standby promotion, resume streaming across the timeline jump."""
    backup_name = "basebackup"
    primary.backup(backup_name)
    standby = create_pg(
        "standby", from_backup=(primary, backup_name), has_streaming=True, start=False
    )
    standby.start()
    archive_slot = "archive_slot"
    standby.psql_capture(
        "CREATE_REPLICATION_SLOT {} PHYSICAL (RESERVE_WAL)".format(archive_slot),
        dbname="",
        replication="1",
    )
    primary.wait_for_catchup(standby)
    replication_slot_lsn = standby.slot(archive_slot)["restart_lsn"]
    walfile_before = primary.safe_psql(
        "SELECT pg_walfile_name('{}');".format(replication_slot_lsn)
    )
    standby.promote()
    walfile_after = standby.safe_psql(
        "SELECT pg_walfile_name(pg_current_wal_insert_lsn());"
    )
    standby.psql_capture("INSERT INTO test_table VALUES (7);")
    standby.psql_capture("SELECT pg_switch_wal();")
    nextlsn = standby.safe_psql("SELECT pg_current_wal_insert_lsn();")
    standby.psql_capture("INSERT INTO test_table VALUES (8);")
    timeline_dir = "{}/timeline_wal".format(primary.basedir)
    os.mkdir(timeline_dir)
    standby.command_ok(
        [
            "pg_receivewal",
            "--directory",
            timeline_dir,
            "--verbose",
            "--endpos",
            nextlsn,
            "--slot",
            archive_slot,
            "--no-sync",
            "--no-loop",
        ],
        "Stream some wal after promoting, resuming from the slot's position",
    )
    assert os.path.exists(
        "{}/{}".format(timeline_dir, walfile_before)
    ), "WAL segment {} archived after timeline jump".format(walfile_before)
    assert os.path.exists(
        "{}/{}".format(timeline_dir, walfile_after)
    ), "WAL segment {} archived after timeline jump".format(walfile_after)
    assert os.path.exists(
        "{}/00000002.history".format(timeline_dir)
    ), "timeline history file archived after timeline jump"

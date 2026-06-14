# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/recovery/t/020_archive_status.pl.

WAL archiving status (.ready/.done) and recovery behavior with archive_mode
on/always on standbys.
"""

import platform
import re

import pypg

windows_os = platform.system() == "Windows"
_BAD_COMMAND = (
    'copy "%p_does_not_exist" "%f_does_not_exist"'
    if windows_os
    else 'cp "%p_does_not_exist" "%f_does_not_exist"'
)


def _status_paths(segment):
    base = "pg_wal/archive_status/{}".format(segment)
    return base + ".ready", base + ".done"


def test_archive_status(create_pg):
    """.ready/.done lifecycle on a primary and on archive_mode on/always standbys."""
    primary = create_pg(
        "primary", has_archiving=True, allows_streaming=True, start=False
    )
    primary.append_conf("autovacuum = off")
    primary.start()
    primary_data = primary.datadir

    # Make archiving fail (a working command given a wrong path).
    primary.safe_psql(
        "ALTER SYSTEM SET archive_command TO '{}';\n"
        "SELECT pg_reload_conf();".format(_BAD_COMMAND)
    )

    seg1 = primary.safe_psql("SELECT pg_walfile_name(pg_current_wal_lsn())")
    seg1_ready, seg1_done = _status_paths(seg1)
    primary.safe_psql(
        "CREATE TABLE mine AS SELECT generate_series(1,10) AS x;\n"
        "SELECT pg_switch_wal();\nCHECKPOINT;"
    )

    assert primary.poll_query_until(
        "SELECT failed_count > 0 FROM pg_stat_archiver"
    ), "archiving to fail"
    assert (primary_data / seg1_ready).is_file(), ".ready exists for {}".format(seg1)
    assert not (primary_data / seg1_done).is_file(), ".done absent for {}".format(seg1)
    assert primary.safe_psql(
        "SELECT archived_count, last_failed_wal FROM pg_stat_archiver"
    ) == "0|{}".format(seg1), "pg_stat_archiver failed to archive {}".format(seg1)

    # Crash, then a cold backup taken while archiving fails (used by standbys).
    primary.stop("immediate")
    primary.backup_fs_cold("backup")
    primary.start()
    assert (primary_data / seg1_ready).is_file(), ".ready survives crash recovery"

    # Allow archiving again; wait for success.
    primary.safe_psql("ALTER SYSTEM RESET archive_command;\nSELECT pg_reload_conf();")
    assert primary.poll_query_until(
        "SELECT archived_count FROM pg_stat_archiver", expected="1"
    ), "archiving to finish"
    assert not (primary_data / seg1_ready).is_file(), ".ready removed for {}".format(
        seg1
    )
    assert (primary_data / seg1_done).is_file(), ".done exists for {}".format(seg1)
    assert (
        primary.safe_psql("SELECT last_archived_wal FROM pg_stat_archiver") == seg1
    ), "archive success reported for {}".format(seg1)

    seg2 = primary.safe_psql("SELECT pg_walfile_name(pg_current_wal_lsn())")
    seg2_ready, seg2_done = _status_paths(seg2)
    primary.safe_psql(
        "INSERT INTO mine SELECT generate_series(10,20) AS x;\nCHECKPOINT;"
    )
    primary_lsn = primary.safe_psql("SELECT pg_switch_wal();")
    assert primary.poll_query_until(
        "SELECT last_archived_wal FROM pg_stat_archiver", expected=seg2
    ), "archiving to finish"

    _test_standby_on(create_pg, primary, primary_lsn, seg1_ready, seg2_ready, seg2_done)
    _test_standby_always(
        create_pg,
        primary,
        primary_lsn,
        seg1_ready,
        seg2_ready,
        seg1_done,
        seg2_done,
        seg2,
    )
    _test_backup_mode(primary)


def _test_standby_on(
    create_pg, primary, primary_lsn, seg1_ready, seg2_ready, seg2_done
):
    standby1 = create_pg(
        "standby", from_backup=(primary, "backup"), has_restoring=True, start=False
    )
    standby1.append_conf("archive_mode = on")
    standby1.start()
    data = standby1.datadir
    assert standby1.poll_query_until(
        "SELECT pg_wal_lsn_diff(pg_last_wal_replay_lsn(), '{}') >= 0".format(
            primary_lsn
        )
    ), "xlog replay on standby1"
    standby1.safe_psql("CHECKPOINT")

    assert not (
        data / seg1_ready
    ).is_file(), "inherited .ready removed (archive_mode=on)"
    assert not (data / seg2_ready).is_file(), ".ready not created (archive_mode=on)"
    assert (data / seg2_done).is_file(), ".done created (archive_mode=on)"


def _test_standby_always(
    create_pg, primary, primary_lsn, seg1_ready, seg2_ready, seg1_done, seg2_done, seg2
):
    standby2 = create_pg(
        "standby2", from_backup=(primary, "backup"), has_restoring=True, start=False
    )
    standby2.append_conf("archive_mode = always")
    standby2.start()
    data = standby2.datadir
    assert standby2.poll_query_until(
        "SELECT pg_wal_lsn_diff(pg_last_wal_replay_lsn(), '{}') >= 0".format(
            primary_lsn
        )
    ), "xlog replay on standby2"
    standby2.safe_psql("CHECKPOINT")

    assert (data / seg1_ready).is_file(), "inherited .ready kept (archive_mode=always)"
    assert (data / seg2_ready).is_file(), ".ready created (archive_mode=always)"

    standby2.safe_psql("SELECT pg_stat_reset_shared('archiver')")

    # Crash recovery must not remove non-archived WAL segments.
    standby2.stop("immediate")
    standby2.start()
    assert (data / seg1_ready).is_file(), "WAL still ready to archive after crash"

    standby2.safe_psql("ALTER SYSTEM RESET archive_command;\nSELECT pg_reload_conf();")
    assert standby2.poll_query_until(
        "SELECT last_archived_wal FROM pg_stat_archiver", expected=seg2
    ), "archiving to finish"
    assert (
        standby2.safe_psql("SELECT archived_count FROM pg_stat_archiver") == "2"
    ), "correct number of WAL segments archived from standby"
    assert (
        not (data / seg1_ready).is_file() and not (data / seg2_ready).is_file()
    ), ".ready files removed after archive success (archive_mode=always)"
    assert (data / seg1_done).is_file() and (
        data / seg2_done
    ).is_file(), ".done files created after archive success (archive_mode=always)"

    # The archiver calls the shell archive module's shutdown callback.
    standby2.append_conf("log_min_messages = debug1")
    standby2.reload()
    standby2.safe_psql("SELECT 1")
    log_location = standby2.current_log_position()
    standby2.stop()
    assert re.search(
        r"archiver process shutting down", pypg.slurp_file(standby2.log, log_location)
    ), "check shutdown callback of shell archive module"


def _test_backup_mode(primary):
    # Enter/leave backup mode without crashes; a too-long label fails cleanly.
    result = primary.psql_capture(
        "SELECT pg_backup_start('onebackup'); "
        "SELECT pg_backup_stop();"
        "SELECT pg_backup_start(repeat('x', 1026))"
    )
    assert result.rc == 3, "psql fails correctly"
    assert re.search(
        r"backup label too long", result.stderr
    ), "pg_backup_start fails gracefully"
    primary.safe_psql("SELECT pg_backup_start('onebackup'); SELECT pg_backup_stop();")
    primary.safe_psql("SELECT pg_backup_start('twobackup')")

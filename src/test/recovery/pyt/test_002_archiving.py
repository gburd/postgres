# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/recovery/t/002_archiving.pl.

WAL archiving with a hot standby, archive_cleanup_command/recovery_end_command,
and removal of recovery temp files and signal files at end of recovery.
"""

import re

import pypg


def test_archiving(create_pg):
    """A standby restores from archives; recovery commands and temp files behave."""
    primary = create_pg("primary", has_archiving=True, allows_streaming=True)
    backup_name = "my_backup"
    primary.backup(backup_name)

    # Standby restoring from the primary's archives (not streaming).
    standby = create_pg(
        "standby", from_backup=(primary, backup_name), has_restoring=True, start=False
    )
    standby.append_conf("wal_retrieve_retry_interval = '100ms'")

    data_dir = standby.datadir
    cleanup_file = "archive_cleanup_command.done"
    recovery_end_file = "recovery_end_command.done"
    standby.append_conf(
        "archive_cleanup_command = 'echo archive_cleanup_done > {}'\n"
        "recovery_end_command = 'echo recovery_ended_done > {}'".format(
            cleanup_file, recovery_end_file
        )
    )
    standby.start()

    primary.safe_psql("CREATE TABLE tab_int AS SELECT generate_series(1,1000) AS a")
    primary.safe_psql("CHECKPOINT")
    current_lsn = primary.safe_psql("SELECT pg_current_wal_lsn();")
    primary.safe_psql("SELECT pg_switch_wal()")
    primary.safe_psql("INSERT INTO tab_int VALUES (generate_series(1001,2000))")

    assert standby.poll_query_until(
        "SELECT '{}'::pg_lsn <= pg_last_wal_replay_lsn()".format(current_lsn)
    ), "standby to catch up"
    assert (
        standby.safe_psql("SELECT count(*) FROM tab_int") == "1000"
    ), "check content from archives"

    # archive_cleanup_command runs after a restartpoint (checkpoint).
    standby.safe_psql("CHECKPOINT")
    assert (data_dir / cleanup_file).is_file(), "archive_cleanup_command executed"
    assert not (data_dir / recovery_end_file).is_file(), "recovery_end_command not yet"

    # Promote, forcing a timeline switch and archiving of the history file.
    standby.promote()
    primary_archive = primary.archive_dir
    assert primary.poll_query_until(
        "SELECT size IS NOT NULL FROM "
        "pg_stat_file('{}/00000002.history', true)".format(primary_archive)
    ), "archiving of 00000002.history"

    assert (
        data_dir / recovery_end_file
    ).is_file(), "recovery_end_command after promote"

    standby2 = create_pg(
        "standby2", from_backup=(primary, backup_name), has_restoring=True, start=False
    )
    # Make recovery_end_command fail; promotion should be unaffected.
    standby2.append_conf(
        "recovery_end_command = 'echo recovery_end_failed > missing_dir/xyz.file'"
    )

    # With both recovery.signal and standby.signal present, standby.signal wins
    # and both are removed at the end of recovery.
    standby2.set_recovery_mode()
    standby2_data = standby2.datadir
    assert (standby2_data / "recovery.signal").is_file(), "recovery.signal present"
    assert (standby2_data / "standby.signal").is_file(), "standby.signal present"

    standby2.start()
    log_location = standby2.current_log_position()

    standby2.promote()

    log_contents = pypg.slurp_file(standby2.log, log_location)
    assert re.search(
        r'(?s)restored log file "00000002.history" from archive', log_contents
    ), "00000002.history retrieved from the archives"
    assert not (
        standby2_data / "pg_wal" / "RECOVERYHISTORY"
    ).is_file(), "RECOVERYHISTORY removed after promotion"
    assert not (
        standby2_data / "pg_wal" / "RECOVERYXLOG"
    ).is_file(), "RECOVERYXLOG removed after promotion"
    assert re.search(
        r"(?s)WARNING:.*recovery_end_command", log_contents
    ), "recovery_end_command failure detected in logs after promotion"

    assert not (
        standby2_data / "recovery.signal"
    ).is_file(), "recovery.signal was left behind after promotion"
    assert not (
        standby2_data / "standby.signal"
    ).is_file(), "standby.signal was left behind after promotion"

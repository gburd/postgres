# Copyright (c) 2024-2026, PostgreSQL Global Development Group

"""Port of src/test/recovery/t/045_archive_restartpoint.pl.

Test restartpoints during archive recovery.
"""

_ARCHIVE_MAX_MB = 320
_WAL_SEGSIZE = 1


def test_archive_restartpoint(create_pg):
    """A restore replays past a recovery target across many WAL segments."""
    primary = create_pg(
        "primary",
        has_archiving=True,
        allows_streaming=True,
        extra=["--wal-segsize", str(_WAL_SEGSIZE)],
    )
    backup_name = "my_backup"
    primary.backup(backup_name)

    primary.safe_psql(
        "DO $$BEGIN FOR i IN 1..{} LOOP CHECKPOINT; PERFORM pg_switch_wal(); "
        "END LOOP; END$$;".format(_ARCHIVE_MAX_MB // _WAL_SEGSIZE)
    )

    # Force archiving of the WAL file containing the recovery target.
    until_lsn = primary.lsn("write")
    primary.safe_psql("SELECT pg_switch_wal()")
    primary.stop()

    restore = create_pg(
        "restore", from_backup=(primary, backup_name), has_restoring=True, start=False
    )
    restore.append_conf("recovery_target_lsn = '{}'".format(until_lsn))
    restore.append_conf("recovery_target_action = pause")
    restore.append_conf("max_wal_size = {}".format(2 * _WAL_SEGSIZE))
    restore.append_conf("log_checkpoints = on")
    restore.start()

    assert restore.poll_query_until(
        "SELECT '{}'::pg_lsn <= pg_last_wal_replay_lsn()".format(until_lsn)
    ), "restore caught up"
    restore.stop()

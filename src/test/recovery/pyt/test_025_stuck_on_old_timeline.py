# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/recovery/t/025_stuck_on_old_timeline.pl.

A cascading standby that restores WAL from the primary's archive (and streams
from an intermediate standby) must follow a timeline switch when the
intermediate standby is promoted: after promotion and a WAL switch, new content
written on the promoted node replicates through to the cascade standby.
"""


def test_025_stuck_on_old_timeline(create_pg):
    """A cascade standby follows the timeline switch via archive + streaming."""
    primary = create_pg(
        "primary", allows_streaming=True, has_archiving=True, start=False
    )
    primary.append_conf("\nwal_keep_size=128MB\n")
    primary.start()
    backup_name = "my_backup"
    primary.backup(backup_name)
    standby = create_pg(
        "standby",
        from_backup=(primary, backup_name),
        allows_streaming=True,
        has_streaming=True,
        has_archiving=True,
        start=False,
    )
    standby.start()
    standby.backup(backup_name, backup_options=["-Xnone"])
    cascade = create_pg(
        "cascade", from_backup=(standby, backup_name), has_streaming=True, start=False
    )
    cascade.enable_restoring(primary)
    cascade.append_conf("\nrecovery_target_timeline='latest'\n")
    standby.promote()
    assert standby.poll_query_until(
        "SELECT NOT pg_is_in_recovery();"
    ), "Timed out while waiting for promotion"
    walfile_to_be_archived = standby.safe_psql(
        "SELECT pg_walfile_name(pg_current_wal_lsn());"
    )
    standby.safe_psql("SELECT pg_switch_wal()")
    assert standby.poll_query_until(
        "SELECT '{}' <= last_archived_wal FROM pg_stat_archiver".format(
            walfile_to_be_archived
        )
    ), "Timed out while waiting for WAL segment to be archived"
    cascade.start()
    standby.safe_psql("CREATE TABLE tab_int AS SELECT 1 AS a")
    standby.wait_for_catchup(cascade)
    result = cascade.safe_psql("SELECT count(*) FROM tab_int")
    assert result == "1", "check streamed content on cascade standby"
    cascade.stop()
    standby.stop()
    primary.stop()

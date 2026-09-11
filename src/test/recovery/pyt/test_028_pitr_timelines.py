# Copyright (c) 2022-2026, PostgreSQL Global Development Group

"""Port of src/test/recovery/t/028_pitr_timelines.pl.

PITR to a target physically located in a WAL segment with a higher TLI than
the target point's TLI: recovery finds the WAL but does not follow the timeline
switch, creating a TLI 1 -> 3 end-of-recovery record.
"""


def test_pitr_timelines(create_pg):
    """PITR across a segment that also holds a later timeline's switch."""
    primary = create_pg("primary", has_archiving=True, allows_streaming=True)
    primary.backup("my_backup")

    primary.psql_capture(
        "CREATE TABLE foo(i int);\n"
        "INSERT INTO foo VALUES(1);\n"
        "SELECT pg_create_restore_point('rp');\n"
        "INSERT INTO foo VALUES(2);"
    )

    standby = create_pg(
        "standby",
        from_backup=(primary, "my_backup"),
        has_streaming=True,
        has_archiving=True,
        start=False,
    )
    standby.append_conf("archive_mode = always")
    standby.start()
    primary.wait_for_catchup(standby)
    assert (
        standby.safe_psql("SELECT max(i) FROM foo;") == "2"
    ), "check table contents after archive recovery"

    # Kill the primary before it archives the segment with all the INSERTs.
    primary.stop("immediate")

    # Promote and switch WAL so the segment is archived on a new timeline.
    standby.promote()
    standby.safe_psql("SELECT pg_walfile_name(pg_current_wal_lsn());")
    standby.safe_psql("SELECT pg_switch_wal()")
    standby.stop()

    # PITR to the restore point: finds the WAL in the TLI-2 segment but does
    # not follow the timeline switch.
    node_pitr = create_pg(
        "node_pitr",
        from_backup=(primary, "my_backup"),
        standby=False,
        has_restoring=True,
        start=False,
    )
    node_pitr.append_conf(
        "recovery_target_name = 'rp'\nrecovery_target_action = 'promote'"
    )
    node_pitr.start()
    assert node_pitr.poll_query_until(
        "SELECT pg_is_in_recovery() = 'f';"
    ), "PITR promotion finished"
    assert (
        node_pitr.safe_psql("SELECT max(i) FROM foo;") == "1"
    ), "check table contents after point-in-time recovery"

    node_pitr.safe_psql("INSERT INTO foo VALUES(3);")
    # Ensure the archiver is running before stopping, so the archive completes.
    assert node_pitr.poll_query_until(
        "SELECT true FROM pg_stat_activity WHERE backend_type = 'archiver';"
    ), "archiver started"
    node_pitr.stop()

    # Archive recovery on the PITR-created timeline replays the TLI 1 -> 3
    # end-of-recovery record.
    node_pitr2 = create_pg(
        "node_pitr2",
        from_backup=(primary, "my_backup"),
        standby=False,
        has_restoring=True,
        start=False,
    )
    node_pitr2.append_conf("recovery_target_action = 'promote'")
    node_pitr2.start()
    assert node_pitr2.poll_query_until(
        "SELECT pg_is_in_recovery() = 'f';"
    ), "PITR promotion finished"
    assert (
        node_pitr2.safe_psql("SELECT max(i) FROM foo;") == "3"
    ), "check table contents after point-in-time recovery"

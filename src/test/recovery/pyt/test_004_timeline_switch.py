# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/recovery/t/004_timeline_switch.pl.

A cascading standby must be able to follow a newly-promoted standby on a new
timeline.
"""


def test_timeline_switch(create_pg):
    """Cascading standby follows a promoted standby across a timeline switch."""
    primary = create_pg("primary", allows_streaming=True)

    backup_name = "my_backup"
    primary.backup(backup_name)

    standby_1 = create_pg(
        "standby_1", from_backup=(primary, backup_name), has_streaming=True
    )
    standby_2 = create_pg(
        "standby_2", from_backup=(primary, backup_name), has_streaming=True
    )

    primary.safe_psql("CREATE TABLE tab_int AS SELECT generate_series(1,1000) AS a")

    # A clean stop ensures both standbys received and flushed all records.
    primary.stop()

    # Promote standby 1, switching it to a new timeline.
    assert (
        standby_1.safe_psql("SELECT pg_promote(wait_seconds => 300)") == "t"
    ), "promotion of standby with pg_promote"

    # Switch standby 2 to replay from standby 1. The WAL receiver should stay
    # alive across the switch and the new conninfo must not leak.
    secret = "dont_show_me"
    connstr_1 = standby_1.connstr()
    standby_2.append_conf("primary_conninfo='{} password={}'".format(connstr_1, secret))

    # Rotate the logfile before restarting, for the log checks below.
    standby_2.rotate_logfile()
    standby_2.restart()

    # Wait for the walreceiver to reconnect after the restart.
    assert standby_2.poll_query_until(
        "SELECT EXISTS(SELECT 1 FROM pg_stat_wal_receiver)"
    )
    wr_pid_before_switch = standby_2.safe_psql("SELECT pid FROM pg_stat_wal_receiver")

    standby_1.safe_psql("INSERT INTO tab_int VALUES (generate_series(1001,2000))")
    standby_1.wait_for_catchup(standby_2)

    assert (
        standby_2.safe_psql("SELECT count(*) FROM tab_int") == "2000"
    ), "check content of standby 2"

    # The WAL receiver should not have been stopped while switching timelines.
    assert not standby_2.log_matches(
        "FATAL: .* terminating walreceiver process due to administrator command"
    ), "WAL receiver should not be stopped across timeline jumps"

    wr_pid_after_switch = standby_2.safe_psql("SELECT pid FROM pg_stat_wal_receiver")
    assert (
        wr_pid_before_switch == wr_pid_after_switch
    ), "WAL receiver PID matches across timeline jumps"

    raw_conninfo_count = standby_2.safe_psql(
        "SELECT count(*) FROM pg_stat_wal_receiver "
        "WHERE conninfo LIKE '%{}%'".format(secret)
    )
    assert (
        raw_conninfo_count == "0"
    ), "pg_stat_wal_receiver.conninfo not updated across timeline jumps"

    _test_archiving_timeline(create_pg, backup_name)


def _test_archiving_timeline(create_pg, backup_name):
    """A standby follows a primary on a newer timeline with WAL archiving on."""
    primary_2 = create_pg(
        "primary_2", allows_streaming=True, has_archiving=True, start=False
    )
    primary_2.append_conf("wal_keep_size = 512MB")
    primary_2.start()

    primary_2.backup(backup_name)

    standby_3 = create_pg(
        "standby_3",
        from_backup=(primary_2, backup_name),
        has_streaming=True,
        start=False,
    )

    # Restart the primary in standby mode and promote it, onto a new timeline.
    primary_2.set_standby_mode()
    primary_2.restart()
    primary_2.promote()

    standby_3.start()
    primary_2.safe_psql("CREATE TABLE tab_int AS SELECT 1 AS a")
    primary_2.wait_for_catchup(standby_3)

    assert (
        standby_3.safe_psql("SELECT count(*) FROM tab_int") == "1"
    ), "check content of standby 3"

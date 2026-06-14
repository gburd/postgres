# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/recovery/t/005_replay_delay.pl.

Checks recovery_min_apply_delay and recovery pause.
"""

import time


def test_replay_delay(create_pg):
    """A standby honors recovery_min_apply_delay; recovery can pause/resume."""
    primary = create_pg("primary", allows_streaming=True)
    primary.safe_psql("CREATE TABLE tab_int AS SELECT generate_series(1, 10) AS a")

    backup_name = "my_backup"
    primary.backup(backup_name)

    delay = 3
    standby = create_pg(
        "standby", from_backup=(primary, backup_name), has_streaming=True, start=False
    )
    standby.append_conf("recovery_min_apply_delay = '{}s'".format(delay))
    standby.start()

    # Record a base timestamp just before the insertion so the delay comparison
    # is predictable even on slow machines.
    primary_insert_time = time.time()
    primary.safe_psql("INSERT INTO tab_int VALUES (generate_series(11, 20))")

    until_lsn = primary.safe_psql("SELECT pg_current_wal_lsn()")
    assert standby.poll_query_until(
        "SELECT (pg_last_wal_replay_lsn() - '{}'::pg_lsn) >= 0".format(until_lsn)
    ), "standby never caught up"

    assert (
        time.time() - primary_insert_time >= delay
    ), "standby applies WAL only after replication delay"

    # Check that recovery can be paused or resumed as expected.
    standby2 = create_pg(
        "standby2", from_backup=(primary, backup_name), has_streaming=True
    )

    assert (
        standby2.safe_psql("SELECT pg_get_wal_replay_pause_state()") == "not paused"
    ), "pg_get_wal_replay_pause_state() reports not paused"

    standby2.safe_psql("SELECT pg_wal_replay_pause()")
    primary.safe_psql("INSERT INTO tab_int VALUES (generate_series(21,30))")
    assert standby2.poll_query_until(
        "SELECT pg_get_wal_replay_pause_state() = 'paused'"
    ), "recovery to be paused"

    # Even if new WAL records stream from the primary, the paused state doesn't
    # replay them.
    receive_lsn = standby2.safe_psql("SELECT pg_last_wal_receive_lsn()")
    replay_lsn = standby2.safe_psql("SELECT pg_last_wal_replay_lsn()")
    primary.safe_psql("INSERT INTO tab_int VALUES (generate_series(31,40))")
    assert standby2.poll_query_until(
        "SELECT '{}'::pg_lsn < pg_last_wal_receive_lsn()".format(receive_lsn)
    ), "new WAL to be streamed"
    assert (
        standby2.safe_psql("SELECT pg_last_wal_replay_lsn()") == replay_lsn
    ), "no WAL is replayed in the paused state"

    # Resume recovery and wait until it's actually resumed.
    standby2.safe_psql("SELECT pg_wal_replay_resume()")
    assert standby2.poll_query_until(
        "SELECT pg_get_wal_replay_pause_state() = 'not paused' "
        "AND pg_last_wal_replay_lsn() > '{}'::pg_lsn".format(replay_lsn)
    ), "recovery to be resumed"

    # A promotion while paused should end the paused state and continue.
    standby2.safe_psql("SELECT pg_wal_replay_pause()")
    primary.safe_psql("INSERT INTO tab_int VALUES (generate_series(41,50))")
    assert standby2.poll_query_until(
        "SELECT pg_get_wal_replay_pause_state() = 'paused'"
    ), "recovery to be paused"

    standby2.promote()
    assert standby2.poll_query_until(
        "SELECT NOT pg_is_in_recovery()"
    ), "promotion to finish"

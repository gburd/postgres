# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/recovery/t/007_sync_rep.pl.

Minimal test of synchronous replication sync_state transitions.
"""

# Query checking sync_priority and sync_state of each standby.
_CHECK_SQL = (
    "SELECT application_name, sync_priority, sync_state FROM pg_stat_replication "
    "ORDER BY application_name;"
)


def _test_sync_state(primary, expected, msg, setting=None):
    if setting is not None:
        primary.safe_psql(
            "ALTER SYSTEM SET synchronous_standby_names = '{}';".format(setting)
        )
        primary.reload()
    assert primary.poll_query_until(_CHECK_SQL, expected=expected), msg


def _start_standby_and_wait(primary, standby):
    standby.start()
    assert primary.poll_query_until(
        "SELECT count(1) = 1 FROM pg_stat_replication "
        "WHERE application_name = '{}'".format(standby.name)
    ), 'standby "{}" registered'.format(standby.name)


def test_sync_rep(create_pg):
    """sync_state is determined correctly across synchronous_standby_names."""
    primary = create_pg("primary", allows_streaming=True)
    backup_name = "primary_backup"
    primary.backup(backup_name)

    def standby(name):
        return create_pg(
            name, from_backup=(primary, backup_name), has_streaming=True, start=False
        )

    standby1 = standby("standby1")
    _start_standby_and_wait(primary, standby1)
    standby2 = standby("standby2")
    _start_standby_and_wait(primary, standby2)
    standby3 = standby("standby3")
    _start_standby_and_wait(primary, standby3)

    _test_sync_state(
        primary,
        "standby1|1|sync\nstandby2|2|potential\nstandby3|0|async",
        "old syntax of synchronous_standby_names",
        "standby1,standby2",
    )
    _test_sync_state(
        primary,
        "standby1|1|sync\nstandby2|1|potential\nstandby3|1|potential",
        "asterisk in synchronous_standby_names",
        "*",
    )

    # Rearrange the order of standbys in the WalSnd array.
    standby1.stop()
    standby2.stop()
    standby3.stop()
    _start_standby_and_wait(primary, standby2)
    _start_standby_and_wait(primary, standby3)

    _test_sync_state(
        primary,
        "standby2|2|sync\nstandby3|3|sync",
        "2 synchronous standbys",
        "2(standby1,standby2,standby3)",
    )

    _start_standby_and_wait(primary, standby1)

    standby4 = standby("standby4")
    standby4.start()

    _test_sync_state(
        primary,
        "standby1|1|sync\nstandby2|2|sync\nstandby3|3|potential\nstandby4|0|async",
        "2 sync, 1 potential, and 1 async",
    )
    _test_sync_state(
        primary,
        "standby1|0|async\nstandby2|4|sync\nstandby3|3|sync\nstandby4|1|sync",
        "num_sync exceeds the num of potential sync standbys",
        "6(standby4,standby0,standby3,standby2)",
    )
    _test_sync_state(
        primary,
        "standby1|1|sync\nstandby2|2|sync\nstandby3|2|potential\nstandby4|2|potential",
        "asterisk before another standby name",
        "2(standby1,*,standby2)",
    )
    _test_sync_state(
        primary,
        "standby1|1|potential\nstandby2|1|sync\nstandby3|1|sync\nstandby4|1|potential",
        "multiple standbys having the same priority are chosen as sync",
        "2(*)",
    )

    standby3.stop()
    _test_sync_state(
        primary,
        "standby1|1|sync\nstandby2|1|sync\nstandby4|1|potential",
        "potential standby found earlier in array is promoted to sync",
    )
    _test_sync_state(
        primary,
        "standby1|1|sync\nstandby2|2|sync\nstandby4|0|async",
        "priority-based sync replication specified by FIRST keyword",
        "FIRST 2(standby1, standby2)",
    )
    _test_sync_state(
        primary,
        "standby1|1|quorum\nstandby2|1|quorum\nstandby4|0|async",
        "2 quorum and 1 async",
        "ANY 2(standby1, standby2)",
    )

    standby3.start()
    _test_sync_state(
        primary,
        "standby1|1|quorum\nstandby2|1|quorum\nstandby3|1|quorum\nstandby4|1|quorum",
        "all standbys are considered as candidates for quorum sync standbys",
        "ANY 2(*)",
    )

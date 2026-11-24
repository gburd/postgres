# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/recovery/t/003_recovery_targets.pl.

Tests for recovery targets: name, timestamp, XID, LSN, immediate, and invalid
configurations.
"""

import os
import re
import time

import pypg


def _test_recovery_standby(
    pg_bin,
    create_pg,
    test_name,
    node_name,
    primary,
    recovery_params,
    num_rows,
    until_lsn,
):
    """Create a restoring standby with a recovery target and check its content."""
    standby = create_pg(
        node_name, from_backup=(primary, "my_backup"), has_restoring=True, start=False
    )
    for param in recovery_params:
        standby.append_conf(param)
    standby.start()

    assert standby.poll_query_until(
        "SELECT '{}'::pg_lsn <= pg_last_wal_replay_lsn()".format(until_lsn)
    ), "standby to catch up"

    result = standby.safe_psql("SELECT count(*) FROM tab_int")
    assert result == num_rows, "check standby content for {}".format(test_name)

    standby.stop()


def _make_primary_data(primary):
    """Create the WAL history with named restore points and return the markers."""
    primary.safe_psql("CREATE TABLE tab_int AS SELECT generate_series(1,1000) AS a")
    lsn1 = primary.safe_psql("SELECT pg_current_wal_lsn();")
    primary.backup("my_backup")

    primary.safe_psql("INSERT INTO tab_int VALUES (generate_series(1001,2000))")
    lsn2, recovery_txid = primary.safe_psql(
        "SELECT pg_current_wal_lsn(), pg_current_xact_id();"
    ).split("|")

    primary.safe_psql("INSERT INTO tab_int VALUES (generate_series(2001,3000))")
    lsn3 = primary.safe_psql("SELECT pg_current_wal_lsn();")
    recovery_time = primary.safe_psql("SELECT now()")

    primary.safe_psql("INSERT INTO tab_int VALUES (generate_series(3001,4000))")
    recovery_name = "my_target"
    lsn4 = primary.safe_psql("SELECT pg_current_wal_lsn();")
    primary.safe_psql("SELECT pg_create_restore_point('{}');".format(recovery_name))

    primary.safe_psql("INSERT INTO tab_int VALUES (generate_series(4001,5000))")
    recovery_lsn = primary.safe_psql("SELECT pg_current_wal_lsn()")

    primary.safe_psql("INSERT INTO tab_int VALUES (generate_series(5001,6000))")
    primary.safe_psql("SELECT pg_switch_wal()")

    return {
        "lsn1": lsn1,
        "lsn2": lsn2,
        "recovery_txid": recovery_txid,
        "lsn3": lsn3,
        "recovery_time": recovery_time,
        "recovery_name": recovery_name,
        "lsn4": lsn4,
        "recovery_lsn": recovery_lsn,
    }


def test_recovery_targets(pg_bin, create_pg):
    """Recovery to each kind of target, and invalid-target handling."""
    primary = create_pg(
        "primary", has_archiving=True, allows_streaming=True, start=False
    )
    # Bump the transaction ID epoch to stress recovery_target_xid parsing.
    pg_bin.command_ok(["pg_resetwal", "--epoch", "1", primary.datadir])
    primary.start()

    m = _make_primary_data(primary)

    _test_recovery_standby(
        pg_bin,
        create_pg,
        "immediate target",
        "standby_1",
        primary,
        ["recovery_target = 'immediate'"],
        "1000",
        m["lsn1"],
    )
    _test_recovery_standby(
        pg_bin,
        create_pg,
        "XID",
        "standby_2",
        primary,
        ["recovery_target_xid = '{}'".format(m["recovery_txid"])],
        "2000",
        m["lsn2"],
    )
    _test_recovery_standby(
        pg_bin,
        create_pg,
        "time",
        "standby_3",
        primary,
        ["recovery_target_time = '{}'".format(m["recovery_time"])],
        "3000",
        m["lsn3"],
    )
    _test_recovery_standby(
        pg_bin,
        create_pg,
        "name",
        "standby_4",
        primary,
        ["recovery_target_name = '{}'".format(m["recovery_name"])],
        "4000",
        m["lsn4"],
    )
    _test_recovery_standby(
        pg_bin,
        create_pg,
        "LSN",
        "standby_5",
        primary,
        ["recovery_target_lsn = '{}'".format(m["recovery_lsn"])],
        "5000",
        m["recovery_lsn"],
    )

    # Multiple overriding settings are allowed (last one wins).
    _test_recovery_standby(
        pg_bin,
        create_pg,
        "multiple overriding settings",
        "standby_6",
        primary,
        [
            "recovery_target_name = '{}'".format(m["recovery_name"]),
            "recovery_target_name = ''",
            "recovery_target_time = '{}'".format(m["recovery_time"]),
        ],
        "3000",
        m["lsn3"],
    )

    _test_conflicting_targets(pg_bin, create_pg, primary, m)
    _test_recovery_ends_early(pg_bin, create_pg, primary)
    _test_invalid_target_gucs(primary)


def _test_conflicting_targets(pg_bin, create_pg, primary, m):
    standby = create_pg(
        "standby_7", from_backup=(primary, "my_backup"), has_restoring=True, start=False
    )
    standby.append_conf(
        "recovery_target_name = '{}'\nrecovery_target_time = '{}'".format(
            m["recovery_name"], m["recovery_time"]
        )
    )
    result = pg_bin.result(
        ["pg_ctl", "--pgdata", standby.datadir, "--log", standby.log, "start"]
    )
    assert result.exit_code != 0, "invalid recovery startup fails"
    assert re.search(
        r"multiple recovery targets specified", pypg.slurp_file(standby.log)
    ), "multiple conflicting settings"


def _test_recovery_ends_early(pg_bin, create_pg, primary):
    standby = create_pg(
        "standby_8",
        from_backup=(primary, "my_backup"),
        has_restoring=True,
        standby=False,
        start=False,
    )
    standby.append_conf("recovery_target_name = 'does_not_exist'")
    pg_bin.result(
        ["pg_ctl", "--pgdata", standby.datadir, "--log", standby.log, "start"]
    )

    # Wait for postgres to terminate.
    pidfile = standby.datadir / "postmaster.pid"
    for _ in range(10 * int(os.environ.get("PG_TEST_TIMEOUT_DEFAULT", "180"))):
        if not pidfile.is_file():
            break
        time.sleep(0.1)

    assert re.search(
        r"FATAL: .* recovery ended before configured recovery target was reached",
        pypg.slurp_file(standby.log),
    ), "recovery end before target reached is a fatal error"


def _test_invalid_target_gucs(primary):
    cases = [
        ("recovery_target_timeline", "bogus", r"is not a valid number"),
        ("recovery_target_timeline", "0", r"must be between 1 and 4294967295"),
        ("recovery_target_timeline", "4294967296", r"must be between 1 and 4294967295"),
        ("recovery_target_xid", "bogus", r"is not a valid number"),
        ("recovery_target_xid", "-1", r"is not a valid number"),
        (
            "recovery_target_xid",
            "0",
            r"without epoch must be greater than or equal to 3",
        ),
    ]
    for guc, value, pattern in cases:
        result = primary.bin.result(
            ["psql", "-c", "ALTER SYSTEM SET {} TO '{}'".format(guc, value)]
        )
        assert re.search(pattern, result.stderr), "invalid {} ({})".format(guc, value)

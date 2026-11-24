# Copyright (c) 2025-2026, PostgreSQL Global Development Group

"""Port of src/test/recovery/t/051_effective_wal_level.pl.

Exercises the effective_wal_level machinery: with wal_level='replica', creating
a logical slot raises effective_wal_level to 'logical' (and dropping the last
one lowers it back). Covers persistence across restart, refusal to start at
wal_level='minimal' with a live logical slot, slot invalidation via
max_slot_wal_keep_size, and propagation of effective_wal_level across
standby/cascade promotions.
"""

import re


def _test_wal_level(node, expected, msg):
    assert (
        node.safe_psql(
            "select current_setting('wal_level'), "
            "current_setting('effective_wal_level');"
        )
        == expected
    ), msg


def _wait_logical_decoding_disabled(node):
    assert node.poll_query_until(
        "select current_setting('effective_wal_level') = 'replica';"
    )


def test_051_effective_wal_level(create_pg, pg_bin):
    """effective_wal_level tracks logical slots and propagates across promotion."""
    primary = create_pg("primary", allows_streaming=True, start=False)
    primary.append_conf("log_min_messages = debug1")
    primary.start()
    _test_wal_level(
        primary,
        "replica|replica",
        "wal_level and effective_wal_level start at 'replica'",
    )
    primary.safe_psql(
        "select pg_create_physical_replication_slot('test_phy_slot', false, false)"
    )
    _test_wal_level(
        primary,
        "replica|replica",
        "effective_wal_level unchanged with a new physical slot",
    )
    primary.safe_psql("select pg_drop_replication_slot('test_phy_slot')")
    primary.safe_psql(
        "select pg_create_logical_replication_slot('test_tmp_slot', "
        "'test_decoding', true)"
    )
    assert primary.log_contains(
        "logical decoding is enabled upon creating a new logical replication slot"
    ), "logical decoding enabled upon creating a temp slot"
    _wait_logical_decoding_disabled(primary)
    primary.safe_psql("create table foo(a int primary key)")
    primary.safe_psql("repack (concurrently) foo;")
    assert primary.log_contains(
        "logical decoding is enabled upon creating a new logical replication slot"
    ), "logical decoding enabled by repack"
    _wait_logical_decoding_disabled(primary)
    _test_wal_level(
        primary, "replica|replica", "logical decoding disabled after repack"
    )
    primary.safe_psql(
        "select pg_create_logical_replication_slot('test_slot', 'pgoutput')"
    )
    _test_wal_level(
        primary,
        "replica|logical",
        "effective_wal_level increased to 'logical' on a logical slot",
    )
    primary.restart()
    _test_wal_level(
        primary,
        "replica|logical",
        "effective_wal_level remains 'logical' after restart",
    )
    primary.safe_psql(
        "select pg_create_logical_replication_slot('test_slot2', 'pgoutput')"
    )
    primary.safe_psql("select pg_drop_replication_slot('test_slot2')")
    _test_wal_level(
        primary,
        "replica|logical",
        "effective_wal_level stays 'logical' as one slot remains",
    )
    _minimal_refusal(primary, pg_bin)
    _invalidation_and_propagation(primary, create_pg)


def _minimal_refusal(primary, pg_bin):
    import pypg  # pylint: disable=import-outside-toplevel

    primary.adjust_conf("wal_level", "minimal")
    primary.adjust_conf("max_wal_senders", "0")
    primary.stop()
    pg_bin.command_fails(
        [
            "pg_ctl",
            "--pgdata",
            str(primary.datadir),
            "--log",
            str(primary.log),
            "start",
        ],
        "cannot start with wal_level='minimal' and an in-use logical slot",
    )
    logfile = pypg.slurp_file(primary.log)
    assert re.search(
        r'logical replication slot "test_slot" exists, but "wal_level" < "replica"',
        logfile,
    ), "logical slots require logical decoding enabled at startup"
    primary.adjust_conf("wal_level", "replica")
    primary.adjust_conf("max_wal_senders", "10")
    primary.append_conf(
        "\nmin_wal_size = 32MB\nmax_wal_size = 32MB\nmax_slot_wal_keep_size = 16MB\n"
    )
    primary.start()
    primary.advance_wal(2)
    primary.safe_psql("CHECKPOINT")
    assert (
        primary.safe_psql(
            "select invalidation_reason = 'wal_removed' from pg_replication_slots "
            "where slot_name = 'test_slot';"
        )
        == "t"
    ), "test_slot invalidated due to wal_removed"
    _wait_logical_decoding_disabled(primary)
    _test_wal_level(
        primary,
        "replica|replica",
        "effective_wal_level decreased to 'replica' after invalidation",
    )
    primary.adjust_conf("max_slot_wal_keep_size", None)
    primary.adjust_conf("min_wal_size", None)
    primary.adjust_conf("max_wal_size", None)
    primary.restart()
    primary.safe_psql("select pg_drop_replication_slot('test_slot')")
    primary.safe_psql(
        "select pg_create_logical_replication_slot('test_slot', 'pgoutput')"
    )


def _invalidation_and_propagation(primary, create_pg):
    primary.backup("my_backup")
    standby1 = create_pg(
        "standby1", from_backup=(primary, "my_backup"), has_streaming=True, start=False
    )
    standby1.start()
    primary.wait_for_replay_catchup(standby1)
    standby1.create_logical_slot_on_standby(primary, "standby1_slot", "postgres")
    standby1.promote()
    _test_wal_level(
        standby1,
        "replica|logical",
        "effective_wal_level remains 'logical' after promotion",
    )
    standby1.safe_psql(
        "select pg_create_logical_replication_slot('standby1_slot2', 'pgoutput')"
    )
    standby1.stop()
    standby2 = create_pg(
        "standby2", from_backup=(primary, "my_backup"), has_streaming=True, start=False
    )
    standby2.append_conf("wal_level = 'logical'")
    standby2.start()
    standby2.backup("my_backup3")
    cascade = create_pg(
        "cascade", from_backup=(standby2, "my_backup3"), has_streaming=True, start=False
    )
    cascade.adjust_conf("wal_level", "replica")
    cascade.start()
    _test_wal_level(standby2, "logical|logical", "wal_levels on standby")
    _test_wal_level(cascade, "replica|logical", "wal_levels on cascaded standby")
    primary.safe_psql("select pg_drop_replication_slot('test_slot')")
    _wait_logical_decoding_disabled(primary)
    primary.wait_for_replay_catchup(standby2)
    standby2.wait_for_replay_catchup(cascade, primary)
    _test_wal_level(primary, "replica|replica", "effective_wal_level down on primary")
    _test_wal_level(standby2, "logical|replica", "effective_wal_level down on standby")
    _test_wal_level(cascade, "replica|replica", "effective_wal_level down on cascade")
    standby2.promote()
    standby2.wait_for_replay_catchup(cascade)
    _test_wal_level(
        cascade,
        "replica|logical",
        "effective_wal_level up on cascade after new primary is logical",
    )
    standby2.stop()
    cascade.stop()
    _standby3_invalidation(primary, create_pg)


def _standby3_invalidation(primary, create_pg):
    standby3 = create_pg(
        "standby3", from_backup=(primary, "my_backup"), has_streaming=True, start=False
    )
    standby3.start()
    primary.safe_psql(
        "select pg_create_logical_replication_slot('test_slot', 'pgoutput')"
    )
    primary.wait_for_replay_catchup(standby3)
    standby3.create_logical_slot_on_standby(primary, "standby3_slot", "postgres")
    primary.safe_psql("select pg_drop_replication_slot('test_slot')")
    _wait_logical_decoding_disabled(primary)
    _test_wal_level(
        primary,
        "replica|replica",
        "effective_wal_level down on primary to invalidate standby slots",
    )
    assert standby3.poll_query_until(
        "select invalidation_reason = 'wal_level_insufficient' "
        "from pg_replication_slots where slot_name = 'standby3_slot';"
    )
    standby3.stop()
    primary.stop()

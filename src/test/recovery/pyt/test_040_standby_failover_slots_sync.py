# Copyright (c) 2024-2026, PostgreSQL Global Development Group

"""Port of src/test/recovery/t/040_standby_failover_slots_sync.pl.

Failover replication-slot synchronization. A publisher (primary), a physical
standby of it, and subscribers. Logical slots created with failover=true on the
publisher are synchronized to the standby via pg_sync_replication_slots() and/or
the slot-sync worker (sync_replication_slots=on); synchronized_standby_slots
makes the publisher wait for the physical standby before logical replication to
the subscriber advances. Covers: a subscription created/altered WITH
(failover=...) toggles its slot's failover flag; pg_sync_replication_slots()
copies, drops, and re-creates synced slots on the standby (synced=true, matching
restart_lsn/confirmed_flush_lsn/two_phase); error/warning cases (sync on a
non-standby or cascading standby, missing dbname in primary_conninfo, decoding/
altering/dropping a synced slot, invalidation); the slot-sync worker lifecycle;
synchronized_standby_slots gating logical replication; promotion of the standby
letting the subscriber continue from the synced slots; and the
slotsync-skip-and-retry path with its statistics.
"""


PUB = "regress_mypub"
SUB1 = "regress_mysub1"
SUB2 = "regress_mysub2"
SLOT1 = "lsub1_slot"
SLOT2 = "lsub2_slot"


def _slot_field(node, slot_name, expr, dbname="postgres"):
    """Return a single pg_replication_slots field/expression for slot_name."""
    return node.safe_psql(
        "SELECT {} FROM pg_replication_slots "
        "WHERE slot_name = '{}';".format(expr, slot_name),
        dbname=dbname,
    )


def _expect_error(node, query, pattern, dbname="postgres", replication=None):
    """Assert that running query fails with stderr matching pattern."""
    res = node.psql_capture(query, dbname=dbname, replication=replication)
    assert pattern in res.stderr, "expected {!r} in stderr, got {!r}".format(
        pattern, res.stderr
    )


def _wait_for_synced_flush_lsn(standby, slot_name, target_lsn, what):
    """Poll until slot_name's confirmed_flush_lsn on standby equals target_lsn."""
    assert standby.poll_query_until(
        "SELECT '{lsn}' = confirmed_flush_lsn FROM pg_replication_slots "
        "WHERE slot_name = '{slot}' AND synced AND NOT temporary;".format(
            lsn=target_lsn, slot=slot_name
        )
    ), what


def _setup_publisher_subscriber(create_pg):
    """Create the publisher (primary) and the first subscriber node."""
    publisher = create_pg(
        "publisher",
        allows_streaming="logical",
        auth_extra=["--create-role", "repl_role"],
        start=False,
    )
    # Disable autovacuum to avoid generating xid during stats update as
    # otherwise the new XID could then be replicated to standby at some random
    # point making slots at primary lag behind standby during slot sync.
    publisher.append_conf("autovacuum = off\nmax_prepared_transactions = 1")
    publisher.start()
    publisher.safe_psql("CREATE PUBLICATION {} FOR ALL TABLES;".format(PUB))

    subscriber1 = create_pg("subscriber1", start=False)
    subscriber1.append_conf("max_prepared_transactions = 1")
    subscriber1.start()
    return publisher, subscriber1


def _test_failover_flag_toggling(publisher, subscriber1, connstr):
    """A subscription's failover option toggles the slot's failover flag, and
    cannot be changed while the subscription is enabled. Returns the captured
    slot-creation timestamp on the primary."""
    slot_creation_time = publisher.safe_psql("SELECT current_timestamp;")

    subscriber1.safe_psql(
        "CREATE SUBSCRIPTION {sub} CONNECTION '{conn}' PUBLICATION {pub} "
        "WITH (slot_name = {slot}, copy_data = false, failover = true, "
        "enabled = false);".format(sub=SUB1, conn=connstr, pub=PUB, slot=SLOT1)
    )
    assert (
        _slot_field(publisher, SLOT1, "failover") == "t"
    ), "logical slot has failover true on the publisher"

    subscriber1.safe_psql("ALTER SUBSCRIPTION {} SET (failover = false)".format(SUB1))
    assert (
        _slot_field(publisher, SLOT1, "failover") == "f"
    ), "logical slot has failover false on the publisher"

    subscriber1.safe_psql("ALTER SUBSCRIPTION {} SET (failover = true)".format(SUB1))
    assert (
        _slot_field(publisher, SLOT1, "failover") == "t"
    ), "logical slot has failover true on the publisher"

    subscriber1.safe_psql("ALTER SUBSCRIPTION {} ENABLE".format(SUB1))
    _expect_error(
        subscriber1,
        "ALTER SUBSCRIPTION {} SET (failover = false)".format(SUB1),
        'cannot set option "failover" for enabled subscription',
    )

    _expect_error(
        publisher,
        "SELECT pg_sync_replication_slots();",
        "replication slots can only be synchronized to a standby server",
    )
    return slot_creation_time


def _create_standby(create_pg, primary, name, slot_name):
    """Take a backup of primary and create a streaming+restoring standby."""
    backup_name = "backup_" + name
    primary.backup(backup_name)
    standby = create_pg(
        name,
        from_backup=(primary, backup_name),
        has_streaming=True,
        has_restoring=True,
        start=False,
    )
    standby.append_conf(
        "hot_standby_feedback = on\n"
        "primary_slot_name = '{slot}'\n"
        "primary_conninfo = '{conn} dbname=postgres'\n"
        "log_min_messages = 'debug2'".format(slot=slot_name, conn=primary.connstr())
    )
    return standby


def _test_sync_two_plugins(primary, standby1, subscriber1, slot_creation_time):
    """Two failover slots with different output plugins sync to the standby and
    are flagged synced; the synced slot gets its own inactive_since."""
    primary.append_conf("log_min_messages = 'debug2'")
    primary.reload()

    # Drop the subscription to prevent further advancement of restart_lsn for
    # lsub1_slot; re-create the slot so restart_lsn is at a recent position.
    subscriber1.safe_psql("DROP SUBSCRIPTION {};".format(SUB1))
    primary.psql_capture(
        "SELECT pg_create_logical_replication_slot('{}', 'pgoutput', false, "
        "false, true);".format(SLOT1)
    )
    primary.psql_capture(
        "SELECT pg_create_logical_replication_slot('{}', 'test_decoding', false, "
        "false, true);".format(SLOT2)
    )
    primary.psql_capture("SELECT pg_create_physical_replication_slot('sb1_slot');")

    standby1.start()

    inactive_since_on_primary = primary.validate_slot_inactive_since(
        SLOT1, slot_creation_time
    )
    primary.wait_for_replay_catchup(standby1)
    standby1.safe_psql("SELECT pg_sync_replication_slots();")

    assert (
        standby1.safe_psql(
            "SELECT count(*) = 2 FROM pg_replication_slots WHERE slot_name IN "
            "('{}', '{}') AND synced AND NOT temporary;".format(SLOT1, SLOT2)
        )
        == "t"
    ), "logical slots have synced as true on standby"

    inactive_since_on_standby = standby1.validate_slot_inactive_since(
        SLOT1, slot_creation_time
    )
    assert (
        standby1.safe_psql(
            "SELECT '{}'::timestamptz < '{}'::timestamptz;".format(
                inactive_since_on_primary, inactive_since_on_standby
            )
        )
        == "t"
    ), "synchronized slot has got its own inactive_since"
    return inactive_since_on_primary


def _test_drop_synced_slot(primary, standby1):
    """A synced slot is dropped on the standby when the remote slot is dropped."""
    primary.psql_capture("SELECT pg_drop_replication_slot('{}');".format(SLOT2))
    standby1.safe_psql("SELECT pg_sync_replication_slots();")
    assert (
        standby1.safe_psql(
            "SELECT count(*) = 0 FROM pg_replication_slots "
            "WHERE slot_name = '{}';".format(SLOT2)
        )
        == "t"
    ), "synchronized slot has been dropped"


def _test_invalidate_and_resync(primary, standby1, publisher):
    """An invalidated synced slot (wal_removed) is dropped and re-created as
    synced by a subsequent pg_sync_replication_slots()."""
    standby1.append_conf("max_slot_wal_keep_size = 64kB")
    standby1.reload()

    primary.advance_wal(1)
    primary.psql_capture("CHECKPOINT")
    primary.wait_for_replay_catchup(standby1)
    standby1.safe_psql("CHECKPOINT")

    assert (
        _slot_field(standby1, SLOT1, "invalidation_reason = 'wal_removed'") == "t"
    ), "synchronized slot has been invalidated"

    standby1.append_conf("max_slot_wal_keep_size = -1")
    standby1.reload()

    slot_creation_time = publisher.safe_psql("SELECT current_timestamp;")
    primary.safe_psql(
        "SELECT pg_drop_replication_slot('{slot}');\n"
        "SELECT pg_create_logical_replication_slot('{slot}', 'pgoutput', false, "
        "false, true);".format(slot=SLOT1)
    )
    primary.validate_slot_inactive_since(SLOT1, slot_creation_time)
    primary.wait_for_replay_catchup(standby1)

    log_offset = standby1.current_log_position()
    standby1.safe_psql("SELECT pg_sync_replication_slots();")
    standby1.wait_for_log(
        r'dropped replication slot "lsub1_slot" of database with OID [0-9]+',
        log_offset,
    )
    assert (
        _slot_field(
            standby1,
            SLOT1,
            "invalidation_reason IS NULL AND synced AND NOT temporary",
        )
        == "t"
    ), "logical slot is re-synced"

    primary.append_conf("log_min_messages = 'warning'")
    primary.reload()
    standby1.append_conf("log_min_messages = 'warning'")
    standby1.reload()


def _test_synced_slot_immutable(standby1):
    """A synced slot cannot be decoded, altered, or dropped by the user."""
    _expect_error(
        standby1,
        "select * from pg_logical_slot_get_changes('{}', NULL, NULL);".format(SLOT1),
        'cannot use replication slot "lsub1_slot" for logical decoding',
    )
    _expect_error(
        standby1,
        "ALTER_REPLICATION_SLOT {} (failover);".format(SLOT1),
        'cannot alter replication slot "lsub1_slot"',
        replication="database",
    )
    _expect_error(
        standby1,
        "SELECT pg_drop_replication_slot('{}');".format(SLOT1),
        'cannot drop replication slot "lsub1_slot"',
    )


def _test_dbname_required(standby1, connstr_1):
    """Slot sync errors out when primary_conninfo has no dbname; restore it."""
    standby1.append_conf("primary_conninfo = '{}'".format(connstr_1))
    log_offset = standby1.current_log_position()
    standby1.reload()
    standby1.wait_for_log(
        r"FATAL: .* terminating walreceiver process due to administrator command",
        log_offset,
    )
    _expect_error(
        standby1,
        "SELECT pg_sync_replication_slots();",
        'replication slot synchronization requires "dbname" to be specified '
        'in "primary_conninfo"',
    )
    standby1.append_conf("primary_conninfo = '{} dbname=postgres'".format(connstr_1))
    standby1.reload()


def _test_cascading_standby(create_pg, standby1):
    """Slot sync cannot run on a cascading standby."""
    backup_name = "backup2"
    standby1.backup(backup_name)
    cascading_standby = create_pg(
        "cascading_standby",
        from_backup=(standby1, backup_name),
        has_streaming=True,
        has_restoring=True,
        start=False,
    )
    cascading_standby.append_conf(
        "hot_standby_feedback = on\n"
        "primary_slot_name = 'cascading_sb_slot'\n"
        "primary_conninfo = '{} dbname=postgres'".format(standby1.connstr())
    )
    standby1.psql_capture(
        "SELECT pg_create_physical_replication_slot('cascading_sb_slot');"
    )
    cascading_standby.start()
    _expect_error(
        cascading_standby,
        "SELECT pg_sync_replication_slots();",
        "cannot synchronize replication slots from a standby server",
    )
    cascading_standby.stop()


def _test_snapshot_consistency(primary, standby1):
    """Create a failover slot, advance its restart_lsn past a running xact, and
    confirm confirmed_flush_lsn syncs to the standby. Returns nothing; the slot
    snap_test_slot is consumed after promotion."""
    primary.safe_psql(
        "SELECT pg_create_logical_replication_slot('snap_test_slot', "
        "'test_decoding', false, false, true);"
    )
    primary.wait_for_replay_catchup(standby1)
    standby1.safe_psql("SELECT pg_sync_replication_slots();")

    # Two xl_running_xacts logs: the first only serializes the snapshot; the
    # second allows restart_lsn to advance to the first log's position.
    primary.safe_psql(
        "BEGIN;\n"
        "SELECT txid_current();\n"
        "SELECT pg_log_standby_snapshot();\n"
        "COMMIT;\n"
        "BEGIN;\n"
        "SELECT txid_current();\n"
        "SELECT pg_log_standby_snapshot();\n"
        "COMMIT;"
    )
    primary.safe_psql(
        "SELECT pg_replication_slot_advance('snap_test_slot', pg_current_wal_lsn());"
    )
    primary.wait_for_replay_catchup(standby1)

    # This message is consumed after promotion using the synced slot.
    primary.safe_psql("SELECT pg_logical_emit_message(false, 'test', 'test');")
    confirmed_flush_lsn = _slot_field(primary, "snap_test_slot", "confirmed_flush_lsn")
    standby1.safe_psql("SELECT pg_sync_replication_slots();")
    _wait_for_synced_flush_lsn(
        standby1,
        "snap_test_slot",
        confirmed_flush_lsn,
        "confirmed_flush_lsn of slot snap_test_slot synced to standby",
    )


_MALICIOUS_SETUP = """
CREATE ROLE repl_role REPLICATION LOGIN;
CREATE SCHEMA myschema;

CREATE FUNCTION myschema.myintne(bigint, int) RETURNS bool as $$
		BEGIN
		  RETURN $1 <> $2;
		END;
	  $$ LANGUAGE plpgsql immutable;

CREATE OPERATOR myschema.= (
	  leftarg    = bigint,
	  rightarg   = int,
	  procedure  = myschema.myintne);

ALTER DATABASE slotsync_test_db SET SEARCH_PATH TO myschema,pg_catalog;
GRANT USAGE on SCHEMA myschema TO repl_role;
"""


def _test_malicious_user(primary, standby1, connstr_1):
    """Slot sync is protected from a malicious user who shadows the '=' operator
    used in slot sync's validation query."""
    primary.psql_capture("CREATE DATABASE slotsync_test_db")
    primary.wait_for_replay_catchup(standby1)
    standby1.stop()
    primary.safe_psql(_MALICIOUS_SETUP, dbname="slotsync_test_db")

    standby1.append_conf(
        "primary_conninfo = '{} dbname=slotsync_test_db "
        "user=repl_role'".format(connstr_1)
    )
    standby1.start()
    # If sync did not handle the attack it would fail validating the
    # primary_slot_name.
    standby1.safe_psql("SELECT pg_sync_replication_slots();", dbname="slotsync_test_db")

    standby1.append_conf("primary_conninfo = '{} dbname=postgres'".format(connstr_1))
    standby1.reload()
    primary.psql_capture("DROP DATABASE slotsync_test_db;")


def _test_slot_sync_worker_guc(standby1):
    """The slot sync worker starts, exits on an invalid GUC, and restarts on a
    valid GUC."""
    log_offset = standby1.current_log_position()
    standby1.append_conf("sync_replication_slots = on")
    standby1.reload()
    standby1.wait_for_log(r"slot sync worker started", log_offset)

    log_offset = standby1.current_log_position()
    standby1.append_conf("hot_standby_feedback = off")
    standby1.reload()
    standby1.wait_for_log(
        r"slot synchronization worker will restart because of a parameter change",
        log_offset,
    )
    standby1.wait_for_log(
        r'slot synchronization requires "hot_standby_feedback" to be enabled',
        log_offset,
    )

    log_offset = standby1.current_log_position()
    standby1.append_conf("hot_standby_feedback = on")
    standby1.reload()
    standby1.wait_for_log(r"slot sync worker started", log_offset)


def _test_worker_syncs_flush_lsn(primary, standby1, subscriber1, connstr):
    """The slot sync worker syncs confirmed_flush_lsn of the logical slot."""
    primary.safe_psql(
        "CREATE TABLE tab_int (a int PRIMARY KEY);\n"
        "INSERT INTO tab_int SELECT generate_series(1, 10);"
    )
    subscriber1.safe_psql(
        "CREATE TABLE tab_int (a int PRIMARY KEY);\n"
        "CREATE SUBSCRIPTION {sub} CONNECTION '{conn}' PUBLICATION {pub} "
        "WITH (slot_name = {slot}, failover = true, create_slot = false);".format(
            sub=SUB1, conn=connstr, pub=PUB, slot=SLOT1
        )
    )
    subscriber1.wait_for_subscription_sync()

    subscriber1.safe_psql("ALTER SUBSCRIPTION {} DISABLE".format(SUB1))
    assert primary.poll_query_until(
        "SELECT COUNT(*) FROM pg_catalog.pg_replication_slots "
        "WHERE slot_name = '{}' AND active='f'".format(SLOT1),
        expected="1",
    )
    primary_flush_lsn = _slot_field(primary, SLOT1, "confirmed_flush_lsn")
    _wait_for_synced_flush_lsn(
        standby1,
        SLOT1,
        primary_flush_lsn,
        "confirmed_flush_lsn of slot lsub1_slot synced to standby",
    )


def _setup_standby2_and_sub2(create_pg, primary, standby1, subscriber1, connstr):
    """Create standby2 (sb2_slot), subscriber2 (failover=false), enable sub1,
    and configure synchronized_standby_slots='sb1_slot'. Returns
    (standby2, subscriber2)."""
    backup_name = "backup3"
    primary.psql_capture("SELECT pg_create_physical_replication_slot('sb2_slot');")
    primary.backup(backup_name)

    standby2 = create_pg(
        "standby2",
        from_backup=(primary, backup_name),
        has_streaming=True,
        has_restoring=True,
        start=False,
    )
    standby2.append_conf("primary_slot_name = 'sb2_slot'")
    standby2.start()
    primary.wait_for_replay_catchup(standby2)

    primary.append_conf("synchronized_standby_slots = 'sb1_slot'")
    primary.reload()

    subscriber2 = create_pg("subscriber2", start=False)
    subscriber2.start()
    subscriber2.safe_psql(
        "CREATE TABLE tab_int (a int PRIMARY KEY);\n"
        "CREATE SUBSCRIPTION {sub} CONNECTION '{conn}' PUBLICATION {pub} "
        "WITH (slot_name = {slot});".format(sub=SUB2, conn=connstr, pub=PUB, slot=SLOT2)
    )
    subscriber2.wait_for_subscription_sync()
    subscriber1.safe_psql("ALTER SUBSCRIPTION {} ENABLE".format(SUB1))
    return standby2, subscriber2


def _test_failover_waits_for_standby(primary, standby1, standby2, subs):
    """A failover logical slot waits for the physical slot named in
    synchronized_standby_slots to catch up before the subscriber gets data."""
    subscriber1, subscriber2 = subs
    offset = primary.current_log_position()
    standby1.stop()

    row_count = 20
    primary.safe_psql(
        "INSERT INTO tab_int SELECT generate_series(11, {});".format(row_count)
    )
    primary.wait_for_replay_catchup(standby2)
    assert (
        standby2.safe_psql("SELECT count(*) = {} FROM tab_int;".format(row_count))
        == "t"
    ), "standby2 gets data from primary"

    primary.wait_for_catchup(SUB2)
    assert (
        subscriber2.safe_psql("SELECT count(*) = {} FROM tab_int;".format(row_count))
        == "t"
    ), "subscriber2 gets data from primary"

    primary.wait_for_log(
        r'replication slot "sb1_slot" specified in parameter '
        r'"synchronized_standby_slots" does not have active_pid',
        offset,
    )
    assert (
        subscriber1.safe_psql("SELECT count(*) <> {} FROM tab_int;".format(row_count))
        == "t"
    ), (
        "subscriber1 doesn't get data from primary until standby1 acknowledges "
        "changes"
    )

    standby1.start()
    primary.wait_for_replay_catchup(standby1)
    assert (
        standby1.safe_psql("SELECT count(*) = {} FROM tab_int;".format(row_count))
        == "t"
    ), "standby1 gets data from primary"

    primary.wait_for_catchup(SUB1)
    assert (
        subscriber1.safe_psql("SELECT count(*) = {} FROM tab_int;".format(row_count))
        == "t"
    ), "subscriber1 gets data from primary after standby1 acknowledges changes"


def _test_get_changes_waits(primary, standby1, subscriber1):
    """pg_logical_slot_get_changes on a failover slot also waits for the slots
    in synchronized_standby_slots; removing the slot lets it return."""
    primary.safe_psql("TRUNCATE tab_int;")
    primary.wait_for_catchup(SUB1)
    standby1.stop()

    subscriber1.safe_psql("ALTER SUBSCRIPTION {} DISABLE".format(SUB1))
    assert primary.poll_query_until(
        "SELECT COUNT(*) FROM pg_catalog.pg_replication_slots "
        "WHERE slot_name = '{}' AND active = 'f'".format(SLOT1),
        expected="1",
    )
    primary.safe_psql(
        "SELECT pg_create_logical_replication_slot('test_slot', "
        "'test_decoding', false, false, true);"
    )

    back_q = primary.background_psql("postgres", on_error_stop=False)
    offset = primary.current_log_position()
    back_q.query_until(
        r"logical_slot_get_changes",
        "\\echo logical_slot_get_changes\n"
        "SELECT pg_logical_slot_get_changes('test_slot', NULL, NULL);\n",
    )
    primary.wait_for_log(
        r'replication slot "sb1_slot" specified in parameter '
        r'"synchronized_standby_slots" does not have active_pid',
        offset,
    )
    primary.adjust_conf("synchronized_standby_slots", "''")
    primary.reload()
    back_q.quit()

    primary.safe_psql("SELECT pg_drop_replication_slot('test_slot');")
    primary.adjust_conf("synchronized_standby_slots", "'sb1_slot'")
    primary.reload()
    subscriber1.safe_psql("ALTER SUBSCRIPTION {} ENABLE".format(SUB1))


def _test_inactive_physical_slot_waits(primary, subscriber1):
    """Logical replication waits for the inactive user-created physical slot in
    synchronized_standby_slots until it is removed from the list."""
    offset = primary.current_log_position()
    row_count = 10
    primary.safe_psql(
        "INSERT INTO tab_int SELECT generate_series(1, {});".format(row_count)
    )
    primary.wait_for_log(
        r'replication slot "sb1_slot" specified in parameter '
        r'"synchronized_standby_slots" does not have active_pid',
        offset,
    )
    assert (
        subscriber1.safe_psql("SELECT count(*) = 0 FROM tab_int;") == "t"
    ), "subscriber1 doesn't get data as the sb1_slot doesn't catch up"

    primary.adjust_conf("synchronized_standby_slots", "''")
    primary.reload()
    primary.wait_for_catchup(SUB1)
    assert (
        subscriber1.safe_psql("SELECT count(*) = {} FROM tab_int;".format(row_count))
        == "t"
    ), (
        "subscriber1 gets data from primary after standby1 is removed from the "
        "synchronized_standby_slots list"
    )
    primary.adjust_conf("synchronized_standby_slots", "'sb1_slot'")
    primary.reload()


def _test_two_phase_sync(primary, standby1, subscriber1):
    """The two_phase setting syncs to the standby; a transaction prepared before
    two_phase was enabled is not yet replicated to the subscriber."""
    standby1.start()
    primary.safe_psql(
        "BEGIN;\n"
        "INSERT INTO tab_int values(0);\n"
        "PREPARE TRANSACTION 'test_twophase_slotsync';"
    )
    primary.wait_for_replay_catchup(standby1)
    primary.wait_for_catchup(SUB1)

    subscriber1.safe_psql("ALTER SUBSCRIPTION {} DISABLE".format(SUB1))
    assert primary.poll_query_until(
        "SELECT COUNT(*) FROM pg_catalog.pg_replication_slots "
        "WHERE slot_name = '{}' AND active='f'".format(SLOT1),
        expected="1",
    )
    subscriber1.safe_psql(
        "ALTER SUBSCRIPTION {sub} SET (two_phase = true);\n"
        "ALTER SUBSCRIPTION {sub} ENABLE;".format(sub=SUB1)
    )
    primary.wait_for_catchup(SUB1)

    two_phase_at = _slot_field(primary, SLOT1, "two_phase_at")
    assert standby1.poll_query_until(
        "SELECT two_phase AND '{at}' = two_phase_at FROM pg_replication_slots "
        "WHERE slot_name = '{slot}' AND synced AND NOT temporary;".format(
            at=two_phase_at, slot=SLOT1
        )
    ), "two_phase setting of slot lsub1_slot synced to standby"
    assert (
        subscriber1.safe_psql("SELECT count(*) = 0 FROM pg_prepared_xacts;") == "t"
    ), "the prepared transaction is not replicated to the subscriber"


def _test_promotion(primary, standby1, subscriber1, inactive_since_on_primary):
    """Promote standby1 to primary: synced slots are retained, logical
    replication resumes, the prepared txn commits and replicates, and the synced
    snap_test_slot can be consumed."""
    primary.wait_for_replay_catchup(standby1)
    promotion_time = standby1.safe_psql("SELECT current_timestamp;")
    standby1.promote()

    # Capture inactive_since before the slot is enabled on the new primary.
    inactive_since_on_new_primary = standby1.validate_slot_inactive_since(
        SLOT1, promotion_time
    )
    assert (
        standby1.safe_psql(
            "SELECT '{}'::timestamptz > '{}'::timestamptz".format(
                inactive_since_on_new_primary, inactive_since_on_primary
            )
        )
        == "t"
    ), (
        "synchronized slot has got its own inactive_since on the new primary "
        "after promotion"
    )

    subscriber1.safe_psql(
        "ALTER SUBSCRIPTION {} CONNECTION '{} dbname=postgres';".format(
            SUB1, standby1.connstr()
        )
    )
    assert (
        standby1.safe_psql(
            "SELECT count(*) = 2 FROM pg_replication_slots WHERE slot_name IN "
            "('{}', 'snap_test_slot') AND synced AND NOT temporary;".format(SLOT1)
        )
        == "t"
    ), "synced slot retained on the new primary"

    standby1.safe_psql("COMMIT PREPARED 'test_twophase_slotsync';")
    standby1.wait_for_catchup(SUB1)
    assert (
        subscriber1.safe_psql("SELECT count(*) FROM tab_int;") == "11"
    ), "prepared data replicated from the new primary"

    standby1.safe_psql("INSERT INTO tab_int SELECT generate_series(11, 20);")
    standby1.wait_for_catchup(SUB1)
    assert (
        subscriber1.safe_psql("SELECT count(*) FROM tab_int;") == "21"
    ), "data replicated from the new primary"

    assert (
        standby1.safe_psql(
            "SELECT count(*) FROM pg_logical_slot_get_changes('snap_test_slot', "
            "NULL, NULL) WHERE data ~ 'message*';"
        )
        == "1"
    ), "data can be consumed using snap_test_slot"


def _cleanup_after_promotion(primary, standby2, subscriber1, subscriber2):
    """Drop slots/subscriptions and clear the prepared transaction on the
    original primary so the environment is clean for the skip-retry test."""
    primary.psql_capture(
        "SELECT pg_drop_replication_slot('sb1_slot');\n"
        "SELECT pg_drop_replication_slot('{}');\n"
        "SELECT pg_drop_replication_slot('snap_test_slot');".format(SLOT1)
    )
    subscriber2.safe_psql("DROP SUBSCRIPTION {};".format(SUB2))
    subscriber1.safe_psql("DROP SUBSCRIPTION {};".format(SUB1))
    subscriber1.safe_psql("TRUNCATE tab_int;")

    primary.adjust_conf("synchronized_standby_slots", "''")
    primary.reload()
    assert (
        primary.safe_psql(
            "SELECT count(*) = 0 FROM pg_replication_slots "
            "WHERE slot_name != 'sb2_slot';"
        )
        == "t"
    ), (
        "all replication slots have been dropped except the physical slot used "
        "by standby2"
    )

    primary.safe_psql("COMMIT PREPARED 'test_twophase_slotsync';")
    primary.wait_for_replay_catchup(standby2)


def _test_skip_and_retry(primary, standby2, subscriber1, connstr, connstr_1):
    """pg_sync_replication_slots() on the standby skips and retries until the
    slot is sync-ready; the slotsync skip reason and skip count are updated.

    This runs against the original primary (publisher) and its physical standby
    standby2 (which still streams from the original primary via sb2_slot). The
    promotion of standby1 did not demote the original primary, so it continues
    to act as a publisher here."""
    subscriber1.safe_psql(
        "CREATE TABLE push_wal (a int);\n"
        "CREATE SUBSCRIPTION {sub} CONNECTION '{conn}' PUBLICATION {pub} "
        "WITH (slot_name = {slot}, failover = true, enabled = false);".format(
            sub=SUB1, conn=connstr, pub=PUB, slot=SLOT1
        )
    )
    primary.safe_psql("CREATE TABLE push_wal (a int);")
    primary.wait_for_replay_catchup(standby2)

    log_offset = standby2.current_log_position()
    standby2.append_conf(
        "hot_standby_feedback = on\n"
        "primary_conninfo = '{} dbname=postgres'\n"
        "log_min_messages = 'debug2'".format(connstr_1)
    )
    standby2.reload()

    handle = standby2.background_psql("postgres", on_error_stop=False)
    handle.query_until(r"start", "\\echo start\nSELECT pg_sync_replication_slots();\n")
    standby2.wait_for_log(
        r'could not synchronize replication slot "lsub1_slot"', log_offset
    )

    assert (
        _slot_field(standby2, SLOT1, "slotsync_skip_reason") == "wal_or_rows_removed"
    ), "check slot sync skip reason"
    assert (
        standby2.safe_psql(
            "SELECT slotsync_skip_count > 0 FROM pg_stat_replication_slots "
            "WHERE slot_name = '{}'".format(SLOT1)
        )
        == "t"
    ), "check slot sync skip count increments"

    primary.append_conf("synchronized_standby_slots = 'sb2_slot'")
    primary.reload()
    subscriber1.safe_psql("ALTER SUBSCRIPTION {} ENABLE".format(SUB1))
    subscriber1.wait_for_subscription_sync()
    primary.safe_psql("SELECT pg_log_standby_snapshot();")
    standby2.wait_for_log(
        r'newly created replication slot "lsub1_slot" is sync-ready now',
        log_offset,
    )
    handle.quit()


def test_040_standby_failover_slots_sync(create_pg):
    """Failover slot synchronization from a publisher to its physical standby."""
    publisher, subscriber1 = _setup_publisher_subscriber(create_pg)
    connstr = publisher.connstr("postgres")

    slot_creation_time = _test_failover_flag_toggling(publisher, subscriber1, connstr)

    primary = publisher
    connstr_1 = primary.connstr()
    standby1 = _create_standby(create_pg, primary, "standby1", "sb1_slot")

    inactive_since_on_primary = _test_sync_two_plugins(
        primary, standby1, subscriber1, slot_creation_time
    )
    _test_drop_synced_slot(primary, standby1)
    _test_invalidate_and_resync(primary, standby1, publisher)
    _test_synced_slot_immutable(standby1)
    _test_dbname_required(standby1, connstr_1)
    _test_cascading_standby(create_pg, standby1)
    _test_snapshot_consistency(primary, standby1)
    _test_malicious_user(primary, standby1, connstr_1)
    _test_slot_sync_worker_guc(standby1)
    _test_worker_syncs_flush_lsn(primary, standby1, subscriber1, connstr)

    standby2, subscriber2 = _setup_standby2_and_sub2(
        create_pg, primary, standby1, subscriber1, connstr
    )
    _test_failover_waits_for_standby(
        primary, standby1, standby2, (subscriber1, subscriber2)
    )
    _test_get_changes_waits(primary, standby1, subscriber1)
    _test_inactive_physical_slot_waits(primary, subscriber1)
    _test_two_phase_sync(primary, standby1, subscriber1)
    _test_promotion(primary, standby1, subscriber1, inactive_since_on_primary)
    _cleanup_after_promotion(primary, standby2, subscriber1, subscriber2)

    # The skip-retry test runs against the original primary (publisher) and
    # standby2; standby1 (now a separate promoted primary) is not involved.
    _test_skip_and_retry(primary, standby2, subscriber1, connstr, connstr_1)

    standby2.stop()
    standby1.stop()
    subscriber1.stop()
    subscriber2.stop()
    publisher.stop()

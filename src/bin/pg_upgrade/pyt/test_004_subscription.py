# Copyright (c) 2023-2026, PostgreSQL Global Development Group

"""Port of src/bin/pg_upgrade/t/004_subscription.pl.

Test for pg_upgrade of logical subscription. Note that after the successful
upgrade test, we can't use the old cluster to prevent failing in --link mode.
"""

import os
import re
import shutil

import pypg


def _check_cmd(old_sub, new_sub, oldbindir, newbindir, mode):
    """Build a pg_upgrade --check command for the old/new subscriber pair."""
    return [
        "pg_upgrade",
        "--no-sync",
        "--old-datadir",
        str(old_sub.datadir),
        "--new-datadir",
        str(new_sub.datadir),
        "--old-bindir",
        oldbindir,
        "--new-bindir",
        newbindir,
        "--socketdir",
        str(new_sub.host),
        "--old-port",
        str(old_sub.port),
        "--new-port",
        str(new_sub.port),
        mode,
        "--check",
    ]


def _upgrade_cmd(old_sub, new_sub, oldbindir, newbindir, mode):
    """Build a (non --check) pg_upgrade command for the old/new subscriber."""
    return [
        "pg_upgrade",
        "--no-sync",
        "--old-datadir",
        str(old_sub.datadir),
        "--new-datadir",
        str(new_sub.datadir),
        "--old-bindir",
        oldbindir,
        "--new-bindir",
        newbindir,
        "--socketdir",
        str(new_sub.host),
        "--old-port",
        str(old_sub.port),
        "--new-port",
        str(new_sub.port),
        mode,
    ]


def _find_under(directory, needle):
    """Return the path of the first file under directory whose name matches.

    Mirrors the File::Find::find usage in the Perl original; the output
    directory contains a milliseconds timestamp so the path can't be predicted.
    """
    for dirpath, _dirs, files in os.walk(directory):
        for name in files:
            if needle in name:
                return os.path.join(dirpath, name)
    return None


def _check_insufficient_origins(pg_bin, pub, old_sub, new_sub, dirs, mode):
    """pg_upgrade fails when max_active_replication_origins is too small."""
    connstr, oldbindir, newbindir = dirs
    # It is sufficient to use disabled subscription to test upgrade failure.
    pub.safe_psql("CREATE PUBLICATION regress_pub1")
    old_sub.safe_psql(
        "CREATE SUBSCRIPTION regress_sub1 CONNECTION '{}' "
        "PUBLICATION regress_pub1 WITH (enabled = false)".format(connstr)
    )
    old_sub.stop()
    new_sub.append_conf("max_active_replication_origins = 0", "postgresql.conf")

    pg_bin.command_checks_all(
        _check_cmd(old_sub, new_sub, oldbindir, newbindir, mode),
        1,
        [
            r'"max_active_replication_origins" \(0\) must be greater than or '
            r"equal to the number of subscriptions \(1\) on the old cluster"
        ],
        [r""],
        "run of pg_upgrade where the new cluster has insufficient "
        "max_active_replication_origins",
    )

    # Reset max_active_replication_origins
    new_sub.append_conf("max_active_replication_origins = 10", "postgresql.conf")

    # Cleanup
    pub.safe_psql("DROP PUBLICATION regress_pub1")
    old_sub.start()
    old_sub.safe_psql("DROP SUBSCRIPTION regress_sub1;")


def _check_insufficient_slots(pg_bin, pub, old_sub, new_sub, dirs, mode):
    """pg_upgrade fails when max_replication_slots is too small.

    The new cluster needs at least the number of logical slots on the old
    cluster plus one for retaining conflict-detection information when a
    subscription enables retain_dead_tuples.
    """
    connstr, oldbindir, newbindir = dirs
    pub.safe_psql("CREATE PUBLICATION regress_pub1")
    old_sub.safe_psql(
        "CREATE SUBSCRIPTION regress_sub1 CONNECTION '{}' "
        "PUBLICATION regress_pub1 WITH (enabled = false, "
        "retain_dead_tuples = true)".format(connstr)
    )
    old_sub.stop()
    new_sub.append_conf("max_replication_slots = 0", "postgresql.conf")

    pg_bin.command_checks_all(
        _check_cmd(old_sub, new_sub, oldbindir, newbindir, mode),
        1,
        [
            r'"max_replication_slots" \(0\) must be greater than or equal to '
            r"the number of logical replication slots on the old cluster plus "
            r"one additional slot required for retaining conflict detection "
            r"information \(1\)"
        ],
        [r""],
        "run of pg_upgrade where the new cluster has insufficient "
        "max_replication_slots",
    )

    # Reset max_replication_slots
    new_sub.append_conf("max_replication_slots = 10", "postgresql.conf")

    # Cleanup
    pub.safe_psql("DROP PUBLICATION regress_pub1")
    old_sub.start()
    old_sub.safe_psql("DROP SUBSCRIPTION regress_sub1;")


def _setup_invalid_states(pub, old_sub, connstr):
    """Create a subscription with a relation in 'd' state and one missing its
    replication origin, returning when both invalid conditions are present."""
    pub.safe_psql(
        "CREATE TABLE tab_primary_key(id serial PRIMARY KEY);\n"
        "INSERT INTO tab_primary_key values(1);\n"
        "CREATE PUBLICATION regress_pub2 FOR TABLE tab_primary_key;"
    )
    # Insert the same value that is already present in publisher to the primary
    # key column of subscriber so that the table sync will fail.
    old_sub.safe_psql(
        "CREATE TABLE tab_primary_key(id serial PRIMARY KEY);\n"
        "INSERT INTO tab_primary_key values(1);\n"
        "CREATE SUBSCRIPTION regress_sub2 CONNECTION '{}' "
        "PUBLICATION regress_pub2;".format(connstr)
    )
    # Table will be in 'd' (data is being copied) state as table sync will fail
    # because of primary key constraint error.
    started_query = (
        "SELECT count(1) = 1 FROM pg_subscription_rel WHERE srsubstate = 'd'"
    )
    assert old_sub.poll_query_until(
        started_query
    ), "Timed out while waiting for the table state to become 'd' (datasync)"

    # Setup another logical replication and drop the subscription's replication
    # origin.
    pub.safe_psql("CREATE PUBLICATION regress_pub3")
    old_sub.safe_psql(
        "CREATE SUBSCRIPTION regress_sub3 CONNECTION '{}' "
        "PUBLICATION regress_pub3 WITH (enabled = false)".format(connstr)
    )
    sub_oid = old_sub.safe_psql(
        "SELECT oid FROM pg_subscription WHERE subname = 'regress_sub3'"
    )
    replorigin = "pg_" + sub_oid
    old_sub.safe_psql("SELECT pg_replication_origin_drop('{}')".format(replorigin))
    old_sub.stop()


def _check_invalid_relstate(pg_bin, pub, old_sub, new_sub, dirs, mode):
    """pg_upgrade refuses to run with invalid subscription rel state / origin.

    Specifically when there's a subscription with tables in a state other than
    'r' (ready) or 'i' (init), and/or the subscription has no replication
    origin.
    """
    connstr, oldbindir, newbindir = dirs
    _setup_invalid_states(pub, old_sub, connstr)

    pg_bin.command_checks_all(
        _check_cmd(old_sub, new_sub, oldbindir, newbindir, mode),
        1,
        [
            re.escape(
                "Your installation contains subscriptions without origin or "
                "having relations not in i (initialize) or r (ready) state"
            )
        ],
        [],
        "run of pg_upgrade --check for old instance with relation in 'd' "
        "datasync(invalid) state and missing replication origin",
    )

    # Verify the reason why the subscriber cannot be upgraded.
    sub_relstate_filename = _find_under(
        os.path.join(new_sub.datadir, "pg_upgrade_output.d"), "subs_invalid.txt"
    )
    assert sub_relstate_filename is not None, "subs_invalid.txt not found"
    contents = pypg.slurp_file(sub_relstate_filename)

    # Check the file content which should have tab_primary_key table in an
    # invalid state.
    assert re.search(
        r'The table sync state "d" is not allowed for database:"postgres" '
        r'subscription:"regress_sub2" schema:"public" '
        r'relation:"tab_primary_key"',
        contents,
        re.MULTILINE,
    ), "the previous test failed due to subscription table in invalid state"
    # Check the file content which should have regress_sub3 subscription.
    assert re.search(
        r'The replication origin is missing for database:"postgres" '
        r'subscription:"regress_sub3"',
        contents,
        re.MULTILINE,
    ), "the previous test failed due to missing replication origin"

    # Cleanup
    old_sub.start()
    pub.safe_psql(
        "DROP PUBLICATION regress_pub2;\n"
        "DROP PUBLICATION regress_pub3;\n"
        "DROP TABLE tab_primary_key;"
    )
    old_sub.safe_psql(
        "DROP SUBSCRIPTION regress_sub2;\n"
        "DROP SUBSCRIPTION regress_sub3;\n"
        "DROP TABLE tab_primary_key;"
    )
    shutil.rmtree(os.path.join(new_sub.datadir, "pg_upgrade_output.d"))


def _setup_ready_and_init(pub, old_sub, connstr):
    """Set up subscriptions with ready and init state tables before upgrade.

    Returns (remote_lsn, oids) where oids is (tab_upgraded, tab_upgraded1,
    tab_upgraded2) relation OIDs on the old subscriber.
    """
    # Use multiple tables to verify deterministic pg_dump ordering of
    # subscription relations during --binary-upgrade.
    pub.safe_psql(
        "CREATE TABLE tab_upgraded(id int);\n"
        "CREATE TABLE tab_upgraded1(id int);\n"
        "CREATE PUBLICATION regress_pub4 FOR TABLE tab_upgraded, tab_upgraded1;"
    )
    old_sub.safe_psql(
        "CREATE TABLE tab_upgraded(id int);\n"
        "CREATE TABLE tab_upgraded1(id int);\n"
        "CREATE SUBSCRIPTION regress_sub4 CONNECTION '{}' "
        "PUBLICATION regress_pub4 WITH (failover = true, "
        "retain_dead_tuples = true);".format(connstr)
    )

    # Wait till the tables tab_upgraded and tab_upgraded1 reach 'ready' state
    synced_query = "SELECT count(1) = 2 FROM pg_subscription_rel WHERE srsubstate = 'r'"
    assert old_sub.poll_query_until(
        synced_query
    ), "Timed out while waiting for the table to reach ready state"

    pub.safe_psql("INSERT INTO tab_upgraded1 VALUES (generate_series(1,50))")
    pub.wait_for_catchup("regress_sub4")

    # Change configuration to prepare a subscription table in init state
    old_sub.append_conf("max_logical_replication_workers = 0", "postgresql.conf")
    old_sub.restart()

    # Setup another logical replication
    pub.safe_psql(
        "CREATE TABLE tab_upgraded2(id int);\n"
        "CREATE PUBLICATION regress_pub5 FOR TABLE tab_upgraded2;"
    )
    old_sub.safe_psql(
        "CREATE TABLE tab_upgraded2(id int);\n"
        "CREATE SUBSCRIPTION regress_sub5 CONNECTION '{}' "
        "PUBLICATION regress_pub5;".format(connstr)
    )

    # The table tab_upgraded2 will be in the init state as the subscriber's
    # configuration for max_logical_replication_workers is set to 0.
    result = old_sub.safe_psql(
        "SELECT count(1) = 1 FROM pg_subscription_rel WHERE srsubstate = 'i'"
    )
    assert result == "t", "Check that the table is in init state"

    # Get the replication origin's remote_lsn of the old subscriber
    remote_lsn = old_sub.safe_psql(
        "SELECT remote_lsn FROM pg_replication_origin_status os, "
        "pg_subscription s WHERE os.external_id = 'pg_' || s.oid "
        "AND s.subname = 'regress_sub4'"
    )
    # Have the subscription in disabled state before upgrade
    old_sub.safe_psql("ALTER SUBSCRIPTION regress_sub5 DISABLE")

    oids = (
        old_sub.safe_psql("SELECT oid FROM pg_class WHERE relname = 'tab_upgraded'"),
        old_sub.safe_psql("SELECT oid FROM pg_class WHERE relname = 'tab_upgraded1'"),
        old_sub.safe_psql("SELECT oid FROM pg_class WHERE relname = 'tab_upgraded2'"),
    )
    old_sub.stop()
    return remote_lsn, oids


def _verify_upgraded_state(pub, new_sub, remote_lsn, oids):
    """Verify subscription state, relations, origin LSN and replicated rows."""
    tab_upgraded_oid, tab_upgraded1_oid, tab_upgraded2_oid = oids

    # Data inserted to the publisher while the new subscriber is down should be
    # replicated once it is started.
    pub.safe_psql(
        "INSERT INTO tab_upgraded1 VALUES(51);\nINSERT INTO tab_upgraded2 VALUES(1);"
    )

    new_sub.start()

    # The subscription's running status, failover option, and
    # retain_dead_tuples option should be preserved in the upgraded instance.
    result = new_sub.safe_psql(
        "SELECT subname, subenabled, subfailover, subretaindeadtuples "
        "FROM pg_subscription ORDER BY subname"
    )
    assert result == "regress_sub4|t|t|t\nregress_sub5|f|f|f", (
        "check that the subscription's running status, failover, and "
        "retain_dead_tuples are preserved"
    )

    # Subscription relations should be preserved
    result = new_sub.safe_psql(
        "SELECT srrelid, srsubstate FROM pg_subscription_rel ORDER BY srrelid"
    )
    assert result == "{}|r\n{}|r\n{}|i".format(
        tab_upgraded_oid, tab_upgraded1_oid, tab_upgraded2_oid
    ), (
        "there should be 3 rows in pg_subscription_rel(representing "
        "tab_upgraded, tab_upgraded1 and tab_upgraded2)"
    )

    # The replication origin's remote_lsn should be preserved
    sub_oid = new_sub.safe_psql(
        "SELECT oid FROM pg_subscription WHERE subname = 'regress_sub4'"
    )
    result = new_sub.safe_psql(
        "SELECT remote_lsn FROM pg_replication_origin_status "
        "WHERE external_id = 'pg_' || {}".format(sub_oid)
    )
    assert result == remote_lsn, "remote_lsn should have been preserved"

    # The conflict detection slot should be created
    result = new_sub.safe_psql(
        "SELECT xmin IS NOT NULL from pg_replication_slots "
        "WHERE slot_name = 'pg_conflict_detection'"
    )
    assert result == "t", "conflict detection slot exists"


def _verify_resumed_sync(pub, new_sub):
    """Resume initial sync, enable regress_sub5, and verify replicated rows."""
    new_sub.append_conf("max_logical_replication_workers = 10", "postgresql.conf")
    new_sub.restart()
    new_sub.safe_psql("ALTER SUBSCRIPTION regress_sub5 ENABLE")
    new_sub.wait_for_subscription_sync(pub, "regress_sub5")

    # wait for regress_sub4 to catchup as well
    pub.wait_for_catchup("regress_sub4")

    # Rows on tab_upgraded1 and tab_upgraded2 should have been replicated
    result = new_sub.safe_psql("SELECT count(*) FROM tab_upgraded1")
    assert result == "51", "check replicated inserts on new subscriber"
    result = new_sub.safe_psql("SELECT count(*) FROM tab_upgraded2")
    assert result == "1", (
        "check the data is synced after enabling the subscription for the "
        "table that was in init state"
    )


def test_004_subscription(create_pg, pg_bin, tmp_check, monkeypatch):
    """pg_upgrade of logical subscription across a same-version upgrade."""
    # Can be changed to test the other modes.
    mode = os.environ.get("PG_TEST_PG_UPGRADE_MODE") or "--copy"

    # Initialize publisher node
    publisher = create_pg("publisher", start=False, allows_streaming="logical")
    publisher.start()

    # Initialize the old subscriber node
    old_sub = create_pg("old_sub", start=False, allows_streaming="physical")
    old_sub.start()
    oldbindir = old_sub.config_data("--bindir")

    # Initialize the new subscriber
    new_sub = create_pg("new_sub", start=False, allows_streaming="physical")
    newbindir = new_sub.config_data("--bindir")

    # In a VPATH build, we'll be started in the source directory, but we want to
    # run pg_upgrade in the build directory so that any files generated finish
    # in it, like delete_old_cluster.{sh,bat}.
    monkeypatch.chdir(tmp_check)

    # Remember a connection string for the publisher node.
    connstr = publisher.connstr() + " dbname=postgres"
    dirs = (connstr, oldbindir, newbindir)

    _check_insufficient_origins(pg_bin, publisher, old_sub, new_sub, dirs, mode)
    _check_insufficient_slots(pg_bin, publisher, old_sub, new_sub, dirs, mode)
    _check_invalid_relstate(pg_bin, publisher, old_sub, new_sub, dirs, mode)

    remote_lsn, oids = _setup_ready_and_init(publisher, old_sub, connstr)

    # Change configuration so that initial table sync does not get started
    # automatically
    new_sub.append_conf("max_logical_replication_workers = 0", "postgresql.conf")

    # Check that pg_upgrade is successful when all tables are in ready or in
    # init state (tab_upgraded and tab_upgraded1 tables are in ready state and
    # tab_upgraded2 table is in init state) along with retaining the
    # replication origin's remote lsn, subscription's running status, failover
    # option, and retain_dead_tuples option.
    pg_bin.command_ok(
        _upgrade_cmd(old_sub, new_sub, oldbindir, newbindir, mode),
        "run of pg_upgrade for old instance when the subscription tables are "
        "in init/ready state",
    )
    assert not os.path.isdir(
        os.path.join(new_sub.datadir, "pg_upgrade_output.d")
    ), "pg_upgrade_output.d/ removed after successful pg_upgrade"

    _verify_upgraded_state(publisher, new_sub, remote_lsn, oids)
    _verify_resumed_sync(publisher, new_sub)

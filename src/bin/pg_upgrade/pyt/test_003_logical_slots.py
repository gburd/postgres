# Copyright (c) 2023-2026, PostgreSQL Global Development Group

"""Port of src/bin/pg_upgrade/t/003_logical_slots.pl.

Tests for upgrading logical replication slots.
"""

import os
import re

import pypg


def _build_pg_upgrade_cmd(oldpub, newpub, mode):
    """Build the common pg_upgrade command used by all the test cases."""
    return [
        "pg_upgrade",
        "--no-sync",
        "--old-datadir",
        str(oldpub.datadir),
        "--new-datadir",
        str(newpub.datadir),
        "--old-bindir",
        oldpub.config_data("--bindir"),
        "--new-bindir",
        newpub.config_data("--bindir"),
        "--socketdir",
        str(newpub.host),
        "--old-port",
        str(oldpub.port),
        "--new-port",
        str(newpub.port),
        mode,
    ]


def _find_under(directory, needle):
    """Return the path of the first file under directory whose name matches.

    Mirrors the File::Find::find usage in the Perl original: the output
    directory contains a milliseconds timestamp, so the file's path cannot be
    predicted and must be discovered by walking the tree.
    """
    for dirpath, _dirs, files in os.walk(directory):
        for name in files:
            if needle in name:
                return os.path.join(dirpath, name)
    return None


def _check_insufficient_max_replication_slots(pg_bin, oldpub, newpub, mode):
    """TEST: pg_upgrade fails when the new cluster has wrong GUC values."""
    # Preparations for the subsequent test:
    # 1. Create three slots on the old cluster
    oldpub.start()
    oldpub.safe_psql(
        "SELECT pg_create_logical_replication_slot('test_slot1', 'test_decoding');\n"
        "SELECT pg_create_logical_replication_slot('test_slot2', 'test_decoding');\n"
        "SELECT pg_create_logical_replication_slot('test_slot3', 'test_decoding');"
    )
    oldpub.stop()

    # 2. Set 'max_replication_slots' to be less than the number of slots (3)
    #    present on the old cluster.
    newpub.append_conf("max_replication_slots = 1", "postgresql.conf")

    # pg_upgrade will fail because the new cluster has insufficient
    # max_replication_slots
    pg_bin.command_checks_all(
        _build_pg_upgrade_cmd(oldpub, newpub, mode),
        1,
        [
            r'"max_replication_slots" \(1\) must be greater than or equal to the '
            r"number of logical replication slots \(3\) on the old cluster"
        ],
        [r""],
        "run of pg_upgrade where the new cluster has insufficient "
        '"max_replication_slots"',
    )
    assert os.path.isdir(
        os.path.join(newpub.datadir, "pg_upgrade_output.d")
    ), "pg_upgrade_output.d/ not removed after pg_upgrade failure"

    # Set 'max_replication_slots' to match the number of slots (3) present on
    # the old cluster.  Both slots will be used for subsequent tests.
    newpub.append_conf("max_replication_slots = 3", "postgresql.conf")


def _check_unconsumed_wal(pg_bin, oldpub, newpub, mode):
    """TEST: pg_upgrade fails when a slot still has unconsumed WAL records."""
    # Preparations for the subsequent test:
    # 1. Generate extra WAL records. At this point none of the slots has
    #    consumed them.
    # 2. Advance the slot test_slot2 up to the current WAL location, but
    #    test_slot1 still has unconsumed WAL records.
    # 3. Emit a non-transactional message. This will cause test_slot2 to detect
    #    the unconsumed WAL record.
    # 4. Advance the slot test_slot3 up to the current WAL location.
    oldpub.start()
    oldpub.safe_psql(
        "CREATE TABLE tbl AS SELECT generate_series(1, 10) AS a;\n"
        "SELECT pg_replication_slot_advance('test_slot2', pg_current_wal_lsn());\n"
        "SELECT count(*) FROM pg_logical_emit_message('false', 'prefix', "
        "'This is a non-transactional message', true);\n"
        "SELECT pg_replication_slot_advance('test_slot3', pg_current_wal_lsn());"
    )
    oldpub.stop()

    # pg_upgrade will fail because there are slots still having unconsumed WAL
    # records
    pg_bin.command_checks_all(
        _build_pg_upgrade_cmd(oldpub, newpub, mode),
        1,
        [
            r"Your installation contains logical replication slots that cannot "
            r"be upgraded\."
        ],
        [r""],
        "run of pg_upgrade of old cluster with slots having unconsumed WAL records",
    )

    # Verify the reason why the logical replication slot cannot be upgraded.
    # Find a txt file that contains a list of logical replication slots that
    # cannot be upgraded. We cannot predict the file's path because the output
    # directory contains a milliseconds timestamp.
    slots_filename = _find_under(
        os.path.join(newpub.datadir, "pg_upgrade_output.d"),
        "invalid_logical_slots.txt",
    )
    assert slots_filename is not None, "invalid_logical_slots.txt not found"

    # Check the file content. While both test_slot1 and test_slot2 should be
    # reporting that they have unconsumed WAL records, test_slot3 should not be
    # reported as it has caught up.
    contents = pypg.slurp_file(slots_filename)

    assert re.search(
        r'The slot "test_slot1" has not consumed the WAL yet', contents, re.MULTILINE
    ), "the previous test failed due to unconsumed WALs"
    assert re.search(
        r'The slot "test_slot2" has not consumed the WAL yet', contents, re.MULTILINE
    ), "the previous test failed due to unconsumed WALs"
    assert not re.search(
        r"test_slot3", contents, re.MULTILINE
    ), "caught-up slot is not reported"


def _check_successful_upgrade(create_pg, pg_bin, oldpub, newpub, mode):
    """TEST: Successful upgrade with logical replication migrated."""
    # Preparations for the subsequent test:
    # 1. Setup logical replication (first, cleanup slots from previous tests)
    old_connstr = oldpub.connstr() + " dbname=postgres"

    oldpub.start()
    oldpub.safe_psql(
        "SELECT * FROM pg_drop_replication_slot('test_slot1');\n"
        "SELECT * FROM pg_drop_replication_slot('test_slot2');\n"
        "SELECT * FROM pg_drop_replication_slot('test_slot3');\n"
        "CREATE PUBLICATION regress_pub FOR ALL TABLES;"
    )

    # Initialize subscriber cluster
    sub = create_pg("sub", start=False)
    sub.start()
    sub.safe_psql(
        "CREATE TABLE tbl (a int);\n"
        "CREATE SUBSCRIPTION regress_sub CONNECTION '{}' PUBLICATION regress_pub "
        "WITH (two_phase = 'true', failover = 'true')".format(old_connstr)
    )
    sub.wait_for_subscription_sync(oldpub, "regress_sub")

    # Also wait for two-phase to be enabled
    twophase_query = (
        "SELECT count(1) = 0 FROM pg_subscription "
        "WHERE subtwophasestate NOT IN ('e');"
    )
    assert sub.poll_query_until(
        twophase_query
    ), "Timed out while waiting for subscriber to enable twophase"

    # 2. Temporarily disable the subscription
    sub.safe_psql("ALTER SUBSCRIPTION regress_sub DISABLE")
    oldpub.stop()

    # pg_upgrade should be successful
    pg_bin.command_ok(
        _build_pg_upgrade_cmd(oldpub, newpub, mode),
        "run of pg_upgrade of old cluster",
    )

    # Check that the slot 'regress_sub' has migrated to the new cluster
    newpub.start()
    result = newpub.safe_psql(
        "SELECT slot_name, two_phase, failover FROM pg_replication_slots"
    )
    assert result == "regress_sub|t|t", "check the slot exists on new cluster"

    # Update the connection
    new_connstr = newpub.connstr() + " dbname=postgres"
    sub.safe_psql(
        "ALTER SUBSCRIPTION regress_sub CONNECTION '{}';\n"
        "ALTER SUBSCRIPTION regress_sub ENABLE;".format(new_connstr)
    )

    # Check whether changes on the new publisher get replicated to the subscriber
    newpub.safe_psql("INSERT INTO tbl VALUES (generate_series(11, 20))")
    newpub.wait_for_catchup("regress_sub")
    result = sub.safe_psql("SELECT count(*) FROM tbl")
    assert result == "20", "check changes are replicated to the sub"

    # Clean up
    sub.stop()
    newpub.stop()


def test_003_logical_slots(create_pg, pg_bin, tmp_check, monkeypatch):
    """Upgrade logical replication slots across a same-version pg_upgrade."""
    # Can be changed to test the other modes
    mode = os.environ.get("PG_TEST_PG_UPGRADE_MODE") or "--copy"

    # Initialize old cluster
    oldpub = create_pg("oldpub", start=False, allows_streaming="logical")
    oldpub.append_conf("autovacuum = off", "postgresql.conf")

    # Initialize new cluster
    newpub = create_pg("newpub", start=False, allows_streaming="logical")

    # During upgrade, when pg_restore performs CREATE DATABASE, bgwriter or
    # checkpointer may flush buffers and hold a file handle for the system
    # table.  So, if later due to some reason we need to re-create the file with
    # the same name like a TRUNCATE command on the same table, then the command
    # will fail if OS (such as older Windows versions) doesn't remove an
    # unlinked file completely till it is open.  The probability of seeing this
    # behavior is higher in this test because we use wal_level as logical via
    # allows_streaming => 'logical' which in turn set shared_buffers as 1MB.
    newpub.append_conf(
        "bgwriter_lru_maxpages = 0\ncheckpoint_timeout = 1h",
        "postgresql.conf",
    )

    # In a VPATH build, we'll be started in the source directory, but we want to
    # run pg_upgrade in the build directory so that any files generated finish
    # in it, like delete_old_cluster.{sh,bat}.
    monkeypatch.chdir(tmp_check)

    _check_insufficient_max_replication_slots(pg_bin, oldpub, newpub, mode)
    _check_unconsumed_wal(pg_bin, oldpub, newpub, mode)
    _check_successful_upgrade(create_pg, pg_bin, oldpub, newpub, mode)

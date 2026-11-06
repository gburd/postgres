# Copyright (c) 2024-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long
"""Port of src/bin/pg_basebackup/t/040_pg_createsubscriber.pl.

Use a standby server as the subscriber: build a publisher (node P) and a
streaming standby (node S), exercise pg_createsubscriber's option validation
and unmet-condition checks, then convert node S into a logical subscriber and
verify replication, --clean, --enable-two-phase, failover-slot removal, and
that a new physical standby (node K) of the promoted subscriber starts cleanly.
"""

import glob
import os
import re

import pypg

# Skip BEL, LF, and CR when generating database names from a character range.
_SKIP_CHARS = (7, 10, 13)


def _generate_db(node, prefix, from_char, to_char, suffix):
    """Create a database whose name spans an ASCII range, return the name.

    Mirrors the generate_db helper extracted from 002_pg_upgrade.pl: build a
    name from prefix + characters from_char..to_char (skipping BEL/LF/CR) +
    suffix, then createdb it. On non-Windows the quotes/backslashes are kept
    verbatim (the Windows IPC::Run quoting workaround does not apply here).
    """
    dbname = prefix
    for i in range(from_char, to_char + 1):
        if i in _SKIP_CHARS:
            continue
        dbname += chr(i)
    dbname += suffix
    node.command_ok(
        ["createdb", dbname],
        "created database with ASCII characters from {} to {}".format(
            from_char, to_char
        ),
    )
    return dbname


def _setup_publisher(create_pg):
    """Init node P (publisher) and node F (about-to-fail, force_initdb)."""
    node_p = create_pg("node_p", allows_streaming="logical", start=False)
    # Disable autovacuum to avoid generating xid during stats update as otherwise
    # the new XID could then be replicated to standby at some random point making
    # slots at primary lag behind standby during slot sync.
    node_p.append_conf("autovacuum = off")
    node_p.start()
    # Force node F to initialize a new cluster instead of copying a previously
    # initdb'd cluster. A new cluster has a different system identifier so we can
    # test that the target cluster is a copy of the source cluster.
    node_f = create_pg(
        "node_f", force_initdb=True, allows_streaming="logical", start=False
    )
    return node_p, node_f


def _mandatory_option_failures(pg_bin, datadir):
    """pg_createsubscriber rejects missing/duplicate/mismatched arguments."""
    pg_bin.command_fails(
        ["pg_createsubscriber"], "no subscriber data directory specified"
    )
    pg_bin.command_fails(
        ["pg_createsubscriber", "--pgdata", datadir],
        "no publisher connection string specified",
    )
    base = [
        "pg_createsubscriber",
        "--verbose",
        "--pgdata",
        datadir,
        "--publisher-server",
        "port=5432",
    ]
    pg_bin.command_fails(base, "no database name specified")
    pg_bin.command_fails(
        base + ["--database", "pg1", "--database", "pg1"],
        "duplicate database name",
    )
    pg_bin.command_fails(
        base
        + [
            "--publication",
            "foo1",
            "--publication",
            "foo1",
            "--database",
            "pg1",
            "--database",
            "pg2",
        ],
        "duplicate publication name",
    )
    pg_bin.command_fails(
        base + ["--publication", "foo1", "--database", "pg1", "--database", "pg2"],
        "wrong number of publication names",
    )
    pg_bin.command_fails(
        base
        + [
            "--publication",
            "foo1",
            "--publication",
            "foo2",
            "--subscription",
            "bar1",
            "--database",
            "pg1",
            "--database",
            "pg2",
        ],
        "wrong number of subscription names",
    )
    pg_bin.command_fails(
        base
        + [
            "--publication",
            "foo1",
            "--publication",
            "foo2",
            "--subscription",
            "bar1",
            "--subscription",
            "bar2",
            "--replication-slot",
            "baz1",
            "--database",
            "pg1",
            "--database",
            "pg2",
        ],
        "wrong number of replication slot names",
    )


def _populate_publisher(node_p, db1, db2):
    """Create tables, a row, and a physical slot on the publisher."""
    node_p.safe_psql("CREATE TABLE tbl1 (a text)", dbname=db1)
    node_p.safe_psql("INSERT INTO tbl1 VALUES('first row')", dbname=db1)
    node_p.safe_psql("CREATE TABLE tbl2 (a text)", dbname=db2)
    slotname = "physical_slot"
    node_p.safe_psql(
        "SELECT pg_create_physical_replication_slot('{}')".format(slotname),
        dbname=db2,
    )
    return slotname


def _setup_standby(create_pg, node_p, slotname):
    """Init node S as a streaming standby of node P, return it started."""
    pconnstr = node_p.connstr()
    node_p.backup("backup_1")
    node_s = create_pg(
        "node_s", from_backup=(node_p, "backup_1"), has_streaming=True, start=False
    )
    node_s.append_conf(
        "\nprimary_slot_name = '{}'\n"
        "primary_conninfo = '{} dbname=postgres'\n"
        "hot_standby_feedback = on\n".format(slotname, pconnstr)
    )
    node_s.set_standby_mode()
    node_s.start()
    return node_s


def _promoted_and_about_to_fail_checks(pg_bin, create_pg, node_p, node_s, db1, db2):
    """Failures on a promoted server, a running standby, and a forced-init node."""
    node_t = create_pg(
        "node_t", from_backup=(node_p, "backup_1"), has_streaming=True, start=False
    )
    node_t.set_standby_mode()
    node_t.start()
    node_t.promote()
    node_t.stop()
    pg_bin.command_fails(
        [
            "pg_createsubscriber",
            "--verbose",
            "--dry-run",
            "--pgdata",
            node_t.datadir,
            "--publisher-server",
            node_p.dbname_connstr(db1),
            "--socketdir",
            node_t.host,
            "--subscriber-port",
            node_t.port,
            "--database",
            db1,
            "--database",
            db2,
        ],
        "target server is not in recovery",
    )
    pg_bin.command_fails(
        [
            "pg_createsubscriber",
            "--verbose",
            "--dry-run",
            "--pgdata",
            node_s.datadir,
            "--publisher-server",
            node_p.dbname_connstr(db1),
            "--socketdir",
            node_s.host,
            "--subscriber-port",
            node_s.port,
            "--database",
            db1,
            "--database",
            db2,
        ],
        "standby is up and running",
    )
    return node_t


def _about_to_fail_node_check(pg_bin, node_f, node_p, db1, db2):
    """A node initialized as a fresh cluster is not a copy of the source."""
    pg_bin.command_fails(
        [
            "pg_createsubscriber",
            "--verbose",
            "--pgdata",
            node_f.datadir,
            "--publisher-server",
            node_p.dbname_connstr(db1),
            "--socketdir",
            node_f.host,
            "--subscriber-port",
            node_f.port,
            "--database",
            db1,
            "--database",
            db2,
        ],
        "subscriber data directory is not a copy of the source database cluster",
    )


def _cascading_standby_check(pg_bin, create_pg, node_s, db1, db2):
    """pg_createsubscriber on a cascaded standby (P -> S -> C) fails."""
    node_s.backup("backup_2")
    node_c = create_pg(
        "node_c", from_backup=(node_s, "backup_2"), has_streaming=True, start=False
    )
    node_c.adjust_conf("primary_slot_name", None)
    node_c.set_standby_mode()
    pg_bin.command_fails(
        [
            "pg_createsubscriber",
            "--verbose",
            "--dry-run",
            "--pgdata",
            node_c.datadir,
            "--publisher-server",
            node_s.dbname_connstr(db1),
            "--socketdir",
            node_c.host,
            "--subscriber-port",
            node_c.port,
            "--database",
            db1,
            "--database",
            db2,
        ],
        "primary server is in recovery",
    )


def _unmet_conditions_checks(pg_bin, node_p, node_s, db1, db2):
    """Unmet-condition failures on the primary and on the standby."""
    node_p.append_conf(
        "\nmax_replication_slots = 1\n"
        "max_wal_senders = 1\n"
        "max_worker_processes = 2\n"
    )
    node_p.restart()
    node_s.stop()
    pg_bin.command_fails(
        [
            "pg_createsubscriber",
            "--verbose",
            "--dry-run",
            "--pgdata",
            node_s.datadir,
            "--publisher-server",
            node_p.dbname_connstr(db1),
            "--socketdir",
            node_s.host,
            "--subscriber-port",
            node_s.port,
            "--database",
            db1,
            "--database",
            db2,
        ],
        "primary contains unmet conditions on node P",
    )
    # Restore default settings here but only apply it after testing standby.
    # Some standby settings should not be a lower setting than on the primary.
    node_p.append_conf(
        "\nmax_replication_slots = 10\n"
        "max_wal_senders = 10\n"
        "max_worker_processes = 8\n"
    )
    node_s.append_conf(
        "\nmax_active_replication_origins = 1\n"
        "max_logical_replication_workers = 1\n"
        "max_worker_processes = 2\n"
    )
    pg_bin.command_fails(
        [
            "pg_createsubscriber",
            "--verbose",
            "--dry-run",
            "--pgdata",
            node_s.datadir,
            "--publisher-server",
            node_p.dbname_connstr(db1),
            "--socketdir",
            node_s.host,
            "--subscriber-port",
            node_s.port,
            "--database",
            db1,
            "--database",
            db2,
        ],
        "standby contains unmet conditions on node S",
    )
    node_s.append_conf(
        "\nmax_active_replication_origins = 10\n"
        "max_logical_replication_workers = 4\n"
        "max_worker_processes = 8\n"
    )
    node_p.restart()


def _prepare_objects_for_removal(node_p, node_s, db1):
    """Create a failover slot, dummy subscription, and publications to remove."""
    fslotname = "failover_slot"
    node_p.safe_psql(
        "SELECT pg_create_logical_replication_slot("
        "'{}', 'pgoutput', false, false, true)".format(fslotname),
        dbname=db1,
    )
    node_s.start()
    # Wait for the standby to catch up so that it is not lagging behind the
    # failover slot.
    node_p.wait_for_replay_catchup(node_s)
    node_s.safe_psql("SELECT pg_sync_replication_slots()")
    result = node_s.safe_psql(
        "SELECT slot_name FROM pg_replication_slots "
        "WHERE slot_name = '{}' AND synced AND NOT temporary".format(fslotname)
    )
    assert result == "failover_slot", "failover slot is synced"
    # Insert another row after syncing the logical slot (otherwise the local
    # slot's xmin on the standby could be ahead of the remote slot, failing
    # synchronization).
    node_p.safe_psql("INSERT INTO tbl1 VALUES('second row')", dbname=db1)
    node_p.wait_for_replay_catchup(node_s)
    dummy_sub = "regress_sub_dummy"
    node_p.safe_psql(
        "CREATE SUBSCRIPTION {} CONNECTION 'dbname=dummy' "
        "PUBLICATION pub_dummy WITH (connect=false)".format(dummy_sub),
        dbname=db1,
    )
    node_p.wait_for_replay_catchup(node_s)
    node_p.safe_psql(
        "CREATE PUBLICATION test_pub1 FOR ALL TABLES;\n"
        "CREATE PUBLICATION test_pub2 FOR ALL TABLES;",
        dbname=db1,
    )
    node_p.wait_for_replay_catchup(node_s)
    assert (
        node_s.safe_psql("SELECT COUNT(*) FROM pg_publication", dbname=db1) == "2"
    ), "two pre-existing publications on subscriber"
    node_s.stop()
    return fslotname, dummy_sub


def _dry_run_and_logdir(pg_bin, node_p, node_s, logdir, db1, db2):
    """--dry-run on node S, checking the created log files and their contents."""
    pg_bin.command_ok(
        [
            "pg_createsubscriber",
            "--verbose",
            "--dry-run",
            "--recovery-timeout",
            pypg.test_timeout_default(),
            "--pgdata",
            node_s.datadir,
            "--publisher-server",
            node_p.dbname_connstr(db1),
            "--socketdir",
            node_s.host,
            "--subscriber-port",
            node_s.port,
            "--publication",
            "pub1",
            "--publication",
            "pub2",
            "--subscription",
            "sub1",
            "--subscription",
            "sub2",
            "--database",
            db1,
            "--database",
            db2,
            "--logdir",
            logdir,
        ],
        "run pg_createsubscriber --dry-run on node S",
    )
    server_log_files = glob.glob("{}/*/pg_createsubscriber_server.log".format(logdir))
    assert len(server_log_files) == 1, "pg_createsubscriber_server.log file was created"
    assert (
        os.path.getsize(server_log_files[0]) != 0
    ), "pg_createsubscriber_server.log file not empty"
    server_log = pypg.slurp_file(server_log_files[0])
    assert re.search(
        r"consistent recovery state reached", server_log
    ), "server reached consistent recovery state"
    internal_log_files = glob.glob(
        "{}/*/pg_createsubscriber_internal.log".format(logdir)
    )
    assert (
        len(internal_log_files) == 1
    ), "pg_createsubscriber_internal.log file was created"
    assert (
        os.path.getsize(internal_log_files[0]) != 0
    ), "pg_createsubscriber_internal.log file not empty"
    internal_log = pypg.slurp_file(internal_log_files[0])
    assert re.search(
        r"target server reached the consistent state", internal_log
    ), "log shows consistent state reached"
    node_s.start()
    assert (
        node_s.safe_psql("SELECT pg_catalog.pg_is_in_recovery()") == "t"
    ), "standby is in recovery"
    node_s.stop()


def _no_databases_and_all_failures(pg_bin, node_p, node_s, db1):
    """--dry-run without --database succeeds; --database/--publication + --all fail."""
    pg_bin.command_ok(
        [
            "pg_createsubscriber",
            "--verbose",
            "--dry-run",
            "--pgdata",
            node_s.datadir,
            "--publisher-server",
            node_p.dbname_connstr(db1),
            "--socketdir",
            node_s.host,
            "--subscriber-port",
            node_s.port,
            "--replication-slot",
            "replslot1",
        ],
        "run pg_createsubscriber without --databases",
    )
    pg_bin.command_fails_like(
        [
            "pg_createsubscriber",
            "--verbose",
            "--pgdata",
            node_s.datadir,
            "--publisher-server",
            node_p.dbname_connstr(db1),
            "--socketdir",
            node_s.host,
            "--subscriber-port",
            node_s.port,
            "--database",
            db1,
            "--all",
        ],
        r"options --database and -a/--all cannot be used together",
        "fail if --database is used with --all",
    )
    pg_bin.command_fails_like(
        [
            "pg_createsubscriber",
            "--verbose",
            "--dry-run",
            "--pgdata",
            node_s.datadir,
            "--publisher-server",
            node_p.dbname_connstr(db1),
            "--socketdir",
            node_s.host,
            "--subscriber-port",
            node_s.port,
            "--all",
            "--publication",
            "pub1",
        ],
        r"options --publication and -a/--all cannot be used together",
        "fail if --publication is used with --all",
    )


def _all_option_counts(pg_bin, node_p, node_s):
    """--all dry-run reports objects for postgres + the two extra databases."""
    result = pg_bin.run_command(
        [
            "pg_createsubscriber",
            "--verbose",
            "--dry-run",
            "--recovery-timeout",
            pypg.test_timeout_default(),
            "--pgdata",
            node_s.datadir,
            "--publisher-server",
            node_p.connstr(),
            "--socketdir",
            node_s.host,
            "--subscriber-port",
            node_s.port,
            "--all",
        ]
    )
    stderr = result.stderr
    # The expected count 3 refers to postgres, db1 and db2 databases.
    assert (
        len(re.findall(r"would create publication", stderr)) == 3
    ), "verify publications are created for all databases"
    assert (
        len(re.findall(r"would create the replication slot", stderr)) == 3
    ), "verify replication slots are created for all databases"
    assert (
        len(re.findall(r"would create subscription", stderr)) == 3
    ), "verify subscriptions are created for all databases"


def _run_on_standby(pg_bin, node_p, node_s, db1, db2):
    """Convert node S with --enable-two-phase and --clean publications."""
    node_p.safe_psql(
        "CREATE PUBLICATION test_pub3 FOR TABLE tbl1;\n"
        "CREATE TABLE not_replicated (a int);",
        dbname=db1,
    )
    pg_bin.command_ok(
        [
            "pg_createsubscriber",
            "--verbose",
            "--verbose",
            "--recovery-timeout",
            pypg.test_timeout_default(),
            "--pgdata",
            node_s.datadir,
            "--publisher-server",
            node_p.dbname_connstr(db1),
            "--socketdir",
            node_s.host,
            "--subscriber-port",
            node_s.port,
            "--publication",
            "test_pub3",
            "--publication",
            "pub2",
            "--replication-slot",
            "replslot1",
            "--replication-slot",
            "replslot2",
            "--database",
            db1,
            "--database",
            db2,
            "--enable-two-phase",
            "--clean",
            "publications",
        ],
        "run pg_createsubscriber on node S",
    )
    assert os.path.isfile(
        node_s.datadir / "pg_createsubscriber.conf.disabled"
    ), "pg_createsubscriber.conf.disabled exists in node S"


def _verify_results(node_p, node_s, db1, db2, slotname, dummy_sub, fslotname):
    """Verify slot removal, replication content, two-phase, and publications."""
    result = node_p.safe_psql(
        "SELECT count(*) FROM pg_replication_slots "
        "WHERE slot_name = '{}'".format(slotname),
        dbname=db1,
    )
    assert (
        result == "0"
    ), "the physical replication slot used as primary_slot_name has been removed"
    node_p.safe_psql("INSERT INTO tbl1 VALUES('third row')", dbname=db1)
    node_p.safe_psql("INSERT INTO tbl2 VALUES('row 1')", dbname=db2)
    node_p.safe_psql("INSERT INTO not_replicated VALUES(0)", dbname=db1)
    node_s.start()
    assert (
        node_s.safe_psql("SELECT COUNT(*) FROM pg_publication", dbname=db1) == "0"
    ), "all publications were removed from db1"
    assert (
        node_s.safe_psql("SELECT COUNT(*) FROM pg_publication", dbname=db2) == "0"
    ), "all publications were removed from db2"
    assert (
        node_s.safe_psql(
            "SELECT count(1) = 0 FROM pg_subscription WHERE subtwophasestate = 'd'"
        )
        == "t"
    ), "subscriptions are created with the two-phase option enabled"
    result = node_s.safe_psql(
        "SELECT count(*) FROM pg_subscription WHERE subname = '{}'".format(dummy_sub)
    )
    assert result == "0", "pre-existing subscription was dropped"
    result = node_s.safe_psql(
        "SELECT subname FROM pg_subscription WHERE subname ~ '^pg_createsubscriber_'"
    )
    subnames = result.split("\n")
    node_s.wait_for_subscription_sync(node_p, subnames[0])
    node_s.wait_for_subscription_sync(node_p, subnames[1])
    result = node_s.safe_psql(
        "SELECT count(*) FROM pg_replication_slots "
        "WHERE slot_name = '{}'".format(fslotname),
        dbname=db1,
    )
    assert result == "0", "failover slot was removed"
    assert (
        node_s.safe_psql("SELECT * FROM tbl1", dbname=db1)
        == "first row\nsecond row\nthird row"
    ), "logical replication works in database {}".format(db1)
    assert (
        node_s.safe_psql("SELECT * FROM not_replicated", dbname=db1) == ""
    ), "table is not replicated in database {}".format(db1)
    assert (
        node_s.safe_psql("SELECT * FROM tbl2", dbname=db2) == "row 1"
    ), "logical replication works in database {}".format(db2)


def _verify_identity_and_publications(node_p, node_s, db1, db2):
    """System identifier changed; publications/subscriptions are correct."""
    sysid_p = node_p.safe_psql("SELECT system_identifier FROM pg_control_system()")
    sysid_s = node_s.safe_psql("SELECT system_identifier FROM pg_control_system()")
    assert sysid_p != sysid_s, "system identifier was changed"
    assert (
        node_p.safe_psql(
            "SELECT COUNT(*) FROM pg_publication WHERE pubname = 'pub2'",
            dbname=db2,
        )
        == "1"
    ), "publication pub2 was created in {}".format(db2)
    result = node_s.safe_psql(
        "SELECT subname, subpublications FROM pg_subscription "
        "WHERE subname ~ '^pg_createsubscriber_'\n"
        "ORDER BY subpublications;"
    )
    assert re.search(
        r"^pg_createsubscriber_\d+_[0-9a-f]+ \|\{pub2\}\n"
        r"\s*pg_createsubscriber_\d+_[0-9a-f]+ \|\{test_pub3\}$",
        result,
        re.VERBOSE,
    ), "subscription and publication names are ok"
    result = node_s.safe_psql(
        "SELECT d.datname, s.subpublications\n"
        "FROM pg_subscription s\n"
        "JOIN pg_database d ON d.oid = s.subdbid\n"
        "WHERE subname ~ '^pg_createsubscriber_'\n"
        "ORDER BY s.subdbid"
    )
    assert result == "{}|{{test_pub3}}\n{}|{{pub2}}".format(
        db1, db2
    ), "subscriptions use the correct publications"


def _physical_standby_of_subscriber(pg_bin, create_pg, node_s, slotname):
    """A new physical standby (node K) of the promoted subscriber starts cleanly."""
    sconnstr = node_s.connstr()
    node_s.safe_psql(
        "SELECT pg_create_physical_replication_slot('{}');".format(slotname)
    )
    node_s.backup("backup_3")
    node_k = create_pg(
        "node_k", from_backup=(node_s, "backup_3"), has_streaming=True, start=False
    )
    assert os.path.isfile(
        node_k.datadir / "pg_createsubscriber.conf.disabled"
    ), "pg_createsubscriber.conf.disabled exists in node K"
    node_k.append_conf(
        "\nprimary_slot_name = '{}'\n"
        "primary_conninfo = '{} dbname=postgres'\n"
        "hot_standby_feedback = on\n".format(slotname, sconnstr)
    )
    node_k.set_standby_mode()
    node_k_name = node_s.name
    pg_bin.command_ok(
        [
            "pg_ctl",
            "--wait",
            "--pgdata",
            node_k.datadir,
            "--log",
            node_k.log,
            "--options",
            "--cluster-name={}".format(node_k_name),
            "start",
        ],
        "node K has started",
    )
    # A direct pg_ctl stop is used rather than node.stop(), because the node's
    # postmaster PID was not tracked (it was not started via node.start()).
    pg_bin.run_command(["pg_ctl", "stop", "--pgdata", node_k.datadir])


def test_040_pg_createsubscriber(create_pg, pg_bin, tmp_path):
    """Convert a standby into a logical subscriber and verify the result."""
    pg_bin.program_help_ok("pg_createsubscriber")
    pg_bin.program_version_ok("pg_createsubscriber")
    pg_bin.program_options_handling_ok("pg_createsubscriber")

    datadir = str(tmp_path / "datadir")
    logdir = str(tmp_path / "logdir")
    os.mkdir(datadir)
    os.mkdir(logdir)

    _mandatory_option_failures(pg_bin, datadir)

    node_p, node_f = _setup_publisher(create_pg)
    db1 = _generate_db(node_p, 'regression\\"\\', 1, 45, '\\\\"\\\\\\')
    db2 = _generate_db(node_p, "regression", 46, 90, "")
    slotname = _populate_publisher(node_p, db1, db2)
    node_s = _setup_standby(create_pg, node_p, slotname)

    _promoted_and_about_to_fail_checks(pg_bin, create_pg, node_p, node_s, db1, db2)
    _about_to_fail_node_check(pg_bin, node_f, node_p, db1, db2)
    _cascading_standby_check(pg_bin, create_pg, node_s, db1, db2)
    _unmet_conditions_checks(pg_bin, node_p, node_s, db1, db2)

    fslotname, dummy_sub = _prepare_objects_for_removal(node_p, node_s, db1)
    _dry_run_and_logdir(pg_bin, node_p, node_s, logdir, db1, db2)
    _no_databases_and_all_failures(pg_bin, node_p, node_s, db1)
    _all_option_counts(pg_bin, node_p, node_s)
    _run_on_standby(pg_bin, node_p, node_s, db1, db2)

    _verify_results(node_p, node_s, db1, db2, slotname, dummy_sub, fslotname)
    _verify_identity_and_publications(node_p, node_s, db1, db2)
    _physical_standby_of_subscriber(pg_bin, create_pg, node_s, slotname)

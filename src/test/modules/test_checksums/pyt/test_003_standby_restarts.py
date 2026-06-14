# Copyright (c) 2024-2026, PostgreSQL Global Development Group

"""Port of src/test/modules/test_checksums/t/003_standby_restarts.pl.

Online checksum changes propagate correctly to a streaming standby: enabling on
the primary moves the standby through inprogress-on to on, disabling moves it
back, unlogged relations behave correctly across the transition and a
promotion, and a promotion while still inprogress-on leaves the new primary with
checksums off. No page-verification errors on either node.
"""

import os
import re

import pypg

import datachecksums_utils as dcu  # pyrefly: ignore

_NO_CSUM_ERR = r"page verification failed,.+\d$"


def _read_back(node, expected, msg):
    assert node.safe_psql("SELECT count(a) FROM t WHERE a > 1") == expected, msg


def test_003_standby_restarts(create_pg):
    """Checksum enable/disable propagates to a streaming standby correctly."""
    primary = create_pg(
        "standby_restarts_primary", allows_streaming=True, no_data_checksums=True
    )
    slotname = "physical_slot"
    primary.safe_psql(
        "SELECT pg_create_physical_replication_slot('{}')".format(slotname)
    )
    backup_name = "my_backup"
    primary.backup(backup_name)
    standby = create_pg(
        "standby_restarts_standby",
        from_backup=(primary, backup_name),
        has_streaming=True,
        start=False,
    )
    standby.append_conf("\nprimary_slot_name = '{}'\n".format(slotname))
    standby.start()
    primary.safe_psql("CREATE TABLE t AS SELECT generate_series(1,10000) AS a;")
    primary.wait_for_catchup(standby, "replay", primary.lsn("insert"))
    dcu.test_checksum_state(primary, "off")
    dcu.test_checksum_state(standby, "off")
    dcu.enable_data_checksums(primary)
    assert primary.poll_query_until(
        "SELECT setting = 'off' FROM pg_catalog.pg_settings "
        "WHERE name = 'data_checksums';",
        "f",
    ), "ensure primary has transitioned from off"
    primary.wait_for_catchup(standby, "replay")
    assert standby.poll_query_until(
        "SELECT setting = 'off' FROM pg_catalog.pg_settings "
        "WHERE name = 'data_checksums';",
        "f",
    ), "ensure standby has absorbed the inprogress-on barrier"
    state = standby.safe_psql(
        "SELECT setting FROM pg_catalog.pg_settings WHERE name = 'data_checksums';"
    )
    assert state in (
        "inprogress-on",
        "on",
    ), "ensure checksums are on, or in progress, on standby_1"
    primary.safe_psql("INSERT INTO t VALUES (generate_series(1, 10000));")
    dcu.wait_for_checksum_state(primary, "on")
    dcu.wait_for_checksum_state(standby, "on")
    _read_back(primary, "19998", "ensure we can safely read all data with checksums")
    assert primary.poll_query_until(
        "SELECT count(*) FROM pg_stat_activity "
        "WHERE backend_type LIKE 'datachecksums%';",
        "0",
    ), "await datachecksums worker/launcher termination"
    dcu.disable_data_checksums(primary)
    primary.wait_for_catchup(standby, "replay")
    dcu.wait_for_checksum_state(primary, "off")
    dcu.wait_for_checksum_state(standby, "off")
    _read_back(primary, "19998", "ensure we can safely read all data without checksums")
    _unlogged_checks(primary, standby)
    _promote_inprogress(create_pg, primary, slotname)


def _unlogged_checks(primary, standby):
    """Unlogged relations behave correctly across a checksum enable + promote."""
    primary.safe_psql(
        "CREATE UNLOGGED TABLE unlogged_tbl AS SELECT generate_series(1,1000) AS a;"
    )
    primary.safe_psql(
        "CREATE UNLOGGED TABLE unlogged_promo (id int PRIMARY KEY, payload text);\n"
        "INSERT INTO unlogged_promo SELECT g, repeat('x', 100) "
        "FROM generate_series(1, 1000) g;\n"
        "CREATE INDEX unlogged_promo_payload_idx ON unlogged_promo (payload);"
    )
    primary.wait_for_catchup(standby, "replay", primary.lsn("insert"))
    unlogged_rfn = primary.safe_psql(
        "SELECT relfilenode FROM pg_class WHERE relname = 'unlogged_tbl';"
    )
    db_oid = primary.safe_psql(
        "SELECT oid FROM pg_database WHERE datname = 'postgres';"
    )
    main_fork = "{}/base/{}/{}".format(standby.datadir, db_oid, unlogged_rfn)
    assert not os.path.isfile(
        main_fork
    ), "standby has no main fork for unlogged table before enable"
    dcu.enable_data_checksums(primary, wait="on")
    dcu.wait_for_checksum_state(standby, "on")
    primary.wait_for_catchup(standby, "replay", primary.lsn("insert"))
    assert not os.path.isfile(
        main_fork
    ), "standby has no main fork for unlogged table after enable"
    assert (
        standby.safe_psql("SELECT pg_relation_size('unlogged_tbl', 'main');") == "0"
    ), "unlogged table has zero size on standby after checksum enable"
    assert (
        primary.safe_psql("SELECT count(*) FROM unlogged_tbl;") == "1000"
    ), "unlogged table readable on primary after checksum enable"
    primary.safe_psql("ALTER TABLE unlogged_tbl SET logged;")
    primary.wait_for_catchup(standby, "replay", primary.lsn("insert"))
    assert (
        primary.safe_psql("SELECT sum(a) FROM unlogged_tbl;") == "500500"
    ), "previously unlogged table can be read on primary"
    assert (
        standby.safe_psql("SELECT sum(a) FROM unlogged_tbl;") == "500500"
    ), "previously unlogged table can be read on standby"
    primary.stop()
    standby.promote()
    assert (
        standby.safe_psql("SELECT count(*) FROM unlogged_promo;") == "0"
    ), "unlogged table readable on promoted standby (truncated as expected)"
    standby.safe_psql(
        "INSERT INTO unlogged_promo SELECT g, repeat('y',100) "
        "FROM generate_series(1,100) g;"
    )
    assert (
        standby.safe_psql(
            "SET enable_seqscan = off; SELECT id FROM unlogged_promo WHERE id = 50;"
        )
        == "50"
    ), "indexed lookup on promoted standby returns expected row"
    standby.stop()
    _assert_clean_log(primary, "primary")
    _assert_clean_log(standby, "standby")
    standby.clean_node()
    primary.start()


def _promote_inprogress(create_pg, primary, slotname):
    """Promotion while still inprogress-on leaves the new primary with off."""
    dcu.disable_data_checksums(primary, wait="off")
    backup_name = "my_new_backup"
    primary.backup(backup_name)
    standby = create_pg(
        "standby_restarts_standby2",
        from_backup=(primary, backup_name),
        has_streaming=True,
        start=False,
    )
    standby.append_conf("\nprimary_slot_name = '{}'\n".format(slotname))
    standby.start()
    primary.wait_for_catchup(standby, "replay")
    primary_bpsql = primary.background_psql("postgres")
    primary_bpsql.query_safe("CREATE TEMPORARY TABLE tt (a integer);")
    standby_bpsql = standby.background_psql("postgres")
    dcu.enable_data_checksums(primary, wait="inprogress-on")
    primary.wait_for_catchup(standby, "replay")
    dcu.test_checksum_state(standby, "inprogress-on")
    primary.teardown_node()
    standby.promote()
    dcu.wait_for_checksum_state(standby, "off")
    assert (
        standby_bpsql.query_safe("SHOW data_checksums;").strip() == "off"
    ), "ensure checksums are set to off after promotion during inprogress-on"
    standby_bpsql.quit()
    primary_bpsql.quit()
    standby.stop()


def _assert_clean_log(node, label):
    log = pypg.slurp_file(node.log, 0)
    assert not re.search(
        _NO_CSUM_ERR, log, re.MULTILINE
    ), "no checksum validation errors in {} log".format(label)

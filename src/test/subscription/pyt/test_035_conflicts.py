# Copyright (c) 2025-2026, PostgreSQL Global Development Group

"""Port of src/test/subscription/t/035_conflicts.pl.

Conflict detection in logical replication: multiple_unique_conflicts on
INSERT/UPDATE (including a leaf partition), and a bidirectional setup that
exercises delete_origin_differs / update_deleted conflicts, the
pg_conflict_detection slot lifecycle, retain_dead_tuples DDL rules,
max_retention_duration stop/resume, and (with injection_points) retention of a
deleted tuple across a DELAY_CHKPT_IN_COMMIT prepared transaction.
"""

import re

import pypg

_MUC_INSERT = (
    r"conflict detected on relation \"public.conf_tab\": "
    r"conflict=multiple_unique_conflicts.*\n"
    r".*Could not apply remote change: remote row \(2, 3, 4\).*\n"
    r".*Key already exists in unique index \"conf_tab_pkey\", modified in "
    r"transaction .*: key \(a\)=\(2\), local row \(2, 2, 2\).*\n"
    r".*Key already exists in unique index \"conf_tab_b_key\", modified in "
    r"transaction .*: key \(b\)=\(3\), local row \(3, 3, 3\).*\n"
    r".*Key already exists in unique index \"conf_tab_c_key\", modified in "
    r"transaction .*: key \(c\)=\(4\), local row \(4, 4, 4\)."
)
_MUC_UPDATE = (
    r"conflict detected on relation \"public.conf_tab\": "
    r"conflict=multiple_unique_conflicts.*\n"
    r".*Could not apply remote change: remote row \(6, 7, 8\), "
    r"replica identity \(a\)=\(5\).*\n"
    r".*Key already exists in unique index \"conf_tab_pkey\", modified in "
    r"transaction .*: key \(a\)=\(6\), local row \(6, 6, 6\).*\n"
    r".*Key already exists in unique index \"conf_tab_b_key\", modified in "
    r"transaction .*: key \(b\)=\(7\), local row \(7, 7, 7\).*\n"
    r".*Key already exists in unique index \"conf_tab_c_key\", modified in "
    r"transaction .*: key \(c\)=\(8\), local row \(8, 8, 8\)."
)
_MUC_PARTITION = (
    r"conflict detected on relation \"public.conf_tab_2_p1\": "
    r"conflict=multiple_unique_conflicts.*\n"
    r".*Could not apply remote change: remote row \(55, 2, 3\).*\n"
    r".*Key already exists in unique index \"conf_tab_2_p1_pkey\", modified in "
    r"transaction .*: key \(a\)=\(55\), local row \(55, 2, 3\).*\n"
    r".*Key already exists in unique index \"conf_tab_2_p1_a_b_key\", modified "
    r"in transaction .*: key \(a, b\)=\(55, 2\), local row \(55, 2, 3\)."
)
_DELETE_ORIGIN_DIFFERS = (
    r'conflict detected on relation "public.tab": conflict=delete_origin_differs.*\n'
    r".*DETAIL:.* Deleting the row that was modified locally in transaction "
    r"[0-9]+ at .*: local row \(1, 3\), replica identity \(a\)=\(1\)."
)
_UPDATE_DELETED = (
    r'conflict detected on relation "public.tab": conflict=update_deleted.*\n'
    r".*DETAIL:.* Could not find the row to be updated: remote row \(1, 3\), "
    r"replica identity \(a\)=\(1\).\n"
    r".*The row to be updated was deleted locally in transaction [0-9]+ at .*"
)
_UPDATE_DELETED_FULL = (
    r'conflict detected on relation "public.tab": conflict=update_deleted.*\n'
    r".*DETAIL:.* Could not find the row to be updated: remote row \(2, 4\), "
    r"replica identity full \(2, 2\).*\n"
    r".*The row to be updated was deleted locally in transaction [0-9]+ at .*"
)
_UPDATE_DELETED_INJ = (
    r'conflict detected on relation "public.tab": conflict=update_deleted.*\n'
    r".*DETAIL:.* Could not find the row to be updated: remote row \(1, 2\), "
    r"replica identity full \(1, 1\).*\n"
    r".*The row to be updated was deleted locally in transaction [0-9]+ at .*"
)
_RETENTION_RESUME = (
    r'logical replication worker for subscription "tap_sub_a_b" will resume '
    r"retaining the information for detecting conflicts\n"
    r".*DETAIL:.* Retention is re-enabled because max_retention_duration has "
    r"been set to unlimited.*"
)

_APPLY_WORKER_STOPPED = (
    "SELECT count(*) = 0 FROM pg_stat_activity "
    "WHERE backend_type = 'logical replication apply worker'"
)
_SLOT = "pg_replication_slots WHERE slot_name = 'pg_conflict_detection'"


def _setup_unidirectional(create_pg):
    """Create pub/sub nodes, tables, publication and subscription."""
    publisher = create_pg("publisher", allows_streaming="logical")
    subscriber = create_pg("subscriber", allows_streaming="logical")

    publisher.safe_psql(
        "CREATE TABLE conf_tab (a int PRIMARY KEY, b int UNIQUE, c int UNIQUE);"
    )
    publisher.safe_psql(
        "CREATE TABLE conf_tab_2 (a int PRIMARY KEY, b int UNIQUE, c int UNIQUE);"
    )
    subscriber.safe_psql(
        "CREATE TABLE conf_tab (a int PRIMARY key, b int UNIQUE, c int UNIQUE);"
    )
    subscriber.safe_psql(
        "CREATE TABLE conf_tab_2 (a int PRIMARY KEY, b int, c int, unique(a,b)) "
        "PARTITION BY RANGE (a);\n"
        "CREATE TABLE conf_tab_2_p1 PARTITION OF conf_tab_2 "
        "FOR VALUES FROM (MINVALUE) TO (100);"
    )
    connstr = publisher.connstr() + " dbname=postgres"
    publisher.safe_psql("CREATE PUBLICATION pub_tab FOR TABLE conf_tab, conf_tab_2")
    appname = "sub_tab"
    subscriber.safe_psql(
        "CREATE SUBSCRIPTION sub_tab\n"
        "CONNECTION '{} application_name={}'\n"
        "PUBLICATION pub_tab;".format(connstr, appname)
    )
    subscriber.wait_for_subscription_sync(publisher, appname)
    return publisher, subscriber, appname


def _test_multiple_unique_conflicts(publisher, subscriber):
    """multiple_unique_conflicts on INSERT, UPDATE and a leaf partition."""
    publisher.safe_psql("INSERT INTO conf_tab VALUES (1,1,1);")
    subscriber.safe_psql("INSERT INTO conf_tab VALUES (2,2,2), (3,3,3), (4,4,4);")

    offset = subscriber.current_log_position()
    publisher.safe_psql("INSERT INTO conf_tab VALUES (2,3,4);")
    subscriber.wait_for_log(_MUC_INSERT, offset)
    subscriber.safe_psql("TRUNCATE conf_tab;")

    offset = subscriber.current_log_position()
    publisher.safe_psql("INSERT INTO conf_tab VALUES (5,5,5);")
    subscriber.safe_psql("INSERT INTO conf_tab VALUES (6,6,6), (7,7,7), (8,8,8);")
    publisher.safe_psql("UPDATE conf_tab set a=6, b=7, c=8 where a=5;")
    subscriber.wait_for_log(_MUC_UPDATE, offset)
    subscriber.safe_psql("TRUNCATE conf_tab;")

    subscriber.safe_psql("INSERT INTO conf_tab_2 VALUES (55,2,3);")
    publisher.safe_psql("INSERT INTO conf_tab_2 VALUES (55,2,3);")
    subscriber.wait_for_log(_MUC_PARTITION, offset)


def _setup_bidirectional(node_a, node_b):
    """Set up bidirectional replication of table tab between node_a/node_b."""
    node_a.append_conf(
        "track_commit_timestamp = on\nautovacuum = off\nlog_min_messages = 'debug2'"
    )
    node_a.restart()
    node_b.append_conf("track_commit_timestamp = on")
    node_b.restart()

    node_a.safe_psql("CREATE TABLE tab (a int PRIMARY KEY, b int)")
    node_b.safe_psql("CREATE TABLE tab (a int PRIMARY KEY, b int)")

    subname_ab, subname_ba = "tap_sub_a_b", "tap_sub_b_a"
    a_connstr = node_a.connstr() + " dbname=postgres"
    node_a.safe_psql("CREATE PUBLICATION tap_pub_A FOR TABLE tab")
    node_b.safe_psql(
        "CREATE SUBSCRIPTION {sub}\n"
        "CONNECTION '{conn} application_name={sub}'\n"
        "PUBLICATION tap_pub_A\n"
        "WITH (origin = none, retain_dead_tuples = true)".format(
            sub=subname_ba, conn=a_connstr
        )
    )
    b_connstr = node_b.connstr() + " dbname=postgres"
    node_b.safe_psql("CREATE PUBLICATION tap_pub_B FOR TABLE tab")
    node_a.safe_psql(
        "CREATE SUBSCRIPTION {sub}\n"
        "CONNECTION '{conn} application_name={sub}'\n"
        "PUBLICATION tap_pub_B\n"
        "WITH (origin = none, copy_data = off)".format(sub=subname_ab, conn=b_connstr)
    )
    node_a.wait_for_subscription_sync(node_b, subname_ab)
    node_b.wait_for_subscription_sync(node_a, subname_ba)
    assert node_b.poll_query_until(
        "SELECT xmin IS NOT NULL from {}".format(_SLOT)
    ), "the xmin value of slot 'pg_conflict_detection' is valid on Node B"
    return subname_ab, subname_ba


def _test_retain_dead_tuples_ddl(node_a, subname_ab):
    """retain_dead_tuples DDL rules and the origin=any warning."""
    result = node_a.psql_capture(
        "ALTER SUBSCRIPTION {} SET (retain_dead_tuples = true)".format(subname_ab),
        on_error_stop=False,
    )
    assert re.search(
        r'ERROR:  cannot set option "retain_dead_tuples" for enabled subscription',
        result.stderr,
    ), "altering retain_dead_tuples is not allowed for enabled subscription"

    node_a.psql_capture(
        "ALTER SUBSCRIPTION {} DISABLE;".format(subname_ab), on_error_stop=False
    )
    node_a.poll_query_until(_APPLY_WORKER_STOPPED)

    result = node_a.psql_capture(
        "ALTER SUBSCRIPTION {} SET (retain_dead_tuples = true);".format(subname_ab),
        on_error_stop=False,
    )
    assert re.search(
        r"NOTICE:  deleted rows to detect conflicts would not be removed until "
        r"the subscription is enabled",
        result.stderr,
    ), "altering retain_dead_tuples is allowed for disabled subscription"

    node_a.safe_psql("ALTER SUBSCRIPTION {} ENABLE;".format(subname_ab))
    assert node_a.poll_query_until(
        "SELECT xmin IS NOT NULL from {}".format(_SLOT)
    ), "the xmin value of slot 'pg_conflict_detection' is valid on Node A"

    result = node_a.psql_capture(
        "ALTER SUBSCRIPTION {} SET (origin = any);".format(subname_ab),
        on_error_stop=False,
    )
    assert re.search(
        r'WARNING:  subscription "tap_sub_a_b" enabled retain_dead_tuples but '
        r"might not reliably detect conflicts for changes from different origins",
        result.stderr,
    ), "warn of receiving changes from origins other than the publisher"
    node_a.psql_capture(
        "ALTER SUBSCRIPTION {} SET (origin = none);".format(subname_ab),
        on_error_stop=False,
    )


def _test_update_deleted_conflicts(node_a, node_b, subname_ab, subname_ba):
    """delete_origin_differs and update_deleted conflicts, xmin advancement."""
    node_a.safe_psql("INSERT INTO tab VALUES (1, 1), (2, 2);")
    node_a.wait_for_catchup(subname_ba)
    assert (
        node_b.safe_psql("SELECT * FROM tab;") == "1|1\n2|2"
    ), "check replicated insert on node B"

    node_a.safe_psql("ALTER SUBSCRIPTION {} DISABLE".format(subname_ab))
    node_a.poll_query_until(_APPLY_WORKER_STOPPED)

    log_location = node_b.current_log_position()
    node_b.safe_psql("UPDATE tab SET b = 3 WHERE a = 1;")
    node_a.safe_psql("DELETE FROM tab WHERE a = 1;")
    result = node_a.psql_capture("VACUUM (verbose) public.tab;", on_error_stop=False)
    assert re.search(
        r"1 are dead but not yet removable", result.stderr
    ), "the deleted column is non-removable"
    node_a.wait_for_catchup(subname_ba)
    logfile = pypg.slurp_file(node_b.log, log_location)
    assert re.search(
        _DELETE_ORIGIN_DIFFERS, logfile
    ), "delete target row was modified in tab"

    log_location = node_a.current_log_position()
    node_a.safe_psql("ALTER SUBSCRIPTION {} ENABLE;".format(subname_ab))
    node_b.wait_for_catchup(subname_ab)
    logfile = pypg.slurp_file(node_a.log, log_location)
    assert re.search(_UPDATE_DELETED, logfile), "update target row was deleted in tab"

    next_xid = node_a.safe_psql("SELECT txid_current() + 1;")
    assert node_a.poll_query_until(
        "SELECT xmin = {} from {}".format(next_xid, _SLOT)
    ), "the xmin value of slot 'pg_conflict_detection' is updated on Node A"


def _test_seqscan_deleted_tuple(node_a, node_b, subname_ab):
    """update_deleted via sequential scan with REPLICA IDENTITY FULL."""
    node_a.safe_psql("ALTER TABLE tab REPLICA IDENTITY FULL")
    node_b.safe_psql("ALTER TABLE tab REPLICA IDENTITY FULL")
    node_a.safe_psql("ALTER TABLE tab DROP CONSTRAINT tab_pkey;")

    node_a.safe_psql("ALTER SUBSCRIPTION {} DISABLE".format(subname_ab))
    node_a.poll_query_until(_APPLY_WORKER_STOPPED)

    node_b.safe_psql("UPDATE tab SET b = 4 WHERE a = 2;")
    node_a.safe_psql("DELETE FROM tab WHERE a = 2;")

    log_location = node_a.current_log_position()
    node_a.safe_psql("ALTER SUBSCRIPTION {} ENABLE;".format(subname_ab))
    node_b.wait_for_catchup(subname_ab)
    logfile = pypg.slurp_file(node_a.log, log_location)
    assert re.search(
        _UPDATE_DELETED_FULL, logfile
    ), "update target row was deleted in tab"


def _test_xmin_advance_no_tables(node_a, node_b, subname_ab):
    """The slot xmin advances when the subscription has no tables."""
    node_b.safe_psql("ALTER PUBLICATION tap_pub_B DROP TABLE tab")
    node_a.safe_psql("ALTER SUBSCRIPTION {} REFRESH PUBLICATION".format(subname_ab))
    next_xid = node_a.safe_psql("SELECT txid_current() + 1;")
    assert node_a.poll_query_until(
        "SELECT xmin = {} from {}".format(next_xid, _SLOT)
    ), "the xmin value of slot 'pg_conflict_detection' is updated on Node A"
    node_b.safe_psql("ALTER PUBLICATION tap_pub_B ADD TABLE tab")
    node_a.safe_psql(
        "ALTER SUBSCRIPTION {} REFRESH PUBLICATION WITH (copy_data = false)".format(
            subname_ab
        )
    )


def _test_delay_chkpt_injection(node_a, node_b, subname_ab, subname_ba):
    """DELAY_CHKPT_IN_COMMIT prepared txn retains a concurrently-deleted tuple."""
    node_b.append_conf(
        "shared_preload_libraries = 'injection_points'\nmax_prepared_transactions = 1"
    )
    node_b.restart()
    node_b.psql_capture(
        "ALTER SUBSCRIPTION {} DISABLE;".format(subname_ba), on_error_stop=False
    )
    node_b.poll_query_until(_APPLY_WORKER_STOPPED)
    node_b.safe_psql("TRUNCATE tab;\nINSERT INTO tab VALUES(1, 1);")
    node_b.wait_for_catchup(subname_ab)
    node_b.safe_psql(
        "CREATE EXTENSION injection_points;\n"
        "SELECT injection_points_attach('commit-after-delay-checkpoint', 'wait');"
    )
    pub_session = node_b.background_psql("postgres")
    pub_session.query_until(
        r"starting_bg_psql",
        "\\echo starting_bg_psql\n"
        "BEGIN;\n"
        "UPDATE tab SET b = 2 WHERE a = 1;\n"
        "PREPARE TRANSACTION 'txn_with_later_commit_ts';\n"
        "COMMIT PREPARED 'txn_with_later_commit_ts';\n",
    )
    node_b.wait_for_event("client backend", "commit-after-delay-checkpoint")
    assert (
        node_b.safe_psql("SELECT * FROM tab WHERE a = 1") == "1|1"
    ), "publisher sees the old row"

    node_a.safe_psql("DELETE FROM tab WHERE a = 1;")
    sub_ts = node_a.safe_psql("SELECT timestamp FROM pg_last_committed_xact();")

    log_location = node_a.current_log_position()
    node_a.wait_for_log(r"sending publisher status request message", log_location)
    log_location = node_a.current_log_position()
    node_a.wait_for_log(r"sending publisher status request message", log_location)

    result = node_a.psql_capture("VACUUM (verbose) public.tab;", on_error_stop=False)
    assert re.search(
        r"1 are dead but not yet removable", result.stderr
    ), "the deleted column is non-removable"

    log_location = node_a.current_log_position()
    node_b.safe_psql(
        "SELECT injection_points_wakeup('commit-after-delay-checkpoint');\n"
        "SELECT injection_points_detach('commit-after-delay-checkpoint');"
    )
    assert pub_session.quit() == 0, "close publisher session"
    assert (
        node_b.safe_psql("SELECT * FROM tab WHERE a = 1") == "1|2"
    ), "publisher sees the new row"
    node_b.wait_for_catchup(subname_ab)
    logfile = pypg.slurp_file(node_a.log, log_location)
    assert re.search(
        _UPDATE_DELETED_INJ, logfile
    ), "update target row was deleted in tab"

    next_xid = node_a.safe_psql("SELECT txid_current() + 1;")
    assert node_a.poll_query_until(
        "SELECT xmin = {} from {}".format(next_xid, _SLOT)
    ), "the xmin value of slot 'pg_conflict_detection' is updated on subscriber"

    pub_ts = node_b.safe_psql(
        "SELECT pg_xact_commit_timestamp(xmin) from tab where a=1;"
    )
    assert (
        node_b.safe_psql(
            "SELECT '{}'::timestamp >= '{}'::timestamp".format(pub_ts, sub_ts)
        )
        == "t"
    ), "pub UPDATE's timestamp is later than that of sub's DELETE"
    node_b.psql_capture(
        "ALTER SUBSCRIPTION {} ENABLE;".format(subname_ba), on_error_stop=False
    )


def _test_max_retention_duration(node_a, node_b, subname_ab):
    """Retention stops past max_retention_duration and resumes when set to 0."""
    node_b.safe_psql("SELECT * FROM pg_create_physical_replication_slot('blocker');")
    node_b.append_conf("synchronized_standby_slots = 'blocker'")
    node_b.reload()
    node_a.safe_psql("ALTER SUBSCRIPTION {} DISABLE;".format(subname_ab))
    node_a.safe_psql("ALTER SUBSCRIPTION {} SET (failover = true);".format(subname_ab))
    node_a.safe_psql("ALTER SUBSCRIPTION {} ENABLE;".format(subname_ab))
    node_b.safe_psql("INSERT INTO tab VALUES (5, 5);")
    node_a.safe_psql("SELECT txid_current() + 1;")

    offset = node_a.current_log_position()
    node_a.safe_psql(
        "ALTER SUBSCRIPTION {} SET (max_retention_duration = 1);".format(subname_ab)
    )
    node_a.wait_for_log(
        r'logical replication worker for subscription "tap_sub_a_b" has stopped '
        r"retaining the information for detecting conflicts",
        offset,
    )
    assert node_a.poll_query_until(
        "SELECT xmin IS NULL from {}".format(_SLOT)
    ), "the xmin value of slot 'pg_conflict_detection' is invalid on Node A"
    assert (
        node_a.safe_psql(
            "SELECT subretentionactive FROM pg_subscription "
            "WHERE subname='{}';".format(subname_ab)
        )
        == "f"
    ), "retention is inactive"

    offset = node_a.current_log_position()
    node_a.safe_psql(
        "ALTER SUBSCRIPTION {} SET (max_retention_duration = 0);".format(subname_ab)
    )
    node_b.safe_psql("SELECT * FROM pg_drop_replication_slot('blocker');")
    node_b.adjust_conf("synchronized_standby_slots", "''")
    node_b.reload()
    node_a.wait_for_log(_RETENTION_RESUME, offset)
    assert node_a.poll_query_until(
        "SELECT xmin IS NOT NULL from {}".format(_SLOT)
    ), "the xmin value of slot 'pg_conflict_detection' is valid on Node A"
    assert (
        node_a.safe_psql(
            "SELECT subretentionactive FROM pg_subscription "
            "WHERE subname='{}';".format(subname_ab)
        )
        == "t"
    ), "retention is active"


def _test_slot_dropped(node_a, node_b, subname_ab, subname_ba):
    """pg_conflict_detection slot is dropped after removing all subscriptions."""
    node_b.safe_psql("DROP SUBSCRIPTION {}".format(subname_ba))
    assert node_b.poll_query_until(
        "SELECT count(*) = 0 FROM {}".format(_SLOT)
    ), "the slot 'pg_conflict_detection' has been dropped on Node B"
    node_a.safe_psql("DROP SUBSCRIPTION {}".format(subname_ab))
    assert node_a.poll_query_until(
        "SELECT count(*) = 0 FROM {}".format(_SLOT)
    ), "the slot 'pg_conflict_detection' has been dropped on Node A"


def test_035_conflicts(create_pg):
    """Logical-replication conflict detection across many scenarios."""
    publisher, subscriber, _appname = _setup_unidirectional(create_pg)
    _test_multiple_unique_conflicts(publisher, subscriber)

    node_a, node_b = publisher, subscriber
    subname_ab, subname_ba = _setup_bidirectional(node_a, node_b)
    _test_retain_dead_tuples_ddl(node_a, subname_ab)
    _test_update_deleted_conflicts(node_a, node_b, subname_ab, subname_ba)
    _test_seqscan_deleted_tuple(node_a, node_b, subname_ab)
    _test_xmin_advance_no_tables(node_a, node_b, subname_ab)

    if node_b.check_extension("injection_points"):
        _test_delay_chkpt_injection(node_a, node_b, subname_ab, subname_ba)

    _test_max_retention_duration(node_a, node_b, subname_ab)
    _test_slot_dropped(node_a, node_b, subname_ab, subname_ba)

# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/subscription/t/100_bugs.pl.

Regression tests for assorted logical-replication bugs found over time: index
predicates needing a snapshot (#15114), temp/unlogged tables under FOR ALL
TABLES, initial-sync protocol (#16643) and cascaded sync, REPLICA IDENTITY
index/relcache invalidation, schema-rename invalidation, REPLICA IDENTITY FULL
with dropped/missing columns, create+drop of a replication slot via replication
commands, origin advancement when a trigger swallows an ERROR, and the DROP
SUBSCRIPTION self-deadlock (#18988).
"""

import pypg


def _test_index_predicate_crash(create_pg):
    """#15114: index predicates with const-expressions must not crash apply."""
    publisher = create_pg("publisher", allows_streaming="logical")
    subscriber = create_pg("subscriber")
    connstr = publisher.connstr() + " dbname=postgres"

    for node in (publisher, subscriber):
        node.safe_psql("CREATE TABLE tab1 (a int PRIMARY KEY, b int)")
        node.safe_psql(
            "CREATE FUNCTION double(x int) RETURNS int IMMUTABLE LANGUAGE SQL "
            "AS 'select x * 2'"
        )
        node.safe_psql("CREATE INDEX ON tab1 (b) WHERE a > double(1)")

    publisher.safe_psql("CREATE PUBLICATION pub1 FOR ALL TABLES")
    subscriber.safe_psql(
        "CREATE SUBSCRIPTION sub1 CONNECTION '{}' PUBLICATION pub1".format(connstr)
    )
    publisher.wait_for_catchup("sub1")
    publisher.safe_psql("INSERT INTO tab1 VALUES (1, 2)")
    publisher.wait_for_catchup("sub1")

    subscriber.safe_psql("DROP SUBSCRIPTION sub1")
    publisher.safe_psql("DROP PUBLICATION pub1")
    publisher.safe_psql("DROP TABLE tab1")
    publisher.stop("fast")
    subscriber.stop("fast")
    return publisher, subscriber


def _test_temp_unlogged_for_all_tables(publisher, subscriber):
    """Temp/unlogged tables are ignored by FOR ALL TABLES (no RI error)."""
    publisher.rotate_logfile()
    publisher.start()
    subscriber.rotate_logfile()

    publisher.safe_psql("CREATE PUBLICATION pub FOR ALL TABLES")
    assert (
        publisher.psql_capture(
            "CREATE TEMPORARY TABLE tt1 AS SELECT 1 AS a; UPDATE tt1 SET a = 2;"
        ).exit_code
        == 0
    ), "update to temporary table without replica identity"
    assert (
        publisher.psql_capture(
            "CREATE UNLOGGED TABLE tu1 AS SELECT 1 AS a; UPDATE tu1 SET a = 2;"
        ).exit_code
        == 0
    ), "update to unlogged table without replica identity"
    publisher.safe_psql("DROP PUBLICATION pub")
    publisher.stop("fast")


def _test_initial_sync_protocol(create_pg):
    """#16643: initial sync of an added table completes under two-way load."""
    node = create_pg("twoways", allows_streaming="logical")
    for db in ("d1", "d2"):
        node.safe_psql("CREATE DATABASE {}".format(db))
        node.safe_psql("CREATE TABLE t (f int)", dbname=db)
        node.safe_psql("CREATE TABLE t2 (f int)", dbname=db)
    rows = 3000
    node.safe_psql(
        "INSERT INTO t SELECT * FROM generate_series(1, {n});\n"
        "INSERT INTO t2 SELECT * FROM generate_series(1, {n});\n"
        "CREATE PUBLICATION testpub FOR TABLE t;\n"
        "SELECT pg_create_logical_replication_slot('testslot', 'pgoutput');".format(
            n=rows
        ),
        dbname="d1",
    )
    node.safe_psql(
        "CREATE SUBSCRIPTION testsub CONNECTION $${}$$ "
        "PUBLICATION testpub WITH (create_slot=false, "
        "slot_name='testslot')".format(node.connstr("d1")),
        dbname="d2",
    )
    node.safe_psql(
        "INSERT INTO t SELECT * FROM generate_series(1, {n});\n"
        "INSERT INTO t2 SELECT * FROM generate_series(1, {n});".format(n=rows),
        dbname="d1",
    )
    node.safe_psql("ALTER PUBLICATION testpub ADD TABLE t2", dbname="d1")
    node.safe_psql("ALTER SUBSCRIPTION testsub REFRESH PUBLICATION", dbname="d2")
    node.wait_for_subscription_sync(node, "testsub", dbname="d2")
    assert node.safe_psql("SELECT count(f) FROM t", dbname="d2") == str(
        rows * 2
    ), "2x{} rows in t".format(rows)
    assert node.safe_psql("SELECT count(f) FROM t2", dbname="d2") == str(
        rows * 2
    ), "2x{} rows in t2".format(rows)
    node.stop("fast")


def _test_cascaded_sync(create_pg):
    """Tablesync-written data replicates through a cascaded pub/sub setup."""
    node_pub = create_pg("testpublisher1", allows_streaming="logical")
    node_pub_sub = create_pg("testpublisher_subscriber", allows_streaming="logical")
    node_sub = create_pg("testsubscriber1")

    for node in (node_pub, node_pub_sub, node_sub):
        node.safe_psql("CREATE TABLE tab1 (a int)")

    node_pub.safe_psql("CREATE PUBLICATION testpub1 FOR TABLE tab1")
    node_pub_sub.safe_psql("CREATE PUBLICATION testpub2 FOR TABLE tab1")
    pub1 = node_pub.connstr() + " dbname=postgres"
    pub2 = node_pub_sub.connstr() + " dbname=postgres"

    # testsub2 must be created before testsub1 so that the data written by
    # testsub1's tablesync worker also gets replicated to testsub2.
    node_sub.safe_psql(
        "CREATE SUBSCRIPTION testsub2 CONNECTION '{}' PUBLICATION testpub2".format(pub2)
    )
    node_pub_sub.safe_psql(
        "CREATE SUBSCRIPTION testsub1 CONNECTION '{}' PUBLICATION testpub1".format(pub1)
    )
    node_pub.safe_psql("INSERT INTO tab1 values(generate_series(1,10))")
    node_pub.wait_for_catchup("testsub1")
    node_pub_sub.wait_for_catchup("testsub2")

    node_pub_sub.safe_psql("DROP SUBSCRIPTION testsub1")
    node_sub.safe_psql("DROP SUBSCRIPTION testsub2")
    node_pub.safe_psql("DROP PUBLICATION testpub1")
    node_pub_sub.safe_psql("DROP PUBLICATION testpub2")
    for node in (node_pub, node_pub_sub, node_sub):
        node.safe_psql("DROP TABLE tab1")
        node.stop("fast")


def _test_replica_identity_index(publisher, subscriber):
    """Changing the REPLICA IDENTITY index invalidates the target relcache."""
    publisher.rotate_logfile()
    publisher.start()
    subscriber.rotate_logfile()
    subscriber.start()

    for node in (publisher, subscriber):
        node.safe_psql(
            "CREATE TABLE tab_replidentity_index(a int not null, b int not null)"
        )
        node.safe_psql(
            "CREATE UNIQUE INDEX idx_replidentity_index_a "
            "ON tab_replidentity_index(a)"
        )
        node.safe_psql(
            "CREATE UNIQUE INDEX idx_replidentity_index_b "
            "ON tab_replidentity_index(b)"
        )
    publisher.safe_psql(
        "ALTER TABLE tab_replidentity_index REPLICA IDENTITY "
        "USING INDEX idx_replidentity_index_a"
    )
    publisher.safe_psql("INSERT INTO tab_replidentity_index VALUES(1, 1),(2, 2)")
    subscriber.safe_psql(
        "ALTER TABLE tab_replidentity_index REPLICA IDENTITY "
        "USING INDEX idx_replidentity_index_b"
    )

    connstr = publisher.connstr() + " dbname=postgres"
    publisher.safe_psql("CREATE PUBLICATION tap_pub FOR TABLE tab_replidentity_index")
    subscriber.safe_psql(
        "CREATE SUBSCRIPTION tap_sub CONNECTION '{}' PUBLICATION tap_pub".format(
            connstr
        )
    )
    subscriber.wait_for_subscription_sync(publisher, "tap_sub")
    assert (
        subscriber.safe_psql("SELECT * FROM tab_replidentity_index") == "1|1\n2|2"
    ), "check initial data on subscriber"

    publisher.safe_psql(
        "ALTER TABLE tab_replidentity_index REPLICA IDENTITY "
        "USING INDEX idx_replidentity_index_b;\n"
        "UPDATE tab_replidentity_index SET a = -a WHERE a = 1;\n"
        "DELETE FROM tab_replidentity_index WHERE a = 2;"
    )
    publisher.wait_for_catchup("tap_sub")
    assert (
        subscriber.safe_psql("SELECT * FROM tab_replidentity_index") == "-1|1"
    ), "update works with REPLICA IDENTITY"

    subscriber.safe_psql("DROP SUBSCRIPTION tap_sub")
    publisher.safe_psql("DROP PUBLICATION tap_pub")
    publisher.safe_psql("DROP TABLE tab_replidentity_index")
    subscriber.safe_psql("DROP TABLE tab_replidentity_index")


def _test_schema_rename(publisher, subscriber):
    """Renaming a schema invalidates replication mapping as expected."""
    connstr = publisher.connstr() + " dbname=postgres"
    publisher.safe_psql("CREATE SCHEMA sch1")
    publisher.safe_psql("CREATE TABLE sch1.t1 (c1 int)")
    subscriber.safe_psql("CREATE SCHEMA sch1")
    subscriber.safe_psql("CREATE TABLE sch1.t1 (c1 int)")
    subscriber.safe_psql("CREATE SCHEMA sch2")
    subscriber.safe_psql("CREATE TABLE sch2.t1 (c1 int)")

    publisher.safe_psql("CREATE PUBLICATION tap_pub_sch FOR ALL TABLES")
    subscriber.safe_psql(
        "CREATE SUBSCRIPTION tap_sub_sch CONNECTION '{}' "
        "PUBLICATION tap_pub_sch".format(connstr)
    )
    subscriber.wait_for_subscription_sync(publisher, "tap_sub_sch")

    publisher.safe_psql(
        "begin;\n"
        "insert into sch1.t1 values(1);\n"
        "alter schema sch1 rename to sch2;\n"
        "create schema sch1;\n"
        "create table sch1.t1(c1 int);\n"
        "insert into sch1.t1 values(2);\n"
        "insert into sch2.t1 values(3);\n"
        "commit;"
    )
    subscriber.wait_for_subscription_sync(publisher, "tap_sub_sch")
    assert (
        subscriber.safe_psql("SELECT * FROM sch1.t1") == "1\n2"
    ), "check data in subscriber sch1.t1 after schema rename"
    assert (
        subscriber.safe_psql("SELECT * FROM sch2.t1") == ""
    ), "no data yet in subscriber sch2.t1 after schema rename"

    subscriber.safe_psql("ALTER SUBSCRIPTION tap_sub_sch REFRESH PUBLICATION")
    subscriber.wait_for_subscription_sync(publisher, "tap_sub_sch")
    assert (
        subscriber.safe_psql("SELECT * FROM sch2.t1") == "1\n3"
    ), "check data in subscriber sch2.t1 after schema rename"

    subscriber.safe_psql("DROP SUBSCRIPTION tap_sub_sch")
    publisher.safe_psql("DROP PUBLICATION tap_pub_sch")
    publisher.stop("fast")
    subscriber.stop("fast")


def _test_ri_full_dropped_columns(publisher, subscriber):
    """REPLICA IDENTITY FULL with a dropped column still applies updates."""
    publisher.rotate_logfile()
    publisher.start()
    subscriber.rotate_logfile()
    subscriber.start()

    publisher.safe_psql(
        "CREATE TABLE dropped_cols (a int, b_drop int, c int);\n"
        "ALTER TABLE dropped_cols REPLICA IDENTITY FULL;\n"
        "CREATE PUBLICATION pub_dropped_cols FOR TABLE dropped_cols;\n"
        "INSERT INTO dropped_cols VALUES (1, 1, 1);"
    )
    subscriber.safe_psql("CREATE TABLE dropped_cols (a int, b_drop int, c int);")
    connstr = publisher.connstr() + " dbname=postgres"
    subscriber.safe_psql(
        "CREATE SUBSCRIPTION sub_dropped_cols CONNECTION '{}' "
        "PUBLICATION pub_dropped_cols".format(connstr)
    )
    subscriber.wait_for_subscription_sync()
    publisher.safe_psql("ALTER TABLE dropped_cols DROP COLUMN b_drop;")
    subscriber.safe_psql("ALTER TABLE dropped_cols DROP COLUMN b_drop;")
    publisher.safe_psql("UPDATE dropped_cols SET a = 100;")
    publisher.wait_for_catchup("sub_dropped_cols")
    assert (
        subscriber.safe_psql("SELECT count(*) FROM dropped_cols WHERE a = 100") == "1"
    ), "replication with RI FULL and dropped columns"
    publisher.stop("fast")
    subscriber.stop("fast")


def _test_missing_attribute(publisher, subscriber):
    """pgoutput must not replace a missing attribute with NULL (RI FULL)."""
    publisher.rotate_logfile()
    publisher.start()
    subscriber.rotate_logfile()
    subscriber.start()

    connstr = publisher.connstr() + " dbname=postgres"
    publisher.safe_psql(
        "CREATE TABLE tab_default (a int);\n"
        "ALTER TABLE tab_default REPLICA IDENTITY FULL;\n"
        "INSERT INTO tab_default VALUES (1);\n"
        "ALTER TABLE tab_default ADD COLUMN b bool DEFAULT false NOT NULL;\n"
        "INSERT INTO tab_default VALUES (2, true);\n"
        "CREATE PUBLICATION pub1 FOR TABLE tab_default;"
    )
    subscriber.safe_psql(
        "CREATE TABLE tab_default (a int, b bool);\n"
        "CREATE SUBSCRIPTION sub1 CONNECTION '{}' PUBLICATION pub1;".format(connstr)
    )
    subscriber.wait_for_subscription_sync(publisher, "sub1")
    assert (
        subscriber.safe_psql("SELECT a, b FROM tab_default") == "1|f\n2|t"
    ), "check snapshot on subscriber"

    publisher.safe_psql("UPDATE tab_default SET a = a + 1")
    publisher.wait_for_catchup("sub1")
    assert (
        subscriber.safe_psql("SELECT a, b FROM tab_default") == "2|f\n3|t"
    ), "check replicated update on subscriber"


def _test_replication_slot_commands(publisher, subscriber):
    """Create and immediately drop a logical slot via replication commands."""
    connstr_db = "host={} port={} replication=database dbname=postgres".format(
        publisher.host, publisher.port
    )
    result = publisher.psql_capture(
        "CREATE_REPLICATION_SLOT test_slot LOGICAL pgoutput (SNAPSHOT export);\n"
        "DROP_REPLICATION_SLOT test_slot;\n",
        on_error_stop=False,
        extra_params=["-d", connstr_db],
        timeout=pypg.test_timeout_default(),
    )
    assert result.exit_code == 0, "create and immediate drop of replication slot"
    publisher.stop("fast")
    subscriber.stop("fast")


def _test_origin_advance_on_caught_error(publisher, subscriber):
    """Origin advances even when a trigger catches the apply-time ERROR."""
    publisher.rotate_logfile()
    publisher.start()
    subscriber.rotate_logfile()
    subscriber.start()

    connstr = publisher.connstr() + " dbname=postgres"
    publisher.safe_psql(
        "CREATE TABLE t1 (a int);\nCREATE PUBLICATION regress_pub FOR TABLE t1;"
    )
    subscriber.safe_psql(
        "CREATE TABLE t1 (a int);\n"
        "CREATE SUBSCRIPTION regress_sub CONNECTION '{}' "
        "PUBLICATION regress_pub;".format(connstr)
    )
    subscriber.wait_for_subscription_sync(publisher, "regress_sub")
    subscriber.safe_psql(_EXCEPTION_TRIGGER_SQL)

    origin_query = (
        "SELECT remote_lsn FROM pg_replication_origin_status os, "
        "pg_subscription s WHERE os.external_id = 'pg_' || s.oid "
        "AND s.subname = 'regress_sub'"
    )
    remote_lsn = subscriber.safe_psql(origin_query)
    publisher.safe_psql("INSERT INTO t1 VALUES (1);")
    publisher.wait_for_catchup("regress_sub")
    assert (
        subscriber.safe_psql(
            "SELECT remote_lsn > '{}' FROM pg_replication_origin_status os, "
            "pg_subscription s WHERE os.external_id = 'pg_' || s.oid "
            "AND s.subname = 'regress_sub'".format(remote_lsn)
        )
        == "t"
    ), "remote_lsn has advanced for apply worker raising an exception"
    publisher.stop("fast")
    subscriber.stop("fast")


_EXCEPTION_TRIGGER_SQL = """\
CREATE FUNCTION handle_exception_trigger()
RETURNS TRIGGER AS $$
BEGIN
    BEGIN
        -- Raise an exception
        RAISE EXCEPTION 'This is a test exception';
    EXCEPTION
        WHEN OTHERS THEN
            RETURN NEW;
    END;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER silent_exception_trigger
AFTER INSERT OR UPDATE ON t1
FOR EACH ROW
EXECUTE FUNCTION handle_exception_trigger();

ALTER TABLE t1 ENABLE ALWAYS TRIGGER silent_exception_trigger;
"""


def _test_drop_subscription_deadlock(publisher):
    """#18988: DROP SUBSCRIPTION on a fresh db must not self-deadlock."""
    publisher.start()
    connstr = publisher.connstr() + " dbname=regress_db"
    publisher.safe_psql(
        "CREATE DATABASE regress_db;\n"
        "CREATE SUBSCRIPTION regress_sub1 CONNECTION '{}' "
        "PUBLICATION regress_pub WITH (connect=false);".format(connstr)
    )
    result = publisher.psql_capture("DROP SUBSCRIPTION regress_sub1")
    assert result.exit_code != 0, "replication slot does not exist: exit code not 0"
    assert (
        'ERROR:  could not drop replication slot "regress_sub1" on publisher'
        in result.stderr
    ), "could not drop replication slot: error message"
    publisher.safe_psql("DROP DATABASE regress_db")
    publisher.stop("fast")


def test_100_bugs(create_pg):
    """Assorted logical-replication bug regressions."""
    publisher, subscriber = _test_index_predicate_crash(create_pg)
    _test_temp_unlogged_for_all_tables(publisher, subscriber)
    _test_initial_sync_protocol(create_pg)
    _test_cascaded_sync(create_pg)
    _test_replica_identity_index(publisher, subscriber)
    _test_schema_rename(publisher, subscriber)
    _test_ri_full_dropped_columns(publisher, subscriber)
    _test_missing_attribute(publisher, subscriber)
    _test_replication_slot_commands(publisher, subscriber)
    _test_origin_advance_on_caught_error(publisher, subscriber)
    _test_drop_subscription_deadlock(publisher)

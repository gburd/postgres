# Copyright (c) 2022-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long,too-many-statements
"""Port of src/test/subscription/t/028_row_filter.pl.

Row filters in logical replication publications: validation of
WHERE clauses (replica identity, columns, expressions), combining filters
across publications (OR), partitioned tables with publish_via_partition_root,
TOAST, inheritance, and initial-sync vs incremental behavior.
Generated from the Perl original via .agent/gen_golden.py.
"""


def test_028_row_filter(create_pg):
    """Generated golden port of 028_row_filter."""
    node_publisher = create_pg("publisher", allows_streaming="logical", start=False)
    node_publisher.start()
    node_subscriber = create_pg("subscriber", start=False)
    node_subscriber.start()
    publisher_connstr = node_publisher.connstr() + " dbname=postgres"
    appname = "tap_sub"
    node_publisher.safe_psql("CREATE TABLE tab_rf_x (x int primary key)")
    node_subscriber.safe_psql("CREATE TABLE tab_rf_x (x int primary key)")
    node_publisher.safe_psql(
        "INSERT INTO tab_rf_x (x) VALUES (0), (5), (10), (15), (20)"
    )
    node_publisher.safe_psql(
        "CREATE PUBLICATION tap_pub_x FOR TABLE tab_rf_x WHERE (x > 10)"
    )
    node_publisher.safe_psql("CREATE PUBLICATION tap_pub_forall FOR ALL TABLES")
    node_subscriber.safe_psql(
        "CREATE SUBSCRIPTION tap_sub CONNECTION '"
        + publisher_connstr
        + " application_name="
        + appname
        + "' PUBLICATION tap_pub_x, tap_pub_forall"
    )
    node_subscriber.wait_for_subscription_sync()
    result = node_subscriber.safe_psql("SELECT count(x) FROM tab_rf_x")
    assert (
        result == "5"
    ), "check initial data copy from table tab_rf_x should not be filtered"
    node_publisher.safe_psql("INSERT INTO tab_rf_x (x) VALUES (-99), (99)")
    node_publisher.wait_for_catchup(appname)
    result = node_subscriber.safe_psql("SELECT count(x) FROM tab_rf_x")
    assert result == "7", "check table tab_rf_x should not be filtered"
    node_publisher.safe_psql("DROP PUBLICATION tap_pub_forall")
    node_publisher.safe_psql("DROP PUBLICATION tap_pub_x")
    node_publisher.safe_psql("DROP TABLE tab_rf_x")
    node_subscriber.safe_psql("DROP SUBSCRIPTION tap_sub")
    node_subscriber.safe_psql("DROP TABLE tab_rf_x")
    node_publisher.safe_psql("CREATE SCHEMA schema_rf_x")
    node_publisher.safe_psql("CREATE TABLE schema_rf_x.tab_rf_x (x int primary key)")
    node_publisher.safe_psql(
        "CREATE TABLE schema_rf_x.tab_rf_partitioned (x int primary key) PARTITION BY RANGE(x)"
    )
    node_publisher.safe_psql(
        "CREATE TABLE public.tab_rf_partition (LIKE schema_rf_x.tab_rf_partitioned)"
    )
    node_publisher.safe_psql(
        "ALTER TABLE schema_rf_x.tab_rf_partitioned ATTACH PARTITION public.tab_rf_partition DEFAULT"
    )
    node_subscriber.safe_psql("CREATE SCHEMA schema_rf_x")
    node_subscriber.safe_psql("CREATE TABLE schema_rf_x.tab_rf_x (x int primary key)")
    node_subscriber.safe_psql(
        "CREATE TABLE schema_rf_x.tab_rf_partitioned (x int primary key) PARTITION BY RANGE(x)"
    )
    node_subscriber.safe_psql(
        "CREATE TABLE public.tab_rf_partition (LIKE schema_rf_x.tab_rf_partitioned)"
    )
    node_subscriber.safe_psql(
        "ALTER TABLE schema_rf_x.tab_rf_partitioned ATTACH PARTITION public.tab_rf_partition DEFAULT"
    )
    node_publisher.safe_psql(
        "INSERT INTO schema_rf_x.tab_rf_x (x) VALUES (0), (5), (10), (15), (20)"
    )
    node_publisher.safe_psql(
        "INSERT INTO schema_rf_x.tab_rf_partitioned (x) VALUES (1), (20)"
    )
    node_publisher.safe_psql(
        "CREATE PUBLICATION tap_pub_x FOR TABLE schema_rf_x.tab_rf_x WHERE (x > 10)"
    )
    node_publisher.safe_psql(
        "CREATE PUBLICATION tap_pub_allinschema FOR TABLES IN SCHEMA schema_rf_x, TABLE schema_rf_x.tab_rf_x WHERE (x > 10)"
    )
    node_publisher.safe_psql(
        "ALTER PUBLICATION tap_pub_allinschema ADD TABLE public.tab_rf_partition WHERE (x > 10)"
    )
    node_subscriber.safe_psql(
        "CREATE SUBSCRIPTION tap_sub CONNECTION '"
        + publisher_connstr
        + " application_name="
        + appname
        + "' PUBLICATION tap_pub_x, tap_pub_allinschema"
    )
    node_subscriber.wait_for_subscription_sync()
    result = node_subscriber.safe_psql("SELECT count(x) FROM schema_rf_x.tab_rf_x")
    assert (
        result == "5"
    ), "check initial data copy from table tab_rf_x should not be filtered"
    node_publisher.safe_psql("INSERT INTO schema_rf_x.tab_rf_x (x) VALUES (-99), (99)")
    node_publisher.safe_psql(
        "INSERT INTO schema_rf_x.tab_rf_partitioned (x) VALUES (5), (25)"
    )
    node_publisher.wait_for_catchup(appname)
    result = node_subscriber.safe_psql("SELECT count(x) FROM schema_rf_x.tab_rf_x")
    assert result == "7", "check table tab_rf_x should not be filtered"
    result = node_subscriber.safe_psql("SELECT * FROM public.tab_rf_partition")
    assert result == "20\n25", "check table tab_rf_partition should be filtered"
    node_publisher.safe_psql("DROP PUBLICATION tap_pub_allinschema")
    node_publisher.safe_psql("DROP PUBLICATION tap_pub_x")
    node_publisher.safe_psql("DROP TABLE public.tab_rf_partition")
    node_publisher.safe_psql("DROP TABLE schema_rf_x.tab_rf_partitioned")
    node_publisher.safe_psql("DROP TABLE schema_rf_x.tab_rf_x")
    node_publisher.safe_psql("DROP SCHEMA schema_rf_x")
    node_subscriber.safe_psql("DROP SUBSCRIPTION tap_sub")
    node_subscriber.safe_psql("DROP TABLE public.tab_rf_partition")
    node_subscriber.safe_psql("DROP TABLE schema_rf_x.tab_rf_partitioned")
    node_subscriber.safe_psql("DROP TABLE schema_rf_x.tab_rf_x")
    node_subscriber.safe_psql("DROP SCHEMA schema_rf_x")
    node_publisher.safe_psql("CREATE TABLE tab_rowfilter_1 (a int primary key, b text)")
    node_publisher.safe_psql("ALTER TABLE tab_rowfilter_1 REPLICA IDENTITY FULL;")
    node_publisher.safe_psql("CREATE TABLE tab_rowfilter_2 (c int primary key)")
    node_publisher.safe_psql(
        "CREATE TABLE tab_rowfilter_3 (a int primary key, b boolean)"
    )
    node_publisher.safe_psql("CREATE TABLE tab_rowfilter_4 (c int primary key)")
    node_publisher.safe_psql(
        "CREATE TABLE tab_rowfilter_partitioned (a int primary key, b integer) PARTITION BY RANGE(a)"
    )
    node_publisher.safe_psql(
        "CREATE TABLE tab_rowfilter_less_10k (LIKE tab_rowfilter_partitioned)"
    )
    node_publisher.safe_psql(
        "ALTER TABLE tab_rowfilter_partitioned ATTACH PARTITION tab_rowfilter_less_10k FOR VALUES FROM (MINVALUE) TO (10000)"
    )
    node_publisher.safe_psql(
        "CREATE TABLE tab_rowfilter_greater_10k (LIKE tab_rowfilter_partitioned)"
    )
    node_publisher.safe_psql(
        "ALTER TABLE tab_rowfilter_partitioned ATTACH PARTITION tab_rowfilter_greater_10k FOR VALUES FROM (10000) TO (MAXVALUE)"
    )
    node_publisher.safe_psql(
        "CREATE TABLE tab_rowfilter_partitioned_2 (a int primary key, b integer) PARTITION BY RANGE(a)"
    )
    node_publisher.safe_psql(
        "CREATE TABLE tab_rowfilter_partition (LIKE tab_rowfilter_partitioned_2)"
    )
    node_publisher.safe_psql(
        "ALTER TABLE tab_rowfilter_partitioned_2 ATTACH PARTITION tab_rowfilter_partition DEFAULT"
    )
    node_publisher.safe_psql(
        "CREATE TABLE tab_rowfilter_toast (a text NOT NULL, b text NOT NULL)"
    )
    node_publisher.safe_psql(
        "ALTER TABLE tab_rowfilter_toast ALTER COLUMN a SET STORAGE EXTERNAL"
    )
    node_publisher.safe_psql(
        "CREATE UNIQUE INDEX tab_rowfilter_toast_ri_index on tab_rowfilter_toast (a, b)"
    )
    node_publisher.safe_psql(
        "ALTER TABLE tab_rowfilter_toast REPLICA IDENTITY USING INDEX tab_rowfilter_toast_ri_index"
    )
    node_publisher.safe_psql("CREATE TABLE tab_rowfilter_inherited (a int)")
    node_publisher.safe_psql(
        "CREATE TABLE tab_rowfilter_child (b text) INHERITS (tab_rowfilter_inherited)"
    )
    node_publisher.safe_psql(
        "CREATE TABLE tab_rowfilter_viaroot_part (a int) PARTITION BY RANGE (a)"
    )
    node_publisher.safe_psql(
        "CREATE TABLE tab_rowfilter_viaroot_part_1 PARTITION OF tab_rowfilter_viaroot_part FOR VALUES FROM (1) TO (20)"
    )
    node_publisher.safe_psql(
        "CREATE TABLE tab_rowfilter_parent_sync (a int) PARTITION BY RANGE (a)"
    )
    node_publisher.safe_psql(
        "CREATE TABLE tab_rowfilter_child_sync PARTITION OF tab_rowfilter_parent_sync FOR VALUES FROM (1) TO (20)"
    )
    node_publisher.safe_psql(
        "CREATE TABLE tab_rowfilter_virtual (id int PRIMARY KEY, x int, y int GENERATED ALWAYS AS (x * 2) VIRTUAL)"
    )
    node_subscriber.safe_psql(
        "CREATE TABLE tab_rowfilter_1 (a int primary key, b text)"
    )
    node_subscriber.safe_psql("CREATE TABLE tab_rowfilter_2 (c int primary key)")
    node_subscriber.safe_psql(
        "CREATE TABLE tab_rowfilter_3 (a int primary key, b boolean)"
    )
    node_subscriber.safe_psql("CREATE TABLE tab_rowfilter_4 (c int primary key)")
    node_subscriber.safe_psql(
        "CREATE TABLE tab_rowfilter_partitioned (a int primary key, b integer) PARTITION BY RANGE(a)"
    )
    node_subscriber.safe_psql(
        "CREATE TABLE tab_rowfilter_less_10k (LIKE tab_rowfilter_partitioned)"
    )
    node_subscriber.safe_psql(
        "ALTER TABLE tab_rowfilter_partitioned ATTACH PARTITION tab_rowfilter_less_10k FOR VALUES FROM (MINVALUE) TO (10000)"
    )
    node_subscriber.safe_psql(
        "CREATE TABLE tab_rowfilter_greater_10k (LIKE tab_rowfilter_partitioned)"
    )
    node_subscriber.safe_psql(
        "ALTER TABLE tab_rowfilter_partitioned ATTACH PARTITION tab_rowfilter_greater_10k FOR VALUES FROM (10000) TO (MAXVALUE)"
    )
    node_subscriber.safe_psql(
        "CREATE TABLE tab_rowfilter_partitioned_2 (a int primary key, b integer) PARTITION BY RANGE(a)"
    )
    node_subscriber.safe_psql(
        "CREATE TABLE tab_rowfilter_partition (LIKE tab_rowfilter_partitioned_2)"
    )
    node_subscriber.safe_psql(
        "ALTER TABLE tab_rowfilter_partitioned_2 ATTACH PARTITION tab_rowfilter_partition DEFAULT"
    )
    node_subscriber.safe_psql(
        "CREATE TABLE tab_rowfilter_toast (a text NOT NULL, b text NOT NULL)"
    )
    node_subscriber.safe_psql(
        "CREATE UNIQUE INDEX tab_rowfilter_toast_ri_index on tab_rowfilter_toast (a, b)"
    )
    node_subscriber.safe_psql(
        "ALTER TABLE tab_rowfilter_toast REPLICA IDENTITY USING INDEX tab_rowfilter_toast_ri_index"
    )
    node_subscriber.safe_psql("CREATE TABLE tab_rowfilter_inherited (a int)")
    node_subscriber.safe_psql(
        "CREATE TABLE tab_rowfilter_child (b text) INHERITS (tab_rowfilter_inherited)"
    )
    node_subscriber.safe_psql("CREATE TABLE tab_rowfilter_viaroot_part (a int)")
    node_subscriber.safe_psql("CREATE TABLE tab_rowfilter_viaroot_part_1 (a int)")
    node_subscriber.safe_psql("CREATE TABLE tab_rowfilter_parent_sync (a int)")
    node_subscriber.safe_psql("CREATE TABLE tab_rowfilter_child_sync (a int)")
    node_subscriber.safe_psql(
        "CREATE TABLE tab_rowfilter_virtual (id int PRIMARY KEY, x int, y int GENERATED ALWAYS AS (x * 2) VIRTUAL)"
    )
    node_publisher.safe_psql(
        "CREATE PUBLICATION tap_pub_1 FOR TABLE tab_rowfilter_1 WHERE (a > 1000 AND b <> 'filtered')"
    )
    node_publisher.safe_psql(
        "ALTER PUBLICATION tap_pub_1 ADD TABLE tab_rowfilter_2 WHERE (c % 7 = 0)"
    )
    node_publisher.safe_psql(
        "ALTER PUBLICATION tap_pub_1 SET TABLE tab_rowfilter_1 WHERE (a > 1000 AND b <> 'filtered'), tab_rowfilter_2 WHERE (c % 2 = 0), tab_rowfilter_3"
    )
    node_publisher.safe_psql(
        "CREATE PUBLICATION tap_pub_2 FOR TABLE tab_rowfilter_2 WHERE (c % 3 = 0)"
    )
    node_publisher.safe_psql(
        "CREATE PUBLICATION tap_pub_3 FOR TABLE tab_rowfilter_partitioned"
    )
    node_publisher.safe_psql(
        "ALTER PUBLICATION tap_pub_3 ADD TABLE tab_rowfilter_less_10k WHERE (a < 6000)"
    )
    node_publisher.safe_psql(
        "CREATE PUBLICATION tap_pub_not_used FOR TABLE tab_rowfilter_1 WHERE (a < 0)"
    )
    node_publisher.safe_psql(
        "CREATE PUBLICATION tap_pub_4a FOR TABLE tab_rowfilter_4 WHERE (c % 2 = 0)"
    )
    node_publisher.safe_psql("CREATE PUBLICATION tap_pub_4b FOR TABLE tab_rowfilter_4")
    node_publisher.safe_psql(
        "CREATE PUBLICATION tap_pub_5a FOR TABLE tab_rowfilter_partitioned_2"
    )
    node_publisher.safe_psql(
        "CREATE PUBLICATION tap_pub_5b FOR TABLE tab_rowfilter_partition WHERE (a > 10)"
    )
    node_publisher.safe_psql(
        "CREATE PUBLICATION tap_pub_toast FOR TABLE tab_rowfilter_toast WHERE (a = repeat('1234567890', 200) AND b < '10')"
    )
    node_publisher.safe_psql(
        "CREATE PUBLICATION tap_pub_inherits FOR TABLE tab_rowfilter_inherited WHERE (a > 15)"
    )
    node_publisher.safe_psql(
        "CREATE PUBLICATION tap_pub_viaroot_1 FOR TABLE tab_rowfilter_viaroot_part WHERE (a > 15) WITH (publish_via_partition_root)"
    )
    node_publisher.safe_psql(
        "CREATE PUBLICATION tap_pub_viaroot_2 FOR TABLE tab_rowfilter_viaroot_part_1 WHERE (a < 15) WITH (publish_via_partition_root)"
    )
    node_publisher.safe_psql(
        "CREATE PUBLICATION tap_pub_parent_sync FOR TABLE tab_rowfilter_parent_sync WHERE (a > 15) WITH (publish_via_partition_root)"
    )
    node_publisher.safe_psql(
        "CREATE PUBLICATION tap_pub_child_sync FOR TABLE tab_rowfilter_child_sync WHERE (a < 15)"
    )
    node_publisher.safe_psql(
        "CREATE PUBLICATION tap_pub_virtual FOR TABLE tab_rowfilter_virtual WHERE (y > 10)"
    )
    node_publisher.safe_psql(
        "INSERT INTO tab_rowfilter_1 (a, b) VALUES (1, 'not replicated')"
    )
    node_publisher.safe_psql(
        "INSERT INTO tab_rowfilter_1 (a, b) VALUES (1500, 'filtered')"
    )
    node_publisher.safe_psql(
        "INSERT INTO tab_rowfilter_1 (a, b) VALUES (1980, 'not filtered')"
    )
    node_publisher.safe_psql(
        "INSERT INTO tab_rowfilter_1 (a, b) SELECT x, 'test ' || x FROM generate_series(990,1002) x"
    )
    node_publisher.safe_psql(
        "INSERT INTO tab_rowfilter_2 (c) SELECT generate_series(1, 20)"
    )
    node_publisher.safe_psql(
        "INSERT INTO tab_rowfilter_3 (a, b) SELECT x, (x % 3 = 0) FROM generate_series(1, 10) x"
    )
    node_publisher.safe_psql(
        "INSERT INTO tab_rowfilter_4 (c) SELECT generate_series(1, 10)"
    )
    node_publisher.safe_psql(
        "INSERT INTO tab_rowfilter_parent_sync(a) VALUES(14), (16)"
    )
    node_publisher.safe_psql(
        "INSERT INTO tab_rowfilter_partitioned (a, b) VALUES(1, 100),(7000, 101),(15000, 102),(5500, 300)"
    )
    node_publisher.safe_psql(
        "INSERT INTO tab_rowfilter_less_10k (a, b) VALUES(2, 200),(6005, 201)"
    )
    node_publisher.safe_psql(
        "INSERT INTO tab_rowfilter_greater_10k (a, b) VALUES(16000, 103)"
    )
    node_publisher.safe_psql(
        "INSERT INTO tab_rowfilter_partitioned_2 (a, b) VALUES(1, 1),(20, 20)"
    )
    node_publisher.safe_psql(
        "INSERT INTO tab_rowfilter_toast(a, b) VALUES(repeat('1234567890', 200), '1234567890')"
    )
    node_publisher.safe_psql("INSERT INTO tab_rowfilter_inherited(a) VALUES(10),(20)")
    node_publisher.safe_psql(
        "INSERT INTO tab_rowfilter_child(a, b) VALUES(0,'0'),(30,'30'),(40,'40')"
    )
    node_publisher.safe_psql(
        "INSERT INTO tab_rowfilter_virtual (id, x) VALUES (1, 2), (2, 4), (3, 6)"
    )
    node_subscriber.safe_psql(
        "CREATE SUBSCRIPTION tap_sub CONNECTION '"
        + publisher_connstr
        + " application_name="
        + appname
        + "' PUBLICATION tap_pub_1, tap_pub_2, tap_pub_3, tap_pub_4a, tap_pub_4b, tap_pub_5a, tap_pub_5b, tap_pub_toast, tap_pub_inherits, tap_pub_viaroot_2, tap_pub_viaroot_1, tap_pub_parent_sync, tap_pub_child_sync, tap_pub_virtual"
    )
    node_subscriber.wait_for_subscription_sync()
    result = node_subscriber.safe_psql("SELECT a, b FROM tab_rowfilter_1 ORDER BY 1, 2")
    assert (
        result == "1001|test 1001\n1002|test 1002\n1980|not filtered"
    ), "check initial data copy from table tab_rowfilter_1"
    result = node_subscriber.safe_psql(
        "SELECT count(c), min(c), max(c) FROM tab_rowfilter_2"
    )
    assert result == "13|2|20", "check initial data copy from table tab_rowfilter_2"
    result = node_subscriber.safe_psql(
        "SELECT count(c), min(c), max(c) FROM tab_rowfilter_4"
    )
    assert result == "10|1|10", "check initial data copy from table tab_rowfilter_4"
    result = node_subscriber.safe_psql("SELECT count(a) FROM tab_rowfilter_3")
    assert result == "10", "check initial data copy from table tab_rowfilter_3"
    result = node_subscriber.safe_psql(
        "SELECT a, b FROM tab_rowfilter_less_10k ORDER BY 1, 2"
    )
    assert (
        result == "1|100\n2|200\n5500|300"
    ), "check initial data copy from partition tab_rowfilter_less_10k"
    result = node_subscriber.safe_psql(
        "SELECT a, b FROM tab_rowfilter_greater_10k ORDER BY 1, 2"
    )
    assert (
        result == "15000|102\n16000|103"
    ), "check initial data copy from partition tab_rowfilter_greater_10k"
    result = node_subscriber.safe_psql(
        "SELECT a, b FROM tab_rowfilter_partition ORDER BY 1, 2"
    )
    assert (
        result == "1|1\n20|20"
    ), "check initial data copy from partition tab_rowfilter_partition"
    result = node_subscriber.safe_psql("SELECT count(*) FROM tab_rowfilter_toast")
    assert result == "0", "check initial data copy from table tab_rowfilter_toast"
    result = node_subscriber.safe_psql(
        "SELECT a FROM tab_rowfilter_inherited ORDER BY a"
    )
    assert (
        result == "20\n30\n40"
    ), "check initial data copy from table tab_rowfilter_inherited"
    result = node_subscriber.safe_psql(
        "SELECT a FROM tab_rowfilter_parent_sync ORDER BY 1"
    )
    assert result == "16", "check initial data copy from tab_rowfilter_parent_sync"
    result = node_subscriber.safe_psql(
        "SELECT a FROM tab_rowfilter_child_sync ORDER BY 1"
    )
    assert result == "", "check initial data copy from tab_rowfilter_child_sync"
    result = node_subscriber.safe_psql(
        "SELECT id, x FROM tab_rowfilter_virtual ORDER BY id"
    )
    assert result == "3|6", "check initial data copy from table tab_rowfilter_virtual"
    node_publisher.safe_psql(
        "INSERT INTO tab_rowfilter_1 (a, b) VALUES (800, 'test 800')"
    )
    node_publisher.safe_psql(
        "INSERT INTO tab_rowfilter_1 (a, b) VALUES (1600, 'test 1600')"
    )
    node_publisher.safe_psql(
        "INSERT INTO tab_rowfilter_1 (a, b) VALUES (1601, 'test 1601')"
    )
    node_publisher.safe_psql(
        "INSERT INTO tab_rowfilter_1 (a, b) VALUES (1602, 'filtered')"
    )
    node_publisher.safe_psql(
        "INSERT INTO tab_rowfilter_1 (a, b) VALUES (1700, 'test 1700')"
    )
    node_publisher.safe_psql("UPDATE tab_rowfilter_1 SET b = NULL WHERE a = 1600")
    node_publisher.safe_psql(
        "UPDATE tab_rowfilter_1 SET b = 'test 1601 updated' WHERE a = 1601"
    )
    node_publisher.safe_psql(
        "UPDATE tab_rowfilter_1 SET b = 'test 1602 updated' WHERE a = 1602"
    )
    node_publisher.safe_psql("DELETE FROM tab_rowfilter_1 WHERE a = 1700")
    node_publisher.safe_psql(
        "INSERT INTO tab_rowfilter_2 (c) VALUES (21), (22), (23), (24), (25)"
    )
    node_publisher.safe_psql("INSERT INTO tab_rowfilter_4 (c) VALUES (0), (11), (12)")
    node_publisher.safe_psql(
        "INSERT INTO tab_rowfilter_inherited (a) VALUES (14), (16)"
    )
    node_publisher.safe_psql(
        "INSERT INTO tab_rowfilter_child (a, b) VALUES (13, '13'), (17, '17')"
    )
    node_publisher.safe_psql(
        "INSERT INTO tab_rowfilter_viaroot_part (a) VALUES (14), (15), (16)"
    )
    node_publisher.safe_psql(
        "INSERT INTO tab_rowfilter_virtual (id, x) VALUES (4, 3), (5, 7)"
    )
    node_publisher.wait_for_catchup(appname)
    result = node_subscriber.safe_psql(
        "SELECT count(c), min(c), max(c) FROM tab_rowfilter_2"
    )
    assert result == "16|2|24", "check replicated rows to tab_rowfilter_2"
    result = node_subscriber.safe_psql(
        "SELECT count(c), min(c), max(c) FROM tab_rowfilter_4"
    )
    assert result == "13|0|12", "check replicated rows to tab_rowfilter_4"
    result = node_subscriber.safe_psql("SELECT a, b FROM tab_rowfilter_1 ORDER BY 1, 2")
    assert (
        result
        == "1001|test 1001\n1002|test 1002\n1601|test 1601 updated\n1602|test 1602 updated\n1980|not filtered"
    ), "check replicated rows to table tab_rowfilter_1"
    node_publisher.safe_psql(
        "ALTER PUBLICATION tap_pub_3 SET (publish_via_partition_root = true)"
    )
    node_publisher.safe_psql(
        "ALTER PUBLICATION tap_pub_3 SET TABLE tab_rowfilter_partitioned WHERE (a < 5000), tab_rowfilter_less_10k WHERE (a < 6000)"
    )
    node_subscriber.safe_psql("TRUNCATE TABLE tab_rowfilter_partitioned")
    node_subscriber.safe_psql(
        "ALTER SUBSCRIPTION tap_sub REFRESH PUBLICATION WITH (copy_data = true)"
    )
    node_subscriber.wait_for_subscription_sync()
    node_publisher.safe_psql(
        "INSERT INTO tab_rowfilter_partitioned (a, b) VALUES(4000, 400),(4001, 401),(4002, 402)"
    )
    node_publisher.safe_psql(
        "INSERT INTO tab_rowfilter_less_10k (a, b) VALUES(4500, 450)"
    )
    node_publisher.safe_psql(
        "INSERT INTO tab_rowfilter_less_10k (a, b) VALUES(5600, 123)"
    )
    node_publisher.safe_psql(
        "INSERT INTO tab_rowfilter_greater_10k (a, b) VALUES(14000, 1950)"
    )
    node_publisher.safe_psql("UPDATE tab_rowfilter_less_10k SET b = 30 WHERE a = 4001")
    node_publisher.safe_psql("DELETE FROM tab_rowfilter_less_10k WHERE a = 4002")
    node_publisher.wait_for_catchup(appname)
    result = node_subscriber.safe_psql(
        "SELECT a, b FROM tab_rowfilter_partitioned ORDER BY 1, 2"
    )
    assert (
        result == "1|100\n2|200\n4000|400\n4001|30\n4500|450"
    ), "check publish_via_partition_root behavior"
    result = node_subscriber.safe_psql(
        "SELECT a FROM tab_rowfilter_inherited ORDER BY a"
    )
    assert (
        result == "16\n17\n20\n30\n40"
    ), "check replicated rows to tab_rowfilter_inherited and tab_rowfilter_child"
    result = node_subscriber.safe_psql(
        "SELECT id, x FROM tab_rowfilter_virtual ORDER BY id"
    )
    assert result == "3|6\n5|7", "check replicated rows to tab_rowfilter_virtual"
    node_publisher.safe_psql("UPDATE tab_rowfilter_toast SET b = '1'")
    node_publisher.wait_for_catchup(appname)
    result = node_subscriber.safe_psql(
        "SELECT a = repeat('1234567890', 200), b FROM tab_rowfilter_toast"
    )
    assert result == "t|1", "check replicated rows to tab_rowfilter_toast"
    result = node_subscriber.safe_psql("SELECT a FROM tab_rowfilter_viaroot_part")
    assert result == "16", "check replicated rows to tab_rowfilter_viaroot_part"
    result = node_subscriber.safe_psql("SELECT a FROM tab_rowfilter_viaroot_part_1")
    assert result == "", "check replicated rows to tab_rowfilter_viaroot_part_1"
    node_subscriber.stop("fast")
    node_publisher.stop("fast")

# Copyright (c) 2022-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long,too-many-statements
"""Port of src/test/subscription/t/013_partition.pl.

Logical replication into partitioned tables: replication via leaf and
via root identity (publish_via_partition_root), schema/identity mismatches,
update/delete row routing across partitions, and conflict-detection log
messages (update_missing / delete_missing / update_origin_differs).
Generated from the Perl original via .agent/gen_golden.py.
"""


def test_013_partition(create_pg):
    """Generated golden port of 013_partition."""
    node_publisher = create_pg("publisher", allows_streaming="logical", start=False)
    node_publisher.start()
    node_subscriber1 = create_pg("subscriber1", start=False)
    node_subscriber1.start()
    node_subscriber2 = create_pg("subscriber2", start=False)
    node_subscriber2.start()
    publisher_connstr = node_publisher.connstr() + " dbname=postgres"
    node_publisher.safe_psql("CREATE PUBLICATION pub1")
    node_publisher.safe_psql("CREATE PUBLICATION pub_all FOR ALL TABLES")
    node_publisher.safe_psql(
        "CREATE TABLE tab1 (a int PRIMARY KEY, b text) PARTITION BY LIST (a)"
    )
    node_publisher.safe_psql("CREATE TABLE tab1_1 (b text, a int NOT NULL)")
    node_publisher.safe_psql(
        "ALTER TABLE tab1 ATTACH PARTITION tab1_1 FOR VALUES IN (1, 2, 3)"
    )
    node_publisher.safe_psql(
        "CREATE TABLE tab1_2 PARTITION OF tab1 FOR VALUES IN (4, 5, 6)"
    )
    node_publisher.safe_psql("CREATE TABLE tab1_def PARTITION OF tab1 DEFAULT")
    node_publisher.safe_psql("ALTER PUBLICATION pub1 ADD TABLE tab1, tab1_1")
    node_subscriber1.safe_psql(
        "CREATE TABLE tab1 (c text, a int PRIMARY KEY, b text) PARTITION BY LIST (a)"
    )
    node_subscriber1.safe_psql("CREATE INDEX tab1_c_brin_idx ON tab1 USING brin (c)")
    node_subscriber1.safe_psql(
        "CREATE TABLE tab1_1 (b text, c text DEFAULT 'sub1_tab1', a int NOT NULL)"
    )
    node_subscriber1.safe_psql(
        "ALTER TABLE tab1 ATTACH PARTITION tab1_1 FOR VALUES IN (1, 2, 3)"
    )
    node_subscriber1.safe_psql(
        "CREATE TABLE tab1_2 PARTITION OF tab1 (c DEFAULT 'sub1_tab1') FOR VALUES IN (4, 5, 6) PARTITION BY LIST (a)"
    )
    node_subscriber1.safe_psql("CREATE TABLE tab1_2_1 (c text, b text, a int NOT NULL)")
    node_subscriber1.safe_psql(
        "ALTER TABLE tab1_2 ATTACH PARTITION tab1_2_1 FOR VALUES IN (5)"
    )
    node_subscriber1.safe_psql(
        "CREATE TABLE tab1_2_2 PARTITION OF tab1_2 FOR VALUES IN (4, 6)"
    )
    node_subscriber1.safe_psql(
        "CREATE TABLE tab1_def PARTITION OF tab1 (c DEFAULT 'sub1_tab1') DEFAULT"
    )
    node_subscriber1.safe_psql(
        "CREATE SUBSCRIPTION sub1 CONNECTION '"
        + publisher_connstr
        + "' PUBLICATION pub1"
    )
    node_subscriber1.safe_psql(
        "CREATE TABLE sub1_trigger_activity (tgtab text, tgop text,\n  tgwhen text, tglevel text, olda int, newa int);\nCREATE FUNCTION sub1_trigger_activity_func() RETURNS TRIGGER AS $$\nBEGIN\n  IF (TG_OP = 'INSERT') THEN\n    INSERT INTO public.sub1_trigger_activity\n      SELECT TG_RELNAME, TG_OP, TG_WHEN, TG_LEVEL, NULL, NEW.a;\n  ELSIF (TG_OP = 'UPDATE') THEN\n    INSERT INTO public.sub1_trigger_activity\n      SELECT TG_RELNAME, TG_OP, TG_WHEN, TG_LEVEL, OLD.a, NEW.a;\n  END IF;\n  RETURN NULL;\nEND;\n$$ LANGUAGE plpgsql;\nCREATE TRIGGER sub1_tab1_log_op_trigger\n  AFTER INSERT OR UPDATE ON tab1\n  FOR EACH ROW EXECUTE PROCEDURE sub1_trigger_activity_func();\nALTER TABLE ONLY tab1 ENABLE REPLICA TRIGGER sub1_tab1_log_op_trigger;\nCREATE TRIGGER sub1_tab1_2_log_op_trigger\n  AFTER INSERT OR UPDATE ON tab1_2\n  FOR EACH ROW EXECUTE PROCEDURE sub1_trigger_activity_func();\nALTER TABLE ONLY tab1_2 ENABLE REPLICA TRIGGER sub1_tab1_2_log_op_trigger;\nCREATE TRIGGER sub1_tab1_2_2_log_op_trigger\n  AFTER INSERT OR UPDATE ON tab1_2_2\n  FOR EACH ROW EXECUTE PROCEDURE sub1_trigger_activity_func();\nALTER TABLE ONLY tab1_2_2 ENABLE REPLICA TRIGGER sub1_tab1_2_2_log_op_trigger;"
    )
    node_subscriber2.safe_psql(
        "CREATE TABLE tab1 (a int PRIMARY KEY, c text DEFAULT 'sub2_tab1', b text)"
    )
    node_subscriber2.safe_psql(
        "CREATE TABLE tab1_1 (a int PRIMARY KEY, c text DEFAULT 'sub2_tab1_1', b text)"
    )
    node_subscriber2.safe_psql(
        "CREATE TABLE tab1_2 (a int PRIMARY KEY, c text DEFAULT 'sub2_tab1_2', b text)"
    )
    node_subscriber2.safe_psql(
        "CREATE TABLE tab1_def (a int PRIMARY KEY, b text, c text DEFAULT 'sub2_tab1_def')"
    )
    node_subscriber2.safe_psql(
        "CREATE SUBSCRIPTION sub2 CONNECTION '"
        + publisher_connstr
        + "' PUBLICATION pub_all"
    )
    node_subscriber2.safe_psql(
        "CREATE TABLE sub2_trigger_activity (tgtab text,\n  tgop text, tgwhen text, tglevel text, olda int, newa int);\nCREATE FUNCTION sub2_trigger_activity_func() RETURNS TRIGGER AS $$\nBEGIN\n  IF (TG_OP = 'INSERT') THEN\n    INSERT INTO public.sub2_trigger_activity\n      SELECT TG_RELNAME, TG_OP, TG_WHEN, TG_LEVEL, NULL, NEW.a;\n  ELSIF (TG_OP = 'UPDATE') THEN\n    INSERT INTO public.sub2_trigger_activity\n      SELECT TG_RELNAME, TG_OP, TG_WHEN, TG_LEVEL, OLD.a, NEW.a;\n  END IF;\n  RETURN NULL;\nEND;\n$$ LANGUAGE plpgsql;\nCREATE TRIGGER sub2_tab1_log_op_trigger\n  AFTER INSERT OR UPDATE ON tab1\n  FOR EACH ROW EXECUTE PROCEDURE sub2_trigger_activity_func();\nALTER TABLE ONLY tab1 ENABLE REPLICA TRIGGER sub2_tab1_log_op_trigger;\nCREATE TRIGGER sub2_tab1_2_log_op_trigger\n  AFTER INSERT OR UPDATE ON tab1_2\n  FOR EACH ROW EXECUTE PROCEDURE sub2_trigger_activity_func();\nALTER TABLE ONLY tab1_2 ENABLE REPLICA TRIGGER sub2_tab1_2_log_op_trigger;"
    )
    node_subscriber1.wait_for_subscription_sync()
    node_subscriber2.wait_for_subscription_sync()
    node_publisher.safe_psql("INSERT INTO tab1 VALUES (1)")
    node_publisher.safe_psql("INSERT INTO tab1_1 (a) VALUES (3)")
    node_publisher.safe_psql("INSERT INTO tab1_2 VALUES (5)")
    node_publisher.safe_psql("INSERT INTO tab1 VALUES (0)")
    node_publisher.wait_for_catchup("sub1")
    node_publisher.wait_for_catchup("sub2")
    result = node_subscriber1.safe_psql("SELECT c, a FROM tab1 ORDER BY 1, 2")
    assert (
        result == "sub1_tab1|0\nsub1_tab1|1\nsub1_tab1|3\nsub1_tab1|5"
    ), "inserts into tab1 and its partitions replicated"
    result = node_subscriber1.safe_psql("SELECT a FROM tab1_2_1 ORDER BY 1")
    assert result == "5", "inserts into tab1_2 replicated into tab1_2_1 correctly"
    result = node_subscriber1.safe_psql("SELECT a FROM tab1_2_2 ORDER BY 1")
    assert result == "", "inserts into tab1_2 replicated into tab1_2_2 correctly"
    result = node_subscriber2.safe_psql("SELECT c, a FROM tab1_1 ORDER BY 1, 2")
    assert result == "sub2_tab1_1|1\nsub2_tab1_1|3", "inserts into tab1_1 replicated"
    result = node_subscriber2.safe_psql("SELECT c, a FROM tab1_2 ORDER BY 1, 2")
    assert result == "sub2_tab1_2|5", "inserts into tab1_2 replicated"
    result = node_subscriber2.safe_psql(
        "SELECT * FROM sub2_trigger_activity ORDER BY tgtab, tgop, tgwhen, olda, newa;"
    )
    assert (
        result == "tab1_2|INSERT|AFTER|ROW||5"
    ), "check replica insert after trigger applied on subscriber"
    result = node_subscriber2.safe_psql("SELECT c, a FROM tab1_def ORDER BY 1, 2")
    assert result == "sub2_tab1_def|0", "inserts into tab1_def replicated"
    node_publisher.safe_psql("UPDATE tab1 SET a = 2 WHERE a = 1")
    node_publisher.safe_psql("UPDATE tab1 SET a = 6 WHERE a = 5")
    node_publisher.safe_psql("UPDATE tab1 SET a = 4 WHERE a = 6")
    node_publisher.safe_psql("UPDATE tab1 SET a = 6 WHERE a = 4")
    node_publisher.wait_for_catchup("sub1")
    node_publisher.wait_for_catchup("sub2")
    result = node_subscriber1.safe_psql("SELECT c, a FROM tab1 ORDER BY 1, 2")
    assert (
        result == "sub1_tab1|0\nsub1_tab1|2\nsub1_tab1|3\nsub1_tab1|6"
    ), "update of tab1_1, tab1_2 replicated"
    result = node_subscriber1.safe_psql("SELECT a FROM tab1_2_1 ORDER BY 1")
    assert result == "", "updates of tab1_2 replicated into tab1_2_1 correctly"
    result = node_subscriber1.safe_psql("SELECT a FROM tab1_2_2 ORDER BY 1")
    assert result == "6", "updates of tab1_2 replicated into tab1_2_2 correctly"
    result = node_subscriber1.safe_psql(
        "SELECT * FROM sub1_trigger_activity ORDER BY tgtab, tgop, tgwhen, olda, newa;"
    )
    assert (
        result
        == "tab1_2_2|INSERT|AFTER|ROW||6\ntab1_2_2|UPDATE|AFTER|ROW|4|6\ntab1_2_2|UPDATE|AFTER|ROW|6|4"
    ), "check replica update after trigger applied on subscriber"
    result = node_subscriber2.safe_psql("SELECT c, a FROM tab1_1 ORDER BY 1, 2")
    assert result == "sub2_tab1_1|2\nsub2_tab1_1|3", "update of tab1_1 replicated"
    result = node_subscriber2.safe_psql("SELECT c, a FROM tab1_2 ORDER BY 1, 2")
    assert result == "sub2_tab1_2|6", "tab1_2 updated"
    result = node_subscriber2.safe_psql(
        "SELECT * FROM sub2_trigger_activity ORDER BY tgtab, tgop, tgwhen, olda, newa;"
    )
    assert (
        result
        == "tab1_2|INSERT|AFTER|ROW||5\ntab1_2|UPDATE|AFTER|ROW|4|6\ntab1_2|UPDATE|AFTER|ROW|5|6\ntab1_2|UPDATE|AFTER|ROW|6|4"
    ), "check replica update after trigger applied on subscriber"
    result = node_subscriber2.safe_psql("SELECT c, a FROM tab1_def ORDER BY 1")
    assert result == "sub2_tab1_def|0", "tab1_def unchanged"
    node_publisher.safe_psql("UPDATE tab1 SET a = 1 WHERE a = 0")
    node_publisher.safe_psql("UPDATE tab1 SET a = 4 WHERE a = 1")
    node_publisher.wait_for_catchup("sub1")
    node_publisher.wait_for_catchup("sub2")
    result = node_subscriber1.safe_psql("SELECT c, a FROM tab1 ORDER BY 1, 2")
    assert (
        result == "sub1_tab1|2\nsub1_tab1|3\nsub1_tab1|4\nsub1_tab1|6"
    ), "update of tab1 (delete from tab1_def + insert into tab1_1) replicated"
    result = node_subscriber1.safe_psql("SELECT a FROM tab1_2_2 ORDER BY 1")
    assert (
        result == "4\n6"
    ), "updates of tab1 (delete + insert) replicated into tab1_2_2 correctly"
    result = node_subscriber2.safe_psql("SELECT c, a FROM tab1_1 ORDER BY 1, 2")
    assert result == "sub2_tab1_1|2\nsub2_tab1_1|3", "tab1_1 unchanged"
    result = node_subscriber2.safe_psql("SELECT c, a FROM tab1_2 ORDER BY 1, 2")
    assert result == "sub2_tab1_2|4\nsub2_tab1_2|6", "insert into tab1_2 replicated"
    result = node_subscriber2.safe_psql("SELECT a FROM tab1_def ORDER BY 1")
    assert result == "", "delete from tab1_def replicated"
    node_publisher.safe_psql("DELETE FROM tab1 WHERE a IN (2, 3, 5)")
    node_publisher.safe_psql("DELETE FROM tab1_2")
    node_publisher.wait_for_catchup("sub1")
    node_publisher.wait_for_catchup("sub2")
    result = node_subscriber1.safe_psql("SELECT a FROM tab1")
    assert result == "", "delete from tab1_1, tab1_2 replicated"
    result = node_subscriber2.safe_psql("SELECT a FROM tab1_1")
    assert result == "", "delete from tab1_1 replicated"
    result = node_subscriber2.safe_psql("SELECT a FROM tab1_2")
    assert result == "", "delete from tab1_2 replicated"
    node_subscriber1.safe_psql("INSERT INTO tab1 (a) VALUES (1), (2), (5)")
    node_subscriber2.safe_psql("INSERT INTO tab1_2 (a) VALUES (2)")
    node_publisher.safe_psql("TRUNCATE tab1_2")
    node_publisher.wait_for_catchup("sub1")
    node_publisher.wait_for_catchup("sub2")
    result = node_subscriber1.safe_psql("SELECT a FROM tab1 ORDER BY 1")
    assert result == "1\n2", "truncate of tab1_2 replicated"
    result = node_subscriber2.safe_psql("SELECT a FROM tab1_2 ORDER BY 1")
    assert result == "", "truncate of tab1_2 replicated"
    node_publisher.safe_psql("TRUNCATE tab1")
    node_publisher.wait_for_catchup("sub1")
    node_publisher.wait_for_catchup("sub2")
    result = node_subscriber1.safe_psql("SELECT a FROM tab1 ORDER BY 1")
    assert result == "", "truncate of tab1_1 replicated"
    result = node_subscriber2.safe_psql("SELECT a FROM tab1 ORDER BY 1")
    assert result == "", "truncate of tab1 replicated"
    node_publisher.safe_psql(
        "INSERT INTO tab1 VALUES (1, 'foo'), (4, 'bar'), (10, 'baz')"
    )
    node_publisher.wait_for_catchup("sub1")
    node_publisher.wait_for_catchup("sub2")
    node_subscriber1.safe_psql("DELETE FROM tab1")
    log_location = node_subscriber1.current_log_position()
    node_publisher.safe_psql("UPDATE tab1 SET b = 'quux' WHERE a = 4")
    node_publisher.safe_psql("DELETE FROM tab1")
    node_publisher.wait_for_catchup("sub1")
    node_publisher.wait_for_catchup("sub2")
    assert node_subscriber1.log_matches(
        r"""conflict detected on relation "public.tab1_2_2": conflict=update_missing.*\n.*DETAIL:.* Could not find the row to be updated: remote row \(null, 4, quux\), replica identity \(a\)=\(4\)""",
        log_location,
    ), "update target row is missing in tab1_2_2"
    assert node_subscriber1.log_matches(
        r"""conflict detected on relation "public.tab1_1": conflict=delete_missing.*\n.*DETAIL:.* Could not find the row to be deleted: replica identity \(a\)=\(1\)""",
        log_location,
    ), "delete target row is missing in tab1_1"
    assert node_subscriber1.log_matches(
        r"""conflict detected on relation "public.tab1_2_2": conflict=delete_missing.*\n.*DETAIL:.* Could not find the row to be deleted: replica identity \(a\)=\(4\)""",
        log_location,
    ), "delete target row is missing in tab1_2_2"
    assert node_subscriber1.log_matches(
        r"""conflict detected on relation "public.tab1_def": conflict=delete_missing.*\n.*DETAIL:.* Could not find the row to be deleted: replica identity \(a\)=\(10\)""",
        log_location,
    ), "delete target row is missing in tab1_def"
    node_publisher.safe_psql("DROP PUBLICATION pub1")
    node_publisher.safe_psql(
        "CREATE TABLE tab2 (a int PRIMARY KEY, b text) PARTITION BY LIST (a)"
    )
    node_publisher.safe_psql("CREATE TABLE tab2_1 (b text, a int NOT NULL)")
    node_publisher.safe_psql(
        "ALTER TABLE tab2 ATTACH PARTITION tab2_1 FOR VALUES IN (0, 1, 2, 3)"
    )
    node_publisher.safe_psql(
        "CREATE TABLE tab2_2 PARTITION OF tab2 FOR VALUES IN (5, 6)"
    )
    node_publisher.safe_psql(
        "CREATE TABLE tab3 (a int PRIMARY KEY, b text) PARTITION BY LIST (a)"
    )
    node_publisher.safe_psql(
        "CREATE TABLE tab3_1 PARTITION OF tab3 FOR VALUES IN (0, 1, 2, 3, 5, 6)"
    )
    node_publisher.safe_psql(
        "CREATE TABLE tab4 (a int PRIMARY KEY) PARTITION BY LIST (a)"
    )
    node_publisher.safe_psql(
        "CREATE TABLE tab4_1 PARTITION OF tab4 FOR VALUES IN (-1, 0, 1) PARTITION BY LIST (a)"
    )
    node_publisher.safe_psql(
        "CREATE TABLE tab4_1_1 PARTITION OF tab4_1 FOR VALUES IN (-1, 0, 1)"
    )
    node_publisher.safe_psql(
        "ALTER PUBLICATION pub_all SET (publish_via_partition_root = true)"
    )
    node_publisher.safe_psql(
        "CREATE PUBLICATION pub_viaroot FOR TABLE tab2, tab2_1, tab3_1 WITH (publish_via_partition_root = true)"
    )
    node_publisher.safe_psql(
        "CREATE PUBLICATION pub_lower_level FOR TABLE tab4_1 WITH (publish_via_partition_root = true)"
    )
    node_publisher.safe_psql("INSERT INTO tab2 VALUES (1)")
    node_publisher.safe_psql("INSERT INTO tab4 VALUES (-1)")
    node_subscriber1.safe_psql("DROP SUBSCRIPTION sub1")
    node_subscriber1.safe_psql(
        "CREATE TABLE tab2 (a int PRIMARY KEY, c text DEFAULT 'sub1_tab2', b text) PARTITION BY RANGE (a)"
    )
    node_subscriber1.safe_psql(
        "CREATE TABLE tab2_1 (c text DEFAULT 'sub1_tab2', b text, a int NOT NULL)"
    )
    node_subscriber1.safe_psql(
        "ALTER TABLE tab2 ATTACH PARTITION tab2_1 FOR VALUES FROM (0) TO (10)"
    )
    node_subscriber1.safe_psql(
        "CREATE TABLE tab3_1 (c text DEFAULT 'sub1_tab3_1', b text, a int NOT NULL PRIMARY KEY)"
    )
    node_subscriber1.safe_psql(
        "CREATE SUBSCRIPTION sub_viaroot CONNECTION '"
        + publisher_connstr
        + "' PUBLICATION pub_viaroot"
    )
    node_subscriber2.safe_psql("DROP TABLE tab1")
    node_subscriber2.safe_psql(
        "CREATE TABLE tab1 (a int PRIMARY KEY, c text DEFAULT 'sub2_tab1', b text) PARTITION BY HASH (a)"
    )
    node_subscriber2.safe_psql(
        "CREATE TABLE tab1_part1 (b text, c text, a int NOT NULL)"
    )
    node_subscriber2.safe_psql(
        "ALTER TABLE tab1 ATTACH PARTITION tab1_part1 FOR VALUES WITH (MODULUS 2, REMAINDER 0)"
    )
    node_subscriber2.safe_psql(
        "CREATE TABLE tab1_part2 PARTITION OF tab1 FOR VALUES WITH (MODULUS 2, REMAINDER 1)"
    )
    node_subscriber2.safe_psql(
        "CREATE TABLE tab2 (a int PRIMARY KEY, c text DEFAULT 'sub2_tab2', b text)"
    )
    node_subscriber2.safe_psql(
        "CREATE TABLE tab3 (a int PRIMARY KEY, c text DEFAULT 'sub2_tab3', b text)"
    )
    node_subscriber2.safe_psql(
        "CREATE TABLE tab3_1 (a int PRIMARY KEY, c text DEFAULT 'sub2_tab3_1', b text)"
    )
    node_subscriber2.safe_psql("CREATE TABLE tab4 (a int PRIMARY KEY)")
    node_subscriber2.safe_psql("CREATE TABLE tab4_1 (a int PRIMARY KEY)")
    node_subscriber2.safe_psql(
        "ALTER SUBSCRIPTION sub2 SET PUBLICATION pub_lower_level, pub_all"
    )
    node_subscriber1.wait_for_subscription_sync()
    node_subscriber2.wait_for_subscription_sync()
    result = node_subscriber1.safe_psql("SELECT c, a FROM tab2")
    assert result == "sub1_tab2|1", "initial data synced for pub_viaroot"
    result = node_subscriber2.safe_psql("SELECT a FROM tab4 ORDER BY 1")
    assert result == "-1", "initial data synced for pub_lower_level and pub_all"
    result = node_subscriber2.safe_psql("SELECT a FROM tab4_1 ORDER BY 1")
    assert result == "", "initial data synced for pub_lower_level and pub_all"
    node_publisher.safe_psql("INSERT INTO tab1 VALUES (1), (0)")
    node_publisher.safe_psql("INSERT INTO tab1_1 (a) VALUES (3)")
    node_publisher.safe_psql("INSERT INTO tab1_2 VALUES (5)")
    node_publisher.safe_psql("INSERT INTO tab2 VALUES (0), (3), (5)")
    node_publisher.safe_psql("INSERT INTO tab3 VALUES (1), (0), (3), (5)")
    node_publisher.safe_psql("INSERT INTO tab4 VALUES (0)")
    node_publisher.wait_for_catchup("sub_viaroot")
    node_publisher.wait_for_catchup("sub2")
    result = node_subscriber1.safe_psql("SELECT c, a FROM tab2 ORDER BY 1, 2")
    assert (
        result == "sub1_tab2|0\nsub1_tab2|1\nsub1_tab2|3\nsub1_tab2|5"
    ), "inserts into tab2 replicated"
    result = node_subscriber1.safe_psql("SELECT c, a FROM tab3_1 ORDER BY 1, 2")
    assert (
        result == "sub1_tab3_1|0\nsub1_tab3_1|1\nsub1_tab3_1|3\nsub1_tab3_1|5"
    ), "inserts into tab3_1 replicated"
    result = node_subscriber2.safe_psql("SELECT c, a FROM tab1 ORDER BY 1, 2")
    assert (
        result == "sub2_tab1|0\nsub2_tab1|1\nsub2_tab1|3\nsub2_tab1|5"
    ), "inserts into tab1 replicated"
    result = node_subscriber2.safe_psql("SELECT c, a FROM tab2 ORDER BY 1, 2")
    assert (
        result == "sub2_tab2|0\nsub2_tab2|1\nsub2_tab2|3\nsub2_tab2|5"
    ), "inserts into tab2 replicated"
    result = node_subscriber2.safe_psql("SELECT c, a FROM tab3 ORDER BY 1, 2")
    assert (
        result == "sub2_tab3|0\nsub2_tab3|1\nsub2_tab3|3\nsub2_tab3|5"
    ), "inserts into tab3 replicated"
    result = node_subscriber2.safe_psql("SELECT a FROM tab4 ORDER BY 1")
    assert result == "-1\n0", "inserts into tab4 replicated"
    result = node_subscriber2.safe_psql("SELECT a FROM tab4_1 ORDER BY 1")
    assert result == "", "inserts into tab4_1 replicated"
    node_subscriber2.safe_psql(
        "ALTER SUBSCRIPTION sub2 SET PUBLICATION pub_all, pub_lower_level"
    )
    node_subscriber2.wait_for_subscription_sync()
    node_publisher.safe_psql("INSERT INTO tab4 VALUES (1)")
    node_publisher.wait_for_catchup("sub2")
    result = node_subscriber2.safe_psql("SELECT a FROM tab4 ORDER BY 1")
    assert result == "-1\n0\n1", "inserts into tab4 replicated"
    result = node_subscriber2.safe_psql("SELECT a FROM tab4_1 ORDER BY 1")
    assert result == "", "inserts into tab4_1 replicated"
    node_publisher.safe_psql("UPDATE tab1 SET a = 6 WHERE a = 5")
    node_publisher.safe_psql("UPDATE tab2 SET a = 6 WHERE a = 5")
    node_publisher.safe_psql("UPDATE tab3 SET a = 6 WHERE a = 5")
    node_publisher.wait_for_catchup("sub_viaroot")
    node_publisher.wait_for_catchup("sub2")
    result = node_subscriber1.safe_psql("SELECT c, a FROM tab2 ORDER BY 1, 2")
    assert (
        result == "sub1_tab2|0\nsub1_tab2|1\nsub1_tab2|3\nsub1_tab2|6"
    ), "update of tab2 replicated"
    result = node_subscriber1.safe_psql("SELECT c, a FROM tab3_1 ORDER BY 1, 2")
    assert (
        result == "sub1_tab3_1|0\nsub1_tab3_1|1\nsub1_tab3_1|3\nsub1_tab3_1|6"
    ), "update of tab3_1 replicated"
    result = node_subscriber2.safe_psql("SELECT c, a FROM tab1 ORDER BY 1, 2")
    assert (
        result == "sub2_tab1|0\nsub2_tab1|1\nsub2_tab1|3\nsub2_tab1|6"
    ), "inserts into tab1 replicated"
    result = node_subscriber2.safe_psql("SELECT c, a FROM tab2 ORDER BY 1, 2")
    assert (
        result == "sub2_tab2|0\nsub2_tab2|1\nsub2_tab2|3\nsub2_tab2|6"
    ), "inserts into tab2 replicated"
    result = node_subscriber2.safe_psql("SELECT c, a FROM tab3 ORDER BY 1, 2")
    assert (
        result == "sub2_tab3|0\nsub2_tab3|1\nsub2_tab3|3\nsub2_tab3|6"
    ), "inserts into tab3 replicated"
    node_publisher.safe_psql("UPDATE tab1 SET a = 2 WHERE a = 6")
    node_publisher.safe_psql("UPDATE tab2 SET a = 2 WHERE a = 6")
    node_publisher.safe_psql("UPDATE tab3 SET a = 2 WHERE a = 6")
    node_publisher.wait_for_catchup("sub_viaroot")
    node_publisher.wait_for_catchup("sub2")
    result = node_subscriber1.safe_psql("SELECT c, a FROM tab2 ORDER BY 1, 2")
    assert (
        result == "sub1_tab2|0\nsub1_tab2|1\nsub1_tab2|2\nsub1_tab2|3"
    ), "update of tab2 replicated"
    result = node_subscriber1.safe_psql("SELECT c, a FROM tab3_1 ORDER BY 1, 2")
    assert (
        result == "sub1_tab3_1|0\nsub1_tab3_1|1\nsub1_tab3_1|2\nsub1_tab3_1|3"
    ), "update of tab3_1 replicated"
    result = node_subscriber2.safe_psql("SELECT c, a FROM tab1 ORDER BY 1, 2")
    assert (
        result == "sub2_tab1|0\nsub2_tab1|1\nsub2_tab1|2\nsub2_tab1|3"
    ), "update of tab1 replicated"
    result = node_subscriber2.safe_psql("SELECT c, a FROM tab2 ORDER BY 1, 2")
    assert (
        result == "sub2_tab2|0\nsub2_tab2|1\nsub2_tab2|2\nsub2_tab2|3"
    ), "update of tab2 replicated"
    result = node_subscriber2.safe_psql("SELECT c, a FROM tab3 ORDER BY 1, 2")
    assert (
        result == "sub2_tab3|0\nsub2_tab3|1\nsub2_tab3|2\nsub2_tab3|3"
    ), "update of tab3 replicated"
    node_publisher.safe_psql("DELETE FROM tab1")
    node_publisher.safe_psql("DELETE FROM tab2")
    node_publisher.safe_psql("DELETE FROM tab3")
    node_publisher.wait_for_catchup("sub_viaroot")
    node_publisher.wait_for_catchup("sub2")
    result = node_subscriber1.safe_psql("SELECT a FROM tab2")
    assert result == "", "delete tab2 replicated"
    result = node_subscriber2.safe_psql("SELECT a FROM tab1")
    assert result == "", "delete from tab1 replicated"
    result = node_subscriber2.safe_psql("SELECT a FROM tab2")
    assert result == "", "delete from tab2 replicated"
    result = node_subscriber2.safe_psql("SELECT a FROM tab3")
    assert result == "", "delete from tab3 replicated"
    node_publisher.safe_psql("INSERT INTO tab1 VALUES (1), (2), (5)")
    node_publisher.safe_psql("INSERT INTO tab2 VALUES (1), (2), (5)")
    node_publisher.safe_psql("TRUNCATE tab1_2, tab2_1, tab3_1")
    node_publisher.wait_for_catchup("sub_viaroot")
    node_publisher.wait_for_catchup("sub2")
    result = node_subscriber1.safe_psql("SELECT a FROM tab2 ORDER BY 1")
    assert result == "1\n2\n5", "truncate of tab2_1 NOT replicated"
    result = node_subscriber2.safe_psql("SELECT a FROM tab1 ORDER BY 1")
    assert result == "1\n2\n5", "truncate of tab1_2 NOT replicated"
    result = node_subscriber2.safe_psql("SELECT a FROM tab2 ORDER BY 1")
    assert result == "1\n2\n5", "truncate of tab2_1 NOT replicated"
    node_publisher.safe_psql("TRUNCATE tab1, tab2, tab3")
    node_publisher.wait_for_catchup("sub_viaroot")
    node_publisher.wait_for_catchup("sub2")
    result = node_subscriber1.safe_psql("SELECT a FROM tab2")
    assert result == "", "truncate of tab2 replicated"
    result = node_subscriber2.safe_psql("SELECT a FROM tab1")
    assert result == "", "truncate of tab1 replicated"
    result = node_subscriber2.safe_psql("SELECT a FROM tab2")
    assert result == "", "truncate of tab2 replicated"
    result = node_subscriber2.safe_psql("SELECT a FROM tab3")
    assert result == "", "truncate of tab3 replicated"
    result = node_subscriber2.safe_psql("SELECT a FROM tab3_1")
    assert result == "", "truncate of tab3_1 replicated"
    node_publisher.safe_psql(
        "ALTER TABLE tab2 DROP b, ADD COLUMN c text DEFAULT 'pub_tab2', ADD b text"
    )
    node_publisher.safe_psql(
        "INSERT INTO tab2 (a, b) VALUES (1, 'xxx'), (3, 'yyy'), (5, 'zzz')"
    )
    node_publisher.safe_psql("INSERT INTO tab2 (a, b, c) VALUES (6, 'aaa', 'xxx_c')")
    node_publisher.wait_for_catchup("sub_viaroot")
    node_publisher.wait_for_catchup("sub2")
    result = node_subscriber1.safe_psql("SELECT c, a, b FROM tab2 ORDER BY 1, 2")
    assert (
        result == "pub_tab2|1|xxx\npub_tab2|3|yyy\npub_tab2|5|zzz\nxxx_c|6|aaa"
    ), "inserts into tab2 replicated"
    result = node_subscriber2.safe_psql("SELECT c, a, b FROM tab2 ORDER BY 1, 2")
    assert (
        result == "pub_tab2|1|xxx\npub_tab2|3|yyy\npub_tab2|5|zzz\nxxx_c|6|aaa"
    ), "inserts into tab2 replicated"
    node_subscriber1.safe_psql("DELETE FROM tab2")
    log_location = node_subscriber1.current_log_position()
    node_publisher.safe_psql("UPDATE tab2 SET b = 'quux' WHERE a = 5")
    node_publisher.safe_psql("DELETE FROM tab2 WHERE a = 1")
    node_publisher.wait_for_catchup("sub_viaroot")
    node_publisher.wait_for_catchup("sub2")
    assert node_subscriber1.log_matches(
        r"""conflict detected on relation "public.tab2_1": conflict=update_missing.*\n.*DETAIL:.* Could not find the row to be updated: remote row \(pub_tab2, quux, 5\), replica identity \(a\)=\(5\)""",
        log_location,
    ), "update target row is missing in tab2_1"
    assert node_subscriber1.log_matches(
        r"""conflict detected on relation "public.tab2_1": conflict=delete_missing.*\n.*DETAIL:.* Could not find the row to be deleted: replica identity \(a\)=\(1\)""",
        log_location,
    ), "delete target row is missing in tab2_1"
    node_subscriber1.append_conf("track_commit_timestamp = on")
    node_subscriber1.restart()
    node_subscriber1.safe_psql("INSERT INTO tab2 VALUES (3, 'yyy')")
    node_publisher.safe_psql("UPDATE tab2 SET b = 'quux' WHERE a = 3")
    node_publisher.wait_for_catchup("sub_viaroot")
    assert node_subscriber1.log_matches(
        r"""conflict detected on relation "public.tab2_1": conflict=update_origin_differs.*\n.*DETAIL:.* Updating the row that was modified locally in transaction [0-9]+ at .*: local row \(yyy, null, 3\), remote row \(pub_tab2, quux, 3\), replica identity \(a\)=\(3\).""",
        log_location,
    ), "updating a row that was modified by a different origin"
    node_subscriber1.append_conf("track_commit_timestamp = off")
    node_subscriber1.restart()
    node_publisher.safe_psql(
        "CREATE TABLE tab5 (a int NOT NULL, b int);\n\tCREATE UNIQUE INDEX tab5_a_idx ON tab5 (a);\n\tALTER TABLE tab5 REPLICA IDENTITY USING INDEX tab5_a_idx;"
    )
    node_subscriber2.safe_psql(
        "CREATE TABLE tab5 (a int NOT NULL, b int, c int) PARTITION BY LIST (a);\n\tCREATE TABLE tab5_1 PARTITION OF tab5 DEFAULT;\n\tCREATE UNIQUE INDEX tab5_a_idx ON tab5 (a);\n\tALTER TABLE tab5 REPLICA IDENTITY USING INDEX tab5_a_idx;\n\tALTER TABLE tab5_1 REPLICA IDENTITY USING INDEX tab5_1_a_idx;"
    )
    node_subscriber2.safe_psql("ALTER SUBSCRIPTION sub2 REFRESH PUBLICATION")
    node_subscriber2.wait_for_subscription_sync()
    node_publisher.safe_psql("INSERT INTO tab5 VALUES (1, 1)")
    node_publisher.safe_psql("UPDATE tab5 SET a = 2 WHERE a = 1")
    node_publisher.wait_for_catchup("sub2")
    result = node_subscriber2.safe_psql("SELECT a, b FROM tab5 ORDER BY 1")
    assert result == "2|1", "updates of tab5 replicated correctly"
    node_subscriber2.safe_psql(
        "ALTER TABLE tab5 DETACH PARTITION tab5_1;\n\tALTER TABLE tab5_1 DROP COLUMN b;\n\tALTER TABLE tab5_1 ADD COLUMN b int;\n\tALTER TABLE tab5 ATTACH PARTITION tab5_1 DEFAULT"
    )
    node_publisher.safe_psql("UPDATE tab5 SET a = 3 WHERE a = 2")
    node_publisher.wait_for_catchup("sub2")
    result = node_subscriber2.safe_psql("SELECT a, b, c FROM tab5 ORDER BY 1")
    assert (
        result == "3|1|"
    ), "updates of tab5 replicated correctly after altering table on subscriber"
    node_publisher.safe_psql(
        "ALTER TABLE tab5 DROP COLUMN b, ADD COLUMN c INT;\n\tALTER TABLE tab5 ADD COLUMN b INT;"
    )
    node_publisher.safe_psql("UPDATE tab5 SET c = 1 WHERE a = 3")
    node_publisher.wait_for_catchup("sub2")
    result = node_subscriber2.safe_psql("SELECT a, b, c FROM tab5 ORDER BY 1")
    assert (
        result == "3||1"
    ), "updates of tab5 replicated correctly after altering table on publisher"
    node_subscriber2.safe_psql("ALTER TABLE tab5 REPLICA IDENTITY NOTHING")
    node_publisher.safe_psql("UPDATE tab5 SET a = 4 WHERE a = 3")
    node_publisher.wait_for_catchup("sub2")
    result = node_subscriber2.safe_psql("SELECT a, b, c FROM tab5_1 ORDER BY 1")
    assert result == "4||1", "updates of tab5 replicated correctly"

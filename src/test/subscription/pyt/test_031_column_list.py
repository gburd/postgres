# Copyright (c) 2022-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long,too-many-statements
"""Port of src/test/subscription/t/031_column_list.pl.

Partial-column publication of tables (column lists), including weird column
names, fewer columns on the subscriber, partitioned tables, enum types,
publish_via_partition_root, and detection of conflicting column lists across
publications. Generated from the Perl original via .agent/gen_golden.py with
the error-detection tail hand-finished.
"""

import re


def test_031_column_list(create_pg):
    """Generated golden port of 031_column_list."""
    node_publisher = create_pg("publisher", allows_streaming="logical", start=False)
    node_publisher.start()
    node_subscriber = create_pg("subscriber", start=False)
    node_subscriber.append_conf("max_logical_replication_workers = 6")
    node_subscriber.start()
    publisher_connstr = node_publisher.connstr() + " dbname=postgres"
    node_publisher.safe_psql('CREATE TABLE tab1 (a int PRIMARY KEY, "B" int, c int)')
    node_subscriber.safe_psql('CREATE TABLE tab1 (a int PRIMARY KEY, "B" int, c int)')
    node_publisher.safe_psql("CREATE TABLE tab2 (a int PRIMARY KEY, b varchar, c int);")
    node_subscriber.safe_psql("CREATE TABLE tab2 (a int PRIMARY KEY, b varchar)")
    node_publisher.safe_psql(
        'CREATE TABLE tab3 ("a\'" int PRIMARY KEY, "B" varchar, "c\'" int)'
    )
    node_subscriber.safe_psql('CREATE TABLE tab3 ("a\'" int PRIMARY KEY, "c\'" int)')
    node_publisher.safe_psql(
        "CREATE TABLE test_part (a int PRIMARY KEY, b text, c timestamptz) PARTITION BY LIST (a);\n\tCREATE TABLE test_part_1_1 PARTITION OF test_part FOR VALUES IN (1,2,3,4,5,6);\n\tCREATE TABLE test_part_2_1 PARTITION OF test_part FOR VALUES IN (7,8,9,10,11,12) PARTITION BY LIST (a);\n\tCREATE TABLE test_part_2_2 PARTITION OF test_part_2_1 FOR VALUES IN (7,8,9,10);"
    )
    node_subscriber.safe_psql(
        "CREATE TABLE test_part (a int PRIMARY KEY, b text) PARTITION BY LIST (a);\n\tCREATE TABLE test_part_1_1 PARTITION OF test_part FOR VALUES IN (1,2,3,4,5,6);\n\tCREATE TABLE test_part_2_1 PARTITION OF test_part FOR VALUES IN (7,8,9,10,11,12) PARTITION BY LIST (a);\n\tCREATE TABLE test_part_2_2 PARTITION OF test_part_2_1 FOR VALUES IN (7,8,9,10);"
    )
    node_publisher.safe_psql(
        "CREATE TYPE test_typ AS ENUM ('blue', 'red');\n\tCREATE TABLE tab4 (a INT PRIMARY KEY, b test_typ, c int, d text);"
    )
    node_subscriber.safe_psql(
        "CREATE TYPE test_typ AS ENUM ('blue', 'red');\n\tCREATE TABLE tab4 (a INT PRIMARY KEY, b test_typ, d text);"
    )
    node_publisher.safe_psql(
        'CREATE PUBLICATION pub1\n\t   FOR TABLE tab1 (a, "B"), tab3 ("a\'", "c\'"), test_part (a, b), tab4 (a, b, d)\n\t  WITH (publish_via_partition_root = \'true\');'
    )
    result = node_publisher.safe_psql(
        "SELECT relname, prattrs\n\tFROM pg_publication_rel pb JOIN pg_class pc ON(pb.prrelid = pc.oid)\n\tORDER BY relname"
    )
    assert (
        result == "tab1|1 2\ntab3|1 3\ntab4|1 2 4\ntest_part|1 2"
    ), "publication relation updated"
    node_publisher.safe_psql(
        "INSERT INTO tab1 VALUES (1, 2, 3);\n\tINSERT INTO tab1 VALUES (4, 5, 6);"
    )
    node_publisher.safe_psql(
        "INSERT INTO tab3 VALUES (1, 2, 3);\n\tINSERT INTO tab3 VALUES (4, 5, 6);"
    )
    node_publisher.safe_psql(
        "INSERT INTO tab4 VALUES (1, 'red', 3, 'oh my');\n\tINSERT INTO tab4 VALUES (2, 'blue', 4, 'hello');"
    )
    node_publisher.safe_psql(
        "INSERT INTO test_part VALUES (1, 'abc', '2021-07-04 12:00:00');\n\tINSERT INTO test_part VALUES (2, 'bcd', '2021-07-03 11:12:13');\n\tINSERT INTO test_part VALUES (7, 'abc', '2021-07-04 12:00:00');\n\tINSERT INTO test_part VALUES (8, 'bcd', '2021-07-03 11:12:13');"
    )
    node_subscriber.safe_psql(
        "CREATE SUBSCRIPTION sub1 CONNECTION '"
        + publisher_connstr
        + "' PUBLICATION pub1"
    )
    node_subscriber.wait_for_subscription_sync()
    result = node_subscriber.safe_psql("SELECT * FROM tab1 ORDER BY a")
    assert result == "1|2|\n4|5|", "insert on column tab1.c is not replicated"
    result = node_subscriber.safe_psql('SELECT * FROM tab3 ORDER BY "a\'"')
    assert result == "1|3\n4|6", "insert on column tab3.b is not replicated"
    result = node_subscriber.safe_psql("SELECT * FROM tab4 ORDER BY a")
    assert (
        result == "1|red|oh my\n2|blue|hello"
    ), "insert on column tab4.c is not replicated"
    result = node_subscriber.safe_psql("SELECT * FROM test_part ORDER BY a")
    assert (
        result == "1|abc\n2|bcd\n7|abc\n8|bcd"
    ), "insert on column test_part.c columns is not replicated"
    node_publisher.safe_psql(
        "INSERT INTO tab1 VALUES (2, 3, 4);\n\tINSERT INTO tab1 VALUES (5, 6, 7);"
    )
    node_publisher.safe_psql(
        "INSERT INTO tab3 VALUES (2, 3, 4);\n\tINSERT INTO tab3 VALUES (5, 6, 7);"
    )
    node_publisher.safe_psql(
        "INSERT INTO tab4 VALUES (3, 'red', 5, 'foo');\n\tINSERT INTO tab4 VALUES (4, 'blue', 6, 'bar');"
    )
    node_publisher.safe_psql(
        "INSERT INTO test_part VALUES (3, 'xxx', '2022-02-01 10:00:00');\n\tINSERT INTO test_part VALUES (4, 'yyy', '2022-03-02 15:12:13');\n\tINSERT INTO test_part VALUES (9, 'zzz', '2022-04-03 21:00:00');\n\tINSERT INTO test_part VALUES (10, 'qqq', '2022-05-04 22:12:13');"
    )
    node_publisher.wait_for_catchup("sub1")
    result = node_subscriber.safe_psql("SELECT * FROM tab1 ORDER BY a")
    assert (
        result == "1|2|\n2|3|\n4|5|\n5|6|"
    ), "insert on column tab1.c is not replicated"
    result = node_subscriber.safe_psql('SELECT * FROM tab3 ORDER BY "a\'"')
    assert result == "1|3\n2|4\n4|6\n5|7", "insert on column tab3.b is not replicated"
    result = node_subscriber.safe_psql("SELECT * FROM tab4 ORDER BY a")
    assert (
        result == "1|red|oh my\n2|blue|hello\n3|red|foo\n4|blue|bar"
    ), "insert on column tab4.c is not replicated"
    result = node_subscriber.safe_psql("SELECT * FROM test_part ORDER BY a")
    assert (
        result == "1|abc\n2|bcd\n3|xxx\n4|yyy\n7|abc\n8|bcd\n9|zzz\n10|qqq"
    ), "insert on column test_part.c columns is not replicated"
    node_publisher.safe_psql('UPDATE tab1 SET "B" = 2 * "B" where a = 1')
    node_publisher.safe_psql("UPDATE tab1 SET c = 2*c where a = 4")
    node_publisher.safe_psql(
        'UPDATE tab3 SET "B" = "B" || \' updated\' where "a\'" = 4'
    )
    node_publisher.safe_psql('UPDATE tab3 SET "c\'" = 2 * "c\'" where "a\'" = 1')
    node_publisher.safe_psql(
        "UPDATE tab4 SET b = 'blue', c = c * 2, d = d || ' updated' where a = 1"
    )
    node_publisher.safe_psql(
        "UPDATE tab4 SET b = 'red', c = c * 2, d = d || ' updated' where a = 2"
    )
    node_publisher.wait_for_catchup("sub1")
    result = node_subscriber.safe_psql("SELECT * FROM tab1 ORDER BY a")
    assert (
        result == "1|4|\n2|3|\n4|5|\n5|6|"
    ), "only update on column tab1.b is replicated"
    result = node_subscriber.safe_psql('SELECT * FROM tab3 ORDER BY "a\'"')
    assert result == "1|6\n2|4\n4|6\n5|7", "only update on column tab3.c is replicated"
    result = node_subscriber.safe_psql("SELECT * FROM tab4 ORDER BY a")
    assert (
        result == "1|blue|oh my updated\n2|red|hello updated\n3|red|foo\n4|blue|bar"
    ), "update on column tab4.c is not replicated"
    node_publisher.safe_psql("INSERT INTO tab2 VALUES (1, 'abc', 3);")
    node_publisher.safe_psql("ALTER PUBLICATION pub1 ADD TABLE tab2 (a, b)")
    node_subscriber.safe_psql("ALTER SUBSCRIPTION sub1 REFRESH PUBLICATION")
    node_subscriber.wait_for_subscription_sync()
    node_publisher.safe_psql("INSERT INTO tab2 VALUES (2, 'def', 6);")
    node_publisher.wait_for_catchup("sub1")
    result = node_subscriber.safe_psql("SELECT * FROM tab2 ORDER BY a")
    assert result == "1|abc\n2|def", "insert on column tab2.c is not replicated"
    node_publisher.safe_psql(
        "UPDATE tab2 SET c = 5 where a = 1;\n\tUPDATE tab2 SET b = 'xyz' where a = 2;"
    )
    node_publisher.wait_for_catchup("sub1")
    result = node_subscriber.safe_psql("SELECT * FROM tab2 ORDER BY a")
    assert result == "1|abc\n2|xyz", "update on column tab2.c is not replicated"
    node_publisher.safe_psql(
        "CREATE TABLE tab5 (a int PRIMARY KEY, b int, c int, d int);\n\tCREATE PUBLICATION pub2 FOR TABLE tab5 (a, b);\n\tCREATE PUBLICATION pub3 FOR TABLE tab5 (a, b);\n\n\t-- insert a couple initial records\n\tINSERT INTO tab5 VALUES (1, 11, 111, 1111);\n\tINSERT INTO tab5 VALUES (2, 22, 222, 2222);"
    )
    node_subscriber.safe_psql("CREATE TABLE tab5 (a int PRIMARY KEY, b int, d int);")
    node_subscriber.safe_psql(
        "DROP SUBSCRIPTION sub1;\n\tCREATE SUBSCRIPTION sub1 CONNECTION '"
        + publisher_connstr
        + "' PUBLICATION pub2, pub3"
    )
    node_subscriber.wait_for_subscription_sync()
    node_publisher.safe_psql(
        "INSERT INTO tab5 VALUES (3, 33, 333, 3333);\n\tINSERT INTO tab5 VALUES (4, 44, 444, 4444);"
    )
    node_publisher.wait_for_catchup("sub1")
    assert (
        node_subscriber.safe_psql("SELECT * FROM tab5 ORDER BY a")
        == "1|11|\n2|22|\n3|33|\n4|44|"
    ), "overlapping publications with overlapping column lists"
    node_publisher.safe_psql(
        "CREATE TABLE tab6 (a int PRIMARY KEY, b int, c int, d int);\n\tCREATE PUBLICATION pub4 FOR TABLE tab6 (a, b);\n\n\t-- initial data\n\tINSERT INTO tab6 VALUES (1, 22, 333, 4444);"
    )
    node_subscriber.safe_psql(
        "CREATE TABLE tab6 (a int PRIMARY KEY, b int, c int, d int);"
    )
    node_subscriber.safe_psql(
        "DROP SUBSCRIPTION sub1;\n\tCREATE SUBSCRIPTION sub1 CONNECTION '"
        + publisher_connstr
        + "' PUBLICATION pub4"
    )
    node_subscriber.wait_for_subscription_sync()
    node_publisher.safe_psql(
        "INSERT INTO tab6 VALUES (2, 33, 444, 5555);\n\tUPDATE tab6 SET b = b * 2, c = c * 3, d = d * 4;"
    )
    node_publisher.wait_for_catchup("sub1")
    assert (
        node_subscriber.safe_psql("SELECT * FROM tab6 ORDER BY a") == "1|44||\n2|66||"
    ), "replication with the original primary key"
    node_publisher.safe_psql(
        "ALTER TABLE tab6 DROP CONSTRAINT tab6_pkey;\n\tALTER TABLE tab6 ADD PRIMARY KEY (b);"
    )
    node_subscriber.safe_psql(
        "ALTER TABLE tab6 DROP CONSTRAINT tab6_pkey;\n\tALTER TABLE tab6 ADD PRIMARY KEY (b);"
    )
    node_subscriber.safe_psql("ALTER SUBSCRIPTION sub1 REFRESH PUBLICATION")
    node_subscriber.wait_for_subscription_sync()
    node_publisher.safe_psql(
        "INSERT INTO tab6 VALUES (3, 55, 666, 8888);\n\tUPDATE tab6 SET b = b * 2, c = c * 3, d = d * 4;"
    )
    node_publisher.wait_for_catchup("sub1")
    assert (
        node_subscriber.safe_psql("SELECT * FROM tab6 ORDER BY a")
        == "1|88||\n2|132||\n3|110||"
    ), "replication with the modified primary key"
    node_publisher.safe_psql(
        "CREATE TABLE tab7 (a int PRIMARY KEY, b int, c int, d int);\n\tCREATE PUBLICATION pub5 FOR TABLE tab7 (a, b);\n\n\t-- some initial data\n\tINSERT INTO tab7 VALUES (1, 22, 333, 4444);"
    )
    node_subscriber.safe_psql(
        "CREATE TABLE tab7 (a int PRIMARY KEY, b int, c int, d int);"
    )
    node_subscriber.safe_psql(
        "DROP SUBSCRIPTION sub1;\n\tCREATE SUBSCRIPTION sub1 CONNECTION '"
        + publisher_connstr
        + "' PUBLICATION pub5"
    )
    node_subscriber.wait_for_subscription_sync()
    node_publisher.safe_psql(
        "INSERT INTO tab7 VALUES (2, 33, 444, 5555);\n\tUPDATE tab7 SET b = b * 2, c = c * 3, d = d * 4;"
    )
    node_publisher.wait_for_catchup("sub1")
    assert (
        node_subscriber.safe_psql("SELECT * FROM tab7 ORDER BY a") == "1|44||\n2|66||"
    ), "replication with the original primary key"
    node_publisher.safe_psql(
        "ALTER TABLE tab7 DROP CONSTRAINT tab7_pkey;\n\tALTER TABLE tab7 ADD PRIMARY KEY (a, b);"
    )
    node_publisher.safe_psql(
        "INSERT INTO tab7 VALUES (3, 55, 666, 7777);\n\tUPDATE tab7 SET b = b * 2, c = c * 3, d = d * 4;"
    )
    node_publisher.wait_for_catchup("sub1")
    assert (
        node_subscriber.safe_psql("SELECT * FROM tab7 ORDER BY a")
        == "1|88||\n2|132||\n3|110||"
    ), "replication with the modified primary key"
    node_publisher.safe_psql(
        "ALTER TABLE tab7 DROP CONSTRAINT tab7_pkey;\n\tINSERT INTO tab7 VALUES (4, 77, 888, 9999);\n\t-- update/delete is not allowed for tables without RI\n\tALTER TABLE tab7 ADD PRIMARY KEY (b, a);\n\tUPDATE tab7 SET b = b * 2, c = c * 3, d = d * 4;\n\tDELETE FROM tab7 WHERE a = 1;"
    )
    node_publisher.safe_psql("")
    node_publisher.wait_for_catchup("sub1")
    assert (
        node_subscriber.safe_psql("SELECT * FROM tab7 ORDER BY a")
        == "2|264||\n3|220||\n4|154||"
    ), "replication with the modified primary key"
    node_publisher.safe_psql(
        "CREATE TABLE test_part_a (a int, b int, c int) PARTITION BY LIST (a);\n\n\tCREATE TABLE test_part_a_1 PARTITION OF test_part_a FOR VALUES IN (1,2,3,4,5);\n\tALTER TABLE test_part_a_1 ADD PRIMARY KEY (a);\n\tALTER TABLE test_part_a_1 REPLICA IDENTITY USING INDEX test_part_a_1_pkey;\n\n\tCREATE TABLE test_part_a_2 PARTITION OF test_part_a FOR VALUES IN (6,7,8,9,10);\n\tALTER TABLE test_part_a_2 ADD PRIMARY KEY (b);\n\tALTER TABLE test_part_a_2 REPLICA IDENTITY USING INDEX test_part_a_2_pkey;\n\n\t-- initial data, one row in each partition\n\tINSERT INTO test_part_a VALUES (1, 3);\n\tINSERT INTO test_part_a VALUES (6, 4);"
    )
    node_subscriber.safe_psql(
        "CREATE TABLE test_part_a (b int, a int) PARTITION BY LIST (a);\n\n\tCREATE TABLE test_part_a_1 PARTITION OF test_part_a FOR VALUES IN (1,2,3,4,5);\n\tALTER TABLE test_part_a_1 ADD PRIMARY KEY (a);\n\tALTER TABLE test_part_a_1 REPLICA IDENTITY USING INDEX test_part_a_1_pkey;\n\n\tCREATE TABLE test_part_a_2 PARTITION OF test_part_a FOR VALUES IN (6,7,8,9,10);\n\tALTER TABLE test_part_a_2 ADD PRIMARY KEY (b);\n\tALTER TABLE test_part_a_2 REPLICA IDENTITY USING INDEX test_part_a_2_pkey;"
    )
    node_publisher.safe_psql(
        "CREATE PUBLICATION pub6 FOR TABLE test_part_a (b, a) WITH (publish_via_partition_root = true);\n\tALTER PUBLICATION pub6 ADD TABLE test_part_a_1 (a);\n\tALTER PUBLICATION pub6 ADD TABLE test_part_a_2 (b);"
    )
    node_subscriber.safe_psql(
        "DROP SUBSCRIPTION sub1;\n\tCREATE SUBSCRIPTION sub1 CONNECTION '"
        + publisher_connstr
        + "' PUBLICATION pub6"
    )
    node_subscriber.wait_for_subscription_sync()
    node_publisher.safe_psql(
        "INSERT INTO test_part_a VALUES (2, 5);\n\tINSERT INTO test_part_a VALUES (7, 6);"
    )
    node_publisher.wait_for_catchup("sub1")
    assert (
        node_subscriber.safe_psql("SELECT a, b FROM test_part_a ORDER BY a, b")
        == "1|3\n2|5\n6|4\n7|6"
    ), "partitions with different replica identities not replicated correctly"
    node_publisher.safe_psql(
        "CREATE TABLE test_part_b (a int, b int) PARTITION BY LIST (a);\n\n\tCREATE TABLE test_part_b_1 PARTITION OF test_part_b FOR VALUES IN (1,2,3,4,5);\n\tALTER TABLE test_part_b_1 ADD PRIMARY KEY (a);\n\tALTER TABLE test_part_b_1 REPLICA IDENTITY USING INDEX test_part_b_1_pkey;\n\n\tCREATE TABLE test_part_b_2 PARTITION OF test_part_b FOR VALUES IN (6,7,8,9,10);\n\tALTER TABLE test_part_b_2 ADD PRIMARY KEY (b);\n\tALTER TABLE test_part_b_2 REPLICA IDENTITY USING INDEX test_part_b_2_pkey;\n\n\t-- initial data, one row in each partitions\n\tINSERT INTO test_part_b VALUES (1, 1);\n\tINSERT INTO test_part_b VALUES (6, 2);"
    )
    node_subscriber.safe_psql(
        "CREATE TABLE test_part_b (a int, b int) PARTITION BY LIST (a);\n\n\tCREATE TABLE test_part_b_1 PARTITION OF test_part_b FOR VALUES IN (1,2,3,4,5);\n\tALTER TABLE test_part_b_1 ADD PRIMARY KEY (a);\n\tALTER TABLE test_part_b_1 REPLICA IDENTITY USING INDEX test_part_b_1_pkey;\n\n\tCREATE TABLE test_part_b_2 PARTITION OF test_part_b FOR VALUES IN (6,7,8,9,10);\n\tALTER TABLE test_part_b_2 ADD PRIMARY KEY (b);\n\tALTER TABLE test_part_b_2 REPLICA IDENTITY USING INDEX test_part_b_2_pkey;"
    )
    node_publisher.safe_psql(
        "CREATE PUBLICATION pub7 FOR TABLE test_part_b (a, b) WITH (publish_via_partition_root = true);"
    )
    node_subscriber.safe_psql(
        "DROP SUBSCRIPTION sub1;\n\tCREATE SUBSCRIPTION sub1 CONNECTION '"
        + publisher_connstr
        + "' PUBLICATION pub7"
    )
    node_subscriber.wait_for_subscription_sync()
    node_publisher.safe_psql(
        "INSERT INTO test_part_b VALUES (2, 3);\n\tINSERT INTO test_part_b VALUES (7, 4);"
    )
    node_publisher.wait_for_catchup("sub1")
    assert (
        node_subscriber.safe_psql("SELECT * FROM test_part_b ORDER BY a, b")
        == "1|1\n2|3\n6|2\n7|4"
    ), "partitions with different replica identities not replicated correctly"
    node_publisher.safe_psql(
        "CREATE TABLE test_part_c (a int, b int, c int) PARTITION BY LIST (a);\n\n\tCREATE TABLE test_part_c_1 PARTITION OF test_part_c FOR VALUES IN (1,3);\n\tALTER TABLE test_part_c_1 ADD PRIMARY KEY (a);\n\tALTER TABLE test_part_c_1 REPLICA IDENTITY USING INDEX test_part_c_1_pkey;\n\n\tCREATE TABLE test_part_c_2 PARTITION OF test_part_c FOR VALUES IN (2,4);\n\tALTER TABLE test_part_c_2 ADD PRIMARY KEY (b);\n\tALTER TABLE test_part_c_2 REPLICA IDENTITY USING INDEX test_part_c_2_pkey;\n\n\t-- initial data, one row for each partition\n\tINSERT INTO test_part_c VALUES (1, 3, 5);\n\tINSERT INTO test_part_c VALUES (2, 4, 6);"
    )
    node_subscriber.safe_psql(
        "CREATE TABLE test_part_c (a int, b int, c int) PARTITION BY LIST (a);\n\n\tCREATE TABLE test_part_c_1 PARTITION OF test_part_c FOR VALUES IN (1,3);\n\tALTER TABLE test_part_c_1 ADD PRIMARY KEY (a);\n\tALTER TABLE test_part_c_1 REPLICA IDENTITY USING INDEX test_part_c_1_pkey;\n\n\tCREATE TABLE test_part_c_2 PARTITION OF test_part_c FOR VALUES IN (2,4);\n\tALTER TABLE test_part_c_2 ADD PRIMARY KEY (b);\n\tALTER TABLE test_part_c_2 REPLICA IDENTITY USING INDEX test_part_c_2_pkey;"
    )
    node_publisher.safe_psql(
        "CREATE PUBLICATION pub8 FOR TABLE test_part_c WITH (publish_via_partition_root = false);\n\tALTER PUBLICATION pub8 ADD TABLE test_part_c_1 (a,c);\n\tALTER PUBLICATION pub8 ADD TABLE test_part_c_2 (a,b);"
    )
    node_subscriber.safe_psql(
        "DROP SUBSCRIPTION sub1;\n\tCREATE SUBSCRIPTION sub1 CONNECTION '"
        + publisher_connstr
        + "' PUBLICATION pub8;"
    )
    node_subscriber.wait_for_subscription_sync()
    node_publisher.safe_psql(
        "INSERT INTO test_part_c VALUES (3, 7, 8);\n\tINSERT INTO test_part_c VALUES (4, 9, 10);"
    )
    node_publisher.wait_for_catchup("sub1")
    assert (
        node_subscriber.safe_psql("SELECT * FROM test_part_c ORDER BY a, b")
        == "1||5\n2|4|\n3||8\n4|9|"
    ), "partitions with different replica identities not replicated correctly"
    node_publisher.safe_psql(
        "DROP PUBLICATION pub8;\n\tCREATE PUBLICATION pub8 FOR TABLE test_part_c WITH (publish_via_partition_root = false);\n\tALTER PUBLICATION pub8 ADD TABLE test_part_c_1 (a);\n\tALTER PUBLICATION pub8 ADD TABLE test_part_c_2 (a,b);"
    )
    node_subscriber.safe_psql(
        "ALTER SUBSCRIPTION sub1 REFRESH PUBLICATION;\n\tTRUNCATE test_part_c;"
    )
    node_subscriber.wait_for_subscription_sync()
    node_publisher.safe_psql(
        "TRUNCATE test_part_c;\n\tINSERT INTO test_part_c VALUES (1, 3, 5);\n\tINSERT INTO test_part_c VALUES (2, 4, 6);"
    )
    node_publisher.wait_for_catchup("sub1")
    assert (
        node_subscriber.safe_psql("SELECT * FROM test_part_c ORDER BY a, b")
        == "1||\n2|4|"
    ), "partitions with different replica identities not replicated correctly"
    node_publisher.safe_psql(
        "CREATE TABLE test_part_d (a int, b int) PARTITION BY LIST (a);\n\n\tCREATE TABLE test_part_d_1 PARTITION OF test_part_d FOR VALUES IN (1,3);\n\tALTER TABLE test_part_d_1 ADD PRIMARY KEY (a);\n\tALTER TABLE test_part_d_1 REPLICA IDENTITY USING INDEX test_part_d_1_pkey;\n\n\tINSERT INTO test_part_d VALUES (1, 2);"
    )
    node_subscriber.safe_psql(
        "CREATE TABLE test_part_d (a int, b int) PARTITION BY LIST (a);\n\n\tCREATE TABLE test_part_d_1 PARTITION OF test_part_d FOR VALUES IN (1,3);\n\tALTER TABLE test_part_d_1 ADD PRIMARY KEY (a);\n\tALTER TABLE test_part_d_1 REPLICA IDENTITY USING INDEX test_part_d_1_pkey;\n\n\tCREATE TABLE test_part_d_2 PARTITION OF test_part_d FOR VALUES IN (2,4);\n\tALTER TABLE test_part_d_2 ADD PRIMARY KEY (a);\n\tALTER TABLE test_part_d_2 REPLICA IDENTITY USING INDEX test_part_d_2_pkey;"
    )
    node_publisher.safe_psql(
        "CREATE PUBLICATION pub9 FOR TABLE test_part_d (a) WITH (publish_via_partition_root = true);"
    )
    node_subscriber.safe_psql(
        "DROP SUBSCRIPTION sub1;\n\tCREATE SUBSCRIPTION sub1 CONNECTION '"
        + publisher_connstr
        + "' PUBLICATION pub9"
    )
    node_subscriber.wait_for_subscription_sync()
    node_publisher.safe_psql("INSERT INTO test_part_d VALUES (3, 4);")
    node_publisher.wait_for_catchup("sub1")
    assert (
        node_subscriber.safe_psql("SELECT * FROM test_part_d ORDER BY a, b") == "1|\n3|"
    ), "partitions with different replica identities not replicated correctly"
    node_publisher.safe_psql(
        "DROP TABLE tab1, tab2, tab3, tab4, tab5, tab6, tab7,\n\t\t\t   test_part, test_part_a, test_part_b, test_part_c, test_part_d;"
    )
    node_publisher.safe_psql(
        "CREATE TABLE test_mix_2 (a int PRIMARY KEY, b int, c int);\n\tCREATE PUBLICATION pub_mix_3 FOR TABLE test_mix_2 (a, b, c);\n\tCREATE PUBLICATION pub_mix_4 FOR ALL TABLES;\n\n\t-- initial data\n\tINSERT INTO test_mix_2 VALUES (1, 2, 3);"
    )
    node_subscriber.safe_psql(
        "CREATE TABLE test_mix_2 (a int PRIMARY KEY, b int, c int);\n\tDROP SUBSCRIPTION sub1;\n\tCREATE SUBSCRIPTION sub1 CONNECTION '"
        + publisher_connstr
        + "' PUBLICATION pub_mix_3, pub_mix_4;"
    )
    node_subscriber.wait_for_subscription_sync()
    node_publisher.safe_psql("INSERT INTO test_mix_2 VALUES (4, 5, 6);")
    node_publisher.wait_for_catchup("sub1")
    assert (
        node_subscriber.safe_psql("SELECT * FROM test_mix_2") == "1|2|3\n4|5|6"
    ), "all columns should be replicated"
    node_subscriber.safe_psql(
        "DROP SUBSCRIPTION sub1;\n\tCREATE TABLE test_mix_3 (a int PRIMARY KEY, b int, c int);"
    )
    node_publisher.safe_psql(
        "DROP TABLE test_mix_2;\n\tCREATE TABLE test_mix_3 (a int PRIMARY KEY, b int, c int);\n\tCREATE PUBLICATION pub_mix_5 FOR TABLE test_mix_3 (a, b, c);\n\tCREATE PUBLICATION pub_mix_6 FOR TABLES IN SCHEMA public;\n\n\t-- initial data\n\tINSERT INTO test_mix_3 VALUES (1, 2, 3);"
    )
    node_subscriber.safe_psql(
        "CREATE SUBSCRIPTION sub1 CONNECTION '"
        + publisher_connstr
        + "' PUBLICATION pub_mix_5, pub_mix_6;"
    )
    node_subscriber.wait_for_subscription_sync()
    node_publisher.safe_psql("INSERT INTO test_mix_3 VALUES (4, 5, 6);")
    node_publisher.wait_for_catchup("sub1")
    assert (
        node_subscriber.safe_psql("SELECT * FROM test_mix_3") == "1|2|3\n4|5|6"
    ), "all columns should be replicated"
    node_subscriber.safe_psql(
        "DROP SUBSCRIPTION sub1;\n\n\tCREATE TABLE test_root (a int PRIMARY KEY, b int, c int) PARTITION BY RANGE (a);\n\tCREATE TABLE test_root_1 PARTITION OF test_root FOR VALUES FROM (1) TO (10);\n\tCREATE TABLE test_root_2 PARTITION OF test_root FOR VALUES FROM (10) TO (20);"
    )
    node_publisher.safe_psql(
        "CREATE TABLE test_root (a int PRIMARY KEY, b int, c int) PARTITION BY RANGE (a);\n\tCREATE TABLE test_root_1 PARTITION OF test_root FOR VALUES FROM (1) TO (10);\n\tCREATE TABLE test_root_2 PARTITION OF test_root FOR VALUES FROM (10) TO (20);\n\n\tCREATE PUBLICATION pub_test_root FOR TABLE test_root (a) WITH (publish_via_partition_root = true);\n\tCREATE PUBLICATION pub_test_root_1 FOR TABLE test_root_1 (a, b);\n\n\t-- initial data\n\tINSERT INTO test_root VALUES (1, 2, 3);\n\tINSERT INTO test_root VALUES (10, 20, 30);"
    )
    node_subscriber.safe_psql(
        "CREATE SUBSCRIPTION sub1 CONNECTION '"
        + publisher_connstr
        + "' PUBLICATION pub_test_root, pub_test_root_1;"
    )
    node_subscriber.wait_for_subscription_sync()
    node_publisher.safe_psql(
        "INSERT INTO test_root VALUES (2, 3, 4);\n\tINSERT INTO test_root VALUES (11, 21, 31);"
    )
    node_publisher.wait_for_catchup("sub1")
    assert (
        node_subscriber.safe_psql("SELECT * FROM test_root ORDER BY a, b, c")
        == "1||\n2||\n10||\n11||"
    ), "publication via partition root applies column list"
    node_publisher.safe_psql(
        "DROP PUBLICATION pub1, pub2, pub3, pub4, pub5, pub6, pub7, pub8;\n\n\tCREATE SCHEMA s1;\n\tCREATE TABLE s1.t (a int, b int, c int) PARTITION BY RANGE (a);\n\tCREATE TABLE t_1 PARTITION OF s1.t FOR VALUES FROM (1) TO (10);\n\n\tCREATE PUBLICATION pub1 FOR TABLES IN SCHEMA s1;\n\tCREATE PUBLICATION pub2 FOR TABLE t_1(a, b, c);\n\n\t-- initial data\n\tINSERT INTO s1.t VALUES (1, 2, 3);"
    )
    node_subscriber.safe_psql(
        "CREATE SCHEMA s1;\n\tCREATE TABLE s1.t (a int, b int, c int) PARTITION BY RANGE (a);\n\tCREATE TABLE t_1 PARTITION OF s1.t FOR VALUES FROM (1) TO (10);\n\n\tDROP SUBSCRIPTION sub1;\n\tCREATE SUBSCRIPTION sub1 CONNECTION '"
        + publisher_connstr
        + "' PUBLICATION pub1, pub2;"
    )
    node_subscriber.wait_for_subscription_sync()
    node_publisher.safe_psql("INSERT INTO s1.t VALUES (4, 5, 6);")
    node_publisher.wait_for_catchup("sub1")
    assert (
        node_subscriber.safe_psql("SELECT * FROM s1.t ORDER BY a") == "1|2|3\n4|5|6"
    ), "two publications, publishing the same relation"
    node_subscriber.safe_psql(
        "TRUNCATE s1.t;\n\n\tALTER SUBSCRIPTION sub1 SET PUBLICATION pub2, pub1;"
    )
    node_subscriber.wait_for_subscription_sync()
    node_publisher.safe_psql("INSERT INTO s1.t VALUES (7, 8, 9);")
    node_publisher.wait_for_catchup("sub1")
    assert (
        node_subscriber.safe_psql("SELECT * FROM s1.t ORDER BY a") == "7|8|9"
    ), "two publications, publishing the same relation"
    node_publisher.safe_psql(
        "DROP SCHEMA s1 CASCADE;\n\tCREATE TABLE t (a int, b int, c int) PARTITION BY RANGE (a);\n\tCREATE TABLE t_1 PARTITION OF t FOR VALUES FROM (1) TO (10)\n\t\t   PARTITION BY RANGE (a);\n\tCREATE TABLE t_2 PARTITION OF t_1 FOR VALUES FROM (1) TO (10);\n\n\tCREATE PUBLICATION pub3 FOR TABLE t_1 (a), t_2\n\t  WITH (PUBLISH_VIA_PARTITION_ROOT);\n\n\t-- initial data\n\tINSERT INTO t VALUES (1, 2, 3);"
    )
    node_subscriber.safe_psql(
        "DROP SCHEMA s1 CASCADE;\n\tCREATE TABLE t (a int, b int, c int) PARTITION BY RANGE (a);\n\tCREATE TABLE t_1 PARTITION OF t FOR VALUES FROM (1) TO (10)\n\t\t   PARTITION BY RANGE (a);\n\tCREATE TABLE t_2 PARTITION OF t_1 FOR VALUES FROM (1) TO (10);\n\n\tDROP SUBSCRIPTION sub1;\n\tCREATE SUBSCRIPTION sub1 CONNECTION '"
        + publisher_connstr
        + "' PUBLICATION pub3;"
    )
    node_subscriber.wait_for_subscription_sync()
    node_publisher.safe_psql("INSERT INTO t VALUES (4, 5, 6);")
    node_publisher.wait_for_catchup("sub1")
    assert (
        node_subscriber.safe_psql("SELECT * FROM t ORDER BY a, b, c") == "1||\n4||"
    ), "publication containing both parent and child relation"
    node_publisher.safe_psql(
        "DROP TABLE t;\n\tCREATE TABLE t (a int, b int, c int) PARTITION BY RANGE (a);\n\tCREATE TABLE t_1 PARTITION OF t FOR VALUES FROM (1) TO (10)\n\t\t   PARTITION BY RANGE (a);\n\tCREATE TABLE t_2 PARTITION OF t_1 FOR VALUES FROM (1) TO (10);\n\n\tCREATE PUBLICATION pub4 FOR TABLE t_1 (a), t_2 (b)\n\t  WITH (PUBLISH_VIA_PARTITION_ROOT);\n\n\t-- initial data\n\tINSERT INTO t VALUES (1, 2, 3);"
    )
    node_subscriber.safe_psql(
        "DROP TABLE t;\n\tCREATE TABLE t (a int, b int, c int) PARTITION BY RANGE (a);\n\tCREATE TABLE t_1 PARTITION OF t FOR VALUES FROM (1) TO (10)\n\t\t   PARTITION BY RANGE (a);\n\tCREATE TABLE t_2 PARTITION OF t_1 FOR VALUES FROM (1) TO (10);\n\n\tDROP SUBSCRIPTION sub1;\n\tCREATE SUBSCRIPTION sub1 CONNECTION '"
        + publisher_connstr
        + "' PUBLICATION pub4;"
    )
    node_subscriber.wait_for_subscription_sync()
    node_publisher.safe_psql("INSERT INTO t VALUES (4, 5, 6);")
    node_publisher.wait_for_catchup("sub1")
    assert (
        node_subscriber.safe_psql("SELECT * FROM t ORDER BY a, b, c") == "1||\n4||"
    ), "publication containing both parent and child relation"
    node_publisher.safe_psql(
        "CREATE TABLE test_oldtuple_col (a int PRIMARY KEY, b int, c int);\n\tCREATE PUBLICATION pub_check_oldtuple FOR TABLE test_oldtuple_col (a, b);\n\tINSERT INTO test_oldtuple_col VALUES(1, 2, 3);\n\tSELECT * FROM pg_create_logical_replication_slot('test_slot', 'pgoutput');\n\tUPDATE test_oldtuple_col SET a = 2;\n\tDELETE FROM test_oldtuple_col;"
    )
    result = node_publisher.safe_psql(
        "SELECT substr(data, 7, 2) = int2send(2::smallint)\n\t\tFROM pg_logical_slot_peek_binary_changes('test_slot', NULL, NULL,\n\t\t\t'proto_version', '1',\n\t\t\t'publication_names', 'pub_check_oldtuple')\n\t\tWHERE get_byte(data, 0) = 85 OR get_byte(data, 0) = 68"
    )
    assert result == "t\nt", "check the number of columns in the old tuple"
    node_publisher.safe_psql(
        "CREATE TABLE test_mix_4 (a int PRIMARY KEY, b int, c int, d int GENERATED ALWAYS AS (a + 1) STORED, e int GENERATED ALWAYS AS (a + 2) VIRTUAL);\n\tALTER TABLE test_mix_4 DROP COLUMN c;\n\n\tCREATE PUBLICATION pub_mix_7 FOR TABLE test_mix_4 (a, b);\n\tCREATE PUBLICATION pub_mix_8 FOR TABLE test_mix_4;\n\n\t-- initial data\n\tINSERT INTO test_mix_4 VALUES (1, 2);"
    )
    node_subscriber.safe_psql(
        "DROP SUBSCRIPTION sub1;\n\tCREATE TABLE test_mix_4 (a int PRIMARY KEY, b int, c int, d int);"
    )
    node_subscriber.safe_psql(
        "CREATE SUBSCRIPTION sub1 CONNECTION '"
        + publisher_connstr
        + "' PUBLICATION pub_mix_7, pub_mix_8;"
    )
    node_subscriber.wait_for_subscription_sync()
    assert (
        node_subscriber.safe_psql("SELECT * FROM test_mix_4 ORDER BY a") == "1|2||"
    ), "initial synchronization with multiple publications with the same column list"
    node_publisher.safe_psql("INSERT INTO test_mix_4 VALUES (3, 4);")
    node_publisher.wait_for_catchup("sub1")
    assert (
        node_subscriber.safe_psql("SELECT * FROM test_mix_4 ORDER BY a")
        == "1|2||\n3|4||"
    ), "replication with multiple publications with the same column list"
    node_publisher.safe_psql(
        "CREATE TABLE test_mix_1 (a int PRIMARY KEY, b int, c int);\n\tCREATE PUBLICATION pub_mix_1 FOR TABLE test_mix_1 (a, b);\n\tCREATE PUBLICATION pub_mix_2 FOR TABLE test_mix_1 (a, c);"
    )
    node_subscriber.safe_psql(
        "DROP SUBSCRIPTION sub1;\n\tCREATE TABLE test_mix_1 (a int PRIMARY KEY, b int, c int);"
    )
    result = node_subscriber.psql_capture(
        "CREATE SUBSCRIPTION sub1 CONNECTION '"
        + publisher_connstr
        + "' PUBLICATION pub_mix_1, pub_mix_2;"
    )
    assert re.search(
        r'cannot use different column lists for table "public.test_mix_1" in different publications',
        result.stderr,
    ), "different column lists detected"
    node_publisher.safe_psql("ALTER PUBLICATION pub_mix_1 SET TABLE test_mix_1 (a, c);")
    node_subscriber.safe_psql(
        "CREATE SUBSCRIPTION sub1 CONNECTION '"
        + publisher_connstr
        + "' PUBLICATION pub_mix_1, pub_mix_2;"
    )
    node_publisher.wait_for_catchup("sub1")
    node_publisher.safe_psql(
        "ALTER PUBLICATION pub_mix_1 SET TABLE test_mix_1 (a, b);\n\tINSERT INTO test_mix_1 VALUES(1, 1, 1);"
    )
    node_publisher.wait_for_log(
        r'cannot use different column lists for table "public.test_mix_1" in different publications'
    )
    node_subscriber.stop("fast")
    node_publisher.stop("fast")

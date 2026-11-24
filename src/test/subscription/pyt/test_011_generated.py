# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/subscription/t/011_generated.pl.

Generated columns in logical replication, including publish_generated_columns,
column lists, and replication into generated subscriber columns.
"""

_TRIGGER = """
CREATE FUNCTION tab1_trigger_func() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  NEW.d := NEW.a + 10;
  RETURN NEW;
END $$;

CREATE TRIGGER test1 BEFORE INSERT OR UPDATE ON tab1
  FOR EACH ROW
  EXECUTE PROCEDURE tab1_trigger_func();

ALTER TABLE tab1 ENABLE REPLICA TRIGGER test1;
"""


def test_generated(create_pg):
    """Generated-column replication across the documented scenarios."""
    publisher = create_pg("publisher", allows_streaming="logical")
    subscriber = create_pg("subscriber")
    connstr = publisher.connstr() + " dbname=postgres"

    _test_basic(publisher, subscriber, connstr)
    _test_gen_to_nogen(publisher, subscriber, connstr)
    _test_column_lists(publisher, subscriber, connstr)
    _test_into_generated(publisher, subscriber, connstr)


def _test_basic(publisher, subscriber, connstr):
    publisher.safe_psql(
        "CREATE TABLE tab1 (a int PRIMARY KEY, "
        "b int GENERATED ALWAYS AS (a * 2) STORED, "
        "c int GENERATED ALWAYS AS (a * 3) VIRTUAL)"
    )
    subscriber.safe_psql(
        "CREATE TABLE tab1 (a int PRIMARY KEY, "
        "b int GENERATED ALWAYS AS (a * 22) STORED, "
        "c int GENERATED ALWAYS AS (a * 33) VIRTUAL, d int)"
    )
    publisher.safe_psql("INSERT INTO tab1 (a) VALUES (1), (2), (3)")
    publisher.safe_psql("CREATE PUBLICATION pub1 FOR ALL TABLES")
    subscriber.safe_psql(
        "CREATE SUBSCRIPTION sub1 CONNECTION '{}' PUBLICATION pub1".format(connstr)
    )
    subscriber.wait_for_subscription_sync()

    assert subscriber.safe_psql("SELECT a, b, c FROM tab1") == (
        "1|22|33\n2|44|66\n3|66|99"
    ), "generated columns initial sync"

    publisher.safe_psql("INSERT INTO tab1 VALUES (4), (5)")
    publisher.safe_psql("UPDATE tab1 SET a = 6 WHERE a = 5")
    publisher.wait_for_catchup("sub1")
    assert subscriber.safe_psql("SELECT * FROM tab1") == (
        "1|22|33|\n2|44|66|\n3|66|99|\n4|88|132|\n6|132|198|"
    ), "generated columns replicated"

    subscriber.safe_psql(_TRIGGER)
    publisher.safe_psql("INSERT INTO tab1 VALUES (7), (8)")
    publisher.safe_psql("UPDATE tab1 SET a = 9 WHERE a = 7")
    publisher.wait_for_catchup("sub1")
    assert subscriber.safe_psql("SELECT * FROM tab1 ORDER BY 1") == (
        "1|22|33|\n2|44|66|\n3|66|99|\n4|88|132|\n6|132|198|\n"
        "8|176|264|18\n9|198|297|19"
    ), "generated columns replicated with trigger"

    subscriber.safe_psql("DROP SUBSCRIPTION sub1")
    publisher.safe_psql("DROP PUBLICATION pub1")


def _test_gen_to_nogen(publisher, subscriber, connstr):
    # publish_generated_columns 'none' (pub1/sub1, postgres) vs 'stored'
    # (pub2/sub2, test_pgc_true database).
    subscriber.safe_psql("CREATE DATABASE test_pgc_true")
    publisher.safe_psql(
        "CREATE TABLE tab_gen_to_nogen "
        "(a int, b int GENERATED ALWAYS AS (a * 2) STORED);"
        " INSERT INTO tab_gen_to_nogen (a) VALUES (1), (2), (3);"
        " CREATE PUBLICATION regress_pub1_gen_to_nogen FOR TABLE tab_gen_to_nogen"
        " WITH (publish_generated_columns = none);"
        " CREATE PUBLICATION regress_pub2_gen_to_nogen FOR TABLE tab_gen_to_nogen"
        " WITH (publish_generated_columns = stored);"
    )
    subscriber.safe_psql(
        "CREATE TABLE tab_gen_to_nogen (a int, b int);"
        " CREATE SUBSCRIPTION regress_sub1_gen_to_nogen CONNECTION '{}'"
        " PUBLICATION regress_pub1_gen_to_nogen WITH (copy_data = true);".format(
            connstr
        )
    )
    subscriber.safe_psql(
        "CREATE TABLE tab_gen_to_nogen (a int, b int);"
        " CREATE SUBSCRIPTION regress_sub2_gen_to_nogen CONNECTION '{}'"
        " PUBLICATION regress_pub2_gen_to_nogen WITH (copy_data = true);".format(
            connstr
        ),
        dbname="test_pgc_true",
    )
    subscriber.wait_for_subscription_sync(
        publisher, "regress_sub1_gen_to_nogen", "postgres"
    )
    subscriber.wait_for_subscription_sync(
        publisher, "regress_sub2_gen_to_nogen", "test_pgc_true"
    )

    assert (
        subscriber.safe_psql("SELECT a, b FROM tab_gen_to_nogen ORDER BY a")
        == "1|\n2|\n3|"
    ), "initial sync, publish_generated_columns=none"
    assert (
        subscriber.safe_psql(
            "SELECT a, b FROM tab_gen_to_nogen ORDER BY a", dbname="test_pgc_true"
        )
        == "1|2\n2|4\n3|6"
    ), "initial sync, publish_generated_columns=stored"

    publisher.safe_psql("INSERT INTO tab_gen_to_nogen VALUES (4), (5)")
    publisher.wait_for_catchup("regress_sub1_gen_to_nogen")
    assert (
        subscriber.safe_psql("SELECT a, b FROM tab_gen_to_nogen ORDER BY a")
        == "1|\n2|\n3|\n4|\n5|"
    ), "incremental, publish_generated_columns=none"
    publisher.wait_for_catchup("regress_sub2_gen_to_nogen")
    assert (
        subscriber.safe_psql(
            "SELECT a, b FROM tab_gen_to_nogen ORDER BY a", dbname="test_pgc_true"
        )
        == "1|2\n2|4\n3|6\n4|8\n5|10"
    ), "incremental, publish_generated_columns=stored"

    subscriber.safe_psql("DROP SUBSCRIPTION regress_sub1_gen_to_nogen")
    subscriber.safe_psql(
        "DROP SUBSCRIPTION regress_sub2_gen_to_nogen", dbname="test_pgc_true"
    )
    publisher.safe_psql(
        "DROP PUBLICATION regress_pub1_gen_to_nogen;"
        " DROP PUBLICATION regress_pub2_gen_to_nogen;"
    )
    subscriber.safe_psql("DROP table tab_gen_to_nogen", dbname="test_pgc_true")
    subscriber.safe_psql("DROP DATABASE test_pgc_true")


def _test_column_lists(publisher, subscriber, connstr):
    # Column lists take precedence over publish_generated_columns.
    publisher.safe_psql(
        "CREATE TABLE tab2 (a int, gen1 int GENERATED ALWAYS AS (a * 2) STORED);"
        " INSERT INTO tab2 (a) VALUES (1), (2);"
        " CREATE PUBLICATION pub1 FOR table tab2(gen1)"
        " WITH (publish_generated_columns=none);"
    )
    subscriber.safe_psql(
        "CREATE TABLE tab2 (a int, gen1 int);"
        " CREATE SUBSCRIPTION sub1 CONNECTION '{}' PUBLICATION pub1"
        " WITH (copy_data = true);".format(connstr)
    )
    subscriber.wait_for_subscription_sync(publisher, "sub1")
    assert (
        subscriber.safe_psql("SELECT * FROM tab2 ORDER BY gen1") == "|2\n|4"
    ), "tab2 initial sync, publish_generated_columns=none"
    publisher.safe_psql("INSERT INTO tab2 VALUES (3), (4)")
    publisher.wait_for_catchup("sub1")
    assert (
        subscriber.safe_psql("SELECT * FROM tab2 ORDER BY gen1") == "|2\n|4\n|6\n|8"
    ), "tab2 incremental, publish_generated_columns=none"
    subscriber.safe_psql("DROP SUBSCRIPTION sub1")
    publisher.safe_psql("DROP PUBLICATION pub1")

    # Only column-list columns are published even with 'stored'.
    publisher.safe_psql(
        "CREATE TABLE tab3 (a int, gen1 int GENERATED ALWAYS AS (a * 2) STORED,"
        " gen2 int GENERATED ALWAYS AS (a * 2) STORED);"
        " INSERT INTO tab3 (a) VALUES (1), (2);"
        " CREATE PUBLICATION pub1 FOR table tab3(gen1)"
        " WITH (publish_generated_columns=stored);"
    )
    subscriber.safe_psql(
        "CREATE TABLE tab3 (a int, gen1 int, gen2 int);"
        " CREATE SUBSCRIPTION sub1 CONNECTION '{}' PUBLICATION pub1"
        " WITH (copy_data = true);".format(connstr)
    )
    subscriber.wait_for_subscription_sync(publisher, "sub1")
    assert (
        subscriber.safe_psql("SELECT * FROM tab3 ORDER BY gen1") == "|2|\n|4|"
    ), "tab3 initial sync, publish_generated_columns=stored"
    publisher.safe_psql("INSERT INTO tab3 VALUES (3), (4)")
    publisher.wait_for_catchup("sub1")
    assert subscriber.safe_psql("SELECT * FROM tab3 ORDER BY gen1") == (
        "|2|\n|4|\n|6|\n|8|"
    ), "tab3 incremental, publish_generated_columns=stored"
    subscriber.safe_psql("DROP SUBSCRIPTION sub1")
    publisher.safe_psql("DROP PUBLICATION pub1")


def _test_into_generated(publisher, subscriber, connstr):
    # Replicating into a generated subscriber column is an error.
    publisher.safe_psql(
        "CREATE TABLE t1(c1 int, c2 int, c3 int GENERATED ALWAYS AS (c1 * 2) STORED);"
        " CREATE PUBLICATION pub1 for table t1(c1, c2, c3);"
        " INSERT INTO t1 VALUES (1);"
    )
    subscriber.safe_psql(
        "CREATE TABLE t1(c1 int, c2 int GENERATED ALWAYS AS (c1 + 2) STORED,"
        " c3 int GENERATED ALWAYS AS (c1 + 2) STORED);"
        " CREATE SUBSCRIPTION sub1 CONNECTION '{}' PUBLICATION pub1;".format(connstr)
    )
    offset = subscriber.current_log_position()
    subscriber.wait_for_log(
        r'ERROR: ( [A-Z0-9]+:)? logical replication target relation "public.t1" '
        r'has incompatible generated columns: "c2", "c3"',
        offset,
    )
    subscriber.safe_psql("DROP SUBSCRIPTION sub1")
    publisher.safe_psql("DROP PUBLICATION pub1")

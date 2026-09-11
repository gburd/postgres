# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/subscription/t/007_ddl.pl.

Logical replication DDL behavior: disable+drop in one transaction, non-existent
publication warnings, and ALTER PUBLICATION RENAME during replication.
"""

import re


def _test_swap(publisher, subscriber, table_name, pubname, appname):
    """Confirm replication before, and not after, swapping publication names."""
    publisher.safe_psql("INSERT INTO {} VALUES (1);".format(table_name))
    publisher.wait_for_catchup(appname)
    assert (
        subscriber.safe_psql("SELECT a FROM {}".format(table_name)) == "1"
    ), "check replication worked well before renaming a publication"

    # Swap the names: pubname <-> pub_empty.
    publisher.safe_psql(
        "ALTER PUBLICATION {0} RENAME TO tap_pub_tmp;"
        " ALTER PUBLICATION pub_empty RENAME TO {0};"
        " ALTER PUBLICATION tap_pub_tmp RENAME TO pub_empty;".format(pubname)
    )

    publisher.safe_psql("INSERT INTO {} VALUES (2);".format(table_name))
    publisher.wait_for_catchup(appname)

    # The second tuple is not replicated: pubname no longer has the relation.
    assert (
        subscriber.safe_psql("SELECT a FROM {} ORDER BY a".format(table_name)) == "1"
    ), "check the tuple inserted after the RENAME was not replicated"

    # Restore the names (this helper may be called several times).
    publisher.safe_psql(
        "ALTER PUBLICATION {0} RENAME TO tap_pub_tmp;"
        " ALTER PUBLICATION pub_empty RENAME TO {0};"
        " ALTER PUBLICATION tap_pub_tmp RENAME TO pub_empty;".format(pubname)
    )


def test_ddl(create_pg):
    """DDL behavior: same-txn disable/drop, missing-pub warnings, RENAME."""
    publisher = create_pg("publisher", allows_streaming="logical")
    subscriber = create_pg("subscriber")
    connstr = publisher.connstr() + " dbname=postgres"

    ddl = "CREATE TABLE test1 (a int, b text);"
    publisher.safe_psql(ddl)
    subscriber.safe_psql(ddl)

    publisher.safe_psql("CREATE PUBLICATION mypub FOR ALL TABLES;")
    subscriber.safe_psql(
        "CREATE SUBSCRIPTION mysub CONNECTION '{}' PUBLICATION mypub;".format(connstr)
    )
    publisher.wait_for_catchup("mysub")

    # Disable and drop in one transaction must not hang.
    subscriber.safe_psql(
        "BEGIN;\n"
        "ALTER SUBSCRIPTION mysub DISABLE;\n"
        "ALTER SUBSCRIPTION mysub SET (slot_name = NONE);\n"
        "DROP SUBSCRIPTION mysub;\n"
        "COMMIT;"
    )

    # One of the specified publications exists -> warning, succeeds.
    result = subscriber.psql_capture(
        "CREATE SUBSCRIPTION mysub1 CONNECTION '{}' "
        "PUBLICATION mypub, non_existent_pub".format(connstr)
    )
    assert re.search(
        r'WARNING:  publication "non_existent_pub" does not exist on the publisher',
        result.stderr,
    ), "Create subscription throws warning for non-existent publication"
    subscriber.wait_for_subscription_sync(publisher, "mysub1")

    result = subscriber.psql_capture(
        "ALTER SUBSCRIPTION mysub1 ADD PUBLICATION "
        "non_existent_pub1, non_existent_pub2"
    )
    assert re.search(
        r'WARNING:  publications "non_existent_pub1", "non_existent_pub2" '
        r"do not exist on the publisher",
        result.stderr,
    ), "Alter subscription add publication warns for non-existent publications"

    result = subscriber.psql_capture(
        "ALTER SUBSCRIPTION mysub1 SET PUBLICATION non_existent_pub"
    )
    assert re.search(
        r'WARNING:  publication "non_existent_pub" does not exist on the publisher',
        result.stderr,
    ), "Alter subscription set publication warns for non-existent publication"

    publisher.safe_psql(
        "DROP PUBLICATION mypub;\nSELECT pg_drop_replication_slot('mysub');"
    )
    subscriber.safe_psql("DROP SUBSCRIPTION mysub1")

    # ALTER PUBLICATION RENAME during replication.
    ddl = "CREATE TABLE test2 (a int, b text);"
    publisher.safe_psql(ddl)
    subscriber.safe_psql(ddl)

    publisher.safe_psql(
        "CREATE PUBLICATION pub_empty;\n"
        "CREATE PUBLICATION pub_for_tab FOR TABLE test1;\n"
        "CREATE PUBLICATION pub_for_all_tables FOR ALL TABLES;"
    )
    subscriber.safe_psql(
        "CREATE SUBSCRIPTION tap_sub CONNECTION '{}' "
        "PUBLICATION pub_for_tab".format(connstr)
    )
    subscriber.wait_for_subscription_sync(publisher, "tap_sub")

    _test_swap(publisher, subscriber, "test1", "pub_for_tab", "tap_sub")

    subscriber.safe_psql(
        "ALTER SUBSCRIPTION tap_sub SET PUBLICATION pub_for_all_tables;"
    )
    subscriber.wait_for_subscription_sync(publisher, "tap_sub")

    _test_swap(publisher, subscriber, "test2", "pub_for_all_tables", "tap_sub")

    publisher.safe_psql(
        "DROP PUBLICATION pub_empty, pub_for_tab, pub_for_all_tables;\n"
        "DROP TABLE test1, test2;"
    )
    subscriber.safe_psql("DROP SUBSCRIPTION tap_sub;\nDROP TABLE test1, test2;")

    subscriber.stop()
    publisher.stop()

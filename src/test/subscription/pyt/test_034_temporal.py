# Copyright (c) 2022-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long,too-many-statements
"""Port of src/test/subscription/t/034_temporal.pl.

Logical replication of temporal tables (WITHOUT OVERLAPS primary/unique keys
over int4range + daterange columns): initial sync and incremental changes, and
the replica-identity error messages for UPDATE/DELETE on tables lacking a
usable replica identity. Generated from the Perl original via
.agent/gen_golden.py with the two helper subroutines inlined as nested
functions.
"""


def test_034_temporal(create_pg):
    """Logical replication of temporal tables and replica-identity errors."""
    node_publisher = create_pg("publisher", allows_streaming="logical", start=False)
    node_publisher.start()
    node_subscriber = create_pg("subscriber", start=False)
    node_subscriber.start()
    publisher_connstr = node_publisher.connstr() + " dbname=postgres"

    def create_tables():
        for node in (node_publisher, node_subscriber):
            node.safe_psql(
                "CREATE TABLE temporal_no_key (id int4range, valid_at daterange, a text)"
            )
            node.safe_psql(
                "CREATE TABLE temporal_pk (id int4range, valid_at daterange, a text, PRIMARY KEY (id, valid_at WITHOUT OVERLAPS))"
            )
            node.safe_psql(
                "CREATE TABLE temporal_unique (id int4range, valid_at daterange, a text, UNIQUE (id, valid_at WITHOUT OVERLAPS))"
            )

    def drop_everything():
        node_publisher.safe_psql("DROP TABLE IF EXISTS temporal_no_key")
        node_publisher.safe_psql("DROP TABLE IF EXISTS temporal_pk")
        node_publisher.safe_psql("DROP TABLE IF EXISTS temporal_unique")
        node_publisher.safe_psql("DROP PUBLICATION pub1")
        node_subscriber.safe_psql("DROP TABLE IF EXISTS temporal_no_key")
        node_subscriber.safe_psql("DROP TABLE IF EXISTS temporal_pk")
        node_subscriber.safe_psql("DROP TABLE IF EXISTS temporal_unique")
        node_subscriber.safe_psql("DROP SUBSCRIPTION sub1")

    create_tables()
    node_publisher.safe_psql(
        "INSERT INTO temporal_no_key (id, valid_at, a)\n   VALUES ('[1,2)', '[2000-01-01,2010-01-01)', 'a')"
    )
    node_publisher.safe_psql(
        "INSERT INTO temporal_pk (id, valid_at, a)\n   VALUES ('[1,2)', '[2000-01-01,2010-01-01)', 'a')"
    )
    node_publisher.safe_psql(
        "INSERT INTO temporal_unique (id, valid_at, a)\n   VALUES ('[1,2)', '[2000-01-01,2010-01-01)', 'a')"
    )
    node_publisher.safe_psql("CREATE PUBLICATION pub1 FOR ALL TABLES")
    node_subscriber.safe_psql(
        "CREATE SUBSCRIPTION sub1 CONNECTION '"
        + publisher_connstr
        + "' PUBLICATION pub1"
    )
    node_subscriber.wait_for_subscription_sync()
    result = node_subscriber.safe_psql(
        "SELECT * FROM temporal_no_key ORDER BY id, valid_at"
    )
    assert result == "[1,2)|[2000-01-01,2010-01-01)|a", "synced temporal_no_key DEFAULT"
    result = node_subscriber.safe_psql(
        "SELECT * FROM temporal_pk ORDER BY id, valid_at"
    )
    assert result == "[1,2)|[2000-01-01,2010-01-01)|a", "synced temporal_pk DEFAULT"
    result = node_subscriber.safe_psql(
        "SELECT * FROM temporal_unique ORDER BY id, valid_at"
    )
    assert result == "[1,2)|[2000-01-01,2010-01-01)|a", "synced temporal_unique DEFAULT"
    node_publisher.safe_psql(
        "INSERT INTO temporal_no_key (id, valid_at, a)\n   VALUES ('[2,3)', '[2000-01-01,2010-01-01)', 'a'),\n          ('[3,4)', '[2000-01-01,2010-01-01)', 'a'),\n          ('[4,5)', '[2000-01-01,2010-01-01)', 'a')"
    )
    result = node_publisher.psql_capture(
        "UPDATE temporal_no_key SET a = 'b' WHERE id = '[2,3)'"
    )
    assert (
        result.stderr
        == 'psql:<stdin>:1: ERROR:  cannot update table "temporal_no_key" because it does not have a replica identity and publishes updates\nHINT:  To enable updating the table, set REPLICA IDENTITY using ALTER TABLE.'
    ), "can't UPDATE temporal_no_key DEFAULT"
    result = node_publisher.psql_capture(
        "DELETE FROM temporal_no_key WHERE id = '[3,4)'"
    )
    assert (
        result.stderr
        == 'psql:<stdin>:1: ERROR:  cannot delete from table "temporal_no_key" because it does not have a replica identity and publishes deletes\nHINT:  To enable deleting from the table, set REPLICA IDENTITY using ALTER TABLE.'
    ), "can't DELETE temporal_no_key DEFAULT"
    result = node_publisher.psql_capture(
        "DELETE FROM temporal_no_key FOR PORTION OF valid_at FROM '2002-01-01' TO '2003-01-01' WHERE id = '[2,3)'"
    )
    assert (
        result.stderr
        == 'psql:<stdin>:1: ERROR:  cannot delete from table "temporal_no_key" because it does not have a replica identity and publishes deletes\nHINT:  To enable deleting from the table, set REPLICA IDENTITY using ALTER TABLE.'
    ), "can't DELETE FOR PORTION OF temporal_no_key DEFAULT"
    node_publisher.wait_for_catchup("sub1")
    result = node_subscriber.safe_psql(
        "SELECT * FROM temporal_no_key ORDER BY id, valid_at"
    )
    assert (
        result
        == "[1,2)|[2000-01-01,2010-01-01)|a\n[2,3)|[2000-01-01,2010-01-01)|a\n[3,4)|[2000-01-01,2010-01-01)|a\n[4,5)|[2000-01-01,2010-01-01)|a"
    ), "replicated temporal_no_key DEFAULT"
    node_publisher.safe_psql(
        "INSERT INTO temporal_pk (id, valid_at, a)\n   VALUES ('[2,3)', '[2000-01-01,2010-01-01)', 'a'),\n          ('[3,4)', '[2000-01-01,2010-01-01)', 'a'),\n          ('[4,5)', '[2000-01-01,2010-01-01)', 'a')"
    )
    node_publisher.safe_psql("UPDATE temporal_pk SET a = 'b' WHERE id = '[2,3)'")
    node_publisher.safe_psql(
        "UPDATE temporal_pk FOR PORTION OF valid_at FROM '2001-01-01' TO '2002-01-01' SET a = 'c' WHERE id = '[2,3)'"
    )
    node_publisher.safe_psql("DELETE FROM temporal_pk WHERE id = '[3,4)'")
    node_publisher.safe_psql(
        "DELETE FROM temporal_pk FOR PORTION OF valid_at FROM '2002-01-01' TO '2003-01-01' WHERE id = '[2,3)'"
    )
    node_publisher.wait_for_catchup("sub1")
    result = node_subscriber.safe_psql(
        "SELECT * FROM temporal_pk ORDER BY id, valid_at"
    )
    assert (
        result
        == "[1,2)|[2000-01-01,2010-01-01)|a\n[2,3)|[2000-01-01,2001-01-01)|b\n[2,3)|[2001-01-01,2002-01-01)|c\n[2,3)|[2003-01-01,2010-01-01)|b\n[4,5)|[2000-01-01,2010-01-01)|a"
    ), "replicated temporal_pk DEFAULT"
    node_publisher.safe_psql(
        "INSERT INTO temporal_unique (id, valid_at, a)\n   VALUES ('[2,3)', '[2000-01-01,2010-01-01)', 'a'),\n          ('[3,4)', '[2000-01-01,2010-01-01)', 'a'),\n          ('[4,5)', '[2000-01-01,2010-01-01)', 'a')"
    )
    result = node_publisher.psql_capture(
        "UPDATE temporal_unique SET a = 'b' WHERE id = '[2,3)'"
    )
    assert (
        result.stderr
        == 'psql:<stdin>:1: ERROR:  cannot update table "temporal_unique" because it does not have a replica identity and publishes updates\nHINT:  To enable updating the table, set REPLICA IDENTITY using ALTER TABLE.'
    ), "can't UPDATE temporal_unique DEFAULT"
    result = node_publisher.psql_capture(
        "DELETE FROM temporal_unique WHERE id = '[3,4)'"
    )
    assert (
        result.stderr
        == 'psql:<stdin>:1: ERROR:  cannot delete from table "temporal_unique" because it does not have a replica identity and publishes deletes\nHINT:  To enable deleting from the table, set REPLICA IDENTITY using ALTER TABLE.'
    ), "can't DELETE temporal_unique DEFAULT"
    result = node_publisher.psql_capture(
        "DELETE FROM temporal_unique FOR PORTION OF valid_at FROM '2002-01-01' TO '2003-01-01' WHERE id = '[2,3)'"
    )
    assert (
        result.stderr
        == 'psql:<stdin>:1: ERROR:  cannot delete from table "temporal_unique" because it does not have a replica identity and publishes deletes\nHINT:  To enable deleting from the table, set REPLICA IDENTITY using ALTER TABLE.'
    ), "can't DELETE FOR PORTION OF temporal_unique DEFAULT"
    node_publisher.wait_for_catchup("sub1")
    result = node_subscriber.safe_psql(
        "SELECT * FROM temporal_unique ORDER BY id, valid_at"
    )
    assert (
        result
        == "[1,2)|[2000-01-01,2010-01-01)|a\n[2,3)|[2000-01-01,2010-01-01)|a\n[3,4)|[2000-01-01,2010-01-01)|a\n[4,5)|[2000-01-01,2010-01-01)|a"
    ), "replicated temporal_unique DEFAULT"
    drop_everything()
    create_tables()
    node_publisher.safe_psql("ALTER TABLE temporal_no_key REPLICA IDENTITY FULL")
    node_publisher.safe_psql("ALTER TABLE temporal_pk REPLICA IDENTITY FULL")
    node_publisher.safe_psql("ALTER TABLE temporal_unique REPLICA IDENTITY FULL")
    node_subscriber.safe_psql("ALTER TABLE temporal_no_key REPLICA IDENTITY FULL")
    node_subscriber.safe_psql("ALTER TABLE temporal_pk REPLICA IDENTITY FULL")
    node_subscriber.safe_psql("ALTER TABLE temporal_unique REPLICA IDENTITY FULL")
    node_publisher.safe_psql(
        "INSERT INTO temporal_no_key (id, valid_at, a)\n   VALUES ('[1,2)', '[2000-01-01,2010-01-01)', 'a')"
    )
    node_publisher.safe_psql(
        "INSERT INTO temporal_pk (id, valid_at, a)\n   VALUES ('[1,2)', '[2000-01-01,2010-01-01)', 'a')"
    )
    node_publisher.safe_psql(
        "INSERT INTO temporal_unique (id, valid_at, a)\n   VALUES ('[1,2)', '[2000-01-01,2010-01-01)', 'a')"
    )
    node_publisher.safe_psql("CREATE PUBLICATION pub1 FOR ALL TABLES")
    node_subscriber.safe_psql(
        "CREATE SUBSCRIPTION sub1 CONNECTION '"
        + publisher_connstr
        + "' PUBLICATION pub1"
    )
    node_subscriber.wait_for_subscription_sync()
    result = node_subscriber.safe_psql(
        "SELECT * FROM temporal_no_key ORDER BY id, valid_at"
    )
    assert result == "[1,2)|[2000-01-01,2010-01-01)|a", "synced temporal_no_key FULL"
    result = node_subscriber.safe_psql(
        "SELECT * FROM temporal_pk ORDER BY id, valid_at"
    )
    assert result == "[1,2)|[2000-01-01,2010-01-01)|a", "synced temporal_pk FULL"
    result = node_subscriber.safe_psql(
        "SELECT * FROM temporal_unique ORDER BY id, valid_at"
    )
    assert result == "[1,2)|[2000-01-01,2010-01-01)|a", "synced temporal_unique FULL"
    node_publisher.safe_psql(
        "INSERT INTO temporal_no_key (id, valid_at, a)\n   VALUES ('[2,3)', '[2000-01-01,2010-01-01)', 'a'),\n          ('[3,4)', '[2000-01-01,2010-01-01)', 'a'),\n          ('[4,5)', '[2000-01-01,2010-01-01)', 'a')"
    )
    node_publisher.safe_psql("UPDATE temporal_no_key SET a = 'b' WHERE id = '[2,3)'")
    node_publisher.safe_psql(
        "UPDATE temporal_no_key FOR PORTION OF valid_at FROM '2001-01-01' TO '2002-01-01' SET a = 'c' WHERE id = '[2,3)'"
    )
    node_publisher.safe_psql("DELETE FROM temporal_no_key WHERE id = '[3,4)'")
    node_publisher.safe_psql(
        "DELETE FROM temporal_no_key FOR PORTION OF valid_at FROM '2002-01-01' TO '2003-01-01' WHERE id = '[2,3)'"
    )
    node_publisher.wait_for_catchup("sub1")
    result = node_subscriber.safe_psql(
        "SELECT * FROM temporal_no_key ORDER BY id, valid_at"
    )
    assert (
        result
        == "[1,2)|[2000-01-01,2010-01-01)|a\n[2,3)|[2000-01-01,2001-01-01)|b\n[2,3)|[2001-01-01,2002-01-01)|c\n[2,3)|[2003-01-01,2010-01-01)|b\n[4,5)|[2000-01-01,2010-01-01)|a"
    ), "replicated temporal_no_key FULL"
    node_publisher.safe_psql(
        "INSERT INTO temporal_pk (id, valid_at, a)\n   VALUES ('[2,3)', '[2000-01-01,2010-01-01)', 'a'),\n          ('[3,4)', '[2000-01-01,2010-01-01)', 'a'),\n          ('[4,5)', '[2000-01-01,2010-01-01)', 'a')"
    )
    node_publisher.safe_psql("UPDATE temporal_pk SET a = 'b' WHERE id = '[2,3)'")
    node_publisher.safe_psql(
        "UPDATE temporal_pk FOR PORTION OF valid_at FROM '2001-01-01' TO '2002-01-01' SET a = 'c' WHERE id = '[2,3)'"
    )
    node_publisher.safe_psql("DELETE FROM temporal_pk WHERE id = '[3,4)'")
    node_publisher.safe_psql(
        "DELETE FROM temporal_pk FOR PORTION OF valid_at FROM '2002-01-01' TO '2003-01-01' WHERE id = '[2,3)'"
    )
    node_publisher.wait_for_catchup("sub1")
    result = node_subscriber.safe_psql(
        "SELECT * FROM temporal_pk ORDER BY id, valid_at"
    )
    assert (
        result
        == "[1,2)|[2000-01-01,2010-01-01)|a\n[2,3)|[2000-01-01,2001-01-01)|b\n[2,3)|[2001-01-01,2002-01-01)|c\n[2,3)|[2003-01-01,2010-01-01)|b\n[4,5)|[2000-01-01,2010-01-01)|a"
    ), "replicated temporal_pk FULL"
    node_publisher.safe_psql(
        "INSERT INTO temporal_unique (id, valid_at, a)\n   VALUES ('[2,3)', '[2000-01-01,2010-01-01)', 'a'),\n          ('[3,4)', '[2000-01-01,2010-01-01)', 'a'),\n          ('[4,5)', '[2000-01-01,2010-01-01)', 'a')"
    )
    node_publisher.safe_psql("UPDATE temporal_unique SET a = 'b' WHERE id = '[2,3)'")
    node_publisher.safe_psql(
        "UPDATE temporal_unique FOR PORTION OF valid_at FROM '2001-01-01' TO '2002-01-01' SET a = 'c' WHERE id = '[2,3)'"
    )
    node_publisher.safe_psql("DELETE FROM temporal_unique WHERE id = '[3,4)'")
    node_publisher.safe_psql(
        "DELETE FROM temporal_unique FOR PORTION OF valid_at FROM '2002-01-01' TO '2003-01-01' WHERE id = '[2,3)'"
    )
    node_publisher.wait_for_catchup("sub1")
    result = node_subscriber.safe_psql(
        "SELECT * FROM temporal_unique ORDER BY id, valid_at"
    )
    assert (
        result
        == "[1,2)|[2000-01-01,2010-01-01)|a\n[2,3)|[2000-01-01,2001-01-01)|b\n[2,3)|[2001-01-01,2002-01-01)|c\n[2,3)|[2003-01-01,2010-01-01)|b\n[4,5)|[2000-01-01,2010-01-01)|a"
    ), "replicated temporal_unique FULL"
    drop_everything()
    node_publisher.safe_psql(
        "CREATE TABLE temporal_pk (id int4range, valid_at daterange, a text, PRIMARY KEY (id, valid_at WITHOUT OVERLAPS))"
    )
    node_publisher.safe_psql(
        "ALTER TABLE temporal_pk REPLICA IDENTITY USING INDEX temporal_pk_pkey"
    )
    node_publisher.safe_psql(
        "CREATE TABLE temporal_unique (id int4range NOT NULL, valid_at daterange NOT NULL, a text, UNIQUE (id, valid_at WITHOUT OVERLAPS))"
    )
    node_publisher.safe_psql(
        "ALTER TABLE temporal_unique REPLICA IDENTITY USING INDEX temporal_unique_id_valid_at_key"
    )
    node_subscriber.safe_psql(
        "CREATE TABLE temporal_pk (id int4range, valid_at daterange, a text, PRIMARY KEY (id, valid_at WITHOUT OVERLAPS))"
    )
    node_subscriber.safe_psql(
        "ALTER TABLE temporal_pk REPLICA IDENTITY USING INDEX temporal_pk_pkey"
    )
    node_subscriber.safe_psql(
        "CREATE TABLE temporal_unique (id int4range NOT NULL, valid_at daterange NOT NULL, a text, UNIQUE (id, valid_at WITHOUT OVERLAPS))"
    )
    node_subscriber.safe_psql(
        "ALTER TABLE temporal_unique REPLICA IDENTITY USING INDEX temporal_unique_id_valid_at_key"
    )
    node_publisher.safe_psql(
        "INSERT INTO temporal_pk (id, valid_at, a)\n   VALUES ('[1,2)', '[2000-01-01,2010-01-01)', 'a')"
    )
    node_publisher.safe_psql(
        "INSERT INTO temporal_unique (id, valid_at, a)\n   VALUES ('[1,2)', '[2000-01-01,2010-01-01)', 'a')"
    )
    node_publisher.safe_psql("CREATE PUBLICATION pub1 FOR ALL TABLES")
    node_subscriber.safe_psql(
        "CREATE SUBSCRIPTION sub1 CONNECTION '"
        + publisher_connstr
        + "' PUBLICATION pub1"
    )
    node_subscriber.wait_for_subscription_sync()
    result = node_subscriber.safe_psql(
        "SELECT * FROM temporal_pk ORDER BY id, valid_at"
    )
    assert result == "[1,2)|[2000-01-01,2010-01-01)|a", "synced temporal_pk USING INDEX"
    result = node_subscriber.safe_psql(
        "SELECT * FROM temporal_unique ORDER BY id, valid_at"
    )
    assert (
        result == "[1,2)|[2000-01-01,2010-01-01)|a"
    ), "synced temporal_unique USING INDEX"
    node_publisher.safe_psql(
        "INSERT INTO temporal_pk (id, valid_at, a)\n   VALUES ('[2,3)', '[2000-01-01,2010-01-01)', 'a'),\n          ('[3,4)', '[2000-01-01,2010-01-01)', 'a'),\n          ('[4,5)', '[2000-01-01,2010-01-01)', 'a')"
    )
    node_publisher.safe_psql("UPDATE temporal_pk SET a = 'b' WHERE id = '[2,3)'")
    node_publisher.safe_psql(
        "UPDATE temporal_pk FOR PORTION OF valid_at FROM '2001-01-01' TO '2002-01-01' SET a = 'c' WHERE id = '[2,3)'"
    )
    node_publisher.safe_psql("DELETE FROM temporal_pk WHERE id = '[3,4)'")
    node_publisher.safe_psql(
        "DELETE FROM temporal_pk FOR PORTION OF valid_at FROM '2002-01-01' TO '2003-01-01' WHERE id = '[2,3)'"
    )
    node_publisher.wait_for_catchup("sub1")
    result = node_subscriber.safe_psql(
        "SELECT * FROM temporal_pk ORDER BY id, valid_at"
    )
    assert (
        result
        == "[1,2)|[2000-01-01,2010-01-01)|a\n[2,3)|[2000-01-01,2001-01-01)|b\n[2,3)|[2001-01-01,2002-01-01)|c\n[2,3)|[2003-01-01,2010-01-01)|b\n[4,5)|[2000-01-01,2010-01-01)|a"
    ), "replicated temporal_pk USING INDEX"
    node_publisher.safe_psql(
        "INSERT INTO temporal_unique (id, valid_at, a)\n   VALUES ('[2,3)', '[2000-01-01,2010-01-01)', 'a'),\n          ('[3,4)', '[2000-01-01,2010-01-01)', 'a'),\n          ('[4,5)', '[2000-01-01,2010-01-01)', 'a')"
    )
    node_publisher.safe_psql("UPDATE temporal_unique SET a = 'b' WHERE id = '[2,3)'")
    node_publisher.safe_psql(
        "UPDATE temporal_unique FOR PORTION OF valid_at FROM '2001-01-01' TO '2002-01-01' SET a = 'c' WHERE id = '[2,3)'"
    )
    node_publisher.safe_psql("DELETE FROM temporal_unique WHERE id = '[3,4)'")
    node_publisher.safe_psql(
        "DELETE FROM temporal_unique FOR PORTION OF valid_at FROM '2002-01-01' TO '2003-01-01' WHERE id = '[2,3)'"
    )
    node_publisher.wait_for_catchup("sub1")
    result = node_subscriber.safe_psql(
        "SELECT * FROM temporal_unique ORDER BY id, valid_at"
    )
    assert (
        result
        == "[1,2)|[2000-01-01,2010-01-01)|a\n[2,3)|[2000-01-01,2001-01-01)|b\n[2,3)|[2001-01-01,2002-01-01)|c\n[2,3)|[2003-01-01,2010-01-01)|b\n[4,5)|[2000-01-01,2010-01-01)|a"
    ), "replicated temporal_unique USING INDEX"
    drop_everything()
    create_tables()
    node_publisher.safe_psql("ALTER TABLE temporal_no_key REPLICA IDENTITY NOTHING")
    node_publisher.safe_psql("ALTER TABLE temporal_pk REPLICA IDENTITY NOTHING")
    node_publisher.safe_psql("ALTER TABLE temporal_unique REPLICA IDENTITY NOTHING")
    node_subscriber.safe_psql("ALTER TABLE temporal_no_key REPLICA IDENTITY NOTHING")
    node_subscriber.safe_psql("ALTER TABLE temporal_pk REPLICA IDENTITY NOTHING")
    node_subscriber.safe_psql("ALTER TABLE temporal_unique REPLICA IDENTITY NOTHING")
    node_publisher.safe_psql(
        "INSERT INTO temporal_no_key (id, valid_at, a)\n   VALUES ('[1,2)', '[2000-01-01,2010-01-01)', 'a')"
    )
    node_publisher.safe_psql(
        "INSERT INTO temporal_pk (id, valid_at, a)\n   VALUES ('[1,2)', '[2000-01-01,2010-01-01)', 'a')"
    )
    node_publisher.safe_psql(
        "INSERT INTO temporal_unique (id, valid_at, a)\n   VALUES ('[1,2)', '[2000-01-01,2010-01-01)', 'a')"
    )
    node_publisher.safe_psql("CREATE PUBLICATION pub1 FOR ALL TABLES")
    node_subscriber.safe_psql(
        "CREATE SUBSCRIPTION sub1 CONNECTION '"
        + publisher_connstr
        + "' PUBLICATION pub1"
    )
    node_subscriber.wait_for_subscription_sync()
    result = node_subscriber.safe_psql(
        "SELECT * FROM temporal_no_key ORDER BY id, valid_at"
    )
    assert result == "[1,2)|[2000-01-01,2010-01-01)|a", "synced temporal_no_key NOTHING"
    result = node_subscriber.safe_psql(
        "SELECT * FROM temporal_pk ORDER BY id, valid_at"
    )
    assert result == "[1,2)|[2000-01-01,2010-01-01)|a", "synced temporal_pk NOTHING"
    result = node_subscriber.safe_psql(
        "SELECT * FROM temporal_unique ORDER BY id, valid_at"
    )
    assert result == "[1,2)|[2000-01-01,2010-01-01)|a", "synced temporal_unique NOTHING"
    node_publisher.safe_psql(
        "INSERT INTO temporal_no_key (id, valid_at, a)\n   VALUES ('[2,3)', '[2000-01-01,2010-01-01)', 'a'),\n          ('[3,4)', '[2000-01-01,2010-01-01)', 'a'),\n          ('[4,5)', '[2000-01-01,2010-01-01)', 'a')"
    )
    result = node_publisher.psql_capture(
        "UPDATE temporal_no_key SET a = 'b' WHERE id = '[2,3)'"
    )
    assert (
        result.stderr
        == 'psql:<stdin>:1: ERROR:  cannot update table "temporal_no_key" because it does not have a replica identity and publishes updates\nHINT:  To enable updating the table, set REPLICA IDENTITY using ALTER TABLE.'
    ), "can't UPDATE temporal_no_key NOTHING"
    result = node_publisher.psql_capture(
        "DELETE FROM temporal_no_key WHERE id = '[3,4)'"
    )
    assert (
        result.stderr
        == 'psql:<stdin>:1: ERROR:  cannot delete from table "temporal_no_key" because it does not have a replica identity and publishes deletes\nHINT:  To enable deleting from the table, set REPLICA IDENTITY using ALTER TABLE.'
    ), "can't DELETE temporal_no_key NOTHING"
    result = node_publisher.psql_capture(
        "DELETE FROM temporal_no_key FOR PORTION OF valid_at FROM '2002-01-01' TO '2003-01-01' WHERE id = '[2,3)'"
    )
    assert (
        result.stderr
        == 'psql:<stdin>:1: ERROR:  cannot delete from table "temporal_no_key" because it does not have a replica identity and publishes deletes\nHINT:  To enable deleting from the table, set REPLICA IDENTITY using ALTER TABLE.'
    ), "can't DELETE temporal_no_key NOTHING"
    node_publisher.wait_for_catchup("sub1")
    result = node_subscriber.safe_psql(
        "SELECT * FROM temporal_no_key ORDER BY id, valid_at"
    )
    assert (
        result
        == "[1,2)|[2000-01-01,2010-01-01)|a\n[2,3)|[2000-01-01,2010-01-01)|a\n[3,4)|[2000-01-01,2010-01-01)|a\n[4,5)|[2000-01-01,2010-01-01)|a"
    ), "replicated temporal_no_key NOTHING"
    node_publisher.safe_psql(
        "INSERT INTO temporal_pk (id, valid_at, a)\n   VALUES ('[2,3)', '[2000-01-01,2010-01-01)', 'a'),\n          ('[3,4)', '[2000-01-01,2010-01-01)', 'a'),\n          ('[4,5)', '[2000-01-01,2010-01-01)', 'a')"
    )
    result = node_publisher.psql_capture(
        "UPDATE temporal_pk SET a = 'b' WHERE id = '[2,3)'"
    )
    assert (
        result.stderr
        == 'psql:<stdin>:1: ERROR:  cannot update table "temporal_pk" because it does not have a replica identity and publishes updates\nHINT:  To enable updating the table, set REPLICA IDENTITY using ALTER TABLE.'
    ), "can't UPDATE temporal_pk NOTHING"
    result = node_publisher.psql_capture("DELETE FROM temporal_pk WHERE id = '[3,4)'")
    assert (
        result.stderr
        == 'psql:<stdin>:1: ERROR:  cannot delete from table "temporal_pk" because it does not have a replica identity and publishes deletes\nHINT:  To enable deleting from the table, set REPLICA IDENTITY using ALTER TABLE.'
    ), "can't DELETE temporal_pk NOTHING"
    result = node_publisher.psql_capture(
        "DELETE FROM temporal_pk FOR PORTION OF valid_at FROM '2002-01-01' TO '2003-01-01' WHERE id = '[2,3)'"
    )
    assert (
        result.stderr
        == 'psql:<stdin>:1: ERROR:  cannot delete from table "temporal_pk" because it does not have a replica identity and publishes deletes\nHINT:  To enable deleting from the table, set REPLICA IDENTITY using ALTER TABLE.'
    ), "can't DELETE temporal_pk NOTHING"
    node_publisher.wait_for_catchup("sub1")
    result = node_subscriber.safe_psql(
        "SELECT * FROM temporal_pk ORDER BY id, valid_at"
    )
    assert (
        result
        == "[1,2)|[2000-01-01,2010-01-01)|a\n[2,3)|[2000-01-01,2010-01-01)|a\n[3,4)|[2000-01-01,2010-01-01)|a\n[4,5)|[2000-01-01,2010-01-01)|a"
    ), "replicated temporal_pk NOTHING"
    node_publisher.safe_psql(
        "INSERT INTO temporal_unique (id, valid_at, a)\n   VALUES ('[2,3)', '[2000-01-01,2010-01-01)', 'a'),\n          ('[3,4)', '[2000-01-01,2010-01-01)', 'a'),\n          ('[4,5)', '[2000-01-01,2010-01-01)', 'a')"
    )
    result = node_publisher.psql_capture(
        "UPDATE temporal_unique SET a = 'b' WHERE id = '[2,3)'"
    )
    assert (
        result.stderr
        == 'psql:<stdin>:1: ERROR:  cannot update table "temporal_unique" because it does not have a replica identity and publishes updates\nHINT:  To enable updating the table, set REPLICA IDENTITY using ALTER TABLE.'
    ), "can't UPDATE temporal_unique NOTHING"
    result = node_publisher.psql_capture(
        "DELETE FROM temporal_unique WHERE id = '[3,4)'"
    )
    assert (
        result.stderr
        == 'psql:<stdin>:1: ERROR:  cannot delete from table "temporal_unique" because it does not have a replica identity and publishes deletes\nHINT:  To enable deleting from the table, set REPLICA IDENTITY using ALTER TABLE.'
    ), "can't DELETE temporal_unique NOTHING"
    result = node_publisher.psql_capture(
        "DELETE FROM temporal_unique FOR PORTION OF valid_at FROM '2002-01-01' TO '2003-01-01' WHERE id = '[2,3)'"
    )
    assert (
        result.stderr
        == 'psql:<stdin>:1: ERROR:  cannot delete from table "temporal_unique" because it does not have a replica identity and publishes deletes\nHINT:  To enable deleting from the table, set REPLICA IDENTITY using ALTER TABLE.'
    ), "can't DELETE FOR PORTION OF temporal_unique NOTHING"
    node_publisher.wait_for_catchup("sub1")
    result = node_subscriber.safe_psql(
        "SELECT * FROM temporal_unique ORDER BY id, valid_at"
    )
    assert (
        result
        == "[1,2)|[2000-01-01,2010-01-01)|a\n[2,3)|[2000-01-01,2010-01-01)|a\n[3,4)|[2000-01-01,2010-01-01)|a\n[4,5)|[2000-01-01,2010-01-01)|a"
    ), "replicated temporal_unique NOTHING"
    drop_everything()

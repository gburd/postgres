# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/subscription/t/012_collation.pl.

Collations, in particular nondeterministic ones (ICU only).
"""

import os

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("with_icu") != "yes", reason="ICU not supported by this build"
)


def test_collation(create_pg):
    """A nondeterministic collation key is matched correctly during apply."""
    publisher = create_pg(
        "publisher", allows_streaming="logical", extra=["--locale=C", "--encoding=UTF8"]
    )
    subscriber = create_pg("subscriber", extra=["--locale=C", "--encoding=UTF8"])

    connstr = publisher.connstr() + " dbname=postgres"

    subscriber.safe_psql(
        "CREATE COLLATION ctest_nondet "
        "(provider = icu, locale = 'und', deterministic = false)"
    )

    # Table with replica identity index. The publisher and subscriber rows are
    # collation-wise equal but byte-wise different (different normal forms).
    publisher.safe_psql("CREATE TABLE tab1 (a text PRIMARY KEY, b text)")
    publisher.safe_psql(r"INSERT INTO tab1 VALUES (U&'\00E4bc', 'foo')")
    subscriber.safe_psql(
        "CREATE TABLE tab1 (a text COLLATE ctest_nondet PRIMARY KEY, b text)"
    )
    subscriber.safe_psql(r"INSERT INTO tab1 VALUES (U&'\0061\0308bc', 'foo')")

    # Table with replica identity full.
    publisher.safe_psql("CREATE TABLE tab2 (a text, b text)")
    publisher.safe_psql("ALTER TABLE tab2 REPLICA IDENTITY FULL")
    publisher.safe_psql(r"INSERT INTO tab2 VALUES (U&'\00E4bc', 'foo')")
    subscriber.safe_psql("CREATE TABLE tab2 (a text COLLATE ctest_nondet, b text)")
    subscriber.safe_psql("ALTER TABLE tab2 REPLICA IDENTITY FULL")
    subscriber.safe_psql(r"INSERT INTO tab2 VALUES (U&'\0061\0308bc', 'foo')")

    publisher.safe_psql("CREATE PUBLICATION pub1 FOR ALL TABLES")
    subscriber.safe_psql(
        "CREATE SUBSCRIPTION sub1 CONNECTION '{}' PUBLICATION pub1 "
        "WITH (copy_data = false)".format(connstr)
    )
    publisher.wait_for_catchup("sub1")

    # Replica identity index: the subscriber must find the row via the
    # nondeterministic collation.
    publisher.safe_psql("UPDATE tab1 SET b = 'bar' WHERE b = 'foo'")
    publisher.wait_for_catchup("sub1")
    assert (
        subscriber.safe_psql("SELECT b FROM tab1") == "bar"
    ), "update with primary key with nondeterministic collation"

    # Replica identity full.
    publisher.safe_psql("UPDATE tab2 SET b = 'bar' WHERE b = 'foo'")
    publisher.wait_for_catchup("sub1")
    assert (
        subscriber.safe_psql("SELECT b FROM tab2") == "bar"
    ), "update with replica identity full with nondeterministic collation"

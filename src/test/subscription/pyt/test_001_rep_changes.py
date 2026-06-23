# Copyright (c) 2022-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long,too-many-statements
# pylint: disable=line-too-long,too-many-statements
"""Port of src/test/subscription/t/001_rep_changes.pl.

Core logical-replication change propagation: initial table sync, incremental
INSERT/UPDATE/DELETE across replica-identity variants, REPLICA IDENTITY
NOTHING/FULL, included-column indexes, no-column tables, ALTER PUBLICATION
ADD/DROP TABLE, multiple publications, CONNECTION-string options reaching the
walsender (log_statement_stats -> QUERY STATISTICS), and the wal_level=minimal
CREATE PUBLICATION warning. Generated from the Perl original via
.agent/gen_golden.py with three procedural spots hand-finished.
"""

import re


def test_001_rep_changes(create_pg):
    """Generated golden port of 001_rep_changes."""
    node_publisher = create_pg("publisher", allows_streaming="logical", start=False)
    node_publisher.start()
    node_subscriber = create_pg("subscriber", start=False)
    node_subscriber.start()
    node_publisher.safe_psql(
        "CREATE FUNCTION public.pg_get_replica_identity_index(int)\n\t RETURNS regclass LANGUAGE sql AS 'SELECT 1/0'"
    )
    node_publisher.safe_psql(
        "CREATE TABLE tab_notrep AS SELECT generate_series(1,10) AS a"
    )
    node_publisher.safe_psql(
        "CREATE TABLE tab_ins AS SELECT generate_series(1,1002) AS a"
    )
    node_publisher.safe_psql(
        "CREATE TABLE tab_full AS SELECT generate_series(1,10) AS a"
    )
    node_publisher.safe_psql("CREATE TABLE tab_full2 (x text)")
    node_publisher.safe_psql("INSERT INTO tab_full2 VALUES ('a'), ('b'), ('b')")
    node_publisher.safe_psql("CREATE TABLE tab_rep (a int primary key)")
    node_publisher.safe_psql(
        "CREATE TABLE tab_mixed (a int primary key, b text, c numeric)"
    )
    node_publisher.safe_psql("INSERT INTO tab_mixed (a, b, c) VALUES (1, 'foo', 1.1)")
    node_publisher.safe_psql(
        "CREATE TABLE tab_include (a int, b text, CONSTRAINT covering PRIMARY KEY(a) INCLUDE(b))"
    )
    node_publisher.safe_psql("CREATE TABLE tab_full_pk (a int primary key, b text)")
    node_publisher.safe_psql("ALTER TABLE tab_full_pk REPLICA IDENTITY FULL")
    node_publisher.safe_psql("CREATE TABLE tab_nothing (a int)")
    node_publisher.safe_psql("ALTER TABLE tab_nothing REPLICA IDENTITY NOTHING")
    node_publisher.safe_psql("CREATE TABLE tab_no_replidentity_index(c1 int)")
    node_publisher.safe_psql(
        "CREATE INDEX idx_no_replidentity_index ON tab_no_replidentity_index(c1)"
    )
    node_publisher.safe_psql("CREATE TABLE tab_no_col()")
    node_publisher.safe_psql("INSERT INTO tab_no_col default VALUES")
    node_subscriber.safe_psql("CREATE TABLE tab_notrep (a int)")
    node_subscriber.safe_psql("CREATE TABLE tab_ins (a int)")
    node_subscriber.safe_psql("CREATE TABLE tab_full (a int)")
    node_subscriber.safe_psql("CREATE TABLE tab_full2 (x text)")
    node_subscriber.safe_psql("CREATE TABLE tab_rep (a int primary key)")
    node_subscriber.safe_psql("CREATE TABLE tab_full_pk (a int primary key, b text)")
    node_subscriber.safe_psql("ALTER TABLE tab_full_pk REPLICA IDENTITY FULL")
    node_subscriber.safe_psql("CREATE TABLE tab_nothing (a int)")
    node_subscriber.safe_psql(
        "CREATE TABLE tab_mixed (d text default 'local', c numeric, b text, a int primary key)"
    )
    node_subscriber.safe_psql(
        "CREATE TABLE tab_include (a int, b text, CONSTRAINT covering PRIMARY KEY(a) INCLUDE(b))"
    )
    node_subscriber.safe_psql("CREATE TABLE tab_no_replidentity_index(c1 int)")
    node_subscriber.safe_psql(
        "CREATE INDEX idx_no_replidentity_index ON tab_no_replidentity_index(c1)"
    )
    node_subscriber.safe_psql("CREATE TABLE tab_no_col()")
    publisher_connstr = node_publisher.connstr() + " dbname=postgres"
    node_publisher.safe_psql("CREATE PUBLICATION tap_pub")
    node_publisher.safe_psql(
        "CREATE PUBLICATION tap_pub_ins_only WITH (publish = insert)"
    )
    node_publisher.safe_psql(
        "ALTER PUBLICATION tap_pub ADD TABLE tab_rep, tab_full, tab_full2, tab_mixed, tab_include, tab_nothing, tab_full_pk, tab_no_replidentity_index, tab_no_col"
    )
    node_publisher.safe_psql("ALTER PUBLICATION tap_pub_ins_only ADD TABLE tab_ins")
    node_subscriber.safe_psql(
        "CREATE SUBSCRIPTION tap_sub CONNECTION '"
        + publisher_connstr
        + "' PUBLICATION tap_pub, tap_pub_ins_only"
    )
    node_subscriber.wait_for_subscription_sync()
    node_publisher.safe_psql("SELECT pg_stat_reset_shared('io')")
    result = node_subscriber.safe_psql("SELECT count(*) FROM tab_notrep")
    assert result == "0", "check non-replicated table is empty on subscriber"
    result = node_subscriber.safe_psql("SELECT count(*) FROM tab_ins")
    assert result == "1002", "check initial data was copied to subscriber"
    node_publisher.safe_psql("INSERT INTO tab_ins SELECT generate_series(1,50)")
    node_publisher.safe_psql("DELETE FROM tab_ins WHERE a > 20")
    node_publisher.safe_psql("UPDATE tab_ins SET a = -a")
    node_publisher.safe_psql("INSERT INTO tab_rep SELECT generate_series(1,50)")
    node_publisher.safe_psql("DELETE FROM tab_rep WHERE a > 20")
    node_publisher.safe_psql("UPDATE tab_rep SET a = -a")
    node_publisher.safe_psql("INSERT INTO tab_mixed VALUES (2, 'bar', 2.2)")
    node_publisher.safe_psql("INSERT INTO tab_full_pk VALUES (1, 'foo'), (2, 'baz')")
    node_publisher.safe_psql("INSERT INTO tab_nothing VALUES (generate_series(1,20))")
    node_publisher.safe_psql("INSERT INTO tab_include SELECT generate_series(1,50)")
    node_publisher.safe_psql("DELETE FROM tab_include WHERE a > 20")
    node_publisher.safe_psql("UPDATE tab_include SET a = -a")
    node_publisher.safe_psql("INSERT INTO tab_no_replidentity_index VALUES(1)")
    node_publisher.safe_psql("INSERT INTO tab_no_col default VALUES")
    node_publisher.wait_for_catchup("tap_sub")
    result = node_subscriber.safe_psql("SELECT count(*), min(a), max(a) FROM tab_ins")
    assert result == "1052|1|1002", "check replicated inserts on subscriber"
    result = node_subscriber.safe_psql("SELECT count(*), min(a), max(a) FROM tab_rep")
    assert result == "20|-20|-1", "check replicated changes on subscriber"
    result = node_subscriber.safe_psql("SELECT * FROM tab_mixed")
    assert (
        result == "local|1.1|foo|1\nlocal|2.2|bar|2"
    ), "check replicated changes with different column order"
    result = node_subscriber.safe_psql("SELECT count(*) FROM tab_nothing")
    assert result == "20", "check replicated changes with REPLICA IDENTITY NOTHING"
    result = node_subscriber.safe_psql(
        "SELECT count(*), min(a), max(a) FROM tab_include"
    )
    assert (
        result == "20|-20|-1"
    ), "check replicated changes with primary key index with included columns"
    assert (
        node_subscriber.safe_psql("SELECT c1 FROM tab_no_replidentity_index") == "1"
    ), "value replicated to subscriber without replica identity index"
    result = node_subscriber.safe_psql("SELECT count(*) FROM tab_no_col")
    assert result == "2", "check replicated changes for table having no columns"
    assert node_publisher.poll_query_until(
        "SELECT sum(reads) > 0\n       FROM pg_catalog.pg_stat_io\n       WHERE backend_type = 'walsender'\n       AND object = 'wal'"
    ), "Timed out while waiting for the walsender to update its IO statistics"
    node_publisher.safe_psql("INSERT INTO tab_full SELECT generate_series(1,10)")
    result = node_subscriber.safe_psql("SELECT count(*), min(a), max(a) FROM tab_ins")
    assert (
        result == "1052|1|1002"
    ), "check rows on subscriber before table drop from publication"
    node_publisher.safe_psql("ALTER PUBLICATION tap_pub_ins_only DROP TABLE tab_ins")
    node_publisher.safe_psql("INSERT INTO tab_ins VALUES(8888)")
    node_publisher.wait_for_catchup("tap_sub")
    result = node_subscriber.safe_psql("SELECT count(*), min(a), max(a) FROM tab_ins")
    assert (
        result == "1052|1|1002"
    ), "check rows on subscriber after table drop from publication"
    node_publisher.safe_psql("DELETE FROM tab_ins WHERE a = 8888")
    node_publisher.safe_psql("ALTER PUBLICATION tap_pub_ins_only ADD TABLE tab_ins")
    node_subscriber.safe_psql("ALTER SUBSCRIPTION tap_sub REFRESH PUBLICATION")
    node_publisher.safe_psql("CREATE TABLE temp1 (a int)")
    node_publisher.safe_psql("CREATE TABLE temp2 (a int)")
    node_subscriber.safe_psql("CREATE TABLE temp1 (a int)")
    node_subscriber.safe_psql("CREATE TABLE temp2 (a int)")
    node_publisher.safe_psql(
        "CREATE PUBLICATION tap_pub_temp1 FOR TABLE temp1 WITH (publish = insert)"
    )
    node_publisher.safe_psql("CREATE PUBLICATION tap_pub_temp2 FOR TABLE temp2")
    node_subscriber.safe_psql(
        "CREATE SUBSCRIPTION tap_sub_temp1 CONNECTION '"
        + publisher_connstr
        + "' PUBLICATION tap_pub_temp1, tap_pub_temp2"
    )
    node_subscriber.wait_for_subscription_sync()
    result = node_subscriber.safe_psql("SELECT count(*) FROM temp1")
    assert result == "0", "check initial rows on subscriber with multiple publications"
    node_publisher.safe_psql("INSERT INTO temp1 VALUES (1)")
    node_publisher.wait_for_catchup("tap_sub_temp1")
    result = node_subscriber.safe_psql("SELECT count(*) FROM temp1")
    assert result == "1", "check rows on subscriber with multiple publications"
    node_subscriber.safe_psql("DROP SUBSCRIPTION tap_sub_temp1")
    node_publisher.safe_psql("DROP PUBLICATION tap_pub_temp1")
    node_publisher.safe_psql("DROP PUBLICATION tap_pub_temp2")
    node_publisher.safe_psql("DROP TABLE temp1")
    node_publisher.safe_psql("DROP TABLE temp2")
    node_subscriber.safe_psql("DROP TABLE temp1")
    node_subscriber.safe_psql("DROP TABLE temp2")
    node_publisher.safe_psql("ALTER TABLE tab_full REPLICA IDENTITY FULL")
    node_subscriber.safe_psql("ALTER TABLE tab_full REPLICA IDENTITY FULL")
    node_publisher.safe_psql("ALTER TABLE tab_full2 REPLICA IDENTITY FULL")
    node_subscriber.safe_psql("ALTER TABLE tab_full2 REPLICA IDENTITY FULL")
    node_publisher.safe_psql("ALTER TABLE tab_ins REPLICA IDENTITY FULL")
    node_subscriber.safe_psql("ALTER TABLE tab_ins REPLICA IDENTITY FULL")
    node_publisher.safe_psql("UPDATE tab_full SET a = a * a")
    node_publisher.safe_psql("UPDATE tab_full2 SET x = 'bb' WHERE x = 'b'")
    node_publisher.safe_psql("UPDATE tab_mixed SET b = 'baz' WHERE a = 1")
    node_publisher.safe_psql("UPDATE tab_full_pk SET b = 'bar' WHERE a = 1")
    node_publisher.wait_for_catchup("tap_sub")
    result = node_subscriber.safe_psql("SELECT count(*), min(a), max(a) FROM tab_full")
    assert (
        result == "20|1|100"
    ), "update works with REPLICA IDENTITY FULL and duplicate tuples"
    result = node_subscriber.safe_psql("SELECT x FROM tab_full2 ORDER BY 1")
    assert (
        result == "a\nbb\nbb"
    ), "update works with REPLICA IDENTITY FULL and text datums"
    result = node_subscriber.safe_psql("SELECT * FROM tab_mixed ORDER BY a")
    assert (
        result == "local|1.1|baz|1\nlocal|2.2|bar|2"
    ), "update works with different column order and subscriber local values"
    result = node_subscriber.safe_psql("SELECT * FROM tab_full_pk ORDER BY a")
    assert (
        result == "1|bar\n2|baz"
    ), "update works with REPLICA IDENTITY FULL and a primary key"
    node_subscriber.safe_psql("DELETE FROM tab_full_pk")
    node_subscriber.safe_psql("DELETE FROM tab_full WHERE a = 25")
    log_location_pub = node_publisher.current_log_position()
    log_location_sub = node_subscriber.current_log_position()
    node_publisher.safe_psql("UPDATE tab_full_pk SET b = 'quux' WHERE a = 1")
    node_publisher.safe_psql("UPDATE tab_full SET a = a + 1 WHERE a = 25")
    node_publisher.safe_psql("DELETE FROM tab_full_pk WHERE a = 2")
    node_publisher.wait_for_catchup("tap_sub")
    assert node_subscriber.log_matches(
        r"""conflict detected on relation "public.tab_full_pk": conflict=update_missing.*\n.*DETAIL:.* Could not find the row to be updated: remote row \(1, quux\), replica identity \(a\)=\(1\)""",
        log_location_sub,
    ), "update target row is missing"
    assert node_subscriber.log_matches(
        r"""conflict detected on relation "public.tab_full": conflict=update_missing.*\n.*DETAIL:.* Could not find the row to be updated: remote row \(26\), replica identity full \(25\)""",
        log_location_sub,
    ), "update target row is missing"
    assert node_subscriber.log_matches(
        r"""conflict detected on relation "public.tab_full_pk": conflict=delete_missing.*\n.*DETAIL:.* Could not find the row to be deleted: replica identity \(a\)=\(2\)""",
        log_location_sub,
    ), "delete target row is missing"
    node_subscriber.append_conf("log_min_messages = warning")
    node_subscriber.reload()
    node_publisher.safe_psql(
        "UPDATE tab_mixed SET b = repeat('xyzzy', 100000) WHERE a = 2"
    )
    node_publisher.wait_for_catchup("tap_sub")
    result = node_subscriber.safe_psql(
        "SELECT a, length(b), c, d FROM tab_mixed ORDER BY a"
    )
    assert (
        result == "1|3|1.1|local\n2|500000|2.2|local"
    ), "update transmits large column value"
    node_publisher.safe_psql("UPDATE tab_mixed SET c = 3.3 WHERE a = 2")
    node_publisher.wait_for_catchup("tap_sub")
    result = node_subscriber.safe_psql(
        "SELECT a, length(b), c, d FROM tab_mixed ORDER BY a"
    )
    assert (
        result == "1|3|1.1|local\n2|500000|3.3|local"
    ), "update with non-transmitted large column value"
    node_publisher.safe_psql("UPDATE tab_mixed SET b = 'bar', c = 2.2 WHERE a = 2")
    node_publisher.safe_psql("ALTER TABLE tab_mixed DROP COLUMN b")
    node_publisher.safe_psql("UPDATE tab_mixed SET c = 11.11 WHERE a = 1")
    node_publisher.wait_for_catchup("tap_sub")
    result = node_subscriber.safe_psql("SELECT * FROM tab_mixed ORDER BY a")
    assert (
        result == "local|11.11|baz|1\nlocal|2.2|bar|2"
    ), "update works with dropped publisher column"
    node_subscriber.safe_psql("ALTER TABLE tab_mixed DROP COLUMN d")
    node_publisher.safe_psql("UPDATE tab_mixed SET c = 22.22 WHERE a = 2")
    node_publisher.wait_for_catchup("tap_sub")
    result = node_subscriber.safe_psql("SELECT * FROM tab_mixed ORDER BY a")
    assert (
        result == "11.11|baz|1\n22.22|bar|2"
    ), "update works with dropped subscriber column"
    assert not node_publisher.log_matches(
        r"""QUERY STATISTICS""",
        log_location_pub,
    ), "log_statement_stats has not been enabled yet"
    log_location_pub = node_publisher.current_log_position()
    oldpid = node_publisher.safe_psql(
        "SELECT pid FROM pg_stat_replication WHERE application_name = 'tap_sub' AND state = 'streaming';"
    )
    node_subscriber.safe_psql(
        "ALTER SUBSCRIPTION tap_sub CONNECTION '"
        + publisher_connstr
        + " options=''-c log_statement_stats=on'''"
    )
    assert node_publisher.poll_query_until(
        "SELECT pid != "
        + str(oldpid)
        + " FROM pg_stat_replication WHERE application_name = 'tap_sub' AND state = 'streaming';"
    ), "Timed out while waiting for apply to restart after changing CONNECTION"
    assert node_publisher.wait_for_log(
        r"QUERY STATISTICS", log_location_pub
    ), "log_statement_stats in CONNECTION string had effect on publisher's walsender"
    oldpid = node_publisher.safe_psql(
        "SELECT pid FROM pg_stat_replication WHERE application_name = 'tap_sub' AND state = 'streaming';"
    )
    node_subscriber.safe_psql(
        "ALTER SUBSCRIPTION tap_sub SET PUBLICATION tap_pub_ins_only WITH (copy_data = false)"
    )
    assert node_publisher.poll_query_until(
        "SELECT pid != "
        + str(oldpid)
        + " FROM pg_stat_replication WHERE application_name = 'tap_sub' AND state = 'streaming';"
    ), "Timed out while waiting for apply to restart after changing PUBLICATION"
    node_publisher.safe_psql("INSERT INTO tab_ins SELECT generate_series(1001,1100)")
    node_publisher.safe_psql("DELETE FROM tab_rep")
    node_publisher.stop("fast")
    node_publisher.start()
    node_publisher.wait_for_catchup("tap_sub")
    result = node_subscriber.safe_psql("SELECT count(*), min(a), max(a) FROM tab_ins")
    assert (
        result == "1152|1|1100"
    ), "check replicated inserts after subscription publication change"
    result = node_subscriber.safe_psql("SELECT count(*), min(a), max(a) FROM tab_rep")
    assert (
        result == "20|-20|-1"
    ), "check changes skipped after subscription publication change"
    node_publisher.safe_psql(
        "ALTER PUBLICATION tap_pub_ins_only SET (publish = 'insert, delete')"
    )
    node_publisher.safe_psql("ALTER PUBLICATION tap_pub_ins_only ADD TABLE tab_full")
    node_publisher.safe_psql("DELETE FROM tab_ins WHERE a > 0")
    node_subscriber.safe_psql(
        "ALTER SUBSCRIPTION tap_sub REFRESH PUBLICATION WITH (copy_data = false)"
    )
    node_publisher.safe_psql("INSERT INTO tab_full VALUES(0)")
    node_publisher.wait_for_catchup("tap_sub")
    node_publisher.append_conf("log_min_messages = debug1")
    node_publisher.reload()
    log_location_pub = node_publisher.current_log_position()
    node_publisher.safe_psql("INSERT INTO tab_notrep VALUES (11)")
    node_publisher.wait_for_catchup("tap_sub")
    assert node_publisher.log_matches(
        r"""skipped replication of an empty transaction with XID""",
        log_location_pub,
    ), "empty transaction is skipped"
    result = node_subscriber.safe_psql("SELECT count(*) FROM tab_notrep")
    assert result == "0", "check non-replicated table is empty on subscriber"
    node_publisher.append_conf("log_min_messages = warning")
    node_publisher.reload()
    result = node_subscriber.safe_psql("SELECT count(*), min(a), max(a) FROM tab_ins")
    assert result == "1052|1|1002", "check replicated deletes after alter publication"
    result = node_subscriber.safe_psql("SELECT count(*), min(a), max(a) FROM tab_full")
    assert result == "19|0|100", "check replicated insert after alter publication"
    oldpid = node_publisher.safe_psql(
        "SELECT pid FROM pg_stat_replication WHERE application_name = 'tap_sub' AND state = 'streaming';"
    )
    node_subscriber.safe_psql("ALTER SUBSCRIPTION tap_sub RENAME TO tap_sub_renamed")
    assert node_publisher.poll_query_until(
        "SELECT pid != "
        + str(oldpid)
        + " FROM pg_stat_replication WHERE application_name = 'tap_sub_renamed' AND state = 'streaming';"
    ), "Timed out while waiting for apply to restart after renaming SUBSCRIPTION"
    node_subscriber.safe_psql("DROP SUBSCRIPTION tap_sub_renamed")
    result = node_subscriber.safe_psql("SELECT count(*) FROM pg_subscription")
    assert result == "0", "check subscription was dropped on subscriber"
    result = node_publisher.safe_psql("SELECT count(*) FROM pg_replication_slots")
    assert result == "0", "check replication slot was dropped on publisher"
    result = node_subscriber.safe_psql("SELECT count(*) FROM pg_subscription_rel")
    assert result == "0", "check subscription relation status was dropped on subscriber"
    result = node_publisher.safe_psql("SELECT count(*) FROM pg_replication_slots")
    assert result == "0", "check replication slot was dropped on publisher"
    result = node_subscriber.safe_psql("SELECT count(*) FROM pg_replication_origin")
    assert result == "0", "check replication origin was dropped on subscriber"
    node_subscriber.stop("fast")
    node_publisher.stop("fast")
    node_publisher.append_conf("\nwal_level=minimal\nmax_wal_senders=0\n")
    node_publisher.start()
    result = node_publisher.psql_capture(
        "BEGIN;\nCREATE TABLE skip_wal();\n"
        "CREATE PUBLICATION tap_pub2 FOR TABLE skip_wal;\nROLLBACK;"
    )
    assert re.search(
        r"WARNING:  logical decoding must be enabled to publish logical changes",
        result.stderr,
    ), 'CREATE PUBLICATION while "wal_level=minimal"'

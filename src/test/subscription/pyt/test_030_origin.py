# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/subscription/t/030_origin.pl.

CREATE SUBSCRIPTION 'origin' parameter and its interaction with 'copy_data'.
"""

import re

_TAB_UNQUOTED = "tab'le"
_TAB = '"tab\'le"'

_AB = "tap_sub_A_B"
_AB2 = "tap_sub_A_B_2"
_BA = "tap_sub_B_A"
_BC = "tap_sub_B_C"


def _warn_copy(subname):
    return (
        r'WARNING: ( [A-Z0-9]+:)? subscription "{}" requested copy_data with '
        r"origin = NONE but might copy data that had a different origin".format(subname)
    )


def _rows(node):
    return node.safe_psql("SELECT * FROM {} ORDER BY 1;".format(_TAB))


def _setup_bidir(node_a, node_b, a_connstr, b_connstr):
    node_a.safe_psql("CREATE TABLE {} (a int PRIMARY KEY)".format(_TAB))
    node_b.safe_psql("CREATE TABLE {} (a int PRIMARY KEY)".format(_TAB))

    node_a.safe_psql("CREATE PUBLICATION tap_pub_A FOR TABLE {}".format(_TAB))
    node_b.safe_psql(
        "CREATE SUBSCRIPTION {0} CONNECTION '{1} application_name={0}' "
        "PUBLICATION tap_pub_A WITH (origin = none)".format(_BA, a_connstr)
    )
    node_b.safe_psql("CREATE PUBLICATION tap_pub_B FOR TABLE {}".format(_TAB))
    node_a.safe_psql(
        "CREATE SUBSCRIPTION {0} CONNECTION '{1} application_name={0}' "
        "PUBLICATION tap_pub_B WITH (origin = none, copy_data = off)".format(
            _AB, b_connstr
        )
    )
    node_a.wait_for_subscription_sync(node_b, _AB)
    node_b.wait_for_subscription_sync(node_a, _BA)


def _check_no_recursion_and_origin(node_a, node_b, node_c, c_connstr):
    node_a.safe_psql("INSERT INTO {} VALUES (11);".format(_TAB))
    node_b.safe_psql("INSERT INTO {} VALUES (21);".format(_TAB))
    node_a.wait_for_catchup(_BA)
    node_b.wait_for_catchup(_AB)
    assert _rows(node_a) == "11\n21", "no infinite recursion (node_A)"
    assert _rows(node_b) == "11\n21", "no infinite recursion (node_B)"

    node_a.safe_psql("DELETE FROM {};".format(_TAB))
    node_a.wait_for_catchup(_BA)
    node_b.wait_for_catchup(_AB)
    assert _rows(node_a) == "", "Check existing data"
    assert _rows(node_b) == "", "Check existing data"

    # node_C -> node_B; its data must not reach node_A (origin = none).
    node_c.safe_psql("CREATE TABLE {} (a int PRIMARY KEY)".format(_TAB))
    node_c.safe_psql("CREATE PUBLICATION tap_pub_C FOR TABLE {}".format(_TAB))
    node_b.safe_psql(
        "CREATE SUBSCRIPTION {0} CONNECTION '{1} application_name={0}' "
        "PUBLICATION tap_pub_C WITH (origin = none)".format(_BC, c_connstr)
    )
    node_b.wait_for_subscription_sync(node_c, _BC)

    node_c.safe_psql("INSERT INTO {} VALUES (32);".format(_TAB))
    node_c.wait_for_catchup(_BC)
    node_b.wait_for_catchup(_AB)
    node_a.wait_for_catchup(_BA)
    assert _rows(node_b) == "32", "node_C data replicated to node_B"
    assert _rows(node_a) == "", "remote data from another node not replicated"


def _check_conflicts(node_a, node_b, node_c):
    node_b.safe_psql("DELETE FROM {};".format(_TAB))
    node_a.safe_psql("INSERT INTO {} VALUES (32);".format(_TAB))
    node_a.wait_for_catchup(_BA)
    node_b.wait_for_catchup(_AB)
    assert _rows(node_b) == "32", "node_A data replicated to node_B"

    node_c.safe_psql("UPDATE {} SET a = 33 WHERE a = 32;".format(_TAB))
    node_b.wait_for_log(
        r'conflict detected on relation "public.' + _TAB_UNQUOTED + r'": '
        r"conflict=update_origin_differs.*\n.*DETAIL:.* Updating the row that "
        r'was modified by a different origin ".*" in transaction [0-9]+ at .*: '
        r"local row \(32\), remote row \(33\), replica identity \(a\)=\(32\)\."
    )

    node_b.safe_psql("DELETE FROM {};".format(_TAB))
    node_a.safe_psql("INSERT INTO {} VALUES (33);".format(_TAB))
    node_a.wait_for_catchup(_BA)
    node_b.wait_for_catchup(_AB)
    assert _rows(node_b) == "33", "node_A data replicated to node_B"

    node_c.safe_psql("DELETE FROM {} WHERE a = 33;".format(_TAB))
    node_b.wait_for_log(
        r'conflict detected on relation "public.' + _TAB_UNQUOTED + r'": '
        r"conflict=delete_origin_differs.*\n.*DETAIL:.* Deleting the row that "
        r'was modified by a different origin ".*" in transaction [0-9]+ at .*: '
        r"local row \(33\), replica identity \(a\)=\(33\).*"
    )


def _check_origin_warnings(node_a, node_b, b_connstr):
    result = node_a.psql_capture(
        "CREATE SUBSCRIPTION {0} CONNECTION '{1} application_name={0}' "
        "PUBLICATION tap_pub_B WITH (origin = none, copy_data = on)".format(
            _AB2, b_connstr
        )
    )
    assert re.search(
        _warn_copy("tap_sub_a_b_2"), result.stderr
    ), "warn on copy_data with origin=none when publisher subscribes same table"
    node_a.wait_for_subscription_sync(node_b, _AB2)

    node_a.safe_psql("ALTER SUBSCRIPTION {} REFRESH PUBLICATION".format(_AB2))

    node_a.safe_psql("CREATE TABLE tab_new (a int PRIMARY KEY)")
    node_b.safe_psql("CREATE TABLE tab_new (a int PRIMARY KEY)")
    node_a.safe_psql("ALTER PUBLICATION tap_pub_A ADD TABLE tab_new")
    node_b.safe_psql("ALTER SUBSCRIPTION {} REFRESH PUBLICATION".format(_BA))
    node_b.wait_for_subscription_sync(node_a, _BA)
    node_b.safe_psql("ALTER PUBLICATION tap_pub_B ADD TABLE tab_new")

    result = node_a.psql_capture(
        "ALTER SUBSCRIPTION {} REFRESH PUBLICATION".format(_AB2)
    )
    assert re.search(
        _warn_copy("tap_sub_a_b_2"), result.stderr
    ), "warn on refresh when new table subscribes from a different publication"

    synced = (
        "SELECT count(1) = 0 FROM pg_subscription_rel WHERE srsubstate NOT IN ('r');"
    )
    assert node_a.poll_query_until(synced), "subscriber synchronized"
    node_b.wait_for_catchup(_AB2)

    node_a.safe_psql(
        "DROP TABLE tab_new;\nDROP SUBSCRIPTION {};\nDROP SUBSCRIPTION {};\n"
        "DROP PUBLICATION tap_pub_A;".format(_AB2, _AB)
    )
    node_b.safe_psql(
        "DROP TABLE tab_new;\nDROP SUBSCRIPTION {};\n"
        "DROP PUBLICATION tap_pub_B;".format(_BA)
    )


def _check_partition_warnings(node_a, node_b, node_c, a_connstr, b_connstr):
    node_a.safe_psql(
        "CREATE TABLE tab_part2(a int);\n"
        "CREATE PUBLICATION tap_pub_A FOR TABLE tab_part2;"
    )
    node_b.safe_psql(
        "CREATE TABLE tab_main(a int) PARTITION BY RANGE(a);\n"
        "CREATE TABLE tab_part1 PARTITION OF tab_main FOR VALUES FROM (0) TO (5);\n"
        "CREATE TABLE tab_part2(a int) PARTITION BY RANGE(a);\n"
        "CREATE TABLE tab_part2_1 PARTITION OF tab_part2 FOR VALUES FROM (5) TO (10);\n"
        "ALTER TABLE tab_main ATTACH PARTITION tab_part2 FOR VALUES FROM (5) to (10);\n"
        "CREATE SUBSCRIPTION tap_sub_A_B CONNECTION '{}' "
        "PUBLICATION tap_pub_A;".format(a_connstr)
    )
    node_c.safe_psql("CREATE TABLE tab_main(a int);\nCREATE TABLE tab_part2_1(a int);")
    node_b.safe_psql(
        "CREATE PUBLICATION tap_pub_B FOR TABLE tab_main "
        "WITH (publish_via_partition_root);\n"
        "CREATE PUBLICATION tap_pub_B_2 FOR TABLE tab_part2_1;"
    )

    for pub, why in (
        ("tap_pub_B", "publisher's partition subscribes from a different origin"),
        ("tap_pub_B_2", "publisher's ancestor subscribes from a different origin"),
    ):
        result = node_c.psql_capture(
            "CREATE SUBSCRIPTION tap_sub_B_C CONNECTION '{}' PUBLICATION {} "
            "WITH (origin = none, copy_data = on);".format(b_connstr, pub)
        )
        assert re.search(_warn_copy("tap_sub_b_c"), result.stderr), why
        node_c.safe_psql("DROP SUBSCRIPTION tap_sub_B_C")

    node_b.safe_psql(
        "DROP SUBSCRIPTION tap_sub_A_B;\nDROP PUBLICATION tap_pub_B;\n"
        "DROP PUBLICATION tap_pub_B_2;\nDROP TABLE tab_main;"
    )
    node_a.safe_psql("DROP PUBLICATION tap_pub_A;\nDROP TABLE tab_part2;")


def test_origin(create_pg):
    """origin=none bidirectional replication, conflict detection, copy warnings."""
    node_a = create_pg("node_A", allows_streaming="logical")
    node_b = create_pg("node_B", allows_streaming="logical", start=False)
    node_b.append_conf("track_commit_timestamp = on")
    node_b.start()
    node_c = create_pg("node_C", allows_streaming="logical")

    a_connstr = node_a.connstr() + " dbname=postgres"
    b_connstr = node_b.connstr() + " dbname=postgres"
    c_connstr = node_c.connstr() + " dbname=postgres"

    _setup_bidir(node_a, node_b, a_connstr, b_connstr)
    _check_no_recursion_and_origin(node_a, node_b, node_c, c_connstr)
    _check_conflicts(node_a, node_b, node_c)

    # The remaining tests no longer exercise conflict detection.
    node_b.append_conf("track_commit_timestamp = off")
    node_b.restart()

    _check_origin_warnings(node_a, node_b, b_connstr)
    _check_partition_warnings(node_a, node_b, node_c, a_connstr, b_connstr)

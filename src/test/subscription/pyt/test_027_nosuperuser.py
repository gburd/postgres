# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/subscription/t/027_nosuperuser.pl.

Logical replication respects permissions: superuser vs role privileges, RLS,
table-owner privileges, apply-worker restart on superuser revocation, and the
password_required connection-string requirement for non-superusers.
"""

import os
import re

_TBL = "alice.unpartitioned"


def _publish(publisher, sql):
    publisher.safe_psql("SET SESSION AUTHORIZATION regress_alice;\n" + sql)


def _agg(subscriber, tbl):
    return subscriber.safe_psql("SELECT COUNT(i), MIN(i), MAX(i) FROM {}".format(tbl))


def _expect_replication(publisher, subscriber, tbl, cnt, mn, mx, msg):
    publisher.wait_for_catchup("admin_sub")
    assert _agg(subscriber, tbl) == "{}|{}|{}".format(cnt, mn, mx), msg


def _expect_failure(subscriber, tbl, offset, cnt, mn, mx, pattern, msg):
    offset = subscriber.wait_for_log(pattern, offset)
    assert _agg(subscriber, tbl) == "{}|{}|{}".format(cnt, mn, mx), msg
    return offset


def _setup(publisher, subscriber, connstr):
    for node, rem_a, rem_b in ((publisher, 0, 1), (subscriber, 1, 0)):
        node.safe_psql(
            "CREATE ROLE regress_admin SUPERUSER LOGIN;\n"
            "CREATE ROLE regress_alice NOSUPERUSER LOGIN;\n"
            "GRANT CREATE ON DATABASE postgres TO regress_alice;\n"
            "GRANT PG_CREATE_SUBSCRIPTION TO regress_alice;\n"
            "SET SESSION AUTHORIZATION regress_alice;\n"
            "CREATE SCHEMA alice;\n"
            "GRANT USAGE ON SCHEMA alice TO regress_admin;\n"
            "CREATE TABLE alice.unpartitioned (i INTEGER);\n"
            "ALTER TABLE alice.unpartitioned REPLICA IDENTITY FULL;\n"
            "GRANT SELECT ON TABLE alice.unpartitioned TO regress_admin;\n"
            "CREATE TABLE alice.hashpart (i INTEGER) PARTITION BY HASH (i);\n"
            "ALTER TABLE alice.hashpart REPLICA IDENTITY FULL;\n"
            "GRANT SELECT ON TABLE alice.hashpart TO regress_admin;\n"
            "CREATE TABLE alice.hashpart_a PARTITION OF alice.hashpart "
            "FOR VALUES WITH (MODULUS 2, REMAINDER {});\n"
            "ALTER TABLE alice.hashpart_a REPLICA IDENTITY FULL;\n"
            "CREATE TABLE alice.hashpart_b PARTITION OF alice.hashpart "
            "FOR VALUES WITH (MODULUS 2, REMAINDER {});\n"
            "ALTER TABLE alice.hashpart_b REPLICA IDENTITY FULL;".format(rem_a, rem_b)
        )
    publisher.safe_psql(
        "SET SESSION AUTHORIZATION regress_alice;\n"
        "CREATE PUBLICATION alice FOR TABLE alice.unpartitioned, alice.hashpart "
        "WITH (publish_via_partition_root = true);"
    )
    subscriber.safe_psql(
        "SET SESSION AUTHORIZATION regress_admin;\n"
        "CREATE SUBSCRIPTION admin_sub CONNECTION '{}' PUBLICATION alice "
        "WITH (password_required=false);".format(connstr)
    )
    subscriber.wait_for_subscription_sync(publisher, "admin_sub")


def _rls_and_owner_privs(publisher, subscriber, offset):
    perm_denied = r"ERROR: ( [A-Z0-9]+:)? permission denied for table unpartitioned"
    rls = (
        r'ERROR: ( [A-Z0-9]+:)? user "regress_alice" cannot replicate into relation '
        r'with row-level security enabled: "unpartitioned\w*"'
    )
    subscriber.safe_psql(
        "SET SESSION AUTHORIZATION regress_alice;\n"
        "ALTER TABLE alice.unpartitioned ENABLE ROW LEVEL SECURITY;\n"
        "ALTER TABLE alice.unpartitioned FORCE ROW LEVEL SECURITY;"
    )
    _publish(publisher, "INSERT INTO {} (i) VALUES (15);".format(_TBL))
    offset = _expect_failure(
        subscriber, _TBL, offset, 2, 11, 13, rls, "insert into forced-rls table fails"
    )
    subscriber.safe_psql("ALTER TABLE alice.unpartitioned NO FORCE ROW LEVEL SECURITY;")
    _expect_replication(
        publisher, subscriber, _TBL, 3, 11, 15, "insert replicates if rls not forced"
    )
    subscriber.safe_psql("ALTER TABLE alice.unpartitioned FORCE ROW LEVEL SECURITY;")
    _publish(publisher, "UPDATE {} SET i = 17 WHERE i = 11;".format(_TBL))
    offset = _expect_failure(
        subscriber, _TBL, offset, 3, 11, 15, rls, "update into forced-rls table fails"
    )
    subscriber.safe_psql("ALTER TABLE alice.unpartitioned NO FORCE ROW LEVEL SECURITY;")
    _expect_replication(
        publisher, subscriber, _TBL, 3, 13, 17, "update replicates if rls not forced"
    )

    subscriber.safe_psql(
        "REVOKE SELECT, INSERT ON alice.unpartitioned FROM regress_alice;"
    )
    _publish(publisher, "INSERT INTO {} (i) VALUES (19);".format(_TBL))
    offset = _expect_failure(
        subscriber,
        _TBL,
        offset,
        3,
        13,
        17,
        perm_denied,
        "insert fails without owner insert",
    )
    subscriber.safe_psql("GRANT INSERT ON alice.unpartitioned TO regress_alice;")
    _expect_replication(
        publisher, subscriber, _TBL, 4, 13, 19, "restoring insert permits replication"
    )

    subscriber.safe_psql(
        "REVOKE UPDATE, DELETE ON alice.unpartitioned FROM regress_alice;"
    )
    _publish(publisher, "UPDATE {} SET i = 21 WHERE i = 13;".format(_TBL))
    _publish(publisher, "DELETE FROM {} WHERE i = 15;".format(_TBL))
    offset = _expect_failure(
        subscriber,
        _TBL,
        offset,
        4,
        13,
        19,
        perm_denied,
        "update/delete fails without perm",
    )
    subscriber.safe_psql(
        "GRANT UPDATE, DELETE ON alice.unpartitioned TO regress_alice;"
    )
    offset = _expect_failure(
        subscriber,
        _TBL,
        offset,
        4,
        13,
        19,
        perm_denied,
        "update/delete fails without SELECT",
    )
    subscriber.safe_psql("GRANT SELECT ON alice.unpartitioned TO regress_alice;")
    _expect_replication(
        publisher, subscriber, _TBL, 3, 17, 21, "restoring SELECT permits replication"
    )
    return offset


def _password_required(create_pg):
    """A non-superuser sub owner must give a password in the connection string."""
    publisher = create_pg("publisher1", allows_streaming="logical")
    subscriber = create_pg("subscriber1")
    base = publisher.connstr() + " user=regress_test_user dbname=postgres"
    connstr1 = base
    connstr2 = base + " password=secret"

    for node in (publisher, subscriber):
        node.safe_psql(
            "CREATE ROLE regress_test_user PASSWORD 'secret' LOGIN REPLICATION;\n"
            "GRANT CREATE ON DATABASE postgres TO regress_test_user;\n"
            "GRANT PG_CREATE_SUBSCRIPTION TO regress_test_user;"
        )
    publisher.safe_psql(
        "SET SESSION AUTHORIZATION regress_test_user;\n"
        "CREATE PUBLICATION regress_test_pub;"
    )
    subscriber.safe_psql(
        "CREATE SUBSCRIPTION regress_test_sub CONNECTION '{}' "
        "PUBLICATION regress_test_pub;".format(connstr1)
    )
    subscriber.wait_for_subscription_sync(publisher, "regress_test_sub")

    save_pw = os.environ.get("PGPASSWORD")
    os.environ["PGPASSWORD"] = "secret"
    try:
        # Require a password for regress_test_user on the publisher.
        with open(publisher.datadir / "pg_hba.conf", "w", encoding="utf-8") as hba:
            hba.write("local all regress_test_user md5\n")
        publisher.reload()
        subscriber.safe_psql(
            "ALTER SUBSCRIPTION regress_test_sub OWNER TO regress_test_user;"
        )

        result = subscriber.psql_capture(
            "SET SESSION AUTHORIZATION regress_test_user;\n"
            "ALTER SUBSCRIPTION regress_test_sub REFRESH PUBLICATION;"
        )
        assert result.exit_code != 0, "non-superuser owner without password fails"
        assert re.search(
            r"DETAIL:  Non-superusers must provide a password in the connection "
            r"string\.",
            result.stderr,
        ), "error requires a password in the connection string"
    finally:
        if save_pw is None:
            os.environ.pop("PGPASSWORD", None)
        else:
            os.environ["PGPASSWORD"] = save_pw

    result = subscriber.psql_capture(
        "SET SESSION AUTHORIZATION regress_test_user;\n"
        "ALTER SUBSCRIPTION regress_test_sub CONNECTION '{}';\n"
        "ALTER SUBSCRIPTION regress_test_sub REFRESH PUBLICATION;".format(connstr2)
    )
    assert result.exit_code == 0, "refresh succeeds once the password is supplied"


def test_nosuperuser(create_pg):
    """Permission, RLS and password-required semantics for logical replication."""
    publisher = create_pg("publisher", allows_streaming="logical")
    subscriber = create_pg("subscriber")
    connstr = publisher.connstr() + " dbname=postgres"
    _setup(publisher, subscriber, connstr)
    offset = 0

    # Superuser admin can replicate.
    _publish(publisher, "INSERT INTO {} (i) VALUES (1);".format(_TBL))
    _publish(publisher, "INSERT INTO {} (i) VALUES (3);".format(_TBL))
    _publish(publisher, "INSERT INTO {} (i) VALUES (5);".format(_TBL))
    _publish(publisher, "UPDATE {} SET i = 7 WHERE i = 1;".format(_TBL))
    _publish(publisher, "DELETE FROM {} WHERE i = 3;".format(_TBL))
    _expect_replication(publisher, subscriber, _TBL, 2, 5, 7, "superuser replicates")

    # Revoke superuser: SET ROLE fails until restored.
    subscriber.safe_psql("ALTER ROLE regress_admin NOSUPERUSER")
    _publish(publisher, "UPDATE {} SET i = 9 WHERE i = 5;".format(_TBL))
    offset = _expect_failure(
        subscriber,
        _TBL,
        offset,
        2,
        5,
        7,
        r'ERROR: ( [A-Z0-9]+:)? role "regress_admin" cannot SET ROLE to '
        r'"regress_alice"',
        "non-superuser admin fails to replicate update",
    )
    subscriber.safe_psql("ALTER ROLE regress_admin SUPERUSER")
    _expect_replication(
        publisher, subscriber, _TBL, 2, 7, 9, "restored superuser replicates update"
    )

    # Privileges on the target role suffice for a non-superuser.
    subscriber.safe_psql(
        "ALTER ROLE regress_admin NOSUPERUSER;\nGRANT regress_alice TO regress_admin;"
    )
    _publish(publisher, "INSERT INTO {} (i) VALUES (11);".format(_TBL))
    _expect_replication(publisher, subscriber, _TBL, 3, 7, 11, "nosuperuser INSERT")
    _publish(publisher, "UPDATE {} SET i = 13 WHERE i = 7;".format(_TBL))
    _expect_replication(publisher, subscriber, _TBL, 3, 9, 13, "nosuperuser UPDATE")
    _publish(publisher, "DELETE FROM {} WHERE i = 9;".format(_TBL))
    _expect_replication(publisher, subscriber, _TBL, 2, 11, 13, "nosuperuser DELETE")

    # Partitioned table.
    _publish(publisher, "INSERT INTO alice.hashpart (i) VALUES (101);")
    _publish(publisher, "INSERT INTO alice.hashpart (i) VALUES (102);")
    _publish(publisher, "INSERT INTO alice.hashpart (i) VALUES (103);")
    _publish(publisher, "UPDATE alice.hashpart SET i = 120 WHERE i = 102;")
    _publish(publisher, "DELETE FROM alice.hashpart WHERE i = 101;")
    _expect_replication(
        publisher,
        subscriber,
        "alice.hashpart",
        2,
        103,
        120,
        "nosuperuser into hashpart",
    )

    offset = _rls_and_owner_privs(publisher, subscriber, offset)

    # Apply worker restarts when the subscription owner loses superuser.
    subscriber.safe_psql("ALTER ROLE regress_alice SUPERUSER")
    subscriber.safe_psql(
        "SET SESSION AUTHORIZATION regress_alice;\n"
        "CREATE SUBSCRIPTION regression_sub CONNECTION '{}' PUBLICATION alice;".format(
            connstr
        )
    )
    subscriber.wait_for_subscription_sync(publisher, "regression_sub")
    offset = subscriber.current_log_position()
    subscriber.safe_psql("ALTER ROLE regress_alice NOSUPERUSER")
    subscriber.wait_for_log(
        r"LOG: ( [A-Z0-9]+:)? logical replication worker for subscription "
        r'"regression_sub" will restart because the subscription owner\'s '
        r"superuser privileges have been revoked",
        offset,
    )

    _password_required(create_pg)

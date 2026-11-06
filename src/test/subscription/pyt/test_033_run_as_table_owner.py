# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/subscription/t/033_run_as_table_owner.pl.

Logical replication respects permissions (run_as_owner and role privileges).
"""

_TBL = "alice.unpartitioned"
_PERM_DENIED = r"ERROR: ( [A-Z0-9]+:)? permission denied for table unpartitioned"


def _publish_insert(publisher, new_i):
    publisher.safe_psql(
        "SET SESSION AUTHORIZATION regress_alice;\n"
        "INSERT INTO {} (i) VALUES ({});".format(_TBL, new_i)
    )


def _publish_update(publisher, old_i, new_i):
    publisher.safe_psql(
        "SET SESSION AUTHORIZATION regress_alice;\n"
        "UPDATE {} SET i = {} WHERE i = {};".format(_TBL, new_i, old_i)
    )


def _publish_delete(publisher, old_i):
    publisher.safe_psql(
        "SET SESSION AUTHORIZATION regress_alice;\n"
        "DELETE FROM {} WHERE i = {};".format(_TBL, old_i)
    )


def _agg(subscriber):
    return subscriber.safe_psql("SELECT COUNT(i), MIN(i), MAX(i) FROM {}".format(_TBL))


def _expect_replication(publisher, subscriber, cnt, mn, mx, name):
    publisher.wait_for_catchup("admin_sub")
    assert _agg(subscriber) == "{}|{}|{}".format(cnt, mn, mx), name


def _expect_failure(subscriber, offset, cnt, mn, mx, name):
    offset = subscriber.wait_for_log(_PERM_DENIED, offset)
    assert _agg(subscriber) == "{}|{}|{}".format(cnt, mn, mx), name
    return offset


def _setup(publisher, subscriber, connstr):
    for node in (publisher, subscriber):
        node.safe_psql(
            "CREATE ROLE regress_admin SUPERUSER LOGIN;\n"
            "CREATE ROLE regress_admin2 SUPERUSER LOGIN;\n"
            "CREATE ROLE regress_alice NOSUPERUSER LOGIN;\n"
            "GRANT CREATE ON DATABASE postgres TO regress_alice;\n"
            "SET SESSION AUTHORIZATION regress_alice;\n"
            "CREATE SCHEMA alice;\n"
            "GRANT USAGE ON SCHEMA alice TO regress_admin;\n"
            "CREATE TABLE alice.unpartitioned (i INTEGER);\n"
            "ALTER TABLE alice.unpartitioned REPLICA IDENTITY FULL;\n"
            "GRANT SELECT ON TABLE alice.unpartitioned TO regress_admin;"
        )
    publisher.safe_psql(
        "SET SESSION AUTHORIZATION regress_alice;\n"
        "CREATE PUBLICATION alice FOR TABLE alice.unpartitioned "
        "WITH (publish_via_partition_root = true);"
    )
    subscriber.safe_psql(
        "SET SESSION AUTHORIZATION regress_admin;\n"
        "CREATE SUBSCRIPTION admin_sub CONNECTION '{}' PUBLICATION alice "
        "WITH (run_as_owner = true, password_required = false);".format(connstr)
    )
    subscriber.wait_for_subscription_sync(publisher, "admin_sub")


def test_run_as_table_owner(create_pg):
    """run_as_owner and table/role privileges gate logical replication."""
    publisher = create_pg("publisher", allows_streaming="logical")
    subscriber = create_pg("subscriber")
    connstr = publisher.connstr() + " dbname=postgres"
    _setup(publisher, subscriber, connstr)
    offset = 0

    # Superuser owner can replicate.
    _publish_insert(publisher, 1)
    _publish_insert(publisher, 3)
    _publish_insert(publisher, 5)
    _publish_update(publisher, 1, 7)
    _publish_delete(publisher, 3)
    _expect_replication(publisher, subscriber, 2, 5, 7, "superuser can replicate")

    # No privileges: replication fails.
    subscriber.safe_psql("ALTER ROLE regress_admin NOSUPERUSER")
    _publish_insert(publisher, 9)
    offset = _expect_failure(
        subscriber, offset, 2, 5, 7, "with no privileges cannot replicate"
    )

    # INSERT privilege (but not SELECT) lets INSERT replicate.
    subscriber.safe_psql(
        "ALTER ROLE regress_admin NOSUPERUSER;\n"
        "SET SESSION AUTHORIZATION regress_alice;\n"
        "GRANT INSERT,UPDATE,DELETE ON alice.unpartitioned TO regress_admin;\n"
        "REVOKE SELECT ON alice.unpartitioned FROM regress_admin;"
    )
    _expect_replication(
        publisher, subscriber, 3, 5, 9, "with INSERT privilege can replicate INSERT"
    )

    # No SELECT: UPDATE/DELETE cannot replicate.
    _publish_update(publisher, 5, 11)
    _publish_delete(publisher, 9)
    offset = _expect_failure(
        subscriber,
        offset,
        3,
        5,
        9,
        "without SELECT privilege cannot replicate UPDATE or DELETE",
    )

    # Grant SELECT: replication resumes.
    subscriber.safe_psql(
        "SET SESSION AUTHORIZATION regress_alice;\n"
        "GRANT SELECT ON alice.unpartitioned TO regress_admin;"
    )
    _expect_replication(
        publisher, subscriber, 2, 7, 11, "with all privileges can replicate"
    )

    # SET ROLE without INHERIT does not grant table privileges here.
    subscriber.safe_psql(
        "SET SESSION AUTHORIZATION regress_alice;\n"
        "REVOKE ALL PRIVILEGES ON alice.unpartitioned FROM regress_admin;\n"
        "RESET SESSION AUTHORIZATION;\n"
        "GRANT regress_alice TO regress_admin WITH INHERIT FALSE, SET TRUE;"
    )
    _publish_insert(publisher, 13)
    offset = _expect_failure(
        subscriber, offset, 2, 7, 11, "with SET ROLE but not INHERIT cannot replicate"
    )

    # INHERIT without SET ROLE works.
    subscriber.safe_psql(
        "GRANT regress_alice TO regress_admin WITH INHERIT TRUE, SET FALSE;"
    )
    _expect_replication(
        publisher, subscriber, 3, 7, 13, "with INHERIT but not SET ROLE can replicate"
    )

    # Back to SET ROLE only: fails again.
    subscriber.safe_psql(
        "SET SESSION AUTHORIZATION regress_alice;\n"
        "REVOKE ALL PRIVILEGES ON alice.unpartitioned FROM regress_admin;\n"
        "RESET SESSION AUTHORIZATION;\n"
        "GRANT regress_alice TO regress_admin WITH INHERIT FALSE, SET TRUE;"
    )
    _publish_insert(publisher, 14)
    offset = _expect_failure(
        subscriber, offset, 3, 7, 13, "with no privileges cannot replicate"
    )

    # run_as_owner = false: replication runs as the table owner.
    subscriber.safe_psql("ALTER SUBSCRIPTION admin_sub SET (run_as_owner = false);")
    _expect_replication(
        publisher,
        subscriber,
        4,
        7,
        14,
        "can replicate after setting run_as_owner false",
    )

    # Initial data sync as table owner (new subscription owned by admin2).
    subscriber.safe_psql("DROP SUBSCRIPTION admin_sub;\nTRUNCATE alice.unpartitioned;")
    subscriber.safe_psql(
        "SET SESSION AUTHORIZATION regress_admin2;\n"
        "CREATE SUBSCRIPTION admin_sub CONNECTION '{}' PUBLICATION alice "
        "WITH (run_as_owner = false, password_required = false, "
        "copy_data = true, enabled = false);".format(connstr)
    )
    subscriber.safe_psql("ALTER ROLE regress_admin2 NOSUPERUSER")
    subscriber.safe_psql(
        "GRANT regress_alice TO regress_admin2 WITH INHERIT FALSE, SET TRUE;\n"
        "ALTER SUBSCRIPTION admin_sub ENABLE;"
    )
    subscriber.wait_for_subscription_sync(publisher, "admin_sub")
    _expect_replication(
        publisher, subscriber, 4, 7, 14, "table owner can do the initial data copy"
    )

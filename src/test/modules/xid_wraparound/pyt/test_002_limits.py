# Copyright (c) 2023-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long
"""Port of src/test/modules/xid_wraparound/t/002_limits.pl.

XID exhaustion limits: with an old open transaction pinning the horizon,
consume XIDs until the server emits the "must be vacuumed within N
transactions" warning, then the "not accepting commands ... to avoid wraparound
data loss" stop-limit error. After the old transaction commits and VACUUM runs,
inserts succeed again. Gated on PG_TEST_EXTRA=xid_wraparound (slow).
"""

import re

import pypg

pytestmark = pypg.require_test_extras("xid_wraparound")


def test_002_limits(create_pg):
    """XID warn-limit and stop-limit fire, then VACUUM restores write access."""
    node = create_pg("wraparound", start=False)
    node.append_conf("\nautovacuum_naptime = 1s\nlog_autovacuum_min_duration = 0\n")
    node.start()
    node.safe_psql("CREATE EXTENSION xid_wraparound")
    node.safe_psql(
        "\nCREATE TABLE wraparoundtest(t text) WITH (autovacuum_enabled = off);\n"
        "INSERT INTO wraparoundtest VALUES ('start');\n"
    )
    psql_timeout_secs = 4 * pypg.test_timeout_default()
    background_psql = node.background_psql(
        "postgres", on_error_stop=False, timeout=psql_timeout_secs
    )
    background_psql.query_safe(
        "\n\tBEGIN;\n\tINSERT INTO wraparoundtest VALUES ('oldxact');\n"
    )
    node.safe_psql("SELECT consume_xids(1000000000)")
    node.safe_psql("INSERT INTO wraparoundtest VALUES ('after 1 billion')")
    node.safe_psql("SELECT consume_xids(1000000000)")
    node.safe_psql("INSERT INTO wraparoundtest VALUES ('after 2 billion')")
    warn_limit = 0
    for _ in range(1, 16):
        res = node.psql_capture("SELECT consume_xids(10000000)")
        assert res.exit_code == 0  # on_error_die => 1
        if re.search(
            r'WARNING:  database "postgres" must be vacuumed within [0-9]+ transactions',
            res.stderr,
        ):
            warn_limit = 1
            break
    assert warn_limit == 1, "warn-limit reached"
    node.safe_psql("INSERT INTO wraparoundtest VALUES ('reached warn-limit')")
    res = node.psql_capture("SELECT consume_xids(100000000)")
    assert re.search(
        r'ERROR:  database is not accepting commands that assign new transaction IDs to avoid wraparound data loss in database "postgres"',
        res.stderr,
    ), "stop-limit"
    background_psql.query_safe("COMMIT")
    background_psql.quit()
    node.safe_psql("VACUUM")
    assert node.poll_query_until(
        "INSERT INTO wraparoundtest VALUES ('after VACUUM')", "INSERT 0 1"
    )
    ret = node.safe_psql("SELECT * from wraparoundtest")
    assert ret == (
        "start\noldxact\nafter 1 billion\nafter 2 billion\n"
        "reached warn-limit\nafter VACUUM"
    )
    node.stop()

# Copyright (c) 2023-2026, PostgreSQL Global Development Group

"""Port of src/test/modules/xid_wraparound/t/004_notify_freeze.pl.

Pending async notifications survive XID freezing: a session LISTENs and holds an
open transaction while 10 NOTIFYs are queued and XIDs are consumed, then
vacuumdb --all --freeze advances datfrozenxid. After the listening session
commits, all 10 notifications must still be delivered in order. Gated on
PG_TEST_EXTRA=xid_wraparound.
"""

import re

import pypg

pytestmark = pypg.require_test_extras("xid_wraparound")


def test_004_notify_freeze(create_pg):
    """Queued notifications survive --freeze and are all delivered on commit."""
    node = create_pg("node")
    node.safe_psql("CREATE EXTENSION xid_wraparound")
    node.safe_psql("ALTER DATABASE template0 WITH ALLOW_CONNECTIONS true")
    psql_session1 = node.background_psql("postgres")
    psql_session1.query_safe("listen s;")
    psql_session1.query_safe("begin;")
    for i in range(1, 11):
        node.safe_psql("NOTIFY s, '{}'".format(i))
    node.safe_psql("select consume_xids(10000000);")
    node.safe_psql("select txid_current()")
    datafronzenxid = node.safe_psql(
        "select min(datfrozenxid::text::bigint) from pg_database"
    )
    node.command_ok(
        ["vacuumdb", "--all", "--freeze", "--port", str(node.port)],
        "vacuumdb --all --freeze",
    )
    datafronzenxid_freeze = node.safe_psql(
        "select min(datfrozenxid::text::bigint) from pg_database"
    )
    assert int(datafronzenxid_freeze) > int(datafronzenxid), "datfrozenxid advanced"
    res = psql_session1.query_safe("commit;")
    lines = res.split("\n")
    while lines and lines[-1] == "":
        lines.pop()
    notifications_count = 0
    for line in lines:
        notifications_count += 1
        assert re.search(
            r'Asynchronous notification "s" with payload "{}" received'.format(
                notifications_count
            ),
            line,
        )
    assert notifications_count == 10, "received all committed notifications"
    node.stop()

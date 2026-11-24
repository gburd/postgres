# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/subscription/t/020_messages.pl.

Logical decoding messages (transactional and non-transactional).
"""

_PEEK = """
SELECT get_byte(data, 0)
FROM pg_logical_slot_peek_binary_changes('tap_sub', NULL, NULL,
    'proto_version', '1', 'publication_names', 'tap_pub', 'messages', 'true')
"""


def test_messages(create_pg):
    """pg_logical_emit_message changes appear (or not) on the slot as expected."""
    publisher = create_pg("publisher", allows_streaming="logical", start=False)
    publisher.append_conf("autovacuum = off")
    publisher.start()
    subscriber = create_pg("subscriber")

    publisher.safe_psql("CREATE TABLE tab_test (a int primary key)")
    subscriber.safe_psql("CREATE TABLE tab_test (a int primary key)")

    connstr = publisher.connstr() + " dbname=postgres"
    publisher.safe_psql("CREATE PUBLICATION tap_pub FOR TABLE tab_test")
    subscriber.safe_psql(
        "CREATE SUBSCRIPTION tap_sub CONNECTION '{}' PUBLICATION tap_pub".format(
            connstr
        )
    )
    publisher.wait_for_catchup("tap_sub")

    # Disable the subscription and wait for the slot to go inactive.
    subscriber.safe_psql("ALTER SUBSCRIPTION tap_sub DISABLE")
    assert publisher.poll_query_until(
        "SELECT COUNT(*) FROM pg_catalog.pg_replication_slots "
        "WHERE slot_name = 'tap_sub' AND active='f'",
        expected="1",
    )

    publisher.safe_psql(
        "SELECT pg_logical_emit_message(true, 'pgoutput', 'a transactional message')"
    )

    # 66 77 67 == B M C == BEGIN MESSAGE COMMIT
    assert (
        publisher.safe_psql(_PEEK) == "66\n77\n67"
    ), "messages on slot are B M C with message option"

    result = publisher.safe_psql(
        """
        SELECT get_byte(data, 1), encode(substr(data, 11, 8), 'escape')
        FROM pg_logical_slot_peek_binary_changes('tap_sub', NULL, NULL,
            'proto_version', '1', 'publication_names', 'tap_pub',
            'messages', 'true')
        OFFSET 1 LIMIT 1
        """
    )
    assert (
        result == "1|pgoutput"
    ), "flag transactional is set to 1 and prefix is pgoutput"

    # Without the messages option, the empty transaction is optimized away.
    result = publisher.safe_psql(
        """
        SELECT get_byte(data, 0)
        FROM pg_logical_slot_get_binary_changes('tap_sub', NULL, NULL,
            'proto_version', '1', 'publication_names', 'tap_pub')
        """
    )
    assert result == "", "messages defaults to false so M is not available on slot"

    publisher.safe_psql("INSERT INTO tab_test VALUES (1)")
    message_lsn = publisher.safe_psql(
        "SELECT pg_logical_emit_message(false, 'pgoutput', "
        "'a non-transactional message')"
    )
    publisher.safe_psql("INSERT INTO tab_test VALUES (2)")

    result = publisher.safe_psql(
        """
        SELECT get_byte(data, 0), get_byte(data, 1)
        FROM pg_logical_slot_get_binary_changes('tap_sub', NULL, NULL,
            'proto_version', '1', 'publication_names', 'tap_pub',
            'messages', 'true')
        WHERE lsn = '{}' AND xid = 0
        """.format(
            message_lsn
        )
    )
    assert result == "77|0", "non-transactional message on slot is M"

    # A non-transactional message emitted inside an aborted transaction still
    # shows up once the LSN advances (forced via a WAL switch).
    publisher.safe_psql(
        """
BEGIN;
SELECT pg_logical_emit_message(false, 'pgoutput',
'a non-transactional message is available even if the transaction is aborted 1');
INSERT INTO tab_test VALUES (3);
SELECT pg_logical_emit_message(true, 'pgoutput',
'a transactional message is not available if the transaction is aborted');
SELECT pg_logical_emit_message(false, 'pgoutput',
'a non-transactional message is available even if the transaction is aborted 2');
ROLLBACK;
SELECT pg_switch_wal();
"""
    )

    result = publisher.safe_psql(
        """
        SELECT get_byte(data, 0), get_byte(data, 1)
        FROM pg_logical_slot_peek_binary_changes('tap_sub', NULL, NULL,
            'proto_version', '1', 'publication_names', 'tap_pub',
            'messages', 'true')
        """
    )
    assert (
        result == "77|0\n77|0"
    ), "non-transactional message on slot from aborted transaction is M"

    subscriber.stop("fast")
    publisher.stop("fast")

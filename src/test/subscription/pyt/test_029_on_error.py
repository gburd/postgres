# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/subscription/t/029_on_error.pl.

disable_on_error and ALTER SUBSCRIPTION ... SKIP transaction features.
"""

import re

from pypg import slurp_file

# Matches the apply-conflict error block, capturing the error transaction's
# finish LSN.
_ERROR_LSN = (
    r'conflict detected on relation "public.tbl".*\n'
    r".*DETAIL:.* Could not apply remote change.*\n"
    r'.*Key already exists in unique index "tbl_pkey", modified by .*origin.* '
    r"in transaction \d+ at .*: key .*, local row .*\n"
    r'.*CONTEXT:.* for replication target relation "public.tbl" in '
    r"transaction \d+, finished at ([0-9A-Fa-f]+/[0-9A-Fa-f]+)"
)


def _test_skip_lsn(publisher, subscriber, offset, nonconflict_data, expected, msg):
    # Wait until a conflict disables the subscription.
    subscriber.poll_query_until(
        "SELECT subenabled = FALSE FROM pg_subscription WHERE subname = 'sub'"
    )

    # Get the finish LSN of the error transaction from the server log.
    match = re.search(_ERROR_LSN, slurp_file(subscriber.log, offset))
    assert match, "could not get error-LSN"
    lsn = match.group(1)

    subscriber.safe_psql("ALTER SUBSCRIPTION sub SKIP (lsn = '{}')".format(lsn))
    subscriber.safe_psql("ALTER SUBSCRIPTION sub ENABLE")
    subscriber.poll_query_until(
        "SELECT subskiplsn = '0/0' FROM pg_subscription WHERE subname = 'sub'"
    )

    offset = subscriber.wait_for_log(
        r"LOG: ( [A-Z0-9]+:)? logical replication completed skipping "
        r"transaction at LSN " + re.escape(lsn),
        offset,
    )

    publisher.safe_psql("INSERT INTO tbl VALUES {}".format(nonconflict_data))
    publisher.wait_for_catchup("sub")
    assert subscriber.safe_psql("SELECT count(*) FROM tbl") == expected, msg
    return offset


def test_on_error(create_pg):
    """disable_on_error then SKIP across normal, 2PC, and streamed conflicts."""
    publisher = create_pg("publisher", allows_streaming="logical", start=False)
    publisher.append_conf(
        "logical_decoding_work_mem = 64kB\nmax_prepared_transactions = 10"
    )
    publisher.start()
    subscriber = create_pg("subscriber", start=False)
    subscriber.append_conf(
        "max_prepared_transactions = 10\ntrack_commit_timestamp = on"
    )
    subscriber.start()

    # The subscriber has a primary key and a preexisting conflicting row.
    publisher.safe_psql(
        "CREATE TABLE tbl (i INT, t BYTEA);\n"
        "ALTER TABLE tbl REPLICA IDENTITY FULL;\n"
        "INSERT INTO tbl VALUES (1, NULL);"
    )
    subscriber.safe_psql(
        "CREATE TABLE tbl (i INT PRIMARY KEY, t BYTEA);\n"
        "INSERT INTO tbl VALUES (1, NULL);"
    )

    connstr = publisher.connstr() + " dbname=postgres"
    publisher.safe_psql("CREATE PUBLICATION pub FOR TABLE tbl")
    subscriber.safe_psql(
        "CREATE SUBSCRIPTION sub CONNECTION '{}' PUBLICATION pub "
        "WITH (disable_on_error = true, streaming = on, two_phase = on)".format(connstr)
    )

    # Initial-sync uniqueness violation disables the subscription.
    subscriber.poll_query_until(
        "SELECT subenabled = false FROM pg_catalog.pg_subscription "
        "WHERE subname = 'sub'"
    )
    subscriber.safe_psql("TRUNCATE tbl")
    subscriber.safe_psql("ALTER SUBSCRIPTION sub ENABLE")
    subscriber.wait_for_subscription_sync(publisher, "sub")
    assert (
        subscriber.safe_psql("SELECT COUNT(*) FROM tbl") == "1"
    ), "subscription sub replicated data"

    offset = 0
    publisher.safe_psql("BEGIN;\nINSERT INTO tbl VALUES (1, NULL);\nCOMMIT;")
    offset = _test_skip_lsn(
        publisher, subscriber, offset, "(2, NULL)", "2", "test skipping transaction"
    )

    publisher.safe_psql(
        "BEGIN;\nUPDATE tbl SET i = 2;\nPREPARE TRANSACTION 'gtx';\n"
        "COMMIT PREPARED 'gtx';"
    )
    offset = _test_skip_lsn(
        publisher,
        subscriber,
        offset,
        "(3, NULL)",
        "3",
        "test skipping prepare and commit prepared ",
    )

    publisher.safe_psql(
        "BEGIN;\n"
        "INSERT INTO tbl SELECT i, sha256(i::text::bytea) "
        "FROM generate_series(1, 10000) s(i);\n"
        "COMMIT;"
    )
    _test_skip_lsn(
        publisher,
        subscriber,
        offset,
        "(4, sha256(4::text::bytea))",
        "4",
        "test skipping stream-commit",
    )

    assert (
        subscriber.safe_psql("SELECT COUNT(*) FROM pg_prepared_xacts") == "0"
    ), "check all prepared transactions are resolved on the subscriber"

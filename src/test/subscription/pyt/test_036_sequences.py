# Copyright (c) 2025-2026, PostgreSQL Global Development Group

"""Port of src/test/subscription/t/036_sequences.pl.

Sequences are synced correctly to the subscriber, including REFRESH
PUBLICATION / REFRESH SEQUENCES semantics and mismatch/missing-sequence
warnings.
"""

_SYNCED = "SELECT count(1) = 0 FROM pg_subscription_rel WHERE srsubstate NOT IN ('r');"
_QUOTE = '"regress\'quote"'


def _seq(node, name):
    return node.safe_psql("SELECT last_value, is_called FROM {}".format(name))


def test_sequences(create_pg):
    """Initial sync, REFRESH PUBLICATION/SEQUENCES, and mismatch warnings."""
    publisher = create_pg("publisher", allows_streaming="logical")
    subscriber = create_pg("subscriber")

    publisher.safe_psql(
        "CREATE TABLE regress_seq_test (v BIGINT);\n"
        "CREATE SEQUENCE regress_s1;\n"
        'CREATE SEQUENCE "regress\'quote";'
    )
    subscriber.safe_psql(
        "CREATE TABLE regress_seq_test (v BIGINT);\n"
        "CREATE SEQUENCE regress_s1;\n"
        "CREATE SEQUENCE regress_s2;\n"
        "CREATE SEQUENCE regress_s3;\n"
        'CREATE SEQUENCE "regress\'quote";'
    )

    publisher.safe_psql(
        "INSERT INTO regress_seq_test SELECT nextval('regress_s1') "
        "FROM generate_series(1,100);\n"
        "INSERT INTO regress_seq_test SELECT nextval('\"regress''quote\"') "
        "FROM generate_series(1,100);"
    )

    connstr = publisher.connstr() + " dbname=postgres"
    publisher.safe_psql("CREATE PUBLICATION regress_seq_pub FOR ALL SEQUENCES")
    subscriber.safe_psql(
        "CREATE SUBSCRIPTION regress_seq_sub CONNECTION '{}' "
        "PUBLICATION regress_seq_pub".format(connstr)
    )
    assert subscriber.poll_query_until(_SYNCED), "subscriber synchronized"

    assert _seq(subscriber, "regress_s1") == "100|t", "initial test data replicated"
    assert _seq(subscriber, _QUOTE) == "100|t", "initial data for quoted sequence name"

    # REFRESH PUBLICATION syncs newly published sequences only.
    publisher.safe_psql(
        "CREATE SEQUENCE regress_s2;\n"
        "INSERT INTO regress_seq_test SELECT nextval('regress_s2') "
        "FROM generate_series(1,100);\n"
        "INSERT INTO regress_seq_test SELECT nextval('regress_s1') "
        "FROM generate_series(1,100);"
    )
    subscriber.safe_psql("ALTER SUBSCRIPTION regress_seq_sub REFRESH PUBLICATION;")
    assert subscriber.poll_query_until(_SYNCED), "subscriber synchronized"
    assert _seq(publisher, "regress_s1") == "200|t", "sequence value in the publisher"
    assert _seq(subscriber, "regress_s1") == "100|t", "REFRESH does not sync existing"
    assert _seq(subscriber, "regress_s2") == "100|t", "REFRESH syncs newly published"

    # REFRESH SEQUENCES re-syncs existing sequences but not newly added ones.
    publisher.safe_psql(
        "CREATE SEQUENCE regress_s3;\n"
        "INSERT INTO regress_seq_test SELECT nextval('regress_s3') "
        "FROM generate_series(1,100);\n"
        "INSERT INTO regress_seq_test SELECT nextval('regress_s2') "
        "FROM generate_series(1,100);"
    )
    subscriber.safe_psql("ALTER SUBSCRIPTION regress_seq_sub REFRESH SEQUENCES;")
    assert subscriber.poll_query_until(_SYNCED), "subscriber synchronized"
    assert _seq(subscriber, "regress_s1") == "200|t", "REFRESH SEQUENCES syncs existing"
    assert _seq(subscriber, "regress_s2") == "200|t", "REFRESH SEQUENCES syncs existing"
    assert _seq(subscriber, "regress_s3") == "1|f", "REFRESH SEQUENCES not new sequence"

    # REFRESH PUBLICATION (copy_data=false) does not sync the new sequence.
    subscriber.safe_psql(
        "ALTER SUBSCRIPTION regress_seq_sub REFRESH PUBLICATION "
        "WITH (copy_data = false);"
    )
    assert subscriber.poll_query_until(_SYNCED), "subscriber synchronized"
    assert _seq(subscriber, "regress_s3") == "1|f", "copy_data=false does not sync new"

    _test_warnings(publisher, subscriber, connstr)


def _test_warnings(publisher, subscriber, connstr):
    """REFRESH PUBLICATION warns on mismatched/missing sequences."""
    publisher.safe_psql("CREATE SEQUENCE regress_s4 START 1 INCREMENT 2;")
    subscriber.safe_psql("CREATE SEQUENCE regress_s4 START 10 INCREMENT 2;")

    offset = subscriber.current_log_position()
    subscriber.safe_psql("ALTER SUBSCRIPTION regress_seq_sub REFRESH PUBLICATION")
    subscriber.wait_for_log(
        r"WARNING: ( [A-Z0-9]+:)? mismatched or renamed sequence on subscriber "
        r'\("public.regress_s4"\)',
        offset,
    )
    publisher.safe_psql("DROP SEQUENCE regress_s4;")
    subscriber.wait_for_log(
        r"WARNING: ( [A-Z0-9]+:)? missing sequence on publisher "
        r'\("public.regress_s4"\)',
        offset,
    )
    publisher.safe_psql("CREATE SEQUENCE regress_s4 START 10 INCREMENT 2;")

    # Insufficient privileges on a sequence must not disrupt the subscriber: it
    # logs a warning and keeps retrying. (The Perl original grants connectivity
    # for regress_seq_repl via init auth_extra, which on Unix sockets is a
    # no-op; under peer auth we grant it a trust line on the publisher.)
    publisher.safe_psql(
        "CREATE ROLE regress_seq_repl LOGIN REPLICATION;\n"
        "GRANT USAGE ON SCHEMA public TO regress_seq_repl;\n"
        "GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO regress_seq_repl;\n"
        "REVOKE ALL ON SEQUENCE regress_s2 FROM regress_seq_repl;"
    )
    with publisher.reloading() as session:
        session.hba.prepend("local all regress_seq_repl trust")

    limited = connstr + " user=regress_seq_repl"
    offset = subscriber.current_log_position()
    subscriber.safe_psql(
        "ALTER SUBSCRIPTION regress_seq_sub CONNECTION '{}'".format(limited)
    )
    subscriber.safe_psql("ALTER SUBSCRIPTION regress_seq_sub REFRESH SEQUENCES")
    subscriber.wait_for_log(
        r"WARNING: ( [A-Z0-9]+:)? missing sequence on publisher "
        r'\("public.regress_s2"\)',
        offset,
    )

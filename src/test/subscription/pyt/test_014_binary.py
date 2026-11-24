# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/subscription/t/014_binary.pl.

Binary mode logical replication.
"""

_DDL = """
CREATE TABLE public.test_numerical (
    a INTEGER PRIMARY KEY,
    b NUMERIC,
    c FLOAT,
    d BIGINT
    );
CREATE TABLE public.test_arrays (
    a INTEGER[] PRIMARY KEY,
    b NUMERIC[],
    c TEXT[]
    );
"""

_SYNC_CHECK = (
    "SELECT a, b, c, d FROM test_numerical ORDER BY a;\n"
    "SELECT a, b, c FROM test_arrays ORDER BY a;"
)


def _setup_custom_type(publisher, subscriber):
    """Custom type without binary send/recv first fails, then succeeds."""
    ddl = (
        "CREATE TYPE myvarchar;\n"
        "CREATE FUNCTION myvarcharin(cstring, oid, integer) RETURNS myvarchar "
        "LANGUAGE internal IMMUTABLE PARALLEL SAFE STRICT AS 'varcharin';\n"
        "CREATE FUNCTION myvarcharout(myvarchar) RETURNS cstring "
        "LANGUAGE internal IMMUTABLE PARALLEL SAFE STRICT AS 'varcharout';\n"
        "CREATE TYPE myvarchar (input = myvarcharin, output = myvarcharout);\n"
        "CREATE TABLE public.test_myvarchar (a myvarchar);"
    )
    publisher.safe_psql(ddl)
    subscriber.safe_psql(ddl)
    publisher.safe_psql("INSERT INTO public.test_myvarchar (a) VALUES ('a');")

    offset = subscriber.current_log_position()
    subscriber.safe_psql("ALTER SUBSCRIPTION tsub REFRESH PUBLICATION")
    subscriber.wait_for_log(
        r"ERROR: ( [A-Z0-9]+:)? no binary input function available for type", offset
    )

    sendrecv = (
        "CREATE FUNCTION myvarcharsend(myvarchar) RETURNS bytea "
        "LANGUAGE internal STABLE PARALLEL SAFE STRICT AS 'varcharsend';\n"
        "CREATE FUNCTION myvarcharrecv(internal, oid, integer) RETURNS myvarchar "
        "LANGUAGE internal STABLE PARALLEL SAFE STRICT AS 'varcharrecv';\n"
        "ALTER TYPE myvarchar SET (send = myvarcharsend, receive = myvarcharrecv);"
    )
    publisher.safe_psql(sendrecv)
    subscriber.safe_psql(sendrecv)
    subscriber.wait_for_subscription_sync(publisher, "tsub")
    assert (
        subscriber.safe_psql("SELECT a FROM test_myvarchar;") == "a"
    ), "check synced data on subscriber with custom type"


def _test_mismatched_types(publisher, subscriber):
    """Type mismatch fails in binary mode but syncs once binary is disabled."""
    publisher.safe_psql(
        "CREATE TABLE public.test_mismatching_types (a bigint PRIMARY KEY);\n"
        "INSERT INTO public.test_mismatching_types (a) VALUES (1), (2);"
    )
    offset = subscriber.current_log_position()
    subscriber.safe_psql(
        "CREATE TABLE public.test_mismatching_types (a int PRIMARY KEY);\n"
        "ALTER SUBSCRIPTION tsub REFRESH PUBLICATION;"
    )
    subscriber.wait_for_log(
        r"ERROR: ( [A-Z0-9]+:)? incorrect binary data format", offset
    )

    pub_offset = publisher.current_log_position()
    subscriber.safe_psql("ALTER SUBSCRIPTION tsub SET (binary = false);")
    publisher.wait_for_log(
        r"LOG: ( [A-Z0-9]+:)? statement: COPY (.+)? TO STDOUT\n", pub_offset
    )
    subscriber.wait_for_subscription_sync(publisher, "tsub")
    assert (
        subscriber.safe_psql("SELECT a FROM test_mismatching_types ORDER BY a;")
        == "1\n2"
    ), "check synced data on subscriber with binary = false"


def test_binary(create_pg):
    """Binary COPY and apply, format switching, custom types, type mismatch."""
    publisher = create_pg("publisher", allows_streaming="logical")
    subscriber = create_pg("subscriber")
    publisher.safe_psql(_DDL)
    subscriber.safe_psql(_DDL)
    publisher.safe_psql("CREATE PUBLICATION tpub FOR ALL TABLES")

    publisher.safe_psql(
        "INSERT INTO public.test_numerical (a, b, c, d) VALUES "
        "(1, 1.2, 1.3, 10), (2, 2.2, 2.3, 20);\n"
        "INSERT INTO public.test_arrays (a, b, c) VALUES "
        "('{1,2,3}', '{1.1, 1.2, 1.3}', '{\"one\", \"two\", \"three\"}'), "
        "('{3,1,2}', '{1.3, 1.1, 1.2}', '{\"three\", \"one\", \"two\"}');"
    )
    connstr = publisher.connstr() + " dbname=postgres"
    subscriber.safe_psql(
        "CREATE SUBSCRIPTION tsub CONNECTION '{}' PUBLICATION tpub "
        "WITH (slot_name = tpub_slot, binary = true)".format(connstr)
    )
    # The publisher's COPY must run in binary format.
    publisher.wait_for_log(
        r"LOG: ( [A-Z0-9]+:)? statement: COPY (.+)? TO STDOUT WITH \(FORMAT binary\)"
    )
    subscriber.wait_for_subscription_sync(publisher, "tsub")
    assert subscriber.safe_psql(_SYNC_CHECK) == (
        "1|1.2|1.3|10\n2|2.2|2.3|20\n"
        "{1,2,3}|{1.1,1.2,1.3}|{one,two,three}\n"
        "{3,1,2}|{1.3,1.1,1.2}|{three,one,two}"
    ), "check synced data on subscriber"

    # Binary apply.
    publisher.safe_psql(
        "INSERT INTO public.test_arrays (a, b, c) VALUES "
        "('{2,1,3}', '{1.2, 1.1, 1.3}', '{\"two\", \"one\", \"three\"}'), "
        "('{1,3,2}', '{1.1, 1.3, 1.2}', '{\"one\", \"three\", \"two\"}');\n"
        "INSERT INTO public.test_numerical (a, b, c, d) VALUES "
        "(3, 3.2, 3.3, 30), (4, 4.2, 4.3, 40);"
    )
    publisher.wait_for_catchup("tsub")
    assert (
        subscriber.safe_psql("SELECT a, b, c, d FROM test_numerical ORDER BY a")
        == "1|1.2|1.3|10\n2|2.2|2.3|20\n3|3.2|3.3|30\n4|4.2|4.3|40"
    ), "check replicated data on subscriber"

    publisher.safe_psql(
        "UPDATE public.test_arrays SET b[1] = 42, c = NULL;\n"
        "UPDATE public.test_numerical SET b = 42, c = NULL;"
    )
    publisher.wait_for_catchup("tsub")
    assert subscriber.safe_psql("SELECT a, b, c FROM test_arrays ORDER BY a") == (
        "{1,2,3}|{42,1.2,1.3}|\n{1,3,2}|{42,1.3,1.2}|\n"
        "{2,1,3}|{42,1.1,1.3}|\n{3,1,2}|{42,1.1,1.2}|"
    ), "check updated replicated data on subscriber"
    assert (
        subscriber.safe_psql("SELECT a, b, c, d FROM test_numerical ORDER BY a")
        == "1|42||10\n2|42||20\n3|42||30\n4|42||40"
    ), "check updated replicated data on subscriber"

    # Switch to text format and back to binary.
    subscriber.safe_psql("ALTER SUBSCRIPTION tsub SET (binary = false);")
    publisher.safe_psql(
        "INSERT INTO public.test_numerical (a, b, c, d) VALUES (5, 5.2, 5.3, 50);"
    )
    publisher.wait_for_catchup("tsub")
    assert (
        subscriber.safe_psql("SELECT a, b, c, d FROM test_numerical ORDER BY a")
        == "1|42||10\n2|42||20\n3|42||30\n4|42||40\n5|5.2|5.3|50"
    ), "check replicated data on subscriber"

    subscriber.safe_psql("ALTER SUBSCRIPTION tsub SET (binary = true);")
    publisher.safe_psql(
        "INSERT INTO public.test_arrays (a, b, c) VALUES "
        "('{2,3,1}', '{1.2, 1.3, 1.1}', '{\"two\", \"three\", \"one\"}');"
    )
    publisher.wait_for_catchup("tsub")
    assert subscriber.safe_psql("SELECT a, b, c FROM test_arrays ORDER BY a") == (
        "{1,2,3}|{42,1.2,1.3}|\n{1,3,2}|{42,1.3,1.2}|\n"
        "{2,1,3}|{42,1.1,1.3}|\n{2,3,1}|{1.2,1.3,1.1}|{two,three,one}\n"
        "{3,1,2}|{42,1.1,1.2}|"
    ), "check replicated data on subscriber"

    _setup_custom_type(publisher, subscriber)
    _test_mismatched_types(publisher, subscriber)

    subscriber.stop("fast")
    publisher.stop("fast")

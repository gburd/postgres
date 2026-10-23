# Copyright (c) 2022-2026, PostgreSQL Global Development Group

"""Port of src/test/subscription/t/032_subscribe_use_index.pl.

Logical replication apply uses an available index on the subscriber (REPLICA
IDENTITY FULL), across multi-column, partitioned, expression, unique and hash
index cases.
"""

_APP = "tap_sub"


def _pubsub(publisher, subscriber, connstr, table):
    publisher.safe_psql(
        "CREATE PUBLICATION tap_pub_rep_full FOR TABLE {}".format(table)
    )
    subscriber.safe_psql(
        "CREATE SUBSCRIPTION tap_sub_rep_full CONNECTION "
        "'{} application_name={}' PUBLICATION tap_pub_rep_full".format(connstr, _APP)
    )
    subscriber.wait_for_subscription_sync(publisher, _APP)


def _drop_pubsub(publisher, subscriber, table):
    publisher.safe_psql("DROP PUBLICATION tap_pub_rep_full")
    publisher.safe_psql("DROP TABLE {}".format(table))
    subscriber.safe_psql("DROP SUBSCRIPTION tap_sub_rep_full")
    subscriber.safe_psql("DROP TABLE {}".format(table))


def _idx_scan(indexrelname, value):
    return "select {} from pg_stat_all_indexes " "where indexrelname = '{}';".format(
        value, indexrelname
    )


def _multi_column(publisher, subscriber, connstr):
    publisher.safe_psql("CREATE TABLE test_replica_id_full (x int, y text)")
    publisher.safe_psql("ALTER TABLE test_replica_id_full REPLICA IDENTITY FULL")
    subscriber.safe_psql("CREATE TABLE test_replica_id_full (x int, y text)")
    subscriber.safe_psql(
        "CREATE INDEX test_replica_id_full_idx ON test_replica_id_full(x,y)"
    )
    publisher.safe_psql(
        "INSERT INTO test_replica_id_full SELECT (i%10), (i%10)::text "
        "FROM generate_series(0,10) i"
    )
    _pubsub(publisher, subscriber, connstr, "test_replica_id_full")
    publisher.safe_psql("DELETE FROM test_replica_id_full WHERE x IN (5, 6)")
    publisher.safe_psql(
        "UPDATE test_replica_id_full SET x = 100, y = '200' WHERE x IN (1, 2)"
    )
    publisher.wait_for_catchup(_APP)
    assert subscriber.poll_query_until(
        _idx_scan("test_replica_id_full_idx", "(idx_scan = 4)")
    ), "4 rows updated via index"
    assert (
        subscriber.safe_psql(
            "select count(*) from test_replica_id_full WHERE (x = 100 and y = '200')"
        )
        == "2"
    ), "correct data after UPDATE"
    assert (
        subscriber.safe_psql(
            "select count(*) from test_replica_id_full where x in (5, 6)"
        )
        == "0"
    ), "correct data after DELETE"
    _drop_pubsub(publisher, subscriber, "test_replica_id_full")


def _partitioned(publisher, subscriber, connstr):
    part_ddl = (
        "CREATE TABLE users_table_part(user_id bigint, value_1 int, value_2 int) "
        "PARTITION BY RANGE (value_1);\n"
        "CREATE TABLE users_table_part_0 PARTITION OF users_table_part "
        "FOR VALUES FROM (0) TO (10);\n"
        "CREATE TABLE users_table_part_1 PARTITION OF users_table_part "
        "FOR VALUES FROM (10) TO (20);"
    )
    publisher.safe_psql(part_ddl)
    for tab in ("users_table_part", "users_table_part_0", "users_table_part_1"):
        publisher.safe_psql("ALTER TABLE {} REPLICA IDENTITY FULL".format(tab))
    subscriber.safe_psql(part_ddl)
    subscriber.safe_psql(
        "CREATE INDEX users_table_part_idx ON users_table_part(user_id, value_1)"
    )
    publisher.safe_psql(
        "INSERT INTO users_table_part SELECT (i%100), (i%20), i "
        "FROM generate_series(0,100) i"
    )
    _pubsub(publisher, subscriber, connstr, "users_table_part")
    publisher.safe_psql("UPDATE users_table_part SET value_1 = 0 WHERE user_id = 4")
    publisher.safe_psql(
        "DELETE FROM users_table_part WHERE user_id = 1 and value_1 = 1"
    )
    publisher.safe_psql(
        "DELETE FROM users_table_part WHERE user_id = 12 and value_1 = 12"
    )
    publisher.wait_for_catchup(_APP)
    assert subscriber.poll_query_until(
        "select sum(idx_scan)=3 from pg_stat_all_indexes "
        "where indexrelname ilike 'users_table_part_%';"
    ), "partitioned table updates via index"
    assert (
        subscriber.safe_psql(
            "select sum(user_id+value_1+value_2) from users_table_part"
        )
        == "10907"
    ), "correct data"
    assert (
        subscriber.safe_psql(
            "select count(DISTINCT(user_id,value_1, value_2)) from users_table_part"
        )
        == "99"
    ), "correct data"
    _drop_pubsub(publisher, subscriber, "users_table_part")


def _expr_or_partial(publisher, subscriber, connstr):
    indexes = (
        "select sum(idx_scan) from pg_stat_all_indexes where indexrelname IN "
        "('people_names_expr_only', 'people_names_partial')"
    )
    publisher.safe_psql("CREATE TABLE people (firstname text, lastname text)")
    publisher.safe_psql("ALTER TABLE people REPLICA IDENTITY FULL")
    subscriber.safe_psql("CREATE TABLE people (firstname text, lastname text)")
    subscriber.safe_psql(
        "CREATE INDEX people_names_expr_only ON people "
        "((firstname || ' ' || lastname))"
    )
    subscriber.safe_psql(
        "CREATE INDEX people_names_partial ON people(firstname) "
        "WHERE (firstname = 'first_name_1')"
    )
    publisher.safe_psql(
        "INSERT INTO people SELECT 'first_name_' || i::text, "
        "'last_name_' || i::text FROM generate_series(0,200) i"
    )
    _pubsub(publisher, subscriber, connstr, "people")
    publisher.safe_psql(
        "UPDATE people SET firstname = 'no-name' WHERE firstname = 'first_name_1'"
    )
    publisher.safe_psql(
        "UPDATE people SET firstname = 'no-name' WHERE firstname = 'first_name_2' "
        "AND lastname = 'last_name_2'"
    )
    publisher.wait_for_catchup(_APP)
    assert subscriber.safe_psql(indexes) == "0", "expression/partial index not used"
    publisher.safe_psql("DELETE FROM people WHERE firstname = 'first_name_3'")
    publisher.safe_psql(
        "DELETE FROM people WHERE firstname = 'first_name_4' "
        "AND lastname = 'last_name_4'"
    )
    publisher.wait_for_catchup(_APP)
    assert subscriber.safe_psql(indexes) == "0", "expression/partial index not used"
    assert subscriber.safe_psql("SELECT count(*) FROM people") == "199", "correct data"
    _drop_pubsub(publisher, subscriber, "people")


def _expr_and_columns(publisher, subscriber, connstr):
    publisher.safe_psql("CREATE TABLE people (firstname text, lastname text)")
    publisher.safe_psql("ALTER TABLE people REPLICA IDENTITY FULL")
    subscriber.safe_psql("CREATE TABLE people (firstname text, lastname text)")
    subscriber.safe_psql(
        "CREATE INDEX people_names ON people "
        "(firstname, lastname, (firstname || ' ' || lastname))"
    )
    publisher.safe_psql(
        "INSERT INTO people SELECT 'first_name_' || i::text, "
        "'last_name_' || i::text FROM generate_series(0, 20) i"
    )
    _pubsub(publisher, subscriber, connstr, "people")
    publisher.safe_psql(
        "UPDATE people SET firstname = 'no-name' WHERE firstname = 'first_name_1'"
    )
    publisher.safe_psql("DELETE FROM people WHERE firstname = 'no-name'")
    publisher.wait_for_catchup(_APP)
    assert subscriber.poll_query_until(
        _idx_scan("people_names", "idx_scan=2")
    ), "two rows deleted via expression+columns index"
    assert subscriber.safe_psql("SELECT count(*) FROM people") == "20", "correct data"
    assert (
        subscriber.safe_psql("SELECT count(*) FROM people WHERE firstname = 'no-name'")
        == "0"
    ), "correct data"
    subscriber.safe_psql("DROP INDEX people_names")
    publisher.safe_psql("DELETE FROM people WHERE lastname = 'last_name_18'")
    publisher.wait_for_catchup(_APP)
    assert (
        subscriber.safe_psql(
            "SELECT count(*) FROM people WHERE lastname = 'last_name_18'"
        )
        == "0"
    ), "correct data via sequential scan"
    _drop_pubsub(publisher, subscriber, "people")


def _null_and_missing(publisher, subscriber, connstr):
    publisher.safe_psql("CREATE TABLE test_replica_id_full (x int)")
    publisher.safe_psql("ALTER TABLE test_replica_id_full REPLICA IDENTITY FULL")
    subscriber.safe_psql("CREATE TABLE test_replica_id_full (x int, y int)")
    subscriber.safe_psql(
        "CREATE INDEX test_replica_id_full_idx ON test_replica_id_full(x,y)"
    )
    _pubsub(publisher, subscriber, connstr, "test_replica_id_full")
    publisher.safe_psql("INSERT INTO test_replica_id_full VALUES (1), (2), (3)")
    publisher.safe_psql("UPDATE test_replica_id_full SET x = x + 1 WHERE x = 1")
    publisher.wait_for_catchup(_APP)
    assert subscriber.poll_query_until(
        _idx_scan("test_replica_id_full_idx", "idx_scan=1")
    ), "index used even with NULL values"
    assert (
        subscriber.safe_psql("select sum(x) from test_replica_id_full WHERE y IS NULL")
        == "7"
    ), "correct data"
    assert (
        subscriber.safe_psql(
            "select count(*) from test_replica_id_full WHERE y IS NULL"
        )
        == "3"
    ), "correct data"
    _drop_pubsub(publisher, subscriber, "test_replica_id_full")


def _unique_index(publisher, subscriber, connstr):
    publisher.safe_psql("CREATE TABLE test_replica_id_full (x int, y int)")
    publisher.safe_psql("ALTER TABLE test_replica_id_full REPLICA IDENTITY FULL")
    subscriber.safe_psql("CREATE TABLE test_replica_id_full (x int, y int)")
    subscriber.safe_psql(
        "CREATE UNIQUE INDEX test_replica_id_full_idxy ON test_replica_id_full(x,y)"
    )
    publisher.safe_psql(
        "INSERT INTO test_replica_id_full SELECT i, i FROM generate_series(0,21) i"
    )
    _pubsub(publisher, subscriber, connstr, "test_replica_id_full")
    subscriber.safe_psql(
        "INSERT INTO test_replica_id_full SELECT i+100, i FROM generate_series(0,21) i"
    )
    publisher.safe_psql("UPDATE test_replica_id_full SET x = 2000 WHERE y = 15")
    publisher.wait_for_catchup(_APP)
    assert subscriber.poll_query_until(
        _idx_scan("test_replica_id_full_idxy", "(idx_scan = 1)")
    ), "one row updated via unique index"
    assert (
        subscriber.safe_psql("SELECT count(*) FROM test_replica_id_full WHERE x = 2000")
        == "1"
    ), "only one row updated"
    _drop_pubsub(publisher, subscriber, "test_replica_id_full")


def _hash_index(publisher, subscriber, connstr):
    publisher.safe_psql("CREATE TABLE test_replica_id_full (x int, y text)")
    publisher.safe_psql("ALTER TABLE test_replica_id_full REPLICA IDENTITY FULL")
    subscriber.safe_psql("CREATE TABLE test_replica_id_full (x int, y text)")
    subscriber.safe_psql(
        "CREATE INDEX test_replica_id_full_idx ON test_replica_id_full USING HASH (x)"
    )
    publisher.safe_psql(
        "INSERT INTO test_replica_id_full SELECT i, (i%10)::text "
        "FROM generate_series(0,10) i"
    )
    _pubsub(publisher, subscriber, connstr, "test_replica_id_full")
    publisher.safe_psql("DELETE FROM test_replica_id_full WHERE x IN (5, 6)")
    publisher.safe_psql(
        "UPDATE test_replica_id_full SET x = 100, y = '200' WHERE x IN (1, 2)"
    )
    publisher.wait_for_catchup(_APP)
    assert subscriber.poll_query_until(
        _idx_scan("test_replica_id_full_idx", "(idx_scan = 4)")
    ), "rows deleted/updated via hash index"
    assert (
        subscriber.safe_psql(
            "select count(*) from test_replica_id_full WHERE (x = 100 and y = '200')"
        )
        == "2"
    ), "correct data after UPDATE"
    assert (
        subscriber.safe_psql(
            "select count(*) from test_replica_id_full where x in (5, 6)"
        )
        == "0"
    ), "correct data after DELETE"
    _drop_pubsub(publisher, subscriber, "test_replica_id_full")


def test_subscribe_use_index(create_pg):
    """Apply uses subscriber indexes across many index kinds and table shapes."""
    publisher = create_pg("publisher", allows_streaming="logical")
    subscriber = create_pg("subscriber")
    connstr = publisher.connstr() + " dbname=postgres"

    _multi_column(publisher, subscriber, connstr)
    _partitioned(publisher, subscriber, connstr)
    _expr_or_partial(publisher, subscriber, connstr)
    _expr_and_columns(publisher, subscriber, connstr)
    _null_and_missing(publisher, subscriber, connstr)
    _unique_index(publisher, subscriber, connstr)
    _hash_index(publisher, subscriber, connstr)

    subscriber.stop("fast")
    publisher.stop("fast")

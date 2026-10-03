# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/bin/scripts/t/011_clusterdb_all.pl."""


def test_clusterdb_all(create_pg):
    """clusterdb --all clusters every database and skips invalid ones."""
    node = create_pg("main")

    # clusterdb -a is not compatible with -d. This relies on PGDATABASE being
    # set, something the pg fixtures do.
    node.issues_sql_like(
        ["clusterdb", "--all"],
        r"(?s)statement: CLUSTER.*statement: CLUSTER",
        "cluster all databases",
    )

    node.safe_psql(
        "CREATE DATABASE regression_invalid;"
        " UPDATE pg_database SET datconnlimit = -2"
        " WHERE datname = 'regression_invalid';"
    )
    node.command_ok(
        ["clusterdb", "--all"], "invalid database not targeted by clusterdb -a"
    )

    # Doesn't quite belong here, but avoids creating an invalid database in
    # 010_clusterdb as well.
    node.command_fails_like(
        ["clusterdb", "--dbname", "regression_invalid"],
        r'FATAL:  cannot connect to invalid database "regression_invalid"',
        "clusterdb cannot target invalid database",
    )

    node.safe_psql(
        "CREATE TABLE test1 (a int); CREATE INDEX test1x ON test1 (a); "
        "CLUSTER test1 USING test1x"
    )
    node.safe_psql(
        "CREATE TABLE test1 (a int); CREATE INDEX test1x ON test1 (a); "
        "CLUSTER test1 USING test1x",
        dbname="template1",
    )
    node.issues_sql_like(
        ["clusterdb", "--all", "--table", "test1"],
        r"(?s)statement: CLUSTER public\.test1",
        "cluster specific table in all databases",
    )

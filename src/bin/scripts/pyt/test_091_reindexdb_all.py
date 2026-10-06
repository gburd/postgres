# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/bin/scripts/t/091_reindexdb_all.pl."""


def test_reindexdb_all(create_pg, monkeypatch):
    """reindexdb --all variants and handling of an invalid database."""
    node = create_pg("main")

    monkeypatch.setenv("PGOPTIONS", "--client-min-messages=WARNING")

    node.safe_psql("CREATE TABLE test1 (a int); CREATE INDEX test1x ON test1 (a);")
    node.safe_psql(
        "CREATE TABLE test1 (a int); CREATE INDEX test1x ON test1 (a);",
        dbname="template1",
    )
    node.issues_sql_like(
        ["reindexdb", "--all"],
        r"(?s)statement: REINDEX.*statement: REINDEX",
        "reindex all databases",
    )
    node.issues_sql_like(
        ["reindexdb", "--all", "--system"],
        r"(?s)statement: REINDEX SYSTEM postgres",
        "reindex system catalogs in all databases",
    )
    node.issues_sql_like(
        ["reindexdb", "--all", "--schema", "public"],
        r"(?s)statement: REINDEX SCHEMA public",
        "reindex schema in all databases",
    )
    node.issues_sql_like(
        ["reindexdb", "--all", "--index", "test1x"],
        r"(?s)statement: REINDEX INDEX public\.test1x",
        "reindex index in all databases",
    )
    node.issues_sql_like(
        ["reindexdb", "--all", "--table", "test1"],
        r"(?s)statement: REINDEX TABLE public\.test1",
        "reindex table in all databases",
    )

    node.safe_psql(
        "CREATE DATABASE regression_invalid;"
        " UPDATE pg_database SET datconnlimit = -2"
        " WHERE datname = 'regression_invalid';"
    )
    node.command_ok(
        ["reindexdb", "--all"], "invalid database not targeted by reindexdb --all"
    )

    node.command_fails_like(
        ["reindexdb", "--dbname", "regression_invalid"],
        r'FATAL:  cannot connect to invalid database "regression_invalid"',
        "reindexdb cannot target invalid database",
    )

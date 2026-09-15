# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/bin/scripts/t/010_clusterdb.pl."""


def test_clusterdb(pg_bin, create_pg):
    """clusterdb basics: SQL issued, nonexistent table, specific table, connstr."""
    pg_bin.program_help_ok("clusterdb")
    pg_bin.program_version_ok("clusterdb")
    pg_bin.program_options_handling_ok("clusterdb")

    node = create_pg("main")

    node.issues_sql_like(["clusterdb"], r"statement: CLUSTER;", "SQL CLUSTER run")

    node.command_fails_like(
        ["clusterdb", "--table", "nonexistent"],
        r'relation "nonexistent" does not exist',
        "fails with nonexistent table",
    )

    node.safe_psql(
        "CREATE TABLE test1 (a int); CREATE INDEX test1x ON test1 (a); "
        "CLUSTER test1 USING test1x"
    )
    node.issues_sql_like(
        ["clusterdb", "--table", "test1"],
        r"statement: CLUSTER public\.test1;",
        "cluster specific table",
    )

    node.command_ok(
        ["clusterdb", "--echo", "--verbose", "dbname=template1"],
        "clusterdb with connection string",
    )

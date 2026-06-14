# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/bin/scripts/t/050_dropdb.pl."""


def test_dropdb(pg_bin, create_pg):
    """dropdb basics, --force, nonexistent, and dropping an invalid database."""
    pg_bin.program_help_ok("dropdb")
    pg_bin.program_version_ok("dropdb")
    pg_bin.program_options_handling_ok("dropdb")

    node = create_pg("main")

    node.safe_psql("CREATE DATABASE foobar1")
    node.issues_sql_like(
        ["dropdb", "foobar1"],
        r"statement: DROP DATABASE foobar1",
        "SQL DROP DATABASE run",
    )

    node.safe_psql("CREATE DATABASE foobar2")
    node.issues_sql_like(
        ["dropdb", "--force", "foobar2"],
        r"statement: DROP DATABASE foobar2 WITH \(FORCE\);",
        "SQL DROP DATABASE (FORCE) run",
    )

    node.command_fails_like(
        ["dropdb", "nonexistent"],
        r'database "nonexistent" does not exist',
        "fails with nonexistent database",
    )

    # An invalid database can be dropped with dropdb.
    node.safe_psql(
        "CREATE DATABASE regression_invalid;"
        " UPDATE pg_database SET datconnlimit = -2"
        " WHERE datname = 'regression_invalid';"
    )
    node.command_ok(["dropdb", "regression_invalid"], "invalid database can be dropped")

# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/bin/scripts/t/101_vacuumdb_all.pl."""


def test_vacuumdb_all(create_pg):
    """vacuumdb --all vacuums every database and skips invalid ones."""
    node = create_pg("main")

    node.issues_sql_like(
        ["vacuumdb", "--all"],
        r"(?s)statement: VACUUM.*statement: VACUUM",
        "vacuum all databases",
    )

    node.safe_psql(
        "CREATE DATABASE regression_invalid;"
        " UPDATE pg_database SET datconnlimit = -2"
        " WHERE datname = 'regression_invalid';"
    )
    node.command_ok(
        ["vacuumdb", "--all"], "invalid database not targeted by vacuumdb -a"
    )

    node.command_fails_like(
        ["vacuumdb", "--dbname", "regression_invalid"],
        r'FATAL:  cannot connect to invalid database "regression_invalid"',
        "vacuumdb cannot target invalid database",
    )

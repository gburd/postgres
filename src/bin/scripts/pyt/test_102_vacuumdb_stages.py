# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/bin/scripts/t/102_vacuumdb_stages.pl."""

_STAGES_ONE_DB = (
    r"(?s)"
    r"statement: SET default_statistics_target=1; SET vacuum_cost_delay=0;"
    r".*statement: ANALYZE"
    r".*statement: SET default_statistics_target=10; RESET vacuum_cost_delay;"
    r".*statement: ANALYZE"
    r".*statement: RESET default_statistics_target;"
    r".*statement: ANALYZE"
)

_STAGES_ALL_DB = (
    r"(?s)"
    r"statement: SET default_statistics_target=1; SET vacuum_cost_delay=0;"
    r".*statement: ANALYZE"
    r".*statement: SET default_statistics_target=1; SET vacuum_cost_delay=0;"
    r".*statement: ANALYZE"
    r".*statement: SET default_statistics_target=10; RESET vacuum_cost_delay;"
    r".*statement: ANALYZE"
    r".*statement: SET default_statistics_target=10; RESET vacuum_cost_delay;"
    r".*statement: ANALYZE"
    r".*statement: RESET default_statistics_target;"
    r".*statement: ANALYZE"
    r".*statement: RESET default_statistics_target;"
    r".*statement: ANALYZE"
)


def test_vacuumdb_stages(create_pg):
    """vacuumdb --analyze-in-stages issues the staged ANALYZE sequence."""
    node = create_pg("main")

    node.issues_sql_like(
        ["vacuumdb", "--analyze-in-stages", "postgres"],
        _STAGES_ONE_DB,
        "analyze three times",
    )

    node.issues_sql_like(
        ["vacuumdb", "--analyze-in-stages", "--all"],
        _STAGES_ALL_DB,
        "analyze more than one database in stages",
    )

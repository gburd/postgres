# Copyright (c) 2022-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long,too-many-statements
"""Port of contrib/pg_stat_statements/t/010_restart.pl.

pg_stat_statements persistence: collected statements survive a server restart,
and are discarded when pg_stat_statements.save is turned off.
Generated from the Perl original via .agent/gen_golden.py.
"""


def test_010_restart(create_pg):
    """pg_stat_statements persistence: collected statements survive a server restart,."""
    node = create_pg("main", start=False)
    node.append_conf("shared_preload_libraries = 'pg_stat_statements'")
    node.start()
    node.safe_psql("CREATE EXTENSION pg_stat_statements")
    node.safe_psql("CREATE TABLE t1 (a int)")
    node.safe_psql("SELECT a FROM t1")
    assert (
        node.safe_psql(
            "SELECT query FROM pg_stat_statements WHERE query NOT LIKE '%pg_stat_statements%' ORDER BY query"
        )
        == "CREATE TABLE t1 (a int)\nSELECT a FROM t1"
    ), "pg_stat_statements populated"
    node.restart()
    assert (
        node.safe_psql(
            "SELECT query FROM pg_stat_statements WHERE query NOT LIKE '%pg_stat_statements%' ORDER BY query"
        )
        == "CREATE TABLE t1 (a int)\nSELECT a FROM t1"
    ), "pg_stat_statements data kept across restart"
    node.append_conf("pg_stat_statements.save = false")
    node.reload()
    node.restart()
    assert (
        node.safe_psql(
            "SELECT count(*) FROM pg_stat_statements WHERE query NOT LIKE '%pg_stat_statements%'"
        )
        == "0"
    ), "pg_stat_statements data not kept across restart with .save=false"
    node.stop()

# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/bin/pg_rewind/t/002_databases.pl.

After the primary and standby diverge, pg_rewind brings the old primary back in
line with the promoted standby: databases created on the old primary after
promotion disappear, those created on the standby appear, and PGDATA retains
group permissions (initdb -g). Exercised for the 'local' and 'remote' source
modes.
"""

import pypg


def _run_test(rt, test_mode):
    rt.setup_cluster(test_mode, ["-g"])
    rt.start_primary()
    rt.primary_psql("CREATE DATABASE inprimary")
    rt.primary_psql("CREATE TABLE inprimary_tab (a int)", dbname="inprimary")
    rt.create_standby(test_mode)
    rt.primary_psql("CREATE DATABASE beforepromotion")
    rt.primary_psql(
        "CREATE TABLE beforepromotion_tab (a int)", dbname="beforepromotion"
    )
    rt.promote_standby()
    rt.primary_psql("CREATE DATABASE primary_afterpromotion")
    rt.primary_psql(
        "CREATE TABLE primary_promotion_tab (a int)", dbname="primary_afterpromotion"
    )
    rt.standby_psql("CREATE DATABASE standby_afterpromotion")
    rt.standby_psql(
        "CREATE TABLE standby_promotion_tab (a int)", dbname="standby_afterpromotion"
    )
    # The clusters are now diverged.
    rt.run_pg_rewind(test_mode)
    rt.check_query(
        "SELECT datname FROM pg_database ORDER BY 1",
        "beforepromotion\ninprimary\npostgres\n"
        "standby_afterpromotion\ntemplate0\ntemplate1",
        "database names",
    )
    # PGDATA should retain group permissions (initdb -g).
    assert pypg.check_mode_recursive(
        rt.primary.datadir, 0o750, 0o640
    ), "check PGDATA permissions"
    rt.clean_rewind_test()


def test_002_databases(rewind_test):
    """pg_rewind reconciles per-database divergence (local and remote modes)."""
    _run_test(rewind_test, "local")
    _run_test(rewind_test, "remote")

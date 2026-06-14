# Copyright (c) 2022-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long,too-many-statements
"""Port of src/test/modules/test_misc/t/007_catcache_inval.pl.

Catalog-cache list invalidation race: with an injection point pausing a
catcache list miss mid systable scan, redefining a function concurrently must
not leave a stale catcache list entry. Skips without injection points.
Generated from the Perl original via .agent/gen_golden.py.
"""

import pypg

import os
import pytest


def test_007_catcache_inval(create_pg):
    """Generated golden port of 007_catcache_inval."""
    if os.environ.get("enable_injection_points") != "yes":
        pytest.skip("Injection points not supported by this build")
    node = create_pg("node", start=False)
    node.start()
    if not node.check_extension("injection_points"):
        pytest.skip("Extension injection_points not installed")
    node.safe_psql("CREATE EXTENSION injection_points;")
    # randStr(10000): a long random comment filler to enlarge the function body
    longtext = pypg.rand_str(10000)
    node.safe_psql(
        "CREATE FUNCTION foofunc(dummy integer) RETURNS integer AS $$ SELECT 1; /* "
        + longtext
        + " */ $$ LANGUAGE SQL"
    )
    psql_session = node.background_psql("postgres")
    psql_session2 = node.background_psql("postgres")
    psql_session.query(
        "SELECT injection_points_set_local();\n    SELECT injection_points_attach('catcache-list-miss-systable-scan-started', 'wait');"
    )
    psql_session.query_until(
        r"""starting_bg_psql""",
        "\n   \\echo starting_bg_psql\n   SELECT foofunc(1);\n",
    )
    node.safe_psql(
        "CREATE FUNCTION foofunc() RETURNS integer AS $$ SELECT 123 $$ LANGUAGE SQL"
    )
    psql_session2.query(
        "SELECT injection_points_wakeup('catcache-list-miss-systable-scan-started');\n    SELECT injection_points_detach('catcache-list-miss-systable-scan-started');"
    )
    psql_session.query("SELECT foofunc();")
    assert psql_session.quit() == 0, ""
    assert psql_session2.quit() == 0, ""

# Copyright (c) 2022-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long,too-many-statements
"""Port of src/test/modules/test_slru/t/001_multixact.pl.

MultiXact SLRU wraparound handling via the test_slru module: creating multixacts that wrap past the SLRU page boundary (with a backend paused at the multixact-create injection point) does not corrupt or lose multixact members.
Generated from the Perl original via .agent/gen_golden.py.
"""

import os
import pytest


def test_001_multixact(create_pg):
    """MultiXact SLRU wraparound member handling."""
    if os.environ.get("enable_injection_points") != "yes":
        pytest.skip("Injection points not supported by this build")
    node = create_pg("main", start=False)
    node.append_conf("shared_preload_libraries = 'test_slru,injection_points'")
    node.start()
    node.safe_psql("CREATE EXTENSION injection_points")
    node.safe_psql("CREATE EXTENSION test_slru")
    bg_psql = node.background_psql("postgres")
    multi1 = bg_psql.query_safe("SELECT test_create_multixact();")
    node.safe_psql(
        "SELECT injection_points_attach('multixact-create-from-members','wait');"
    )
    bg_psql.query_until(
        r"""assigning lost multi""",
        "\\echo assigning lost multi\n\tSELECT test_create_multixact();",
    )
    node.wait_for_event("client backend", "multixact-create-from-members")
    node.safe_psql("SELECT injection_points_detach('multixact-create-from-members')")
    multi2 = node.safe_psql("SELECT test_create_multixact();")
    node.stop("immediate")
    node.start()
    bg_psql.quit()
    assert (
        node.safe_psql("SELECT test_read_multixact('" + str(multi1) + "');") == ""
    ), "first recorded multi is readable"
    assert (
        node.safe_psql("SELECT test_read_multixact('" + str(multi2) + "');") == ""
    ), "second recorded multi is readable"

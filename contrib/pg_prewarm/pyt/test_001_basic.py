# Copyright (c) 2022-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long,too-many-statements
"""Port of contrib/pg_prewarm/t/001_basic.pl.

pg_prewarm smoke test: prewarming in buffer/read/prefetch modes succeeds (or reports prefetch unsupported), permission checks are enforced on tables/indexes for an unprivileged role, and the cluster shuts down cleanly.
Generated from the Perl original via .agent/gen_golden.py.
"""

import re


def test_001_basic(pg_bin, create_pg):
    """pg_prewarm buffer/read/prefetch modes and permission enforcement."""
    node = create_pg("main", start=False)
    node.append_conf(
        "shared_preload_libraries = 'pg_prewarm'\n    pg_prewarm.autoprewarm = true\n    pg_prewarm.autoprewarm_interval = 0"
    )
    node.start()
    node.safe_psql(
        "CREATE EXTENSION pg_prewarm;\nCREATE TABLE test(c1 int);\nINSERT INTO test SELECT generate_series(1, 100);\nCREATE INDEX test_idx ON test(c1);\nCREATE ROLE test_user LOGIN;"
    )
    result = node.safe_psql("SELECT pg_prewarm('test', 'read');")
    assert re.search(
        r"""^[1-9][0-9]*$""",
        result,
    ), "read mode succeeded"
    result = node.safe_psql("SELECT pg_prewarm('test', 'buffer');")
    assert re.search(
        r"""^[1-9][0-9]*$""",
        result,
    ), "buffer mode succeeded"
    result = node.psql_capture("SELECT pg_prewarm('test', 'prefetch');")
    assert re.search(r"""^[1-9][0-9]*$""", result.stdout) or re.search(
        r"""prefetch is not supported by this build""", result.stderr
    ), "prefetch mode succeeded"
    result = node.psql_capture(
        "SELECT pg_prewarm('test');", extra_params=["--username", "test_user"]
    )
    assert re.search(
        r"""permission denied for table test""", result.stderr
    ), "pg_prewarm failed as expected"
    result = node.psql_capture(
        "SELECT pg_prewarm('test_idx');", extra_params=["--username", "test_user"]
    )
    assert re.search(
        r"""permission denied for index test_idx""", result.stderr
    ), "pg_prewarm failed as expected"
    node.safe_psql("GRANT SELECT ON test TO test_user;")
    result = node.psql_capture(
        "SELECT pg_prewarm('test');", extra_params=["--username", "test_user"]
    )
    assert re.search(
        r"""^[1-9][0-9]*$""",
        result.stdout,
    ), "pg_prewarm succeeded as expected"
    result = node.psql_capture(
        "SELECT pg_prewarm('test_idx');", extra_params=["--username", "test_user"]
    )
    assert re.search(
        r"""^[1-9][0-9]*$""",
        result.stdout,
    ), "pg_prewarm succeeded as expected"
    result = node.safe_psql("SELECT autoprewarm_dump_now();")
    assert re.search(
        r"""^[1-9][0-9]*$""",
        result,
    ), "autoprewarm_dump_now succeeded"
    node.restart()
    node.wait_for_log(
        r"""autoprewarm successfully prewarmed [1-9][0-9]* of [0-9]+ previously-loaded blocks"""
    )
    node.stop()
    pg_bin.command_like(
        ["pg_controldata", str(node.datadir)],
        r"""Database cluster state:\s*shut down""",
        "cluster shut down normally",
    )

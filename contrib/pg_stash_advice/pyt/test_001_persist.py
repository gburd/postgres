# Copyright (c) 2022-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long,too-many-statements
"""Port of contrib/pg_stash_advice/t/001_persist.pl.

pg_stash_advice persistence: advice stashes/entries are dumped to pg_stash_advice.tsv and reloaded on restart (verified via the startup log), and the dump file is removed once all stashes are dropped.
Generated from the Perl original via .agent/gen_golden.py.
"""


def test_001_persist(create_pg):
    """pg_stash_advice persistence across restart and cleanup on drop."""
    node = create_pg("main", start=False)
    node.append_conf(
        "shared_preload_libraries = 'pg_plan_advice, pg_stash_advice'\npg_stash_advice.persist = true\npg_stash_advice.persist_interval = 0"
    )
    node.start()
    node.safe_psql("CREATE EXTENSION pg_stash_advice;")
    node.safe_psql(
        "SELECT pg_create_advice_stash('stash_a');\n\tSELECT pg_set_stashed_advice('stash_a', 1001, 'IndexScan(t)');\n\tSELECT pg_set_stashed_advice('stash_a', 1002, E'line1\\nline2\\ttab\\\\backslash');\n\tSELECT pg_create_advice_stash('stash_b');\n\tSELECT pg_set_stashed_advice('stash_b', 2001, 'SeqScan(t)');"
    )
    result = node.safe_psql(
        "SELECT stash_name, num_entries FROM pg_get_advice_stashes() ORDER BY stash_name"
    )
    assert result == "stash_a|2\nstash_b|1", "stashes present before restart"
    node.restart()
    node.wait_for_log(r"""loaded 2 advice stashes and 3 entries""")
    result = node.safe_psql(
        "SELECT stash_name, num_entries FROM pg_get_advice_stashes() ORDER BY stash_name"
    )
    assert result == "stash_a|2\nstash_b|1", "stashes survived restart"
    result = node.safe_psql(
        "SELECT stash_name, query_id, advice_string FROM pg_get_advice_stash_contents(NULL) ORDER BY stash_name, query_id"
    )
    assert (
        result
        == "stash_a|1001|IndexScan(t)\nstash_a|1002|line1\nline2\ttab\\backslash\nstash_b|2001|SeqScan(t)"
    ), "entry contents survived restart with special characters intact"
    node.safe_psql("SELECT pg_create_advice_stash('stash_c');")
    node.restart()
    node.wait_for_log(r"""loaded 3 advice stashes and 3 entries""")
    result = node.safe_psql(
        "SELECT stash_name, num_entries FROM pg_get_advice_stashes() ORDER BY stash_name"
    )
    assert (
        result == "stash_a|2\nstash_b|1\nstash_c|0"
    ), "all three stashes survived second restart"
    node.safe_psql(
        "SELECT pg_drop_advice_stash('stash_a');\n\tSELECT pg_drop_advice_stash('stash_b');\n\tSELECT pg_drop_advice_stash('stash_c');"
    )
    node.restart()
    result = node.safe_psql("SELECT count(*) FROM pg_get_advice_stashes()")
    assert result == "0", "no stashes after dropping all and restarting"
    assert not (
        node.datadir / "pg_stash_advice.tsv"
    ).exists(), "dump file removed after all stashes dropped"
    node.stop()

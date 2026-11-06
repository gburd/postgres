# Copyright (c) 2022-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long,too-many-statements
"""Port of src/test/modules/test_custom_stats/t/001_custom_stats.pl.

Custom cumulative-statistics test modules (variable- and fixed-numbered custom
stats kinds loaded via shared_preload_libraries): custom stats are recorded,
queried, reset, and persisted across server restarts. Generated from the Perl
original via .agent/gen_golden.py.
"""


def test_001_custom_stats(create_pg):
    """Custom cumulative-statistics test modules (variable- and fixed-numbered."""
    node = create_pg("main", start=False)
    node.append_conf(
        "shared_preload_libraries = 'test_custom_var_stats, test_custom_fixed_stats'"
    )
    node.start()
    node.safe_psql("CREATE EXTENSION test_custom_var_stats")
    node.safe_psql("CREATE EXTENSION test_custom_fixed_stats")
    node.safe_psql("select test_custom_stats_var_create('entry1', 'Test entry 1')")
    node.safe_psql("select test_custom_stats_var_create('entry2', 'Test entry 2')")
    node.safe_psql("select test_custom_stats_var_create('entry3', 'Test entry 3')")
    node.safe_psql("select test_custom_stats_var_create('entry4', 'Test entry 4')")
    node.safe_psql("select test_custom_stats_var_update('entry1')")
    node.safe_psql("select test_custom_stats_var_update('entry1')")
    node.safe_psql("select test_custom_stats_var_update('entry2')")
    node.safe_psql("select test_custom_stats_var_update('entry2')")
    node.safe_psql("select test_custom_stats_var_update('entry2')")
    node.safe_psql("select test_custom_stats_var_update('entry3')")
    node.safe_psql("select test_custom_stats_var_update('entry3')")
    node.safe_psql("select test_custom_stats_var_update('entry4')")
    node.safe_psql("select test_custom_stats_var_update('entry4')")
    node.safe_psql("select test_custom_stats_var_update('entry4')")
    node.safe_psql("select test_custom_stats_fixed_update()")
    node.safe_psql("select test_custom_stats_fixed_update()")
    node.safe_psql("select test_custom_stats_fixed_update()")
    result = node.safe_psql("select * from test_custom_stats_var_report('entry1')")
    assert result == "entry1|2|Test entry 1", "report for variable-sized data of entry1"
    result = node.safe_psql("select * from test_custom_stats_var_report('entry2')")
    assert result == "entry2|3|Test entry 2", "report for variable-sized data of entry2"
    result = node.safe_psql("select * from test_custom_stats_var_report('entry3')")
    assert result == "entry3|2|Test entry 3", "report for variable-sized data of entry3"
    result = node.safe_psql("select * from test_custom_stats_var_report('entry4')")
    assert result == "entry4|3|Test entry 4", "report for variable-sized data of entry4"
    result = node.safe_psql("select * from test_custom_stats_fixed_report()")
    assert result == "3|", "report for fixed-sized stats"
    node.safe_psql("select * from test_custom_stats_var_drop('entry3')")
    result = node.safe_psql("select * from test_custom_stats_var_report('entry3')")
    assert result == "", "entry3 not found after drop"
    node.safe_psql("select * from test_custom_stats_var_drop('entry4')")
    result = node.safe_psql("select * from test_custom_stats_var_report('entry4')")
    assert result == "", "entry4 not found after drop"
    node.stop()
    node.start()
    result = node.safe_psql("select * from test_custom_stats_var_report('entry1')")
    assert (
        result == "entry1|2|Test entry 1"
    ), "variable-sized stats persist after clean restart"
    result = node.safe_psql("select * from test_custom_stats_var_report('entry2')")
    assert (
        result == "entry2|3|Test entry 2"
    ), "variable-sized stats persist after clean restart"
    result = node.safe_psql("select * from test_custom_stats_fixed_report()")
    assert result == "3|", "fixed-sized stats persist after clean restart"
    node.stop("immediate")
    node.start()
    result = node.safe_psql("select * from test_custom_stats_var_report('entry1')")
    assert result == "", "variable-sized stats of entry1 lost after crash recovery"
    result = node.safe_psql("select * from test_custom_stats_var_report('entry2')")
    assert result == "", "variable-sized stats of entry2 lost after crash recovery"
    result = node.safe_psql(
        "select numcalls from test_custom_stats_fixed_report() where stats_reset is not null"
    )
    assert result == "0", "fixed-sized stats are reset after crash recovery"
    node.safe_psql("select test_custom_stats_fixed_update()")
    node.safe_psql("select test_custom_stats_fixed_update()")
    node.safe_psql("select test_custom_stats_fixed_update()")
    result = node.safe_psql("select numcalls from test_custom_stats_fixed_report()")
    assert result == "3", "report of fixed-sized before manual reset"
    node.safe_psql("select test_custom_stats_fixed_reset()")
    result = node.safe_psql(
        "select numcalls from test_custom_stats_fixed_report() where stats_reset is not null"
    )
    assert result == "0", "report of fixed-sized after manual reset"

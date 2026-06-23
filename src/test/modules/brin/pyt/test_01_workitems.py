# Copyright (c) 2022-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long,too-many-statements
"""Port of src/test/modules/brin/t/01_workitems.pl.

BRIN autosummarization work-items: autovacuum processes queued BRIN summarization requests so that index ranges get summarized.
Generated from the Perl original via .agent/gen_golden.py.
"""

import time


def test_01_workitems(create_pg):
    """BRIN autosummarization work-items."""
    node = create_pg("tango", start=False)
    node.append_conf("autovacuum_naptime=1s")
    node.start()
    node.safe_psql("create extension pageinspect")
    node.safe_psql(
        "create table brin_wi (a int) with (fillfactor = 10);\n\t create index brin_wi_idx on brin_wi using brin (a) with (pages_per_range=1, autosummarize=on);"
    )
    node.safe_psql(
        "create table journal (d timestamp) with (fillfactor = 10);\n\t create function packdate(d timestamp) returns text language plpgsql\n\t   as $$ begin return to_char(d, 'yyyymm'); end; $$\n\t   returns null on null input immutable;\n\t create index brin_packdate_idx on journal using brin (packdate(d))\n\t   with (autosummarize = on, pages_per_range = 1);"
    )
    count = node.safe_psql(
        "select count(*) from brin_page_items(get_raw_page('brin_wi_idx', 2), 'brin_wi_idx'::regclass)"
    )
    assert count == "1", "initial brin_wi_idx index state is correct"
    count = node.safe_psql(
        "select count(*) from brin_page_items(get_raw_page('brin_packdate_idx', 2), 'brin_packdate_idx'::regclass)"
    )
    assert count == "1", "initial brin_packdate_idx index state is correct"
    node.safe_psql("insert into brin_wi select * from generate_series(1, 100)")
    node.safe_psql(
        "insert into journal select * from generate_series(timestamp '1976-08-01', '1976-10-28', '1 day')"
    )
    time.sleep(1)
    assert node.poll_query_until(
        "select count(*) > 1 from brin_page_items(get_raw_page('brin_wi_idx', 2), 'brin_wi_idx'::regclass)",
        expected="t",
    ), "brin_wi_idx summarization completed"
    count = node.safe_psql(
        "select count(*) from brin_page_items(get_raw_page('brin_wi_idx', 2), 'brin_wi_idx'::regclass)\n\t where not placeholder;"
    )
    assert int(count) > 1, f"{count} brin_wi_idx ranges got summarized"
    assert node.poll_query_until(
        "select count(*) > 1 from brin_page_items(get_raw_page('brin_packdate_idx', 2), 'brin_packdate_idx'::regclass)",
        expected="t",
    ), "brin_packdate_idx summarization completed"
    count = node.safe_psql(
        "select count(*) from brin_page_items(get_raw_page('brin_packdate_idx', 2), 'brin_packdate_idx'::regclass)\n\t where not placeholder;"
    )
    assert int(count) > 1, f"{count} brin_packdate_idx ranges got summarized"
    node.stop()

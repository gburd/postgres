# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/modules/test_json_parser/t/004_test_parser_perf.pl.

The JSON parser performance harness runs to a clean exit over a 50x-replicated
input with both the recursive-descent and table-driven parsers.
"""

import os

import pypg

_TEST_FILE = os.path.join(os.path.dirname(__file__), "..", "tiny.json")


def test_004_test_parser_perf(pg_bin, tmp_path):
    """The perf harness exits 0 with the recursive-descent and table-driven parsers."""
    contents = pypg.slurp_file(_TEST_FILE)
    fname = tmp_path / "perf.json"
    fname.write_text("[" + contents + ("," + contents) * 49 + "]", encoding="utf-8")
    result = pg_bin.result(["test_json_parser_perf", "1", str(fname)])
    assert result.rc == 0, "perf test runs with recursive descent parser"
    result = pg_bin.result(["test_json_parser_perf", "-i", "1", str(fname)])
    assert result.rc == 0, "perf test runs with table driven parser"

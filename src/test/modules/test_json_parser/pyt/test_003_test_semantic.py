# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/modules/test_json_parser/t/003_test_semantic.pl.

The incremental JSON parser driven with semantic routines (all four executable
variants) produces output identical to the expected tiny.out for tiny.json.
"""

import os

_EXES = (
    ["test_json_parser_incremental"],
    ["test_json_parser_incremental", "-o"],
    ["test_json_parser_incremental_shlib"],
    ["test_json_parser_incremental_shlib", "-o"],
)
_TEST_FILE = os.path.join(os.path.dirname(__file__), "..", "tiny.json")
_TEST_OUT = os.path.join(os.path.dirname(__file__), "..", "tiny.out")


def test_003_test_semantic(pg_bin):
    """Semantic-routine output matches the expected tiny.out for every variant."""
    with open(_TEST_OUT, encoding="utf-8") as fh:
        expected = fh.read()
    for exe in _EXES:
        result = pg_bin.run_command(exe + ["-s", _TEST_FILE])
        assert result.stderr == "", "no error output"
        assert result.stdout + "\n" == expected, "no output diff"

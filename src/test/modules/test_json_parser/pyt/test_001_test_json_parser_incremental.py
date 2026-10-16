# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/modules/test_json_parser/t/001_test_json_parser_incremental.pl.

The incremental JSON parser (both the in-tree and shlib builds, with and without
the -o semantic option) reports a usage error when given too few arguments, and
succeeds for every chunk size from 64 down to 1.
"""

import os
import re

_EXES = (
    ["test_json_parser_incremental"],
    ["test_json_parser_incremental", "-o"],
    ["test_json_parser_incremental_shlib"],
    ["test_json_parser_incremental_shlib", "-o"],
)
_TEST_FILE = os.path.join(os.path.dirname(__file__), "..", "tiny.json")


def test_001_test_json_parser_incremental(pg_bin):
    """Incremental JSON parser: usage error, then success for chunk sizes 64..1."""
    for exe in _EXES:
        result = pg_bin.run_command(exe + ["-c", "10"])
        assert re.search(
            r"Usage:", result.stderr
        ), "error message if not enough arguments"
        for size in range(64, 0, -1):
            result = pg_bin.run_command(exe + ["-c", str(size), _TEST_FILE])
            assert re.search(
                r"SUCCESS", result.stdout
            ), "chunk size {}: test succeeds".format(size)
            assert result.stderr == "", "chunk size {}: no error output".format(size)

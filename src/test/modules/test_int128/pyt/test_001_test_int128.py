# Copyright (c) 2023-2026, PostgreSQL Global Development Group

"""Port of src/test/modules/test_int128/t/001_test_int128.pl.

Runs the test_int128 C executable (which exercises the 128-bit integer
emulation against native __int128 on a large random sample) and requires it to
produce no output. Skips when the build has no native int128 type.
"""

import pytest


def test_001_test_int128(pg_bin):
    """test_int128 runs cleanly (no stdout/stderr), or skips without int128."""
    size = 1_000_000
    result = pg_bin.run_command(["test_int128", str(size)])
    if "skipping tests" in result.stdout:
        pytest.skip("no native int128 type")
    assert result.stdout == "", "test_int128: no stdout"
    assert result.stderr == "", "test_int128: no stderr"

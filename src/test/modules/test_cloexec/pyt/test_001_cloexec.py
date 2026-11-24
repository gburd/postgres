# Copyright (c) 2024-2026, PostgreSQL Global Development Group

"""Port of src/test/modules/test_cloexec/t/001_cloexec.pl.

Windows-specific: runs the test_cloexec executable and verifies that O_CLOEXEC
prevents handle inheritance. Always skips on non-Windows platforms.
"""

import platform
import re

import pytest


def test_001_cloexec(pg_bin):
    """O_CLOEXEC prevents handle inheritance (Windows-only; skips elsewhere)."""
    if platform.system() != "Windows":
        pytest.skip("test is Windows-specific")
    result = pg_bin.result(["test_cloexec"])
    assert result.exit_code == 0 and re.search(
        r"SUCCESS.*O_CLOEXEC behavior verified", result.stdout, re.DOTALL
    ), "O_CLOEXEC prevents handle inheritance"

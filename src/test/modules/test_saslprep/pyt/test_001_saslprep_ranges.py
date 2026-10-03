# Copyright (c) 2022-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long,too-many-statements
"""Port of src/test/modules/test_saslprep/t/001_saslprep_ranges.pl.

SASLprep (RFC 4013) codepoint range handling via the test_saslprep module: valid codepoints normalize/return empty, prohibited/unassigned codepoints error. Gated on PG_TEST_EXTRA=saslprep.
Generated from the Perl original via .agent/gen_golden.py.
"""

import os
import pytest
import re


def test_001_saslprep_ranges(create_pg):
    """SASLprep codepoint range handling (gated on PG_TEST_EXTRA=saslprep)."""
    if (not os.environ.get("PG_TEST_EXTRA")) or (
        not re.search(r"""\bsaslprep\b""", os.environ.get("PG_TEST_EXTRA", ""))
    ):
        pytest.skip("test saslprep not enabled in PG_TEST_EXTRA")
    node = create_pg("main", start=False)
    node.start()
    node.safe_psql("CREATE EXTENSION test_saslprep;")
    result = node.safe_psql(
        "SELECT * FROM test_saslprep_ranges()\n  WHERE status = 'SUCCESS' AND res IN (NULL, '')"
    )
    assert result == "", "valid codepoints returning an empty password"
    node.stop()

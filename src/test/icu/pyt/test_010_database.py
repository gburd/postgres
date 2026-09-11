# Copyright (c) 2022-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long,too-many-statements
"""Port of src/test/icu/t/010_database.pl.

ICU per-database collation: databases created with LOCALE_PROVIDER icu sort correctly by default/explicit collations, C and custom ICU locales work, and a CREATE DATABASE whose locale provider differs from its template is rejected.
Generated from the Perl original via .agent/gen_golden.py.
"""

import os
import pytest
import re


def test_010_database(create_pg):
    """ICU per-database collation and locale-provider template matching."""
    if os.environ.get("with_icu") != "yes":
        pytest.skip("ICU not supported by this build")
    node1 = create_pg("node1", start=False)
    node1.start()
    node1.safe_psql(
        "CREATE DATABASE dbicu LOCALE_PROVIDER icu LOCALE 'C' ICU_LOCALE 'en@colCaseFirst=upper' ENCODING 'UTF8' TEMPLATE template0"
    )
    node1.safe_psql(
        "CREATE COLLATION upperfirst (provider = icu, locale = 'en@colCaseFirst=upper');\nCREATE TABLE icu (def text, en text COLLATE \"en-x-icu\", upfirst text COLLATE upperfirst);\nINSERT INTO icu VALUES ('a', 'a', 'a'), ('b', 'b', 'b'), ('A', 'A', 'A'), ('B', 'B', 'B');",
        dbname="dbicu",
    )
    assert (
        node1.safe_psql("SELECT icu_unicode_version() IS NOT NULL", dbname="dbicu")
        == "t"
    ), "ICU unicode version defined"
    assert (
        node1.safe_psql("SELECT def FROM icu ORDER BY def", dbname="dbicu")
        == "A\na\nB\nb"
    ), "sort by database default locale"
    assert (
        node1.safe_psql(
            'SELECT def FROM icu ORDER BY def COLLATE "en-x-icu"', dbname="dbicu"
        )
        == "a\nA\nb\nB"
    ), "sort by explicit collation standard"
    assert (
        node1.safe_psql(
            "SELECT def FROM icu ORDER BY en COLLATE upperfirst", dbname="dbicu"
        )
        == "A\na\nB\nb"
    ), "sort by explicit collation upper first"
    assert (
        node1.psql_capture(
            "CREATE DATABASE dbicu1 LOCALE_PROVIDER icu LOCALE 'C' TEMPLATE template0 ENCODING UTF8"
        ).exit_code
        == 0
    ), "C locale works for ICU"
    assert (
        node1.psql_capture(
            "CREATE DATABASE dbicu2 LOCALE_PROVIDER icu LOCALE '@colStrength=primary'\n          LC_COLLATE='C' LC_CTYPE='C' TEMPLATE template0 ENCODING UTF8"
        ).exit_code
        == 0
    ), "LOCALE works for ICU locales if LC_COLLATE and LC_CTYPE are specified"
    result = node1.psql_capture(
        "CREATE DATABASE dbicu3 LOCALE_PROVIDER builtin LOCALE 'C' TEMPLATE dbicu"
    )
    assert result.exit_code != 0, "locale provider must match template: exit code not 0"
    assert re.search(
        r"""ERROR:  new locale provider \(builtin\) does not match locale provider of the template database \(icu\)""",
        result.stderr,
    ), "locale provider must match template: error message"

# Copyright (c) 2024-2026, PostgreSQL Global Development Group

"""Port of src/test/modules/test_escape/t/001_test_escape.pl.

Runs the test_escape C executable against a live sql_ascii database. The program
emits TAP for each escaping case; this wrapper requires a clean exit with no
stderr and fails on any 'not ok' line or any line it cannot map to TAP.
"""

import re

import pytest


def test_001_test_escape(create_pg, pg_bin):
    """test_escape exits cleanly and every emitted TAP line is 'ok'."""
    node = create_pg("node")
    node.safe_psql(
        'CREATE DATABASE db_sql_ascii ENCODING "sql_ascii" TEMPLATE template0;'
    )
    conninfo = node.connstr() + " dbname=db_sql_ascii"
    result = pg_bin.result(["test_escape", "--conninfo", conninfo])
    assert result.rc == 0, "test_escape returns 0"
    assert result.stderr == "", "test_escape stderr is empty"
    for line in result.stdout.split("\n"):
        if re.match(r"^ok \d+ ?(.*)", line):
            continue
        not_ok = re.match(r"^not ok \d+ ?(.*)", line)
        if not_ok:
            pytest.fail(not_ok.group(1))
        elif re.match(r"^# ?(.*)", line) or re.match(r"^\d+\.\.\d+$", line):
            continue
        elif line == "":
            continue
        else:
            pytest.fail("no unmapped lines, got {}".format(line))

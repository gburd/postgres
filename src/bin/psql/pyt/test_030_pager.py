# Copyright (c) 2024-2026, PostgreSQL Global Development Group

"""Port of src/bin/psql/t/030_pager.pl.

With PSQL_PAGER set to "wc -l" and a fixed 24x80 terminal, psql invokes the
pager exactly when output exceeds the screen: a 23-row result is printed
directly, a 24-row result is paged (wc reports the line count), and expanded
mode and a \\d+ footer also trigger paging with the expected counts.
"""

import re
import shutil
import subprocess
import sys

import pytest


def _wc_l_works():
    if not shutil.which("wc"):
        return False
    out = subprocess.run(
        ["wc", "-l"],
        input="foo bar\nbaz\n",
        encoding="utf-8",
        capture_output=True,
        check=False,
    )
    return out.returncode == 0 and out.stdout.strip() == "2"


@pytest.mark.skipif(sys.platform == "win32", reason="requires a PTY")
def test_030_pager(create_pg, monkeypatch):
    """psql invokes the pager only when output exceeds the 24x80 screen."""
    if not _wc_l_works():
        pytest.skip('"wc -l" is needed to run this test')
    monkeypatch.setenv("PSQL_PAGER", "wc -l")
    node = create_pg("main")
    cols = ",\n".join("{} as {}".format(i + 1, chr(ord("a") + i)) for i in range(26))
    node.safe_psql("create view public.view_030_pager as select\n" + cols)
    psql = node.interactive_psql("postgres")
    psql.set_query_timer_restart()
    psql.set_winsize(24, 80)
    _do(
        psql,
        "SELECT 'test' AS t FROM generate_series(1,23);\n",
        r"test\r?$",
        "execute SELECT query that needs no pagination",
    )
    _do(
        psql,
        "SELECT 'test' AS t FROM generate_series(1,24);\n",
        r"24\r?$",
        "execute SELECT query that needs pagination",
    )
    _do(
        psql,
        "\\pset expanded\nSELECT generate_series(1,20) as g;\n",
        r"39\r?$",
        "execute SELECT query that needs pagination in expanded mode",
    )
    _do(
        psql,
        "\\pset tuples_only off\n\\d+ public.view_030_pager\n",
        r"55\r?$",
        "execute command with footer that needs pagination",
    )
    psql.quit()
    node.stop()


def _do(psql, send, pattern, annotation):
    out = psql.query_until(re.compile(pattern, re.M), send)
    assert re.search(pattern, out, re.M) and not psql.timed_out, annotation

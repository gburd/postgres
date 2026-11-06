# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""
Port of src/bin/pg_ctl/t/004_logrotate.pl.

Verifies that the logging collector writes to stderr/csvlog/jsonlog files, that
pg_ctl logrotate switches to fresh files, and that pg_current_logfile() agrees
with current_logfiles.
"""

import os
import re
import time

import pypg

# Matches the three-line current_logfiles content (one line per destination).
_CURRENT_LOGFILES_RE = (
    r"^stderr log/postgresql-.*log\n"
    r"csvlog log/postgresql-.*csv\n"
    r"jsonlog log/postgresql-.*json$"
)


def _max_attempts():
    return 10 * int(os.environ.get("PG_TEST_TIMEOUT_DEFAULT", "180"))


def _fetch_file_name(logfiles, fmt):
    """Return the file name recorded for a destination in current_logfiles."""
    filename = None
    for line in logfiles.splitlines():
        match = re.search(r"{} (.*)$".format(fmt), line)
        if match:
            filename = match.group(1)
    return filename


def _check_log_pattern(fmt, logfiles, pattern, node):
    """Assert pattern appears in the fmt log file and pg_current_logfile agrees."""
    lfname = _fetch_file_name(logfiles, fmt)

    contents = ""
    for _ in range(_max_attempts()):
        contents = pypg.slurp_file(node.datadir / lfname)
        if re.search(pattern, contents):
            break
        time.sleep(0.1)

    assert re.search(
        pattern, contents
    ), "found expected log file content for {}".format(fmt)

    # While we're at it, test the pg_current_logfile() function.
    assert (
        node.safe_psql("SELECT pg_current_logfile('{}')".format(fmt)) == lfname
    ), "pg_current_logfile() gives correct answer with {}".format(fmt)


def _wait_for_current_logfiles(node, differs_from=None):
    """Slurp current_logfiles, retrying until it exists (and optionally changes)."""
    path = node.datadir / "current_logfiles"
    contents = ""
    for _ in range(_max_attempts()):
        try:
            contents = pypg.slurp_file(path)
        except FileNotFoundError:
            time.sleep(0.1)
            continue
        if differs_from is None or contents != differs_from:
            return contents
        time.sleep(0.1)
    return contents


def test_logrotate(create_pg):
    """Logging collector output, rotation, and pg_current_logfile()."""
    node = create_pg("primary", start=False)
    node.append_conf(
        "\n".join(
            [
                "logging_collector = on",
                "log_destination = 'stderr, csvlog, jsonlog'",
                # these ensure stability of test results:
                "log_rotation_age = 0",
                "lc_messages = 'C'",
            ]
        )
    )
    node.start()

    # Verify that log output gets to the file (division by zero error).
    node.bin.result(["psql", "-c", "SELECT 1/0"])

    current_logfiles = _wait_for_current_logfiles(node)
    assert re.search(_CURRENT_LOGFILES_RE, current_logfiles), "current_logfiles is sane"

    _check_log_pattern("stderr", current_logfiles, "division by zero", node)
    _check_log_pattern("csvlog", current_logfiles, "division by zero", node)
    _check_log_pattern("jsonlog", current_logfiles, "division by zero", node)

    # Sleep 2 seconds and ask for log rotation; this should result in output
    # into a different log file name.
    time.sleep(2)
    node.logrotate()

    new_current_logfiles = _wait_for_current_logfiles(
        node, differs_from=current_logfiles
    )
    assert re.search(
        _CURRENT_LOGFILES_RE, new_current_logfiles
    ), "new current_logfiles is sane"

    # Verify that log output gets to this file, too (syntax error).
    node.bin.result(["psql", "-c", "fee fi fo fum"])

    _check_log_pattern("stderr", new_current_logfiles, "syntax error", node)
    _check_log_pattern("csvlog", new_current_logfiles, "syntax error", node)
    _check_log_pattern("jsonlog", new_current_logfiles, "syntax error", node)

    node.stop()

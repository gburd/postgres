# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/bin/psql/t/020_cancel.pl.

Query cancellation by sending SIGINT to a running psql.
"""

import os
import platform
import re
import signal

import pytest

# Sending SIGINT on Windows would terminate the test itself.
pytestmark = pytest.mark.skipif(
    platform.system() == "Windows",
    reason="sending SIGINT on Windows terminates the test itself",
)


def test_cancel(create_pg):
    """SIGINT to psql cancels its running statement."""
    node = create_pg("main")
    timeout_default = int(os.environ.get("PG_TEST_TIMEOUT_DEFAULT", "180"))

    session = node.background_psql()

    # Send a sleep and wait until the server has registered it.
    session.send("select pg_sleep({});\n".format(timeout_default))
    assert node.poll_query_until(
        "SELECT (SELECT count(*) FROM pg_stat_activity "
        "WHERE query ~ '^select pg_sleep') > 0;"
    ), "server registered the sleep"

    # Send the cancel request.
    session.signal(signal.SIGINT)
    result = session.finish()

    assert result != 0, "query failed as expected"
    assert re.search(
        r"canceling statement due to user request", session.stderr
    ), "query was canceled"

# Copyright (c) 2022-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long,too-many-statements,implicit-str-concat
"""Port of src/test/modules/test_misc/t/005_timeouts.pl.

FATAL timeout handling (transaction_timeout, idle_in_transaction_session_timeout, idle_session_timeout) using injection points to deterministically await the timeout; verifies the backend is terminated with the expected log message. Skips without injection points.
Generated from the Perl original via .agent/gen_golden.py.
"""

import os
import pytest


def test_005_timeouts(create_pg):
    """FATAL session/transaction timeout handling (gated on injection points)."""
    if os.environ.get("enable_injection_points") != "yes":
        pytest.skip("Injection points not supported by this build")
    node = create_pg("master", start=False)
    node.start()
    if not node.check_extension("injection_points"):
        pytest.skip("Extension injection_points not installed")
    node.safe_psql("CREATE EXTENSION injection_points;")
    node.safe_psql("SELECT injection_points_attach('transaction-timeout', 'wait');")
    psql_session = node.background_psql("postgres")
    psql_session.query_until(
        r"""starting_bg_psql""",
        "\\echo starting_bg_psql\n"
        "   SET transaction_timeout to '10ms';\n"
        "   BEGIN;\n"
        "   SELECT 1 \\watch 0.001\n"
        "   \\q\n",
    )
    node.wait_for_event("client backend", "transaction-timeout")
    log_offset = node.current_log_position()
    node.safe_psql("SELECT injection_points_wakeup('transaction-timeout');")
    node.wait_for_log(
        r"""terminating connection due to transaction timeout""", log_offset
    )
    psql_session.quit()
    node.safe_psql(
        "SELECT injection_points_attach('idle-in-transaction-session-timeout', 'wait');"
    )
    psql_session = node.background_psql("postgres")
    psql_session.query_until(
        r"""starting_bg_psql""",
        "\n   \\echo starting_bg_psql\n"
        "   SET idle_in_transaction_session_timeout to '10ms';\n"
        "   BEGIN;\n",
    )
    node.wait_for_event("client backend", "idle-in-transaction-session-timeout")
    log_offset = node.current_log_position()
    node.safe_psql(
        "SELECT injection_points_wakeup('idle-in-transaction-session-timeout');"
    )
    node.wait_for_log(
        r"""terminating connection due to idle-in-transaction timeout""", log_offset
    )
    assert psql_session.quit() == 0, ""
    node.safe_psql("SELECT injection_points_attach('idle-session-timeout', 'wait');")
    psql_session = node.background_psql("postgres")
    psql_session.query_until(
        r"""starting_bg_psql""",
        "\n   \\echo starting_bg_psql\n" "   SET idle_session_timeout to '10ms';\n",
    )
    node.wait_for_event("client backend", "idle-session-timeout")
    log_offset = node.current_log_position()
    node.safe_psql("SELECT injection_points_wakeup('idle-session-timeout');")
    node.wait_for_log(
        r"""terminating connection due to idle-session timeout""", log_offset
    )
    assert psql_session.quit() == 0, ""

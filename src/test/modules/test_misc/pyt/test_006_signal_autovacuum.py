# Copyright (c) 2024-2026, PostgreSQL Global Development Group

"""Port of src/test/modules/test_misc/t/006_signal_autovacuum.pl.

Test signaling autovacuum worker with pg_signal_autovacuum_worker.

Only roles with privileges of pg_signal_autovacuum_worker are allowed to
signal autovacuum workers.  This test uses an injection point located at the
beginning of the autovacuum worker startup.  Skips without injection points.
"""

import os
import re

import pytest


def test_006_signal_autovacuum(create_pg):
    """Only pg_signal_autovacuum_worker members may terminate av workers."""
    if os.environ.get("enable_injection_points") != "yes":
        pytest.skip("Injection points not supported by this build")

    # Initialize postgres
    node = create_pg("node", start=False)

    # This ensures a quick worker spawn.
    node.append_conf("autovacuum_naptime = 1")
    node.start()

    # Check if the extension injection_points is available, as it may be
    # possible that this script is run with installcheck, where the module
    # would not be installed by default.
    if not node.check_extension("injection_points"):
        pytest.skip("Extension injection_points not installed")

    node.safe_psql("CREATE EXTENSION injection_points;")

    node.safe_psql(
        """
    CREATE ROLE regress_regular_role;
    CREATE ROLE regress_worker_role;
    GRANT pg_signal_autovacuum_worker TO regress_worker_role;
"""
    )

    # From this point, autovacuum worker will wait at startup.
    node.safe_psql("SELECT injection_points_attach('autovacuum-worker-start', 'wait');")

    # Accelerate worker creation in case we reach this point before the naptime
    # ends.
    node.reload()

    # Wait until an autovacuum worker starts.
    node.wait_for_event("autovacuum worker", "autovacuum-worker-start")

    # And grab one of them.
    av_pid = node.safe_psql(
        """
    SELECT pid FROM pg_stat_activity WHERE backend_type = 'autovacuum worker' """
        """AND wait_event = 'autovacuum-worker-start' LIMIT 1;
"""
    )

    # Regular role cannot terminate autovacuum worker.
    result = node.psql_capture(
        """
    SET ROLE regress_regular_role;
    SELECT pg_terminate_backend('{}');
""".format(
            av_pid
        ),
        on_error_stop=False,
    )

    assert _like(
        result.stderr,
        r"ERROR:  permission denied to terminate process\nDETAIL:  "
        r'Only roles with privileges of the "pg_signal_autovacuum_worker" '
        r"role may terminate autovacuum workers\.",
    ), "autovacuum worker not signaled with regular role"

    offset = node.current_log_position()

    # Role with pg_signal_autovacuum_worker can terminate autovacuum worker.
    node.psql_capture(
        """
    SET ROLE regress_worker_role;
    SELECT pg_terminate_backend('{}');
""".format(
            av_pid
        ),
        on_error_stop=False,
    )

    # Wait for the autovacuum worker to exit before scanning the logs.
    node.poll_query_until(
        "SELECT count(*) = 0 FROM pg_stat_activity "
        "WHERE pid = '{}' AND backend_type = 'autovacuum worker';".format(av_pid)
    )

    # Check that the primary server logs a FATAL indicating that autovacuum
    # is terminated.
    assert node.log_matches(
        r"FATAL: .*terminating autovacuum process due to administrator command",
        offset,
    ), "autovacuum worker signaled with pg_signal_autovacuum_worker granted"

    # Release injection point.
    node.safe_psql("SELECT injection_points_detach('autovacuum-worker-start');")


def _like(text, pattern):
    """Return True if pattern (a regex) matches text, mirroring Perl like()."""
    return re.search(pattern, text) is not None

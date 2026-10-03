# Copyright (c) 2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long,too-many-statements
"""Port of src/test/modules/test_misc/t/011_lock_stats.pl.

Test for the lock statistics and log_lock_waits.

This test creates multiple locking situations when a session (s2) has to wait
on a lock for longer than deadlock_timeout.  The first tests each test a
dedicated lock type.  The last one checks that log_lock_waits has no impact on
the statistics counters.

This test also checks that log_lock_waits messages are emitted both when a wait
occurs and when the lock is acquired, and that the "still waiting for" message
is logged exactly once per wait, even if the backend wakes due to signals.

Skips without injection points.
"""

import os
import re

import pytest

import pypg

_DEADLOCK_TIMEOUT = 10


def _setup_sessions(node):
    """Set up the 2 sessions (mirrors the Perl setup_sessions sub)."""
    s1 = node.background_psql("postgres")
    s2 = node.background_psql("postgres")

    # Setup injection points for the waiting session
    s2.query_until(
        r"attaching_injection_point",
        "\n\t\t\t\\echo attaching_injection_point\n"
        "\t\t\tSELECT injection_points_attach('deadlock-timeout-fired', 'wait');\n",
    )
    return s1, s2


def _wait_for_pg_stat_lock(node, lock_type):
    """Fetch waits and wait_time from pg_stat_lock for a given lock type until
    they reach expected values: at least one wait and waiting longer than the
    deadlock_timeout."""
    assert node.poll_query_until(
        """
\t\tSELECT waits > 0 AND wait_time >= {timeout}
\t\tFROM pg_stat_lock
\t\tWHERE locktype = '{lock_type}';
\t""".format(
            timeout=_DEADLOCK_TIMEOUT, lock_type=lock_type
        )
    ), "Timed out waiting for pg_stat_lock for {}".format(lock_type)


def _wait_and_detach(node, point_name):
    """Wait for a point, then detach it (mirrors Perl wait_and_detach)."""
    node.wait_for_event("client backend", point_name)
    node.safe_psql(
        """
SELECT injection_points_detach('{point}');
SELECT injection_points_wakeup('{point}');
""".format(
            point=point_name
        )
    )


def test_011_lock_stats(create_pg):
    """Lock statistics and log_lock_waits behavior across lock types."""
    if os.environ.get("enable_injection_points") != "yes":
        pytest.skip("Injection points not supported by this build")

    # Node initialization
    node = create_pg("node", start=False)
    node.append_conf("deadlock_timeout = {}ms".format(_DEADLOCK_TIMEOUT))
    node.start()

    # Check if the extension injection_points is available
    if not node.check_extension("injection_points"):
        pytest.skip("Extension injection_points not installed")

    node.safe_psql("CREATE EXTENSION injection_points;")

    node.safe_psql(
        """
CREATE TABLE test_stat_tab(key text not null, value int);
INSERT INTO test_stat_tab(key, value) VALUES('k0', 1);
"""
    )

    ##########################################################################

    # ###### Relation lock

    s1, s2 = _setup_sessions(node)

    log_offset = node.current_log_position()

    s1.query_safe(
        """
SELECT pg_stat_reset_shared('lock');
BEGIN;
LOCK TABLE test_stat_tab;
"""
    )

    # s2 setup
    s2.query_safe(
        """
BEGIN;
SELECT pg_stat_force_next_flush();
"""
    )
    # s2 blocks on LOCK.
    s2.query_until(
        r"lock_s2",
        "\n\\echo lock_s2\nLOCK TABLE test_stat_tab;\n",
    )

    _wait_and_detach(node, "deadlock-timeout-fired")

    # Check that log_lock_waits message is emitted during a lock wait.
    node.wait_for_log(r"still waiting for AccessExclusiveLock on relation", log_offset)

    # Wake the backend waiting on the lock and confirm it woke by calling
    # pg_log_backend_memory_contexts() and checking for the logged memory
    # contexts. This is necessary to test later that the "still waiting for"
    # message is logged exactly once per wait, even if the backend wakes
    # during the wait.
    node.safe_psql(
        "SELECT pg_log_backend_memory_contexts(pid)\n"
        "\tFROM pg_locks WHERE locktype = 'relation' AND\n"
        "\trelation = 'test_stat_tab'::regclass AND NOT granted;"
    )
    node.wait_for_log(r"logging memory contexts", log_offset)

    # deadlock_timeout fired, now commit in s1 and s2
    s1.query_safe("COMMIT")
    s2.query_safe("COMMIT")

    # check that pg_stat_lock has been updated
    _wait_for_pg_stat_lock(node, "relation")
    assert True, "Lock stats ok for relation"

    # Check that log_lock_waits message is emitted when the lock is acquired
    # after waiting.
    node.wait_for_log(r"acquired AccessExclusiveLock on relation", log_offset)

    # Check that the "still waiting for" message is logged exactly once per
    # wait, even if the backend wakes during the wait.
    log_contents = pypg.slurp_file(node.log, log_offset)
    still_waiting = _find_all("still waiting for", log_contents)
    assert len(still_waiting) == 1, (
        "still waiting logged exactly once despite wakeups from "
        "pg_log_backend_memory_contexts()"
    )

    # close sessions
    s1.quit()
    s2.quit()

    # ###### transaction lock

    s1, s2 = _setup_sessions(node)

    log_offset = node.current_log_position()

    s1.query_safe(
        """
SELECT pg_stat_reset_shared('lock');
INSERT INTO test_stat_tab(key, value) VALUES('k1', 1), ('k2', 1), ('k3', 1);
BEGIN;
UPDATE test_stat_tab SET value = value + 1 WHERE key = 'k1';
"""
    )

    # s2 setup
    s2.query_safe(
        """
SET log_lock_waits = on;
BEGIN;
SELECT pg_stat_force_next_flush();
"""
    )
    # s2 blocks here on UPDATE
    s2.query_until(
        r"lock_s2",
        "\n\\echo lock_s2\n"
        "UPDATE test_stat_tab SET value = value + 1 WHERE key = 'k1';\n",
    )

    _wait_and_detach(node, "deadlock-timeout-fired")

    # Check that log_lock_waits message is emitted during a lock wait.
    node.wait_for_log(r"still waiting for ShareLock on transaction", log_offset)

    # deadlock_timeout fired, now commit in s1 and s2
    s1.query_safe("COMMIT")
    s2.query_safe("COMMIT")

    # check that pg_stat_lock has been updated
    _wait_for_pg_stat_lock(node, "transactionid")
    assert True, "Lock stats ok for transactionid"

    # Check that log_lock_waits message is emitted when the lock is acquired
    # after waiting.
    node.wait_for_log(r"acquired ShareLock on transaction", log_offset)

    # Close sessions
    s1.quit()
    s2.quit()

    # ###### advisory lock

    s1, s2 = _setup_sessions(node)

    log_offset = node.current_log_position()

    s1.query_safe(
        """
SELECT pg_stat_reset_shared('lock');
SELECT pg_advisory_lock(1);
"""
    )

    # s2 setup
    s2.query_safe(
        """
SET log_lock_waits = on;
BEGIN;
SELECT pg_stat_force_next_flush();
"""
    )
    # s2 blocks on the advisory lock.
    s2.query_until(
        r"lock_s2",
        "\n\\echo lock_s2\nSELECT pg_advisory_lock(1);\n",
    )

    _wait_and_detach(node, "deadlock-timeout-fired")

    # Check that log_lock_waits message is emitted during a lock wait.
    node.wait_for_log(r"still waiting for ExclusiveLock on advisory lock", log_offset)

    # deadlock_timeout fired, now unlock and commit s2
    s1.query_safe("SELECT pg_advisory_unlock(1)")
    s2.query_safe(
        """
SELECT pg_advisory_unlock(1);
COMMIT;
"""
    )

    # check that pg_stat_lock has been updated
    _wait_for_pg_stat_lock(node, "advisory")
    assert True, "Lock stats ok for advisory"

    # Check that log_lock_waits message is emitted when the lock is acquired
    # after waiting.
    node.wait_for_log(r"acquired ExclusiveLock on advisory lock", log_offset)

    # Close sessions
    s1.quit()
    s2.quit()

    # ###### Ensure log_lock_waits has no impact

    s1, s2 = _setup_sessions(node)

    log_offset = node.current_log_position()

    s1.query_safe(
        """
SELECT pg_stat_reset_shared('lock');
BEGIN;
LOCK TABLE test_stat_tab;
"""
    )

    # s2 setup
    s2.query_safe(
        """
SET log_lock_waits = off;
BEGIN;
SELECT pg_stat_force_next_flush();
"""
    )
    # s2 blocks on LOCK.
    s2.query_until(
        r"lock_s2",
        "\n\\echo lock_s2\nLOCK TABLE test_stat_tab;\n",
    )

    _wait_and_detach(node, "deadlock-timeout-fired")

    # deadlock_timeout fired, now commit in s1 and s2
    s1.query_safe("COMMIT")
    s2.query_safe("COMMIT")

    # check that pg_stat_lock has been updated
    _wait_for_pg_stat_lock(node, "relation")
    assert True, "log_lock_waits has no impact on Lock stats"

    # Check that no log_lock_waits messages are emitted
    assert not node.log_matches(
        "still waiting for AccessExclusiveLock on relation", log_offset
    ), "check that no log_lock_waits message is emitted during a lock wait"
    assert not node.log_matches(
        "acquired AccessExclusiveLock on relation", log_offset
    ), (
        "check that no log_lock_waits message is emitted when the lock is "
        "acquired after waiting"
    )

    # close sessions
    s1.quit()
    s2.quit()

    # cleanup
    node.safe_psql("DROP TABLE test_stat_tab;")


def _find_all(needle, haystack):
    """Return all non-overlapping occurrences of needle (a regex) in haystack."""
    return re.findall(needle, haystack)

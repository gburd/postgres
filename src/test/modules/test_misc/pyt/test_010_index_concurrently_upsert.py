# Copyright (c) 2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long,too-many-statements
"""Port of src/test/modules/test_misc/t/010_index_concurrently_upsert.pl.

Test INSERT ON CONFLICT DO UPDATE behavior concurrent with CREATE INDEX
CONCURRENTLY and REINDEX CONCURRENTLY.

These tests verify the fix for "duplicate key value violates unique
constraint" errors that occurred when infer_arbiter_indexes() only considered
indisvalid indexes, causing different transactions to use different arbiter
indexes.

Skips without injection points.
"""

import os
import re
import time

import pytest

import pypg


def _wait_for_injection_point(node, point_name, timeout=None):
    """Wait for a session to hit an injection point.  Returns True if found,
    False if timeout.  On timeout, logs diagnostic information about all active
    queries (mirrors the Perl wait_for_injection_point sub)."""
    if timeout is None:
        timeout = pypg.test_timeout_default() / 2

    for _ in range(int(timeout * 10)):
        pid = node.safe_psql(
            """
\t\t\tSELECT pid FROM pg_stat_activity
\t\t\tWHERE wait_event_type = 'InjectionPoint'
\t\t\t  AND wait_event = '{}'
\t\t\tLIMIT 1;
\t\t""".format(
                point_name
            )
        )
        if pid != "":
            return True
        time.sleep(0.1)

    # Timeout - report diagnostic information
    activity = node.safe_psql(
        """
\t\tSELECT format('pid=%s, state=%s, wait_event_type=%s, wait_event=%s, backend_xmin=%s, backend_xid=%s, query=%s',
\t\t\tpid, state, wait_event_type, wait_event, backend_xmin, backend_xid, left(query, 100))
\t\tFROM pg_stat_activity
\t\tORDER BY pid;
\t"""
    )
    print(
        "wait_for_injection_point timeout waiting for: {}\n"
        "Current queries in pg_stat_activity:\n{}".format(point_name, activity)
    )

    return False


def _ok_injection_point(node, injection_point, testname=None):
    """ok() a wait for the given injection point (mirrors ok_injection_point)."""
    if testname is None:
        testname = "hit injection point {}".format(injection_point)
    assert _wait_for_injection_point(node, injection_point), testname


def _wait_for_idle(node, pid, timeout=None):
    """Wait for a specific backend to become idle.  Returns True if idle,
    False if waiting for injection point or timeout (mirrors wait_for_idle)."""
    if timeout is None:
        timeout = pypg.test_timeout_default() / 2

    for _ in range(int(timeout * 10)):
        result = node.safe_psql(
            "\n\t\t\tSELECT state, wait_event_type FROM pg_stat_activity "
            "WHERE pid = {};\n\t\t".format(pid)
        )
        state, _, wait_event_type = result.partition("|")
        if state == "idle":
            return True
        if wait_event_type == "InjectionPoint":
            return False
        time.sleep(0.1)
    return False


def _wakeup_injection_point(node, point_name):
    """Detach and wakeup an injection point (mirrors wakeup_injection_point)."""
    node.safe_psql(
        """
SELECT injection_points_detach('{point}');
SELECT injection_points_wakeup('{point}');
""".format(
            point=point_name
        )
    )


def _safe_quit(session):
    """Wait for any pending query to complete, capture stderr, and close the
    session.  Returns the stderr output (excluding internal markers).  Mirrors
    the Perl safe_quit sub."""
    banner = "safe_quit_marker"
    banner_match = re.compile(r"(^|\n)" + banner + r"\r?\n")

    session.send("\\echo {b}\n\\warn {b}\n".format(b=banner))

    # Send a marker and wait for it to ensure any pending query completes.
    session.query_until(banner_match.pattern)
    deadline = time.monotonic() + pypg.test_timeout_default()
    while not banner_match.search(session.stderr):
        if time.monotonic() > deadline:
            raise TimeoutError("safe_quit timed out waiting for banner on stderr")
        time.sleep(0.02)

    # Capture stderr (excluding the banner)
    stderr = banner_match.sub("", session.stderr)

    # Close the session
    session.quit()

    return stderr


def _clean_safe_quit_ok(*sessions):
    """Verify that the given sessions exit cleanly (mirrors clean_safe_quit_ok)."""
    for i, session in enumerate(sessions, start=1):
        assert _safe_quit(session) == "", "session {} quit cleanly".format(i)


def test_010_index_concurrently_upsert(create_pg):
    """UPSERT concurrent with (RE)INDEX CONCURRENTLY across many permutations."""
    if os.environ.get("enable_injection_points") != "yes":
        pytest.skip("Injection points not supported by this build")

    # Node initialization
    node = create_pg("node", start=False)
    node.start()

    # Check if the extension injection_points is available
    if not node.check_extension("injection_points"):
        pytest.skip("Extension injection_points not installed")

    node.safe_psql("CREATE EXTENSION injection_points;")

    node.safe_psql(
        """
CREATE SCHEMA test;
CREATE UNLOGGED TABLE test.tblpk (i int PRIMARY KEY, updated_at timestamp);
ALTER TABLE test.tblpk SET (parallel_workers=0);

CREATE TABLE test.tblparted(i int primary key, updated_at timestamp) PARTITION BY RANGE (i);
CREATE TABLE test.tbl_partition PARTITION OF test.tblparted
    FOR VALUES FROM (0) TO (10000)
    WITH (parallel_workers = 0);

CREATE UNLOGGED TABLE test.tblexpr(i int, updated_at timestamp);
CREATE UNIQUE INDEX tbl_pkey_special ON test.tblexpr(abs(i)) WHERE i < 1000;
ALTER TABLE test.tblexpr SET (parallel_workers=0);

"""
    )

    ##########################################################################
    # Test: REINDEX CONCURRENTLY + UPSERT (wakeup at set-dead phase)

    # Create sessions with on_error_stop => 0 so psql doesn't exit on SQL
    # errors.  This allows us to collect stderr and detect errors after the
    # test completes.
    s1 = node.background_psql("postgres", on_error_stop=False)
    s2 = node.background_psql("postgres", on_error_stop=False)
    s3 = node.background_psql("postgres", on_error_stop=False)

    # Setup injection points for each session
    s1.query_safe(
        """
SELECT injection_points_set_local();
SELECT injection_points_attach('check-exclusion-or-unique-constraint-no-conflict', 'wait');
"""
    )

    s2.query_safe(
        """
SELECT injection_points_set_local();
SELECT injection_points_attach('exec-insert-before-insert-speculative', 'wait');
"""
    )

    s3.query_safe(
        """
SELECT injection_points_set_local();
SELECT injection_points_attach('reindex-relation-concurrently-before-set-dead', 'wait');
"""
    )

    # s3 starts REINDEX (will block on reindex-relation-concurrently-before-set-dead)
    s3.query_until(
        r"starting_reindex",
        "\n\\echo starting_reindex\nREINDEX INDEX CONCURRENTLY test.tblpk_pkey;\n",
    )

    # Wait for s3 to hit injection point
    _ok_injection_point(node, "reindex-relation-concurrently-before-set-dead")

    # s1 starts UPSERT (will block on check-exclusion-or-unique-constraint-no-conflict)
    s1.query_until(
        r"starting_upsert_s1",
        "\n\\echo starting_upsert_s1\n"
        "INSERT INTO test.tblpk VALUES (13,now()) ON CONFLICT (i) DO UPDATE SET updated_at = now();\n",
    )

    # Wait for s1 to hit injection point
    _ok_injection_point(node, "check-exclusion-or-unique-constraint-no-conflict")

    # Wakeup s3 to continue (reindex-relation-concurrently-before-set-dead)
    _wakeup_injection_point(node, "reindex-relation-concurrently-before-set-dead")

    # s2 starts UPSERT (will block on exec-insert-before-insert-speculative)
    s2.query_until(
        r"starting_upsert_s2",
        "\n\\echo starting_upsert_s2\n"
        "INSERT INTO test.tblpk VALUES (13,now()) ON CONFLICT (i) DO UPDATE SET updated_at = now();\n",
    )

    # Wait for s2 to hit injection point
    _ok_injection_point(node, "exec-insert-before-insert-speculative")

    # Wakeup s1 (check-exclusion-or-unique-constraint-no-conflict)
    _wakeup_injection_point(node, "check-exclusion-or-unique-constraint-no-conflict")

    # Wakeup s2 (exec-insert-before-insert-speculative)
    _wakeup_injection_point(node, "exec-insert-before-insert-speculative")

    _clean_safe_quit_ok(s1, s2, s3)

    # Cleanup test 1
    node.safe_psql("TRUNCATE TABLE test.tblpk")

    ##########################################################################
    # Test: REINDEX CONCURRENTLY + UPSERT (wakeup at swap phase)

    s1 = node.background_psql("postgres", on_error_stop=False)
    s2 = node.background_psql("postgres", on_error_stop=False)
    s3 = node.background_psql("postgres", on_error_stop=False)

    s1.query_safe(
        """
SELECT injection_points_set_local();
SELECT injection_points_attach('check-exclusion-or-unique-constraint-no-conflict', 'wait');
"""
    )

    s2.query_safe(
        """
SELECT injection_points_set_local();
SELECT injection_points_attach('exec-insert-before-insert-speculative', 'wait');
"""
    )

    s3.query_safe(
        """
SELECT injection_points_set_local();
SELECT injection_points_attach('reindex-relation-concurrently-before-swap', 'wait');
"""
    )

    s3.query_until(
        r"starting_reindex",
        "\n\\echo starting_reindex\nREINDEX INDEX CONCURRENTLY test.tblpk_pkey;\n",
    )

    _ok_injection_point(node, "reindex-relation-concurrently-before-swap")

    s1.query_until(
        r"starting_upsert_s1",
        "\n\\echo starting_upsert_s1\n"
        "INSERT INTO test.tblpk VALUES (13,now()) ON CONFLICT (i) DO UPDATE SET updated_at = now();\n",
    )

    _ok_injection_point(node, "check-exclusion-or-unique-constraint-no-conflict")

    _wakeup_injection_point(node, "reindex-relation-concurrently-before-swap")

    s2.query_until(
        r"starting_upsert_s2",
        "\n\\echo starting_upsert_s2\n"
        "INSERT INTO test.tblpk VALUES (13,now()) ON CONFLICT (i) DO UPDATE SET updated_at = now();\n",
    )

    _ok_injection_point(node, "exec-insert-before-insert-speculative")

    _wakeup_injection_point(node, "exec-insert-before-insert-speculative")
    _wakeup_injection_point(node, "check-exclusion-or-unique-constraint-no-conflict")

    _clean_safe_quit_ok(s1, s2, s3)

    node.safe_psql("TRUNCATE TABLE test.tblpk")

    ##########################################################################
    # Test: REINDEX CONCURRENTLY + UPSERT (s1 wakes before reindex)

    s1 = node.background_psql("postgres", on_error_stop=False)
    s2 = node.background_psql("postgres", on_error_stop=False)
    s3 = node.background_psql("postgres", on_error_stop=False)

    s1.query_safe(
        """
SELECT injection_points_set_local();
SELECT injection_points_attach('check-exclusion-or-unique-constraint-no-conflict', 'wait');
"""
    )

    s2.query_safe(
        """
SELECT injection_points_set_local();
SELECT injection_points_attach('exec-insert-before-insert-speculative', 'wait');
"""
    )

    s3.query_safe(
        """
SELECT injection_points_set_local();
SELECT injection_points_attach('reindex-relation-concurrently-before-set-dead', 'wait');
"""
    )

    s3.query_until(
        r"starting_reindex",
        "\n\\echo starting_reindex\nREINDEX INDEX CONCURRENTLY test.tblpk_pkey;\n",
    )

    _ok_injection_point(node, "reindex-relation-concurrently-before-set-dead")

    s1.query_until(
        r"starting_upsert_s1",
        "\n\\echo starting_upsert_s1\n"
        "INSERT INTO test.tblpk VALUES (13,now()) ON CONFLICT (i) DO UPDATE SET updated_at = now();\n",
    )

    _ok_injection_point(node, "check-exclusion-or-unique-constraint-no-conflict")

    # Start s2 BEFORE waking reindex (key difference from permutation 1)
    s2.query_until(
        r"starting_upsert_s2",
        "\n\\echo starting_upsert_s2\n"
        "INSERT INTO test.tblpk VALUES (13,now()) ON CONFLICT (i) DO UPDATE SET updated_at = now();\n",
    )

    _ok_injection_point(node, "exec-insert-before-insert-speculative")

    # Wake s1 first, then reindex, then s2
    _wakeup_injection_point(node, "check-exclusion-or-unique-constraint-no-conflict")
    _wakeup_injection_point(node, "reindex-relation-concurrently-before-set-dead")
    _wakeup_injection_point(node, "exec-insert-before-insert-speculative")

    _clean_safe_quit_ok(s1, s2, s3)

    node.safe_psql("TRUNCATE TABLE test.tblpk")

    ##########################################################################
    # Test: REINDEX + UPSERT ON CONSTRAINT (set-dead phase)

    s1 = node.background_psql("postgres", on_error_stop=False)
    s2 = node.background_psql("postgres", on_error_stop=False)
    s3 = node.background_psql("postgres", on_error_stop=False)

    s1.query_safe(
        """
SELECT injection_points_set_local();
SELECT injection_points_attach('check-exclusion-or-unique-constraint-no-conflict', 'wait');
"""
    )

    s2.query_safe(
        """
SELECT injection_points_set_local();
SELECT injection_points_attach('exec-insert-before-insert-speculative', 'wait');
"""
    )

    s3.query_safe(
        """
SELECT injection_points_set_local();
SELECT injection_points_attach('reindex-relation-concurrently-before-set-dead', 'wait');
"""
    )

    s3.query_until(
        r"starting_reindex",
        "\n\\echo starting_reindex\nREINDEX INDEX CONCURRENTLY test.tblpk_pkey;\n",
    )

    _ok_injection_point(node, "reindex-relation-concurrently-before-set-dead")

    s1.query_until(
        r"starting_upsert_s1",
        "\n\\echo starting_upsert_s1\n"
        "INSERT INTO test.tblpk VALUES (13, now()) ON CONFLICT ON CONSTRAINT tblpk_pkey DO UPDATE SET updated_at = now();\n",
    )

    _ok_injection_point(node, "check-exclusion-or-unique-constraint-no-conflict")

    _wakeup_injection_point(node, "reindex-relation-concurrently-before-set-dead")

    s2.query_until(
        r"starting_upsert_s2",
        "\n\\echo starting_upsert_s2\n"
        "INSERT INTO test.tblpk VALUES (13, now()) ON CONFLICT ON CONSTRAINT tblpk_pkey DO UPDATE SET updated_at = now();\n",
    )

    _ok_injection_point(node, "exec-insert-before-insert-speculative")

    _wakeup_injection_point(node, "check-exclusion-or-unique-constraint-no-conflict")
    _wakeup_injection_point(node, "exec-insert-before-insert-speculative")

    _clean_safe_quit_ok(s1, s2, s3)

    node.safe_psql("TRUNCATE TABLE test.tblpk")

    ##########################################################################
    # Test: REINDEX + UPSERT ON CONSTRAINT (swap phase)

    s1 = node.background_psql("postgres", on_error_stop=False)
    s2 = node.background_psql("postgres", on_error_stop=False)
    s3 = node.background_psql("postgres", on_error_stop=False)

    s1.query_safe(
        """
SELECT injection_points_set_local();
SELECT injection_points_attach('check-exclusion-or-unique-constraint-no-conflict', 'wait');
"""
    )

    s2.query_safe(
        """
SELECT injection_points_set_local();
SELECT injection_points_attach('exec-insert-before-insert-speculative', 'wait');
"""
    )

    s3.query_safe(
        """
SELECT injection_points_set_local();
SELECT injection_points_attach('reindex-relation-concurrently-before-swap', 'wait');
"""
    )

    s3.query_until(
        r"starting_reindex",
        "\n\\echo starting_reindex\nREINDEX INDEX CONCURRENTLY test.tblpk_pkey;\n",
    )

    _ok_injection_point(node, "reindex-relation-concurrently-before-swap")

    s1.query_until(
        r"starting_upsert_s1",
        "\n\\echo starting_upsert_s1\n"
        "INSERT INTO test.tblpk VALUES (13, now()) ON CONFLICT ON CONSTRAINT tblpk_pkey DO UPDATE SET updated_at = now();\n",
    )

    _ok_injection_point(node, "check-exclusion-or-unique-constraint-no-conflict")

    _wakeup_injection_point(node, "reindex-relation-concurrently-before-swap")

    s2.query_until(
        r"starting_upsert_s2",
        "\n\\echo starting_upsert_s2\n"
        "INSERT INTO test.tblpk VALUES (13, now()) ON CONFLICT ON CONSTRAINT tblpk_pkey DO UPDATE SET updated_at = now();\n",
    )

    _ok_injection_point(node, "exec-insert-before-insert-speculative")

    _wakeup_injection_point(node, "exec-insert-before-insert-speculative")
    _wakeup_injection_point(node, "check-exclusion-or-unique-constraint-no-conflict")

    _clean_safe_quit_ok(s1, s2, s3)

    node.safe_psql("TRUNCATE TABLE test.tblpk")

    ##########################################################################
    # Test: REINDEX + UPSERT ON CONSTRAINT (s1 wakes before reindex)

    s1 = node.background_psql("postgres", on_error_stop=False)
    s2 = node.background_psql("postgres", on_error_stop=False)
    s3 = node.background_psql("postgres", on_error_stop=False)

    s1.query_safe(
        """
SELECT injection_points_set_local();
SELECT injection_points_attach('check-exclusion-or-unique-constraint-no-conflict', 'wait');
"""
    )

    s2.query_safe(
        """
SELECT injection_points_set_local();
SELECT injection_points_attach('exec-insert-before-insert-speculative', 'wait');
"""
    )

    s3.query_safe(
        """
SELECT injection_points_set_local();
SELECT injection_points_attach('reindex-relation-concurrently-before-set-dead', 'wait');
"""
    )

    s3.query_until(
        r"starting_reindex",
        "\n\\echo starting_reindex\nREINDEX INDEX CONCURRENTLY test.tblpk_pkey;\n",
    )

    _ok_injection_point(node, "reindex-relation-concurrently-before-set-dead")

    s1.query_until(
        r"starting_upsert_s1",
        "\n\\echo starting_upsert_s1\n"
        "INSERT INTO test.tblpk VALUES (13, now()) ON CONFLICT ON CONSTRAINT tblpk_pkey DO UPDATE SET updated_at = now();\n",
    )

    _ok_injection_point(node, "check-exclusion-or-unique-constraint-no-conflict")

    # Start s2 BEFORE waking reindex
    s2.query_until(
        r"starting_upsert_s2",
        "\n\\echo starting_upsert_s2\n"
        "INSERT INTO test.tblpk VALUES (13, now()) ON CONFLICT ON CONSTRAINT tblpk_pkey DO UPDATE SET updated_at = now();\n",
    )

    _ok_injection_point(node, "exec-insert-before-insert-speculative")

    # Wake s1 first, then reindex, then s2
    _wakeup_injection_point(node, "check-exclusion-or-unique-constraint-no-conflict")
    _wakeup_injection_point(node, "reindex-relation-concurrently-before-set-dead")
    _wakeup_injection_point(node, "exec-insert-before-insert-speculative")

    _clean_safe_quit_ok(s1, s2, s3)

    node.safe_psql("TRUNCATE TABLE test.tblpk")

    ##########################################################################
    # Test: REINDEX on partitioned table (set-dead phase)

    s1 = node.background_psql("postgres", on_error_stop=False)
    s2 = node.background_psql("postgres", on_error_stop=False)
    s3 = node.background_psql("postgres", on_error_stop=False)

    s1.query_safe(
        """
SELECT injection_points_set_local();
SELECT injection_points_attach('check-exclusion-or-unique-constraint-no-conflict', 'wait');
"""
    )

    s2.query_safe(
        """
SELECT injection_points_set_local();
SELECT injection_points_attach('exec-insert-before-insert-speculative', 'wait');
"""
    )

    s3.query_safe(
        """
SELECT injection_points_set_local();
SELECT injection_points_attach('reindex-relation-concurrently-before-set-dead', 'wait');
"""
    )

    s3.query_until(
        r"starting_reindex",
        "\n\\echo starting_reindex\nREINDEX INDEX CONCURRENTLY test.tbl_partition_pkey;\n",
    )

    _ok_injection_point(node, "reindex-relation-concurrently-before-set-dead")

    s1.query_until(
        r"starting_upsert_s1",
        "\n\\echo starting_upsert_s1\n"
        "INSERT INTO test.tblparted VALUES (13, now()) ON CONFLICT (i) DO UPDATE SET updated_at = now();\n",
    )

    _ok_injection_point(node, "check-exclusion-or-unique-constraint-no-conflict")

    _wakeup_injection_point(node, "reindex-relation-concurrently-before-set-dead")

    s2.query_until(
        r"starting_upsert_s2",
        "\n\\echo starting_upsert_s2\n"
        "INSERT INTO test.tblparted VALUES (13, now()) ON CONFLICT (i) DO UPDATE SET updated_at = now();\n",
    )

    _ok_injection_point(node, "exec-insert-before-insert-speculative")

    _wakeup_injection_point(node, "check-exclusion-or-unique-constraint-no-conflict")
    _wakeup_injection_point(node, "exec-insert-before-insert-speculative")

    _clean_safe_quit_ok(s1, s2, s3)

    node.safe_psql("TRUNCATE TABLE test.tblparted")

    ##########################################################################
    # Test: REINDEX on partitioned table (swap phase)

    s1 = node.background_psql("postgres", on_error_stop=False)
    s2 = node.background_psql("postgres", on_error_stop=False)
    s3 = node.background_psql("postgres", on_error_stop=False)

    s1.query_safe(
        """
SELECT injection_points_set_local();
SELECT injection_points_attach('check-exclusion-or-unique-constraint-no-conflict', 'wait');
"""
    )

    s2.query_safe(
        """
SELECT injection_points_set_local();
SELECT injection_points_attach('exec-insert-before-insert-speculative', 'wait');
"""
    )

    s3.query_safe(
        """
SELECT injection_points_set_local();
SELECT injection_points_attach('reindex-relation-concurrently-before-swap', 'wait');
"""
    )

    s3.query_until(
        r"starting_reindex",
        "\n\\echo starting_reindex\nREINDEX INDEX CONCURRENTLY test.tbl_partition_pkey;\n",
    )

    _ok_injection_point(node, "reindex-relation-concurrently-before-swap")

    s1.query_until(
        r"starting_upsert_s1",
        "\n\\echo starting_upsert_s1\n"
        "INSERT INTO test.tblparted VALUES (13, now()) ON CONFLICT (i) DO UPDATE SET updated_at = now();\n",
    )

    _ok_injection_point(node, "check-exclusion-or-unique-constraint-no-conflict")

    _wakeup_injection_point(node, "reindex-relation-concurrently-before-swap")

    s2.query_until(
        r"starting_upsert_s2",
        "\n\\echo starting_upsert_s2\n"
        "INSERT INTO test.tblparted VALUES (13, now()) ON CONFLICT (i) DO UPDATE SET updated_at = now();\n",
    )

    _ok_injection_point(node, "exec-insert-before-insert-speculative")

    _wakeup_injection_point(node, "exec-insert-before-insert-speculative")
    _wakeup_injection_point(node, "check-exclusion-or-unique-constraint-no-conflict")

    _clean_safe_quit_ok(s1, s2, s3)

    node.safe_psql("TRUNCATE TABLE test.tblparted")

    ##########################################################################
    # Test: REINDEX on partitioned table (s1 wakes before reindex)

    s1 = node.background_psql("postgres", on_error_stop=False)
    s2 = node.background_psql("postgres", on_error_stop=False)
    s3 = node.background_psql("postgres", on_error_stop=False)

    s1.query_safe(
        """
SELECT injection_points_set_local();
SELECT injection_points_attach('check-exclusion-or-unique-constraint-no-conflict', 'wait');
"""
    )

    s2.query_safe(
        """
SELECT injection_points_set_local();
SELECT injection_points_attach('exec-insert-before-insert-speculative', 'wait');
"""
    )

    s3.query_safe(
        """
SELECT injection_points_set_local();
SELECT injection_points_attach('reindex-relation-concurrently-before-set-dead', 'wait');
"""
    )

    s3.query_until(
        r"starting_reindex",
        "\n\\echo starting_reindex\nREINDEX INDEX CONCURRENTLY test.tbl_partition_pkey;\n",
    )

    _ok_injection_point(node, "reindex-relation-concurrently-before-set-dead")

    s1.query_until(
        r"starting_upsert_s1",
        "\n\\echo starting_upsert_s1\n"
        "INSERT INTO test.tblparted VALUES (13, now()) ON CONFLICT (i) DO UPDATE SET updated_at = now();\n",
    )

    _ok_injection_point(node, "check-exclusion-or-unique-constraint-no-conflict")

    # Start s2 BEFORE waking reindex
    s2.query_until(
        r"starting_upsert_s2",
        "\n\\echo starting_upsert_s2\n"
        "INSERT INTO test.tblparted VALUES (13, now()) ON CONFLICT (i) DO UPDATE SET updated_at = now();\n",
    )

    _ok_injection_point(node, "exec-insert-before-insert-speculative")

    # Wake s1 first, then reindex, then s2
    _wakeup_injection_point(node, "check-exclusion-or-unique-constraint-no-conflict")
    _wakeup_injection_point(node, "reindex-relation-concurrently-before-set-dead")
    _wakeup_injection_point(node, "exec-insert-before-insert-speculative")

    _clean_safe_quit_ok(s1, s2, s3)

    node.safe_psql("TRUNCATE TABLE test.tblparted")

    ##########################################################################
    # Test: REINDEX on partitioned table, cache inval between two
    # get_partition_ancestors

    s1 = node.background_psql("postgres", on_error_stop=False)
    s2 = node.background_psql("postgres", on_error_stop=False)
    s3 = node.background_psql("postgres", on_error_stop=False)

    s1.query_safe(
        """
SELECT injection_points_set_local();
SELECT injection_points_attach('exec-init-partition-after-get-partition-ancestors', 'wait');
"""
    )

    s2.query_safe(
        """
SELECT injection_points_set_local();
SELECT injection_points_attach('reindex-relation-concurrently-before-swap', 'wait');
"""
    )

    s2.query_until(
        r"starting_reindex",
        "\n\\echo starting_reindex\nREINDEX INDEX CONCURRENTLY test.tbl_partition_pkey;\n",
    )

    _ok_injection_point(node, "reindex-relation-concurrently-before-swap")

    s1.query_until(
        r"starting_upsert_s1",
        "\n\\echo starting_upsert_s1\n"
        "INSERT INTO test.tblparted VALUES (13, now()) ON CONFLICT (i) DO UPDATE SET updated_at = now();\n",
    )

    _ok_injection_point(node, "exec-init-partition-after-get-partition-ancestors")

    _wakeup_injection_point(node, "reindex-relation-concurrently-before-swap")

    _wakeup_injection_point(node, "exec-init-partition-after-get-partition-ancestors")

    _clean_safe_quit_ok(s1, s2, s3)

    node.safe_psql("TRUNCATE TABLE test.tblparted")

    ##########################################################################
    # Test: CREATE INDEX CONCURRENTLY + UPSERT
    # Uses invalidate-catalog-snapshot-end to test catalog invalidation
    # during UPSERT

    s1 = node.background_psql("postgres", on_error_stop=False)
    s2 = node.background_psql("postgres", on_error_stop=False)
    s3 = node.background_psql("postgres", on_error_stop=False)

    s1_pid = s1.query_safe("SELECT pg_backend_pid()")

    # s1 attaches BOTH injection points - the unique constraint check AND
    # catalog snapshot
    s1.query_safe(
        """
SELECT injection_points_set_local();
SELECT injection_points_attach('check-exclusion-or-unique-constraint-no-conflict', 'wait');
"""
    )

    s1.query_until(
        r"attaching_injection_point",
        "\n\\echo attaching_injection_point\n"
        "SELECT injection_points_attach('invalidate-catalog-snapshot-end', 'wait');\n",
    )

    # In cases of cache clobbering, s1 may hit the injection point during
    # attach.  Wait for that session to become idle (attach completed), or
    # wake it up if it becomes stuck on injection point.
    if not _wait_for_idle(node, s1_pid):
        _ok_injection_point(
            node,
            "invalidate-catalog-snapshot-end",
            "s1 hit injection point during attach (cache clobbering mode)",
        )
        node.safe_psql(
            "\n\t\tSELECT injection_points_wakeup('invalidate-catalog-snapshot-end');\n\t"
        )

    s2.query_safe(
        """
SELECT injection_points_set_local();
SELECT injection_points_attach('exec-insert-before-insert-speculative', 'wait');
"""
    )

    s3.query_safe(
        """
SELECT injection_points_set_local();
SELECT injection_points_attach('define-index-before-set-valid', 'wait');
"""
    )

    # s3: Start CREATE INDEX CONCURRENTLY (blocks on define-index-before-set-valid)
    s3.query_until(
        r"starting_create_index",
        "\n\\echo starting_create_index\n"
        "CREATE UNIQUE INDEX CONCURRENTLY tbl_pkey_duplicate ON test.tblpk(i);\n",
    )

    _ok_injection_point(node, "define-index-before-set-valid")

    # s1: Start UPSERT (blocks on invalidate-catalog-snapshot-end)
    s1.query_until(
        r"starting_upsert_s1",
        "\n\\echo starting_upsert_s1\n"
        "INSERT INTO test.tblpk VALUES (13,now()) ON CONFLICT (i) DO UPDATE SET updated_at = now();\n",
    )

    _ok_injection_point(node, "invalidate-catalog-snapshot-end")

    # Wakeup s3 (CREATE INDEX continues, triggers catalog invalidation)
    _wakeup_injection_point(node, "define-index-before-set-valid")

    # s2: Start UPSERT (blocks on exec-insert-before-insert-speculative)
    s2.query_until(
        r"starting_upsert_s2",
        "\n\\echo starting_upsert_s2\n"
        "INSERT INTO test.tblpk VALUES (13,now()) ON CONFLICT (i) DO UPDATE SET updated_at = now();\n",
    )

    _ok_injection_point(node, "exec-insert-before-insert-speculative")

    _wakeup_injection_point(node, "invalidate-catalog-snapshot-end")

    _ok_injection_point(node, "check-exclusion-or-unique-constraint-no-conflict")

    _wakeup_injection_point(node, "exec-insert-before-insert-speculative")

    _wakeup_injection_point(node, "check-exclusion-or-unique-constraint-no-conflict")

    _clean_safe_quit_ok(s1, s2, s3)

    node.safe_psql("TRUNCATE TABLE test.tblparted")

    ##########################################################################
    # Test: CREATE INDEX CONCURRENTLY on partial index + UPSERT
    # Uses invalidate-catalog-snapshot-end to test catalog invalidation during
    # UPSERT

    s1 = node.background_psql("postgres", on_error_stop=False)
    s2 = node.background_psql("postgres", on_error_stop=False)
    s3 = node.background_psql("postgres", on_error_stop=False)

    s1_pid = s1.query_safe("SELECT pg_backend_pid()")

    # s1 attaches BOTH injection points - the unique constraint check AND
    # catalog snapshot
    s1.query_safe(
        """
SELECT injection_points_set_local();
SELECT injection_points_attach('check-exclusion-or-unique-constraint-no-conflict', 'wait');
"""
    )

    s1.query_until(
        r"attaching_injection_point",
        "\n\\echo attaching_injection_point\n"
        "SELECT injection_points_attach('invalidate-catalog-snapshot-end', 'wait');\n",
    )

    # In cases of cache clobbering, s1 may hit the injection point during
    # attach.  Wait for that session to become idle (attach completed), or
    # wake it up if it becomes stuck on injection point.
    if not _wait_for_idle(node, s1_pid):
        _ok_injection_point(
            node,
            "invalidate-catalog-snapshot-end",
            "s1 hit injection point during attach (cache clobbering mode)",
        )
        node.safe_psql(
            "\n\t\tSELECT injection_points_wakeup('invalidate-catalog-snapshot-end');\n\t"
        )

    s2.query_safe(
        """
SELECT injection_points_set_local();
SELECT injection_points_attach('exec-insert-before-insert-speculative', 'wait');
"""
    )

    s3.query_safe(
        """
SELECT injection_points_set_local();
SELECT injection_points_attach('define-index-before-set-valid', 'wait');
"""
    )

    # s3: Start CREATE INDEX CONCURRENTLY (blocks on define-index-before-set-valid)
    s3.query_until(
        r"starting_create_index",
        "\n\\echo starting_create_index\n"
        "CREATE UNIQUE INDEX CONCURRENTLY tbl_pkey_special_duplicate ON test.tblexpr(abs(i)) WHERE i < 10000;\n",
    )

    _ok_injection_point(node, "define-index-before-set-valid")

    # s1: Start UPSERT (blocks on invalidate-catalog-snapshot-end)
    s1.query_until(
        r"starting_upsert_s1",
        "\n\\echo starting_upsert_s1\n"
        "INSERT INTO test.tblexpr VALUES(13,now()) ON CONFLICT (abs(i)) WHERE i < 100 DO UPDATE SET updated_at = now();\n",
    )

    _ok_injection_point(node, "invalidate-catalog-snapshot-end")

    # Wakeup s3 (CREATE INDEX continues, triggers catalog invalidation)
    _wakeup_injection_point(node, "define-index-before-set-valid")

    # s2: Start UPSERT (blocks on exec-insert-before-insert-speculative)
    s2.query_until(
        r"starting_upsert_s2",
        "\n\\echo starting_upsert_s2\n"
        "INSERT INTO test.tblexpr VALUES(13,now()) ON CONFLICT (abs(i)) WHERE i < 100 DO UPDATE SET updated_at = now();\n",
    )

    _ok_injection_point(node, "exec-insert-before-insert-speculative")
    _wakeup_injection_point(node, "invalidate-catalog-snapshot-end")
    _ok_injection_point(node, "check-exclusion-or-unique-constraint-no-conflict")
    _wakeup_injection_point(node, "exec-insert-before-insert-speculative")
    _wakeup_injection_point(node, "check-exclusion-or-unique-constraint-no-conflict")

    _clean_safe_quit_ok(s1, s2, s3)

    node.safe_psql("TRUNCATE TABLE test.tblexpr")

# Copyright (c) 2022-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long,too-many-statements
"""Port of src/test/modules/commit_ts/t/004_restart.pl.

Commit-timestamp behaviour across server restarts and track_commit_timestamp
GUC toggling, including that timestamps recorded while enabled remain readable
and that querying commit timestamps errors out when the feature is disabled.
Generated from the Perl original via .agent/gen_golden.py.
"""

import re


def test_004_restart(create_pg):
    """Generated golden port of 004_restart."""
    node_primary = create_pg("primary", allows_streaming=True, start=False)
    node_primary.append_conf("track_commit_timestamp = on")
    node_primary.start()
    result = node_primary.psql_capture("SELECT pg_xact_commit_timestamp('0');")
    assert result.exit_code == 3, "getting ts of InvalidTransactionId reports error"
    assert re.search(
        r"""cannot retrieve commit timestamp for transaction""",
        result.stderr,
    ), "expected error from InvalidTransactionId"
    result = node_primary.psql_capture("SELECT pg_xact_commit_timestamp('1');")
    assert result.exit_code == 0, "getting ts of BootstrapTransactionId succeeds"
    assert result.stdout == "", "timestamp of BootstrapTransactionId is null"
    result = node_primary.psql_capture("SELECT pg_xact_commit_timestamp('2');")
    assert result.exit_code == 0, "getting ts of FrozenTransactionId succeeds"
    assert result.stdout == "", "timestamp of FrozenTransactionId is null"
    assert (
        node_primary.safe_psql("SELECT pg_xact_commit_timestamp('3');") == ""
    ), "committs for FirstNormalTransactionId is null"
    node_primary.safe_psql(
        "CREATE TABLE committs_test(x integer, y timestamp with time zone);"
    )
    xid = node_primary.safe_psql(
        "BEGIN;\n\tINSERT INTO committs_test(x, y) VALUES (1, current_timestamp);\n\tSELECT pg_current_xact_id()::xid;\n\tCOMMIT;"
    )
    before_restart_ts = node_primary.safe_psql(
        "SELECT pg_xact_commit_timestamp('" + str(xid) + "');"
    )
    assert (
        before_restart_ts != "" and before_restart_ts != "null"
    ), "commit timestamp recorded"
    node_primary.stop("immediate")
    node_primary.start()
    after_crash_ts = node_primary.safe_psql(
        "SELECT pg_xact_commit_timestamp('" + str(xid) + "');"
    )
    assert (
        after_crash_ts == before_restart_ts
    ), "timestamps before and after crash are equal"
    node_primary.stop("fast")
    node_primary.start()
    after_restart_ts = node_primary.safe_psql(
        "SELECT pg_xact_commit_timestamp('" + str(xid) + "');"
    )
    assert (
        after_restart_ts == before_restart_ts
    ), "timestamps before and after restart are equal"
    node_primary.append_conf("track_commit_timestamp = off")
    node_primary.stop("fast")
    node_primary.start()
    node_primary.restart()
    node_primary.safe_psql(
        "CREATE PROCEDURE consume_xid(cnt int)\nAS $$\nDECLARE\n    i int;\n    BEGIN\n        FOR i in 1..cnt LOOP\n            EXECUTE 'SELECT pg_current_xact_id()';\n            COMMIT;\n        END LOOP;\n    END;\n$$\nLANGUAGE plpgsql;"
    )
    node_primary.safe_psql("CALL consume_xid(2000)")
    result = node_primary.psql_capture(
        "SELECT pg_xact_commit_timestamp('" + str(xid) + "');"
    )
    assert result.exit_code == 3, "no commit timestamp from enable tx when cts disabled"
    assert re.search(
        r"""could not get commit timestamp data""",
        result.stderr,
    ), "expected error from enabled tx when committs disabled"
    xid_disabled = node_primary.safe_psql(
        "BEGIN;\n\tINSERT INTO committs_test(x, y) VALUES (2, current_timestamp);\n\tSELECT pg_current_xact_id();\n\tCOMMIT;"
    )
    result = node_primary.psql_capture(
        "SELECT pg_xact_commit_timestamp('" + str(xid_disabled) + "');"
    )
    assert result.exit_code == 3, "no commit timestamp when disabled"
    assert re.search(
        r"""could not get commit timestamp data""",
        result.stderr,
    ), "expected error from disabled tx when committs disabled"
    node_primary.append_conf("track_commit_timestamp = on")
    node_primary.stop("immediate")
    node_primary.start()
    after_enable_ts = node_primary.safe_psql(
        "SELECT pg_xact_commit_timestamp('" + str(xid) + "');"
    )
    assert after_enable_ts == "", "timestamp of enabled tx null after re-enable"
    after_enable_disabled_ts = node_primary.safe_psql(
        "SELECT pg_xact_commit_timestamp('" + str(xid_disabled) + "');"
    )
    assert (
        after_enable_disabled_ts == ""
    ), "timestamp of disabled tx null after re-enable"
    node_primary.stop()

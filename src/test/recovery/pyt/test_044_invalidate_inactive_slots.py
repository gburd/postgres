# Copyright (c) 2024-2026, PostgreSQL Global Development Group

"""Port of src/test/recovery/t/044_invalidate_inactive_slots.pl.

Idle replication slots are invalidated for idle_timeout: with an injection point
forcing the timeout check, a CHECKPOINT invalidates both a physical and a
logical idle slot (reason 'idle_timeout'), and acquiring the invalidated logical
slot afterward errors. Requires an injection-points build.
"""

import os
import re

import pytest


def _wait_for_slot_invalidation(node, slot_name, offset):
    node.wait_for_log(
        r'invalidating obsolete replication slot "{}"'.format(slot_name), offset
    )
    assert node.poll_query_until(
        "SELECT COUNT(slot_name) = 1 FROM pg_replication_slots\n"
        "    WHERE slot_name = '{}' AND\n"
        "    invalidation_reason = 'idle_timeout';".format(slot_name)
    ), (
        "Timed out while waiting for invalidation reason of slot {} to be "
        "set".format(slot_name)
    )


def test_044_invalidate_inactive_slots(create_pg):
    """Idle physical and logical slots are invalidated on checkpoint."""
    if os.environ.get("enable_injection_points") != "yes":
        pytest.skip("Injection points not supported by this build")
    node = create_pg("node", allows_streaming="logical", start=False)
    node.append_conf(
        "\ncheckpoint_timeout = 1h\nidle_replication_slot_timeout = 1min\n"
    )
    node.start()
    if not node.check_extension("injection_points"):
        pytest.skip("Extension injection_points not installed")
    node.safe_psql(
        "SELECT pg_create_physical_replication_slot(slot_name := 'physical_slot', "
        "immediately_reserve := true);\n"
        "SELECT pg_create_logical_replication_slot('logical_slot', "
        "'test_decoding');"
    )
    log_offset = node.current_log_position()
    node.safe_psql("CREATE EXTENSION injection_points;")
    node.safe_psql("SELECT injection_points_attach('slot-timeout-inval', 'error');")
    node.safe_psql("CHECKPOINT")
    _wait_for_slot_invalidation(node, "physical_slot", log_offset)
    _wait_for_slot_invalidation(node, "logical_slot", log_offset)
    res = node.psql_capture(
        "SELECT pg_replication_slot_advance('logical_slot', '0/1');"
    )
    assert re.search(
        r'can no longer access replication slot "logical_slot"', res.stderr
    ), "detected error upon trying to acquire invalidated slot on node"

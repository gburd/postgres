# Copyright (c) 2024-2026, PostgreSQL Global Development Group

"""Python port of DataChecksums::Utils (src/test/modules/test_checksums/t).

Helpers for driving and observing online data-checksum enable/disable in a
running cluster, shared by the test_checksums pytest suite.
"""

import random
import time


def test_checksum_state(node, state):
    """Assert the data_checksums GUC equals state; return whether it matched."""
    result = node.safe_psql(
        "SELECT setting FROM pg_catalog.pg_settings WHERE name = 'data_checksums';"
    )
    assert result == state, "ensure checksums are set to {} on {}".format(
        state, node.name
    )
    return result == state


def wait_for_checksum_state(node, state):
    """Poll until data_checksums reaches state; assert success; return bool."""
    res = node.poll_query_until(
        "SELECT setting FROM pg_catalog.pg_settings WHERE name = 'data_checksums';",
        state,
    )
    assert res, "ensure data checksums are transitioned to {} on {}".format(
        state, node.name
    )
    return res


def enable_data_checksums(node, cost_delay=0, cost_limit=100, wait=None):
    """Enable data checksums online, optionally waiting for the end state."""
    node.safe_psql(
        "SELECT pg_enable_data_checksums({}, {});".format(cost_delay, cost_limit)
    )
    if wait is not None:
        wait_for_checksum_state(node, wait)
        if wait in ("on", "off"):
            node.poll_query_until(
                "SELECT count(*) = 0 FROM pg_catalog.pg_stat_activity "
                "WHERE backend_type = 'datachecksums launcher';"
            )


def disable_data_checksums(node, wait=None):
    """Disable data checksums, optionally waiting for the off state."""
    node.safe_psql("SELECT pg_disable_data_checksums();")
    if wait is not None:
        wait_for_checksum_state(node, "off")
        node.poll_query_until(
            "SELECT count(*) = 0 FROM pg_catalog.pg_stat_activity "
            "WHERE backend_type = 'datachecksums launcher';"
        )


def cointoss():
    """Return 0 or 1 with even probability."""
    return int(random.random() < 0.5)


def random_sleep(max_seconds=3):
    """Sleep a random (0, max_seconds) interval about half the time."""
    if max_seconds == 0:
        return
    if cointoss():
        time.sleep(int(random.random() * max_seconds))


def stopmode():
    """Pick a valid stop mode ('immediate' or 'fast') at random."""
    return "immediate" if cointoss() else "fast"

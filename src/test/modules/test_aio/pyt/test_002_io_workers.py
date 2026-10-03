# Copyright (c) 2025-2026, PostgreSQL Global Development Group

"""Port of src/test/modules/test_aio/t/002_io_workers.pl.

Test changing the number of I/O worker processes while also evaluating the
handling of their termination.
"""

import random
import re


def _check_io_worker_count(node, worker_count):
    assert node.poll_query_until(
        "SELECT COUNT(*) FROM pg_stat_activity WHERE backend_type = 'io worker'",
        str(worker_count),
    ), "io worker count is {}".format(worker_count)


def _terminate_io_worker(node, worker_count):  # pylint: disable=unused-argument
    # Select a random io worker.
    pid = node.safe_psql(
        "SELECT pid FROM pg_stat_activity WHERE\n"
        "\t\t\tbackend_type = 'io worker' ORDER BY RANDOM() LIMIT 1"
    )

    # terminate IO worker with SIGINT
    node.command_ok(
        ["pg_ctl", "kill", "INT", pid],
        "random io worker process signalled with INT",
    )

    # Check that worker exits
    assert node.poll_query_until(
        "SELECT COUNT(*) FROM pg_stat_activity WHERE pid = {}".format(pid), "0"
    ), "random io worker process exited after signal"


def _change_number_of_io_workers(node, worker_count, prev_worker_count, expect_failure):
    result = node.psql_capture(
        "ALTER SYSTEM SET io_min_workers = {}".format(worker_count)
    )
    node.safe_psql("SELECT pg_reload_conf()")

    if expect_failure:
        assert re.search(
            r'{} is outside the valid range for parameter "io_min_workers"'.format(
                worker_count
            ),
            result.stderr,
        ), "updating io_min_workers to {} failed, as expected".format(worker_count)
        return prev_worker_count

    assert node.safe_psql("SHOW io_min_workers") == str(
        worker_count
    ), "updating number of io_min_workers from {} to {}".format(
        prev_worker_count, worker_count
    )

    _check_io_worker_count(node, worker_count)
    _terminate_io_worker(node, worker_count)
    _check_io_worker_count(node, worker_count)

    return worker_count


def _test_number_of_io_workers_dynamic(node):
    prev_worker_count = node.safe_psql("SHOW io_min_workers")

    # Verify that worker count can't be set to 0
    _change_number_of_io_workers(node, 0, prev_worker_count, True)

    # Verify that worker count can't be set to 33 (above the max)
    _change_number_of_io_workers(node, 33, prev_worker_count, True)

    # Try changing IO workers to a random value and verify that the worker count
    # ends up as expected. Always test the min/max of workers.
    #
    # Valid range for io_workers is [1, 32]. 8 tests in total seems reasonable.
    io_workers_range = list(range(1, 33))
    random.shuffle(io_workers_range)
    for worker_count in (1, 32, io_workers_range[0], io_workers_range[6]):
        prev_worker_count = _change_number_of_io_workers(
            node, worker_count, prev_worker_count, False
        )


def test_002_io_workers(create_pg):
    """Dynamically resize the io worker pool and verify termination handling."""
    node = create_pg("worker", start=False)
    node.append_conf(
        "\n"
        "io_method=worker\n"
        "io_worker_idle_timeout=0ms\n"
        "io_worker_launch_interval=0ms\n"
        "io_max_workers=32\n"
    )

    node.start()

    # Test changing the number of I/O worker processes while also evaluating the
    # handling of their termination.
    _test_number_of_io_workers_dynamic(node)

    node.stop()

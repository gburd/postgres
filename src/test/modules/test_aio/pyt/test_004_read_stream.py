# Copyright (c) 2025-2026, PostgreSQL Global Development Group

"""Port of src/test/modules/test_aio/t/004_read_stream.pl.

Exercises read-stream behaviour across io methods: repeatedly missing/hitting
the same blocks (normal and temp tables), and -- when the build has injection
points -- a read stream encountering buffers undergoing IO in another backend
(succeeding, failing, and two buffers in one IO).
"""

import os

import testaio  # pyrefly: ignore


def _test_repeated_blocks(io_method, node):
    psql = node.background_psql("postgres", on_error_stop=False)

    # Preventing larger reads makes testing easier
    psql.query_safe("SET io_combine_limit = 1")

    # test miss of the same block twice in a row
    psql.query_safe("SELECT evict_rel('largeish');")

    # block 0 grows the distance enough that the stream will look ahead and try
    # to start a pending read for block 2 (and later block 4) twice before
    # returning any buffers.
    psql.query_safe(
        "SELECT * FROM read_stream_for_blocks('largeish',\n"
        "\t\t   ARRAY[0, 2, 2, 4, 4]);"
    )
    # {io_method}: stream missing the same block repeatedly

    psql.query_safe(
        "SELECT * FROM read_stream_for_blocks('largeish',\n"
        "\t\t   ARRAY[0, 2, 2, 4, 4]);"
    )
    # {io_method}: stream hitting the same block repeatedly

    # test hit of the same block twice in a row
    psql.query_safe("SELECT evict_rel('largeish');")
    psql.query_safe(
        "SELECT * FROM read_stream_for_blocks('largeish',\n"
        "\t\t   ARRAY[0, 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1, 0]);"
    )
    # {io_method}: stream accessing same block

    # Test repeated blocks with a temp table, using invalidate_rel_block() to
    # evict individual local buffers.
    psql.query_safe(
        "CREATE TEMP TABLE largeish_temp(k int not null) WITH (FILLFACTOR=10);\n"
        "\t\t   INSERT INTO largeish_temp(k) SELECT generate_series(1, 200);"
    )

    # Evict the specific blocks we'll request to force misses
    psql.query_safe("SELECT invalidate_rel_block('largeish_temp', 0);")
    psql.query_safe("SELECT invalidate_rel_block('largeish_temp', 2);")
    psql.query_safe("SELECT invalidate_rel_block('largeish_temp', 4);")

    psql.query_safe(
        "SELECT * FROM read_stream_for_blocks('largeish_temp',\n"
        "\t\t   ARRAY[0, 2, 2, 4, 4]);"
    )
    # {io_method}: temp stream missing the same block repeatedly

    # Now the blocks are cached, so repeated access should be hits
    psql.query_safe(
        "SELECT * FROM read_stream_for_blocks('largeish_temp',\n"
        "\t\t   ARRAY[0, 2, 2, 4, 4]);"
    )
    # {io_method}: temp stream hitting the same block repeatedly

    psql.quit()


def _wait_completion_wait(node):
    assert node.poll_query_until(
        "SELECT wait_event FROM pg_stat_activity\n"
        "\t\t\tWHERE wait_event = 'completion_wait';",
        "completion_wait",
    )


def _foreign_succeeding(node, psql_a, psql_b, pid_a):
    # Test read stream encountering buffers undergoing IO in another backend,
    # with the other backend's reads succeeding.
    psql_a.query_safe("SELECT evict_rel('largeish');")

    psql_b.query_safe(
        "SELECT inj_io_completion_wait(pid=>pg_backend_pid(),\n"
        "\t\t   relfilenode=>pg_relation_filenode('largeish'));"
    )

    psql_b.send("SELECT read_rel_block_ll('largeish',\n\t\tblockno=>5, nblocks=>1);\n")

    _wait_completion_wait(node)

    # Block 5 is undergoing IO in session b, so session a will move on to start
    # a new IO for block 7.
    psql_a.send(
        "SELECT array_agg(blocknum) FROM\n"
        "\t\tread_stream_for_blocks('largeish', ARRAY[0, 2, 5, 7]);\n"
    )

    assert node.poll_query_until(
        "SELECT wait_event FROM pg_stat_activity WHERE pid = {}".format(pid_a),
        "AioIoCompletion",
    )

    node.safe_psql("SELECT inj_io_completion_continue()")

    psql_a.query_until(r"\{0,2,5,7\}")
    psql_a.clear()
    # {io_method}: read stream encounters succeeding IO by another backend


def _foreign_failing(node, psql_a, psql_b, pid_a):
    # Test read stream encountering buffers undergoing IO in another backend,
    # with the other backend's reads failing.
    psql_a.query_safe("SELECT evict_rel('largeish');")

    psql_b.query_safe(
        "SELECT inj_io_completion_wait(pid=>pg_backend_pid(),\n"
        "\t\t   relfilenode=>pg_relation_filenode('largeish'));"
    )

    psql_b.query_safe(
        "SELECT inj_io_short_read_attach(-errno_from_string('EIO'),\n"
        "\t\t   pid=>pg_backend_pid(),\n"
        "\t\t   relfilenode=>pg_relation_filenode('largeish'));"
    )

    psql_b.send("SELECT read_rel_block_ll('largeish',\n\t\tblockno=>5, nblocks=>1);\n")

    _wait_completion_wait(node)

    psql_a.send(
        "SELECT array_agg(blocknum) FROM\n"
        "\t\tread_stream_for_blocks('largeish', ARRAY[0, 2, 5, 7]);\n"
    )

    assert node.poll_query_until(
        "SELECT wait_event FROM pg_stat_activity WHERE pid = {}".format(pid_a),
        "AioIoCompletion",
    )

    node.safe_psql("SELECT inj_io_completion_continue()")

    psql_a.query_until(r"\{0,2,5,7\}")
    psql_a.clear()

    psql_b.wait_for_stderr(r"ERROR.*could not read blocks 5\.\.5")
    # {io_method}: injected error occurred
    psql_b.query_safe("SELECT inj_io_short_read_detach();")
    # {io_method}: read stream encounters failing IO by another backend


def _foreign_two_buffers(node, psql_a, psql_b, pid_a):
    # Test read stream encountering two buffers that are undergoing the same IO,
    # started by another backend.
    psql_a.query_safe("SELECT evict_rel('largeish');")

    psql_b.query_safe(
        "SELECT inj_io_completion_wait(pid=>pg_backend_pid(),\n"
        "\t\t   relfilenode=>pg_relation_filenode('largeish'));"
    )

    psql_b.send("SELECT read_rel_block_ll('largeish',\n\t\tblockno=>2, nblocks=>3);\n")

    _wait_completion_wait(node)

    # Blocks 2 and 4 are undergoing IO initiated by session b
    psql_a.send(
        "SELECT array_agg(blocknum) FROM\n"
        "\t\tread_stream_for_blocks('largeish', ARRAY[0, 2, 4]);\n"
    )

    assert node.poll_query_until(
        "SELECT wait_event FROM pg_stat_activity WHERE pid = {}".format(pid_a),
        "AioIoCompletion",
    )

    node.safe_psql("SELECT inj_io_completion_continue()")

    psql_a.query_until(r"\{0,2,4\}")
    psql_a.clear()
    # {io_method}: read stream encounters two buffer read in one IO


def _test_inject_foreign(io_method, node):  # pylint: disable=unused-argument
    psql_a = node.background_psql("postgres", on_error_stop=False)
    psql_b = node.background_psql("postgres", on_error_stop=False)

    pid_a = psql_a.query_safe("SELECT pg_backend_pid();")

    _foreign_succeeding(node, psql_a, psql_b, pid_a)
    _foreign_failing(node, psql_a, psql_b, pid_a)
    _foreign_two_buffers(node, psql_a, psql_b, pid_a)

    psql_a.quit()
    psql_b.quit()


def _test_setup(node):
    node.safe_psql(
        "\n"
        "CREATE EXTENSION test_aio;\n"
        "\n"
        "CREATE TABLE largeish(k int not null) WITH (FILLFACTOR=10);\n"
        "INSERT INTO largeish(k) SELECT generate_series(1, 10000);\n"
    )
    # setup


def _test_io_method(io_method, node):
    assert (
        node.safe_psql("SHOW io_method") == io_method
    ), "{}: io_method set correctly".format(io_method)

    _test_repeated_blocks(io_method, node)

    if os.environ.get("enable_injection_points") == "yes":
        _test_inject_foreign(io_method, node)


def test_004_read_stream(create_pg):
    """Drive the read-stream scenarios for each supported io_method."""
    node = create_pg("test", start=False)

    testaio.configure(node)

    node.append_conf("\nmax_connections=8\nio_method=worker\n")

    node.start()
    _test_setup(node)
    node.stop()

    for method in testaio.supported_io_methods():
        node.adjust_conf("io_method", method)
        node.start()
        _test_io_method(method, node)
        node.stop()

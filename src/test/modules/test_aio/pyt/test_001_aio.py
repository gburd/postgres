# Copyright (c) 2025-2026, PostgreSQL Global Development Group

"""Port of src/test/modules/test_aio/t/001_aio.pl.

Exercises the test_aio extension's read/write paths across every supported
io_method: the IO-handle and batchmode APIs, invalid-page and checksum-failure
reporting, StartBufferIO/TerminateBufferIO interplay, foreign-IO completion,
FD-close handling, relation invalidation during IO, ZERO_ON_ERROR /
zero_damaged_pages, cross-database CREATE DATABASE checksum accounting,
StartReadBuffers(), and -- when the build has injection points -- hard IO
errors, short reads and worker-reopen failures.
"""

# pylint: disable=too-many-lines

import os
import re

import testaio  # pyrefly: ignore


def _psql_like(psql, sql, expected_stdout, expected_stderr):
    """Run sql on a background psql and match its stdout/stderr (cf. psql_like).

    Returns the statement's stdout. Mirrors the Perl psql_like helper: the
    statement's stdout must match expected_stdout, its stderr must match
    expected_stderr, and the live stderr buffer is then cleared.
    """
    output = psql.query(sql)
    assert re.search(expected_stdout, output), "expected stdout {!r}, got {!r}".format(
        expected_stdout, output
    )
    assert re.search(
        expected_stderr, psql.last_stderr
    ), "expected stderr {!r}, got {!r}".format(expected_stderr, psql.last_stderr)
    psql.clear()
    return output


def _query_wait_block(node, psql, sql, waitfor, wait_current_session):
    """Issue sql, then wait for waitfor to be observed (cf. query_wait_block).

    If wait_current_session is true, wait for the event in the issuing session,
    otherwise wait for any session.
    """
    pid = psql.query_safe("SELECT pg_backend_pid()")

    psql.send("{};\n".format(sql))

    if wait_current_session:
        waitquery = "SELECT wait_event FROM pg_stat_activity WHERE pid = {}".format(pid)
    else:
        waitquery = (
            "SELECT wait_event FROM pg_stat_activity "
            "WHERE wait_event = '{}'".format(waitfor)
        )

    assert node.poll_query_until(waitquery, waitfor)


def _checksum_failures(psql, datname=None):
    """Return (count, last_failure) for datname, or shared rels if None.

    Mirrors the Perl checksum_failures helper.
    """
    if datname is not None:
        checksum_count = psql.query_safe(
            "\nSELECT checksum_failures FROM pg_stat_database "
            "WHERE datname = '{}';\n".format(datname)
        )
        checksum_last_failure = psql.query_safe(
            "\nSELECT checksum_last_failure FROM pg_stat_database "
            "WHERE datname = '{}';\n".format(datname)
        )
    else:
        checksum_count = psql.query_safe(
            "\nSELECT checksum_failures FROM pg_stat_database "
            "WHERE datname IS NULL;\n"
        )
        checksum_last_failure = psql.query_safe(
            "\nSELECT checksum_last_failure FROM pg_stat_database "
            "WHERE datname IS NULL;\n"
        )
    return checksum_count, checksum_last_failure


def _test_handle(node):
    """Sanity checks for the IO handle API."""
    psql = node.background_psql("postgres", on_error_stop=False)

    # leak warning: implicit xact
    _psql_like(psql, "SELECT handle_get()", r"^$", r"leaked AIO handle")

    # leak warning: explicit xact
    _psql_like(psql, "BEGIN; SELECT handle_get(); COMMIT", r"^$", r"leaked AIO handle")

    # leak warning: explicit xact, rollback
    _psql_like(
        psql, "BEGIN; SELECT handle_get(); ROLLBACK;", r"^$", r"leaked AIO handle"
    )

    # leak warning: subtrans
    _psql_like(
        psql,
        "BEGIN; SAVEPOINT foo; SELECT handle_get(); COMMIT;",
        r"^$",
        r"leaked AIO handle",
    )

    # leak warning + error: released in different command (thus resowner)
    _psql_like(
        psql,
        "BEGIN; SELECT handle_get(); SELECT handle_release_last(); COMMIT;",
        r"^$",
        r"(?s)leaked AIO handle.*release in unexpected state",
    )

    # no leak, release in same command
    _psql_like(
        psql,
        "BEGIN; SELECT handle_get() UNION ALL SELECT handle_release_last(); COMMIT;",
        r"^$",
        r"^$",
    )

    # normal handle use
    _psql_like(psql, "SELECT handle_get_release()", r"^$", r"^$")

    # should error out, API violation
    _psql_like(
        psql,
        "SELECT handle_get_twice()",
        r"^$",
        r"ERROR:  API violation: Only one IO can be handed out$",
    )

    # recover after error in implicit xact
    _psql_like(
        psql,
        "SELECT handle_get_and_error(); SELECT 'ok', handle_get_release()",
        r"^|ok$",
        r"ERROR.*as you command",
    )

    # recover after error in explicit xact
    _psql_like(
        psql,
        "BEGIN; SELECT handle_get_and_error(); "
        "SELECT handle_get_release(), 'ok'; COMMIT;",
        r"^|ok$",
        r"ERROR.*as you command",
    )

    # recover after error in subtrans
    _psql_like(
        psql,
        "BEGIN; SAVEPOINT foo; SELECT handle_get_and_error(); "
        "ROLLBACK TO SAVEPOINT foo; SELECT handle_get_release(); ROLLBACK;",
        r"^|ok$",
        r"ERROR.*as you command",
    )

    psql.quit()


def _test_batchmode(node):
    """Sanity checks for the batchmode API."""
    psql = node.background_psql("postgres", on_error_stop=False)

    # In a build with RELCACHE_FORCE_RELEASE and CATCACHE_FORCE_RELEASE, just
    # using SELECT batch_start() causes spurious test failures, because the
    # lookup of the type information when printing the result tuple also starts
    # a batch. The easiest way around is to not print a result tuple.
    batch_start_sql = "SELECT WHERE batch_start() IS NULL"

    # leak warning & recovery: implicit xact
    _psql_like(psql, batch_start_sql, r"^$", r"open AIO batch at end")

    # leak warning & recovery: explicit xact
    _psql_like(
        psql,
        "BEGIN; {}; COMMIT;".format(batch_start_sql),
        r"^$",
        r"open AIO batch at end",
    )

    # leak warning & recovery: explicit xact, rollback
    #
    # XXX: This doesn't fail right now, due to not getting a chance to do
    # something at transaction command commit. That's not a correctness issue,
    # it just means it's a bit harder to find buggy code.

    # no warning, batch closed in same command
    _psql_like(
        psql,
        "{} UNION ALL SELECT WHERE batch_end() IS NULL".format(batch_start_sql),
        r"^$",
        r"^$",
    )

    psql.quit()


def _test_io_error(node):
    """Test that simple cases of invalid pages are reported."""
    psql = node.background_psql("postgres", on_error_stop=False)

    psql.query_safe(
        "\n"
        "CREATE TEMPORARY TABLE tmp_corr(data int not null);\n"
        "INSERT INTO tmp_corr SELECT generate_series(1, 10000);\n"
        "SELECT modify_rel_block('tmp_corr', 1, corrupt_header=>true);\n"
    )

    for tblname in ("tbl_corr", "tmp_corr"):
        if tblname == "tbl_corr":
            invalid_page_re = r'invalid page in block 1 of relation "base/\d+/\d+'
        else:
            invalid_page_re = r'invalid page in block 1 of relation "base/\d+/t\d+_\d+'

        # verify the error is reported in custom C code
        _psql_like(
            psql,
            "SELECT read_rel_block_ll('{}', 1)".format(tblname),
            r"^$",
            invalid_page_re,
        )

        # verify the error is reported for bufmgr reads, seq scan
        _psql_like(
            psql, "SELECT count(*) FROM {}".format(tblname), r"^$", invalid_page_re
        )

        # verify the error is reported for bufmgr reads, tid scan
        _psql_like(
            psql,
            "SELECT count(*) FROM {} WHERE ctid = '(1, 1)'".format(tblname),
            r"^$",
            invalid_page_re,
        )

    psql.quit()


def _startwait_normal(node, psql_a, psql_b):
    """StartBufferIO/TerminateBufferIO interplay for a normal table."""
    # create a buffer we can play around with
    buf_id = _psql_like(
        psql_a, "SELECT buffer_create_toy('tbl_ok', 1)", r"^\d+$", r"^$"
    )

    # check that one backend can perform StartBufferIO
    _psql_like(
        psql_a,
        "SELECT buffer_call_start_io({}, for_input=>true, wait=>true);".format(buf_id),
        r"^t$",
        r"^$",
    )

    # but not twice on the same buffer (non-waiting)
    _psql_like(
        psql_a,
        "SELECT buffer_call_start_io({}, for_input=>true, wait=>false);".format(buf_id),
        r"^f$",
        r"^$",
    )
    _psql_like(
        psql_b,
        "SELECT buffer_call_start_io({}, for_input=>true, wait=>false);".format(buf_id),
        r"^f$",
        r"^$",
    )

    # start io in a different session, will block
    _query_wait_block(
        node,
        psql_b,
        "SELECT buffer_call_start_io({}, for_input=>true, wait=>true);".format(buf_id),
        "BufferIo",
        1,
    )

    # Terminate the IO, without marking it as success, this should trigger the
    # waiting session to be able to start the io
    _psql_like(
        psql_a,
        "SELECT buffer_call_terminate_io({}, for_input=>true, succeed=>false, "
        "io_error=>false, release_aio=>false)".format(buf_id),
        r"^$",
        r"^$",
    )

    # Because the IO was terminated, but not marked as valid, second session
    # should get the right to start io
    psql_b.query_until(r"t")
    psql_b.clear()

    # terminate the IO again
    psql_b.query_safe(
        "SELECT buffer_call_terminate_io({}, for_input=>true, succeed=>false, "
        "io_error=>false, release_aio=>false);".format(buf_id)
    )

    # same as the above scenario, but mark IO as having succeeded
    _psql_like(
        psql_a,
        "SELECT buffer_call_start_io({}, for_input=>true, wait=>true);".format(buf_id),
        r"^t$",
        r"^$",
    )

    # start io in a different session, will block
    _query_wait_block(
        node,
        psql_b,
        "SELECT buffer_call_start_io({}, for_input=>true, wait=>true);".format(buf_id),
        "BufferIo",
        1,
    )

    # Terminate the IO, marking it as success
    _psql_like(
        psql_a,
        "SELECT buffer_call_terminate_io({}, for_input=>true, succeed=>true, "
        "io_error=>false, release_aio=>false)".format(buf_id),
        r"^$",
        r"^$",
    )

    # Because the IO was terminated, and marked as valid, second session should
    # complete but not need io
    psql_b.query_until(r"f")
    psql_b.clear()

    # buffer is valid now, make it invalid again
    psql_a.query_safe("SELECT buffer_create_toy('tbl_ok', 1);")


def _startwait_temp(node, psql_a):  # pylint: disable=unused-argument
    """StartLocalBufferIO behaviour for a temporary table."""
    # create a buffer we can play around with
    psql_a.query_safe(
        "\n"
        "CREATE TEMPORARY TABLE tmp_ok(data int not null);\n"
        "INSERT INTO tmp_ok SELECT generate_series(1, 10000);\n"
    )
    buf_id = psql_a.query_safe("SELECT buffer_create_toy('tmp_ok', 3);")

    # check that one backend can perform StartLocalBufferIO
    _psql_like(
        psql_a,
        "SELECT buffer_call_start_io({}, for_input=>true, wait=>false);".format(buf_id),
        r"^t$",
        r"^$",
    )

    # Because local buffers don't use IO_IN_PROGRESS, a second
    # StartLocalBufferIO succeeds as well. This test mostly serves as a
    # documentation of that fact. If we had actually started IO, it'd be
    # different.
    _psql_like(
        psql_a,
        "SELECT buffer_call_start_io({}, for_input=>true, wait=>false);".format(buf_id),
        r"^t$",
        r"^$",
    )

    # Terminate the IO again, without marking it as a success
    psql_a.query_safe(
        "SELECT buffer_call_terminate_io({}, for_input=>true, succeed=>false, "
        "io_error=>false, release_aio=>false);".format(buf_id)
    )
    _psql_like(
        psql_a,
        "SELECT buffer_call_start_io({}, for_input=>true, wait=>false);".format(buf_id),
        r"^t$",
        r"^$",
    )

    # Terminate the IO again, marking it as a success
    psql_a.query_safe(
        "SELECT buffer_call_terminate_io({}, for_input=>true, succeed=>true, "
        "io_error=>false, release_aio=>false);".format(buf_id)
    )

    # Now another StartLocalBufferIO should fail, this time because the buffer
    # is already valid.
    _psql_like(
        psql_a,
        "SELECT buffer_call_start_io({}, for_input=>true, wait=>true);".format(buf_id),
        r"^f$",
        r"^$",
    )


def _test_startwait_io(node):
    """Test interplay between StartBufferIO and TerminateBufferIO."""
    psql_a = node.background_psql("postgres", on_error_stop=False)
    psql_b = node.background_psql("postgres", on_error_stop=False)

    _startwait_normal(node, psql_a, psql_b)
    _startwait_temp(node, psql_a)

    psql_a.quit()
    psql_b.quit()


def _test_complete_foreign(node):
    """If the issuing backend doesn't wait, another backend completes the IO."""
    psql_a = node.background_psql("postgres", on_error_stop=False)
    psql_b = node.background_psql("postgres", on_error_stop=False)

    # Issue IO without waiting for completion, then sleep
    psql_a.query_safe("SELECT read_rel_block_ll('tbl_ok', 1, wait_complete=>false);")

    # Check that another backend can read the relevant block
    _psql_like(
        psql_b,
        "SELECT count(*) FROM tbl_ok WHERE ctid = '(1,1)' LIMIT 1",
        r"^1$",
        r"^$",
    )

    # Issue IO without waiting for completion, then exit.
    psql_a.query_safe("SELECT read_rel_block_ll('tbl_ok', 1, wait_complete=>false);")
    psql_a.reconnect_and_clear()

    # Check that another backend can read the relevant block. This verifies that
    # the exiting backend left the AIO in a sane state.
    _psql_like(
        psql_b,
        "SELECT count(*) FROM tbl_ok WHERE ctid = '(1,1)' LIMIT 1",
        r"^1$",
        r"^$",
    )

    # Read a tbl_corr block, then sleep. The other session will retry the IO and
    # also fail. The easiest thing to verify that seems to be to check that both
    # are in the log.
    log_location = node.current_log_position()
    psql_a.query_safe("SELECT read_rel_block_ll('tbl_corr', 1, wait_complete=>false);")

    _psql_like(
        psql_b,
        "SELECT count(*) FROM tbl_corr WHERE ctid = '(1,1)' LIMIT 1",
        r"^$",
        r"invalid page in block",
    )

    # The log message issued for the read_rel_block_ll() should be logged as a
    # LOG
    node.wait_for_log(r"LOG[^\n]+invalid page in", log_location)

    # But for the SELECT, it should be an ERROR
    node.wait_for_log(r"ERROR[^\n]+invalid page in", log_location)

    psql_a.quit()
    psql_b.quit()


def _test_close_fd(node):
    """Test that we deal correctly with FDs being closed while IO is in progress."""
    psql = node.background_psql("postgres", on_error_stop=False)

    _psql_like(
        psql,
        "\n"
        "\t\t\tSELECT read_rel_block_ll('tbl_ok', 1,\n"
        "\t\t\t\twait_complete=>true,\n"
        "\t\t\t\tbatchmode_enter=>true,\n"
        "\t\t\t\tsmgrreleaseall=>true,\n"
        "\t\t\t\tbatchmode_exit=>true\n"
        "\t\t\t);",
        r"^$",
        r"^$",
    )

    _psql_like(
        psql,
        "\n"
        "\t\t\tSELECT read_rel_block_ll('tbl_ok', 1,\n"
        "\t\t\t\twait_complete=>false,\n"
        "\t\t\t\tbatchmode_enter=>true,\n"
        "\t\t\t\tsmgrreleaseall=>true,\n"
        "\t\t\t\tbatchmode_exit=>true\n"
        "\t\t\t);",
        r"^$",
        r"^$",
    )

    # Check that another backend can read the relevant block
    _psql_like(
        psql,
        "SELECT count(*) FROM tbl_ok WHERE ctid = '(1,1)' LIMIT 1",
        r"^1$",
        r"^$",
    )

    psql.quit()


def _test_inject(node):
    """Tests using injection points, mostly to exercise hard IO errors."""
    psql = node.background_psql("postgres", on_error_stop=False)

    # injected what we'd expect
    psql.query_safe("SELECT inj_io_short_read_attach(8192);")
    psql.query_safe("SELECT invalidate_rel_block('tbl_ok', 2);")
    _psql_like(psql, "SELECT count(*) FROM tbl_ok WHERE ctid = '(2, 1)'", r"^1$", r"^$")

    # injected a read shorter than a single block, expecting error
    psql.query_safe("SELECT inj_io_short_read_attach(17);")
    psql.query_safe("SELECT invalidate_rel_block('tbl_ok', 2);")
    _psql_like(
        psql,
        "SELECT count(*) FROM tbl_ok WHERE ctid = '(2, 1)'",
        r"^$",
        r'ERROR:.*could not read blocks 2\.\.2 in file "base/.*": '
        r"read only 0 of 8192 bytes",
    )

    # shorten multi-block read to a single block, should retry
    inval_query = (
        "SELECT invalidate_rel_block('tbl_ok', 0);\n"
        "SELECT invalidate_rel_block('tbl_ok', 1);\n"
        "SELECT invalidate_rel_block('tbl_ok', 2);\n"
        "SELECT invalidate_rel_block('tbl_ok', 3);\n"
        "/* gap */\n"
        "SELECT invalidate_rel_block('tbl_ok', 5);\n"
        "SELECT invalidate_rel_block('tbl_ok', 6);\n"
        "SELECT invalidate_rel_block('tbl_ok', 7);\n"
        "SELECT invalidate_rel_block('tbl_ok', 8);"
    )

    psql.query_safe(inval_query)
    psql.query_safe("SELECT inj_io_short_read_attach(8192);")
    _psql_like(psql, "SELECT count(*) FROM tbl_ok", r"^10000$", r"^$")

    # shorten multi-block read to two blocks, should retry
    psql.query_safe(inval_query)
    psql.query_safe("SELECT inj_io_short_read_attach(8192*2);")
    _psql_like(psql, "SELECT count(*) FROM tbl_ok", r"^10000$", r"^$")

    # verify that page verification errors are detected even as part of a
    # shortened multi-block read (tbl_corr, block 1 is corrupted)
    psql.query_safe(
        "\n"
        "SELECT invalidate_rel_block('tbl_corr', 0);\n"
        "SELECT invalidate_rel_block('tbl_corr', 1);\n"
        "SELECT invalidate_rel_block('tbl_corr', 2);\n"
        "SELECT inj_io_short_read_attach(8192);\n"
        "\t"
    )
    _psql_like(
        psql,
        "SELECT count(*) FROM tbl_corr WHERE ctid < '(2, 1)'",
        r"^$",
        r'ERROR:.*invalid page in block 1 of relation "base/.*',
    )

    # trigger a hard error, should error out
    psql.query_safe(
        "\n"
        "SELECT inj_io_short_read_attach(-errno_from_string('EIO'));\n"
        "SELECT invalidate_rel_block('tbl_ok', 2);\n"
        "\t"
    )
    hard_eio = (
        r'ERROR:.*could not read blocks 2\.\.2 in file "base/.*": '
        r"(?:I/O|Input/output) error"
    )
    _psql_like(psql, "SELECT count(*) FROM tbl_ok", r"^$", hard_eio)
    _psql_like(psql, "SELECT count(*) FROM tbl_ok", r"^$", hard_eio)

    psql.query_safe("SELECT inj_io_short_read_detach()")

    # now the IO should be ok.
    _psql_like(psql, "SELECT count(*) FROM tbl_ok", r"^10000$", r"^$")

    # trigger a different hard error, should error out
    psql.query_safe(
        "\n"
        "SELECT inj_io_short_read_attach(-errno_from_string('EROFS'));\n"
        "SELECT invalidate_rel_block('tbl_ok', 2);\n"
        "\t"
    )
    _psql_like(
        psql,
        "SELECT count(*) FROM tbl_ok",
        r"^$",
        r'ERROR:.*could not read blocks 2\.\.2 in file "base/.*": '
        r"Read-only file system",
    )
    psql.query_safe("SELECT inj_io_short_read_detach()")

    psql.quit()


def _test_inject_worker(node):
    """Tests using injection points, only for io_method=worker (file reopen)."""
    psql = node.background_psql("postgres", on_error_stop=False)

    # trigger a failure to reopen, should error out, but should recover
    psql.query_safe(
        "\nSELECT inj_io_reopen_attach();\n"
        "SELECT invalidate_rel_block('tbl_ok', 1);\n\t"
    )

    _psql_like(
        psql,
        "SELECT count(*) FROM tbl_ok",
        r"^$",
        r'ERROR:.*could not read blocks 1\.\.1 in file "base/.*": '
        r"No such file or directory",
    )

    psql.query_safe("SELECT inj_io_reopen_detach();")

    # check that we indeed recover
    _psql_like(psql, "SELECT count(*) FROM tbl_ok", r"^10000$", r"^$")

    psql.quit()


def _test_invalidate(node):
    """Handle a relation being removed (rollback/DROP) while IO is ongoing."""
    psql = node.background_psql("postgres", on_error_stop=False)

    for persistency in ("normal", "unlogged", "temporary"):
        sql_persistency = "" if persistency == "normal" else persistency
        tblname = persistency + "_transactional"

        create_sql = (
            "\n"
            "CREATE {persistency} TABLE {tbl} (id int not null, data text not null) "
            "WITH (AUTOVACUUM_ENABLED = false);\n"
            "INSERT INTO {tbl}(id, data) SELECT generate_series(1, 10000) as id, "
            "repeat('a', 200);\n".format(persistency=sql_persistency, tbl=tblname)
        )

        # Verify that outstanding read IO does not cause problems with
        # AbortTransaction -> smgrDoPendingDeletes -> smgrdounlinkall -> ... ->
        # Invalidate[Local]Buffer.
        psql.query_safe("BEGIN; {};".format(create_sql))
        psql.query_safe(
            "\nSELECT read_rel_block_ll('{}', 1, wait_complete=>false);\n".format(
                tblname
            )
        )
        _psql_like(psql, "ROLLBACK", r"^$", r"^$")

        # Verify that outstanding read IO does not cause problems with
        # CommitTransaction -> smgrDoPendingDeletes -> smgrdounlinkall -> ... ->
        # Invalidate[Local]Buffer.
        psql.query_safe("BEGIN; {}; COMMIT;".format(create_sql))
        psql.query_safe(
            "\nBEGIN;\n"
            "SELECT read_rel_block_ll('{}', 1, wait_complete=>false);\n".format(tblname)
        )

        _psql_like(psql, "DROP TABLE {}".format(tblname), r"^$", r"^$")
        _psql_like(psql, "COMMIT", r"^$", r"^$")

    psql.quit()


def _test_zero(node):
    """Test behavior related to ZERO_ON_ERROR and zero_damaged_pages."""
    psql_a = node.background_psql("postgres", on_error_stop=False)
    psql_b = node.background_psql("postgres", on_error_stop=False)

    for persistency in ("normal", "temporary"):
        sql_persistency = "" if persistency == "normal" else persistency
        _zero_one_persistency(psql_a, psql_b, persistency, sql_persistency)

    psql_a.clear()

    psql_a.quit()
    psql_b.quit()


def _zero_one_persistency(psql_a, psql_b, persistency, sql_persistency):
    """One ZERO_ON_ERROR / zero_damaged_pages pass for a persistency level."""
    psql_a.query_safe(
        "\nCREATE {} TABLE tbl_zero(id int) WITH (AUTOVACUUM_ENABLED = false);\n"
        "INSERT INTO tbl_zero SELECT generate_series(1, 10000);\n".format(
            sql_persistency
        )
    )

    psql_a.query_safe(
        "\nSELECT modify_rel_block('tbl_zero', 0, corrupt_header=>true);\n"
    )

    # Check that page validity errors are detected
    _psql_like(
        psql_a,
        "\nSELECT read_rel_block_ll('tbl_zero', 0, zero_on_error=>false)",
        r"^$",
        r"^psql:<stdin>:\d+: ERROR:  invalid page in block 0 of relation "
        r'"base/.*/.*$',
    )

    # Check that page validity errors are zeroed
    _psql_like(
        psql_a,
        "\nSELECT read_rel_block_ll('tbl_zero', 0, zero_on_error=>true)",
        r"^$",
        r"^psql:<stdin>:\d+: WARNING:  invalid page in block 0 of relation "
        r'"base/.*/.*"; zeroing out page$',
    )

    # And that once the corruption is fixed, we can read again
    psql_a.query("\nSELECT modify_rel_block('tbl_zero', 0, zero=>true);\n")
    psql_a.clear()

    _psql_like(
        psql_a,
        "\nSELECT read_rel_block_ll('tbl_zero', 0, zero_on_error=>false)",
        r"^$",
        r"^$",
    )

    # Check a page validity error in another block, to ensure we report the
    # correct block number
    psql_a.query_safe(
        "\nSELECT modify_rel_block('tbl_zero', 3, corrupt_header=>true);\n"
    )
    _psql_like(
        psql_a,
        "SELECT read_rel_block_ll('tbl_zero', 3, zero_on_error=>true);",
        r"^$",
        r"^psql:<stdin>:\d+: WARNING:  invalid page in block 3 of relation "
        r'"base/.*/.*"; zeroing out page$',
    )

    _zero_multiblock(psql_a, persistency)
    _zero_bufmgr(psql_a, psql_b, persistency, sql_persistency)

    # Clean up
    psql_a.query_safe("\nDROP TABLE tbl_zero;\n")


def _zero_multiblock(psql_a, persistency):  # pylint: disable=unused-argument
    """Check one read reporting multiple invalid blocks (error/zero variants)."""
    psql_a.query_safe(
        "\nSELECT modify_rel_block('tbl_zero', 2, corrupt_header=>true);\n"
        "SELECT modify_rel_block('tbl_zero', 3, corrupt_header=>true);\n"
    )
    # First test error
    _psql_like(
        psql_a,
        "SELECT read_rel_block_ll('tbl_zero', 1, nblocks=>4, zero_on_error=>false)",
        r"^$",
        r"^psql:<stdin>:\d+: ERROR:  2 invalid pages among blocks 1..4 of "
        r'relation "base/.*/.*\nDETAIL:  Block 2 held the first invalid page\.\n'
        r"HINT:[^\n]+$",
    )

    # Then test zeroing via ZERO_ON_ERROR flag
    _psql_like(
        psql_a,
        "SELECT read_rel_block_ll('tbl_zero', 1, nblocks=>4, zero_on_error=>true)",
        r"^$",
        r"^psql:<stdin>:\d+: WARNING:  zeroing out 2 invalid pages among "
        r'blocks 1..4 of relation "base/.*/.*\nDETAIL:  Block 2 held the first '
        r"zeroed page\.\nHINT:[^\n]+$",
    )

    # Then test zeroing via zero_damaged_pages
    _psql_like(
        psql_a,
        "\nBEGIN;\n"
        "SET LOCAL zero_damaged_pages = true;\n"
        "SELECT read_rel_block_ll('tbl_zero', 1, nblocks=>4, zero_on_error=>false)\n"
        "COMMIT;\n",
        r"^$",
        r"^psql:<stdin>:\d+: WARNING:  zeroing out 2 invalid pages among "
        r'blocks 1..4 of relation "base/.*/.*\nDETAIL:  Block 2 held the first '
        r"zeroed page\.\nHINT:[^\n]+$",
    )

    psql_a.query_safe("COMMIT")


def _zero_bufmgr(psql_a, psql_b, persistency, sql_persistency):
    """Verify bufmgr.c IO detects / zeroes page validity errors."""
    psql_a.query(
        "\nSELECT invalidate_rel_block('tbl_zero', g.i)\n"
        "FROM generate_series(0, 15) g(i);\n"
        "SELECT modify_rel_block('tbl_zero', 3, zero=>true);\n"
    )
    psql_a.clear()

    _psql_like(
        psql_a,
        "\nSELECT count(*) FROM tbl_zero",
        r"^$",
        r"^psql:<stdin>:\d+: ERROR:  invalid page in block 2 of relation "
        r'"base/.*/.*$',
    )

    # Verify that bufmgr.c IO zeroes out pages with page validity errors
    _psql_like(
        psql_a,
        "\nBEGIN;\n"
        "SET LOCAL zero_damaged_pages = true;\n"
        "SELECT count(*) FROM tbl_zero;\n"
        "COMMIT;\n",
        r"^\d+$",
        r"^psql:<stdin>:\d+: WARNING:  invalid page in block 2 of relation "
        r'"base/.*/.*$',
    )

    # Check that warnings/errors about page validity in an IO started by session
    # A that session B might complete aren't logged visibly to session B.
    #
    # This will only ever trigger for io_method's like io_uring, that can
    # complete IO's in a client backend. But it doesn't seem worth restricting
    # to that.
    #
    # This requires cross-session access to the same relation, hence the
    # restriction to non-temporary table.
    if sql_persistency != "temporary":
        # Create a corruption and then read the block without waiting for
        # completion.
        psql_a.query(
            "\nSELECT modify_rel_block('tbl_zero', 1, corrupt_header=>true);\n"
            "SELECT read_rel_block_ll('tbl_zero', 1, wait_complete=>false, "
            "zero_on_error=>true)\n"
        )

        _psql_like(psql_b, "SELECT count(*) > 0 FROM tbl_zero;", r"^t$", r"^$")


def _test_checksum(node):
    """Test that we detect checksum failures and report them."""
    psql_a = node.background_psql("postgres", on_error_stop=False)

    psql_a.query_safe(
        "\nCREATE TABLE tbl_normal(id int) WITH (AUTOVACUUM_ENABLED = false);\n"
        "INSERT INTO tbl_normal SELECT generate_series(1, 5000);\n"
        "SELECT modify_rel_block('tbl_normal', 3, corrupt_checksum=>true);\n"
        "\n"
        "CREATE TEMPORARY TABLE tbl_temp(id int) WITH (AUTOVACUUM_ENABLED = false);\n"
        "INSERT INTO tbl_temp SELECT generate_series(1, 5000);\n"
        "SELECT modify_rel_block('tbl_temp', 3, corrupt_checksum=>true);\n"
        "SELECT modify_rel_block('tbl_temp', 4, corrupt_checksum=>true);\n"
    )

    # To be able to test checksum failures on shared rels we need a shared rel
    # with invalid pages - which is a bit scary. pg_shseclabel seems like a good
    # bet, as it's not accessed in a default configuration.
    psql_a.query_safe(
        "\nSELECT grow_rel('pg_shseclabel', 4);\n"
        "SELECT modify_rel_block('pg_shseclabel', 2, corrupt_checksum=>true);\n"
        "SELECT modify_rel_block('pg_shseclabel', 3, corrupt_checksum=>true);\n"
    )

    # normal rel
    cs_count_before, _ = _checksum_failures(psql_a, "postgres")
    _psql_like(
        psql_a,
        "\nSELECT read_rel_block_ll('tbl_normal', 3, nblocks=>1, "
        "zero_on_error=>false);",
        r"^$",
        r"^psql:<stdin>:\d+: ERROR:  invalid page in block 3 of relation "
        r'"base/\d+/\d+"$',
    )
    cs_count_after, cs_ts_after = _checksum_failures(psql_a, "postgres")
    assert int(cs_count_before) + 1 <= int(cs_count_after), "normal rel checksum count"
    assert cs_ts_after != "", "normal rel checksum timestamp is not null"

    # temp rel
    cs_count_after, cs_ts_after = _checksum_failures(psql_a, "postgres")
    _psql_like(
        psql_a,
        "\nSELECT read_rel_block_ll('tbl_temp', 4, nblocks=>2, "
        "zero_on_error=>false);",
        r"^$",
        r"^psql:<stdin>:\d+: ERROR:  invalid page in block 4 of relation "
        r'"base/\d+/t\d+_\d+"$',
    )
    cs_count_after, cs_ts_after = _checksum_failures(psql_a, "postgres")
    assert int(cs_count_before) + 1 <= int(cs_count_after), "temp rel checksum count"
    assert cs_ts_after != "", "temp rel checksum timestamp is not null"

    # shared rel
    cs_count_before, cs_ts_after = _checksum_failures(psql_a)
    _psql_like(
        psql_a,
        "\nSELECT read_rel_block_ll('pg_shseclabel', 2, nblocks=>2, "
        "zero_on_error=>false);",
        r"^$",
        r"^psql:<stdin>:\d+: ERROR:  2 invalid pages among blocks 2..3 of "
        r'relation "global/\d+"\nDETAIL:  Block 2 held the first invalid page\.\n'
        r"HINT:[^\n]+$",
    )
    cs_count_after, cs_ts_after = _checksum_failures(psql_a)
    assert int(cs_count_before) + 1 <= int(cs_count_after), "shared rel checksum count"
    assert cs_ts_after != "", "shared rel checksum timestamp is not null"

    # and restore sanity
    psql_a.query(
        "\nSELECT modify_rel_block('pg_shseclabel', 1, zero=>true);\n"
        "DROP TABLE tbl_normal;\n"
    )
    psql_a.clear()

    psql_a.quit()


def _test_checksum_createdb(node):
    """CREATE DATABASE from a source with an invalid block (cross-database IO)."""
    psql = node.background_psql("postgres", on_error_stop=False)

    node.safe_psql("CREATE DATABASE regression_createdb_source")

    node.safe_psql(
        "\nCREATE EXTENSION test_aio;\n"
        "CREATE TABLE tbl_cs_fail(data int not null) "
        "WITH (AUTOVACUUM_ENABLED = false);\n"
        "INSERT INTO tbl_cs_fail SELECT generate_series(1, 1000);\n"
        "SELECT modify_rel_block('tbl_cs_fail', 1, corrupt_checksum=>true);\n",
        dbname="regression_createdb_source",
    )

    createdb_sql = (
        "\nCREATE DATABASE regression_createdb_target\n"
        "TEMPLATE regression_createdb_source\n"
        "STRATEGY wal_log;\n"
    )

    # Verify that CREATE DATABASE of an invalid database fails and is accounted
    # for accurately.
    cs_count_before, _ = _checksum_failures(psql, "regression_createdb_source")
    _psql_like(
        psql,
        createdb_sql,
        r"^$",
        r"psql:<stdin>:\d+: ERROR:  invalid page in block 1 of relation "
        r'"base/\d+/\d+"$',
    )
    cs_count_after, _ = _checksum_failures(psql, "regression_createdb_source")
    assert int(cs_count_before) + 1 <= int(
        cs_count_after
    ), "create database w/ wal strategy, invalid source: checksum count increased"

    # Verify that CREATE DATABASE of the fixed database succeeds.
    node.safe_psql(
        "\nSELECT modify_rel_block('tbl_cs_fail', 1, zero=>true);\n",
        dbname="regression_createdb_source",
    )
    _psql_like(psql, createdb_sql, r"^$", r"^$")

    psql.quit()


def _ignore_checksum_basic(psql, count_sql, invalidate_sql, expect):
    """Very basic ignore_checksum_failure=off / on tests."""
    psql.query_safe(
        "\nSELECT modify_rel_block('tbl_cs_fail', 1, corrupt_checksum=>true);\n"
        "SELECT modify_rel_block('tbl_cs_fail', 5, corrupt_checksum=>true);\n"
        "SELECT modify_rel_block('tbl_cs_fail', 6, corrupt_checksum=>true);\n"
    )

    psql.query_safe(invalidate_sql)
    _psql_like(psql, count_sql, r"^$", r"ERROR:  invalid page in block")

    psql.query_safe("SET ignore_checksum_failure=on")

    psql.query_safe(invalidate_sql)
    _psql_like(
        psql,
        count_sql,
        r"^{}$".format(expect),
        r"WARNING:  ignoring (checksum failure|\d checksum failures)",
    )


def _ignore_checksum_multiblock(node, psql):
    """Verify ignore_checksum_failure=off works in multi-block reads."""
    psql.query_safe(
        "\nSELECT modify_rel_block('tbl_cs_fail', 2, zero=>true);\n"
        "SELECT modify_rel_block('tbl_cs_fail', 3, corrupt_checksum=>true);\n"
        "SELECT modify_rel_block('tbl_cs_fail', 4, corrupt_header=>true);\n"
    )

    log_location = node.current_log_position()
    _psql_like(
        psql,
        "\nSELECT read_rel_block_ll('tbl_cs_fail', 3, nblocks=>1, "
        "zero_on_error=>false);",
        r"^$",
        r"^psql:<stdin>:\d+: WARNING:  ignoring checksum failure in block 3",
    )

    # Check that the log contains a LOG message about the failure
    node.wait_for_log(r"LOG:  ignoring checksum failure", log_location)

    # check that we error
    _psql_like(
        psql,
        "\nSELECT read_rel_block_ll('tbl_cs_fail', 2, nblocks=>3, "
        "zero_on_error=>false);",
        r"^$",
        r"^psql:<stdin>:\d+: ERROR:  invalid page in block 4 of relation "
        r'"base/\d+/\d+"$',
    )


def _ignore_checksum_multiproblem(node, psql):
    """Multi-block read with different problems in different blocks."""
    psql.query(
        "\nSELECT modify_rel_block('tbl_cs_fail', 1, zero=>true);\n"
        "SELECT modify_rel_block('tbl_cs_fail', 2, corrupt_checksum=>true);\n"
        "SELECT modify_rel_block('tbl_cs_fail', 3, corrupt_checksum=>true, "
        "corrupt_header=>true);\n"
        "SELECT modify_rel_block('tbl_cs_fail', 4, corrupt_header=>true);\n"
        "SELECT modify_rel_block('tbl_cs_fail', 5, corrupt_header=>true);\n"
    )
    psql.clear()

    log_location = node.current_log_position()
    _psql_like(
        psql,
        "\nSELECT read_rel_block_ll('tbl_cs_fail', 1, nblocks=>5, "
        "zero_on_error=>true);",
        r"^$",
        r"^psql:<stdin>:\d+: WARNING:  zeroing 3 page\(s\) and ignoring 2 "
        r'checksum failure\(s\) among blocks 1..5 of relation "',
    )

    # Unfortunately have to scan the whole log since determining $log_location
    # above in each of the tests, as wait_for_log() returns the size of the
    # file.
    node.wait_for_log(r"LOG:  ignoring checksum failure in block 2", log_location)
    node.wait_for_log(
        r'LOG:  invalid page in block 3 of relation "base.*"; zeroing out page',
        log_location,
    )
    node.wait_for_log(
        r'LOG:  invalid page in block 4 of relation "base.*"; zeroing out page',
        log_location,
    )
    node.wait_for_log(
        r'LOG:  invalid page in block 5 of relation "base.*"; zeroing out page',
        log_location,
    )


def _ignore_checksum_both(psql):
    """Reading a page with both an invalid header and an invalid checksum."""
    psql.query(
        "\nSELECT modify_rel_block('tbl_cs_fail', 3, corrupt_checksum=>true, "
        "corrupt_header=>true);\n"
    )
    psql.clear()

    _psql_like(
        psql,
        "\nSELECT read_rel_block_ll('tbl_cs_fail', 3, nblocks=>1, "
        "zero_on_error=>false);",
        r"^$",
        r"^psql:<stdin>:\d+: ERROR:  invalid page in block 3 of relation \"",
    )

    _psql_like(
        psql,
        "\nSELECT read_rel_block_ll('tbl_cs_fail', 3, nblocks=>1, "
        "zero_on_error=>true);",
        r"^$",
        r"^psql:<stdin>:\d+: WARNING:  invalid page in block 3 of relation "
        r'"base/.*"; zeroing out page',
    )


def _test_ignore_checksum(node):
    """Test detecting/ignoring checksum failures, with per-block log detail."""
    psql = node.background_psql("postgres", on_error_stop=False)

    # Test setup
    psql.query_safe(
        "\nCREATE TABLE tbl_cs_fail(id int) WITH (AUTOVACUUM_ENABLED = false);\n"
        "INSERT INTO tbl_cs_fail SELECT generate_series(1, 10000);\n"
    )

    count_sql = "SELECT count(*) FROM tbl_cs_fail"
    invalidate_sql = (
        "\nSELECT invalidate_rel_block('tbl_cs_fail', g.i)\n"
        "FROM generate_series(0, 6) g(i);\n"
    )

    expect = psql.query_safe(count_sql)

    _ignore_checksum_basic(psql, count_sql, invalidate_sql, expect)
    _ignore_checksum_multiblock(node, psql)
    _ignore_checksum_multiproblem(node, psql)
    _ignore_checksum_both(psql)

    psql.quit()


def _read_buffers_combine(
    psql_a, persistency, table
):  # pylint: disable=unused-argument
    """Combining / hit-splitting cases for read_buffers()."""
    # check that consecutive misses are combined into one read
    psql_a.query_safe("SELECT evict_rel('{}')".format(table))
    _psql_like(
        psql_a,
        "SELECT blockoff, blocknum, io_reqd, nblocks FROM "
        "read_buffers('{}', 0, 2)".format(table),
        r"^0\|0\|t\|2$",
        r"^$",
    )

    # but if we do it again, i.e. it's in the buffer pool, there will be two
    # operations
    _psql_like(
        psql_a,
        "SELECT blockoff, blocknum, io_reqd, nblocks FROM "
        "read_buffers('{}', 0, 2)".format(table),
        r"^0\|0\|f\|1\n1\|1\|f\|1$",
        r"^$",
    )

    # Check that a larger read interrupted by a hit works
    _psql_like(
        psql_a,
        "SELECT blockoff, blocknum, io_reqd, nblocks FROM "
        "read_buffers('{}', 3, 1)".format(table),
        r"^0\|3\|t\|1$",
        r"^$",
    )
    _psql_like(
        psql_a,
        "SELECT blockoff, blocknum, io_reqd, nblocks FROM "
        "read_buffers('{}', 2, 4)".format(table),
        r"^0\|2\|t\|1\n1\|3\|f\|1\n2\|4\|t\|2$",
        r"^$",
    )


def _read_buffers_hits(psql_a, table):
    """Reads with initial buffer hits / trailing hits, and io_combine_limit."""
    # Verify that a read with an initial buffer hit works
    psql_a.query_safe("SELECT evict_rel('{}')".format(table))
    for sql, expected in (
        ("read_buffers('{}', 0, 1)".format(table), r"^0\|0\|t\|1$"),
        ("read_buffers('{}', 0, 1)".format(table), r"^0\|0\|f\|1$"),
        ("read_buffers('{}', 1, 1)".format(table), r"^0\|1\|t\|1$"),
        ("read_buffers('{}', 1, 1)".format(table), r"^0\|1\|f\|1$"),
        ("read_buffers('{}', 0, 2)".format(table), r"^0\|0\|f\|1\n1\|1\|f\|1$"),
        (
            "read_buffers('{}', 0, 3)".format(table),
            r"^0\|0\|f\|1\n1\|1\|f\|1\n2\|2\|t\|1$",
        ),
    ):
        _psql_like(
            psql_a,
            "SELECT blockoff, blocknum, io_reqd, nblocks FROM " + sql,
            expected,
            r"^$",
        )

    # Verify that a read with an initial miss and trailing buffer hit(s) works
    psql_a.query_safe("SELECT invalidate_rel_block('{}', 0)".format(table))
    _psql_like(
        psql_a,
        "SELECT blockoff, blocknum, io_reqd, nblocks FROM "
        "read_buffers('{}', 0, 3)".format(table),
        r"^0\|0\|t\|1\n1\|1\|f\|1\n2\|2\|f\|1$",
        r"^$",
    )
    psql_a.query_safe("SELECT invalidate_rel_block('{}', 1)".format(table))
    psql_a.query_safe("SELECT invalidate_rel_block('{}', 2)".format(table))
    psql_a.query_safe("SELECT * FROM read_buffers('{}', 3, 2)".format(table))
    _psql_like(
        psql_a,
        "SELECT blockoff, blocknum, io_reqd, nblocks FROM "
        "read_buffers('{}', 1, 4)".format(table),
        r"^0\|1\|t\|2\n2\|3\|f\|1\n3\|4\|f\|1$",
        r"^$",
    )

    # Verify that we aren't doing reads larger than io_combine_limit. That's
    # just enforced in read_buffers() function, but kinda still worth testing.
    psql_a.query_safe("SELECT evict_rel('{}')".format(table))
    psql_a.query_safe("SET io_combine_limit=3")
    _psql_like(
        psql_a,
        "SELECT blockoff, blocknum, io_reqd, nblocks FROM "
        "read_buffers('{}', 1, 5)".format(table),
        r"^0\|1\|t\|3\n3\|4\|t\|2$",
        r"^$",
    )
    psql_a.query_safe("RESET io_combine_limit")


def _read_buffers_inprogress(psql_a, table):
    """Encountering in-progress IO at the start/middle/end of the range."""
    # Test encountering buffer IO we started in the first block of the range.
    #
    # Depending on how quick the IO we start completes, the IO might be
    # completed or we "join" the foreign IO. To hide that variability, the query
    # below treats a foreign IO as not having needed to do IO.
    psql_a.query_safe("SELECT evict_rel('{}')".format(table))
    psql_a.query_safe(
        "SELECT read_rel_block_ll('{}', 1, wait_complete=>false)".format(table)
    )
    _psql_like(
        psql_a,
        "SELECT blockoff, blocknum, io_reqd and not foreign_io, nblocks FROM "
        "read_buffers('{}', 1, 3)".format(table),
        r"^0\|1\|f\|1\n1\|2\|t\|2$",
        r"^$",
    )

    # Test in-progress IO in the middle block of the range
    psql_a.query_safe("SELECT evict_rel('{}')".format(table))
    psql_a.query_safe(
        "SELECT read_rel_block_ll('{}', 2, wait_complete=>false)".format(table)
    )
    _psql_like(
        psql_a,
        "SELECT blockoff, blocknum, io_reqd and not foreign_io, nblocks FROM "
        "read_buffers('{}', 1, 3)".format(table),
        r"^0\|1\|t\|1\n1\|2\|f\|1\n2\|3\|t\|1$",
        r"^$",
    )

    # Test in-progress IO on the last block of the range
    psql_a.query_safe("SELECT evict_rel('{}')".format(table))
    psql_a.query_safe(
        "SELECT read_rel_block_ll('{}', 3, wait_complete=>false)".format(table)
    )
    _psql_like(
        psql_a,
        "SELECT blockoff, blocknum, io_reqd and not foreign_io, nblocks FROM "
        "read_buffers('{}', 1, 3)".format(table),
        r"^0\|1\|t\|2\n2\|3\|f\|1$",
        r"^$",
    )


def _read_buffers_split(node, io_method, psql_a, psql_b):
    """Start buffer IO splits an IO if there's concurrent IO in progress."""
    table = "tbl_ok"
    persistency = "normal"

    # Test start buffer IO will split IO if there's IO in progress. We can't
    # observe this with sync, as that does not start the IO operation in
    # StartReadBuffers().
    if io_method == "sync":
        return

    psql_a.query_safe("SELECT evict_rel('{}')".format(table))

    buf_id = psql_b.query_safe("SELECT buffer_create_toy('{}', 3)".format(table))
    psql_b.query_safe(
        "SELECT buffer_call_start_io({}, for_input=>true, wait=>true)".format(buf_id)
    )

    _query_wait_block(
        node,
        psql_a,
        "SELECT blockoff, blocknum, io_reqd, foreign_io, nblocks FROM "
        "read_buffers('{}', 1, 5);\n".format(table),
        "BufferIo",
        1,
    )
    psql_b.query_safe(
        "SELECT buffer_call_terminate_io({}, for_input=>true, succeed=>false, "
        "io_error=>false, release_aio=>false)".format(buf_id)
    )
    # Because no IO wref was assigned, block 3 should not report foreign IO
    psql_a.query_until(r"0\|1\|t\|f\|2\n2\|3\|t\|f\|3")
    psql_a.clear()
    # {io_method}: {persistency}: IO was split due to concurrent failed IO

    # Same as before, except the concurrent IO succeeds this time
    psql_a.query_safe("SELECT evict_rel('{}')".format(table))
    buf_id = psql_b.query_safe("SELECT buffer_create_toy('{}', 3)".format(table))
    psql_b.query_safe(
        "SELECT buffer_call_start_io({}, for_input=>true, wait=>true)".format(buf_id)
    )

    _query_wait_block(
        node,
        psql_a,
        "SELECT blockoff, blocknum, io_reqd, foreign_io, nblocks FROM "
        "read_buffers('{}', 1, 5);\n".format(table),
        "BufferIo",
        1,
    )
    psql_b.query_safe(
        "SELECT buffer_call_terminate_io({}, for_input=>true, succeed=>true, "
        "io_error=>false, release_aio=>false)".format(buf_id)
    )
    # Because no IO wref was assigned, block 3 should not report foreign IO
    psql_a.query_until(r"0\|1\|t\|f\|2\n2\|3\|f\|f\|1\n3\|4\|t\|f\|2")
    psql_a.clear()
    assert persistency == "normal"


def _test_read_buffers(io_method, node):
    """Tests for StartReadBuffers()."""
    psql_a = node.background_psql("postgres", on_error_stop=False)
    psql_b = node.background_psql("postgres", on_error_stop=False)

    psql_a.query_safe(
        "\nCREATE TEMPORARY TABLE tmp_ok(data int not null);\n"
        "INSERT INTO tmp_ok SELECT generate_series(1, 5000);\n"
    )

    for persistency in ("normal", "temporary"):
        table = "tbl_ok" if persistency == "normal" else "tmp_ok"
        _read_buffers_combine(psql_a, persistency, table)
        _read_buffers_hits(psql_a, table)
        _read_buffers_inprogress(psql_a, table)

    # The remaining tests don't make sense for temp tables, as they are
    # concerned with multiple sessions interacting with each other.
    _read_buffers_split(node, io_method, psql_a, psql_b)

    psql_a.quit()
    psql_b.quit()


def _read_buffers_inject_one(node, io_method, psqls):
    """One foreign-IO read_buffers scenario (single in-progress block)."""
    psql_a, psql_b, psql_c = psqls
    table = "tbl_ok"

    # Test if a read buffers encounters AIO in progress by another backend, it
    # recognizes that other IO as a foreign IO.
    psql_a.query_safe("SELECT evict_rel('{}')".format(table))

    # B: Trigger wait in the next AIO read for block 1.
    psql_b.query_safe(
        "SELECT inj_io_completion_wait(pid=>pg_backend_pid(),\n"
        "\t\t   relfilenode=>pg_relation_filenode('{}'),\n"
        "\t\t   blockno=>1);".format(table)
    )

    # B: Read block 1 and wait for the completion hook to be reached (which
    # could be in B itself or in an IO worker)
    _query_wait_block(
        node,
        psql_b,
        "SELECT read_rel_block_ll('{}', blockno=>1, nblocks=>1)".format(table),
        "completion_wait",
        0,
    )

    # A: Start read, wait until we're waiting for IO completion
    _query_wait_block(
        node,
        psql_a,
        "SELECT blockoff, blocknum, io_reqd, foreign_io, nblocks FROM "
        "read_buffers('{}', 1, 4)".format(table),
        "AioIoCompletion",
        1,
    )

    # C: Release B from completion hook
    psql_c.query_safe("SELECT inj_io_completion_continue()")

    # A: Check that we recognized the foreign IO wait, if possible
    #
    # Due to sync mode not actually issuing IO below StartReadBuffers(), we
    # can't observe encountering foreign IO. It still seems worth exercising
    # these paths however.
    if io_method != "sync":
        # A foreign IO covering block 1, and one IO covering blocks 2-4.
        expected = r"0\|1\|t\|t\|1\n1\|2\|t\|f\|3"
    else:
        # One IO covering everything, as that's what StartReadBuffers() will
        # return for something with misses in sync mode.
        expected = r"0\|1\|t\|f\|4"
    psql_a.query_until(expected)
    psql_a.clear()


def _read_buffers_inject_two(node, io_method, psqls):
    """Foreign-IO read_buffers scenario encountered multiple times."""
    psql_a, psql_b, psql_c = psqls
    table = "tbl_ok"

    # Test if a read buffers encounters AIO in progress by another backend, it
    # recognizes that other IO as a foreign IO. This time we encounter the
    # foreign IO multiple times.
    psql_a.query_safe("SELECT evict_rel('{}')".format(table))

    # B: Trigger wait in the next AIO read for block 3.
    psql_b.query_safe(
        "SELECT inj_io_completion_wait(pid=>pg_backend_pid(),\n"
        "\t\t   relfilenode=>pg_relation_filenode('{}'),\n"
        "\t\t   blockno=>3);".format(table)
    )

    # B: Read block 2-3 and wait for the completion hook to be reached (which
    # could be in B itself or in an IO worker)
    _query_wait_block(
        node,
        psql_b,
        "SELECT read_rel_block_ll('{}', blockno=>2, nblocks=>2)".format(table),
        "completion_wait",
        0,
    )

    # A: Start read, wait until we're waiting for IO completion
    #
    # Note that we need to defer waiting for IO until the end of read_buffers(),
    # to be able to see that the IO on 3 is still in progress.
    _query_wait_block(
        node,
        psql_a,
        "SELECT blockoff, blocknum, io_reqd, foreign_io, nblocks FROM\n"
        "read_buffers('{}', 0, 4)".format(table),
        "AioIoCompletion",
        1,
    )

    # C: Release B from completion hook
    psql_c.query_safe("SELECT inj_io_completion_continue()")

    # A: Check that we recognized the foreign IO wait, if possible
    #
    # See comment further up about sync mode.
    if io_method != "sync":
        # One IO covering blocks 0-1, A foreign IO covering block 2, and a
        # foreign IO covering block 3 (same wref as for block 2).
        expected = r"0\|0\|t\|f\|2\n2\|2\|t\|t\|1\n3\|3\|t\|t\|1"
    else:
        # One IO covering everything, as that's what StartReadBuffers() will
        # return for something with misses in sync mode.
        expected = r"0\|0\|t\|f\|4"
    psql_a.query_until(expected)
    psql_a.clear()


def _test_read_buffers_inject(io_method, node):
    """Tests for StartReadBuffers() that depend on injection point support."""
    psql_a = node.background_psql("postgres", on_error_stop=False)
    psql_b = node.background_psql("postgres", on_error_stop=False)
    psql_c = node.background_psql("postgres", on_error_stop=False)

    # We can't easily test waiting for foreign IOs on temporary tables, as the
    # waiting in the completion hook will just stall the backend. For worker
    # that is because temporary table IO is executed synchronously, for io_uring
    # the completion will be executed in the same process, but due to temporary
    # tables not being shared, we can't do the wait in another backend.
    psqls = (psql_a, psql_b, psql_c)
    _read_buffers_inject_one(node, io_method, psqls)
    _read_buffers_inject_two(node, io_method, psqls)

    psql_a.quit()
    psql_b.quit()
    psql_c.quit()


def _test_io_method(io_method, node):
    """Run all tests for the specified node / io_method."""
    assert (
        node.safe_psql("SHOW io_method") == io_method
    ), "{}: io_method set correctly".format(io_method)

    node.safe_psql(
        "\nCREATE EXTENSION test_aio;\n"
        "CREATE TABLE tbl_corr(data int not null) "
        "WITH (AUTOVACUUM_ENABLED = false);\n"
        "CREATE TABLE tbl_ok(data int not null) "
        "WITH (AUTOVACUUM_ENABLED = false);\n"
        "\n"
        "INSERT INTO tbl_corr SELECT generate_series(1, 10000);\n"
        "INSERT INTO tbl_ok SELECT generate_series(1, 10000);\n"
        "SELECT grow_rel('tbl_corr', 16);\n"
        "SELECT grow_rel('tbl_ok', 16);\n"
        "\n"
        "SELECT modify_rel_block('tbl_corr', 1, corrupt_header=>true);\n"
        "CHECKPOINT;\n"
    )

    _test_handle(node)
    _test_io_error(node)
    _test_batchmode(node)
    _test_startwait_io(node)
    _test_complete_foreign(node)
    _test_close_fd(node)
    _test_invalidate(node)
    _test_zero(node)
    _test_checksum(node)
    _test_ignore_checksum(node)
    _test_checksum_createdb(node)
    _test_read_buffers(io_method, node)

    # generic injection tests
    if os.environ.get("enable_injection_points") == "yes":
        _test_inject(node)
        _test_read_buffers_inject(io_method, node)

    # worker specific injection tests
    if io_method == "worker":
        if os.environ.get("enable_injection_points") == "yes":
            _test_inject_worker(node)


def test_001_aio(create_pg):
    """Create one node per io_method, configure, and run every test in turn."""
    methods = testaio.supported_io_methods()
    nodes = {}

    # Create and configure one instance for each io_method
    for method in methods:
        node = create_pg(method, start=False)
        nodes[method] = node
        node.append_conf("io_method={}".format(method))
        testaio.configure(node)

    # Just to have one test not use the default auto-tuning
    nodes["sync"].append_conf("\n io_max_concurrency=4\n")

    # Execute the tests for each io_method
    for method in methods:
        node = nodes[method]
        node.start()
        _test_io_method(method, node)
        node.stop()

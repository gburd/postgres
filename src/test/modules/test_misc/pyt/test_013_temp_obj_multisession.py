# Copyright (c) 2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long,too-many-statements
"""Port of src/test/modules/test_misc/t/013_temp_obj_multisession.pl.

Tests that one session cannot read or modify data in another session's
temporary table.  Each session keeps its temp data in its own local buffer
pool, and a different backend has no visibility into those buffers, so any
command that needs to look at the data must be rejected.

DROP TABLE is intentionally allowed: it does not touch the table's contents,
and autovacuum relies on this to clean up orphaned temp relations left behind
by a crashed backend.

A regression caught here typically means a new buffer-access entry point
bypasses the RELATION_IS_OTHER_TEMP() check.
"""

import re


def test_013_temp_obj_multisession(create_pg):
    """A session cannot read/modify another session's temp table data."""
    node = create_pg("temp_lock", start=False)
    node.start()

    def like(text, pattern, _msg):
        assert re.search(pattern, text), "{}\nstderr:\n{}".format(_msg, text)

    # Owner session.  Created via background_psql so it stays alive while
    # the second session probes its temp objects.
    psql1 = node.background_psql("postgres")

    # Initially create the table without an index, so read paths go straight
    # through the read-stream / buffer-manager entry points without being
    # masked by an index scan that would hit ReadBuffer_common from nbtree.
    psql1.query_safe("CREATE TEMP TABLE foo AS SELECT 42 AS val;")

    # Resolve the owner's temp schema so the probing session can refer to
    # the table by a fully-qualified name.
    tempschema = node.safe_psql(
        """
      SELECT n.nspname
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE relname = 'foo' AND relpersistence = 't';
    """
    )
    assert re.match(r"^pg_temp_\d+$", tempschema), "got temp schema: {}".format(
        tempschema
    )

    # DML and SELECT have to read the table's data and therefore go through
    # the buffer manager.  With no index on the table, the planner cannot
    # use index access, so SELECT/UPDATE/DELETE/MERGE/COPY all run through
    # the read-stream path and are caught by read_stream_begin_impl().

    result = node.psql_capture(
        "SELECT val FROM {}.foo;".format(tempschema), on_error_stop=False
    )
    like(
        result.stderr,
        r"cannot access temporary tables of other sessions",
        "SELECT (seqscan via read_stream)",
    )

    # INSERT goes through hio.c which calls ReadBufferExtended() to find a
    # page with free space; that hits the existing check before any data
    # is written.
    result = node.psql_capture(
        "INSERT INTO {}.foo VALUES (73);".format(tempschema), on_error_stop=False
    )
    like(
        result.stderr,
        r"cannot access temporary tables of other sessions",
        "INSERT (caught via hio.c)",
    )

    result = node.psql_capture(
        "UPDATE {}.foo SET val = NULL;".format(tempschema), on_error_stop=False
    )
    like(
        result.stderr,
        r"cannot access temporary tables of other sessions",
        "UPDATE",
    )

    result = node.psql_capture(
        "DELETE FROM {}.foo;".format(tempschema), on_error_stop=False
    )
    like(
        result.stderr,
        r"cannot access temporary tables of other sessions",
        "DELETE",
    )

    result = node.psql_capture(
        "MERGE INTO {schema}.foo USING (VALUES (42)) AS s(val) "
        "ON foo.val = s.val WHEN MATCHED THEN DELETE;".format(schema=tempschema),
        on_error_stop=False,
    )
    like(
        result.stderr,
        r"cannot access temporary tables of other sessions",
        "MERGE",
    )

    result = node.psql_capture(
        "COPY {}.foo TO STDOUT;".format(tempschema), on_error_stop=False
    )
    like(
        result.stderr,
        r"cannot access temporary tables of other sessions",
        "COPY",
    )

    # DDL and maintenance commands have their own command-specific checks
    # (older than the buffer-manager check above), so they fail with
    # command-specific error messages.  Verifying them here documents the
    # expected behaviour and guards against accidental removal of those
    # checks.

    result = node.psql_capture(
        "TRUNCATE TABLE {}.foo;".format(tempschema), on_error_stop=False
    )
    like(
        result.stderr,
        r"cannot truncate temporary tables of other sessions",
        "TRUNCATE",
    )

    result = node.psql_capture(
        "ALTER TABLE {}.foo ALTER COLUMN val TYPE bigint;".format(tempschema),
        on_error_stop=False,
    )
    like(
        result.stderr,
        r"cannot alter temporary tables of other sessions",
        "ALTER TABLE",
    )

    # VACUUM silently skips other sessions' temp tables (vacuum_rel() returns
    # without warning to avoid noise during database-wide VACUUM).  Verify
    # that no error is reported, and that no buffer-access path is hit.
    result = node.psql_capture("VACUUM {}.foo;".format(tempschema), on_error_stop=False)
    assert result.stderr == "", "VACUUM is silently skipped"

    result = node.psql_capture(
        "CLUSTER {}.foo;".format(tempschema), on_error_stop=False
    )
    like(
        result.stderr,
        r"cannot execute CLUSTER on temporary tables of other sessions",
        "CLUSTER",
    )

    # Now create an index to exercise the index-scan path.  nbtree calls
    # ReadBuffer (which is ReadBufferExtended -> ReadBuffer_common), so
    # this exercises a different chain of buffer-manager entry points.
    psql1.query_safe("CREATE INDEX ON foo(val);")

    result = node.psql_capture(
        "SET enable_seqscan = off; SELECT val FROM {}.foo WHERE val = 42;".format(
            tempschema
        ),
        on_error_stop=False,
    )
    like(
        result.stderr,
        r"cannot access temporary tables of other sessions",
        "index scan (ReadBuffer_common via nbtree)",
    )

    # ALTER INDEX goes through the same CheckAlterTableIsSafe() path as
    # ALTER TABLE, so it produces the same error.
    result = node.psql_capture(
        "ALTER INDEX {}.foo_val_idx SET (fillfactor = 50);".format(tempschema),
        on_error_stop=False,
    )
    like(
        result.stderr,
        r"cannot alter temporary tables of other sessions",
        "ALTER INDEX",
    )

    # A function created by the owner in its own pg_temp using its own
    # row type can be observed via the catalog by a separate session.
    # ALTER FUNCTION and DROP FUNCTION on it must work as catalog
    # operations -- they don't read the underlying table -- which
    # documents the boundary between catalog and data access for temp
    # objects.
    psql1.query_safe(
        "CREATE FUNCTION pg_temp.foo_id(r foo) RETURNS int LANGUAGE SQL "
        "AS 'SELECT r.val';"
    )

    result = node.psql_capture(
        "ALTER FUNCTION {schema}.foo_id({schema}.foo) "
        "SET search_path = pg_catalog;".format(schema=tempschema),
        on_error_stop=False,
    )
    assert (
        result.stderr == ""
    ), "ALTER FUNCTION on function over other session's row type"

    result = node.psql_capture(
        "DROP FUNCTION {schema}.foo_id({schema}.foo);".format(schema=tempschema),
        on_error_stop=False,
    )
    assert (
        result.stderr == ""
    ), "DROP FUNCTION on function over other session's row type"

    # DROP TABLE on another session's temp table is intentionally permitted.
    # DROP doesn't touch the table's contents, and autovacuum relies on this
    # to remove temp relations orphaned by a crashed backend.  Verify that
    # the bare DROP succeeds without error.
    result = node.psql_capture(
        "DROP TABLE {}.foo;".format(tempschema), on_error_stop=False
    )
    assert result.stderr == "", "DROP TABLE is allowed"

    # Cross-session CREATE FUNCTION scenario.  The owner creates a fresh
    # temp table foo2 in its pg_temp namespace, and a separate session
    # then creates a function whose argument type is that row type.
    # PostgreSQL allows this and emits a NOTICE: the function is moved
    # into the creator's pg_temp namespace with an auto-dependency on
    # the borrowed type, so it disappears together with the session that
    # created it.
    psql1.query_safe("CREATE TEMP TABLE foo2 AS SELECT 42 AS val;")

    result = node.psql_capture(
        "CREATE FUNCTION public.cross_session_func(r {}.foo2) "
        "RETURNS int LANGUAGE SQL AS 'SELECT 1';".format(tempschema),
        on_error_stop=False,
    )
    like(
        result.stderr,
        r'function "cross_session_func" will be effectively temporary',
        "CREATE FUNCTION using other session's row type is effectively temporary",
    )

    # A bare DROP TABLE on foo2 now fails because cross_session_func
    # depends on its row type.  This is normal SQL dependency behaviour
    # and documents that DROP itself is not blocked by buffer-manager
    # checks -- we get a catalog-level error instead.
    result = node.psql_capture(
        "DROP TABLE {}.foo2;".format(tempschema), on_error_stop=False
    )
    like(
        result.stderr,
        r"cannot drop table .*\.foo2 because other objects depend on it",
        "DROP TABLE blocked by cross-session dependency",
    )

    foo2_oid = node.safe_psql("SELECT oid FROM pg_class WHERE relname='foo2';")

    # Cross-session LOCK TABLE scenario.  Ensure that LockRelationOid is
    # working properly for other temp tables since this mechanism is also
    # used by autovacuum during orphaned tables cleanup.
    psql2 = node.background_psql("postgres")
    psql2.query_safe(
        """
\tBEGIN;
\tLOCK TABLE {}.foo2 IN ACCESS SHARE MODE;
""".format(
            tempschema
        )
    )

    # When the owner session ends, its temp objects are dropped via the
    # normal session-exit cleanup, which cascades through DEPENDENCY_NORMAL
    # and also removes the cross-session function that depended on the temp
    # row type.  This is the same mechanism autovacuum relies on to clean up
    # temp relations left behind by a crashed backend.
    # Access share lock on the foo2 will block session-exit cleanup, because
    # an owner will try to acquire deletion lock all its temp objects via
    # findDependentObjects.
    log_offset = node.current_log_position()
    psql1.quit()

    # Check whether session-exit cleanup is blocked.
    node.wait_for_log(
        r"waiting for AccessExclusiveLock on relation {}".format(foo2_oid),
        log_offset,
    )

    # Release lock on foo2 and allow session-exit cleanup to finish.
    psql2.query_safe("COMMIT;")
    psql2.quit()

    # After releasing the lock, the owner can finally acquire
    # AccessExclusiveLock on foo2 and finish session-exit cleanup.  Verify
    # directly that both foo2 (the locked temp table) and cross_session_func
    # (which depended on its row type) have been dropped.  Both being gone
    # confirms the owner's cleanup got past the blocked findDependentObjects()
    # call and completed normally.
    assert node.poll_query_until(
        "SELECT NOT EXISTS (SELECT 1 FROM pg_class WHERE oid = {})".format(foo2_oid)
    ), "foo2 was not cleaned up after owner session exit"

    assert (
        node.safe_psql(
            "SELECT count(*) FROM pg_proc WHERE proname = 'cross_session_func'"
        )
        == "0"
    ), "cross_session_func cleaned up by cascade from foo2"

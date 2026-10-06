# Copyright (c) 2025, PostgreSQL Global Development Group

"""
Tests for libpq error types and SQLSTATE-based exception mapping.
"""

import pytest
import pypg
from libpq import LibpqError, QueryCanceled, SyntaxErrorState, UniqueViolation


def test_syntax_error(conn):
    """Invalid SQL syntax raises LibpqError with correct SQLSTATE."""
    with pytest.raises(LibpqError) as exc_info:
        conn.sql("SELEC 1")

    err = exc_info.value
    assert err.sqlstate == "42601"
    assert err.sqlstate_class == "42"
    assert "syntax" in str(err).lower()


def test_unique_violation(conn):
    """Unique violation includes all error fields."""
    conn.sql("CREATE TEMP TABLE test_uv (id int CONSTRAINT test_uv_pk PRIMARY KEY)")
    conn.sql("INSERT INTO test_uv VALUES (1)")

    with pytest.raises(LibpqError) as exc_info:
        conn.sql("INSERT INTO test_uv VALUES (1)")

    err = exc_info.value
    assert err.sqlstate == "23505"
    assert err.table_name == "test_uv"
    assert err.constraint_name == "test_uv_pk"
    assert err.detail == "Key (id)=(1) already exists."


def test_named_exception_subclass(conn):
    """A specific SQLSTATE raises its named subclass, matchable directly."""
    with pytest.raises(SyntaxErrorState):
        conn.sql("SELEC 1")
    with pytest.raises(UniqueViolation):
        conn.sql(
            "CREATE TEMP TABLE t (id int PRIMARY KEY);"
            "INSERT INTO t VALUES (1); INSERT INTO t VALUES (1);"
        )


def test_named_exception_is_libpqerror(conn):
    """A named subclass is still catchable as the base LibpqError/PgSqlError."""
    with pytest.raises(LibpqError):
        conn.sql("SELEC 1")
    with pytest.raises(pypg.PgSqlError):
        conn.sql("SELEC 1")


def test_query_canceled_matches(conn):
    """statement_timeout cancellation raises QueryCanceled (SQLSTATE 57014)."""
    conn.sql("SET statement_timeout = '50ms'")
    with pytest.raises(QueryCanceled) as exc_info:
        conn.sql("SELECT pg_sleep(5)")
    assert exc_info.value.sqlstate == "57014"


def test_wait_until_breaks(pg):
    """wait_until polls until the body breaks, without timing out."""
    seen = []
    for _ in pypg.wait_until("never reached", timeout=5, interval=0.01):
        seen.append(pg.sql("SELECT 1").scalar())
        if seen[-1] == "1":
            break
    assert seen[-1] == "1"


def test_wait_until_times_out():
    """wait_until raises TimeoutError when the body never breaks."""
    with pytest.raises(TimeoutError):
        for _ in pypg.wait_until("deliberate timeout", timeout=0.2, interval=0.05):
            pass

# Copyright (c) 2025, PostgreSQL Global Development Group

"""Tests for the node-level sql() API and its typed SqlResult."""

import pytest

import pypg


def test_sql_scalar(pg):
    """A one-row, one-column result yields its scalar value as text."""
    assert pg.sql("SELECT 1").scalar() == "1"
    assert pg.sql("SELECT 'hello'").scalar() == "hello"


def test_sql_empty_is_falsey(pg):
    """A result with no rows is falsey and scalar() is None."""
    result = pg.sql("SELECT 1 WHERE false")
    assert not result
    assert len(result) == 0
    assert result.scalar() is None


def test_sql_row_and_columns(pg):
    """row() returns the single row; column() projects one column."""
    assert pg.sql("SELECT 1, 'a'").row() == ("1", "a")
    assert pg.sql("SELECT x FROM (VALUES (1), (2), (3)) v(x) ORDER BY x").column() == [
        "1",
        "2",
        "3",
    ]
    assert pg.sql("SELECT x FROM (VALUES (1), (2), (3)) v(x) ORDER BY x").rows == [
        ("1",),
        ("2",),
        ("3",),
    ]


def test_sql_raises_pgsqlerror(pg):
    """A failing statement raises PgSqlError, not a bare CalledProcessError."""
    with pytest.raises(pypg.PgSqlError):
        pg.sql("SELECT * FROM no_such_table")


def test_sql_error_carries_primary(pg):
    """The raised error exposes the primary message text from psql."""
    with pytest.raises(pypg.PgSqlError) as excinfo:
        pg.sql("SELECT * FROM no_such_table")
    assert "no_such_table" in str(excinfo.value)


def test_sql_libpq_channel(pg):
    """The libpq channel returns the same shape as the psql channel."""
    assert pg.sql("SELECT 42", channel="libpq").scalar() == "42"


def test_sql_libpq_channel_raises(pg):
    """The libpq channel also raises PgSqlError on a bad statement."""
    with pytest.raises(pypg.PgSqlError):
        pg.sql("SELECT * FROM no_such_table", channel="libpq")


def test_sql_bad_channel(pg):
    """An unknown channel is a ValueError."""
    with pytest.raises(ValueError):
        pg.sql("SELECT 1", channel="bogus")


def test_scalar_rejects_multi_row(pg):
    """scalar() refuses a result that is not exactly one cell."""
    result = pg.sql("SELECT * FROM (VALUES (1), (2)) v")
    with pytest.raises(ValueError):
        result.scalar()


def test_background_psql_context_manager(pg):
    """A background psql session can be used as a context manager."""
    with pg.background_psql("postgres") as bg:
        assert bg.query("SELECT 1").strip() == "1"
    # Leaving the block quits the session; a second one starts cleanly.
    with pg.background_psql("postgres") as bg:
        assert bg.query("SELECT 2").strip() == "2"

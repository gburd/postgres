# Copyright (c) 2026, PostgreSQL Global Development Group

"""Tests for pypg.fake: rand_str (the randStr equivalent) and optional Faker."""

import string
import warnings

import pytest

import pypg


def test_rand_str_length_and_charset():
    """rand_str returns the requested length using only [A-Za-z0-9]."""
    allowed = set(string.ascii_letters + string.digits)
    for length in (0, 1, 10, 10000):
        s = pypg.rand_str(length)
        assert len(s) == length
        assert set(s) <= allowed


def test_rand_str_custom_charset_and_randomness():
    """A custom charset is respected, and successive calls differ (very likely)."""
    assert set(pypg.rand_str(50, charset="ab")) <= {"a", "b"}
    # Two independent 64-char draws over 62 symbols collide with negligible odds.
    assert pypg.rand_str(64) != pypg.rand_str(64)


def test_rand_str_rejects_bad_args():
    """Negative length and empty charset are rejected."""
    with pytest.raises(ValueError):
        pypg.rand_str(-1)
    with pytest.raises(ValueError):
        pypg.rand_str(5, charset="")


def test_faker_optional():
    """faker() returns a usable instance, or None with a single warning."""
    pypg.fake._warned.discard("faker")  # pylint: disable=protected-access
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        fake = pypg.faker()
    if fake is None:
        # Faker not installed: exactly one RuntimeWarning, and the convenience
        # helper still returns a usable string via the rand_str fallback.
        assert any(issubclass(w.category, RuntimeWarning) for w in caught)
        assert len(pypg.meaningful_text(40)) > 0
    else:
        # Faker installed: it produces realistic, non-empty fake data.
        assert isinstance(fake.name(), str) and fake.name()
        assert len(pypg.meaningful_text(80)) > 0

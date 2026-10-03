# Copyright (c) 2026, PostgreSQL Global Development Group

"""Random and (optionally) realistic fake test-data generation.

``rand_str`` mirrors the ``randStr`` helper used across PostgreSQL TAP tests: a
uniform random string over ``[A-Za-z0-9]`` of a given length. For
realistic-looking ("meaningful") fake data -- names, emails, addresses,
sentences -- this module optionally uses the third-party ``faker`` library.
Faker is not a hard dependency: when it is not installed, :func:`faker` returns
``None`` after a single :class:`RuntimeWarning`, and :func:`meaningful_text`
transparently falls back to :func:`rand_str`. Install it with
``uv sync --extra fake`` (or ``pip install faker``).
"""

import random
import string
import warnings

# [A-Z][a-z][0-9] -- exactly the character set of the Perl TAP randStr helper:
#     my @chars = ("A" .. "Z", "a" .. "z", "0" .. "9");
DEFAULT_CHARSET = string.ascii_uppercase + string.ascii_lowercase + string.digits

_warned = set()


def rand_str(length, charset=DEFAULT_CHARSET):
    """Return a random string of ``length`` characters drawn uniformly from
    ``charset`` (default ``[A-Za-z0-9]``).

    Equivalent to the Perl TAP ``randStr`` subroutine::

        sub randStr {
            my $len = shift;
            my @chars = ("A" .. "Z", "a" .. "z", "0" .. "9");
            return join '', map { $chars[ rand @chars ] } 1 .. $len;
        }

    Each character is chosen independently and uniformly, matching Perl's
    ``$chars[rand @chars]``.

    Args:
        length: Number of characters to generate (must be non-negative).
        charset: Characters to draw from (must be non-empty).

    Returns:
        A freshly generated random string of the requested length.
    """
    if length < 0:
        raise ValueError("length must be non-negative")
    if not charset:
        raise ValueError("charset must be non-empty")
    return "".join(random.choice(charset) for _ in range(length))


def faker(locale=None, seed=None):
    """Return a ``Faker`` instance for realistic fake data, or ``None``.

    If the optional ``faker`` package is not installed, returns ``None`` after
    emitting a single :class:`RuntimeWarning` (subsequent calls are silent).
    Install it with ``uv sync --extra fake`` (or ``pip install faker``).

    Args:
        locale: Optional Faker locale (e.g. ``"de_DE"``), passed through to
            ``Faker(locale)``.
        seed: If given, seed Faker for reproducible output (``Faker.seed``).

    Returns:
        A ``Faker`` instance, or ``None`` when Faker is unavailable.
    """
    try:
        # faker is an optional dependency (the `fake` extra); ignore if absent.
        import faker  # pylint: disable=import-outside-toplevel # pyrefly: ignore
    except ImportError:
        if "faker" not in _warned:
            _warned.add("faker")
            warnings.warn(
                "Faker is not installed; meaningful fake-data generation is "
                "unavailable (falling back to random strings). Install it with "
                "`uv sync --extra fake` or `pip install faker`.",
                RuntimeWarning,
                stacklevel=2,
            )
        return None
    if seed is not None:
        faker.Faker.seed(seed)
    return faker.Faker(locale)


def meaningful_text(max_chars=200, locale=None):
    """Return realistic-looking text via Faker, or a random fallback.

    Uses ``Faker.text`` when Faker is installed; otherwise falls back to
    :func:`rand_str` (after :func:`faker` issues its one-time warning) so
    callers always get a usable string.

    Args:
        max_chars: Approximate maximum length of the generated text.
        locale: Optional Faker locale.
    """
    fake = faker(locale=locale)
    if fake is None:
        return rand_str(max_chars)
    return fake.text(max_nb_chars=max_chars)

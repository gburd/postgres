# Copyright (c) 2025-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long
"""Port of src/test/modules/oauth_validator/t/002_client.pl.

Exercises the API for custom OAuth client flows, using the oauth_hook_client
test driver. These tests do not use the builtin flow and do not contact a real
authorization server, so the issuer address is an invalid IP (any accidental
connection attempt then fails noisily). Gated behind PG_TEST_EXTRA=oauth.
"""

import contextlib
import os

import pytest

import pypg

pytestmark = pypg.require_test_extras("oauth")

_ISSUER = "https://256.256.256.256"
_SCOPE = "openid postgres"
_USER = "test"


@contextlib.contextmanager
def _env(**overrides):
    """Temporarily set environment variables (mirrors Perl local $ENV{...})."""
    saved = {k: os.environ.get(k) for k in overrides}
    os.environ.update({k: str(v) for k, v in overrides.items()})
    try:
        yield
    finally:
        for key, value in saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


@pytest.fixture(scope="module")
def oauth_node(create_pg_module):
    """A server configured for the custom-flow client tests.

    Mirrors the cluster setup at the top of 002_client.pl: the validator
    library is loaded, connection logging is on, debug2 messages are enabled so
    connection failures can be inspected in the log, and pg_hba grants the test
    user OAuth auth for the (unreachable) issuer.
    """
    node = create_pg_module("primary", start=False)
    node.append_conf("log_connections = all\n")
    node.append_conf("oauth_validator_libraries = 'validator'\n")
    node.append_conf("log_min_messages = debug2")
    node.start()

    node.safe_psql("CREATE USER test;")

    (node.datadir / "pg_hba.conf").unlink()
    node.append_conf(
        'local all test oauth issuer="{}" scope="{}"\n'.format(_ISSUER, _SCOPE),
        filename="pg_hba.conf",
    )
    node.reload()
    node.wait_for_log(r"reloading configuration files")

    with _env(PGOAUTHDEBUG="UNSAFE"):
        yield node


def _run_hook_client(node, common_connstr, flags=None):
    """Run oauth_hook_client with the given flags and connstr.

    Returns (CommandResult, log_start) where log_start is the server log offset
    captured immediately before the run, for later log_check assertions.
    """
    cmd = ["oauth_hook_client"] + list(flags or []) + [common_connstr]
    log_start = node.current_log_position()
    result = node.bin.run_command(cmd)
    return result, log_start


def _check(
    node,
    result,
    log_start,
    test_name,
    *,
    expect_success=False,
    expected_stderr=None,
    log_like=None,
):
    """Apply the stdout/stderr/log assertions of 002_client.pl's test()."""
    if expect_success:
        assert "connection succeeded" in result.stdout, "{}: stdout matches\n{}".format(
            test_name, result.stdout
        )

    if expected_stderr is not None:
        import re

        assert re.search(
            expected_stderr, result.stderr
        ), "{}: stderr matches {!r}\n{}".format(
            test_name, expected_stderr, result.stderr
        )
    else:
        assert result.stderr == "", "{}: no stderr, got {!r}".format(
            test_name, result.stderr
        )

    if log_like is not None:
        # Wait for the postmaster to flush the finished connection's log, to
        # avoid races (see Cluster::connect_fails()).
        node.wait_for_log(
            r"(?s)DEBUG:  (?:00000: )?forked new client backend, pid=(\d+) socket"
            r".*DEBUG:  (?:00000: )?client backend \(PID \1\) exited with exit code \d",
            log_start,
        )
        node.log_check(test_name, log_start, log_like=log_like)


def test_basic_synchronous_hook_provides_token(oauth_node):
    """A basic v2 synchronous hook can provide a token."""
    connstr = "{} user={} oauth_issuer={} oauth_client_id=myID".format(
        oauth_node.connstr(), _USER, _ISSUER
    )
    result, log_start = _run_hook_client(
        oauth_node,
        connstr,
        flags=[
            "--token",
            "my-token",
            "--expected-uri",
            "{}/.well-known/openid-configuration".format(_ISSUER),
            "--expected-issuer",
            _ISSUER,
            "--expected-scope",
            _SCOPE,
        ],
    )
    _check(
        oauth_node,
        result,
        log_start,
        "basic synchronous hook can provide a token",
        expect_success=True,
        log_like=[r'oauth_validator: token="my-token", role="{}"'.format(_USER)],
    )


def test_derived_issuer_id_provided(oauth_node):
    """The issuer ID provided to the hook is derived from oauth_issuer."""
    connstr = (
        "{} user={} oauth_issuer={}/.well-known/openid-configuration "
        "oauth_client_id=myID oauth_scope='{}'"
    ).format(oauth_node.connstr(), _USER, _ISSUER, _SCOPE)
    result, log_start = _run_hook_client(
        oauth_node,
        connstr,
        flags=[
            "--token",
            "my-token",
            "--expected-uri",
            "{}/.well-known/openid-configuration".format(_ISSUER),
            "--expected-issuer",
            _ISSUER,
            "--expected-scope",
            _SCOPE,
        ],
    )
    _check(
        oauth_node,
        result,
        log_start,
        "derived issuer ID is correctly provided",
        expect_success=True,
        log_like=[r'oauth_validator: token="my-token", role="{}"'.format(_USER)],
    )


def test_v1_synchronous_hook_provides_token(oauth_node):
    """The v1 synchronous hook continues to work."""
    connstr = "{} user={} oauth_issuer={} oauth_client_id=myID".format(
        oauth_node.connstr(), _USER, _ISSUER
    )
    result, log_start = _run_hook_client(
        oauth_node,
        connstr,
        flags=[
            "-v1",
            "--token",
            "my-token-v1",
            "--expected-uri",
            "{}/.well-known/openid-configuration".format(_ISSUER),
            "--expected-scope",
            _SCOPE,
        ],
    )
    _check(
        oauth_node,
        result,
        log_start,
        "v1 synchronous hook can provide a token",
        expect_success=True,
        log_like=[r'oauth_validator: token="my-token-v1", role="{}"'.format(_USER)],
    )


def test_fails_without_custom_hook_when_no_libcurl(oauth_node):
    """Without a custom hook and without libcurl, libpq points at libpq-oauth."""
    if os.environ.get("with_libcurl") == "yes":
        pytest.skip("builtin flow is available; no-hook fallback message not emitted")
    connstr = "{} user={} oauth_issuer={} oauth_client_id=myID".format(
        oauth_node.connstr(), _USER, _ISSUER
    )
    result, log_start = _run_hook_client(oauth_node, connstr, flags=["--no-hook"])
    _check(
        oauth_node,
        result,
        log_start,
        "fails without custom hook installed",
        expected_stderr=r"no OAuth flows are available \(try installing the libpq-oauth package\)",
    )


def test_synchronous_hook_sets_error_message(oauth_node):
    """A v2 synchronous flow can set a custom error message."""
    connstr = "{} user={} oauth_issuer={} oauth_client_id=myID".format(
        oauth_node.connstr(), _USER, _ISSUER
    )
    result, log_start = _run_hook_client(
        oauth_node, connstr, flags=["--error", "a custom error message"]
    )
    _check(
        oauth_node,
        result,
        log_start,
        "basic synchronous hook can set error messages",
        expected_stderr=r"user-defined OAuth flow failed: a custom error message",
    )


def test_connect_timeout_interrupts_hung_flow(oauth_node):
    """connect_timeout interrupts a client flow that never responds."""
    connstr = (
        "{} user={} oauth_issuer={} oauth_client_id=myID connect_timeout=1"
    ).format(oauth_node.connstr(), _USER, _ISSUER)
    result, log_start = _run_hook_client(oauth_node, connstr, flags=["--hang-forever"])
    _check(
        oauth_node,
        result,
        log_start,
        "connect_timeout interrupts hung client flow",
        expected_stderr=r"failed: timeout expired",
    )


_MISBEHAVE_CASES = [
    (
        "--misbehave=no-hook",
        r"user-defined OAuth flow provided neither a token nor an async callback",
    ),
    ("--misbehave=fail-async", r"user-defined OAuth flow failed"),
    ("--misbehave=no-token", r"user-defined OAuth flow did not provide a token"),
    (
        "--misbehave=no-socket",
        r"user-defined OAuth flow did not provide a socket for polling",
    ),
]


@pytest.mark.parametrize("flag,expected_error", _MISBEHAVE_CASES)
@pytest.mark.parametrize("v1", [False, True])
def test_hook_misbehavior(oauth_node, flag, expected_error, v1):
    """Each client-hook misbehavior is reported, for both v1 and v2 hooks."""
    connstr = "{} user={} oauth_issuer={} oauth_client_id=myID".format(
        oauth_node.connstr(), _USER, _ISSUER
    )
    flags = ["-v1", flag] if v1 else [flag]
    suffix = " (v1)" if v1 else ""
    result, log_start = _run_hook_client(oauth_node, connstr, flags=flags)
    _check(
        oauth_node,
        result,
        log_start,
        "hook misbehavior: {}{}".format(flag, suffix),
        expected_stderr=expected_error,
    )


def test_async_hook_sets_error_message(oauth_node):
    """A v2 async flow can also set a custom error message."""
    connstr = "{} user={} oauth_issuer={} oauth_client_id=myID".format(
        oauth_node.connstr(), _USER, _ISSUER
    )
    result, log_start = _run_hook_client(
        oauth_node,
        connstr,
        flags=["--misbehave", "fail-async", "--error", "async error message"],
    )
    _check(
        oauth_node,
        result,
        log_start,
        "asynchronous hook can set error messages",
        expected_stderr=r"user-defined OAuth flow failed: async error message",
    )

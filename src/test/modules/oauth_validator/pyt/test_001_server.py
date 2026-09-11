# Copyright (c) 2025-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long
"""Port of src/test/modules/oauth_validator/t/001_server.pl.

Tests the libpq builtin OAuth flow, along with server-side HBA and validator
setup, against a mock OAuth issuer (pyt/oauth_server.py, launched via the
OAuthServer harness). Gated behind PG_TEST_EXTRA=oauth; additionally requires a
platform with epoll/kqueue, a libcurl-enabled build (with_libcurl=yes) and
Python support (with_python=yes), exactly as the Perl original.

The test runs as a single ordered function because, like the Perl script, it
mutates shared server state (pg_hba.conf, pg_ident.conf, ALTER SYSTEM settings,
oauth_validator_libraries) sequentially and each step depends on the previous
one.
"""

import base64
import json
import os
import re

import pytest

import pypg

pytestmark = pypg.require_test_extras("oauth")

# Every allowed character for a client_id/secret (the OAuth "VSCHAR" class).
# Unlike Perl we do not need a separately backslash-escaped variant: connection
# options are passed as discrete strings, not embedded in a quoted connstr.
_VSCHARS = (
    " !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`"
    "abcdefghijklmnopqrstuvwxyz{|}~"
)

_VISIT = r"Visit https://example\.com/ and enter the code: postgresuser"
_VISIT_ORG = r"Visit https://example\.org/ and enter the code: postgresuser"


@pytest.fixture(scope="module", autouse=True)
def _require_platform():
    """Skip on platforms or builds without OAuth server-side support."""
    if not (
        pypg.check_pg_config(r"#define HAVE_SYS_EVENT_H 1")
        or pypg.check_pg_config(r"#define HAVE_SYS_EPOLL_H 1")
    ):
        pytest.skip("OAuth server-side tests are not supported on this platform")
    if os.environ.get("with_libcurl") != "yes":
        pytest.skip("client-side OAuth not supported by this build")
    if os.environ.get("with_python") != "yes":
        pytest.skip("OAuth tests require --with-python to run")


def _conninfo_quote(value):
    """Quote a value for inclusion in a libpq connection string.

    Mirrors libpq's keyword=value parsing: wrap in single quotes and escape
    embedded backslashes and single quotes, so the full VSCHAR set survives.
    """
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return "'{}'".format(escaped)


def _encode_connstr(base_connstr, **params):
    """Return base_connstr with a Base64(JSON) params blob as oauth_client_id.

    Mirrors the connstr() helper in 001_server.pl: the mock server's /param
    issuer decodes the test instructions out of the client_id field.
    """
    js = json.dumps(params, separators=(",", ":"))
    encoded = base64.b64encode(js.encode("utf-8")).decode("ascii")
    return "{} oauth_client_id={}".format(base_connstr, encoded)


def _wait_reload(node, offset):
    """Reload and wait for the config-reload log line, returning the new offset."""
    node.reload()
    return node.wait_for_log(r"reloading configuration files", offset)


def _setup_node(node):
    """Initialize the server exactly as the top of 001_server.pl does."""
    node.append_conf("log_connections = all\n")
    node.append_conf("oauth_validator_libraries = 'validator'\n")
    node.append_conf("log_min_messages = debug2")
    node.start()
    node.safe_psql("CREATE USER test;")
    node.safe_psql("CREATE USER testalt;")
    node.safe_psql("CREATE USER testparam;")


def _phase_http_rejected(node, issuer):
    """HTTP and untrusted HTTPS are refused without a real debug marker."""
    node.connect_fails(
        "user=test dbname=postgres oauth_issuer={} oauth_client_id=f02c6361-0635".format(
            issuer
        ),
        "HTTPS is required without debug mode",
        expected_stderr=(
            r'OAuth discovery URI "'
            + re.escape(issuer + "/.well-known/openid-configuration")
            + r'" must use HTTPS'
        ),
    )

    # PGOAUTHDEBUG=http should have no effect (it needs an UNSAFE: marker).
    saved = os.environ.get("PGOAUTHDEBUG")
    os.environ["PGOAUTHDEBUG"] = "http"
    try:
        node.connect_fails(
            "user=test dbname=postgres oauth_issuer={} oauth_client_id=f02c6361-0635".format(
                issuer
            ),
            "HTTPS is required without debug mode (bad PGOAUTHDEBUG value)",
            expected_stderr=(
                r'(?ms)^WARNING: .* option "http" is unsafe.*'
                r'OAuth discovery URI "'
                + re.escape(issuer + "/.well-known/openid-configuration")
                + r'" must use HTTPS'
            ),
        )
    finally:
        if saved is None:
            os.environ.pop("PGOAUTHDEBUG", None)
        else:
            os.environ["PGOAUTHDEBUG"] = saved


def _phase_https_hba(node, issuer, bgconn, offset):
    """Switch HBA to HTTPS issuers and verify pg_hba_file_rules() reflects it."""
    (node.datadir / "pg_hba.conf").unlink()
    node.append_conf(
        "\n"
        'local all test      oauth issuer="{0}"       scope="openid postgres"\n'
        'local all testalt   oauth issuer="{0}/.well-known/oauth-authorization-server/alternate" scope="openid postgres alt"\n'
        'local all testparam oauth issuer="{0}/param" scope="openid postgres"\n'.format(
            issuer
        ),
        filename="pg_hba.conf",
    )
    offset = _wait_reload(node, offset)

    contents = bgconn.query_safe(
        "SELECT rule_number, auth_method, options\n"
        "  FROM pg_hba_file_rules\n"
        "  ORDER BY rule_number;"
    )
    expected = (
        '1|oauth|{{issuer={0},"scope=openid postgres",validator=validator}}\n'
        '2|oauth|{{issuer={0}/.well-known/oauth-authorization-server/alternate,"scope=openid postgres alt",validator=validator}}\n'
        '3|oauth|{{issuer={0}/param,"scope=openid postgres",validator=validator}}'.format(
            issuer
        )
    )
    assert contents == expected, "pg_hba_file_rules recreates OAuth HBA settings"
    return offset


def _phase_ca_handling(node, issuer, alternative_ca):
    """Certificate verification: UNSAFE keeps cert checks; oauth_ca_file works."""
    saved = os.environ.get("PGOAUTHDEBUG")
    os.environ["PGOAUTHDEBUG"] = "UNSAFE"
    try:
        node.connect_fails(
            "user=test dbname=postgres oauth_issuer={} oauth_client_id=f02c6361-0635".format(
                issuer
            ),
            "HTTPS trusts only system CA roots by default",
            expected_stderr=r"(?i)could not fetch OpenID discovery document:.*peer certificate",
        )
    finally:
        if saved is None:
            os.environ.pop("PGOAUTHDEBUG", None)
        else:
            os.environ["PGOAUTHDEBUG"] = saved

    node.connect_ok(
        "user=test dbname=postgres oauth_issuer={} oauth_client_id=f02c6361-0635 oauth_ca_file={}".format(
            issuer, alternative_ca
        ),
        "connect as test (oauth_ca_file)",
        expected_stderr=_VISIT,
        log_like=[
            r'oauth_validator: token="9243959234", role="test"',
            r'oauth_validator: issuer="'
            + re.escape(issuer)
            + r'", scope="openid postgres"',
            r'connection authenticated: identity="test" method=oauth',
            r"connection authorized",
        ],
    )

    # From here on, rely on PGOAUTHCAFILE in the environment.
    os.environ["PGOAUTHCAFILE"] = alternative_ca
    node.connect_ok(
        "user=test dbname=postgres oauth_issuer={} oauth_client_id=f02c6361-0635".format(
            issuer
        ),
        "connect as test",
        expected_stderr=_VISIT,
        log_like=[
            r'oauth_validator: token="9243959234", role="test"',
            r'oauth_validator: issuer="'
            + re.escape(issuer)
            + r'", scope="openid postgres"',
            r'connection authenticated: identity="test" method=oauth',
            r"connection authorized",
        ],
        log_unlike=[r"FATAL.*OAuth bearer authentication failed"],
    )


def _phase_alternate_and_require_auth(node, issuer):
    """The /alternate issuer, issuer mismatch, and require_auth matrix."""
    # Enable extra debugging features for the remaining tests:
    # trace, dos-endpoint (faster handshake), and call-count.
    os.environ["PGOAUTHDEBUG"] = "UNSAFE:trace,dos-endpoint,call-count"

    node.connect_ok(
        "user=testalt dbname=postgres oauth_issuer={}/alternate oauth_client_id=f02c6361-0636".format(
            issuer
        ),
        "connect as testalt",
        expected_stderr=_VISIT_ORG,
        log_like=[
            r'oauth_validator: token="9243959234-alt", role="testalt"',
            r'oauth_validator: issuer="'
            + re.escape(issuer + "/.well-known/oauth-authorization-server/alternate")
            + r'", scope="openid postgres alt"',
            r'connection authenticated: identity="testalt" method=oauth',
            r"connection authorized",
        ],
        log_unlike=[r"FATAL.*OAuth bearer authentication failed"],
    )

    node.connect_fails(
        "user=testalt dbname=postgres oauth_issuer={} oauth_client_id=f02c6361-0636".format(
            issuer
        ),
        "oauth_issuer must match discovery",
        expected_stderr=(
            r"server's discovery document at "
            + re.escape(issuer + "/.well-known/oauth-authorization-server/alternate")
            + r' \(issuer "'
            + re.escape(issuer + "/alternate")
            + r'"\) is incompatible with oauth_issuer \('
            + re.escape(issuer)
            + r"\)"
        ),
    )

    ok_cases = [
        "oauth",
        "oauth,scram-sha-256",
        "password,oauth",
        "none,oauth",
        "!scram-sha-256",
        "!none",
    ]
    fail_cases = [
        ("!oauth", r"server requested OAUTHBEARER authentication"),
        ("scram-sha-256", r"server requested OAUTHBEARER authentication"),
        ("!password,!oauth", r"server requested OAUTHBEARER authentication"),
        ("none", r"server requested SASL authentication"),
        ("!oauth,!scram-sha-256", r"server requested SASL authentication"),
    ]
    base = "user=test dbname=postgres oauth_issuer={} oauth_client_id=f02c6361-0635".format(
        issuer
    )
    for require in ok_cases:
        node.connect_ok(
            "{} require_auth={}".format(base, require),
            "require_auth={} succeeds".format(require),
            expected_stderr=_VISIT,
        )
    for require, failure in fail_cases:
        node.connect_fails(
            "{} require_auth={}".format(base, require),
            "require_auth={} fails".format(require),
            expected_stderr=failure,
        )


def _phase_vschars(node, issuer):
    """The client_id/secret VSCHAR set is transmitted and encoded correctly."""
    node.connect_ok(
        "user=test dbname=postgres oauth_issuer={} oauth_client_id={}".format(
            issuer, _conninfo_quote(_VSCHARS)
        ),
        "escapable characters: client_id",
        expected_stderr=_VISIT,
    )
    node.connect_ok(
        "user=test dbname=postgres oauth_issuer={} oauth_client_id={} oauth_client_secret={}".format(
            issuer, _conninfo_quote(_VSCHARS), _conninfo_quote(_VSCHARS)
        ),
        "escapable characters: client_id and secret",
        expected_stderr=_VISIT,
    )


def _phase_param_basics(node, common):
    """The /param magic system works end-to-end, including token retries."""
    node.connect_ok(
        _encode_connstr(common), "connect to /param", expected_stderr=_VISIT
    )
    node.connect_ok(
        _encode_connstr(common, stage="token", retries=1),
        "token retry",
        expected_stderr=_VISIT,
    )
    node.connect_ok(
        _encode_connstr(common, stage="token", retries=2),
        "token retry (twice)",
        expected_stderr=_VISIT,
    )
    node.connect_ok(
        _encode_connstr(common, stage="all", retries=1, interval=2),
        "token retry (two second interval)",
        expected_stderr=_VISIT,
    )
    node.connect_ok(
        _encode_connstr(common, stage="all", retries=1, interval=None),
        "token retry (default interval)",
        expected_stderr=_VISIT,
    )


def _phase_param_content_type(node, common):
    """Content-type handling and the alternative verification_uri spelling."""
    node.connect_ok(
        _encode_connstr(
            common, stage="all", content_type="application/json;charset=utf-8"
        ),
        "content type with charset",
        expected_stderr=_VISIT,
    )
    node.connect_ok(
        _encode_connstr(
            common, stage="all", content_type="application/json \t;\t charset=utf-8"
        ),
        "content type with charset (whitespace)",
        expected_stderr=_VISIT,
    )
    node.connect_ok(
        _encode_connstr(common, stage="device", uri_spelling="verification_url"),
        "alternative spelling of verification_uri",
        expected_stderr=_VISIT,
    )


def _phase_param_bad_responses(node, common):
    """Overlarge, over-nested, and wrong-content-type responses are rejected."""
    node.connect_fails(
        _encode_connstr(common, stage="device", huge_response=True),
        "bad device authz response: overlarge JSON",
        expected_stderr=r"could not obtain device authorization: response is too large",
    )
    node.connect_fails(
        _encode_connstr(common, stage="token", huge_response=True),
        "bad token response: overlarge JSON",
        expected_stderr=r"could not obtain access token: response is too large",
    )

    nesting_limit = 16
    node.connect_ok(
        _encode_connstr(
            common,
            stage="device",
            nested_array=nesting_limit,
            nested_object=nesting_limit,
        ),
        "nested arrays and objects, up to parse limit",
        expected_stderr=_VISIT,
    )
    node.connect_fails(
        _encode_connstr(common, stage="device", nested_array=nesting_limit + 1),
        "bad discovery response: overly nested JSON array",
        expected_stderr=r"could not parse device authorization: JSON is too deeply nested",
    )
    node.connect_fails(
        _encode_connstr(common, stage="device", nested_object=nesting_limit + 1),
        "bad discovery response: overly nested JSON object",
        expected_stderr=r"could not parse device authorization: JSON is too deeply nested",
    )

    node.connect_fails(
        _encode_connstr(common, stage="device", content_type="text/plain"),
        "bad device authz response: wrong content type",
        expected_stderr=r"could not parse device authorization: unexpected content type",
    )
    node.connect_fails(
        _encode_connstr(common, stage="token", content_type="text/plain"),
        "bad token response: wrong content type",
        expected_stderr=r"could not parse access token response: unexpected content type",
    )
    node.connect_fails(
        _encode_connstr(common, stage="token", content_type="application/jsonx"),
        "bad token response: wrong content type (correct prefix)",
        expected_stderr=r"could not parse access token response: unexpected content type",
    )


def _phase_param_token_errors(node, common):
    """Interval overflow and the various OAuth token error responses."""
    node.connect_fails(
        _encode_connstr(
            common, stage="all", interval=(2**64) - 1, retries=1, retry_code="slow_down"
        ),
        "bad token response: server overflows the device authz interval",
        expected_stderr=r"could not obtain access token: slow_down interval overflow",
    )

    node.connect_fails(
        _encode_connstr(common, stage="token", error_code="invalid_grant"),
        "bad token response: invalid_grant, no description",
        expected_stderr=r"could not obtain access token: \(invalid_grant\)",
    )
    node.connect_fails(
        _encode_connstr(
            common,
            stage="token",
            error_code="invalid_grant",
            error_desc="grant expired",
        ),
        "bad token response: expired grant",
        expected_stderr=r"could not obtain access token: grant expired \(invalid_grant\)",
    )
    node.connect_fails(
        _encode_connstr(
            common, stage="token", error_code="invalid_client", error_status=401
        ),
        "bad token response: client authentication failure, default description",
        expected_stderr=r"could not obtain access token: provider requires client authentication, and no oauth_client_secret is set \(invalid_client\)",
    )
    node.connect_fails(
        _encode_connstr(
            common,
            stage="token",
            error_code="invalid_client",
            error_status=401,
            error_desc="authn failure",
        ),
        "bad token response: client authentication failure, provided description",
        expected_stderr=r"could not obtain access token: authn failure \(invalid_client\)",
    )

    node.connect_fails(
        _encode_connstr(common, stage="token", token=""),
        "server rejects access: empty token",
        expected_stderr=r"bearer authentication failed",
    )
    node.connect_fails(
        _encode_connstr(common, stage="token", token="****"),
        "server rejects access: invalid token contents",
        expected_stderr=r"bearer authentication failed",
    )


def _phase_client_secret(node, common):
    """oauth_client_secret is forwarded and reflected in error descriptions."""
    base = "{} oauth_client_secret=''".format(common)
    node.connect_ok(
        _encode_connstr(base, stage="all", expected_secret=""),
        "empty oauth_client_secret",
        expected_stderr=_VISIT,
    )

    base = "{} oauth_client_secret={}".format(common, _conninfo_quote(_VSCHARS))
    node.connect_ok(
        _encode_connstr(base, stage="all", expected_secret=_VSCHARS),
        "nonempty oauth_client_secret",
        expected_stderr=_VISIT,
    )

    node.connect_fails(
        _encode_connstr(
            base, stage="token", error_code="invalid_client", error_status=401
        ),
        "bad token response: client authentication failure, default description with oauth_client_secret",
        expected_stderr=r"could not obtain access token: provider rejected the oauth_client_secret \(invalid_client\)",
    )
    node.connect_fails(
        _encode_connstr(
            base,
            stage="token",
            error_code="invalid_client",
            error_status=401,
            error_desc="mutual TLS required for client",
        ),
        "bad token response: client authentication failure, provided description with oauth_client_secret",
        expected_stderr=r"could not obtain access token: mutual TLS required for client \(invalid_client\)",
    )


def _phase_call_count(node, common):
    """A retrying flow must not loop excessively (sanity-bound the poll count)."""
    result = node.psql_capture(
        "SELECT 'connected for call count'",
        extra_params=["-w"],
        connstr=_encode_connstr(common, stage="token", retries=2),
        on_error_stop=False,
    )
    assert result.exit_code == 0, "call count connection succeeds\n{}".format(
        result.stderr
    )
    assert re.search(_VISIT, result.stderr), "call count: stderr matches"

    match = re.search(r"\[libpq\] total number of polls: (\d+)", result.stderr)
    assert match is not None, "call count: count is printed"
    assert int(match.group(1)) < 100, "call count is reasonably small"


def _phase_stress_async(node, common):
    """The builtin flow must work even if the app ignores polling signals."""
    base = "{} port={} host={}".format(common, node.port, node.host)
    result = node.bin.run_command(
        [
            "oauth_hook_client",
            "--no-hook",
            "--stress-async",
            _encode_connstr(base, stage="all", retries=1, interval=1),
        ]
    )
    assert re.search(
        r"connection succeeded", result.stdout
    ), "stress-async: stdout matches"
    assert not re.search(
        r"connection to database failed", result.stderr
    ), "stress-async: stderr matches"


def _phase_validator_failshut(node, bgconn, common, offset):
    """A misbehaving validator must fail shut (no identity / not authorized)."""
    bgconn.query_safe("ALTER SYSTEM SET oauth_validator.authn_id TO ''")
    offset = _wait_reload(node, offset)
    node.connect_fails(
        "{} user=test".format(common),
        "validator must set authn_id",
        expected_stderr=r"OAuth bearer authentication failed",
        log_like=[
            r'connection authenticated: identity=""',
            r"FATAL:\s+OAuth bearer authentication failed",
            r"DETAIL:\s+Validator provided no identity",
        ],
    )

    bgconn.query_safe("ALTER SYSTEM SET oauth_validator.authn_id TO 'test@example.org'")
    bgconn.query_safe("ALTER SYSTEM SET oauth_validator.authorize_tokens TO false")
    offset = _wait_reload(node, offset)
    node.connect_fails(
        "{} user=test".format(common),
        "validator must authorize token explicitly",
        expected_stderr=r"OAuth bearer authentication failed",
        log_like=[
            r'connection authenticated: identity="test@example\.org"',
            r"FATAL:\s+OAuth bearer authentication failed",
            r"DETAIL:\s+Validator failed to authorize the provided token",
        ],
    )

    bgconn.query_safe(
        "ALTER SYSTEM SET oauth_validator.error_detail TO 'something failed'"
    )
    offset = _wait_reload(node, offset)
    node.connect_fails(
        "{} user=test".format(common),
        "validator must authorize token explicitly (custom logdetail)",
        expected_stderr=r"OAuth bearer authentication failed",
        log_like=[
            r'connection authenticated: identity="test@example\.org"',
            r"FATAL:\s+OAuth bearer authentication failed",
            r"DETAIL:\s+something failed",
        ],
    )

    bgconn.query_safe("ALTER SYSTEM SET oauth_validator.internal_error TO true")
    offset = _wait_reload(node, offset)
    node.connect_fails(
        "{} user=test".format(common),
        "validator internal error (custom logdetail)",
        expected_stderr=r"OAuth bearer authentication failed",
        log_like=[
            r"WARNING:\s+internal error in OAuth validator module",
            r"DETAIL:\s+something failed",
        ],
    )

    bgconn.query_safe("ALTER SYSTEM RESET oauth_validator.error_detail")
    bgconn.query_safe("ALTER SYSTEM RESET oauth_validator.internal_error")
    return offset


def _phase_bad_hba_option(node, bgconn, common, offset):
    """Registering a bad HBA option warns but lets connections proceed."""
    bgconn.query_safe("ALTER SYSTEM RESET oauth_validator.authn_id")
    bgconn.query_safe("ALTER SYSTEM RESET oauth_validator.authorize_tokens")
    bgconn.query_safe("ALTER SYSTEM SET oauth_validator.invalid_hba TO true")
    offset = _wait_reload(node, offset)
    node.connect_ok(
        "{} user=test".format(common),
        "bad registered HBA option",
        expected_stderr=_VISIT,
        log_like=[
            r'WARNING:\s+HBA option name "bad option name" is invalid and will be ignored',
            r'CONTEXT:\s+validator module "validator", in call to RegisterOAuthHBAOptions',
        ],
    )
    bgconn.query_safe("ALTER SYSTEM RESET oauth_validator.invalid_hba")
    return offset


def _phase_user_mapping(node, bgconn, common, issuer, offset):
    """User-mapping vs. ident delegation under an OAuth HBA."""
    (node.datadir / "pg_ident.conf").unlink()
    node.append_conf("\noauthmap\tuser@example.com\ttest\n", filename="pg_ident.conf")
    (node.datadir / "pg_hba.conf").unlink()
    node.append_conf(
        "\n"
        'local all test      oauth issuer="{0}" scope="" map=oauthmap\n'
        'local all testalt   oauth issuer="{0}" scope="" map=oauthmap\n'
        'local all testparam oauth issuer="{0}" scope="" delegate_ident_mapping=1\n'.format(
            issuer
        ),
        filename="pg_hba.conf",
    )
    bgconn.query_safe("ALTER SYSTEM RESET oauth_validator.authn_id")
    bgconn.query_safe("ALTER SYSTEM RESET oauth_validator.authorize_tokens")
    offset = _wait_reload(node, offset)

    node.connect_fails(
        "{} user=test".format(common),
        "mismatched username map (test)",
        expected_stderr=r"OAuth bearer authentication failed",
    )
    node.connect_fails(
        "{} user=testalt".format(common),
        "mismatched username map (testalt)",
        expected_stderr=r"OAuth bearer authentication failed",
    )

    bgconn.query_safe("ALTER SYSTEM SET oauth_validator.authn_id TO 'user@example.com'")
    offset = _wait_reload(node, offset)

    node.connect_ok(
        "{} user=test".format(common),
        "matched username map (test)",
        expected_stderr=_VISIT,
    )
    node.connect_fails(
        "{} user=testalt".format(common),
        "mismatched username map (testalt)",
        expected_stderr=r"OAuth bearer authentication failed",
    )
    node.connect_ok(
        "{} user=testparam".format(common),
        "delegated ident (testparam)",
        expected_stderr=_VISIT,
    )

    bgconn.query_safe("ALTER SYSTEM RESET oauth_validator.authn_id")
    offset = _wait_reload(node, offset)
    return offset


def _phase_validator_hba_options(node, common, issuer, offset):
    """Validator-specific HBA options, including bad-syntax restart failures."""
    (node.datadir / "pg_hba.conf").unlink()
    node.append_conf(
        "\n"
        'local all test    oauth issuer="{0}" scope="openid postgres" delegate_ident_mapping=1 \\\n'
        '                        validator.authn_id="ignored" validator.authn_id="other-identity"\n'
        'local all testalt oauth issuer="{0}" scope="openid postgres" validator.log="testalt message"\n'.format(
            issuer
        ),
        filename="pg_hba.conf",
    )
    offset = _wait_reload(node, offset)

    node.connect_ok(
        "{} user=test".format(common),
        "custom HBA setting (test)",
        expected_stderr=_VISIT,
        log_like=[r'connection authenticated: identity="other-identity"'],
    )
    node.connect_ok(
        "{} user=testalt".format(common),
        "custom HBA setting (testalt)",
        expected_stderr=_VISIT,
        log_like=[
            r"LOG:\s+testalt message",
            r'connection authenticated: identity="testalt"',
        ],
    )

    # Bad syntax: empty option name.
    (node.datadir / "pg_hba.conf").unlink()
    node.append_conf(
        '\nlocal all testalt oauth issuer="{0}" scope="openid postgres" validator.=1\n'.format(
            issuer
        ),
        filename="pg_hba.conf",
    )
    log_start = node.current_log_position()
    assert (
        node.restart(
            fail_ok=True,
            log_like=[r'invalid OAuth validator option name: "validator\."'],
        )
        is False
    ), "empty HBA option name"

    # Bad syntax: invalid characters in option name.
    (node.datadir / "pg_hba.conf").unlink()
    node.append_conf(
        '\nlocal all testalt oauth issuer="{0}" scope="openid postgres" validator.@@=1\n'.format(
            issuer
        ),
        filename="pg_hba.conf",
    )
    node.current_log_position()
    assert (
        node.restart(
            fail_ok=True,
            log_like=[r'invalid OAuth validator option name: "validator\.@@"'],
        )
        is False
    ), "invalid HBA option name"

    # Unknown settings: validation deferred to connect time.
    (node.datadir / "pg_hba.conf").unlink()
    node.append_conf(
        "\n"
        'local all testalt oauth issuer="{0}" scope="openid postgres" \\\n'
        "                        validator.log=ignored validator.bad=1\n".format(
            issuer
        ),
        filename="pg_hba.conf",
    )
    node.restart()
    node.connect_fails(
        "{} user=testalt".format(common),
        "bad HBA setting",
        expected_stderr=r"OAuth bearer authentication failed",
        log_like=[
            r'WARNING:\s+unrecognized authentication option name: "validator\.bad"',
            r"FATAL:\s+OAuth bearer authentication failed",
            r'DETAIL:\s+unrecognized authentication option name: "validator\.bad"',
        ],
    )
    _ = log_start
    return node.current_log_position()


def _phase_multiple_validators(node, issuer, offset):
    """With multiple validators each HBA line must name one explicitly."""
    node.append_conf("oauth_validator_libraries = 'validator, fail_validator'\n")
    with pytest.raises(pypg.PgServerError):
        # restart fails without explicit validators in oauth HBA entries
        node.restart()
    offset = node.wait_for_log(
        r'authentication method "oauth" requires option "validator" to be set', offset
    )

    (node.datadir / "pg_hba.conf").unlink()
    node.append_conf(
        "\n"
        'local all test    oauth validator=validator      issuer="{0}"           scope="openid postgres"\n'
        'local all testalt oauth validator=fail_validator issuer="{0}/.well-known/oauth-authorization-server/alternate" scope="openid postgres alt"\n'.format(
            issuer
        ),
        filename="pg_hba.conf",
    )
    node.restart()
    offset = node.wait_for_log(r"ready to accept connections", offset)

    node.connect_ok(
        "user=test dbname=postgres oauth_issuer={} oauth_client_id=f02c6361-0635".format(
            issuer
        ),
        "validator is used for test",
        expected_stderr=_VISIT,
        log_like=[r"connection authorized"],
    )
    node.connect_fails(
        "user=testalt dbname=postgres oauth_issuer={}/.well-known/oauth-authorization-server/alternate oauth_client_id=f02c6361-0636".format(
            issuer
        ),
        "fail_validator is used for testalt",
        expected_stderr=r"FATAL:\s+fail_validator: sentinel error",
    )
    return offset


def _phase_magic_validator(node, issuer, offset):
    """The ABI magic-marker mismatch is detected at module load."""
    node.append_conf("oauth_validator_libraries = 'magic_validator'\n")
    (node.datadir / "pg_hba.conf").unlink()
    node.append_conf(
        "\n"
        'local all test    oauth validator=magic_validator      issuer="{0}"           scope="openid postgres"\n'.format(
            issuer
        ),
        filename="pg_hba.conf",
    )
    node.restart()
    node.wait_for_log(r"ready to accept connections", offset)

    node.connect_fails(
        "user=test dbname=postgres oauth_issuer={}/.well-known/oauth-authorization-server/alternate oauth_client_id=f02c6361-0636".format(
            issuer
        ),
        "magic_validator is used",
        expected_stderr=r'FATAL:\s+OAuth validator module "magic_validator": magic number mismatch',
    )


def test_001_server(create_pg, webserver):  # pylint: disable=too-many-locals
    """End-to-end OAuth server-side, HBA, and validator behavior."""
    node = create_pg("primary", start=False)
    _setup_node(node)

    bgconn = node.background_psql()

    port = webserver.port
    cert_dir = os.environ["cert_dir"]
    alternative_ca = os.path.join(cert_dir, "root+server_ca.crt")

    try:
        # First confirm HTTP / untrusted HTTPS are refused.
        http_issuer = "http://127.0.0.1:{}".format(port)
        (node.datadir / "pg_hba.conf").unlink()
        node.append_conf(
            '\nlocal all test oauth issuer="{}" scope="openid postgres"\n'.format(
                http_issuer
            ),
            filename="pg_hba.conf",
        )
        offset = _wait_reload(node, 0)
        _phase_http_rejected(node, http_issuer)

        # Switch to HTTPS for the remainder of the test.
        issuer = "https://127.0.0.1:{}".format(port)
        offset = _phase_https_hba(node, issuer, bgconn, offset)
        _phase_ca_handling(node, issuer, alternative_ca)
        _phase_alternate_and_require_auth(node, issuer)
        _phase_vschars(node, issuer)

        common = "user=testparam dbname=postgres oauth_issuer={}/param ".format(issuer)
        _phase_param_basics(node, common)
        _phase_param_content_type(node, common)
        _phase_param_bad_responses(node, common)
        _phase_param_token_errors(node, common)
        _phase_client_secret(node, common)
        _phase_call_count(node, common)
        _phase_stress_async(node, common)

        # The validator-reconfiguration phases hardcode the discovery URI and an
        # empty scope to keep the logs uncluttered.
        common = (
            "dbname=postgres oauth_issuer={}/.well-known/openid-configuration "
            "oauth_scope='' oauth_client_id=f02c6361-0635".format(issuer)
        )
        offset = _phase_validator_failshut(node, bgconn, common, offset)
        offset = _phase_bad_hba_option(node, bgconn, common, offset)
        offset = _phase_user_mapping(node, bgconn, common, issuer, offset)

        bgconn.quit()  # the remaining phases restart the server

        offset = _phase_validator_hba_options(node, common, issuer, offset)
        offset = _phase_multiple_validators(node, issuer, offset)
        _phase_magic_validator(node, issuer, offset)
        node.stop()
    finally:
        os.environ.pop("PGOAUTHCAFILE", None)
        os.environ.pop("PGOAUTHDEBUG", None)

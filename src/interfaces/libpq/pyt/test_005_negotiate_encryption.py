# Copyright (c) 2021-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long
"""Port of src/interfaces/libpq/t/005_negotiate_encryption.pl.

Tests negotiation of SSL and GSSAPI encryption across the cube of libpq client
options that affect protocol negotiation (gssencmode, sslmode, sslnegotiation),
the server accepting or rejecting authentication via pg_hba.conf, and SSL/GSS
being enabled or disabled in the server.

The approach is table-driven: each combination is a line listing the options and
an expected outcome (whether the connection succeeds and which encryption it
uses) plus a condensed trace of the negotiation EVENTS, scraped from the server
log. That catches useless retries or wrong-order negotiation even when the final
outcome is unaffected.

This test is gated on PG_TEST_EXTRA containing "libpq_encryption" (it uses TCP
with trust auth, which is potentially unsafe on multiuser systems). The GSSAPI
matrices additionally require a GSSAPI build plus "kerberos" in PG_TEST_EXTRA,
and the SSL matrices require an OpenSSL build. Sections whose prerequisites are
absent are skipped exactly as in the Perl original, so on a build without SSL
and GSS only the plain-negotiation matrix runs.
"""

import os
import pathlib
import shutil

import pytest

import pypg
from pypg.util import slurp_file

_HOST = "enc-test-localhost.postgresql.example.com"
_HOSTADDR = "127.0.0.1"
_SERVERCIDR = "127.0.0.1/32"

_ALL_TEST_USERS = ["testuser", "ssluser", "nossluser", "gssuser", "nogssuser"]
_ALL_GSSENCMODES = ["disable", "prefer", "require"]
_ALL_SSLMODES = ["disable", "allow", "prefer", "require"]
_ALL_SSLNEGOTIATIONS = ["postgres", "direct"]

_CURRENT_ENC_FN = """
CREATE FUNCTION current_enc() RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  ssl_in_use bool;
  gss_in_use bool;
BEGIN
  ssl_in_use = (SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid());
  gss_in_use = (SELECT encrypted FROM pg_stat_gssapi WHERE pid = pg_backend_pid());

  raise log 'ssl %  gss %', ssl_in_use, gss_in_use;

  IF ssl_in_use AND gss_in_use THEN
    RETURN 'ssl+gss';   -- shouldn't happen
  ELSIF ssl_in_use THEN
    RETURN 'ssl';
  ELSIF gss_in_use THEN
    RETURN 'gss';
  ELSE
    RETURN 'plain';
  END IF;
END;
$$;
"""


class _Env:
    """Shared per-run state: the node, host, and SSL/GSS capability flags."""

    def __init__(self, node, ssl_supported, gss_supported, injection_points, unixdir):
        self.node = node
        self.ssl_supported = ssl_supported
        self.gss_supported = gss_supported
        self.injection_points = injection_points
        self.unixdir = unixdir
        self.failures = []


def _parse_log_events(log_contents):
    """Scrape the server log for the negotiation events of the test tables.

    Mirrors parse_log_events in the Perl original. Returns the ordered list of
    event tokens, or ["-"] when no events are present.
    """
    events = []
    for line in log_contents.split("\n"):
        if "connection received" in line:
            events.append("reconnect" if events else "connect")
        if "SSLRequest accepted" in line:
            events.append("sslaccept")
        if "SSLRequest rejected" in line:
            events.append("sslreject")
        if "direct SSL connection accepted" in line:
            events.append("directsslaccept")
        if "direct SSL connection rejected" in line:
            events.append("directsslreject")
        if "GSSENCRequest accepted" in line:
            events.append("gssaccept")
        if "GSSENCRequest rejected" in line:
            events.append("gssreject")
        if "no pg_hba.conf entry" in line:
            events.append("authfail")
        if "connection authenticated" in line:
            events.append("authok")
        if "error triggered for injection point backend-" in line:
            events.append("backenderror")
        if "protocol version 2 error triggered" in line:
            events.append("v2error")
    if not events:
        events.append("-")
    return events


def _expand_expected_line(user, gssencmode, sslmode, sslnegotiation, expected):
    """Expand any '*' wildcard fields into all of their possible values.

    Mirrors expand_expected_line: returns a dict mapping
    "user gssencmode sslmode sslnegotiation" to the expected events-and-outcome.
    """
    result = {}
    if user == "*":
        for value in _ALL_TEST_USERS:
            result.update(
                _expand_expected_line(
                    value, gssencmode, sslmode, sslnegotiation, expected
                )
            )
    elif gssencmode == "*":
        for value in _ALL_GSSENCMODES:
            result.update(
                _expand_expected_line(user, value, sslmode, sslnegotiation, expected)
            )
    elif sslmode == "*":
        for value in _ALL_SSLMODES:
            result.update(
                _expand_expected_line(user, gssencmode, value, sslnegotiation, expected)
            )
    elif sslnegotiation == "*":
        for value in _ALL_SSLNEGOTIATIONS:
            result.update(
                _expand_expected_line(user, gssencmode, sslmode, value, expected)
            )
    else:
        result["{} {} {} {}".format(user, gssencmode, sslmode, sslnegotiation)] = (
            expected
        )
    return result


def _parse_table(table):
    """Parse a test table into a dict of expected events-and-outcomes.

    Mirrors parse_table: trims comments and whitespace, ignores empty lines, and
    expands wildcards and the '.' (same-as-previous) shorthand.
    """
    expected = {}
    user = gssencmode = sslmode = sslnegotiation = None
    for raw_line in table.split("\n"):
        line = raw_line.split("#", 1)[0].strip()
        if line == "":
            continue
        fields = line.split(None, 4)
        if len(fields) != 5:
            raise ValueError('could not parse line "{}"'.format(line))
        col_user, col_gss, col_ssl, col_neg, rest = fields
        if col_user != ".":
            user = col_user
        if col_gss != ".":
            gssencmode = col_gss
        if col_ssl != ".":
            sslmode = col_ssl
        if col_neg != ".":
            sslnegotiation = col_neg

        events_part, outcome = rest.split("->")
        events = [e.strip() for e in events_part.split(",")]
        events_str = ", ".join(events).rstrip()
        events_and_outcome = "{} -> {}".format(events_str, outcome.strip())

        expected.update(
            _expand_expected_line(
                user, gssencmode, sslmode, sslnegotiation, events_and_outcome
            )
        )
    return expected


def _connect_test(env, connstr, expected_events_and_outcome):
    """Connect with connstr and verify the negotiation events and outcome.

    Mirrors connect_test: runs psql with -c 'SELECT current_enc()', records the
    log produced during the attempt, derives the EVENTS from it, and compares
    "events -> outcome" against the expectation. A non-zero psql exit yields the
    outcome 'fail', otherwise the trimmed stdout (plain/ssl/gss) is the outcome.
    """
    node = env.node
    test_name = " '{}' -> {}".format(connstr, expected_events_and_outcome)

    connstr_full = ""
    if "dbname=" not in connstr:
        connstr_full += "dbname=postgres "
    if "host=" not in connstr:
        connstr_full += "host={} hostaddr={} ".format(_HOST, _HOSTADDR)
    connstr_full += connstr

    log_location = node.current_log_position()

    result = node.psql_capture(
        "",
        connstr=connstr_full,
        on_error_stop=False,
        extra_params=["--no-password", "--command", "SELECT current_enc()"],
    )
    outcome = result.stdout if result.rc == 0 else "fail"

    log_contents = slurp_file(node.log, log_location)
    events = _parse_log_events(log_contents)
    events_and_outcome = ", ".join(events) + " -> {}".format(outcome)

    if events_and_outcome != expected_events_and_outcome:
        env.failures.append(
            "{}: got {!r} stderr {!r}".format(
                test_name, events_and_outcome, result.stderr
            )
        )


def _test_matrix(env, test_users, gssencmodes, sslmodes, sslnegotiations, expected):
    """Test the cube of user x gssencmode x sslmode x sslnegotiation.

    Mirrors test_matrix: missing table entries are reported as a missing line.
    """
    for test_user in test_users:
        for gssencmode in gssencmodes:
            for client_mode in sslmodes:
                for negotiation in sslnegotiations:
                    key = "{} {} {} {}".format(
                        test_user, gssencmode, client_mode, negotiation
                    )
                    expected_events = expected.get(
                        key, "<line missing from expected output table>"
                    )
                    _connect_test(
                        env,
                        "user={} gssencmode={} sslmode={} sslnegotiation={}".format(
                            test_user, gssencmode, client_mode, negotiation
                        ),
                        expected_events,
                    )


def _setup_server(create_pg, ssl_supported, gss_supported):
    """Initialise and start the test server (SSL/GSS not yet enabled).

    Mirrors the server preparation: TCP listener on the loopback, connection-
    negotiation logging, the users, current_enc(), and the narrow pg_hba.conf.
    """
    node = create_pg("node", start=False)
    node.append_conf(
        "listen_addresses = '{}'\n"
        "log_connections = 'receipt,authentication,authorization'\n"
        "log_disconnections = on\n"
        "trace_connection_negotiation = on\n"
        "lc_messages = 'C'\n".format(_HOSTADDR)
    )
    pgdata = node.datadir

    if ssl_supported:
        # The SSL cert fixtures live under src/test/ssl/ssl. From this file
        # (src/interfaces/libpq/pyt/) the repo root is four parents up.
        repo_root = pathlib.Path(__file__).resolve().parents[4]
        certdir = repo_root / "src" / "test" / "ssl" / "ssl"
        shutil.copy(certdir / "server-cn-only.crt", pgdata / "server.crt")
        shutil.copy(certdir / "server-cn-only.key", pgdata / "server.key")
        os.chmod(pgdata / "server.key", 0o600)
        node.append_conf("ssl = off\n")

    node.start()

    injection_points = node.check_extension("injection_points")

    for user in (
        "localuser",
        "testuser",
        "ssluser",
        "nossluser",
        "gssuser",
        "nogssuser",
    ):
        node.safe_psql("CREATE USER {};".format(user))
    if injection_points:
        node.safe_psql("CREATE EXTENSION injection_points;")

    node.safe_psql(_CURRENT_ENC_FN)

    # Capture the socket directory now, while the default trust-all pg_hba.conf
    # still admits the OS user; the narrow rules written below would reject it.
    unixdir = node.safe_psql("SHOW unix_socket_directories;")

    hba = (
        "\n"
        "# TYPE        DATABASE        USER            ADDRESS                 METHOD             OPTIONS\n"
        "local         postgres        localuser                               trust\n"
        "host          postgres        testuser        {cidr}             trust\n"
        "hostnossl     postgres        nossluser       {cidr}             trust\n"
        "hostnogssenc  postgres        nogssuser       {cidr}             trust\n"
    ).format(cidr=_SERVERCIDR)
    if ssl_supported:
        hba += (
            "\nhostssl       postgres        ssluser         {cidr}             trust\n"
        ).format(cidr=_SERVERCIDR)
    if gss_supported:
        hba += (
            "\nhostgssenc    postgres        gssuser         {cidr}             trust\n"
        ).format(cidr=_SERVERCIDR)
    with open(pgdata / "pg_hba.conf", "w", encoding="utf-8") as fh:
        fh.write(hba)
    node.reload()

    return node, injection_points, unixdir


def _table_ssl_gss_disabled(ssl_supported):
    """The expected table for SSL and GSS both disabled in the server."""
    if ssl_supported:
        table = """
# USER      GSSENCMODE   SSLMODE      SSLNEGOTIATION EVENTS                      -> OUTCOME
testuser    disable      disable      postgres       connect, authok             -> plain
.           .            allow        postgres       connect, authok             -> plain
.           .            prefer       postgres       connect, sslreject, authok  -> plain
.           .            require      postgres       connect, sslreject          -> fail
.           .            .            direct         connect, directsslreject    -> fail
.           prefer       disable      postgres       connect, authok             -> plain
.           .            allow        postgres       connect, authok             -> plain
.           .            prefer       postgres       connect, sslreject, authok  -> plain
.           .            require      postgres       connect, sslreject          -> fail
.           .            .            direct         connect, directsslreject    -> fail

# sslnegotiation=direct is not accepted unless sslmode=require or stronger
*           *            disable      direct         -     -> fail
*           *            allow        direct         -     -> fail
*           *            prefer       direct         -     -> fail
"""
    else:
        table = """
# USER      GSSENCMODE   SSLMODE      SSLNEGOTIATION EVENTS                      -> OUTCOME
testuser    disable      disable      postgres       connect, authok             -> plain
.           .            allow        postgres       connect, authok             -> plain
.           .            prefer       postgres       connect, authok             -> plain
.           prefer       disable      postgres       connect, authok             -> plain
.           .            allow        postgres       connect, authok             -> plain
.           .            prefer       postgres       connect, authok             -> plain

# Without SSL support, sslmode=require and sslnegotiation=direct are
# not accepted at all
*           *            require      *              -     -> fail
*           *            *            direct         -     -> fail
	"""
    # All attempts with gssencmode=require fail without connecting because no
    # credential cache has been configured (or GSS is not compiled in).
    table += """
testuser    require      *            *              - -> fail
"""
    return table


def _table_ssl_enabled():
    """The expected table for SSL enabled and GSS disabled in the server."""
    return """
# USER      GSSENCMODE   SSLMODE      SSLNEGOTIATION EVENTS                                          -> OUTCOME
testuser    disable      disable      postgres       connect, authok                                 -> plain
.           .            allow        postgres       connect, authok                                 -> plain
.           .            prefer       postgres       connect, sslaccept, authok                      -> ssl
.           .            require      postgres       connect, sslaccept, authok                      -> ssl
.           .            .            direct         connect, directsslaccept, authok                -> ssl
ssluser     .            disable      postgres       connect, authfail                               -> fail
.           .            allow        postgres       connect, authfail, reconnect, sslaccept, authok -> ssl
.           .            prefer       postgres       connect, sslaccept, authok                      -> ssl
.           .            require      postgres       connect, sslaccept, authok                      -> ssl
.           .            .            direct         connect, directsslaccept, authok                -> ssl
nossluser   .            disable      postgres       connect, authok                                 -> plain
.           .            allow        postgres       connect, authok                                 -> plain
.           .            prefer       postgres       connect, sslaccept, authfail, reconnect, authok -> plain
.           .            require      postgres       connect, sslaccept, authfail                    -> fail
.           .            require      direct         connect, directsslaccept, authfail              -> fail

# sslnegotiation=direct is not accepted unless sslmode=require or stronger
*           *            disable      direct         -     -> fail
*           *            allow        direct         -     -> fail
*           *            prefer       direct         -     -> fail
"""


def _table_gss_enabled(ssl_supported):
    """The expected table for GSS enabled and SSL disabled in the server."""
    base = """
# USER      GSSENCMODE   SSLMODE      SSLNEGOTIATION EVENTS                       -> OUTCOME
testuser    disable      disable      postgres       connect, authok              -> plain
.           .            allow        postgres       connect, authok              -> plain
.           .            prefer       postgres       connect, sslreject, authok   -> plain
.           .            require      postgres       connect, sslreject                -> fail
.           .            .            direct         connect, directsslreject          -> fail
.           prefer       *            postgres       connect, gssaccept, authok        -> gss
.           prefer       require      direct         connect, gssaccept, authok        -> gss
.           require      *            postgres       connect, gssaccept, authok        -> gss
.           .            require      direct         connect, gssaccept, authok        -> gss

gssuser     disable      disable      postgres       connect, authfail                  -> fail
.           .            allow        postgres       connect, authfail, reconnect, sslreject -> fail
.           .            prefer       postgres       connect, sslreject, authfail       -> fail
.           .            require      postgres       connect, sslreject                 -> fail
.           .            .            direct         connect, directsslreject           -> fail
.           prefer       *            postgres       connect, gssaccept, authok   -> gss
.           prefer       require      direct         connect, gssaccept, authok   -> gss
.           require      *            postgres       connect, gssaccept, authok   -> gss
.           .            require      direct         connect, gssaccept, authok   -> gss

nogssuser   disable      disable      postgres       connect, authok              -> plain
.           .            allow        postgres       connect, authok              -> plain
.           .            prefer       postgres       connect, sslreject, authok   -> plain
.           .            require      postgres       connect, sslreject                 -> fail
.           .            .            direct         connect, directsslreject           -> fail
.           prefer       disable      postgres       connect, gssaccept, authfail, reconnect, authok             -> plain
.           .            allow        postgres       connect, gssaccept, authfail, reconnect, authok             -> plain
.           .            prefer       postgres       connect, gssaccept, authfail, reconnect, sslreject, authok  -> plain
.           .            require      postgres       connect, gssaccept, authfail, reconnect, sslreject          -> fail
.           .            .            direct         connect, gssaccept, authfail, reconnect, directsslreject          -> fail
.           require      disable      postgres       connect, gssaccept, authfail -> fail
.           .            allow        postgres       connect, gssaccept, authfail -> fail
.           .            prefer       postgres       connect, gssaccept, authfail -> fail
.           .            require      postgres       connect, gssaccept, authfail -> fail   # If both GSSAPI and sslmode are required, and GSS is not available -> fail
.           .            .            direct         connect, gssaccept, authfail -> fail   # If both GSSAPI and sslmode are required, and GSS is not available -> fail

# sslnegotiation=direct is not accepted unless sslmode=require or stronger
*           *            disable      direct         -     -> fail
*           *            allow        direct         -     -> fail
*           *            prefer       direct         -     -> fail
	"""
    if ssl_supported:
        sslmodes, sslnegotiations = _ALL_SSLMODES, _ALL_SSLNEGOTIATIONS
    else:
        sslmodes, sslnegotiations = ["disable"], ["postgres"]
    return base, sslmodes, sslnegotiations


def _table_ssl_and_gss_enabled():
    """The expected table for both GSS and SSL enabled in the server."""
    return """
# USER      GSSENCMODE   SSLMODE      SSLNEGOTIATION EVENTS                       -> OUTCOME
testuser    disable      disable      postgres       connect, authok              -> plain
.           .            allow        postgres       connect, authok              -> plain
.           .            prefer       postgres       connect, sslaccept, authok   -> ssl
.           .            require      postgres       connect, sslaccept, authok   -> ssl
.           .            .            direct         connect, directsslaccept, authok   -> ssl
.           prefer       disable      postgres       connect, gssaccept, authok   -> gss
.           .            allow        postgres       connect, gssaccept, authok   -> gss
.           .            prefer       postgres       connect, gssaccept, authok   -> gss
.           .            require      postgres       connect, gssaccept, authok   -> gss     # If both GSS and SSL is possible, GSS is chosen over SSL, even if sslmode=require
.           .            .            direct         connect, gssaccept, authok   -> gss
.           require      disable      postgres       connect, gssaccept, authok   -> gss
.           .            allow        postgres       connect, gssaccept, authok   -> gss
.           .            prefer       postgres       connect, gssaccept, authok   -> gss
.           .            require      postgres       connect, gssaccept, authok   -> gss     # If both GSS and SSL is possible, GSS is chosen over SSL, even if sslmode=require
.           .            .            direct         connect, gssaccept, authok   -> gss

gssuser     disable      disable      postgres       connect, authfail            -> fail
.           .            allow        postgres       connect, authfail, reconnect, sslaccept, authfail -> fail
.           .            prefer       postgres       connect, sslaccept, authfail, reconnect, authfail -> fail
.           .            require      postgres       connect, sslaccept, authfail       -> fail
.           .            .            direct         connect, directsslaccept, authfail -> fail
.           prefer       disable      postgres       connect, gssaccept, authok   -> gss
.           .            allow        postgres       connect, gssaccept, authok   -> gss
.           .            prefer       postgres       connect, gssaccept, authok   -> gss
.           .            require      postgres       connect, gssaccept, authok   -> gss   # GSS is chosen over SSL, even though sslmode=require
.           .            .            direct         connect, gssaccept, authok   -> gss
.           require      disable      postgres       connect, gssaccept, authok   -> gss
.           .            allow        postgres       connect, gssaccept, authok   -> gss
.           .            prefer       postgres       connect, gssaccept, authok   -> gss
.           .            require      postgres       connect, gssaccept, authok   -> gss     # If both GSS and SSL is possible, GSS is chosen over SSL, even if sslmode=require
.           .            .            direct         connect, gssaccept, authok   -> gss

ssluser     disable      disable      postgres       connect, authfail            -> fail
.           .            allow        postgres       connect, authfail, reconnect, sslaccept, authok       -> ssl
.           .            prefer       postgres       connect, sslaccept, authok         -> ssl
.           .            require      postgres       connect, sslaccept, authok         -> ssl
.           .            .            direct         connect, directsslaccept, authok   -> ssl
.           prefer       disable      postgres       connect, gssaccept, authfail, reconnect, authfail -> fail
.           .            allow        postgres       connect, gssaccept, authfail, reconnect, authfail, reconnect, sslaccept, authok       -> ssl
.           .            prefer       postgres       connect, gssaccept, authfail, reconnect, sslaccept, authok       -> ssl
.           .            require      postgres       connect, gssaccept, authfail, reconnect, sslaccept, authok       -> ssl
.           .            .            direct         connect, gssaccept, authfail, reconnect, directsslaccept, authok -> ssl
.           require      disable      postgres       connect, gssaccept, authfail -> fail
.           .            allow        postgres       connect, gssaccept, authfail -> fail
.           .            prefer       postgres       connect, gssaccept, authfail -> fail
.           .            require      postgres       connect, gssaccept, authfail -> fail         # If both GSS and SSL are required, the sslmode=require is effectively ignored and GSS is required
.           .            .            direct         connect, gssaccept, authfail -> fail

nogssuser   disable      disable      postgres       connect, authok              -> plain
.           .            allow        postgres       connect, authok              -> plain
.           .            prefer       postgres       connect, sslaccept, authok   -> ssl
.           .            require      postgres       connect, sslaccept, authok   -> ssl
.           .            .            direct         connect, directsslaccept, authok   -> ssl
.           prefer       disable      postgres       connect, gssaccept, authfail, reconnect, authok              -> plain
.           .            allow        postgres       connect, gssaccept, authfail, reconnect, authok              -> plain
.           .            prefer       postgres       connect, gssaccept, authfail, reconnect, sslaccept, authok         -> ssl
.           .            require      postgres       connect, gssaccept, authfail, reconnect, sslaccept, authok         -> ssl
.           .            .            direct         connect, gssaccept, authfail, reconnect, directsslaccept, authok   -> ssl
.           require      disable      postgres       connect, gssaccept, authfail -> fail
.           .            allow        postgres       connect, gssaccept, authfail -> fail
.           .            prefer       postgres       connect, gssaccept, authfail -> fail
.           .            require      postgres       connect, gssaccept, authfail -> fail   # If both GSS and SSL are required, the sslmode=require is effectively ignored and GSS is required
.           .            .            direct         connect, gssaccept, authfail -> fail

nossluser   disable      disable      postgres       connect, authok              -> plain
.           .            allow        postgres       connect, authok              -> plain
.           .            prefer       postgres       connect, sslaccept, authfail, reconnect, authok       -> plain
.           .            require      postgres       connect, sslaccept, authfail       -> fail
.           .            .            direct         connect, directsslaccept, authfail -> fail
.           prefer       *            postgres       connect, gssaccept, authok   -> gss
.           .            require      direct         connect, gssaccept, authok   -> gss
.           require      *            postgres       connect, gssaccept, authok   -> gss
.           .            require      direct         connect, gssaccept, authok   -> gss

# sslnegotiation=direct is not accepted unless sslmode=require or stronger
*           *            disable      direct         -     -> fail
*           *            allow        direct         -     -> fail
*           *            prefer       direct         -     -> fail
	"""


def _run_injection_ssl(env, unixdir):
    """Injection-point error scenarios with SSL enabled in the server."""
    node = env.node
    node.safe_psql(
        "SELECT injection_points_attach('backend-initialize', 'error');",
        connstr="user=localuser host={}".format(unixdir),
    )
    _connect_test(env, "user=testuser sslmode=prefer", "connect, backenderror -> fail")
    node.restart()

    node.safe_psql(
        "SELECT injection_points_attach('backend-initialize-v2-error', 'error');",
        connstr="user=localuser host={}".format(unixdir),
    )
    _connect_test(env, "user=testuser sslmode=prefer", "connect, v2error -> fail")
    node.restart()

    node.safe_psql(
        "SELECT injection_points_attach('backend-ssl-startup', 'error');",
        connstr="user=localuser host={}".format(unixdir),
    )
    _connect_test(
        env,
        "user=testuser sslmode=prefer",
        "connect, sslaccept, backenderror, reconnect, authok -> plain",
    )
    node.restart()


def _run_injection_gss(env, unixdir):
    """Injection-point error scenarios with GSS enabled in the server."""
    node = env.node
    node.safe_psql(
        "SELECT injection_points_attach('backend-initialize', 'error');",
        connstr="user=localuser host={}".format(unixdir),
    )
    _connect_test(
        env,
        "user=testuser gssencmode=prefer sslmode=disable",
        "connect, backenderror, reconnect, backenderror -> fail",
    )
    node.restart()

    node.safe_psql(
        "SELECT injection_points_attach('backend-initialize-v2-error', 'error');",
        connstr="user=localuser host={}".format(unixdir),
    )
    _connect_test(
        env,
        "user=testuser gssencmode=prefer sslmode=disable",
        "connect, v2error, reconnect, v2error -> fail",
    )
    node.restart()

    node.safe_psql(
        "SELECT injection_points_attach('backend-gssapi-startup', 'error');",
        connstr="user=localuser host={}".format(unixdir),
    )
    _connect_test(
        env,
        "user=testuser gssencmode=prefer sslmode=disable",
        "connect, gssaccept, backenderror, reconnect, authok -> plain",
    )
    node.restart()


def _run_ssl_section(env, kerberos_enabled):
    """Tests with GSS disabled and SSL enabled in the server."""
    if not env.ssl_supported:
        return
    node = env.node
    table = _table_ssl_enabled()

    node.adjust_conf("ssl", "on")
    node.reload()

    _test_matrix(
        env,
        ["testuser", "ssluser", "nossluser"],
        ["disable"],
        _ALL_SSLMODES,
        _ALL_SSLNEGOTIATIONS,
        _parse_table(table),
    )

    if env.injection_points:
        _run_injection_ssl(env, env.unixdir)

    node.adjust_conf("ssl", "off")
    node.reload()
    _ = kerberos_enabled


def _run_unix_section(env):
    """Negotiation over Unix-domain sockets (no SSL or GSSAPI attempted)."""
    unixdir = env.unixdir
    if unixdir == "":
        return
    _connect_test(
        env,
        "user=localuser gssencmode=prefer sslmode=prefer host={}".format(unixdir),
        "connect, authok -> plain",
    )
    _connect_test(
        env,
        "user=localuser gssencmode=require sslmode=prefer host={}".format(unixdir),
        "- -> fail",
    )


def test_005_negotiate_encryption(create_pg):
    """SSL/GSS encryption negotiation across the full client-option matrix."""
    pypg.skip_unless_test_extras("libpq_encryption")

    gss_supported = os.environ.get("with_gssapi") == "yes"
    kerberos_enabled = "kerberos" in os.environ.get("PG_TEST_EXTRA", "").split()
    ssl_supported = os.environ.get("with_ssl") == "openssl"

    node, injection_points, unixdir = _setup_server(
        create_pg, ssl_supported, gss_supported
    )
    env = _Env(node, ssl_supported, gss_supported, injection_points, unixdir)

    # Run tests with GSS and SSL disabled in the server.
    table = _table_ssl_gss_disabled(ssl_supported)
    _test_matrix(
        env,
        ["testuser"],
        _ALL_GSSENCMODES,
        _ALL_SSLMODES,
        _ALL_SSLNEGOTIATIONS,
        _parse_table(table),
    )

    _run_ssl_section(env, kerberos_enabled)

    # The GSSAPI sections require a GSSAPI build and kerberos in PG_TEST_EXTRA,
    # and rely on PostgreSQL::Test::Kerberos to provision principals/tickets.
    # That Kerberos test infrastructure has no pypg equivalent yet, so when GSS
    # would otherwise run we surface that as a skip rather than silently passing.
    if gss_supported and kerberos_enabled:
        pytest.skip(
            "GSSAPI/Kerberos sections require PostgreSQL::Test::Kerberos, "
            "which has no pypg port yet"
        )

    _run_unix_section(env)

    node.teardown_node()

    assert not env.failures, "negotiation mismatches:\n" + "\n".join(env.failures)

# Copyright (c) 2021-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long
"""Port of src/test/ssl/t/003_sslinfo.pl.

Exercises the sslinfo extension over a TLS connection: ssl_is_used,
ssl_version, ssl_cipher, ssl_client_cert_present, ssl_client_serial,
ssl_client_dn_field, ssl_issuer_dn, ssl_issuer_field and ssl_extension_info,
cross-checked against pg_stat_ssl, plus sslcertmode handling.
"""

import os

import pytest

import pypg
from pypg.ssl_server import SSLServer

# This suite opens up local TCP ports and is hidden behind PG_TEST_EXTRA=ssl.
pytestmark = pypg.require_test_extras("ssl")

SERVERHOSTADDR = "127.0.0.1"
SERVERHOSTCIDR = "127.0.0.1/32"

# Defaults that protect against any ~/.postgresql certificate/key files.
DEFAULT_SSL_CONNSTR = (
    "sslkey=invalid sslcert=invalid sslrootcert=invalid "
    "sslcrl=invalid sslcrldir=invalid"
)


def test_003_sslinfo(create_pg, tmp_path):
    """sslinfo functions report the expected TLS/cert details via pg_stat_ssl."""
    if os.environ.get("with_ssl") != "openssl":
        pytest.skip("OpenSSL not supported by this build")

    ssl_server = SSLServer(tmp_path)
    supports_sslcertmode_require = pypg.check_pg_config(
        "#define HAVE_SSL_CTX_SET_CERT_CB 1"
    )

    node = create_pg("primary", hostaddr=SERVERHOSTADDR, start=True)
    ssl_server.configure_test_server_for_ssl(
        node, SERVERHOSTADDR, SERVERHOSTCIDR, "trust", extensions=["sslinfo"]
    )
    # server-revoked is reused as the server cert as in the 001 test; no CRLs
    # are exercised here.
    ssl_server.switch_server_cert(node, certfile="server-revoked")

    common_connstr = (
        "{default} sslrootcert=ssl/root+server_ca.crt sslmode=require dbname=certdb "
        "hostaddr={addr} host=localhost user=ssltestuser sslcert=ssl/client_ext.crt{key}".format(
            default=DEFAULT_SSL_CONNSTR,
            addr=SERVERHOSTADDR,
            key=ssl_server.sslkey("client_ext.key"),
        )
    )

    node.connect_ok(
        common_connstr,
        "certificate authorization succeeds with correct client cert in PEM format",
    )

    _test_with_cert(node, common_connstr)
    _test_without_cert(node)
    _test_sslcertmode(node, common_connstr, supports_sslcertmode_require)


def _test_with_cert(node, common_connstr):
    """sslinfo functions for a connection that presents a client certificate."""
    assert (
        node.safe_psql("SELECT ssl_is_used();", connstr=common_connstr) == "t"
    ), "ssl_is_used() for TLS connection"

    assert (
        node.safe_psql(
            "SELECT ssl_version();",
            connstr=common_connstr
            + " ssl_min_protocol_version=TLSv1.2 ssl_max_protocol_version=TLSv1.2",
        )
        == "TLSv1.2"
    ), "ssl_version() correctly returning TLS protocol"

    assert (
        node.safe_psql(
            "SELECT ssl_cipher() = cipher FROM pg_stat_ssl WHERE pid = pg_backend_pid();",
            connstr=common_connstr,
        )
        == "t"
    ), "ssl_cipher() compared with pg_stat_ssl"

    assert (
        node.safe_psql("SELECT ssl_client_cert_present();", connstr=common_connstr)
        == "t"
    ), "ssl_client_cert_present() for connection with cert"

    assert (
        node.safe_psql(
            "SELECT ssl_client_serial() = client_serial FROM pg_stat_ssl WHERE pid = pg_backend_pid();",
            connstr=common_connstr,
        )
        == "t"
    ), "ssl_client_serial() compared with pg_stat_ssl"

    # Must not use safe_psql since we expect an error here (exit code 3).
    result = node.psql_capture(
        "SELECT ssl_client_dn_field('invalid');", connstr=common_connstr
    )
    assert result.rc == 3, "ssl_client_dn_field() for an invalid field"

    assert (
        node.safe_psql(
            "SELECT '/CN=' || ssl_client_dn_field('commonName') = client_dn FROM pg_stat_ssl WHERE pid = pg_backend_pid();",
            connstr=common_connstr,
        )
        == "t"
    ), "ssl_client_dn_field() for commonName"

    assert (
        node.safe_psql(
            "SELECT ssl_issuer_dn() = issuer_dn FROM pg_stat_ssl WHERE pid = pg_backend_pid();",
            connstr=common_connstr,
        )
        == "t"
    ), "ssl_issuer_dn() for connection with cert"

    assert (
        node.safe_psql(
            "SELECT '/CN=' || ssl_issuer_field('commonName') = issuer_dn FROM pg_stat_ssl WHERE pid = pg_backend_pid();",
            connstr=common_connstr,
        )
        == "t"
    ), "ssl_issuer_field() for commonName"

    assert (
        node.safe_psql(
            "SELECT value, critical FROM ssl_extension_info() WHERE name = 'basicConstraints';",
            connstr=common_connstr,
        )
        == "CA:FALSE|t"
    ), "extract extension from cert"


def _test_without_cert(node):
    """sslinfo functions for a TLS connection that presents no client cert."""
    trust_connstr = (
        "{default} sslrootcert=ssl/root+server_ca.crt sslmode=require "
        "dbname=trustdb hostaddr={addr} user=ssltestuser host=localhost".format(
            default=DEFAULT_SSL_CONNSTR, addr=SERVERHOSTADDR
        )
    )

    assert (
        node.safe_psql("SELECT ssl_client_cert_present();", connstr=trust_connstr)
        == "f"
    ), "ssl_client_cert_present() for connection without cert"

    assert (
        node.safe_psql(
            "SELECT ssl_client_dn_field('commonName');", connstr=trust_connstr
        )
        == ""
    ), "ssl_client_dn_field() for connection without cert"


def _test_sslcertmode(node, common_connstr, supports_sslcertmode_require):
    """ssl_client_cert_present() across the sslcertmode connection options."""
    cases = [
        ("sslcertmode=allow", "t"),
        ("sslcertmode=allow sslcert=invalid", "f"),
        ("sslcertmode=disable", "f"),
    ]
    if supports_sslcertmode_require:
        cases.append(("sslcertmode=require", "t"))

    for opts, present in cases:
        result = node.safe_psql(
            "SELECT ssl_client_cert_present();",
            connstr="{} dbname=trustdb {}".format(common_connstr, opts),
        )
        assert result == present, "ssl_client_cert_present() for {}".format(opts)

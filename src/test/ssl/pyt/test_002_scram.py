# Copyright (c) 2021-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long
"""Port of src/test/ssl/t/002_scram.pl.

SCRAM authentication and TLS channel binding over SSL: channel_binding
disable/require/invalid, MD5 rejection of channel binding, cert auth without
channel binding, require_auth interplay, and RSA-PSS server certificates.
"""

import os
import shutil

import pytest

import pypg
from pypg.ssl_server import SSLServer, ssl_file_path

# This suite opens up local TCP ports and is hidden behind PG_TEST_EXTRA=ssl.
pytestmark = pypg.require_test_extras("ssl")

SERVERHOSTADDR = "127.0.0.1"
SERVERHOSTCIDR = "127.0.0.1/32"


def test_002_scram(create_pg, tmp_path):
    """SCRAM over SSL: channel binding, MD5, cert auth and RSA-PSS handling."""
    if os.environ.get("with_ssl") != "openssl":
        pytest.skip("OpenSSL not supported by this build")

    ssl_server = SSLServer(tmp_path)
    libressl = ssl_server.is_libressl()
    supports_rsapss_certs = pypg.check_pg_config(
        "#define HAVE_X509_GET_SIGNATURE_INFO 1"
    )
    if libressl:
        # As of 5/2025, LibreSSL doesn't actually work for RSA-PSS certs.
        supports_rsapss_certs = False

    node = create_pg("primary", hostaddr=SERVERHOSTADDR, start=True)
    md5_works = node.psql_capture("select md5('')").rc == 0

    ssl_server.configure_test_server_for_ssl(
        node,
        SERVERHOSTADDR,
        SERVERHOSTCIDR,
        "scram-sha-256",
        password="pass",
        password_enc="scram-sha-256",
    )
    ssl_server.switch_server_cert(node, certfile="server-cn-only")

    old_pgpassword = os.environ.get("PGPASSWORD")
    os.environ["PGPASSWORD"] = "pass"
    try:
        _run_scram_tests(node, ssl_server, tmp_path, md5_works, supports_rsapss_certs)
    finally:
        if old_pgpassword is None:
            os.environ.pop("PGPASSWORD", None)
        else:
            os.environ["PGPASSWORD"] = old_pgpassword


def _run_scram_tests(node, ssl_server, tmp_path, md5_works, supports_rsapss_certs):
    """The body of the SCRAM test, with PGPASSWORD set in the environment."""
    common_connstr = (
        "dbname=trustdb sslmode=require sslcert=invalid sslrootcert=invalid "
        "hostaddr={} host=localhost".format(SERVERHOSTADDR)
    )

    _test_channel_binding(node, common_connstr, md5_works)
    _test_cert_auth(node, tmp_path)
    _test_require_auth(node, common_connstr, md5_works)
    _test_rsapss(node, ssl_server, common_connstr, supports_rsapss_certs)


def _test_channel_binding(node, common_connstr, md5_works):
    """channel_binding=disable/require/invalid and MD5 rejection."""
    node.connect_ok(
        "{} user=ssltestuser".format(common_connstr),
        "Basic SCRAM authentication with SSL",
    )
    node.connect_fails(
        "{} user=ssltestuser channel_binding=invalid_value".format(common_connstr),
        "SCRAM with SSL and channel_binding=invalid_value",
        expected_stderr=r'invalid channel_binding value: "invalid_value"',
    )
    node.connect_ok(
        "{} user=ssltestuser channel_binding=disable".format(common_connstr),
        "SCRAM with SSL and channel_binding=disable",
    )
    node.connect_ok(
        "{} user=ssltestuser channel_binding=require".format(common_connstr),
        "SCRAM with SSL and channel_binding=require",
    )

    if md5_works:
        node.connect_fails(
            "{} user=md5testuser channel_binding=require".format(common_connstr),
            "MD5 with SSL and channel_binding=require",
            expected_stderr=r"channel binding required but not supported by server's authentication request",
        )


def _test_cert_auth(node, tmp_path):
    """cert auth and channel_binding, plus clientcert=verify-full."""
    # A unique client key copy, since ssl/client.key may be used elsewhere.
    client_tmp_key = tmp_path / "client_scram.key"
    shutil.copyfile(ssl_file_path("client.key"), client_tmp_key)
    os.chmod(client_tmp_key, 0o600)

    node.connect_fails(
        "sslcert=ssl/client.crt sslkey={key} sslrootcert=invalid hostaddr={addr} "
        "host=localhost dbname=certdb user=ssltestuser channel_binding=require".format(
            key=client_tmp_key, addr=SERVERHOSTADDR
        ),
        "Cert authentication and channel_binding=require",
        expected_stderr=r"channel binding required, but server authenticated client without channel binding",
    )

    node.connect_ok(
        "sslcert=ssl/client.crt sslkey={key} sslrootcert=invalid hostaddr={addr} "
        "host=localhost dbname=verifydb user=ssltestuser".format(
            key=client_tmp_key, addr=SERVERHOSTADDR
        ),
        "SCRAM with clientcert=verify-full",
        log_like=[
            r'connection authenticated: identity="ssltestuser" method=scram-sha-256'
        ],
    )


def _test_require_auth(node, common_connstr, md5_works):
    """channel_binding works independently of require_auth."""
    node.connect_ok(
        "{} user=ssltestuser channel_binding=disable "
        "require_auth=scram-sha-256".format(common_connstr),
        "SCRAM with SSL, channel_binding=disable, and require_auth=scram-sha-256",
    )

    if md5_works:
        node.connect_fails(
            "{} user=md5testuser require_auth=md5 channel_binding=require".format(
                common_connstr
            ),
            "channel_binding can fail even when require_auth succeeds",
            expected_stderr=r"channel binding required but not supported by server's authentication request",
        )

    node.connect_ok(
        "{} user=ssltestuser channel_binding=require "
        "require_auth=scram-sha-256".format(common_connstr),
        "SCRAM with SSL, channel_binding=require, and require_auth=scram-sha-256",
    )


def _test_rsapss(node, ssl_server, common_connstr, supports_rsapss_certs):
    """A server certificate using the RSA-PSS algorithm (bug #17760)."""
    if not supports_rsapss_certs:
        return
    ssl_server.switch_server_cert(node, certfile="server-rsapss")
    node.connect_ok(
        "{} user=ssltestuser channel_binding=require".format(common_connstr),
        "SCRAM with SSL and channel_binding=require, server certificate uses 'rsassaPss'",
        log_like=[
            r'connection authenticated: identity="ssltestuser" method=scram-sha-256'
        ],
    )

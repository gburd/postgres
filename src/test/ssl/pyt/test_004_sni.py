# Copyright (c) 2024-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long
"""Port of src/test/ssl/t/004_sni.pl.

Server Name Indication (SNI) support: per-host certificate selection via
pg_hosts.conf, the ssl_sni GUC, multiple/@-included hostnames, duplicate and
malformed entry rejection, passphrase-protected keys and reload behavior, the
/no_sni/ marker, and per-host client CA verification including CRL handling.
"""

import os
import platform

import pytest

import pypg
from pypg.ssl_server import SSLServer

# This suite opens up local TCP ports and is hidden behind PG_TEST_EXTRA=ssl.
pytestmark = pypg.require_test_extras("ssl")

# The hostaddr used to connect; the server certificate is for a fixed domain so
# this cannot be a hostname. The CIDR is used to match incoming connections.
SERVERHOSTADDR = "127.0.0.1"
SERVERHOSTCIDR = "127.0.0.1/32"


def _data_dir(node):
    return node.datadir


def test_004_sni(create_pg, tmp_path):
    """SNI server: pg.conf and pg_hosts.conf host/cert selection and errors."""
    if os.environ.get("with_ssl") != "openssl":
        pytest.skip("OpenSSL not supported by this build")

    ssl_server = SSLServer(tmp_path)
    if ssl_server.is_libressl():
        pytest.skip("SNI not supported when building with LibreSSL")

    node = create_pg("primary", hostaddr=SERVERHOSTADDR, start=True)
    exec_backend = node.safe_psql("SHOW debug_exec_backend")

    ssl_server.configure_test_server_for_ssl(
        node, SERVERHOSTADDR, SERVERHOSTCIDR, "trust"
    )
    ssl_server.switch_server_cert(node, certfile="server-cn-only")

    connstr = "user=ssltestuser dbname=trustdb hostaddr={} sslsni=1".format(
        SERVERHOSTADDR
    )

    _test_postgresql_conf(node, connstr)
    _test_pg_hosts_conf(node, connstr)
    _test_passphrase_reload(node, connstr, exec_backend)
    _test_non_sni_only(node, connstr)
    _test_client_cas(node, ssl_server)


def _test_postgresql_conf(node, connstr):
    """The postgresql.conf branch: cert in pg.conf used until SNI flips on."""
    node.connect_ok(
        "{} sslrootcert=ssl/root+server_ca.crt sslmode=require".format(connstr),
        "pg.conf: connect with correct server CA cert file sslmode=require",
    )
    node.connect_fails(
        "{} sslrootcert=ssl/root_ca.crt sslmode=verify-ca".format(connstr),
        "pg.conf: connect fails without intermediate for sslmode=verify-ca",
        expected_stderr=r"certificate verify failed",
    )

    node.append_conf(
        "example.org server-cn-only.crt server-cn-only.key", "pg_hosts.conf"
    )
    node.reload()
    node.connect_ok(
        "{} sslrootcert=ssl/root+server_ca.crt sslmode=require".format(connstr),
        "pg.conf: connect with correct server CA cert file sslmode=require",
    )

    node.append_conf("ssl_sni = on", "postgresql.conf")
    os.unlink(_data_dir(node) / "pg_hosts.conf")
    node.reload()
    node.connect_ok(
        "{} sslrootcert=ssl/root+server_ca.crt sslmode=require".format(connstr),
        "pg.conf: connect after deleting pg_hosts.conf",
    )


def _test_pg_hosts_conf(node, connstr):
    """The pg_hosts.conf branch: default host, per-host CA, name lists, errors."""
    node.append_conf("* server-cn-only.crt server-cn-only.key", "pg_hosts.conf")
    node.reload()
    node.connect_ok(
        "{} sslrootcert=ssl/root+server_ca.crt sslmode=require".format(connstr),
        "pg_hosts.conf: connect to default, with correct server CA cert file sslmode=require",
    )
    node.connect_fails(
        "{} sslrootcert=ssl/root_ca.crt sslmode=verify-ca".format(connstr),
        "pg_hosts.conf: connect to default, fail without intermediate for sslmode=verify-ca",
        expected_stderr=r"certificate verify failed",
    )

    node.append_conf(
        "example.org server-cn-only+server_ca.crt server-cn-only.key root_ca.crt",
        "pg_hosts.conf",
    )
    node.reload()
    node.connect_ok(
        "{} host=example.org sslrootcert=ssl/root_ca.crt sslmode=verify-ca".format(
            connstr
        ),
        "pg_hosts.conf: connect to example.org and verify server CA",
    )
    node.connect_ok(
        "{} host=Example.ORG sslrootcert=ssl/root_ca.crt sslmode=verify-ca".format(
            connstr
        ),
        "pg_hosts.conf: connect to Example.ORG and verify server CA",
    )
    node.connect_fails(
        "{} host=example.org sslrootcert=invalid sslmode=verify-ca".format(connstr),
        "pg_hosts.conf: connect to example.org but without server root cert, sslmode=verify-ca",
        expected_stderr=r'root certificate file "invalid" does not exist',
    )
    node.connect_fails(
        "{} sslrootcert=ssl/root_ca.crt sslmode=verify-ca".format(connstr),
        "pg_hosts.conf: connect to default and fail to verify CA",
        expected_stderr=r"certificate verify failed",
    )
    node.connect_ok(
        "{} sslrootcert=ssl/root+server_ca.crt sslmode=require".format(connstr),
        "pg_hosts.conf: connect to default with sslmode=require",
    )

    _test_hostname_lists(node, connstr)
    _test_invalid_pg_hosts(node)


def _test_hostname_lists(node, connstr):
    """Multiple hostnames per entry, including @-file inclusion."""
    os.unlink(_data_dir(node) / "pg_hosts.conf")
    node.append_conf(
        "example.org,example.com,example.net server-cn-only+server_ca.crt "
        "server-cn-only.key root_ca.crt",
        "pg_hosts.conf",
    )
    node.reload()
    for host in ("example.org", "example.com", "example.net"):
        node.connect_ok(
            "{} host={} sslrootcert=ssl/root_ca.crt sslmode=verify-ca".format(
                connstr, host
            ),
            "pg_hosts.conf: connect to {} and verify server CA".format(host),
        )
    node.connect_fails(
        "{} sslrootcert=ssl/root+server_ca.crt sslmode=require host=example.se".format(
            connstr
        ),
        "pg_hosts.conf: connect to default with sslmode=require",
        expected_stderr=r"unrecognized name",
    )

    os.unlink(_data_dir(node) / "pg_hosts.conf")
    node.append_conf(
        "example.org,@hostnames.txt server-cn-only+server_ca.crt "
        "server-cn-only.key root_ca.crt",
        "pg_hosts.conf",
    )
    node.append_conf("\nexample.com\nexample.net\n", "hostnames.txt")
    node.reload()
    for host in ("example.org", "example.com", "example.net"):
        node.connect_ok(
            "{} host={} sslrootcert=ssl/root_ca.crt sslmode=verify-ca".format(
                connstr, host
            ),
            "@hostnames.txt: connect to {} and verify server CA".format(host),
        )
    node.connect_fails(
        "{} sslrootcert=ssl/root+server_ca.crt sslmode=require host=example.se".format(
            connstr
        ),
        "@hostnames.txt: connect to default with sslmode=require",
        expected_stderr=r"unrecognized name",
    )


def _test_invalid_pg_hosts(node):
    """Malformed/duplicate pg_hosts.conf entries make the server fail to start."""
    cases = [
        (
            "example.org,*,example.net server-cn-only+server_ca.crt "
            "server-cn-only.key root_ca.crt",
            "pg_hosts.conf: restart fails with default entry combined with hostnames",
        ),
        (
            "\n* server-cn-only.crt server-cn-only.key"
            "\n* server-cn-only.crt server-cn-only.key\n",
            "pg_hosts.conf: restart fails with two default entries",
        ),
        (
            "\n/no_sni/ server-cn-only.crt server-cn-only.key"
            "\n/no_sni/ server-cn-only.crt server-cn-only.key\n",
            "pg_hosts.conf: restart fails with two no_sni entries",
        ),
        (
            "\nexample.org server-cn-only.crt server-cn-only.key"
            "\nexample.net server-cn-only.crt server-cn-only.key"
            "\nexample.org server-cn-only.crt server-cn-only.key\n",
            "pg_hosts.conf: restart fails with two identical hostname entries",
        ),
        (
            "\nexample.org server-cn-only.crt server-cn-only.key"
            "\nexample.net,example.com,Example.org server-cn-only.crt "
            "server-cn-only.key\n",
            "pg_hosts.conf: restart fails with two identical hostname entries in lists",
        ),
    ]
    for conf, msg in cases:
        os.unlink(_data_dir(node) / "pg_hosts.conf")
        node.append_conf(conf, "pg_hosts.conf")
        assert node.restart(fail_ok=True) is False, msg


def _test_passphrase_reload(node, connstr, exec_backend):
    """No-default host plus passphrase-protected key reload semantics."""
    os.unlink(_data_dir(node) / "pg_hosts.conf")
    node.append_conf(
        "example.org server-cn-only+server_ca.crt server-cn-only.key root_ca.crt",
        "pg_hosts.conf",
    )
    node.restart()

    node.connect_fails(
        "{} sslrootcert=ssl/root+server_ca.crt sslmode=require sslsni=0".format(
            connstr
        ),
        "pg_hosts.conf: connect to default with sslmode=require",
        expected_stderr=r"handshake failure",
    )
    node.connect_fails(
        "{} sslrootcert=ssl/root+server_ca.crt sslmode=require host=example.com".format(
            connstr
        ),
        "pg_hosts.conf: connect to default with sslmode=require",
        expected_stderr=r"unrecognized name",
    )
    node.connect_fails(
        "{} sslrootcert=ssl/root+server_ca.crt sslmode=require host=example".format(
            connstr
        ),
        "pg_hosts.conf: connect to 'example' with sslmode=require",
        expected_stderr=r"unrecognized name",
    )

    # Wrong passphrase command: server must not start.
    os.unlink(_data_dir(node) / "pg_hosts.conf")
    node.append_conf(
        "localhost server-cn-only.crt server-password.key root+client_ca.crt "
        '"echo wrongpassword" on',
        "pg_hosts.conf",
    )
    assert node.restart(fail_ok=True) is False, (
        "pg_hosts.conf: restart fails with password-protected key when using "
        "the wrong passphrase command"
    )

    # Correct passphrase command: server must start.
    os.unlink(_data_dir(node) / "pg_hosts.conf")
    node.append_conf(
        "localhost server-cn-only.crt server-password.key root+client_ca.crt "
        '"echo secret1" on',
        "pg_hosts.conf",
    )
    assert node.restart(fail_ok=True) is True, (
        "pg_hosts.conf: restart succeeds with password-protected key when using "
        "the correct passphrase command"
    )

    localhost_connstr = (
        "{} sslrootcert=ssl/root+server_ca.crt sslmode=require host=localhost".format(
            connstr
        )
    )
    node.connect_ok(
        localhost_connstr,
        "pg_hosts.conf: connect with correct server CA cert file sslmode=require",
    )
    node.reload()
    node.reload()
    node.connect_ok(
        localhost_connstr,
        "pg_hosts.conf: connect with correct server CA cert file after reloads",
    )
    node.reload()
    node.reload()
    node.connect_ok(
        localhost_connstr,
        "pg_hosts.conf: connect with correct server CA cert file after more reloads",
    )

    _test_passphrase_no_reload(node, localhost_connstr, exec_backend)


def _test_passphrase_no_reload(node, localhost_connstr, exec_backend):
    """Passphrase key without reload support: restart clean, reload warns."""
    os.unlink(_data_dir(node) / "pg_hosts.conf")
    node.append_conf(
        "localhost server-cn-only.crt server-password.key root+client_ca.crt "
        '"echo secret1" off',
        "pg_hosts.conf",
    )
    node_loglocation = node.current_log_position()
    assert node.restart(fail_ok=True) is True, (
        "pg_hosts.conf: restart succeeds with password-protected key when using "
        "the correct passphrase command"
    )
    log = pypg.slurp_file(node.log, node_loglocation)
    assert (
        "cannot be reloaded because it requires a passphrase" not in log
    ), "log reload failure due to passphrase command reloading"

    windows_os = platform.system() == "Windows"
    if windows_os or "on" in exec_backend:
        pytest.skip("Passphrase command reload required on Windows and EXEC_BACKEND")

    node.connect_ok(
        localhost_connstr,
        "pg_hosts.conf: connect with correct server CA cert file sslmode=require",
    )
    node_loglocation = node.current_log_position()
    node.reload()
    node.connect_ok(
        localhost_connstr,
        "pg_hosts.conf: connect with correct server CA cert file sslmode=require",
    )
    log = node.wait_for_log(
        r"cannot be reloaded because it requires a passphrase", node_loglocation
    )
    assert log, "log reload failure due to passphrase command reloading"


def _test_non_sni_only(node, connstr):
    """The /no_sni/ marker: only non-SNI connections are accepted."""
    os.unlink(_data_dir(node) / "pg_hosts.conf")
    node.append_conf("/no_sni/ server-cn-only.crt server-cn-only.key", "pg_hosts.conf")
    node.restart()

    node.connect_ok(
        "{} sslrootcert=ssl/root+server_ca.crt sslmode=require sslsni=0".format(
            connstr
        ),
        "pg_hosts.conf: only non-SNI connections allowed",
    )
    node.connect_fails(
        "{} sslrootcert=ssl/root+server_ca.crt sslmode=require host=example.org".format(
            connstr
        ),
        "pg_hosts.conf: only non-SNI connections allowed, connecting with SNI",
        expected_stderr=r"unrecognized name",
    )


def _test_client_cas(node, ssl_server):
    """Per-host client CA configuration, with global CRL dir interaction."""
    os.unlink(_data_dir(node) / "pg_hosts.conf")
    node.append_conf("ssl_ca_file = 'root+client_ca.crt'", "postgresql.conf")
    node.append_conf(
        "* server-cn-only.crt server-cn-only.key root+client_ca.crt", "pg_hosts.conf"
    )
    node.append_conf(
        "example.org server-cn-only.crt server-cn-only.key", "pg_hosts.conf"
    )
    node.append_conf(
        "example.com server-cn-only.crt server-cn-only.key root+client_ca.crt",
        "pg_hosts.conf",
    )
    node.append_conf(
        "example.net server-cn-only.crt server-cn-only.key root+server_ca.crt",
        "pg_hosts.conf",
    )
    node.restart()

    connstr = (
        "user=ssltestuser dbname=certdb hostaddr={} sslmode=require sslsni=1".format(
            SERVERHOSTADDR
        )
    )

    node.connect_fails(
        "{} host=example.org sslcertmode=require sslcert=ssl/client.crt{}".format(
            connstr, ssl_server.sslkey("client.key")
        ),
        "host: 'example.org', ca: '': connect with sslcert, no client CA configured",
        expected_stderr=r"client certificates can only be checked if a root certificate store is available",
    )
    node.connect_fails(
        "{} host=example.com sslcertmode=disable".format(connstr),
        "host: 'example.com', ca: 'root+client_ca.crt': connect fails if no client certificate sent",
        expected_stderr=r"connection requires a valid client certificate",
    )
    node.connect_ok(
        "{} host=example.com sslcertmode=require sslcert=ssl/client.crt {}".format(
            connstr, ssl_server.sslkey("client.key")
        ),
        "host: 'example.com', ca: 'root+client_ca.crt': connect with sslcert, client certificate sent",
    )
    node.connect_fails(
        "{} host=example.net sslcertmode=disable".format(connstr),
        "host: 'example.net', ca: 'root+server_ca.crt': connect fails if no client certificate sent",
        expected_stderr=r"connection requires a valid client certificate",
    )
    node.connect_fails(
        "{} host=example.net sslcertmode=require sslcert=ssl/client.crt {}".format(
            connstr, ssl_server.sslkey("client.key")
        ),
        "host: 'example.net', ca: 'root+server_ca.crt': connect with sslcert, client certificate sent",
        expected_stderr=r"unknown ca",
    )

    # Global CRL dir interacts with per-host trust.
    ssl_server.switch_server_cert(
        node, certfile="server-cn-only", crldir="client-crldir"
    )
    node.connect_fails(
        "{} host=example.com sslcertmode=require sslcert=ssl/client-revoked.crt {}".format(
            connstr, ssl_server.sslkey("client-revoked.key")
        ),
        "host: 'example.com', ca: 'root+client_ca.crt': connect fails with revoked client cert",
        expected_stderr=r"certificate revoked",
    )

    _test_client_cas_eol(node)


def _test_client_cas_eol(node):
    """Trailing/garbage tokens in pg_hosts.conf entries fail server start."""
    os.unlink(_data_dir(node) / "pg_hosts.conf")
    node.append_conf(
        "example.org server-cn-only.crt server-cn-only.key root+client_ca.crt "
        '"cmd" on TRAILING_TEXT MORE_TEXT',
        "pg_hosts.conf",
    )
    assert (
        node.restart(fail_ok=True) is False
    ), "pg_hosts.conf: restart fails with extra data at EOL"

    os.unlink(_data_dir(node) / "pg_hosts.conf")
    node.append_conf(
        "example.org server-cn-only.crt server-cn-only.key root+client_ca.crt "
        '"cmd" notabooleanvalue',
        "pg_hosts.conf",
    )
    assert (
        node.restart(fail_ok=True) is False
    ), "pg_hosts.conf: restart fails with non-boolean value in boolean field"

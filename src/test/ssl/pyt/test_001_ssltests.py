# Copyright (c) 2021-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long
"""Port of src/test/ssl/t/001_ssltests.pl.

The core SSL regression suite: password-protected server keys and reload
behavior, SSL protocol bounds, ssl_groups parsing, client-side sslmode/root
cert/CRL/sslcertmode handling, host name verification against CN and Subject
Alternative Names (including IP and IPv6 SANs), system trusted roots, and
server-side client-certificate authorization (cert/DN/CN mapping, revoked
certs, intermediate CAs and server-side CRL directories).
"""

import os
import platform

import pytest

import pypg
from pypg.ssl_server import SSLServer, stat_is_world_readable

# This suite opens up local TCP ports and is hidden behind PG_TEST_EXTRA=ssl.
pytestmark = pypg.require_test_extras("ssl")

SERVERHOSTADDR = "127.0.0.1"
SERVERHOSTCIDR = "127.0.0.1/32"

DEFAULT_SSL_CONNSTR = (
    "sslkey=invalid sslcert=invalid sslrootcert=invalid "
    "sslcrl=invalid sslcrldir=invalid"
)


def test_001_ssltests(create_pg, tmp_path):
    """Run the full SSL regression suite against an OpenSSL-enabled server."""
    if os.environ.get("with_ssl") != "openssl":
        pytest.skip("OpenSSL not supported by this build")

    ssl_server = SSLServer(tmp_path)
    libressl = ssl_server.is_libressl()
    supports_sslcertmode_require = pypg.check_pg_config(
        "#define HAVE_SSL_CTX_SET_CERT_CB 1"
    )
    has_inet_pton = pypg.check_pg_config("#define HAVE_INET_PTON 1")

    node = create_pg("primary", hostaddr=SERVERHOSTADDR, start=False)
    # Needed to allow connect_fails to inspect postmaster log.
    node.append_conf("log_min_messages = debug2")
    node.start()

    assert (
        node.safe_psql("SHOW ssl_library") == ssl_server.ssl_library()
    ), "ssl_library parameter"
    exec_backend = node.safe_psql("SHOW debug_exec_backend")

    ssl_server.configure_test_server_for_ssl(
        node, SERVERHOSTADDR, SERVERHOSTCIDR, "trust"
    )

    _test_password_keys(node, ssl_server, exec_backend)
    _test_protocol_and_groups(node, ssl_server)

    common_connstr = (
        "{} user=ssltestuser dbname=trustdb hostaddr={} "
        "host=common-name.pg-ssltest.test".format(DEFAULT_SSL_CONNSTR, SERVERHOSTADDR)
    )
    ssl_server.switch_server_cert(node, certfile="server-cn-only")

    _test_keylogging(node, common_connstr, libressl)
    _test_root_certs(node, common_connstr, supports_sslcertmode_require)
    _test_crls(node, common_connstr)
    _test_hostname_verification(node, ssl_server, has_inet_pton)
    _test_cn_and_san(node, ssl_server, has_inet_pton)
    _test_system_roots(node, ssl_server, libressl)
    _test_server_crl(node, ssl_server)
    _test_protocol_versions(node)
    _test_cert_authorization(node, ssl_server, supports_sslcertmode_require)


def _test_password_keys(node, ssl_server, exec_backend):
    """Password-protected server keys and passphrase reload behavior."""
    # Wrong passphrase: server must not start at all.
    ssl_server.switch_server_cert(
        node,
        certfile="server-cn-only",
        cafile="root+client_ca",
        keyfile="server-password",
        passphrase_cmd="echo wrongpassword",
        restart="no",
    )
    assert (
        node.restart(fail_ok=True, log_like=[r"could not load private key file"])
        is False
    ), "restart fails with password-protected key file with wrong password"

    # Correct passphrase but no reload support.
    ssl_server.switch_server_cert(
        node,
        certfile="server-cn-only",
        cafile="root+client_ca",
        keyfile="server-password",
        passphrase_cmd="echo secret1",
        passphrase_cmd_reload="off",
        restart="no",
    )
    assert (
        node.restart(fail_ok=True, log_unlike=[r"could not load private key file"])
        is True
    ), "restart succeeds with password-protected key file"

    common_connstr = (
        "{} user=ssltestuser dbname=trustdb hostaddr={} "
        "host=common-name.pg-ssltest.test".format(DEFAULT_SSL_CONNSTR, SERVERHOSTADDR)
    )
    require = "{} sslrootcert=ssl/root+server_ca.crt sslmode=require".format(
        common_connstr
    )
    if "on" in exec_backend:
        node.connect_fails(
            require,
            "connect with correct server CA cert file sslmode=require",
            expected_stderr=r"server does not support SSL",
        )
    else:
        node.connect_ok(
            require, "connect with correct server CA cert file sslmode=require"
        )

    # Reloading should fail since we cannot execute the passphrase command.
    node.reload()
    log_start = node.wait_for_log(
        r"cannot be reloaded because it requires a passphrase"
    )

    # Correct passphrase that can be reloaded.
    ssl_server.switch_server_cert(
        node,
        certfile="server-cn-only",
        cafile="root+client_ca",
        keyfile="server-password",
        passphrase_cmd="echo secret1",
        passphrase_cmd_reload="on",
        restart="no",
    )
    assert (
        node.restart(fail_ok=True, log_unlike=[r"could not load private key file"])
        is True
    ), "restart succeeds with password-protected key file"
    node.connect_ok(require, "connect with correct server CA cert file sslmode=require")

    # Reloading should execute the reload command and reload the key.
    node.reload()
    log_start = node.wait_for_log(r"reloading configuration files", log_start)
    log = pypg.slurp_file(node.log, log_start)
    assert (
        "cannot be reloaded because it requires a passphrase" not in log
    ), "passphrase could reload private key"
    node.connect_ok(require, "connect with correct server CA cert file sslmode=require")


def _test_protocol_and_groups(node, ssl_server):
    """SSL protocol bounds and ssl_groups parsing failures."""
    node.append_conf(
        "ssl_min_protocol_version='TLSv1.2'\nssl_max_protocol_version='TLSv1.1'"
    )
    with pytest.raises(pypg.PgServerError):
        # restart fails with incorrect SSL protocol bounds
        node.restart()

    node.append_conf("ssl_min_protocol_version='TLSv1.2'\nssl_max_protocol_version=''")
    # restart succeeds with correct SSL protocol bounds
    node.restart()

    # Colon-separated groups: a bad value fails to start. The value is reset
    # later by switch_server_cert (which rewrites sslconfig.conf from scratch).
    node.append_conf("ssl_groups='bad:value'", "sslconfig.conf")
    with pytest.raises(pypg.PgServerError):
        # restart fails with incorrect groups
        node.restart()
    assert not node.log_matches(r"no SSL error reported"), "error message translated"
    node.append_conf("ssl_groups='prime256v1'", "ssl_config.conf")
    node.restart(fail_ok=True)


def _test_keylogging(node, common_connstr, libressl):
    """sslkeylogfile creates a non-world-readable file, errors are non-fatal."""
    if libressl:
        pytest.skip("Keylogging is not supported with LibreSSL")

    tempdir = node.basedir
    keylog = tempdir / "key.txt"
    node.connect_ok(
        "{} sslrootcert=ssl/root+server_ca.crt sslkeylogfile={} sslmode=require".format(
            common_connstr, keylog
        ),
        "connect with server root cert and sslkeylogfile={}".format(keylog),
    )
    assert keylog.is_file(), "keylog file exists at: {}".format(keylog)

    if platform.system() == "Windows":
        pytest.skip("Permissions check not enforced on Windows")

    assert not stat_is_world_readable(keylog), "keylog file is not world readable"

    node.connect_ok(
        "{} sslrootcert=ssl/root+server_ca.crt sslkeylogfile={}/invalid/key.txt "
        "sslmode=require".format(common_connstr, tempdir),
        "connect with server root cert and incorrect sslkeylogfile path",
        expected_stderr=r"could not open",
    )


def _test_root_certs(node, common_connstr, supports_sslcertmode_require):
    """sslmode/root-cert combinations and sslcertmode without a client cert."""
    node.connect_fails(
        "{} sslmode=disable".format(common_connstr),
        "server doesn't accept non-SSL connections",
        expected_stderr=r"no pg_hba.conf entry",
    )

    node.connect_ok(
        "{} sslrootcert=invalid sslmode=require".format(common_connstr),
        "connect without server root cert sslmode=require",
    )
    for mode in ("verify-ca", "verify-full"):
        node.connect_fails(
            "{} sslrootcert=invalid sslmode={}".format(common_connstr, mode),
            "connect without server root cert sslmode={}".format(mode),
            expected_stderr=r'root certificate file "invalid" does not exist',
        )

    for mode in ("require", "verify-ca", "verify-full"):
        node.connect_fails(
            "{} sslrootcert=ssl/client_ca.crt sslmode={}".format(common_connstr, mode),
            "connect with wrong server root cert sslmode={}".format(mode),
            expected_stderr=r"SSL error: certificate verify failed",
        )

    node.connect_fails(
        "{} sslrootcert=ssl/server_ca.crt sslmode=verify-ca".format(common_connstr),
        "connect with server CA cert, without root CA",
        expected_stderr=r"SSL error: certificate verify failed",
    )

    for mode in ("require", "verify-ca", "verify-full"):
        node.connect_ok(
            "{} sslrootcert=ssl/root+server_ca.crt sslmode={}".format(
                common_connstr, mode
            ),
            "connect with correct server CA cert file sslmode={}".format(mode),
        )

    node.connect_ok(
        "{} sslrootcert=ssl/both-cas-1.crt sslmode=verify-ca".format(common_connstr),
        "cert root file that contains two certificates, order 1",
    )
    node.connect_ok(
        "{} sslrootcert=ssl/both-cas-2.crt sslmode=verify-ca".format(common_connstr),
        "cert root file that contains two certificates, order 2",
    )

    node.connect_ok(
        "{} sslrootcert=ssl/root+server_ca.crt sslmode=require "
        "sslcertmode=disable".format(common_connstr),
        "connect with sslcertmode=disable",
    )
    node.connect_ok(
        "{} sslrootcert=ssl/root+server_ca.crt sslmode=require "
        "sslcertmode=allow".format(common_connstr),
        "connect with sslcertmode=allow",
    )
    node.connect_fails(
        "{} sslrootcert=ssl/root+server_ca.crt sslmode=require "
        "sslcertmode=require".format(common_connstr),
        "connect with sslcertmode=require fails without a client certificate",
        expected_stderr=(
            r"server accepted connection without a valid SSL certificate"
            if supports_sslcertmode_require
            else r'sslcertmode value "require" is not supported'
        ),
    )


def _test_crls(node, common_connstr):
    """Client-side sslcrl / sslcrldir handling."""
    node.connect_ok(
        "{} sslrootcert=ssl/root+server_ca.crt sslmode=verify-ca sslcrl=invalid".format(
            common_connstr
        ),
        "sslcrl option with invalid file name",
    )
    node.connect_fails(
        "{} sslrootcert=ssl/root+server_ca.crt sslmode=verify-ca "
        "sslcrl=ssl/client.crl".format(common_connstr),
        "CRL belonging to a different CA",
        expected_stderr=r"SSL error: certificate verify failed",
    )
    node.connect_fails(
        "{} sslcrl='' sslrootcert=ssl/root+server_ca.crt sslmode=verify-ca "
        "sslcrldir=ssl/client-crldir".format(common_connstr),
        "directory CRL belonging to a different CA",
        expected_stderr=r"SSL error: certificate verify failed",
    )
    node.connect_ok(
        "{} sslrootcert=ssl/root+server_ca.crt sslmode=verify-ca "
        "sslcrl=ssl/root+server.crl".format(common_connstr),
        "CRL with a non-revoked cert",
    )
    node.connect_ok(
        "{} sslrootcert=ssl/root+server_ca.crt sslmode=verify-ca "
        "sslcrldir=ssl/root+server-crldir".format(common_connstr),
        "directory CRL with a non-revoked cert",
    )


def _verify_full_base():
    """Common connstr for the host name verification tests (verify-full)."""
    return (
        "{} user=ssltestuser dbname=trustdb sslrootcert=ssl/root+server_ca.crt "
        "hostaddr={}".format(DEFAULT_SSL_CONNSTR, SERVERHOSTADDR)
    )


def _test_hostname_verification(node, ssl_server, has_inet_pton):
    """Host name vs. server certificate matching for CN, IP CN and SANs."""
    base = _verify_full_base()
    node.connect_ok(
        "{} sslmode=require host=wronghost.test".format(base),
        "mismatch between host name and server certificate sslmode=require",
    )
    node.connect_ok(
        "{} sslmode=verify-ca host=wronghost.test".format(base),
        "mismatch between host name and server certificate sslmode=verify-ca",
    )
    node.connect_fails(
        "{} sslmode=verify-full host=wronghost.test".format(base),
        "mismatch between host name and server certificate sslmode=verify-full",
        expected_stderr=r'server certificate for "common-name.pg-ssltest.test" does not match host name "wronghost.test"',
    )

    ssl_server.switch_server_cert(node, certfile="server-ip-cn-only")
    vf = "{} sslmode=verify-full".format(base)
    node.connect_ok(
        "{} host=192.0.2.1 sslsni=0".format(vf), "IP address in the Common Name"
    )
    node.connect_fails(
        "{} host=192.000.002.001 sslsni=0".format(vf),
        "mismatch between host name and server certificate IP address",
        expected_stderr=r'server certificate for "192.0.2.1" does not match host name "192.000.002.001"',
    )

    ssl_server.switch_server_cert(node, certfile="server-ip-in-dnsname")
    node.connect_ok("{} host=192.0.2.1 sslsni=0".format(vf), "IP address in a dNSName")

    _test_san(node, ssl_server, vf, has_inet_pton)


def _test_san(node, ssl_server, vf, has_inet_pton):
    """X.509 Subject Alternative Name matching (DNS, wildcard, IP, IPv6)."""
    ssl_server.switch_server_cert(node, certfile="server-multiple-alt-names")
    node.connect_ok(
        "{} host=dns1.alt-name.pg-ssltest.test".format(vf),
        "host name matching with X.509 Subject Alternative Names 1",
    )
    node.connect_ok(
        "{} host=dns2.alt-name.pg-ssltest.test".format(vf),
        "host name matching with X.509 Subject Alternative Names 2",
    )
    node.connect_ok(
        "{} host=foo.wildcard.pg-ssltest.test".format(vf),
        "host name matching with X.509 Subject Alternative Names wildcard",
    )
    node.connect_fails(
        "{} host=wronghost.alt-name.pg-ssltest.test".format(vf),
        "host name not matching with X.509 Subject Alternative Names",
        expected_stderr=r'server certificate for "dns1.alt-name.pg-ssltest.test" \(and 2 other names\) does not match host name "wronghost.alt-name.pg-ssltest.test"',
    )
    node.connect_fails(
        "{} host=deep.subdomain.wildcard.pg-ssltest.test".format(vf),
        "host name not matching with X.509 Subject Alternative Names wildcard",
        expected_stderr=r'server certificate for "dns1.alt-name.pg-ssltest.test" \(and 2 other names\) does not match host name "deep.subdomain.wildcard.pg-ssltest.test"',
    )

    ssl_server.switch_server_cert(node, certfile="server-single-alt-name")
    node.connect_ok(
        "{} host=single.alt-name.pg-ssltest.test".format(vf),
        "host name matching with a single X.509 Subject Alternative Name",
    )
    node.connect_fails(
        "{} host=wronghost.alt-name.pg-ssltest.test".format(vf),
        "host name not matching with a single X.509 Subject Alternative Name",
        expected_stderr=r'server certificate for "single.alt-name.pg-ssltest.test" does not match host name "wronghost.alt-name.pg-ssltest.test"',
    )
    node.connect_fails(
        "{} host=deep.subdomain.wildcard.pg-ssltest.test".format(vf),
        "host name not matching with a single X.509 Subject Alternative Name wildcard",
        expected_stderr=r'server certificate for "single.alt-name.pg-ssltest.test" does not match host name "deep.subdomain.wildcard.pg-ssltest.test"',
    )

    if not has_inet_pton:
        return
    _test_ip_san(node, ssl_server, vf)


def _test_ip_san(node, ssl_server, vf):
    """IP and IPv6 addresses in Subject Alternative Names."""
    ssl_server.switch_server_cert(node, certfile="server-ip-alt-names")
    node.connect_ok(
        "{} host=192.0.2.1".format(vf),
        "host matching an IPv4 address (Subject Alternative Name 1)",
    )
    node.connect_ok(
        "{} host=192.000.002.001".format(vf),
        "host matching an IPv4 address in alternate form (Subject Alternative Name 1)",
    )
    node.connect_fails(
        "{} host=192.0.2.2".format(vf),
        "host not matching an IPv4 address (Subject Alternative Name 1)",
        expected_stderr=r'server certificate for "192.0.2.1" \(and 1 other name\) does not match host name "192.0.2.2"',
    )
    node.connect_ok(
        "{} host=2001:DB8::1".format(vf),
        "host matching an IPv6 address (Subject Alternative Name 2)",
    )
    node.connect_ok(
        "{} host=2001:db8:0:0:0:0:0:1".format(vf),
        "host matching an IPv6 address in alternate form (Subject Alternative Name 2)",
    )
    node.connect_ok(
        "{} host=2001:db8::0.0.0.1".format(vf),
        "host matching an IPv6 address in mixed form (Subject Alternative Name 2)",
    )
    node.connect_fails(
        "{} host=::1".format(vf),
        "host not matching an IPv6 address (Subject Alternative Name 2)",
        expected_stderr=r'server certificate for "192.0.2.1" \(and 1 other name\) does not match host name "::1"',
    )
    node.connect_fails(
        "{} host=2001:DB8::1/128".format(vf),
        "IPv6 host with CIDR mask does not match",
        expected_stderr=r'server certificate for "192.0.2.1" \(and 1 other name\) does not match host name "2001:DB8::1/128"',
    )


def _test_cn_and_san(node, ssl_server, has_inet_pton):
    """CN-vs-SAN precedence rules (RFCs 2818/6125) and no-name certificates."""
    ssl_server.switch_server_cert(node, certfile="server-cn-and-alt-names")
    vf = "{} sslmode=verify-full".format(_verify_full_base())
    node.connect_ok(
        "{} host=dns1.alt-name.pg-ssltest.test".format(vf),
        "certificate with both a CN and SANs 1",
    )
    node.connect_ok(
        "{} host=dns2.alt-name.pg-ssltest.test".format(vf),
        "certificate with both a CN and SANs 2",
    )
    node.connect_fails(
        "{} host=common-name.pg-ssltest.test".format(vf),
        "certificate with both a CN and SANs ignores CN",
        expected_stderr=r'server certificate for "dns1.alt-name.pg-ssltest.test" \(and 1 other name\) does not match host name "common-name.pg-ssltest.test"',
    )

    if has_inet_pton:
        _test_cn_and_ip_san(node, ssl_server, vf)

    ssl_server.switch_server_cert(node, certfile="server-ip-cn-and-dns-alt-names")
    node.connect_ok(
        "{} host=192.0.2.1".format(vf),
        "certificate with both an IP CN and DNS SANs matches CN",
    )
    node.connect_ok(
        "{} host=dns1.alt-name.pg-ssltest.test".format(vf),
        "certificate with both an IP CN and DNS SANs matches SAN 1",
    )
    node.connect_ok(
        "{} host=dns2.alt-name.pg-ssltest.test".format(vf),
        "certificate with both an IP CN and DNS SANs matches SAN 2",
    )

    _test_no_names(node, ssl_server)


def _test_cn_and_ip_san(node, ssl_server, vf):
    """Fall back to the CN only when the SANs are all IP addresses."""
    ssl_server.switch_server_cert(node, certfile="server-cn-and-ip-alt-names")
    node.connect_ok(
        "{} host=common-name.pg-ssltest.test".format(vf),
        "certificate with both a CN and IP SANs matches CN",
    )
    node.connect_ok(
        "{} host=192.0.2.1".format(vf),
        "certificate with both a CN and IP SANs matches SAN 1",
    )
    node.connect_ok(
        "{} host=2001:db8::1".format(vf),
        "certificate with both a CN and IP SANs matches SAN 2",
    )

    ssl_server.switch_server_cert(node, certfile="server-ip-cn-and-alt-names")
    node.connect_ok(
        "{} host=192.0.2.2".format(vf),
        "certificate with both an IP CN and IP SANs 1",
    )
    node.connect_ok(
        "{} host=2001:db8::1".format(vf),
        "certificate with both an IP CN and IP SANs 2",
    )
    node.connect_fails(
        "{} host=192.0.2.1".format(vf),
        "certificate with both an IP CN and IP SANs ignores CN",
        expected_stderr=r'server certificate for "192.0.2.2" \(and 1 other name\) does not match host name "192.0.2.1"',
    )


def _test_no_names(node, ssl_server):
    """A server certificate with no CN and no SANs is handled gracefully."""
    ssl_server.switch_server_cert(node, certfile="server-no-names")
    base = (
        "{} user=ssltestuser dbname=trustdb sslrootcert=ssl/root+server_ca.crt "
        "hostaddr={}".format(DEFAULT_SSL_CONNSTR, SERVERHOSTADDR)
    )
    node.connect_ok(
        "{} sslmode=verify-ca host=common-name.pg-ssltest.test".format(base),
        "server certificate without CN or SANs sslmode=verify-ca",
    )
    node.connect_fails(
        "{} sslmode=verify-full host=common-name.pg-ssltest.test".format(base),
        "server certificate without CN or SANs sslmode=verify-full",
        expected_stderr=r"could not get server's host name from server certificate",
    )


def _test_system_roots(node, ssl_server, libressl):
    """sslrootcert=system and the SSL_CERT_FILE override."""
    ssl_server.switch_server_cert(
        node,
        certfile="server-cn-only+server_ca",
        keyfile="server-cn-only",
        cafile="root_ca",
    )
    base = "{} user=ssltestuser dbname=trustdb sslrootcert=system hostaddr={}".format(
        DEFAULT_SSL_CONNSTR, SERVERHOSTADDR
    )

    # By default our custom-CA-signed certificate should not be trusted.
    node.connect_fails(
        "{} sslmode=verify-full host=common-name.pg-ssltest.test".format(base),
        "sslrootcert=system does not connect with private CA",
        expected_stderr=r"SSL error: (certificate verify failed|unregistered scheme)",
    )
    node.connect_fails(
        "{} sslmode=verify-ca host=common-name.pg-ssltest.test".format(base),
        "sslrootcert=system only accepts sslmode=verify-full",
        expected_stderr=r'weak sslmode "verify-ca" may not be used with sslrootcert=system',
    )

    if libressl:
        pytest.skip("SSL_CERT_FILE is not supported with LibreSSL")

    # Override the system trust store to point at our private root CA. On a Nix
    # build OpenSSL is patched so NIX_SSL_CERT_FILE takes precedence over the
    # standard SSL_CERT_FILE, so both are overridden here to reproduce the Perl
    # test's local $ENV{SSL_CERT_FILE} behavior.
    root_ca = str(node.datadir / "root_ca.crt")
    saved = {k: os.environ.get(k) for k in ("SSL_CERT_FILE", "NIX_SSL_CERT_FILE")}
    os.environ["SSL_CERT_FILE"] = root_ca
    if "NIX_SSL_CERT_FILE" in os.environ:
        os.environ["NIX_SSL_CERT_FILE"] = root_ca
    try:
        node.connect_ok(
            "{} sslmode=verify-full host=common-name.pg-ssltest.test".format(base),
            "sslrootcert=system connects with overridden SSL_CERT_FILE",
        )
        node.connect_fails(
            "{} host=common-name.pg-ssltest.test.bad".format(base),
            "sslrootcert=system defaults to sslmode=verify-full",
            expected_stderr=r'server certificate for "common-name.pg-ssltest.test" does not match host name "common-name.pg-ssltest.test.bad"',
        )
    finally:
        for name, value in saved.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


def _test_server_crl(node, ssl_server):
    """Client-side CRL handling and the pg_stat_ssl view without a client cert."""
    ssl_server.switch_server_cert(node, certfile="server-revoked")
    common_connstr = (
        "{} user=ssltestuser dbname=trustdb hostaddr={} "
        "host=common-name.pg-ssltest.test".format(DEFAULT_SSL_CONNSTR, SERVERHOSTADDR)
    )

    node.connect_ok(
        "{} sslrootcert=ssl/root+server_ca.crt sslmode=verify-ca".format(
            common_connstr
        ),
        "connects without client-side CRL",
    )
    node.connect_fails(
        "{} sslrootcert=ssl/root+server_ca.crt sslmode=verify-ca "
        "sslcrl=ssl/root+server.crl".format(common_connstr),
        "does not connect with client-side CRL file",
        expected_stderr=r"SSL error: certificate verify failed",
    )
    node.connect_fails(
        "{} sslcrl='' sslrootcert=ssl/root+server_ca.crt sslmode=verify-ca "
        "sslcrldir=ssl/root+server-crldir".format(common_connstr),
        "does not connect with client-side CRL directory",
        expected_stderr=r"SSL error: certificate verify failed",
    )

    node.command_like(
        [
            "psql",
            "--no-psqlrc",
            "--no-align",
            "--field-separator",
            ",",
            "--pset",
            "null=_null_",
            "--dbname",
            "{} sslrootcert=invalid".format(common_connstr),
            "--command",
            "SELECT * FROM pg_stat_ssl WHERE pid = pg_backend_pid()",
        ],
        r"(?mx)^pid,ssl,version,cipher,bits,client_dn,client_serial,issuer_dn\r?\n"
        r"^\d+,t,TLSv[\d.]+,[\w-]+,\d+,_null_,_null_,_null_\r?$",
        "pg_stat_ssl view without client certificate",
    )


def _test_protocol_versions(node):
    """ssl_min/max_protocol_version negotiation and validation."""
    common_connstr = (
        "{} user=ssltestuser dbname=trustdb hostaddr={} "
        "host=common-name.pg-ssltest.test".format(DEFAULT_SSL_CONNSTR, SERVERHOSTADDR)
    )
    base = "{} sslrootcert=ssl/root+server_ca.crt sslmode=require".format(
        common_connstr
    )
    node.connect_ok(
        "{} ssl_min_protocol_version=TLSv1.2 "
        "ssl_max_protocol_version=TLSv1.2".format(base),
        "connection success with correct range of TLS protocol versions",
    )
    node.connect_fails(
        "{} ssl_min_protocol_version=TLSv1.2 "
        "ssl_max_protocol_version=TLSv1.1".format(base),
        "connection failure with incorrect range of TLS protocol versions",
        expected_stderr=r"invalid SSL protocol version range",
    )
    node.connect_fails(
        "{} ssl_min_protocol_version=incorrect_tls".format(base),
        "connection failure with an incorrect SSL protocol minimum bound",
        expected_stderr=r'invalid "ssl_min_protocol_version" value',
    )
    node.connect_fails(
        "{} ssl_max_protocol_version=incorrect_tls".format(base),
        "connection failure with an incorrect SSL protocol maximum bound",
        expected_stderr=r'invalid "ssl_max_protocol_version" value',
    )


def _cert_base():
    """Common connstr for the certificate-authorization (certdb) tests."""
    return (
        "{} sslrootcert=ssl/root+server_ca.crt sslmode=require dbname=certdb "
        "hostaddr={} host=localhost".format(DEFAULT_SSL_CONNSTR, SERVERHOSTADDR)
    )


def _test_cert_authorization(node, ssl_server, supports_sslcertmode_require):
    """Server-side client-certificate authorization (cert auth)."""
    common = _cert_base()
    key = ssl_server.sslkey

    node.connect_fails(
        "{} user=ssltestuser sslcert=invalid".format(common),
        "certificate authorization fails without client cert",
        expected_stderr=r"connection requires a valid client certificate",
    )
    node.connect_ok(
        "{} user=ssltestuser sslcert=ssl/client.crt{}".format(
            common, key("client.key")
        ),
        "certificate authorization succeeds with correct client cert in PEM format",
    )
    node.connect_ok(
        "{} user=ssltestuser sslcert=ssl/client.crt{}".format(
            common, key("client-der.key")
        ),
        "certificate authorization succeeds with correct client cert in DER format",
    )
    node.connect_ok(
        "{} user=ssltestuser sslcert=ssl/client.crt{} sslpassword='dUmmyP^#+'".format(
            common, key("client-encrypted-pem.key")
        ),
        "certificate authorization succeeds with correct client cert in encrypted PEM format",
    )
    node.connect_ok(
        "{} user=ssltestuser sslcert=ssl/client.crt{} sslpassword='dUmmyP^#+'".format(
            common, key("client-encrypted-der.key")
        ),
        "certificate authorization succeeds with correct client cert in encrypted DER format",
    )

    if supports_sslcertmode_require:
        node.connect_ok(
            "{} user=ssltestuser sslcertmode=require sslcert=ssl/client.crt{}".format(
                common, key("client.key")
            ),
            "certificate authorization succeeds with correct client cert and sslcertmode=require",
        )
    node.connect_ok(
        "{} user=ssltestuser sslcertmode=allow sslcert=ssl/client.crt{}".format(
            common, key("client.key")
        ),
        "certificate authorization succeeds with correct client cert and sslcertmode=allow",
    )
    node.connect_fails(
        "{} user=ssltestuser sslcertmode=disable sslcert=ssl/client.crt{}".format(
            common, key("client.key")
        ),
        "certificate authorization fails with correct client cert and sslcertmode=disable",
        expected_stderr=r"connection requires a valid client certificate",
    )
    node.connect_fails(
        "{} user=ssltestuser sslcert=ssl/client.crt{} sslpassword='wrong'".format(
            common, key("client-encrypted-pem.key")
        ),
        "certificate authorization fails with correct client cert and wrong password in encrypted PEM format",
        expected_stderr=r'private key file ".*client-encrypted-pem\.key": bad decrypt',
    )

    _test_dn_cn_mapping(node, common, key)
    _test_cert_failures(node, ssl_server, common, key, supports_sslcertmode_require)


def _test_dn_cn_mapping(node, common, key):
    """DN/regex/CN ident mapping for certificate authentication."""
    node.connect_ok(
        "{} user=ssltestuser sslcert=ssl/client-dn.crt{}".format(
            common.replace("dbname=certdb", "dbname=certdb_dn"), key("client-dn.key")
        ),
        "certificate authorization succeeds with DN mapping",
        log_like=[
            r'connection authenticated: identity="CN=ssltestuser-dn,OU=Testing,OU=Engineering,O=PGDG" method=cert'
        ],
    )
    node.connect_ok(
        "{} user=ssltestuser sslcert=ssl/client-dn.crt{}".format(
            common.replace("dbname=certdb", "dbname=certdb_dn_re"),
            key("client-dn.key"),
        ),
        "certificate authorization succeeds with DN regex mapping",
    )
    node.connect_ok(
        "{} user=ssltestuser sslcert=ssl/client-dn.crt{}".format(
            common.replace("dbname=certdb", "dbname=certdb_cn"), key("client-dn.key")
        ),
        "certificate authorization succeeds with CN mapping",
        log_like=[
            r'connection authenticated: identity="CN=ssltestuser-dn,OU=Testing,OU=Engineering,O=PGDG" method=cert'
        ],
    )


def _test_cert_failures(node, ssl_server, common, key, supports_sslcertmode_require):
    """Wrong permissions, wrong user, revoked certs and verify-full/verify-ca HBA."""
    if platform.system() != "Windows":
        node.connect_fails(
            "{} user=ssltestuser sslcert=ssl/client.crt{}".format(
                common, key("client_wrongperms.key")
            ),
            "certificate authorization fails because of file permissions",
            expected_stderr=r'private key file ".*client_wrongperms\.key" has group or world access',
        )

    node.connect_fails(
        "{} user=anotheruser sslcert=ssl/client.crt{}".format(
            common, key("client.key")
        ),
        "certificate authorization fails with client cert belonging to another user",
        expected_stderr=r'certificate authentication failed for user "anotheruser"',
        log_like=[r'connection authenticated: identity="CN=ssltestuser" method=cert'],
    )

    node.connect_fails(
        "{} user=ssltestuser sslcert=ssl/client-revoked.crt{}".format(
            common, key("client-revoked.key")
        ),
        "certificate authorization fails with revoked client cert",
        expected_stderr=r"SSL error: (ssl[a-z0-9/]*|tls) alert certificate revoked",
        log_like=[
            r"Client certificate verification failed at depth 0: certificate revoked",
            r'Failed certificate data \(unverified\): subject "/CN=ssltestuser", serial number \d+, issuer "/CN=Test CA for PostgreSQL SSL regression test client certs"',
        ],
        log_unlike=[r"connection authenticated:"],
    )

    _test_verify_full_ca(node, key)
    _test_intermediate_ca(node, ssl_server, key)
    _test_server_crl_dir(node, ssl_server, key)
    _test_client_cas(node, ssl_server, key, supports_sslcertmode_require)


def _test_verify_full_ca(node, key):
    """clientcert=verify-full vs verify-ca on the verifydb database."""
    common = (
        "{} sslrootcert=ssl/root+server_ca.crt sslmode=require dbname=verifydb "
        "hostaddr={} host=localhost".format(DEFAULT_SSL_CONNSTR, SERVERHOSTADDR)
    )
    node.connect_ok(
        "{} user=ssltestuser sslcert=ssl/client.crt{}".format(
            common, key("client.key")
        ),
        "auth_option clientcert=verify-full succeeds with matching username and Common Name",
        log_like=[r'connection authenticated: user="ssltestuser" method=trust'],
    )
    node.connect_fails(
        "{} user=anotheruser sslcert=ssl/client.crt{}".format(
            common, key("client.key")
        ),
        "auth_option clientcert=verify-full fails with mismatching username and Common Name",
        expected_stderr=r'FATAL: .* "trust" authentication failed for user "anotheruser"',
        log_unlike=[r"connection authenticated:"],
    )
    node.connect_ok(
        "{} user=yetanotheruser sslcert=ssl/client.crt{}".format(
            common, key("client.key")
        ),
        "auth_option clientcert=verify-ca succeeds with mismatching username and Common Name",
        log_like=[r'connection authenticated: user="yetanotheruser" method=trust'],
    )


def _test_intermediate_ca(node, ssl_server, key):
    """Intermediate client CA provided by the client; missing/untrusted cases."""
    ssl_server.switch_server_cert(node, certfile="server-cn-only", cafile="root_ca")
    base = (
        "{} user=ssltestuser dbname=certdb sslrootcert=ssl/root+server_ca.crt "
        "hostaddr={} host=localhost".format(DEFAULT_SSL_CONNSTR, SERVERHOSTADDR)
    )
    common = "{}{}".format(base, key("client.key"))
    node.connect_ok(
        "{} sslmode=require sslcert=ssl/client+client_ca.crt".format(common),
        "intermediate client certificate is provided by client",
    )
    node.connect_fails(
        "{} sslmode=require sslcert=ssl/client.crt".format(common),
        "intermediate client certificate is missing",
        expected_stderr=r"SSL error: tlsv1 alert unknown ca",
        log_like=[
            r"Client certificate verification failed at depth 0: unable to get local issuer certificate",
            r'Failed certificate data \(unverified\): subject "/CN=ssltestuser", serial number \d+, issuer "/CN=Test CA for PostgreSQL SSL regression test client certs"',
        ],
    )
    node.connect_fails(
        "{} sslmode=require sslcert=ssl/client-long.crt{}".format(
            base, key("client-long.key")
        ),
        "logged client certificate Subjects are truncated if they're too long",
        expected_stderr=r"SSL error: tlsv1 alert unknown ca",
        log_like=[
            r"Client certificate verification failed at depth 0: unable to get local issuer certificate",
            r'Failed certificate data \(unverified\): subject "\.\.\./CN=ssl-123456789012345678901234567890123456789012345678901234567890", serial number \d+, issuer "/CN=Test CA for PostgreSQL SSL regression test client certs"',
        ],
    )

    # Untrusted intermediate: cert chain depth > 0 error logging. (The
    # LibreSSL-specific variant of the failed-cert-data line is not exercised
    # here because this build uses OpenSSL.)
    ssl_server.switch_server_cert(
        node, certfile="server-cn-only", cafile="server-cn-only"
    )
    node.connect_fails(
        "{} sslmode=require sslcert=ssl/client+client_ca.crt".format(common),
        "intermediate client certificate is untrusted",
        expected_stderr=r"SSL error: tlsv1 alert unknown ca",
        log_like=[
            r"Client certificate verification failed at depth 1: unable to get local issuer certificate",
            r'Failed certificate data \(unverified\): subject "/CN=Test CA for PostgreSQL SSL regression test client certs", serial number \d+, issuer "/CN=Test root CA for PostgreSQL SSL regression test suite"',
        ],
    )


def _test_server_crl_dir(node, ssl_server, key):
    """Server-side CRL directory revokes client certs (ASCII and UTF-8)."""
    base = (
        "{} user=ssltestuser dbname=certdb sslrootcert=ssl/root+server_ca.crt "
        "hostaddr={} host=localhost sslmode=require".format(
            DEFAULT_SSL_CONNSTR, SERVERHOSTADDR
        )
    )
    ssl_server.switch_server_cert(
        node, certfile="server-cn-only", crldir="root+client-crldir"
    )
    node.connect_fails(
        "{} sslcert=ssl/client-revoked.crt{}".format(base, key("client-revoked.key")),
        "certificate authorization fails with revoked client cert with server-side CRL directory",
        expected_stderr=r"SSL error: (ssl[a-z0-9/]*|tls) alert certificate revoked",
        log_like=[
            r"Client certificate verification failed at depth 0: certificate revoked",
            r'Failed certificate data \(unverified\): subject "/CN=ssltestuser", serial number \d+, issuer "/CN=Test CA for PostgreSQL SSL regression test client certs"',
        ],
    )
    node.connect_fails(
        "{} sslcert=ssl/client-revoked-utf8.crt{}".format(
            base, key("client-revoked-utf8.key")
        ),
        "certificate authorization fails with revoked UTF-8 client cert with server-side CRL directory",
        expected_stderr=r"SSL error: (ssl[a-z0-9/]*|tls) alert certificate revoked",
        log_like=[
            r"Client certificate verification failed at depth 0: certificate revoked",
            r'Failed certificate data \(unverified\): subject "/CN=\\xce\\x9f\\xce\\xb4\\xcf\\x85\\xcf\\x83\\xcf\\x83\\xce\\xad\\xce\\xb1\\xcf\\x82", serial number \d+, issuer "/CN=Test CA for PostgreSQL SSL regression test client certs"',
        ],
    )


def _test_client_cas(node, ssl_server, key, supports_sslcertmode_require):
    """Per-host client CA configuration (requires sslcertmode=require support)."""
    if not supports_sslcertmode_require:
        pytest.skip("sslmode require not supported in this build")

    connstr = (
        "user=ssltestuser dbname=certdb hostaddr={} sslmode=require sslsni=1".format(
            SERVERHOSTADDR
        )
    )

    ssl_server.switch_server_cert(node, certfile="server-cn-only", cafile="")
    node.connect_fails(
        "{} host=example.org sslcertmode=require sslcert=ssl/client.crt{}".format(
            connstr, key("client.key")
        ),
        "host: 'example.org', ca: '': connect with sslcert, no client CA configured",
        expected_stderr=r"client certificates can only be checked if a root certificate store is available",
    )

    ssl_server.switch_server_cert(
        node, certfile="server-cn-only", cafile="root+client_ca"
    )
    node.connect_fails(
        "{} host=example.com sslcertmode=disable".format(connstr),
        "host: 'example.com', ca: 'root+client_ca.crt': connect fails if no client certificate sent",
        expected_stderr=r"connection requires a valid client certificate",
    )
    node.connect_ok(
        "{} host=example.com sslcertmode=require sslcert=ssl/client.crt{}".format(
            connstr, key("client.key")
        ),
        "host: 'example.com', ca: 'root+client_ca.crt': connect with sslcert, client certificate sent",
    )

    ssl_server.switch_server_cert(
        node, certfile="server-cn-only", cafile="root+server_ca"
    )
    node.connect_fails(
        "{} host=example.net sslcertmode=disable".format(connstr),
        "host: 'example.net', ca: 'root+server_ca.crt': connect fails if no client certificate sent",
        expected_stderr=r"connection requires a valid client certificate",
    )
    node.connect_fails(
        "{} host=example.net sslcertmode=require sslcert=ssl/client.crt{}".format(
            connstr, key("client.key")
        ),
        "host: 'example.net', ca: 'root+server_ca.crt': connect with sslcert, client certificate sent",
        expected_stderr=r"unknown ca",
    )

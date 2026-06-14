# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/modules/ssl_passphrase_callback/t/001_testfunc.pl.

The ssl_passphrase_func module supplies the TLS key passphrase via a GUC: the
server starts with the correct passphrase, warns that ssl_passphrase_command is
ignored, fails to start with a wrong passphrase, and (non-LibreSSL) warns that
an installed TLS init hook is ignored when SNI is enabled -- exactly once.
Requires an OpenSSL build.
"""

import os
import re
import shutil

import pytest

import pypg

_SRCDIR = os.path.join(os.path.dirname(__file__), "..")


def test_001_testfunc(create_pg):
    """ssl_passphrase_func passphrase GUC drives TLS key unlock and warnings."""
    if os.environ.get("with_ssl") != "openssl":
        pytest.skip("OpenSSL not supported by this build")
    libressl = not pypg.check_pg_config(r"#define HAVE_SSL_CTX_SET_CERT_CB 1")
    rot13pass = "SbbOnE1"
    node = create_pg("main", start=False)
    node.append_conf("ssl_passphrase.passphrase = '{}'".format(rot13pass))
    node.append_conf("shared_preload_libraries = 'ssl_passphrase_func'")
    node.append_conf("ssl = 'on'")
    ddir = node.datadir
    shutil.copy(os.path.join(_SRCDIR, "server.crt"), ddir)
    shutil.copy(os.path.join(_SRCDIR, "server.key"), ddir)
    os.chmod(os.path.join(ddir, "server.key"), 0o600)
    node.start()
    assert os.path.exists("{}/postmaster.pid".format(ddir)), "postgres started"
    node.stop("fast")
    log = node.rotate_logfile()
    node.append_conf("ssl_passphrase_command = 'echo spl0tz'")
    node.start()
    node.stop("fast")
    log_contents = pypg.slurp_file(log)
    assert re.search(
        r'WARNING.*"ssl_passphrase_command" setting ignored by '
        r"ssl_passphrase_func module",
        log_contents,
    ), "ssl_passphrase_command set warning"
    node.append_conf("ssl_passphrase.passphrase = 'blurfl'")
    result = node.bin.run_command(
        ["pg_ctl", "--pgdata", str(node.datadir), "--log", str(node.log), "start"]
    )
    assert result.rc != 0, "pg_ctl fails with bad passphrase"
    assert not os.path.exists(
        "{}/postmaster.pid".format(ddir)
    ), "postgres not started with bad passphrase"
    node.stop("fast")
    if libressl:
        return
    node.append_conf("\nssl_passphrase_command = 'echo FooBaR1'\nssl_sni = on\n")
    node.append_conf(
        'example.org "{d}/server.crt" "{d}/server.key" "" "echo FooBaR1" on\n'
        'example.com "{d}/server.crt" "{d}/server.key" "" "echo FooBaR1" on\n'.format(
            d=ddir
        ),
        "pg_hosts.conf",
    )
    node.start()
    assert os.path.exists(
        "{}/postmaster.pid".format(ddir)
    ), "postgres started after SNI"
    node.stop("fast")
    log_contents = pypg.slurp_file(log)
    assert re.search(
        r"WARNING.*SNI is enabled; installed TLS init hook will be ignored",
        log_contents,
    ), "server warns that init hook and SNI are incompatible"
    count = len(re.findall(r"installed TLS init hook will be ignored", log_contents))
    assert count == 1, "Only one WARNING"

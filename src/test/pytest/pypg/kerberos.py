# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Stand-alone KDC for testing PostgreSQL GSSAPI / Kerberos functionality.

This is the Python port of ``src/test/perl/PostgreSQL/Test/Kerberos.pm``. It
locates the MIT krb5 binaries, writes ``krb5.conf`` + ``kdc.conf``, creates a
KDC realm/database, adds the PostgreSQL service principal plus arbitrary test
user principals, starts ``krb5kdc`` and tears it all down.

Like the Perl module it sets the ``KRB5_CONFIG``, ``KRB5_KDC_PROFILE`` and
``KRB5CCNAME`` environment variables so that every subprocess (psql, the
server, kinit, ...) uses this test realm rather than any global configuration.
"""

import os
import pathlib
import re
import shutil
import signal
import subprocess
import sys
from typing import List, Optional

from .util import append_to_file, get_free_port


def _eprint(*args) -> None:
    """Print a diagnostic line to stderr (mirrors note()/diag() in Perl)."""
    print(*args, file=sys.stderr)


def _which(name: str) -> str:
    """Resolve a krb5 binary from PATH, raising if it cannot be found.

    The harness relies on the krb5 bin/sbin directories being on PATH (the
    Perl module hard-codes platform-specific directories; we resolve via PATH
    so the same code works for the nix store path and system installs).
    """
    found = shutil.which(name)
    if found is None:
        raise FileNotFoundError(
            "could not find krb5 binary {!r} on PATH; ensure the krb5 "
            "bin/sbin directories are on PATH".format(name)
        )
    return found


def _detect_krb5_version() -> float:
    """Return the MIT krb5 minor release as a float (e.g. 1.21 -> 1.21).

    Mirrors Kerberos.pm: run ``krb5-config --version`` and parse the
    ``Kerberos 5 release X.Y`` line, bailing on Heimdal. If ``krb5-config`` is
    not installed (it ships only with the krb5 dev package) we fall back to
    ``krb5kdc``'s usage banner, and finally to a conservative 1.15 so the
    newer ``kdc_listen``/``kdc_tcp_listen`` settings are used.
    """
    krb5_config = shutil.which("krb5-config")
    if krb5_config is not None:
        proc = subprocess.run(
            [krb5_config, "--version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            encoding="utf-8",
            check=False,
        )
        out = proc.stdout
        if "heimdal" in out.lower():
            raise RuntimeError("Heimdal is not supported")
        match = re.search(r"Kerberos 5 release ([0-9]+\.[0-9]+)", out)
        if match:
            return float(match.group(1))

    # krb5-config absent: probe krb5kdc's own version/usage text.
    krb5kdc = shutil.which("krb5kdc")
    if krb5kdc is not None:
        proc = subprocess.run(
            [krb5kdc, "-r", "VERSION_PROBE", "-n"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            encoding="utf-8",
            check=False,
            timeout=5,
        )
        match = re.search(r"release ([0-9]+\.[0-9]+)", proc.stdout)
        if match:
            return float(match.group(1))

    # Conservative default: the _listen settings exist since krb5 1.15, which
    # has been the norm for many years.
    return 1.15


class KerberosServer:
    """A running stand-alone KDC and its generated configuration.

    Create with :func:`KerberosServer.setup`; the realm is configured, the
    service principal added and ``krb5kdc`` started. Call
    :meth:`create_principal` to add user principals, :meth:`create_ticket` to
    run ``kinit`` for one of them, and :meth:`stop` (or use as a context
    manager) to shut the KDC down.

    Attributes:
        keytab: Path to the service principal keytab (krb_server_keyfile).
        krb5_conf: Path to the generated krb5.conf.
        kdc_conf: Path to the generated kdc.conf.
        krb5_cache: Path to the credentials cache (KRB5CCNAME).
        realm: The Kerberos realm name.
        kdc_port: The TCP/UDP port the KDC listens on.
    """

    def __init__(
        self,
        tmp_check: pathlib.Path,
        log_path: pathlib.Path,
        host: str,
        hostaddr: str,
        realm: str,
    ):
        """Initialize paths and binary locations; does not start anything.

        Args:
            tmp_check: Directory for generated config, cache and KDC database.
            log_path: Directory for the krb5 library and KDC log files.
            host: Hostname used in the PostgreSQL service principal.
            hostaddr: Interface address the KDC listens on.
            realm: Kerberos realm name.
        """
        self.host = host
        self.hostaddr = hostaddr
        self.realm = realm

        self._krb5_config = shutil.which("krb5-config") or "krb5-config"
        self._kinit = _which("kinit")
        self._klist = _which("klist")
        self._kdb5_util = _which("kdb5_util")
        self._kadmin_local = _which("kadmin.local")
        self._krb5kdc = _which("krb5kdc")

        self.krb5_conf = tmp_check / "krb5.conf"
        self.kdc_conf = tmp_check / "kdc.conf"
        self.krb5_cache = tmp_check / "krb5cc"
        self._krb5_log = log_path / "krb5libs.log"
        self._kdc_log = log_path / "krb5kdc.log"
        self.kdc_port = get_free_port()
        self._kdc_datadir = tmp_check / "krb5kdc"
        self._kdc_pidfile = tmp_check / "krb5kdc.pid"
        self.keytab = tmp_check / "krb5.keytab"
        self._stopped = False

    @classmethod
    def setup(
        cls,
        tmp_check: pathlib.Path,
        log_path: pathlib.Path,
        host: str,
        hostaddr: str,
        realm: str,
    ) -> "KerberosServer":
        """Build the realm, add the service principal and start krb5kdc.

        Mirrors ``PostgreSQL::Test::Kerberos->new``: assigns a free port for the
        KDC, writes the config, creates the KDC database with a master key,
        adds the ``$with_krb_srvnam/$host`` service principal, extracts its
        keytab and launches ``krb5kdc``. The required ``KRB5_CONFIG``,
        ``KRB5_KDC_PROFILE`` and ``KRB5CCNAME`` environment variables are set as
        a side effect so all child processes use this test realm.
        """
        self = cls(tmp_check, log_path, host, hostaddr, realm)
        self._write_config()
        self._kdc_datadir.mkdir()

        # Ensure we use the test's config and cache files, not global ones.
        os.environ["KRB5_CONFIG"] = str(self.krb5_conf)
        os.environ["KRB5_KDC_PROFILE"] = str(self.kdc_conf)
        os.environ["KRB5CCNAME"] = str(self.krb5_cache)

        krb_srvnam = os.environ.get("with_krb_srvnam", "postgres")
        service_principal = "{}/{}".format(krb_srvnam, host)

        self._run_or_bail([self._kdb5_util, "create", "-s", "-P", "secret0"])
        self._run_or_bail(
            [self._kadmin_local, "-q", "addprinc -randkey " + service_principal]
        )
        self._run_or_bail(
            [
                self._kadmin_local,
                "-q",
                "ktadd -k {} {}".format(self.keytab, service_principal),
            ]
        )
        self._run_or_bail([self._krb5kdc, "-P", str(self._kdc_pidfile)])
        return self

    def _write_config(self) -> None:
        """Write krb5.conf and kdc.conf for this realm (mirrors Kerberos.pm).

        DNS realm/KDC lookups and reverse DNS are explicitly disabled, the
        non-standard KDC port is pinned, and for krb5 >= 1.15 the bind is
        restricted to the test interface via kdc_listen/kdc_tcp_listen.

        dns_canonicalize_hostname is disabled so the GSSAPI client uses the
        literal service hostname for the SPN instead of doing a forward DNS
        lookup. The test's service host (auth-test-localhost...example.com) does
        not resolve, so without this each GSS connection blocks ~20s on a
        resolver timeout; upstream CI sidesteps this by putting the name in
        /etc/hosts, which is not writable here. The keytab principal already
        uses the literal name, so authentication is unchanged.
        """
        append_to_file(
            self.krb5_conf,
            "[logging]\n"
            "default = FILE:{krb5_log}\n"
            "kdc = FILE:{kdc_log}\n"
            "\n"
            "[libdefaults]\n"
            "dns_lookup_realm = false\n"
            "dns_lookup_kdc = false\n"
            "dns_canonicalize_hostname = false\n"
            "default_realm = {realm}\n"
            "forwardable = false\n"
            "rdns = false\n"
            "\n"
            "[realms]\n"
            "{realm} = {{\n"
            "    kdc = {hostaddr}:{kdc_port}\n"
            "}}\n".format(
                krb5_log=self._krb5_log,
                kdc_log=self._kdc_log,
                realm=self.realm,
                hostaddr=self.hostaddr,
                kdc_port=self.kdc_port,
            ),
        )

        append_to_file(self.kdc_conf, "[kdcdefaults]\n")

        krb5_version = _detect_krb5_version()
        if krb5_version >= 1.15:
            append_to_file(
                self.kdc_conf,
                "kdc_listen = {hostaddr}:{kdc_port}\n"
                "kdc_tcp_listen = {hostaddr}:{kdc_port}\n".format(
                    hostaddr=self.hostaddr, kdc_port=self.kdc_port
                ),
            )
        else:
            append_to_file(
                self.kdc_conf,
                "kdc_ports = {kdc_port}\n"
                "kdc_tcp_ports = {kdc_port}\n".format(kdc_port=self.kdc_port),
            )

        append_to_file(
            self.kdc_conf,
            "\n"
            "[realms]\n"
            "{realm} = {{\n"
            "    database_name = {datadir}/principal\n"
            "    admin_keytab = FILE:{datadir}/kadm5.keytab\n"
            "    acl_file = {datadir}/kadm5.acl\n"
            "    key_stash_file = {datadir}/_k5.{realm}\n"
            "}}".format(realm=self.realm, datadir=self._kdc_datadir),
        )

    def create_principal(self, principal: str, password: str) -> None:
        """Add a user principal with a fixed password (mirrors create_principal)."""
        self._run_or_bail(
            [
                self._kadmin_local,
                "-q",
                "addprinc -pw {} {}".format(password, principal),
            ]
        )

    def create_ticket(
        self, principal: str, password: str, *, forwardable: bool = False
    ) -> None:
        """Obtain a TGT for principal via kinit (mirrors create_ticket).

        With ``forwardable=True`` the ``-f`` flag is passed so the ticket can be
        delegated. The password is supplied on kinit's stdin; ``klist -f`` is
        then run for diagnostics, exactly as the Perl module does.
        """
        cmd = [self._kinit, principal]
        if forwardable:
            cmd.append("-f")
        self._run_or_bail(cmd, stdin=password)
        self._run_or_bail([self._klist, "-f"])

    def _run_or_bail(self, cmd: List[str], stdin: Optional[str] = None) -> None:
        """Run a krb5 command, echoing it and raising with output on failure.

        Mirrors system_or_bail/run_log: the command is logged to stderr, and on
        a non-zero exit a RuntimeError carrying stdout+stderr is raised so the
        KDC's complaint (realm casing, FQDN, port binding, ...) is visible.
        """
        _eprint("+ " + " ".join(str(c) for c in cmd))
        proc = subprocess.run(
            [str(c) for c in cmd],
            input=stdin,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            encoding="utf-8",
            check=False,
        )
        if proc.stdout:
            _eprint(proc.stdout)
        if proc.returncode != 0:
            raise RuntimeError(
                "command failed (exit {}): {}\noutput:\n{}".format(
                    proc.returncode, " ".join(str(c) for c in cmd), proc.stdout
                )
            )

    def stop(self) -> None:
        """Stop the KDC by signalling the pid in its pidfile (mirrors END).

        Sends SIGINT to the daemonized krb5kdc, taking care to be idempotent so
        it is safe to call from a fixture teardown and a context-manager exit.
        """
        if self._stopped:
            return
        self._stopped = True
        try:
            pid = int(self._kdc_pidfile.read_text(encoding="utf-8").strip())
        except (OSError, ValueError):
            return
        try:
            os.kill(pid, signal.SIGINT)
        except ProcessLookupError:
            pass

    def __enter__(self) -> "KerberosServer":
        return self

    def __exit__(self, *exc) -> None:
        self.stop()

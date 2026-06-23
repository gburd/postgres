# Copyright (c) 2023-2026, PostgreSQL Global Development Group

"""In-process OpenLDAP server for testing pg_hba.conf ldap authentication.

This is the Python twin of src/test/ldap/LdapServer.pm. It locates a suitable
``slapd`` binary and the OpenLDAP schema directory, writes a slapd config,
starts an LDAP server, loads LDIF data, and tears the server down.

Like the Perl module, ``slapd`` is resolved to an absolute path while the
client tools (``ldapadd``, ``ldapsearch``, ``ldappasswd``) are expected to be
found on PATH. The binary/schema detection mirrors LdapServer.pm's OS-specific
locations and adds a generic fallback that locates ``slapd`` via PATH and the
schema directory relative to its install prefix (e.g. a Nix-style
``<prefix>/etc/schema`` layout), so the tests run wherever OpenLDAP is
installed.
"""

import os
import pathlib
import platform
import shutil
import subprocess
import time
from typing import List, Optional, Tuple

import pytest

from pypg.util import append_to_file, eprint, get_free_port


# The four schema files slapd.conf includes, mirroring LdapServer.pm.
_REQUIRED_SCHEMAS = ("core", "cosine", "nis", "inetorgperson")


def _schema_dir_has_all(directory: pathlib.Path) -> bool:
    """Return True if directory contains every required .schema file."""
    return all((directory / (name + ".schema")).is_file() for name in _REQUIRED_SCHEMAS)


def _detect_from_known_paths() -> Tuple[Optional[str], Optional[pathlib.Path]]:
    """Find slapd and the schema dir from LdapServer.pm's OS-specific paths.

    Returns (slapd_path, schema_dir) or (None, None) if no known layout matches.
    """
    system = platform.system()
    candidates: List[Tuple[str, str]] = []
    if system == "Darwin":
        candidates = [
            (
                "/opt/homebrew/opt/openldap/libexec/slapd",
                "/opt/homebrew/etc/openldap/schema",
            ),
            ("/usr/local/opt/openldap/libexec/slapd", "/usr/local/etc/openldap/schema"),
            ("/opt/local/libexec/slapd", "/opt/local/etc/openldap/schema"),
        ]
    elif system == "Linux":
        candidates = [
            ("/usr/sbin/slapd", "/etc/ldap/schema"),
            ("/usr/sbin/slapd", "/etc/openldap/schema"),
        ]
    elif system == "FreeBSD":
        candidates = [("/usr/local/libexec/slapd", "/usr/local/etc/openldap/schema")]
    elif system == "OpenBSD":
        candidates = [
            ("/usr/local/libexec/slapd", "/usr/local/share/examples/openldap/schema")
        ]
    for slapd, schema in candidates:
        schema_dir = pathlib.Path(schema)
        if schema_dir.is_dir() and _schema_dir_has_all(schema_dir):
            return slapd, schema_dir
    return None, None


def _detect_generic() -> Tuple[Optional[str], Optional[pathlib.Path]]:
    """Find slapd via PATH/well-known dirs and the schema dir near its prefix.

    Covers installations (e.g. Nix) whose schema files live under
    ``<prefix>/etc/schema`` instead of the OS-standard locations.
    """
    slapd = shutil.which("slapd")
    if slapd is None:
        for libexec in ("/usr/lib/openldap", "/usr/libexec/openldap"):
            cand = os.path.join(libexec, "slapd")
            if os.path.isfile(cand):
                slapd = cand
                break
    if slapd is None:
        return None, None

    # Walk up from the slapd binary looking for a schema directory containing
    # the required *.schema files (e.g. <prefix>/etc/schema, <prefix>/share,
    # <prefix>/etc/openldap/schema).
    base = pathlib.Path(slapd).resolve().parent
    search_roots = [base, base.parent, base.parent.parent]
    schema_subdirs = [
        ("etc", "schema"),
        ("etc", "openldap", "schema"),
        ("etc", "ldap", "schema"),
        ("share", "openldap", "schema"),
        ("share", "schema"),
    ]
    for root in search_roots:
        for parts in schema_subdirs:
            schema_cand = root.joinpath(*parts)
            if schema_cand.is_dir() and _schema_dir_has_all(schema_cand):
                return slapd, schema_cand
    return None, None


def detect_ldap() -> Tuple[bool, Optional[str], Optional[pathlib.Path], Optional[str]]:
    """Detect a usable slapd binary and OpenLDAP schema directory.

    Mirrors LdapServer.pm's INIT-phase detection (the OS-specific known paths)
    and falls back to locating slapd on PATH with a schema dir near its install
    prefix.

    Returns:
        A 4-tuple ``(setup, slapd, schema_dir, error)``. ``setup`` is True when
        a binary and schema dir were found; on failure ``error`` carries a
        human-readable reason, matching ``$LdapServer::setup_error``.
    """
    system = platform.system()
    if system not in ("Darwin", "Linux", "FreeBSD", "OpenBSD"):
        return False, None, None, "ldap tests not supported on {}".format(system)

    slapd, schema_dir = _detect_from_known_paths()
    if slapd is None or schema_dir is None:
        slapd, schema_dir = _detect_generic()

    if slapd is None or schema_dir is None or not os.path.isfile(slapd):
        return False, None, None, "OpenLDAP server installation not found"
    return True, slapd, schema_dir, None


# Module-level detection results, mirroring $LdapServer::setup / $setup_error.
setup, _SLAPD, _SCHEMA_DIR, setup_error = detect_ldap()

# All running servers, terminated by stop_all() (mirrors the Perl END block).
_SERVERS: "List[LdapServer]" = []


class LdapServer:
    """A running OpenLDAP server for testing pg_hba.conf ldap authentication.

    Mirrors the LdapServer.pm class: the constructor writes a slapd config,
    copies TLS certificates, starts slapd on freshly allocated ldap/ldaps
    ports, and waits until it accepts requests. Use ``ldapadd_file`` and
    ``ldapsetpw`` to populate the directory, ``prop`` to read settings, and
    ``stop`` to terminate it.
    """

    def __init__(
        self, rootpw: str, authtype: str, *, testname: str, test_temp, log_dir
    ):
        """Create and start a new LDAP server.

        Args:
            rootpw: The rootdn password (used with the ldapbindpasswd option).
            authtype: Either ``'users'`` or ``'anonymous'`` (the slapd ACL
                ``by <authtype> auth`` clause).
            testname: A short name used in the slapd log filename.
            test_temp: A directory (path-like) to hold the server's data,
                config, certs, pid and password files.
            log_dir: Directory where the slapd log file is written.
        """
        if not setup:
            raise RuntimeError("no suitable binaries found")

        test_temp = pathlib.Path(test_temp)
        self._test_temp = test_temp
        ldap_datadir = test_temp / "openldap-data"
        slapd_certs = test_temp / "slapd-certs"
        self._pidfile = test_temp / "slapd.pid"
        slapd_conf = test_temp / "slapd.conf"
        slapd_logfile = pathlib.Path(log_dir) / "slapd-{}.log".format(testname)

        self._server = "localhost"
        self._port = get_free_port()
        self._s_port = get_free_port()
        self._url = "ldap://{}:{}".format(self._server, self._port)
        self._s_url = "ldaps://{}:{}".format(self._server, self._s_port)
        self._basedn = "dc=example,dc=net"
        self._rootdn = "cn=Manager,dc=example,dc=net"
        self._rootpw = rootpw
        self._pwfile = test_temp / "ldappassword"
        self._process: Optional[subprocess.Popen] = None

        assert _SCHEMA_DIR is not None  # guaranteed by setup check above
        conf = (
            "include {schema}/core.schema\n"
            "include {schema}/cosine.schema\n"
            "include {schema}/nis.schema\n"
            "include {schema}/inetorgperson.schema\n"
            "\n"
            "pidfile {pidfile}\n"
            "logfile {logfile}\n"
            "\n"
            "access to *\n"
            "        by * read\n"
            "        by {authtype} auth\n"
            "\n"
            "database ldif\n"
            "directory {datadir}\n"
            "\n"
            "TLSCACertificateFile {certs}/ca.crt\n"
            "TLSCertificateFile {certs}/server.crt\n"
            "TLSCertificateKeyFile {certs}/server.key\n"
            "\n"
            'suffix "dc=example,dc=net"\n'
            'rootdn "{rootdn}"\n'
            'rootpw "{rootpw}"\n'
        ).format(
            schema=_SCHEMA_DIR,
            pidfile=self._pidfile,
            logfile=slapd_logfile,
            authtype=authtype,
            datadir=ldap_datadir,
            certs=slapd_certs,
            rootdn=self._rootdn,
            rootpw=self._rootpw,
        )
        append_to_file(slapd_conf, conf)

        ldap_datadir.mkdir()
        slapd_certs.mkdir()

        certdir = pathlib.Path(__file__).resolve().parent.parent.parent / "ssl" / "ssl"
        shutil.copyfile(certdir / "server_ca.crt", slapd_certs / "ca.crt")
        if not (slapd_certs / "ca.crt").is_file():
            raise RuntimeError("copying ca.crt (error unknown)")
        shutil.copyfile(certdir / "server-cn-only.crt", slapd_certs / "server.crt")
        shutil.copyfile(certdir / "server-cn-only.key", slapd_certs / "server.key")

        append_to_file(self._pwfile, self._rootpw)
        os.chmod(self._pwfile, 0o600)

        self._start(slapd_conf)
        _SERVERS.append(self)

    def _start(self, slapd_conf: pathlib.Path) -> None:
        """Start slapd and poll with ldapsearch until it accepts requests."""
        assert _SLAPD is not None  # guaranteed by setup check
        # -s0 prevents log messages ending up in syslog.
        self._process = subprocess.Popen(  # pylint: disable=consider-using-with
            [
                _SLAPD,
                "-f",
                str(slapd_conf),
                "-s0",
                "-h",
                "{} {}".format(self._url, self._s_url),
            ]
        )
        retries = 0
        while True:
            probe = subprocess.run(
                [
                    "ldapsearch",
                    "-sbase",
                    "-H",
                    self._url,
                    "-b",
                    self._basedn,
                    "-D",
                    self._rootdn,
                    "-y",
                    str(self._pwfile),
                    "-n",
                    "'objectclass=*'",
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )
            if probe.returncode == 0:
                break
            retries += 1
            if retries >= 300:
                raise RuntimeError("cannot connect to slapd")
            eprint("# waiting for slapd to accept requests...")
            time.sleep(1)

    def _ldapenv(self) -> dict:
        """Return an environment with LDAPURI/LDAPBINDDN set (cf. _ldapenv)."""
        env = dict(os.environ)
        env["LDAPURI"] = self._url
        env["LDAPBINDDN"] = self._rootdn
        return env

    def ldapadd_file(self, path) -> None:
        """Add the contents of an LDIF file to the LDAP server.

        Args:
            path: Path to a file containing LDIF data to add.
        """
        subprocess.run(
            ["ldapadd", "-x", "-y", str(self._pwfile), "-f", str(path)],
            env=self._ldapenv(),
            check=True,
        )

    def ldapsetpw(self, user: str, password: str) -> None:
        """Set a user's password in the LDAP server (cf. ldapsetpw)."""
        subprocess.run(
            ["ldappasswd", "-x", "-y", str(self._pwfile), "-s", password, user],
            env=self._ldapenv(),
            check=True,
        )

    def prop(self, *names: str):
        """Return the values of the named properties (cf. LdapServer.pm prop).

        Recognized names: server, port, s_port, url, s_url, basedn, rootdn,
        pwfile.
        """
        mapping = {
            "server": self._server,
            "port": self._port,
            "s_port": self._s_port,
            "url": self._url,
            "s_url": self._s_url,
            "basedn": self._basedn,
            "rootdn": self._rootdn,
            "pwfile": self._pwfile,
        }
        return tuple(mapping[name] for name in names)

    @property
    def server(self) -> str:
        """The LDAP server hostname (localhost)."""
        return self._server

    @property
    def port(self) -> int:
        """The plaintext (ldap://) port."""
        return self._port

    @property
    def s_port(self) -> int:
        """The TLS (ldaps://) port."""
        return self._s_port

    @property
    def url(self) -> str:
        """The ldap:// URL."""
        return self._url

    @property
    def s_url(self) -> str:
        """The ldaps:// URL."""
        return self._s_url

    @property
    def basedn(self) -> str:
        """The directory base DN (dc=example,dc=net)."""
        return self._basedn

    @property
    def rootdn(self) -> str:
        """The directory root DN (cn=Manager,dc=example,dc=net)."""
        return self._rootdn

    def stop(self) -> None:
        """Terminate the slapd process if it is running (cf. the Perl END block)."""
        if self._process is not None and self._process.poll() is None:
            self._process.terminate()
            try:
                self._process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                self._process.kill()
                self._process.wait()
        self._process = None
        if self in _SERVERS:
            _SERVERS.remove(self)


def stop_all() -> None:
    """Stop every running LdapServer (mirrors the Perl END block)."""
    for server in list(_SERVERS):
        server.stop()


def require_ldap_enabled() -> None:
    """Skip unless LDAP is built in, opted into, and slapd is available.

    Mirrors the three ``plan skip_all`` checks at the top of the .pl tests:
    the ``with_ldap`` build flag, the ``ldap`` PG_TEST_EXTRA opt-in, and the
    presence of a usable slapd binary.
    """
    if os.environ.get("with_ldap") != "yes":
        pytest.skip("LDAP not supported by this build")
    extra = os.environ.get("PG_TEST_EXTRA", "")
    if "ldap" not in extra.split():
        pytest.skip("Potentially unsafe test LDAP not enabled in PG_TEST_EXTRA")
    ok, _slapd, _schema, error = detect_ldap()
    if not ok:
        pytest.skip(error or "OpenLDAP server installation not found")


@pytest.fixture(name="ldap_server")
def ldap_server_fixture(request, tmp_path_factory):
    """Factory fixture that starts LDAP servers and stops them after the test.

    The returned callable takes ``(rootpw, authtype)`` (``authtype`` is
    ``'users'`` or ``'anonymous'``) and returns a running ``LdapServer``,
    mirroring ``LdapServer->new`` in the Perl suite. Each server gets its own
    temp directory; the slapd log is written under a per-test log directory so
    it survives for failure inspection.
    """
    require_ldap_enabled()

    testname = request.node.name
    log_dir = tmp_path_factory.mktemp("ldap-log-{}".format(testname))
    started: List[LdapServer] = []

    def _make(rootpw: str, authtype: str) -> LdapServer:
        test_temp = tmp_path_factory.mktemp("ldap-{}".format(testname))
        server = LdapServer(
            rootpw,
            authtype,
            testname=testname,
            test_temp=test_temp,
            log_dir=log_dir,
        )
        started.append(server)
        return server

    yield _make

    for server in started:
        server.stop()
    stop_all()

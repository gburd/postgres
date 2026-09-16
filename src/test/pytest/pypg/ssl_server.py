# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Python port of src/test/ssl/t/SSL/Server.pm and SSL/Backend/OpenSSL.pm.

Configures a PostgresServer (created via the create_pg fixture) for the SSL
regression tests, mirroring SSL::Server / SSL::Backend::OpenSSL:

- enables SSL and rejects non-SSL connections
- creates the trustdb/certdb/... databases and ssltestuser/... users
- installs the pre-generated certificates, keys and CRLs into the data dir
- switches the active server certificate (switch_server_cert)

The certificate fixtures live in the ssl/ subdirectory of the SSL test
directory (the same files the Perl suite uses); they are reused verbatim and
never regenerated. Client keys must not be world-readable, so they are copied
to a private temporary directory with 0600 permissions, exactly as the Perl
backend does.
"""

import glob
import os
import pathlib
import shutil
import stat

import pypg

# The SSL test directory (src/test/ssl), holding the ssl/ fixtures. The Perl
# suite runs from this directory and refers to certs as e.g.
# "ssl/root+server_ca.crt"; resolve the same way regardless of cwd. This helper
# lives in pypg/, so the repo root is four parents up.
_SSL_TEST_DIR = pathlib.Path(__file__).resolve().parents[4] / "src" / "test" / "ssl"
_SSL_FILES_DIR = _SSL_TEST_DIR / "ssl"

# The databases and users created by configure_test_server_for_ssl.
_DATABASES = ("trustdb", "certdb", "certdb_dn", "certdb_dn_re", "certdb_cn", "verifydb")
_USERS = ("ssltestuser", "md5testuser", "anotheruser", "yetanotheruser")

# Client keys whose permissions must be tightened before use, mirroring the
# list in SSL::Backend::OpenSSL->init.
_CLIENT_KEYS = (
    "client.key",
    "client-revoked.key",
    "client-der.key",
    "client-encrypted-pem.key",
    "client-encrypted-der.key",
    "client-dn.key",
    "client_ext.key",
    "client-long.key",
    "client-revoked-utf8.key",
)


class OpenSSLBackend:
    """Mirror of SSL::Backend::OpenSSL.

    Installs the OpenSSL certificate fixtures into a cluster's data directory
    and produces the ssl_cert_file/ssl_key_file/... configuration fragments.
    """

    def __init__(self, key_tempdir: pathlib.Path):
        self._library = "OpenSSL"
        self._key: dict = {}
        self._key_tempdir = key_tempdir

    def init(self, pgdata: pathlib.Path) -> None:
        """Install certificates, keys and CRLs into the cluster data dir.

        Copies the server certs/keys, CA certs and CRLs the Perl backend copies,
        tightens server key permissions to 0600, then copies the client keys to
        a private temp directory (also 0600) plus a deliberately world-readable
        copy of client.key for the file-permission test.
        """
        pgdata = pathlib.Path(pgdata)
        self._copy_files("server-*.crt", pgdata)
        self._copy_files("server-*.key", pgdata)
        for key in glob.glob(str(pgdata / "server-*.key")):
            os.chmod(key, 0o600)
        for name in (
            "root+client_ca.crt",
            "root+server_ca.crt",
            "root_ca.crt",
            "root+client.crl",
        ):
            self._copy_files(name, pgdata)

        crldir = pgdata / "root+client-crldir"
        crldir.mkdir()
        self._copy_files("root+client-crldir/*", crldir)

        for keyfile in _CLIENT_KEYS:
            dest = self._key_tempdir / keyfile
            shutil.copyfile(_SSL_FILES_DIR / keyfile, dest)
            os.chmod(dest, 0o600)
            self._key[keyfile] = str(dest)

        # A world-readable copy of client.key, to test rejection of bad perms.
        wrongperms = self._key_tempdir / "client_wrongperms.key"
        shutil.copyfile(_SSL_FILES_DIR / "client.key", wrongperms)
        os.chmod(wrongperms, 0o644)
        self._key["client_wrongperms.key"] = str(wrongperms)

    def get_sslkey(self, keyfile: str) -> str:
        """Return an ' sslkey=<path>' connstr fragment for a tightened key."""
        return " sslkey={}".format(self._key[keyfile])

    def set_server_cert(self, params: dict) -> str:
        """Return the sslconfig.conf body selecting the given cert/key/CRL.

        Mirrors SSL::Backend::OpenSSL->set_server_cert, including the cafile and
        crlfile defaults and the empty-cafile special case (which sets
        ssl_ca_file='').
        """
        cafile = params.get("cafile", "root+client_ca")
        crlfile = params.get("crlfile", "root+client.crl")
        certfile = params["certfile"]
        keyfile = params.get("keyfile", certfile)

        sslconf = (
            "ssl_cert_file='{}.crt'\n"
            "ssl_key_file='{}.key'\n"
            "ssl_crl_file='{}'\n".format(certfile, keyfile, crlfile)
        )
        if cafile != "":
            sslconf += "ssl_ca_file='{}.crt'\n".format(cafile)
        else:
            sslconf += "ssl_ca_file=''\n"
        if "crldir" in params and params["crldir"] is not None:
            sslconf += "ssl_crl_dir='{}'\n".format(params["crldir"])
        return sslconf

    def get_library(self) -> str:
        """Return the SSL library name, "OpenSSL"."""
        return self._library

    def library_is_libressl(self) -> bool:
        """Detect whether the SSL library is LibreSSL.

        The HAVE_SSL_CTX_SET_CERT_CB macro isn't defined for LibreSSL, matching
        the (admittedly bogus) heuristic in the Perl backend.
        """
        return not pypg.check_pg_config("#define HAVE_SSL_CTX_SET_CERT_CB 1")

    @staticmethod
    def _copy_files(orig_glob: str, dest: pathlib.Path) -> None:
        """Copy files matching a glob (relative to the ssl/ dir) into dest."""
        for src in glob.glob(str(_SSL_FILES_DIR / orig_glob)):
            shutil.copyfile(src, dest / os.path.basename(src))


class SSLServer:
    """Mirror of SSL::Server for the OpenSSL backend.

    Wraps a PostgresServer (from the create_pg fixture) and provides the
    configure_test_server_for_ssl / switch_server_cert / sslkey helpers the
    ported SSL tests use.
    """

    def __init__(self, key_tempdir: pathlib.Path):
        self._backend = OpenSSLBackend(key_tempdir)

    def sslkey(self, keyfile: str) -> str:
        """Return an ' sslkey=<path>' connstr fragment for keyfile."""
        return self._backend.get_sslkey(keyfile)

    def ssl_library(self) -> str:
        """Return the SSL backend library name."""
        return self._backend.get_library()

    def is_libressl(self) -> bool:
        """Return True if the SSL backend is LibreSSL."""
        return self._backend.library_is_libressl()

    def configure_test_server_for_ssl(
        self, node, serverhost, servercidr, authmethod, **params
    ):
        """Configure node for SSL connections.

        Creates the trustdb/certdb/... databases and ssltestuser/... users,
        optionally setting passwords (password + password_enc), creating
        extensions, then enables SSL logging, installs the cert fixtures,
        restarts to pick up listen_addresses, and writes pg_hba/pg_ident for
        SSL, exactly as SSL::Server->configure_test_server_for_ssl does.
        """
        pgdata = pathlib.Path(node.datadir)

        for user in _USERS:
            node.psql("postgres", "-c", "CREATE USER " + user)
        for db in _DATABASES:
            node.psql("postgres", "-c", "CREATE DATABASE " + db)

        self._set_passwords(node, params)
        self._create_extensions(node, params)

        node.append_conf(
            "fsync=off\n"
            "log_connections=all\n"
            "log_hostname=on\n"
            "listen_addresses='{}'\n"
            "log_statement=all".format(serverhost)
        )
        node.append_conf("include 'sslconfig.conf'")

        # SSL configuration is appended here by switch_server_cert.
        (pgdata / "sslconfig.conf").write_text("", encoding="utf-8")

        self._backend.init(pgdata)

        # Restart to load the new listen_addresses.
        node.restart()

        # pg_hba must change after restart because hostssl requires ssl=on.
        self._configure_hba_for_ssl(node, servercidr, authmethod)

    def switch_server_cert(self, node, **params):
        """Rewrite sslconfig.conf to use the given cert/key/CA/CRL set.

        Mirrors SSL::Server->switch_server_cert: clears sslconfig.conf, writes
        ssl=on plus the backend cert selection, exercises ssl_groups and
        ssl_tls13_ciphers syntax, optionally sets the passphrase command (and
        its reload flag), then restarts unless restart='no'.
        """
        pgdata = pathlib.Path(node.datadir)
        (pgdata / "sslconfig.conf").unlink()
        node.append_conf("ssl=on", "sslconfig.conf")
        node.append_conf(self._backend.set_server_cert(params), "sslconfig.conf")
        node.append_conf("ssl_groups=prime256v1:secp521r1", "sslconfig.conf")
        node.append_conf(
            "ssl_tls13_ciphers=TLS_AES_256_GCM_SHA384:TLS_AES_128_GCM_SHA256",
            "sslconfig.conf",
        )
        if "passphrase_cmd" in params:
            node.append_conf(
                "ssl_passphrase_command='{}'".format(params["passphrase_cmd"]),
                "sslconfig.conf",
            )
        if "passphrase_cmd_reload" in params:
            node.append_conf(
                "ssl_passphrase_command_supports_reload='{}'".format(
                    params["passphrase_cmd_reload"]
                ),
                "sslconfig.conf",
            )
        if params.get("restart") == "no":
            return
        node.restart()

    @staticmethod
    def _set_passwords(node, params):
        """Set per-user passwords when configure params request them."""
        if "password" not in params:
            return
        if "password_enc" not in params:
            raise ValueError("password_enc must be set when password is set")
        password = params["password"]
        enc = params["password_enc"]
        node.psql(
            "postgres",
            "-c",
            "SET password_encryption='{}'; "
            "ALTER USER ssltestuser PASSWORD '{}';".format(enc, password),
        )
        node.psql(
            "postgres",
            "-c",
            "SET password_encryption='md5'; "
            "ALTER USER md5testuser PASSWORD '{}';".format(password),
        )
        node.psql(
            "postgres",
            "-c",
            "SET password_encryption='{}'; "
            "ALTER USER anotheruser PASSWORD '{}';".format(enc, password),
        )

    @staticmethod
    def _create_extensions(node, params):
        """Create requested extensions in every test database."""
        for extension in params.get("extensions", []):
            for db in _DATABASES:
                node.psql(db, "-c", "CREATE EXTENSION {} CASCADE;".format(extension))

    @staticmethod
    def _configure_hba_for_ssl(node, servercidr, authmethod):
        """Write the SSL pg_hba.conf and the DN/CN ident maps."""
        pgdata = pathlib.Path(node.datadir)
        (pgdata / "pg_hba.conf").unlink()
        node.append_conf(
            "# TYPE  DATABASE      USER            ADDRESS       METHOD"
            "         OPTIONS\n"
            "hostssl trustdb       md5testuser     {cidr}   md5\n"
            "hostssl trustdb       all             {cidr}   {auth}\n"
            "hostssl verifydb      ssltestuser     {cidr}   {auth}"
            "    clientcert=verify-full\n"
            "hostssl verifydb      anotheruser     {cidr}   {auth}"
            "    clientcert=verify-full\n"
            "hostssl verifydb      yetanotheruser  {cidr}   {auth}"
            "    clientcert=verify-ca\n"
            "hostssl certdb        all             {cidr}   cert\n"
            "hostssl certdb_dn     all   {cidr}   cert clientname=DN map=dn\n"
            "hostssl certdb_dn_re  all   {cidr}   cert clientname=DN map=dnre\n"
            "hostssl certdb_cn     all   {cidr}   cert clientname=CN map=cn".format(
                cidr=servercidr, auth=authmethod
            ),
            "pg_hba.conf",
        )
        (pgdata / "pg_ident.conf").unlink()
        node.append_conf(
            "# MAPNAME SYSTEM-USERNAME"
            "                                         PG-USERNAME\n"
            'dn        "CN=ssltestuser-dn,OU=Testing,OU=Engineering,O=PGDG"'
            "    ssltestuser\n"
            'dnre      "/^.*OU=Testing,.*$"                                   '
            "ssltestuser\n"
            "cn        ssltestuser-dn"
            "                                          ssltestuser",
            "pg_ident.conf",
        )


def stat_is_world_readable(path) -> bool:
    """Return True if path is group- or world-readable (perms & 0o066)."""
    mode = stat.S_IMODE(os.stat(path).st_mode)
    return bool(mode & 0o066)


def ssl_file_path(name: str) -> pathlib.Path:
    """Return the path to a fixture under the ssl/ directory (e.g. client.key)."""
    return _SSL_FILES_DIR / name

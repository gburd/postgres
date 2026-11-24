# Copyright (c) 2025-2026, PostgreSQL Global Development Group

"""Port of src/test/ldap/t/003_ldap_connection_param_lookup.pl.

Tests connection-parameter lookup through an LDAP-backed pg_service.conf,
covering combinations of the service name, the PGSERVICE/PGSERVICEFILE/
PGSYSCONFDIR environment variables, and a default pg_service.conf in
PGSYSCONFDIR. The service entry stores an LDAP URL whose directory lookup
returns the running server's host and port.
"""

import contextlib
import os
import shutil

from pypg.util import append_to_file


@contextlib.contextmanager
def _env(**overrides):
    """Temporarily set/unset environment variables (mirrors Perl ``local``).

    A value of None removes the variable for the duration of the block.
    """
    saved = {key: os.environ.get(key) for key in overrides}
    try:
        for key, value in overrides.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        yield
    finally:
        for key, value in saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def test_003_ldap_connection_param_lookup(create_pg, ldap_server, tmp_path):
    """Service-name/file lookups resolve a connection via an LDAP service entry."""
    dummy_node = create_pg("dummy_node", start=False)

    node = create_pg("node", start=False)
    node.start()

    ldap_rootpw = "secret"
    ldap = ldap_server(ldap_rootpw, "anonymous")  # use anonymous auth
    ldap_dir = os.path.dirname(__file__)
    ldap.ldapadd_file(os.path.join(ldap_dir, "..", "authdata.ldif"))
    ldap.ldapsetpw("uid=test1,dc=example,dc=net", "secret1")
    ldap.ldapsetpw("uid=test2,dc=example,dc=net", "secret2")

    td = tmp_path

    # Create ldap file based on postgres connection info.
    ldif_valid = td / "connection_params.ldif"
    append_to_file(
        ldif_valid,
        "\n"
        "version:1\n"
        "dn:cn=mydatabase,dc=example,dc=net\n"
        "changetype:add\n"
        "objectclass:top\n"
        "objectclass:device\n"
        "cn:mydatabase\n"
        "description:host=" + str(node.host) + "\n"
        "description:port=" + str(node.port) + "\n",
    )
    ldap.ldapadd_file(ldif_valid)

    (ldap_port,) = ldap.prop("port")

    with _env(LDAPTLS_REQCERT="never"):
        _run_service_tests(dummy_node, td, ldap_port)

    node.teardown_node()


def _run_service_tests(dummy_node, td, ldap_port):
    """Create the service files and run all service lookup scenarios."""
    # File that includes a valid service name, using a decomposed connection
    # string for its contents (an LDAP URL).
    srvfile_valid = td / "pg_service_valid.conf"
    append_to_file(
        srvfile_valid,
        "\n[my_srv]\n"
        "ldap://localhost:{}/dc=example,dc=net?description?one?"
        "(cn=mydatabase)\n".format(ldap_port),
    )

    # Empty file, used as default PGSERVICEFILE so no home-directory lookup is
    # attempted.
    srvfile_empty = td / "pg_service_empty.conf"
    append_to_file(srvfile_empty, "")

    # Missing service file.
    srvfile_missing = td / "pg_service_missing.conf"

    # Set the fallback service-file lookup directory (PGSYSCONFDIR) to this
    # test's temp dir, and force PGSERVICEFILE to a default so the test never
    # looks at a home directory.
    with _env(PGSYSCONFDIR=str(td), PGSERVICEFILE=str(srvfile_empty)):
        _check_valid_service_file(dummy_node, srvfile_valid)
        _check_missing_service_file(dummy_node, srvfile_missing)
        _check_default_service_file(dummy_node, srvfile_valid, td)


def _check_valid_service_file(dummy_node, srvfile_valid):
    """Combinations of service name and a valid PGSERVICEFILE."""
    with _env(PGSERVICEFILE=str(srvfile_valid)):
        dummy_node.connect_ok(
            "service=my_srv",
            'connection with correct "service" string and PGSERVICEFILE',
            sql="SELECT 'connect1_1'",
            expected_stdout="connect1_1",
        )
        dummy_node.connect_ok(
            "postgres://?service=my_srv",
            'connection with correct "service" URI and PGSERVICEFILE',
            sql="SELECT 'connect1_2'",
            expected_stdout="connect1_2",
        )
        dummy_node.connect_fails(
            "service=undefined-service",
            'connection with incorrect "service" string and PGSERVICEFILE',
            expected_stderr=r'definition of service "undefined-service" not found',
        )
        with _env(PGSERVICE="my_srv"):
            dummy_node.connect_ok(
                "",
                "connection with correct PGSERVICE and PGSERVICEFILE",
                sql="SELECT 'connect1_3'",
                expected_stdout="connect1_3",
            )
        with _env(PGSERVICE="undefined-service"):
            # connect_fails ignores expected_stdout (as the Perl harness does),
            # so this only asserts a non-zero exit.
            dummy_node.connect_fails(
                "",
                "connection with incorrect PGSERVICE and PGSERVICEFILE",
            )


def _check_missing_service_file(dummy_node, srvfile_missing):
    """Case of an incorrect (missing) service file."""
    with _env(PGSERVICEFILE=str(srvfile_missing)):
        dummy_node.connect_fails(
            "service=my_srv",
            'connection with correct "service" string and incorrect PGSERVICEFILE',
            expected_stderr=r'service file ".*pg_service_missing.conf" not found',
        )


def _check_default_service_file(dummy_node, srvfile_valid, td):
    """Case of a service file named pg_service.conf in PGSYSCONFDIR."""
    # Create copy of valid file as the default pg_service.conf.
    srvfile_default = td / "pg_service.conf"
    shutil.copyfile(srvfile_valid, srvfile_default)
    try:
        dummy_node.connect_ok(
            "service=my_srv",
            'connection with correct "service" string and pg_service.conf',
            sql="SELECT 'connect2_1'",
            expected_stdout="connect2_1",
        )
        dummy_node.connect_ok(
            "postgres://?service=my_srv",
            'connection with correct "service" URI and default pg_service.conf',
            sql="SELECT 'connect2_2'",
            expected_stdout="connect2_2",
        )
        dummy_node.connect_fails(
            "service=undefined-service",
            'connection with incorrect "service" string and default pg_service.conf',
            expected_stderr=r'definition of service "undefined-service" not found',
        )
        with _env(PGSERVICE="my_srv"):
            dummy_node.connect_ok(
                "",
                "connection with correct PGSERVICE and default pg_service.conf",
                sql="SELECT 'connect2_3'",
                expected_stdout="connect2_3",
            )
        with _env(PGSERVICE="undefined-service"):
            # connect_fails ignores expected_stdout (matching the Perl harness).
            dummy_node.connect_fails(
                "",
                "connection with incorrect PGSERVICE and default pg_service.conf",
            )
    finally:
        srvfile_default.unlink()

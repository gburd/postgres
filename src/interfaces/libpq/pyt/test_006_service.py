# Copyright (c) 2025-2026, PostgreSQL Global Development Group

"""Port of src/interfaces/libpq/t/006_service.pl.

Tests connection scenarios driven by the service name and the service file,
covering the "service"/"servicefile" connection options and their environment
variables (PGSERVICE, PGSERVICEFILE, PGSYSCONFDIR), including nested-directive
rejection and the precedence of the servicefile option over PGSERVICEFILE.

A real server ("node") provides the working host/port, written into a service
file. A second, never-started "dummy_node" is used for the connection attempts:
that way the environment variables used for the connection do not interfere with
the connection attempts, and the service file's contents are exercised instead.
"""

import os
import re
import shutil


def _append(path, text):
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(text)


def _connect_ok(dummy_node, env_overrides, connstr, name, sql, expected_stdout):
    """connect_ok with PGSERVICE*/PGSYSCONFDIR temporarily applied to os.environ."""
    saved = {k: os.environ.get(k) for k in env_overrides}
    try:
        _apply_env(env_overrides)
        dummy_node.connect_ok(connstr, name, sql=sql, expected_stdout=expected_stdout)
    finally:
        _restore_env(saved)


def _connect_fails(dummy_node, env_overrides, connstr, name, expected_stderr):
    """connect_fails with PGSERVICE*/PGSYSCONFDIR temporarily applied."""
    saved = {k: os.environ.get(k) for k in env_overrides}
    try:
        _apply_env(env_overrides)
        dummy_node.connect_fails(connstr, name, expected_stderr=expected_stderr)
    finally:
        _restore_env(saved)


def _apply_env(env_overrides):
    for key, value in env_overrides.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


def _restore_env(saved):
    for key, value in saved.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


def _build_service_files(td, node):
    """Create the set of service files used by the tests; return their paths."""
    # File that includes a valid service name, using a decomposed connection
    # string for its contents, split on spaces.
    srvfile_valid = os.path.join(td, "pg_service_valid.conf")
    _append(srvfile_valid, "[my_srv]\n")
    for param in re.split(r"\s+", node.connstr()):
        if param:
            _append(srvfile_valid, param + "\n")

    # File defined with no contents, used as default value for PGSERVICEFILE so
    # that no lookup is attempted in the user's home directory.
    srvfile_empty = os.path.join(td, "pg_service_empty.conf")
    _append(srvfile_empty, "")

    # Default service file in PGSYSCONFDIR.
    srvfile_default = os.path.join(td, "pg_service.conf")

    # Missing service file.
    srvfile_missing = os.path.join(td, "pg_service_missing.conf")

    # Service file with nested "service" defined.
    srvfile_nested = os.path.join(td, "pg_service_nested.conf")
    shutil.copy(srvfile_valid, srvfile_nested)
    _append(srvfile_nested, "service=invalid_srv\n")

    # Service file with nested "servicefile" defined.
    srvfile_nested_2 = os.path.join(td, "pg_service_nested_2.conf")
    shutil.copy(srvfile_valid, srvfile_nested_2)
    _append(srvfile_nested_2, "servicefile=" + srvfile_default + "\n")

    return {
        "valid": srvfile_valid,
        "empty": srvfile_empty,
        "default": srvfile_default,
        "missing": srvfile_missing,
        "nested": srvfile_nested,
        "nested_2": srvfile_nested_2,
    }


def _test_valid_service_file(dummy_node, td, files):
    """Checks combinations of service name and a valid service file."""
    base = {"PGSYSCONFDIR": td, "PGSERVICEFILE": files["valid"]}
    _connect_ok(
        dummy_node,
        base,
        "service=my_srv",
        'connection with correct "service" string and PGSERVICEFILE',
        "SELECT 'connect1_1'",
        r"connect1_1",
    )
    _connect_ok(
        dummy_node,
        base,
        "postgres://?service=my_srv",
        'connection with correct "service" URI and PGSERVICEFILE',
        "SELECT 'connect1_2'",
        r"connect1_2",
    )
    _connect_fails(
        dummy_node,
        base,
        "service=undefined-service",
        'connection with incorrect "service" string and PGSERVICEFILE',
        r'definition of service "undefined-service" not found',
    )
    _connect_ok(
        dummy_node,
        {**base, "PGSERVICE": "my_srv"},
        "",
        "connection with correct PGSERVICE and PGSERVICEFILE",
        "SELECT 'connect1_3'",
        r"connect1_3",
    )
    # The Perl original uses expected_stdout here even though the message goes
    # to stderr; connect_fails only asserts the non-zero exit in that case, so
    # match the same observable behaviour by checking the failure alone.
    _connect_fails(
        dummy_node,
        {**base, "PGSERVICE": "undefined-service"},
        "",
        "connection with incorrect PGSERVICE and PGSERVICEFILE",
        None,
    )


def _test_missing_service_file(dummy_node, td, files):
    """Checks case of an incorrect (missing) service file."""
    _connect_fails(
        dummy_node,
        {"PGSYSCONFDIR": td, "PGSERVICEFILE": files["missing"]},
        "service=my_srv",
        'connection with correct "service" string and incorrect PGSERVICEFILE',
        r'service file ".*pg_service_missing.conf" not found',
    )


def _test_default_service_file(dummy_node, td, files):
    """Checks the service file named "pg_service.conf" in PGSYSCONFDIR."""
    srvfile_default = os.path.join(td, "pg_service.conf")
    shutil.copy(files["valid"], srvfile_default)
    base = {"PGSYSCONFDIR": td, "PGSERVICEFILE": files["empty"]}

    _connect_ok(
        dummy_node,
        base,
        "service=my_srv",
        'connection with correct "service" string and pg_service.conf',
        "SELECT 'connect2_1'",
        r"connect2_1",
    )
    _connect_ok(
        dummy_node,
        base,
        "postgres://?service=my_srv",
        'connection with correct "service" URI and default pg_service.conf',
        "SELECT 'connect2_2'",
        r"connect2_2",
    )
    _connect_fails(
        dummy_node,
        base,
        "service=undefined-service",
        'connection with incorrect "service" string and default pg_service.conf',
        r'definition of service "undefined-service" not found',
    )
    _connect_ok(
        dummy_node,
        {**base, "PGSERVICE": "my_srv"},
        "",
        "connection with correct PGSERVICE and default pg_service.conf",
        "SELECT 'connect2_3'",
        r"connect2_3",
    )
    _connect_ok(
        dummy_node,
        base,
        "service=my_srv servicefile='{}'".format(files["empty"]),
        "SERVICEFILE updated when service is found in default pg_service.conf",
        r"\echo :SERVICEFILE",
        r"^{}$".format(re.escape(srvfile_default)),
    )
    _connect_fails(
        dummy_node,
        {**base, "PGSERVICE": "undefined-service"},
        "",
        "connection with incorrect PGSERVICE and default pg_service.conf",
        None,
    )
    # Remove default pg_service.conf.
    os.unlink(srvfile_default)


def _test_nested_service_file(dummy_node, td, files):
    """Checks nested service file contents are rejected."""
    _connect_fails(
        dummy_node,
        {"PGSYSCONFDIR": td, "PGSERVICEFILE": files["nested"]},
        "service=my_srv",
        'connection with "service" in nested service file',
        r'nested "service" specifications not supported in service file',
    )
    _connect_fails(
        dummy_node,
        {"PGSYSCONFDIR": td, "PGSERVICEFILE": files["nested_2"]},
        "service=my_srv",
        'connection with "servicefile" in nested service file',
        r'nested "servicefile" specifications not supported in service file',
    )


def _test_servicefile_option(dummy_node, td, files):
    """Checks that the "servicefile" option works as expected."""
    base = {"PGSYSCONFDIR": td, "PGSERVICEFILE": files["empty"]}
    srvfile = files["valid"]

    # Encode slashes and backslashes for the URI form.
    encoded_srvfile = re.sub(
        r"[\\/]", lambda m: "%2F" if m.group(0) == "/" else "%5C", srvfile
    )
    # Additionally encode a colon (Windows servicefile paths).
    encoded_srvfile = encoded_srvfile.replace(":", "%3A")

    _connect_ok(
        dummy_node,
        base,
        "service=my_srv servicefile='{}'".format(srvfile),
        "connection with valid servicefile in connection string",
        "SELECT 'connect3_1'",
        r"connect3_1",
    )
    _connect_ok(
        dummy_node,
        base,
        "postgresql:///?service=my_srv&servicefile=" + encoded_srvfile,
        "connection with valid servicefile in URI",
        "SELECT 'connect3_2'",
        r"connect3_2",
    )
    _connect_ok(
        dummy_node,
        {**base, "PGSERVICE": "my_srv"},
        "servicefile='{}'".format(srvfile),
        "connection with PGSERVICE and servicefile in connection string",
        "SELECT 'connect3_3'",
        r"connect3_3",
    )
    _connect_ok(
        dummy_node,
        {**base, "PGSERVICE": "my_srv"},
        "postgresql://?servicefile=" + encoded_srvfile,
        "connection with PGSERVICE and servicefile in URI",
        "SELECT 'connect3_4'",
        r"connect3_4",
    )


def _test_servicefile_priority(dummy_node, td, files):
    """servicefile option takes priority over the PGSERVICEFILE env var."""
    srvfile = files["valid"]
    _connect_fails(
        dummy_node,
        {"PGSYSCONFDIR": td, "PGSERVICEFILE": "non-existent-file.conf"},
        "service=my_srv",
        "connection with invalid PGSERVICEFILE",
        r'service file "non-existent-file\.conf" not found',
    )
    _connect_ok(
        dummy_node,
        {"PGSYSCONFDIR": td, "PGSERVICEFILE": "non-existent-file.conf"},
        "service=my_srv servicefile='{}'".format(srvfile),
        "connection with both servicefile and PGSERVICEFILE",
        "SELECT 'connect4_1'",
        r"connect4_1",
    )


def test_006_service(create_pg, tmp_path):
    """Service name and service file connection scenarios."""
    node = create_pg("node")

    # Set up a dummy node used for the connection tests, but do not start it.
    # This ensures the environment variables used for the connection do not
    # interfere with the connection attempts, and the service file's contents
    # are used.
    dummy_node = create_pg("dummy_node", start=False)

    td = str(tmp_path)
    files = _build_service_files(td, node)

    # PGSYSCONFDIR is used if the service file defined in PGSERVICEFILE cannot
    # be found, or when a service file is found but not the service name.
    # PGSERVICEFILE is forced to a default location so this test never looks at
    # a home directory.
    saved = {k: os.environ.get(k) for k in ("PGSYSCONFDIR", "PGSERVICEFILE")}
    os.environ["PGSYSCONFDIR"] = td
    os.environ["PGSERVICEFILE"] = files["empty"]
    try:
        _test_valid_service_file(dummy_node, td, files)
        _test_missing_service_file(dummy_node, td, files)
        _test_default_service_file(dummy_node, td, files)
        _test_nested_service_file(dummy_node, td, files)
        _test_servicefile_option(dummy_node, td, files)
        _test_servicefile_priority(dummy_node, td, files)
    finally:
        _restore_env(saved)

    node.teardown_node()

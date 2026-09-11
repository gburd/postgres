# Copyright (c) 2026, PostgreSQL Global Development Group

"""Port of src/bin/pg_upgrade/t/008_extension_control_path.pl.

Test pg_upgrade with the extension_control_path GUC active: a C extension is
installed from a custom directory layout (its .control/.sql under
``extension/`` and its .so under ``lib/``), discovered via
``extension_control_path`` and ``dynamic_library_path``.  The extension must
keep working after the cluster is upgraded into a new cluster configured with
the same paths.

This is a same-version port: ``oldinstall`` is unset, so both clusters are the
current build.  ``TEST_EXT_LIB`` (the built test_ext shared module) is supplied
by the meson pytest env block; if it is genuinely unavailable the test skips
with a precise reason.
"""

import os
import shutil

import pytest


def _create_extension_files(ext_name, ext_dir):
    """Write the extension's .control and --1.0.sql into ext_dir/extension/.

    module_pathname uses the ``$libdir/`` prefix to mimic the majority of
    extensions, mirroring the Perl create_extension_files helper.
    """
    control_path = os.path.join(ext_dir, "extension", ext_name + ".control")
    with open(control_path, "w", encoding="utf-8") as cf:
        cf.write(
            "comment = 'Test C extension for pg_upgrade + extension_control_path'\n"
        )
        cf.write("default_version = '1.0'\n")
        cf.write("module_pathname = '$libdir/{}'\n".format(ext_name))
        cf.write("relocatable = true\n")

    sql_path = os.path.join(ext_dir, "extension", "{}--1.0.sql".format(ext_name))
    with open(sql_path, "w", encoding="utf-8") as sqlf:
        sqlf.write("/* {}--1.0.sql */\n".format(ext_name))
        sqlf.write(
            "-- complain if script is sourced in psql, rather than via "
            "CREATE EXTENSION\n"
        )
        sqlf.write(
            '\\echo Use "CREATE EXTENSION {}" to load this file. '
            "\\quit\n".format(ext_name)
        )
        sqlf.write("CREATE FUNCTION test_ext()\n")
        sqlf.write("RETURNS void AS 'MODULE_PATHNAME'\n")
        sqlf.write("LANGUAGE C;\n")


def _control_path_conf(ext_path, ext_lib_path):
    """Return the postgresql.conf snippet wiring up the extension's paths."""
    sep = ":"  # POSIX path separator ($windows_os is false here)
    return (
        "\nextension_control_path = '$system{sep}{ext}'\n"
        "dynamic_library_path = '$libdir{sep}{lib}'\n".format(
            sep=sep, ext=ext_path, lib=ext_lib_path
        )
    )


def _assert_extension_works(node, when):
    """Assert SELECT test_ext() succeeds and emits its NOTICE."""
    result = node.psql_capture("SELECT test_ext()")
    assert result.rc == 0, "extension works {} upgrade".format(when)
    assert "NOTICE:  running successful" in result.stderr, "extension working"


def test_008_extension_control_path(create_pg, pg_bin, tmp_path, monkeypatch):
    """pg_upgrade preserves an extension installed via extension_control_path."""
    # Make sure the extension's .so path is provided by the meson env block.
    ext_lib_so = os.environ.get("TEST_EXT_LIB")
    if not ext_lib_so or not os.path.exists(ext_lib_so):
        pytest.skip(
            "TEST_EXT_LIB is not set to a built test_ext shared module "
            "(needed by the extension_control_path test)"
        )

    # Create the custom extension directory layout:
    #   ext_dir/extension/  -- .control and .sql files
    #   ext_dir/lib/        -- .so file
    ext_dir = str(tmp_path / "ext")
    os.makedirs(os.path.join(ext_dir, "extension"))
    os.makedirs(os.path.join(ext_dir, "lib"))
    ext_lib = os.path.join(ext_dir, "lib")

    # Copy the .so file into the lib/ subdirectory.
    shutil.copy(ext_lib_so, ext_lib)

    _create_extension_files("test_ext", ext_dir)

    extension_control_path_conf = _control_path_conf(ext_dir, ext_lib)

    old = create_pg("old", start=False)
    # Configure extension_control_path so the .control file is found in our
    # extension/ directory, and dynamic_library_path so the .so is found in
    # lib/.
    old.append_conf(extension_control_path_conf)
    old.start()

    # CREATE EXTENSION 'test_ext'
    old.safe_psql("CREATE EXTENSION test_ext")

    # Verify the extension works before the upgrade.
    _assert_extension_works(old, "before")

    old.stop()

    new = create_pg("new", start=False)
    # Pre-configure the new cluster with dynamic_library_path and
    # extension_control_path before running pg_upgrade.
    new.append_conf(extension_control_path_conf)

    # In a VPATH build, we'll be started in the source directory, but we want to
    # run pg_upgrade in the build directory so that any files generated finish
    # in it, like delete_old_cluster.{sh,bat}.
    monkeypatch.chdir(tmp_path)

    pg_bin.command_ok(
        [
            "pg_upgrade",
            "--no-sync",
            "--old-datadir",
            str(old.datadir),
            "--new-datadir",
            str(new.datadir),
            "--old-bindir",
            old.config_data("--bindir"),
            "--new-bindir",
            new.config_data("--bindir"),
            "--socketdir",
            str(new.host),
            "--old-port",
            str(old.port),
            "--new-port",
            str(new.port),
            "--copy",
        ],
        "pg_upgrade succeeds with extension installed via extension_control_path",
    )

    new.start()

    # Verify the extension still works after the upgrade.
    _assert_extension_works(new, "after")

    new.stop()

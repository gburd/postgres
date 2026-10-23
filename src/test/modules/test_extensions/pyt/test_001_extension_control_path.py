# Copyright (c) 2024-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long
"""Port of src/test/modules/test_extensions/t/001_extension_control_path.pl.

extension_control_path lets extensions live outside $system: custom .control/
.sql files placed in mapped directories are found by CREATE EXTENSION and shown
(with their location) in pg_available_extensions[_versions], the location is
hidden from unprivileged users, $system extensions still resolve, and a
nonexistent extension fails cleanly.
"""

import os
import re
import tempfile


def _create_extension(ext_name, ext_dir, directory=None):
    control_file = "{}/extension/{}.control".format(ext_dir, ext_name)
    if directory is not None:
        sql_file = "{}/{}/{}--1.0.sql".format(ext_dir, directory, ext_name)
    else:
        sql_file = "{}/extension/{}--1.0.sql".format(ext_dir, ext_name)
    with open(control_file, "w", encoding="utf-8") as cf:
        cf.write("comment = 'Test extension_control_path'\n")
        cf.write("default_version = '1.0'\n")
        cf.write("relocatable = true\n")
        if directory is not None:
            cf.write("directory = {}".format(directory))
    with open(sql_file, "w", encoding="utf-8") as sqlf:
        sqlf.write("/* {} */\n".format(sql_file))
        sqlf.write(
            "-- complain if script is sourced in psql, rather than via "
            "CREATE EXTENSION\n"
        )
        sqlf.write(
            '\\echo Use "CREATE EXTENSION {}" to load this file. \\quit\n'.format(
                ext_name
            )
        )


def test_001_extension_control_path(create_pg):
    """Custom extension_control_path directories are honored and access-gated."""
    node = create_pg("node", auth_extra=["--create-role", "user01"], start=False)
    ext_dir = tempfile.mkdtemp(prefix="ecp1_")
    os.makedirs("{}/extension".format(ext_dir))
    ext_dir2 = tempfile.mkdtemp(prefix="ecp2_")
    os.makedirs("{}/extension".format(ext_dir2))
    ext_name = "test_custom_ext_paths"
    _create_extension(ext_name, ext_dir)
    _create_extension(ext_name, ext_dir2)
    ext_name2 = "test_custom_ext_paths_using_directory"
    os.makedirs("{}/{}".format(ext_dir, ext_name2))
    _create_extension(ext_name2, ext_dir, ext_name2)
    sep = ":"
    node.append_conf(
        "\nextension_control_path = '$system{s}{d1}{s}{d2}'\n".format(
            s=sep, d1=ext_dir, d2=ext_dir2
        )
    )
    node.start()
    user = "user01"
    node.safe_psql("CREATE USER {}".format(user))
    ecp = node.safe_psql("show extension_control_path;")
    assert ecp == "$system{s}{d1}{s}{d2}".format(
        s=sep, d1=ext_dir, d2=ext_dir2
    ), "custom extension control directory path configured"
    node.safe_psql("CREATE EXTENSION {}".format(ext_name))
    node.safe_psql("CREATE EXTENSION {}".format(ext_name2))
    assert node.safe_psql(
        "select * from pg_available_extensions where name = '{}'".format(ext_name)
    ) == "test_custom_ext_paths|1.0|1.0|{}/extension|Test extension_control_path".format(
        ext_dir
    ), "extension is shown correctly in pg_available_extensions"
    assert node.safe_psql(
        "select * from pg_available_extension_versions where name = '{}'".format(
            ext_name
        )
    ) == "test_custom_ext_paths|1.0|t|t|f|t|||{}/extension|Test extension_control_path".format(
        ext_dir
    ), "extension is shown correctly in pg_available_extension_versions"
    assert node.safe_psql(
        "select * from pg_available_extensions where name = '{}'".format(ext_name2)
    ) == "test_custom_ext_paths_using_directory|1.0|1.0|{}/extension|Test extension_control_path".format(
        ext_dir
    ), "extension is shown correctly in pg_available_extensions"
    assert node.safe_psql(
        "select * from pg_available_extension_versions where name = '{}'".format(
            ext_name2
        )
    ) == "test_custom_ext_paths_using_directory|1.0|t|t|f|t|||{}/extension|Test extension_control_path".format(
        ext_dir
    ), "extension is shown correctly in pg_available_extension_versions"
    assert (
        node.psql_capture(
            "select location from pg_available_extensions where name = '{}'".format(
                ext_name2
            ),
            connstr=node.connstr("postgres") + " user=" + user,
        ).stdout
        == "<insufficient privilege>"
    ), (
        "extension location is hidden in pg_available_extensions for users with "
        "insufficient privilege"
    )
    assert (
        node.psql_capture(
            "select location from pg_available_extension_versions where name = '{}'".format(
                ext_name2
            ),
            connstr=node.connstr("postgres") + " user=" + user,
        ).stdout
        == "<insufficient privilege>"
    ), (
        "extension location is hidden in pg_available_extension_versions for "
        "users with insufficient privilege"
    )
    assert (
        node.safe_psql(
            "select count(*) > 0 as ok from pg_available_extensions where name = 'plpgsql'"
        )
        == "t"
    ), "$system extension is shown correctly in pg_available_extensions"
    assert (
        node.safe_psql(
            "set extension_control_path = ''; select location from "
            "pg_available_extensions where name = 'plpgsql'"
        )
        == "$system"
    ), (
        "$system location is shown correctly in pg_available_extensions with "
        "empty extension_control_path"
    )
    res = node.psql_capture("CREATE EXTENSION invalid")
    assert res.exit_code == 3, "error creating an extension that does not exist"
    assert re.search(r'ERROR:  extension "invalid" is not available', res.stderr)

# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/bin/pg_dump/t/010_dump_connstr.pl.

pg_dumpall/pg_dump/pg_restore against databases and roles whose names span the
full LATIN1 byte range, exercising connection-string handling for high-bit and
punctuation-laden identifiers, parallel dump/restore, and restoring a full
pg_dumpall script through psql using both environment variables and
command-line connection parameters.
"""

import os
import subprocess

# Source/destination bootstrap superusers (plain ASCII).
_SRC_SUPER = "regress_postgres"
_DST_SUPER = "boot"


def _ascii_string(from_char, to_char):
    """Return the bytes for code points from_char..to_char (cf. Utils)."""
    return bytes(range(from_char, to_char + 1))


def _build_names():
    """Construct the four LATIN1 db names and matching role names (bytes)."""
    # Skip ',' (pg_regress --create-role), [\n\r] (pg_dumpall), and many ASCII
    # letters to fit the tested characters into four names. '"x"' exercises a
    # quoted identifier.
    dbname1 = (
        b"regression"
        + _ascii_string(1, 9)
        + _ascii_string(11, 12)
        + _ascii_string(14, 33)
        + b'"x"'
        + _ascii_string(35, 43)
        + _ascii_string(45, 54)
    )
    dbname2 = (
        b"regression"
        + _ascii_string(55, 65)
        + _ascii_string(88, 99)
        + _ascii_string(120, 149)
    )
    dbname3 = b"regression" + _ascii_string(150, 202)
    dbname4 = b"regression" + _ascii_string(203, 255)
    dbnames = [dbname1, dbname2, dbname3, dbname4]
    usernames = [b"regress_" + d[len(b"regression") :] for d in dbnames]
    return dbnames, usernames


def _connstr_bytes(node, dbname):
    """Return a libpq connection string (bytes) for dbname (bytes).

    Mirrors PostgreSQL::Test::Cluster->connstr: backslashes and single quotes in
    the database name are escaped and the value is single-quoted.
    """
    escaped = dbname.replace(b"\\", b"\\\\").replace(b"'", b"\\'")
    prefix = "host={} port={} dbname=".format(node.host, node.port)
    return prefix.encode("latin-1") + b"'" + escaped + b"'"


def _latin1_env(node, **extra):
    """Connection env forcing C/LATIN1 byte handling for high-bit names."""
    env = dict(node.connenv)
    env["LC_ALL"] = "C"
    env["PGCLIENTENCODING"] = "LATIN1"
    env.update(extra)
    return env


def _config_auth_roles(node, super_user, roles):
    """Run pg_regress --config-auth creating the given roles (bytes-safe)."""
    pg_regress = os.environ["PG_REGRESS"]
    roles_arg = b",".join(roles)
    subprocess.run(
        [
            pg_regress,
            "--config-auth",
            str(node.datadir),
            "--user",
            super_user,
            "--create-role",
            roles_arg,
        ],
        env=_latin1_env(node),
        check=True,
    )


def _create_db_and_super(node, dbname, username):
    """createdb dbname and a superuser username, both owned by the src super."""
    env = {"LC_ALL": "C", "PGCLIENTENCODING": "LATIN1"}
    node.bin.command_ok(
        ["createdb", "--username", _SRC_SUPER, dbname], "createdb", extra_env=env
    )
    node.bin.command_ok(
        ["createuser", "--username", _SRC_SUPER, "--superuser", username],
        "createuser",
        extra_env=env,
    )


def _dumpall_roles_only(node, dbname, username, no_sync, msg, discard):
    """pg_dumpall --roles-only over a connstr/username (discarding output)."""
    cmd = ["pg_dumpall", "--roles-only"]
    if no_sync:
        cmd = ["pg_dumpall", "--no-sync", "--roles-only"]
    cmd += [
        "--file",
        discard,
        "--dbname",
        _connstr_bytes(node, dbname),
        "--username",
        username,
    ]
    node.bin.command_ok(
        cmd, msg, extra_env={"LC_ALL": "C", "PGCLIENTENCODING": "LATIN1"}
    )


def _restore_full_dump(create_pg, name, plain, restore_super):
    """Init a fresh LATIN1 node, create the restore super, return the node."""
    node = create_pg(
        name,
        start=False,
        extra=["--username", _DST_SUPER, "--locale", "C", "--encoding", "LATIN1"],
        auth_extra=["--user", _DST_SUPER, "--create-role", restore_super],
    )
    node.start()
    node.bin.command_ok(
        ["createuser", "--username", _DST_SUPER, "--superuser", restore_super],
        "createuser restore super",
        extra_env={"LC_ALL": "C", "PGCLIENTENCODING": "LATIN1"},
    )
    return node


def _parallel_dump_restore(node, dbname1, username1, dirfmt):
    """Parallel directory dump of dbname1 and parallel restore (with --create)."""
    env = {"LC_ALL": "C", "PGCLIENTENCODING": "LATIN1"}
    node.bin.command_ok(
        [
            "psql",
            "--username",
            _SRC_SUPER,
            "--dbname",
            _connstr_bytes(node, dbname1),
            "--no-psqlrc",
            "-c",
            "CREATE TABLE t0()",
        ],
        "make a table for the parallel worker to dump",
        extra_env=env,
    )
    node.bin.command_ok(
        [
            "pg_dump",
            "--format",
            "directory",
            "--no-sync",
            "--jobs",
            "2",
            "--file",
            dirfmt,
            "--username",
            username1,
            _connstr_bytes(node, dbname1),
        ],
        "parallel dump",
        extra_env=env,
    )
    node.bin.command_ok(
        ["dropdb", "--username", _SRC_SUPER, dbname1], "dropdb", extra_env=env
    )
    node.bin.command_ok(
        ["createdb", "--username", _SRC_SUPER, dbname1], "createdb", extra_env=env
    )
    node.bin.command_ok(
        [
            "pg_restore",
            "--verbose",
            "--dbname",
            "template1",
            "--jobs",
            "2",
            "--username",
            username1,
            dirfmt,
        ],
        "parallel restore",
        extra_env=env,
    )
    node.bin.command_ok(
        ["dropdb", "--username", _SRC_SUPER, dbname1], "dropdb", extra_env=env
    )
    node.bin.command_ok(
        [
            "pg_restore",
            "--create",
            "--verbose",
            "--dbname",
            "template1",
            "--jobs",
            "2",
            "--username",
            username1,
            dirfmt,
        ],
        "parallel restore with create",
        extra_env=env,
    )


def test_010_dump_connstr(create_pg, pg_bin):
    """Full-range LATIN1 db/role names round-trip through dump/restore."""
    dbnames, usernames = _build_names()
    dbname1 = dbnames[0]

    node = create_pg(
        "main",
        start=False,
        extra=["--username", _SRC_SUPER, "--locale", "C", "--encoding", "LATIN1"],
    )
    _config_auth_roles(node, _SRC_SUPER, usernames)
    node.start()

    backupdir = str(node.backup_dir)
    discard = backupdir + "/discard.sql"
    plain = backupdir + "/plain.sql"
    dirfmt = backupdir + "/dirfmt"

    for dbname, username in zip(dbnames, usernames):
        _create_db_and_super(node, dbname, username)

    # pg_dumpall --roles-only because it produces a short dump; cross dbname and
    # username so each long name is used as both a connection db and a user.
    _dumpall_roles_only(
        node, dbnames[0], usernames[3], False, "long ASCII name 1", discard
    )
    _dumpall_roles_only(
        node, dbnames[1], usernames[2], True, "long ASCII name 2", discard
    )
    _dumpall_roles_only(
        node, dbnames[2], usernames[1], True, "long ASCII name 3", discard
    )
    _dumpall_roles_only(
        node, dbnames[3], usernames[0], True, "long ASCII name 4", discard
    )
    node.bin.command_ok(
        [
            "pg_dumpall",
            "--no-sync",
            "--roles-only",
            "--username",
            _SRC_SUPER,
            "--dbname",
            "dbname=template1",
        ],
        "pg_dumpall --dbname accepts connection string",
        extra_env={"LC_ALL": "C", "PGCLIENTENCODING": "LATIN1"},
    )

    _parallel_dump_restore(node, dbname1, usernames[0], dirfmt)

    node.bin.command_ok(
        [
            "pg_dumpall",
            "--no-sync",
            "--file",
            plain,
            "--username",
            usernames[0],
        ],
        "take full dump",
        extra_env={"LC_ALL": "C", "PGCLIENTENCODING": "LATIN1"},
    )

    restore_super = "regress_a'b\\c=d\\ne\"f"

    # Restore through psql using environment variables for connection params.
    envar_node = _restore_full_dump(
        create_pg, "destination_envar", plain, restore_super
    )
    result = pg_bin.result(
        ["psql", "--no-psqlrc", "--file", plain],
        extra_env=_envar_restore_env(envar_node, restore_super),
    )
    assert (
        result.rc == 0
    ), "restore full dump using environment variables for connection parameters"
    assert result.stderr == "", "no dump errors"

    # Restore through psql using command-line connection params.
    cmdline_node = _restore_full_dump(
        create_pg, "destination_cmdline", plain, restore_super
    )
    result = pg_bin.result(
        [
            "psql",
            "--port",
            str(cmdline_node.port),
            "--username",
            restore_super,
            "--no-psqlrc",
            "--file",
            plain,
        ],
        extra_env=_cmdline_restore_env(cmdline_node),
    )
    assert (
        result.rc == 0
    ), "restore full dump with command-line options for connection parameters"
    assert result.stderr == "", "no dump errors"


def _envar_restore_env(node, restore_super):
    """Env restoring via PGPORT/PGUSER (no command-line connection params)."""
    return {
        "LC_ALL": "C",
        "PGCLIENTENCODING": "LATIN1",
        "PGHOST": str(node.host),
        "PGPORT": str(node.port),
        "PGUSER": restore_super,
        "PGDATABASE": "postgres",
    }


def _cmdline_restore_env(node):
    """Env for command-line restore: only the socket host is provided."""
    return {
        "LC_ALL": "C",
        "PGCLIENTENCODING": "LATIN1",
        "PGHOST": str(node.host),
        "PGDATABASE": "postgres",
    }

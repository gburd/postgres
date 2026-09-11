# Copyright (c) 2022-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long
"""Port of src/bin/pg_upgrade/t/002_pg_upgrade.pl.

The canonical same-version pg_upgrade test: set up an old cluster, populate it
with the full core regression suite, take a logical dump, pg_upgrade it into a
fresh new cluster, dump again, and verify the two dumps are identical after
filtering.

This is the default ``make check`` path, where neither ``oldinstall`` nor
``olddump`` is set in the environment: the old and new clusters are both built
from this tree (v19+).  Every cross-version branch in the Perl original gated on
``$oldnode->pg_version`` resolves to the v19 value, so:

* ``pg_version >= 11`` -> custom initdb opts (--wal-segsize, --allow-group-access).
* ``pg_version >= 17devel`` -> builtin C.UTF-8 locale provider for the old node.
* ``pg_version < 18``/``< 12`` -> false, so no -k and no --extra-float-digits.
* the ``adjust_database_contents`` / probin-rewrite blocks are gated on
  ``oldinstall`` being defined and are therefore skipped.
* ``adjust_old_dumpfile`` / ``adjust_new_dumpfile`` reduce to their unconditional
  parts (CRLF normalization, version-comment removal, stats-version masking and
  blank-line suppression); see _filter_dump.

The dump/restore round-trip section is gated on ``regress_dump_restore`` being
listed in ``PG_TEST_EXTRA`` (as in the Perl original) and is skipped otherwise.
"""

import os
import re
import shutil

import pypg

_MODE = os.environ.get("PG_TEST_PG_UPGRADE_MODE") or "--copy"

# Same-version port: the old node is the current build (v19+), so these are the
# values the Perl original computes for pg_version >= 17devel.
_ORIGINAL_ENCODING = 6
_ORIGINAL_PROVIDER = "b"
_ORIGINAL_DATCOLLATE = "C"
_ORIGINAL_DATCTYPE = "C"
_ORIGINAL_DATLOCALE = "C.UTF-8"
_EXPECTED_LOCALE_ROW = "{}|{}|{}|{}|{}".format(
    _ORIGINAL_ENCODING,
    _ORIGINAL_PROVIDER,
    _ORIGINAL_DATCOLLATE,
    _ORIGINAL_DATCTYPE,
    _ORIGINAL_DATLOCALE,
)

# To increase coverage of non-standard segment size and group access without
# increasing test runtime, run with a custom setting (--wal-segsize,
# --allow-group-access added in v11).
_CUSTOM_OPTS = ["--wal-segsize", "1", "--allow-group-access"]


def _generate_db(old, prefix, from_char, to_char, suffix):
    """Create a database whose name spans a range of ASCII bytes.

    Mirrors the Perl generate_db: BEL, LF and CR are skipped.  createdb runs
    against the old node (PGHOST/PGPORT from its connection env).
    """
    dbname = prefix
    for i in range(from_char, to_char + 1):
        if i in (7, 10, 13):  # skip BEL, LF, and CR
            continue
        dbname += chr(i)
    dbname += suffix
    old.bin.command_ok(
        ["createdb", dbname],
        "created database with ASCII characters from {} to {}".format(
            from_char, to_char
        ),
    )


def _filter_dump(dump_file):
    """Filter a dump for content comparison; return the filtered file path.

    Mirrors PostgreSQL::Test::AdjustUpgrade::adjust_old_dumpfile /
    adjust_new_dumpfile for the same-version (old_version == current) case:
    every version-conditional rewrite is inactive, so both reduce to the same
    set of unconditional transforms.
    """
    contents = pypg.slurp_file(dump_file)
    # use Unix newlines
    contents = contents.replace("\r\n", "\n")
    # Version comments will certainly not match.
    contents = re.sub(r"^-- Dumped from database version.*\n", "", contents, flags=re.M)
    # Same with the version argument to pg_restore_relation_stats(),
    # pg_restore_attribute_stats() or pg_restore_extended_stats().
    contents = re.sub(
        r"\n(\s+'version',) '\d+'::integer,$",
        r"\n\1 '000000'::integer,",
        contents,
        flags=re.M,
    )
    # Suppress blank lines, as some places in pg_dump emit more or fewer.
    contents = re.sub(r"\n\n+", "\n", contents)
    filtered = dump_file + "_filtered"
    with open(filtered, "w", encoding="utf-8") as fh:
        fh.write(contents)
    return filtered


def _adjust_regress_dumpfile(dump, adjust_child_columns):
    """Remove the known dump/restore differences from a regression-db dump.

    Mirrors PostgreSQL::Test::AdjustDump::adjust_regress_dumpfile.  Only used by
    the (normally skipped) regress_dump_restore round-trip section.
    """
    dump = dump.replace("\r\n", "\n")
    if adjust_child_columns:
        dump = re.sub(
            r"(^CREATE\sTABLE\sgenerated_stored_tests\.gtestxx_4\s\()"
            r"(\n\s+b\sinteger),"
            r"(\n\s+a\sinteger\sNOT\sNULL)",
            r"\1\3,\2",
            dump,
            flags=re.M | re.X,
        )
        dump = re.sub(
            r"(^CREATE\sTABLE\sgenerated_virtual_tests\.gtestxx_4\s\()"
            r"(\n\s+b\sinteger),"
            r"(\n\s+a\sinteger\sNOT\sNULL)",
            r"\1\3,\2",
            dump,
            flags=re.M | re.X,
        )
        dump = re.sub(
            r"(^CREATE\sTABLE\spublic\.test_type_diff2_c1\s\()"
            r"(\n\s+int_four\sbigint),"
            r"(\n\s+int_eight\sbigint),"
            r"(\n\s+int_two\ssmallint)",
            r"\1\4,\2,\3",
            dump,
            flags=re.M | re.X,
        )
        dump = re.sub(
            r"(^CREATE\sTABLE\spublic\.test_type_diff2_c2\s\()"
            r"(\n\s+int_eight\sbigint),"
            r"(\n\s+int_two\ssmallint),"
            r"(\n\s+int_four\sbigint)",
            r"\1\3,\4,\2",
            dump,
            flags=re.M | re.X,
        )
    for table in (
        r"public\.b_star",
        r"public\.c_star",
        r"public\.cc2",
        r"public\.d_star",
        r"public\.e_star",
        r"public\.f_star",
        r"public\.renamecolumnanother",
        r"public\.renamecolumnchild",
        r"public\.test_type_diff2_c1",
        r"public\.test_type_diff2_c2",
        r"public\.test_type_diff_c",
    ):
        dump = re.sub(r"^COPY " + table + r" \(.+?^\\\.$", "", dump, flags=re.S | re.M)
    dump = re.sub(r"\n\n+", "\n", dump)
    return dump


def _init_old_node(create_pg):
    """Initialize, configure and start the old node; return it."""
    # Set up locale settings for the original cluster so we can later test that
    # pg_upgrade copies template0's locale from the old to the new cluster.
    old_initdb_params = _CUSTOM_OPTS + [
        "--encoding",
        "UTF-8",
        "--lc-collate",
        _ORIGINAL_DATCOLLATE,
        "--lc-ctype",
        _ORIGINAL_DATCTYPE,
        "--locale-provider",
        "builtin",
        "--builtin-locale",
        _ORIGINAL_DATLOCALE,
    ]
    old = create_pg("old_node", start=False, extra=old_initdb_params)
    # Override log_statement=all set by Cluster.pm to avoid log traffic that
    # slows this test down, and run the regression tests at the same wal_level
    # as 'make check'.
    old.append_conf("log_statement = none")
    old.append_conf("wal_level = replica")
    old.start()
    return old


def _check_original_locales(old):
    """Assert template0's locale fields in the original cluster."""
    result = old.safe_psql(
        "SELECT encoding, datlocprovider, datcollate, datctype, datlocale\n"
        "                 FROM pg_database WHERE datname='template0'"
    )
    assert result == _EXPECTED_LOCALE_ROW, "check locales in original cluster"


def _populate_old_with_regress(old, pg_bin):
    """Create the boundary databases and run the full regression suite."""
    # Create databases with names covering most ASCII bytes.  The first name
    # exercises backslashes adjacent to double quotes, a Windows special case.
    _generate_db(old, 'regression\\"\\', 1, 45, '\\\\"\\\\\\')
    _generate_db(old, "regression", 46, 90, "")
    _generate_db(old, "regression", 91, 127, "")

    # Repo root: pyt/ -> pg_upgrade -> bin -> src -> root (the Perl computes
    # abs_path("../../..") relative to src/bin/pg_upgrade).
    srcdir = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "..", "..")
    )
    extra_opts = os.environ.get("EXTRA_REGRESS_OPTS") or ""
    # --dlpath finds regress.so and any libraries the tests require.
    dlpath = os.path.dirname(os.environ["REGRESS_SHLIB"])
    inputdir = os.path.join(srcdir, "src", "test", "regress")
    outputdir = str(_tmp_check())
    cmd = (
        [os.environ["PG_REGRESS"]]
        + extra_opts.split()
        + [
            "--dlpath=" + dlpath,
            "--bindir=",
            "--host=" + str(old.host),
            "--port=" + str(old.port),
            "--schedule=" + os.path.join(inputdir, "parallel_schedule"),
            "--max-concurrent-tests=20",
            "--inputdir=" + inputdir,
            "--outputdir=" + outputdir,
        ]
    )
    pg_bin.command_ok(cmd, "regression tests in old instance")


def _init_new_node(create_pg):
    """Initialize and configure the new node (different locale, overwritten)."""
    new_initdb_params = _CUSTOM_OPTS + [
        "--encoding",
        "SQL_ASCII",
        "--locale-provider",
        "libc",
    ]
    new = create_pg("new_node", start=False, extra=new_initdb_params)
    new.append_conf("log_statement = none")
    # Stabilize stats for comparison.
    new.append_conf("autovacuum = off")
    return new


def _maybe_regress_dump_restore(create_pg, pg_bin, old, tempdir):
    """Round-trip the regression database through dump/restore and compare.

    Skipped unless regress_dump_restore is listed in PG_TEST_EXTRA, mirroring
    the SKIP block in the Perl original.  (The "different versions" and
    "non-default install" skips never apply in this same-version port.)  This
    is one section of the larger test, so an unset extra returns early rather
    than skipping the whole test.
    """
    extra = os.environ.get("PG_TEST_EXTRA", "")
    if not re.search(r"\bregress_dump_restore\b", extra):
        return

    dstnode = create_pg("dst_node", start=False, extra=_old_node_params())
    dstnode.append_conf("log_statement = none")
    dstnode.append_conf("autovacuum = off")
    dstnode.start()

    # Use --create so the restored database keeps the source's configurable
    # settings (avoids locale-driven dump differences) and to cover --create.
    # Use directory format for parallel dump/restore.
    dump_file = os.path.join(tempdir, "regression.dump")
    pg_bin.command_ok(
        [
            "pg_dump",
            "-Fd",
            "-j2",
            "--no-sync",
            "-d",
            old.connstr("regression"),
            "--create",
            "-f",
            dump_file,
        ],
        "pg_dump on source instance",
    )
    dstnode.bin.command_ok(
        ["pg_restore", "--create", "-j2", "-d", "postgres", dump_file],
        "pg_restore to destination instance",
    )
    src_dump = _get_dump_for_comparison(old, "regression", "src_dump", 1, tempdir)
    dst_dump = _get_dump_for_comparison(dstnode, "regression", "dest_dump", 0, tempdir)
    pypg.compare_files(
        src_dump,
        dst_dump,
        "dump outputs from original and restored regression databases match",
    )


def _get_dump_for_comparison(node, db, file_prefix, adjust_child_columns, tempdir):
    """Plain-format dump of db adjusted for original/restored comparison.

    Mirrors the Perl get_dump_for_comparison helper.  Returns the path of the
    adjusted dump file.
    """
    dumpfile = os.path.join(tempdir, file_prefix + ".sql")
    dump_adjusted = dumpfile + "_adjusted"
    node.bin.run_command(
        [
            "pg_dump",
            "--no-sync",
            "--restrict-key",
            "test",
            "-d",
            node.connstr(db),
            "-f",
            dumpfile,
        ]
    )
    with open(dump_adjusted, "w", encoding="utf-8") as fh:
        fh.write(
            _adjust_regress_dumpfile(pypg.slurp_file(dumpfile), adjust_child_columns)
        )
    return dump_adjusted


def _old_node_params():
    """The initdb params used for the old node (shared with dst_node)."""
    return _CUSTOM_OPTS + [
        "--encoding",
        "UTF-8",
        "--lc-collate",
        _ORIGINAL_DATCOLLATE,
        "--lc-ctype",
        _ORIGINAL_DATCTYPE,
        "--locale-provider",
        "builtin",
        "--builtin-locale",
        _ORIGINAL_DATLOCALE,
    ]


def _dumpall(pg_bin, target_connstr, dump_file, msg):
    """Run pg_dumpall against target_connstr into dump_file and assert success.

    Mirrors the Perl @dump_command run via the new node; --extra-float-digits
    (only for old pg_version < 12) is never needed in this same-version port.
    """
    pg_bin.command_ok(
        [
            "pg_dumpall",
            "--no-sync",
            "--restrict-key",
            "test",
            "--dbname",
            target_connstr,
            "--file",
            dump_file,
        ],
        msg,
    )


def _pg_upgrade_cmd(old, new, oldbindir, newbindir, *extra):
    """Build the pg_upgrade command line shared by every invocation."""
    return [
        "pg_upgrade",
        "--no-sync",
        "--old-datadir",
        str(old.datadir),
        "--new-datadir",
        str(new.datadir),
        "--old-bindir",
        oldbindir,
        "--new-bindir",
        newbindir,
        "--socketdir",
        str(new.host),
        "--old-port",
        str(old.port),
        "--new-port",
        str(new.port),
        _MODE,
        *extra,
    ]


def _output_dir(new):
    """Path of the pg_upgrade logging directory under the new data dir."""
    return os.path.join(new.datadir, "pg_upgrade_output.d")


def _check_phase(pg_bin, old, new, oldbindir, newbindir):
    """Run the pg_upgrade --check failure/success cases and clean up.

    Covers: a bad old-bindir leaving pg_upgrade_output.d behind, an invalid
    database aborting --check, and a clean --check that removes the directory.
    """
    # Cause a failure at the very start of pg_upgrade; this should create the
    # logging directory pg_upgrade_output.d but leave it around.  --check keeps
    # an early exit.
    pg_bin.command_checks_all(
        _pg_upgrade_cmd(old, new, oldbindir + "/does/not/exist/", newbindir, "--check"),
        1,
        [r'check for ".*?does/not/exist" failed'],
        [],
        "run of pg_upgrade --check for new instance with incorrect binary path",
    )
    assert os.path.isdir(
        _output_dir(new)
    ), "pg_upgrade_output.d/ not removed after pg_upgrade failure"
    shutil.rmtree(_output_dir(new))

    # pg_upgrade aborts when it encounters an invalid database.
    pg_bin.command_checks_all(
        _pg_upgrade_cmd(old, new, oldbindir, newbindir, "--check"),
        1,
        [r"datconnlimit"],
        [r"^$"],
        "invalid database causes failure",
    )
    shutil.rmtree(_output_dir(new))


def _final_check_and_upgrade(pg_bin, old, new, oldbindir, newbindir):
    """A clean --check then the real pg_upgrade; verify the log dir lifecycle."""
    pg_bin.command_ok(
        _pg_upgrade_cmd(old, new, oldbindir, newbindir, "--check"),
        "run of pg_upgrade --check for new instance",
    )
    assert not os.path.isdir(
        _output_dir(new)
    ), "pg_upgrade_output.d/ removed after pg_upgrade --check success"

    pg_bin.command_ok(
        _pg_upgrade_cmd(old, new, oldbindir, newbindir),
        "run of pg_upgrade for new instance",
    )
    assert not os.path.isdir(
        _output_dir(new)
    ), "pg_upgrade_output.d/ removed after pg_upgrade success"


def test_002_pg_upgrade(create_pg, pg_bin, tmp_path, monkeypatch):
    """Same-version pg_upgrade: regression dump matches before and after."""
    tempdir = str(tmp_path)
    dump1_file = os.path.join(tempdir, "dump1.sql")
    dump2_file = os.path.join(tempdir, "dump2.sql")

    old = _init_old_node(create_pg)
    _check_original_locales(old)
    _populate_old_with_regress(old, pg_bin)

    new = _init_new_node(create_pg)
    newbindir = new.config_data("--bindir")
    oldbindir = old.config_data("--bindir")

    # Stabilize stats before pg_dump / pg_dumpall.  Doing it after initializing
    # the new node gives autovacuum enough time to update old-node statistics.
    old.append_conf("autovacuum = off")
    old.restart()

    _maybe_regress_dump_restore(create_pg, pg_bin, old, tempdir)

    # Take a dump before the upgrade as a base comparison, using the new node's
    # pg_dumpall (here equivalent to the old node's, same version).
    _dumpall(
        pg_bin, old.connstr("postgres"), dump1_file, "dump before running pg_upgrade"
    )

    # Create an invalid database; deleted below after the --check tests.
    old.safe_psql(
        "CREATE DATABASE regression_invalid;\n"
        "UPDATE pg_database SET datconnlimit = -2 "
        "WHERE datname = 'regression_invalid';"
    )

    # In a VPATH build we start in the source directory, but we want to run
    # pg_upgrade in the build directory so generated files (e.g.
    # delete_old_cluster.{sh,bat}) finish there.
    monkeypatch.chdir(tempdir)

    old.stop()
    _check_phase(pg_bin, old, new, oldbindir, newbindir)

    # Drop the invalid database so we can continue.
    old.start()
    old.safe_psql("DROP DATABASE regression_invalid")
    old.stop()

    _final_check_and_upgrade(pg_bin, old, new, oldbindir, newbindir)

    new.start()

    # Test that the upgraded cluster has the original locale settings.
    result = new.safe_psql(
        "SELECT encoding, datlocprovider, datcollate, datctype, datlocale\n"
        "                 FROM pg_database WHERE datname='template0'"
    )
    assert (
        result == _EXPECTED_LOCALE_ROW
    ), "check that locales in new cluster match original cluster"

    # Second dump from the upgraded instance.
    _dumpall(
        pg_bin, new.connstr("postgres"), dump2_file, "dump after running pg_upgrade"
    )

    # Filter the contents of the dumps, then compare; there should be no diffs.
    dump1_filtered = _filter_dump(dump1_file)
    dump2_filtered = _filter_dump(dump2_file)
    pypg.compare_files(
        dump1_filtered, dump2_filtered, "old and new dumps match after pg_upgrade"
    )


def _tmp_check():
    import tempfile  # pylint: disable=import-outside-toplevel

    return tempfile.mkdtemp(prefix="pgupgrade_regress_")

# Copyright (c) 2025-2026, PostgreSQL Global Development Group

"""Port of src/bin/pg_upgrade/t/006_transfer_modes.pl.

Tests for file transfer modes.

This is a same-version port: old and new clusters are both built from this tree
(v19+), so $ENV{oldinstall} is never set.  The non-in-place tablespace tests
(gated on oldinstall in the Perl original) are therefore not exercised; the
in-place tablespace tests (gated on old pg_version >= 10) do run.  The
old-version-too-old skip for --swap (pg_version < 10) likewise never applies.
"""

_NOT_SUPPORTED_RE = (
    r".* not supported on this platform"
    r"|could not .* between old and new data directories: .*"
)


def _create_test_objects(old):
    """Create a small variety of simple test objects on the old cluster.

    We'll check that these reach the new version after upgrading.  Includes the
    in-place tablespace objects (old pg_version >= 10 in the same-version port)
    and large objects.
    """
    old.start()
    old.safe_psql("CREATE TABLE test1 AS SELECT generate_series(1, 100)")
    old.safe_psql("CREATE DATABASE testdb1")
    old.safe_psql("CREATE TABLE test2 AS SELECT generate_series(200, 300)", "testdb1")
    old.safe_psql("VACUUM FULL test2", "testdb1")
    old.safe_psql("CREATE SEQUENCE testseq START 5432", "testdb1")

    # In-place tablespaces (available as far back as v10).
    old.safe_psql("CREATE TABLESPACE inplc_tblspc LOCATION ''")
    old.safe_psql("CREATE DATABASE testdb3 TABLESPACE inplc_tblspc")
    old.safe_psql(
        "CREATE TABLE test5 TABLESPACE inplc_tblspc "
        "AS SELECT generate_series(503, 606)"
    )
    old.safe_psql("CREATE TABLE test6 AS SELECT generate_series(607, 711)", "testdb3")

    # While we are here, test handling of large objects.
    old.safe_psql(
        r"""
        CREATE ROLE regress_lo_1;
        CREATE ROLE regress_lo_2;

        SELECT lo_from_bytea(4532, '\xffffff00');
        COMMENT ON LARGE OBJECT 4532 IS 'test';

        SELECT lo_from_bytea(4533, '\x0f0f0f0f');
        ALTER LARGE OBJECT 4533 OWNER TO regress_lo_1;
        GRANT SELECT ON LARGE OBJECT 4533 TO regress_lo_2;
    """
    )


def _create_seclabel_objects(old):
    """Create the dummy_seclabel extension and a labelled large object."""
    old.safe_psql(
        r"""
        CREATE EXTENSION dummy_seclabel;

        SELECT lo_from_bytea(4534, '\x00ffffff');
        SECURITY LABEL ON LARGE OBJECT 4534 IS 'classified';
    """
    )


def _verify_test_objects(new, mode):
    """Verify the simple test objects reached the new version after upgrade."""
    assert (
        new.safe_psql("SELECT COUNT(*) FROM test1") == "100"
    ), "test1 data after pg_upgrade {}".format(mode)
    assert (
        new.safe_psql("SELECT COUNT(*) FROM test2", "testdb1") == "101"
    ), "test2 data after pg_upgrade {}".format(mode)
    assert (
        new.safe_psql("SELECT nextval('testseq')", "testdb1") == "5432"
    ), "sequence data after pg_upgrade {}".format(mode)

    # In-place tablespaces.
    assert (
        new.safe_psql("SELECT COUNT(*) FROM test5") == "104"
    ), "test5 data after pg_upgrade {}".format(mode)
    assert (
        new.safe_psql("SELECT COUNT(*) FROM test6", "testdb3") == "105"
    ), "test6 data after pg_upgrade {}".format(mode)


def _verify_large_objects(new):
    """Verify large-object contents, owner and ACL reached the new version."""
    assert (
        new.safe_psql("SELECT lo_get(4532)") == r"\xffffff00"
    ), "LO contents after upgrade"
    assert (
        new.safe_psql("SELECT obj_description(4532, 'pg_largeobject')") == "test"
    ), "comment on LO after pg_upgrade"

    assert (
        new.safe_psql("SELECT lo_get(4533)") == r"\x0f0f0f0f"
    ), "LO contents after upgrade"
    assert (
        new.safe_psql(
            "SELECT lomowner::regrole FROM pg_largeobject_metadata WHERE oid = 4533"
        )
        == "regress_lo_1"
    ), "LO owner after upgrade"
    assert (
        new.safe_psql("SELECT lomacl FROM pg_largeobject_metadata WHERE oid = 4533")
        == "{regress_lo_1=rw/regress_lo_1,regress_lo_2=r/regress_lo_1}"
    ), "LO ACL after upgrade"


def _verify_seclabel(new):
    """Verify the security label on the labelled large object after upgrade."""
    assert (
        new.safe_psql("SELECT lo_get(4534)") == r"\x00ffffff"
    ), "LO contents after upgrade"
    result = new.safe_psql(
        "SELECT label FROM pg_seclabel WHERE objoid = 4534 "
        "AND classoid = 'pg_largeobject'::regclass"
    )
    assert result == "classified", "seclabel on LO after pg_upgrade"


def _pg_upgrade_cmd(old, new, mode):
    """Build the pg_upgrade command line for the given transfer mode."""
    return [
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
        mode,
    ]


def _test_mode(create_pg, pg_bin, mode, index):
    """Run pg_upgrade once with the given transfer mode and verify the result.

    index disambiguates per-mode node names within the single test process,
    standing in for the Perl test's fixed 'old'/'new' names (one mode per run).
    """
    old = create_pg("old{}".format(index), start=False)
    new = create_pg("new{}".format(index), start=False)

    # allow_in_place_tablespaces is available as far back as v10.
    new.append_conf("allow_in_place_tablespaces = true", "postgresql.conf")
    old.append_conf("allow_in_place_tablespaces = true", "postgresql.conf")

    # We can only test security labels if both the old and new installations
    # have dummy_seclabel.
    test_seclabel = True
    old.start()
    if not old.check_extension("dummy_seclabel"):
        test_seclabel = False
    old.stop()
    new.start()
    if not new.check_extension("dummy_seclabel"):
        test_seclabel = False
    new.stop()

    _create_test_objects(old)
    if test_seclabel:
        _create_seclabel_objects(old)
    old.stop()

    result = pg_bin.command_ok_or_fails_like(
        _pg_upgrade_cmd(old, new, mode),
        _NOT_SUPPORTED_RE,
        r"^$",
        "pg_upgrade with transfer mode {}".format(mode),
    )

    # If pg_upgrade was successful, check that all of our test objects reached
    # the new version.
    if result:
        new.start()
        _verify_test_objects(new, mode)
        _verify_large_objects(new)
        if test_seclabel:
            _verify_seclabel(new)
        new.stop()


def test_006_transfer_modes(create_pg, pg_bin, tmp_check, monkeypatch):
    """Exercise every pg_upgrade transfer mode on a same-version upgrade."""
    # Run pg_upgrade in tmp_check to avoid leaving files like
    # delete_old_cluster.{sh,bat} in the source directory for VPATH and meson
    # builds.
    tmp_check.mkdir(parents=True, exist_ok=True)
    monkeypatch.chdir(tmp_check)

    modes = ["--clone", "--copy", "--copy-file-range", "--link", "--swap"]
    for index, mode in enumerate(modes):
        _test_mode(create_pg, pg_bin, mode, index)

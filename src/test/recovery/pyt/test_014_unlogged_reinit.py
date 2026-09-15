# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/recovery/t/014_unlogged_reinit.pl.

Unlogged tables are properly reinitialized after a crash.
"""


def _exists(node, relpath):
    return (node.datadir / relpath).is_file()


def test_unlogged_reinit(create_pg, tmp_path):
    """Unlogged relation forks are reinitialized from the init fork on crash."""
    node = create_pg("main")

    node.safe_psql("CREATE UNLOGGED TABLE base_unlogged (id int)")
    node.safe_psql("CREATE UNLOGGED SEQUENCE seq_unlogged")

    base = node.safe_psql("select pg_relation_filepath('base_unlogged')")
    seq = node.safe_psql("select pg_relation_filepath('seq_unlogged')")

    # Main and init forks should exist.
    assert _exists(node, base + "_init"), "table init fork exists"
    assert _exists(node, base), "table main fork exists"
    assert _exists(node, seq + "_init"), "sequence init fork exists"
    assert _exists(node, seq), "sequence main fork exists"

    assert node.safe_psql("SELECT nextval('seq_unlogged')") == "1", "sequence nextval"
    assert node.safe_psql("SELECT nextval('seq_unlogged')") == "2", "sequence nextval"

    # Unlogged table in a tablespace.
    tablespace_dir = tmp_path / "ts1"
    tablespace_dir.mkdir()
    node.safe_psql("CREATE TABLESPACE ts1 LOCATION '{}'".format(tablespace_dir))
    node.safe_psql("CREATE UNLOGGED TABLE ts1_unlogged (id int) TABLESPACE ts1")
    ts1 = node.safe_psql("select pg_relation_filepath('ts1_unlogged')")
    assert _exists(node, ts1 + "_init"), "init fork in tablespace exists"
    assert _exists(node, ts1), "main fork in tablespace exists"

    # More unlogged sequences for testing.
    node.safe_psql("CREATE UNLOGGED SEQUENCE seq_unlogged2")
    node.safe_psql("ALTER SEQUENCE seq_unlogged2 INCREMENT 2")
    node.safe_psql("SELECT nextval('seq_unlogged2')")

    node.safe_psql(
        "CREATE UNLOGGED TABLE tab_seq_unlogged3 "
        "(a int GENERATED ALWAYS AS IDENTITY)"
    )
    node.safe_psql("TRUNCATE tab_seq_unlogged3 RESTART IDENTITY")
    node.safe_psql("INSERT INTO tab_seq_unlogged3 DEFAULT VALUES")

    # Crash the postmaster.
    node.stop("immediate")

    # Fake forks that recovery should remove.
    (node.datadir / (base + "_vm")).write_text("TEST_VM", encoding="utf-8")
    (node.datadir / (base + "_fsm")).write_text("TEST_FSM", encoding="utf-8")

    # Remove main forks to test that they are recopied from init.
    (node.datadir / base).unlink()
    (node.datadir / seq).unlink()

    (node.datadir / (ts1 + "_vm")).write_text("TEST_VM", encoding="utf-8")
    (node.datadir / (ts1 + "_fsm")).write_text("TEST_FSM", encoding="utf-8")
    (node.datadir / ts1).unlink()

    node.start()

    assert _exists(node, base + "_init"), "table init fork in base still exists"
    assert _exists(node, base), "table main fork in base recreated at startup"
    assert not _exists(node, base + "_vm"), "vm fork in base removed at startup"
    assert not _exists(node, base + "_fsm"), "fsm fork in base removed at startup"

    assert _exists(node, seq + "_init"), "sequence init fork still exists"
    assert _exists(node, seq), "sequence main fork recreated at startup"

    assert (
        node.safe_psql("SELECT nextval('seq_unlogged')") == "1"
    ), "nextval after restart"
    assert (
        node.safe_psql("SELECT nextval('seq_unlogged')") == "2"
    ), "nextval after restart"

    assert _exists(node, ts1 + "_init"), "init fork still exists in tablespace"
    assert _exists(node, ts1), "main fork in tablespace recreated at startup"
    assert not _exists(node, ts1 + "_vm"), "vm fork in tablespace removed at startup"
    assert not _exists(node, ts1 + "_fsm"), "fsm fork in tablespace removed at startup"

    assert node.safe_psql("SELECT nextval('seq_unlogged2')") == "1", "altered nextval"
    assert node.safe_psql("SELECT nextval('seq_unlogged2')") == "3", "altered nextval"

    node.safe_psql("INSERT INTO tab_seq_unlogged3 VALUES (DEFAULT), (DEFAULT)")
    assert (
        node.safe_psql("SELECT * FROM tab_seq_unlogged3") == "1\n2"
    ), "reset sequence nextval after restart"

# Copyright (c) 2017-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long
"""Port of src/test/recovery/t/018_wal_optimize.pl.

When wal_skip_threshold lets a relation created/rewritten in a transaction skip
WAL (file is fsynced at commit instead), an immediate crash must still leave the
data consistent after recovery. The same battery (CREATE/SET TABLESPACE,
TRUNCATE, TRUNCATE+INSERT/COPY, subtransaction SET TABLESPACE patterns, hint
bits, triggers on COPY/TRUNCATE, temp tables) is run twice, once with
wal_level='minimal' and once with 'replica', crashing and recovering after each
step. A final check confirms no orphan relfilenodes remain on disk.
"""

import os
import re

import pypg


def _crash_check(node, sql, query, expected, msg):
    """Run sql, crash-restart, then assert query returns expected."""
    node.safe_psql(sql)
    node.stop("immediate")
    node.start()
    assert node.safe_psql(query) == expected, msg


def _check_orphan_relfilenodes(node, test_name):
    db_oid = node.safe_psql("SELECT oid FROM pg_database WHERE datname = 'postgres'")
    prefix = "base/{}/".format(db_oid)
    referenced = node.safe_psql(
        "SELECT pg_relation_filepath(oid) FROM pg_class\n"
        "WHERE reltablespace = 0 AND relpersistence <> 't' AND\n"
        "pg_relation_filepath(oid) IS NOT NULL;"
    )
    on_disk = sorted(
        prefix + name
        for name in pypg.slurp_dir(str(node.datadir / prefix))
        if name.isdigit()
    )
    want = sorted(referenced.split("\n"))
    assert on_disk == want, test_name


def _run_wal_optimize(create_pg, wal_level):
    node = create_pg("node_{}".format(wal_level), start=False)
    node.append_conf(
        "\nwal_level = {}\nmax_prepared_transactions = 1\n"
        "max_wal_senders = 0\nwal_log_hints = on\nwal_skip_threshold = 0\n".format(
            wal_level
        )
    )
    node.start()
    wl = wal_level
    tablespace_dir = os.path.join(node.basedir, "tablespace_other_{}".format(wal_level))
    os.mkdir(tablespace_dir)
    _crash_check(
        node,
        "CREATE TABLE moved (id int);\nINSERT INTO moved VALUES (1);\n"
        "CREATE TABLESPACE other LOCATION '{}';\nBEGIN;\n"
        "ALTER TABLE moved SET TABLESPACE other;\n"
        "CREATE TABLE originated (id int);\nINSERT INTO originated VALUES (1);\n"
        "CREATE UNIQUE INDEX ON originated(id) TABLESPACE other;\n"
        "COMMIT;".format(tablespace_dir),
        "SELECT count(*) FROM moved;",
        "1",
        "wal_level = {}, CREATE+SET TABLESPACE".format(wl),
    )
    assert (
        node.safe_psql(
            "INSERT INTO originated VALUES (1) ON CONFLICT (id)\n"
            "  DO UPDATE set id = originated.id + 1\n  RETURNING id;"
        )
        == "2"
    ), "wal_level = {}, CREATE TABLESPACE, CREATE INDEX".format(wl)
    _crash_check(
        node,
        "BEGIN;\nCREATE TABLE trunc (id serial PRIMARY KEY);\n"
        "TRUNCATE trunc;\nCOMMIT;",
        "SELECT count(*) FROM trunc;",
        "0",
        "wal_level = {}, TRUNCATE with empty table".format(wl),
    )
    _crash_check(
        node,
        "BEGIN;\nCREATE TABLE trunc_ins (id serial PRIMARY KEY);\n"
        "INSERT INTO trunc_ins VALUES (DEFAULT);\nTRUNCATE trunc_ins;\n"
        "INSERT INTO trunc_ins VALUES (DEFAULT);\nCOMMIT;",
        "SELECT count(*), min(id) FROM trunc_ins;",
        "1|2",
        "wal_level = {}, TRUNCATE INSERT".format(wl),
    )
    _crash_check(
        node,
        "BEGIN;\nCREATE TABLE twophase (id serial PRIMARY KEY);\n"
        "INSERT INTO twophase VALUES (DEFAULT);\nTRUNCATE twophase;\n"
        "INSERT INTO twophase VALUES (DEFAULT);\nPREPARE TRANSACTION 't';\n"
        "COMMIT PREPARED 't';",
        "SELECT count(*), min(id) FROM trunc_ins;",
        "1|2",
        "wal_level = {}, TRUNCATE INSERT PREPARE".format(wl),
    )
    _crash_check(
        node,
        "SET wal_skip_threshold = '1GB';\nBEGIN;\n"
        "CREATE TABLE noskip (id serial PRIMARY KEY);\n"
        "INSERT INTO noskip (SELECT FROM generate_series(1, 20000) a) ;\nCOMMIT;",
        "SELECT count(*) FROM noskip;",
        "20000",
        "wal_level = {}, end-of-xact WAL".format(wl),
    )
    copy_file = os.path.join(node.basedir, "copy_data_{}.txt".format(wal_level))
    pypg.append_to_file(copy_file, "20000,30000\n20001,30001\n20002,30002")
    _copy_battery(node, wl, copy_file)
    _trigger_battery(node, wl, copy_file)
    node.safe_psql("CREATE TEMP TABLE temp (id serial PRIMARY KEY, id2 text);")
    node.stop("immediate")
    node.start()
    _check_orphan_relfilenodes(
        node, "wal_level = {}, no orphan relfilenode remains".format(wl)
    )


def _copy_battery(node, wl, copy_file):
    _crash_check(
        node,
        "BEGIN;\nCREATE TABLE ins_trunc (id serial PRIMARY KEY, id2 int);\n"
        "INSERT INTO ins_trunc VALUES (DEFAULT, generate_series(1,10000));\n"
        "TRUNCATE ins_trunc;\n"
        "INSERT INTO ins_trunc (id, id2) VALUES (DEFAULT, 10000);\n"
        "COPY ins_trunc FROM '{}' DELIMITER ',';\n"
        "INSERT INTO ins_trunc (id, id2) VALUES (DEFAULT, 10000);\n"
        "COMMIT;".format(copy_file),
        "SELECT count(*) FROM ins_trunc;",
        "5",
        "wal_level = {}, TRUNCATE COPY INSERT".format(wl),
    )
    _crash_check(
        node,
        "BEGIN;\nCREATE TABLE trunc_copy (id serial PRIMARY KEY, id2 int);\n"
        "INSERT INTO trunc_copy VALUES (DEFAULT, generate_series(1,3000));\n"
        "TRUNCATE trunc_copy;\nCOPY trunc_copy FROM '{}' DELIMITER ',';\n"
        "COMMIT;".format(copy_file),
        "SELECT count(*) FROM trunc_copy;",
        "3",
        "wal_level = {}, TRUNCATE COPY".format(wl),
    )
    _crash_check(
        node,
        "BEGIN;\nCREATE TABLE spc_abort (id serial PRIMARY KEY, id2 int);\n"
        "INSERT INTO spc_abort VALUES (DEFAULT, generate_series(1,3000));\n"
        "TRUNCATE spc_abort;\nSAVEPOINT s;\n"
        "  ALTER TABLE spc_abort SET TABLESPACE other; ROLLBACK TO s;\n"
        "COPY spc_abort FROM '{}' DELIMITER ',';\nCOMMIT;".format(copy_file),
        "SELECT count(*) FROM spc_abort;",
        "3",
        "wal_level = {}, SET TABLESPACE abort subtransaction".format(wl),
    )
    _crash_check(
        node,
        "BEGIN;\nCREATE TABLE spc_commit (id serial PRIMARY KEY, id2 int);\n"
        "INSERT INTO spc_commit VALUES (DEFAULT, generate_series(1,3000));\n"
        "TRUNCATE spc_commit;\n"
        "SAVEPOINT s; ALTER TABLE spc_commit SET TABLESPACE other; RELEASE s;\n"
        "COPY spc_commit FROM '{}' DELIMITER ',';\nCOMMIT;".format(copy_file),
        "SELECT count(*) FROM spc_commit;",
        "3",
        "wal_level = {}, SET TABLESPACE commit subtransaction".format(wl),
    )
    _crash_check(
        node,
        "BEGIN;\nCREATE TABLE spc_nest (id serial PRIMARY KEY, id2 int);\n"
        "INSERT INTO spc_nest VALUES (DEFAULT, generate_series(1,3000));\n"
        "TRUNCATE spc_nest;\nSAVEPOINT s;\n"
        "\tALTER TABLE spc_nest SET TABLESPACE other;\n\tSAVEPOINT s2;\n"
        "\t\tALTER TABLE spc_nest SET TABLESPACE pg_default;\n\tROLLBACK TO s2;\n"
        "\tSAVEPOINT s2;\n\t\tALTER TABLE spc_nest SET TABLESPACE pg_default;\n"
        "\tRELEASE s2;\nROLLBACK TO s;\n"
        "COPY spc_nest FROM '{}' DELIMITER ',';\nCOMMIT;".format(copy_file),
        "SELECT count(*) FROM spc_nest;",
        "3",
        "wal_level = {}, SET TABLESPACE nested subtransaction".format(wl),
    )
    _crash_check(
        node,
        "CREATE TABLE spc_hint (id int);\nINSERT INTO spc_hint VALUES (1);\n"
        "BEGIN;\nALTER TABLE spc_hint SET TABLESPACE other;\nCHECKPOINT;\n"
        "SELECT * FROM spc_hint;\nINSERT INTO spc_hint VALUES (2);\nCOMMIT;",
        "SELECT count(*) FROM spc_hint;",
        "2",
        "wal_level = {}, SET TABLESPACE, hint bit".format(wl),
    )


def _trigger_battery(node, wl, copy_file):
    node.safe_psql(
        "BEGIN;\nCREATE TABLE idx_hint (c int PRIMARY KEY);\n"
        "SAVEPOINT q; INSERT INTO idx_hint VALUES (1); ROLLBACK TO q;\n"
        "CHECKPOINT;\nINSERT INTO idx_hint VALUES (1);\n"
        "INSERT INTO idx_hint VALUES (2);\nCOMMIT;"
    )
    node.stop("immediate")
    node.start()
    res = node.psql_capture("INSERT INTO idx_hint VALUES (2);")
    assert res.rc == 3, "wal_level = {}, unique index LP_DEAD".format(wl)
    assert re.search(
        r"violates unique", res.stderr
    ), "wal_level = {}, unique index LP_DEAD message".format(wl)
    _crash_check(
        node,
        "BEGIN;\nCREATE TABLE upd (id serial PRIMARY KEY, id2 int);\n"
        "INSERT INTO upd (id, id2) VALUES (DEFAULT, generate_series(1,10000));\n"
        "COPY upd FROM '{}' DELIMITER ',';\nUPDATE upd SET id2 = id2 + 1;\n"
        "DELETE FROM upd;\nCOMMIT;".format(copy_file),
        "SELECT count(*) FROM upd;",
        "0",
        "wal_level = {}, UPDATE touches two buffers for one row".format(wl),
    )
    _crash_check(
        node,
        "BEGIN;\nCREATE TABLE ins_copy (id serial PRIMARY KEY, id2 int);\n"
        "INSERT INTO ins_copy VALUES (DEFAULT, 1);\n"
        "COPY ins_copy FROM '{}' DELIMITER ',';\nCOMMIT;".format(copy_file),
        "SELECT count(*) FROM ins_copy;",
        "4",
        "wal_level = {}, INSERT COPY".format(wl),
    )
    _crash_check(
        node,
        _INS_TRIG_SQL.format(copy_file),
        "SELECT count(*) FROM ins_trig;",
        "9",
        "wal_level = {}, COPY with INSERT triggers".format(wl),
    )
    _crash_check(
        node,
        _TRUNC_TRIG_SQL.format(copy_file),
        "SELECT count(*) FROM trunc_trig;",
        "4",
        "wal_level = {}, TRUNCATE COPY with TRUNCATE triggers".format(wl),
    )


_INS_TRIG_SQL = """BEGIN;
CREATE TABLE ins_trig (id serial PRIMARY KEY, id2 text);
CREATE FUNCTION ins_trig_before_row_trig() RETURNS trigger
  LANGUAGE plpgsql as $$
  BEGIN
    IF new.id2 NOT LIKE 'triggered%' THEN
      INSERT INTO ins_trig VALUES (DEFAULT, 'triggered row before' || NEW.id2);
    END IF;
    RETURN NEW;
  END; $$;
CREATE FUNCTION ins_trig_after_row_trig() RETURNS trigger
  LANGUAGE plpgsql as $$
  BEGIN
    IF new.id2 NOT LIKE 'triggered%' THEN
      INSERT INTO ins_trig VALUES (DEFAULT, 'triggered row after' || NEW.id2);
    END IF;
    RETURN NEW;
  END; $$;
CREATE TRIGGER ins_trig_before_row_insert
  BEFORE INSERT ON ins_trig
  FOR EACH ROW EXECUTE PROCEDURE ins_trig_before_row_trig();
CREATE TRIGGER ins_trig_after_row_insert
  AFTER INSERT ON ins_trig
  FOR EACH ROW EXECUTE PROCEDURE ins_trig_after_row_trig();
COPY ins_trig FROM '{}' DELIMITER ',';
COMMIT;"""

_TRUNC_TRIG_SQL = """BEGIN;
CREATE TABLE trunc_trig (id serial PRIMARY KEY, id2 text);
CREATE FUNCTION trunc_trig_before_stat_trig() RETURNS trigger
  LANGUAGE plpgsql as $$
  BEGIN
    INSERT INTO trunc_trig VALUES (DEFAULT, 'triggered stat before');
    RETURN NULL;
  END; $$;
CREATE FUNCTION trunc_trig_after_stat_trig() RETURNS trigger
  LANGUAGE plpgsql as $$
  BEGIN
    INSERT INTO trunc_trig VALUES (DEFAULT, 'triggered stat before');
    RETURN NULL;
  END; $$;
CREATE TRIGGER trunc_trig_before_stat_truncate
  BEFORE TRUNCATE ON trunc_trig
  FOR EACH STATEMENT EXECUTE PROCEDURE trunc_trig_before_stat_trig();
CREATE TRIGGER trunc_trig_after_stat_truncate
  AFTER TRUNCATE ON trunc_trig
  FOR EACH STATEMENT EXECUTE PROCEDURE trunc_trig_after_stat_trig();
INSERT INTO trunc_trig VALUES (DEFAULT, 1);
TRUNCATE trunc_trig;
COPY trunc_trig FROM '{}' DELIMITER ',';
COMMIT;"""


def test_018_wal_optimize(create_pg):
    """WAL-skip optimizations stay crash-consistent at minimal and replica."""
    _run_wal_optimize(create_pg, "minimal")
    _run_wal_optimize(create_pg, "replica")

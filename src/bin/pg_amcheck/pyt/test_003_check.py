# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/bin/pg_amcheck/t/003_check.pl.

End-to-end pg_amcheck corruption checks across multiple databases and schemas:
amcheck installed in an unexpected schema, decoy catalog-named tables and decoy
amcheck-named functions, planned corruptions applied in a single restart
(removed relation files and clobbered first pages), and the resulting
exit-code/stdout/stderr expectations for many option combinations.
"""

import os
import struct


def _relation_filepath(node, dbname, relname):
    """Return the absolute on-disk path of a relation's main fork."""
    rel = node.safe_psql(
        "SELECT pg_relation_filepath('{}')".format(relname), dbname=dbname
    )
    assert rel, "path not found for relation {}".format(relname)
    return os.path.join(node.datadir, rel)


def _relation_toast(node, dbname, relname):
    """Return the toast relation name for relname, or '' if none."""
    return node.safe_psql(
        "SELECT c.reltoastrelid::regclass\n"
        "    FROM pg_catalog.pg_class c\n"
        "    WHERE c.oid = '{}'::regclass\n"
        "      AND c.reltoastrelid != 0".format(relname),
        dbname=dbname,
    )


def _corrupt_first_page(relpath):
    """Clobber the first page's line pointers with corruption-triggering junk."""
    # The values are chosen to hit the various line-pointer-corruption checks
    # in verify_heapam.c on both little-endian and big-endian architectures
    # (Perl pack("L*", ...) is native unsigned 32-bit).
    payload = struct.pack(
        "=7I",
        0xAAA15550,
        0xAAA0D550,
        0x00010000,
        0x00008000,
        0x0000800F,
        0x001E8000,
        0xFFFFFFFF,
    )
    with open(relpath, "r+b") as fh:
        fh.seek(32)
        fh.write(payload)


_AMCHECK_DECOYS = """\
CREATE SCHEMA amcheck_schema;
CREATE EXTENSION amcheck WITH SCHEMA amcheck_schema;
CREATE TABLE amcheck_schema.pg_database (junk text);
CREATE TABLE amcheck_schema.pg_namespace (junk text);
CREATE TABLE amcheck_schema.pg_class (junk text);
CREATE TABLE amcheck_schema.pg_operator (junk text);
CREATE TABLE amcheck_schema.pg_proc (junk text);
CREATE TABLE amcheck_schema.pg_tablespace (junk text);

CREATE FUNCTION public.bt_index_check(index regclass,
                                      heapallindexed boolean default false)
RETURNS VOID AS $$
BEGIN
    RAISE EXCEPTION 'Invoked wrong bt_index_check!';
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION public.bt_index_parent_check(index regclass,
                                             heapallindexed boolean default false,
                                             rootdescend boolean default false)
RETURNS VOID AS $$
BEGIN
    RAISE EXCEPTION 'Invoked wrong bt_index_parent_check!';
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION public.verify_heapam(relation regclass,
                                     on_error_stop boolean default false,
                                     check_toast boolean default false,
                                     skip text default 'none',
                                     startblock bigint default null,
                                     endblock bigint default null,
                                     blkno OUT bigint,
                                     offnum OUT integer,
                                     attnum OUT integer,
                                     msg OUT text)
RETURNS SETOF record AS $$
BEGIN
    RAISE EXCEPTION 'Invoked wrong verify_heapam!';
END;
$$ LANGUAGE plpgsql;
"""


def _schema_objects_sql(schema):
    """SQL creating the identical per-schema set of relations and indexes."""
    return """\
CREATE SCHEMA {s};
CREATE SEQUENCE {s}.seq1;
CREATE SEQUENCE {s}.seq2;
CREATE TABLE {s}.t1 (
    i INTEGER,
    b BOX,
    ia int4[],
    ir int4range,
    t TEXT
);
CREATE TABLE {s}.t2 (
    i INTEGER,
    b BOX,
    ia int4[],
    ir int4range,
    t TEXT
);
CREATE VIEW {s}.t2_view AS (
    SELECT i*2, t FROM {s}.t2
);
ALTER TABLE {s}.t2
    ALTER COLUMN t
    SET STORAGE EXTERNAL;

INSERT INTO {s}.t1 (i, b, ia, ir, t)
    (SELECT gs::INTEGER AS i,
            box(point(gs,gs+5),point(gs*2,gs*3)) AS b,
            array[gs, gs + 1]::int4[] AS ia,
            int4range(gs, gs+100) AS ir,
            repeat('foo', gs) AS t
         FROM generate_series(1,10000,3000) AS gs);

INSERT INTO {s}.t2 (i, b, ia, ir, t)
    (SELECT gs::INTEGER AS i,
            box(point(gs,gs+5),point(gs*2,gs*3)) AS b,
            array[gs, gs + 1]::int4[] AS ia,
            int4range(gs, gs+100) AS ir,
            repeat('foo', gs) AS t
         FROM generate_series(1,10000,3000) AS gs);

CREATE MATERIALIZED VIEW {s}.t1_mv AS SELECT * FROM {s}.t1;
CREATE MATERIALIZED VIEW {s}.t2_mv AS SELECT * FROM {s}.t2;

create table {s}.p1 (a int, b int) PARTITION BY list (a);
create table {s}.p2 (a int, b int) PARTITION BY list (a);

create table {s}.p1_1 partition of {s}.p1 for values in (1, 2, 3);
create table {s}.p1_2 partition of {s}.p1 for values in (4, 5, 6);
create table {s}.p2_1 partition of {s}.p2 for values in (1, 2, 3);
create table {s}.p2_2 partition of {s}.p2 for values in (4, 5, 6);

CREATE INDEX t1_btree ON {s}.t1 USING BTREE (i);
CREATE INDEX t2_btree ON {s}.t2 USING BTREE (i);

CREATE INDEX t1_hash ON {s}.t1 USING HASH (i);
CREATE INDEX t2_hash ON {s}.t2 USING HASH (i);

CREATE INDEX t1_brin ON {s}.t1 USING BRIN (i);
CREATE INDEX t2_brin ON {s}.t2 USING BRIN (i);

CREATE INDEX t1_gist ON {s}.t1 USING GIST (b);
CREATE INDEX t2_gist ON {s}.t2 USING GIST (b);

CREATE INDEX t1_gin ON {s}.t1 USING GIN (ia);
CREATE INDEX t2_gin ON {s}.t2 USING GIN (ia);

CREATE INDEX t1_spgist ON {s}.t1 USING SPGIST (ir);
CREATE INDEX t2_spgist ON {s}.t2 USING SPGIST (ir);

CREATE UNIQUE INDEX t1_btree_unique ON {s}.t1 USING BTREE (i);
CREATE UNIQUE INDEX t2_btree_unique ON {s}.t2 USING BTREE (i);
""".format(
        s=schema
    )


class _CorruptionPlan:
    """Accumulates relation files to corrupt or remove, applied in one restart."""

    def __init__(self, node):
        self._node = node
        self._corrupt_page = set()
        self._remove_relation = set()

    def corrupt_first_page(self, dbname, relname):
        """Plan to clobber the first page of (dbname, relname)."""
        self._corrupt_page.add(_relation_filepath(self._node, dbname, relname))

    def remove_relation_file(self, dbname, relname):
        """Plan to remove the relation file of (dbname, relname)."""
        self._remove_relation.add(_relation_filepath(self._node, dbname, relname))

    def remove_toast_file(self, dbname, relname):
        """Plan to remove (dbname, relname)'s toast relation file, if any."""
        toastname = _relation_toast(self._node, dbname, relname)
        if toastname:
            self.remove_relation_file(dbname, toastname)

    def perform_all(self):
        """Stop the node, apply every planned corruption, restart the node."""
        self._node.stop()
        for relpath in self._corrupt_page:
            _corrupt_first_page(relpath)
        for relpath in self._remove_relation:
            os.unlink(relpath)
        self._node.start()


def _setup_databases(node):
    """Create db1/db2/db3 with decoys and five identical schemas each."""
    for dbname in ("db1", "db2", "db3"):
        node.safe_psql("CREATE DATABASE {}".format(dbname))
        node.safe_psql(_AMCHECK_DECOYS, dbname=dbname)
        for schema in ("s1", "s2", "s3", "s4", "s5"):
            node.safe_psql(_schema_objects_sql(schema), dbname=dbname)


def _plan_db1_corruptions(plan):
    """Plan all the db1 corruptions across schemas s1..s5."""
    # s1: corrupt indexes.
    plan.remove_relation_file("db1", "s1.t1_btree")
    plan.corrupt_first_page("db1", "s1.t2_btree")
    # s2: corrupt tables.
    plan.remove_relation_file("db1", "s2.t1")
    plan.corrupt_first_page("db1", "s2.t2")
    # s3: corrupt tables, partitions, matviews, and btrees.
    plan.remove_relation_file("db1", "s3.t1")
    plan.corrupt_first_page("db1", "s3.t2")
    plan.remove_relation_file("db1", "s3.t1_mv")
    plan.remove_relation_file("db1", "s3.p1_1")
    plan.corrupt_first_page("db1", "s3.t2_mv")
    plan.corrupt_first_page("db1", "s3.p2_1")
    plan.remove_relation_file("db1", "s3.t1_btree")
    plan.corrupt_first_page("db1", "s3.t2_btree")
    # s4: corrupt only the toast table.
    plan.remove_toast_file("db1", "s4.t2")
    # s5: corrupt object types amcheck does not support (must not error).
    plan.remove_relation_file("db1", "s5.seq1")
    plan.remove_relation_file("db1", "s5.t1_hash")
    plan.remove_relation_file("db1", "s5.t1_gist")
    plan.remove_relation_file("db1", "s5.t1_gin")
    plan.remove_relation_file("db1", "s5.t1_brin")
    plan.remove_relation_file("db1", "s5.t1_spgist")
    plan.corrupt_first_page("db1", "s5.seq2")
    plan.corrupt_first_page("db1", "s5.t2_hash")
    plan.corrupt_first_page("db1", "s5.t2_gist")
    plan.corrupt_first_page("db1", "s5.t2_gin")
    plan.corrupt_first_page("db1", "s5.t2_brin")
    plan.corrupt_first_page("db1", "s5.t2_spgist")


_NO_OUTPUT_RE = r"^$"
_LINE_POINTER_RE = r"line pointer"
_MISSING_FILE_RE = r'could not open file ".*": No such file or directory'
_INDEX_MISSING_FORK_RE = r'index ".*" lacks a main relation fork'


def _check_corruption_reports(node, cmd):
    """All command_checks_all assertions over the corrupted databases."""
    corrupt_set = [_INDEX_MISSING_FORK_RE, _LINE_POINTER_RE, _MISSING_FILE_RE]

    node.command_checks_all(
        cmd + ["db1"],
        2,
        corrupt_set,
        [_NO_OUTPUT_RE],
        "pg_amcheck all schemas, tables and indexes in database db1",
    )
    node.command_checks_all(
        cmd + ["--database", "db1", "--database", "db2", "--database", "db3"],
        2,
        corrupt_set,
        [_NO_OUTPUT_RE],
        "pg_amcheck all schemas, tables and indexes in databases db1, db2, and db3",
    )
    node.command_checks_all(
        cmd + ["--all", "--schema", "s1", "--index", "t1_btree"],
        2,
        [_INDEX_MISSING_FORK_RE],
        [
            r'pg_amcheck: warning: skipping database "postgres": '
            r"amcheck is not installed"
        ],
        "pg_amcheck index s1.t1_btree reports missing main relation fork",
    )
    node.command_checks_all(
        cmd + ["--database", "db1", "--schema", "s1", "--index", "t2_btree"],
        2,
        [r".+"],
        [_NO_OUTPUT_RE],
        "pg_amcheck index s1.s2 reports index corruption",
    )
    node.command_checks_all(
        cmd + ["--table", "s1.*", "--no-dependent-indexes", "db1"],
        0,
        [_NO_OUTPUT_RE],
        [_NO_OUTPUT_RE],
        "pg_amcheck of db1.s1 excluding indexes",
    )
    node.command_checks_all(
        cmd + ["--table", "s1.*", "--no-dependent-indexes", "db2"],
        2,
        [_MISSING_FILE_RE],
        [_NO_OUTPUT_RE],
        "pg_amcheck of db2.s1 excluding indexes",
    )
    node.command_checks_all(
        cmd + ["--schema", "s3", "db1"],
        2,
        corrupt_set,
        [_NO_OUTPUT_RE],
        "pg_amcheck schema s3 reports table and index errors",
    )


def _check_toast_and_exclusions(node, cmd):
    """Toast handling and the schema/table/index exclusion assertions."""
    node.command_checks_all(
        cmd + ["--schema", "s4", "db1"],
        2,
        [_MISSING_FILE_RE],
        [_NO_OUTPUT_RE],
        "pg_amcheck in schema s4 reports toast corruption",
    )
    node.command_checks_all(
        cmd
        + [
            "--no-dependent-toast",
            "--exclude-toast-pointers",
            "--schema",
            "s4",
            "db1",
        ],
        0,
        [_NO_OUTPUT_RE],
        [_NO_OUTPUT_RE],
        "pg_amcheck in schema s4 excluding toast reports no corruption",
    )
    node.command_checks_all(
        cmd + ["--schema", "s5", "db1"],
        0,
        [_NO_OUTPUT_RE],
        [_NO_OUTPUT_RE],
        "pg_amcheck over schema s5 reports no corruption",
    )
    node.command_checks_all(
        cmd
        + [
            "--schema",
            "s1",
            "--exclude-index",
            "t1_btree",
            "--exclude-index",
            "t2_btree",
            "db1",
        ],
        0,
        [_NO_OUTPUT_RE],
        [_NO_OUTPUT_RE],
        "pg_amcheck over schema s1 with corrupt indexes excluded reports no "
        "corruption",
    )
    node.command_checks_all(
        cmd + ["--table", "s1.*", "--no-dependent-indexes", "db1"],
        0,
        [_NO_OUTPUT_RE],
        [_NO_OUTPUT_RE],
        "pg_amcheck over schema s1 with all indexes excluded reports no corruption",
    )
    node.command_checks_all(
        cmd
        + [
            "--schema",
            "s2",
            "--exclude-table",
            "t1",
            "--exclude-table",
            "t2",
            "db1",
        ],
        0,
        [_NO_OUTPUT_RE],
        [_NO_OUTPUT_RE],
        "pg_amcheck over schema s2 with corrupt tables excluded reports no "
        "corruption",
    )


def _check_block_range_and_modes(node, cmd):
    """Bad block-range arguments and the index-mode smoke tests."""
    node.command_fails_like(
        cmd + ["--schema", "s5", "--startblock", "junk", "db1"],
        r"invalid start block",
        "pg_amcheck rejects garbage startblock",
    )
    node.command_fails_like(
        cmd + ["--schema", "s5", "--endblock", "1234junk", "db1"],
        r"invalid end block",
        "pg_amcheck rejects garbage endblock",
    )
    node.command_fails_like(
        cmd + ["--schema", "s5", "--startblock", "5", "--endblock", "4", "db1"],
        r"end block precedes start block",
        "pg_amcheck rejects invalid block range",
    )
    node.command_checks_all(
        cmd + ["--schema", "s1", "--index", "t1_btree", "--parent-check", "db1"],
        2,
        [_INDEX_MISSING_FORK_RE],
        [_NO_OUTPUT_RE],
        "pg_amcheck smoke test --parent-check",
    )
    node.command_checks_all(
        cmd
        + [
            "--schema",
            "s1",
            "--index",
            "t1_btree",
            "--heapallindexed",
            "--rootdescend",
            "db1",
        ],
        2,
        [_INDEX_MISSING_FORK_RE],
        [_NO_OUTPUT_RE],
        "pg_amcheck smoke test --heapallindexed --rootdescend",
    )
    node.command_checks_all(
        cmd
        + [
            "--database",
            "db1",
            "--database",
            "db2",
            "--database",
            "db3",
            "--exclude-schema",
            "s*",
        ],
        0,
        [_NO_OUTPUT_RE],
        [_NO_OUTPUT_RE],
        "pg_amcheck excluding all corrupt schemas",
    )


def _check_checkunique(node, cmd):
    """--checkunique smoke tests, including the unsupported-version warning."""
    node.command_checks_all(
        cmd
        + [
            "--schema",
            "s1",
            "--index",
            "t1_btree",
            "--parent-check",
            "--checkunique",
            "db1",
        ],
        2,
        [_INDEX_MISSING_FORK_RE],
        [_NO_OUTPUT_RE],
        "pg_amcheck smoke test --parent-check --checkunique",
    )
    node.command_checks_all(
        cmd
        + [
            "--schema",
            "s1",
            "--index",
            "t1_btree",
            "--heapallindexed",
            "--rootdescend",
            "--checkunique",
            "db1",
        ],
        2,
        [_INDEX_MISSING_FORK_RE],
        [_NO_OUTPUT_RE],
        "pg_amcheck smoke test --heapallindexed --rootdescend --checkunique",
    )
    node.command_checks_all(
        cmd
        + [
            "--checkunique",
            "--database",
            "db1",
            "--database",
            "db2",
            "--database",
            "db3",
            "--exclude-schema",
            "s*",
        ],
        0,
        [_NO_OUTPUT_RE],
        [_NO_OUTPUT_RE],
        "pg_amcheck excluding all corrupt schemas with --checkunique option",
    )
    node.safe_psql(
        "DROP EXTENSION amcheck;\n"
        "CREATE EXTENSION amcheck WITH SCHEMA amcheck_schema VERSION '1.3' ;",
        dbname="db3",
    )
    node.command_checks_all(
        cmd + ["--checkunique", "db3"],
        0,
        [_NO_OUTPUT_RE],
        [
            r"pg_amcheck: warning: option --checkunique is not supported by "
            r"amcheck version 1.3"
        ],
        "pg_amcheck smoke test --checkunique",
    )


def test_003_check(create_pg):
    """pg_amcheck detects planned multi-database, multi-schema corruption."""
    node = create_pg("test", no_data_checksums=True, start=False)
    node.append_conf("autovacuum=off")
    node.start()

    _setup_databases(node)

    cmd = ["pg_amcheck", "--port", str(node.port)]

    # No corruption yet: nothing reported.
    node.command_checks_all(
        cmd + ["--database", "db1", "--database", "db2", "--database", "db3"],
        0,
        [_NO_OUTPUT_RE],
        [_NO_OUTPUT_RE],
        "pg_amcheck prior to corruption",
    )

    plan = _CorruptionPlan(node)
    _plan_db1_corruptions(plan)
    # db2: corrupt s1.t1 and its btree; leave db3 clean.
    plan.remove_relation_file("db2", "s1.t1")
    plan.remove_relation_file("db2", "s1.t1_btree")
    plan.perform_all()

    _check_corruption_reports(node, cmd)
    _check_toast_and_exclusions(node, cmd)
    _check_block_range_and_modes(node, cmd)
    _check_checkunique(node, cmd)

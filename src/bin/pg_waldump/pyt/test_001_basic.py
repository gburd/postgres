# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/bin/pg_waldump/t/001_basic.pl.

Basic pg_waldump tests: option/argument validation, the rmgr list, and decoding
of a range of WAL records (including a contrecord spanning WAL segments) read
both from a live data directory and from tar archives (none/gzip).
"""

import os
import random
import re
import shutil
import struct
import subprocess

_RMGR_LIST = """\
XLOG
Transaction
Storage
CLOG
Database
Tablespace
MultiXact
RelMap
Standby
Heap2
Heap
Btree
Hash
Gin
Gist
Sequence
SPGist
BRIN
CommitTs
ReplicationOrigin
Generic
LogicalMessage
XLOG2"""

# Schema/workload exercising heap, btree, hash, sequence, abort, unlogged/init
# fork, gin, gist, spgist, brin, vacuum, logical message, relmap, and database
# records.
_WORKLOAD = """\
-- heap, btree, hash, sequence
CREATE TABLE t1 (a int GENERATED ALWAYS AS IDENTITY, b text);
CREATE INDEX i1a ON t1 USING btree (a);
CREATE INDEX i1b ON t1 USING hash (b);
INSERT INTO t1 VALUES (default, 'one'), (default, 'two');
DELETE FROM t1 WHERE b = 'one';
TRUNCATE t1;

-- abort
START TRANSACTION;
INSERT INTO t1 VALUES (default, 'three');
ROLLBACK;

-- unlogged/init fork
CREATE UNLOGGED TABLE t2 (x int);
CREATE INDEX i2 ON t2 USING btree (x);
INSERT INTO t2 SELECT generate_series(1, 10);

-- gin
CREATE TABLE gin_idx_tbl (id bigserial PRIMARY KEY, data jsonb);
CREATE INDEX gin_idx ON gin_idx_tbl USING gin (data);
INSERT INTO gin_idx_tbl
    WITH random_json AS (
        SELECT json_object_agg(key, trunc(random() * 10)) as json_data
            FROM unnest(array['a', 'b', 'c']) as u(key))
          SELECT generate_series(1,500), json_data FROM random_json;

-- gist, spgist
CREATE TABLE gist_idx_tbl (p point);
CREATE INDEX gist_idx ON gist_idx_tbl USING gist (p);
CREATE INDEX spgist_idx ON gist_idx_tbl USING spgist (p);
INSERT INTO gist_idx_tbl (p) VALUES (point '(1, 1)'), (point '(3, 2)'), (point '(6, 3)');

-- brin
CREATE TABLE brin_idx_tbl (col1 int, col2 text, col3 text );
CREATE INDEX brin_idx ON brin_idx_tbl USING brin (col1, col2, col3) WITH (autosummarize=on);
INSERT INTO brin_idx_tbl SELECT generate_series(1, 10000), 'dummy', 'dummy';
UPDATE brin_idx_tbl SET col2 = 'updated' WHERE col1 BETWEEN 1 AND 5000;
SELECT brin_summarize_range('brin_idx', 0);
SELECT brin_desummarize_range('brin_idx', 0);

VACUUM;

-- logical message
SELECT pg_logical_emit_message(true, 'foo', 'bar');

-- relmap
VACUUM FULL pg_authid;

-- database
CREATE DATABASE d1;
DROP DATABASE d1;
"""

# Consume remaining room in the current WAL segment, leaving space enough only
# for the start of a largish record (sets up a contrecord that spans segments).
_FILL_SEGMENT = """\
DO $$
DECLARE
    wal_segsize int := setting::int FROM pg_settings WHERE name = 'wal_segment_size';
    remain int;
    iters  int := 0;
BEGIN
    LOOP
        INSERT into t1(b)
        select repeat(encode(sha256(g::text::bytea), 'hex'), (random() * 15 + 1)::int)
        from generate_series(1, 10) g;

        remain := wal_segsize - (pg_current_wal_insert_lsn() - '0/0') % wal_segsize;
        IF remain < 2 * setting::int from pg_settings where name = 'block_size' THEN
            RAISE log 'exiting after % iterations, % bytes to end of WAL segment', iters, remain;
            EXIT;
        END IF;
        iters := iters + 1;
    END LOOP;
END
$$;
"""


def _tar_portability_options(tar):
    """Return tar flags forcing a readable ustar archive (cf. Utils helper)."""
    if not tar:
        return []
    devnull = os.devnull
    ustar = subprocess.run(
        [tar, "--format=ustar", "--owner=0", "--group=0", "-cf", devnull, devnull],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    if ustar.returncode == 0:
        return ["--format=ustar", "--owner=0", "--group=0"]
    bsd = subprocess.run(
        [tar, "-F", "ustar", "-cf", devnull, devnull],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    if bsd.returncode == 0:
        return ["-F", "ustar"]
    return []


def _generate_archive(tar, tar_p_flags, archive, directory, compression_flags):
    """Create a tar archive of directory's entries in a shuffled order."""
    files = [e for e in os.listdir(directory) if e not in (".", "..")]
    random.shuffle(files)
    # tar is invoked from inside the WAL directory so the archived members are
    # stored with bare names (mirrors the Perl chdir before command_ok).
    result = subprocess.run(
        [tar, *tar_p_flags, compression_flags, archive, *files],
        cwd=directory,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    assert result.returncode == 0, "tar archive created: {}".format(
        result.stderr.decode("utf-8", "replace")
    )


def _run_waldump(pg_bin, *args):
    """Run pg_waldump capturing (rc, stdout, stderr); never raises."""
    return pg_bin.result(["pg_waldump", *args])


def _test_pg_waldump_skip_bytes(pg_bin, path, startlsn, endlsn):
    """Starting one byte past a record boundary prints a 'skipping' message."""
    part1, part2 = startlsn.split("/")
    new_start = "{}/{:X}".format(part1, int(part2, 16) + 1)
    result = _run_waldump(pg_bin, "--start", new_start, "--end", endlsn, "--path", path)
    assert result.rc == 0, "runs with start segment and start LSN specified"
    assert re.search(r"first record is after", result.stderr), "info message printed"


def _test_pg_waldump(pg_bin, path, startlsn, endlsn, *opts):
    """Run pg_waldump over a range; assert clean run and return stdout lines."""
    result = _run_waldump(
        pg_bin, "--start", startlsn, "--end", endlsn, "--path", path, *opts
    )
    assert result.rc == 0, "pg_waldump {}: runs ok".format(" ".join(opts))
    assert result.stderr == "", "pg_waldump {}: no stderr".format(" ".join(opts))
    lines = result.stdout.split("\n")
    if lines and lines[-1] == "":
        lines.pop()
    assert len(lines) > 0, "pg_waldump {}: some lines are output".format(" ".join(opts))
    return lines


def _basic_option_checks(pg_bin):
    """Help/version/option handling plus argument and option-value errors."""
    pg_bin.program_help_ok("pg_waldump")
    pg_bin.program_version_ok("pg_waldump")
    pg_bin.program_options_handling_ok("pg_waldump")

    pg_bin.command_fails_like(["pg_waldump"], r"error: no arguments", "no arguments")
    pg_bin.command_fails_like(
        ["pg_waldump", "foo", "bar", "baz"],
        r"error: too many command-line arguments",
        "too many arguments",
    )
    pg_bin.command_fails_like(
        ["pg_waldump", "--block", "bad"],
        r"error: invalid block number",
        "invalid block number",
    )
    pg_bin.command_fails_like(
        ["pg_waldump", "--fork", "bad"],
        r"error: invalid fork name",
        "invalid fork name",
    )
    pg_bin.command_fails_like(
        ["pg_waldump", "--limit", "bad"], r"error: invalid value", "invalid limit"
    )
    pg_bin.command_fails_like(
        ["pg_waldump", "--relation", "bad"],
        r"error: invalid relation",
        "invalid relation specification",
    )
    pg_bin.command_fails_like(
        ["pg_waldump", "--rmgr", "bad"],
        r"error: resource manager .* does not exist",
        "invalid rmgr name",
    )
    pg_bin.command_fails_like(
        ["pg_waldump", "--start", "bad"],
        r"error: invalid WAL location",
        "invalid start LSN",
    )
    pg_bin.command_fails_like(
        ["pg_waldump", "--end", "bad"],
        r"error: invalid WAL location",
        "invalid end LSN",
    )
    pg_bin.command_like(
        ["pg_waldump", "--rmgr=list"],
        r"^" + _RMGR_LIST + r"$",
        "rmgr list",
    )


def _file_checks(pg_bin, node, start_walfile, end_walfile, tmp_path):
    """Range-by-file checks plus the invalid-magic-number broken-WAL check."""
    wal = os.path.join(node.datadir, "pg_wal")
    pg_bin.command_fails_like(
        ["pg_waldump", "foo", "bar"],
        r'error: could not locate WAL file "foo"',
        "start file not found",
    )
    pg_bin.command_like(
        ["pg_waldump", os.path.join(wal, start_walfile)],
        r".",
        "runs with start segment specified",
    )
    pg_bin.command_fails_like(
        ["pg_waldump", os.path.join(wal, start_walfile), "bar"],
        r'error: could not open file "bar"',
        "end file not found",
    )
    pg_bin.command_like(
        [
            "pg_waldump",
            os.path.join(wal, start_walfile),
            os.path.join(wal, end_walfile),
        ],
        r".",
        "runs with start and end segment specified",
    )
    pg_bin.command_like(
        ["pg_waldump", "--quiet", "--path", wal, start_walfile],
        r"^$",
        "no output with --quiet option",
    )

    broken_wal_dir = tmp_path / "broken_wal"
    broken_wal_dir.mkdir()
    broken_wal = broken_wal_dir / start_walfile
    shutil.copy(os.path.join(wal, start_walfile), broken_wal)
    with open(broken_wal, "r+b") as fh:
        fh.seek(0)
        fh.write(struct.pack("<H", 0))
    pg_bin.command_fails_like(
        ["pg_waldump", str(broken_wal)],
        r"(?i)invalid magic number 0000",
        "detailed error message shown for invalid WAL page magic",
    )


def _scenario_checks(pg_bin, path, oids, lsns):
    """All per-scenario range/option assertions for a given WAL source path."""
    start_lsn, contrecord_lsn, end_lsn = lsns
    default_ts_oid, postgres_db_oid, rel_t1_oid, rel_i1a_oid = oids

    pg_bin.command_fails_like(
        ["pg_waldump", "--path", path],
        r"error: no start WAL location given",
        "path option requires start location",
    )
    pg_bin.command_like(
        ["pg_waldump", "--path", path, "--start", start_lsn, "--end", end_lsn],
        r".",
        "runs with path option and start and end locations",
    )
    pg_bin.command_fails_like(
        ["pg_waldump", "--path", path, "--start", start_lsn],
        r"error: error in WAL record at",
        "falling off the end of the WAL results in an error",
    )
    pg_bin.command_fails_like(
        ["pg_waldump", "--quiet", "--path", path, "--start", start_lsn],
        r"error: error in WAL record at",
        "errors are shown with --quiet",
    )

    _test_pg_waldump_skip_bytes(pg_bin, path, start_lsn, end_lsn)

    lines = _test_pg_waldump(pg_bin, path, start_lsn, end_lsn)
    assert (
        sum(1 for ln in lines if not re.match(r"^rmgr: \w", ln)) == 0
    ), "all output lines are rmgr lines"

    lines = _test_pg_waldump(pg_bin, path, contrecord_lsn, end_lsn)
    assert (
        sum(1 for ln in lines if not re.match(r"^rmgr: \w", ln)) == 0
    ), "all output lines are rmgr lines"

    _test_pg_waldump_skip_bytes(pg_bin, path, contrecord_lsn, end_lsn)

    lines = _test_pg_waldump(pg_bin, path, start_lsn, end_lsn, "--limit", "6")
    assert len(lines) == 6, "limit option observed"

    lines = _test_pg_waldump(pg_bin, path, start_lsn, end_lsn, "--fullpage")
    assert (
        sum(1 for ln in lines if not re.search(r"^rmgr:.*\bFPW\b", ln)) == 0
    ), "all output lines are FPW"

    lines = _test_pg_waldump(pg_bin, path, start_lsn, end_lsn, "--stats")
    assert re.search(r"WAL statistics", lines[0]), "statistics on stdout"
    assert sum(1 for ln in lines if re.match(r"^rmgr:", ln)) == 0, "no rmgr lines"

    lines = _test_pg_waldump(pg_bin, path, start_lsn, end_lsn, "--stats=record")
    assert re.search(r"WAL statistics", lines[0]), "statistics on stdout"
    assert sum(1 for ln in lines if re.match(r"^rmgr:", ln)) == 0, "no rmgr lines"

    lines = _test_pg_waldump(pg_bin, path, start_lsn, end_lsn, "--rmgr", "Btree")
    assert (
        sum(1 for ln in lines if not re.match(r"^rmgr: Btree", ln)) == 0
    ), "only Btree lines"

    lines = _test_pg_waldump(pg_bin, path, start_lsn, end_lsn, "--fork", "init")
    assert (
        sum(1 for ln in lines if not re.search(r"fork init", ln)) == 0
    ), "only init fork lines"

    rel = "{}/{}/{}".format(default_ts_oid, postgres_db_oid, rel_t1_oid)
    lines = _test_pg_waldump(pg_bin, path, start_lsn, end_lsn, "--relation", rel)
    assert (
        sum(1 for ln in lines if not re.search(r"rel " + re.escape(rel), ln)) == 0
    ), "only lines for selected relation"

    rel = "{}/{}/{}".format(default_ts_oid, postgres_db_oid, rel_i1a_oid)
    lines = _test_pg_waldump(
        pg_bin, path, start_lsn, end_lsn, "--relation", rel, "--block", "1"
    )
    assert (
        sum(1 for ln in lines if not re.search(r"\bblk 1\b", ln)) == 0
    ), "only lines for selected block"


def test_001_basic(pg_bin, create_pg, tmp_path):
    """pg_waldump option validation and WAL decoding from dir and archives."""
    _basic_option_checks(pg_bin)

    node = create_pg("main", start=False)
    node.append_conf(
        "\n".join(
            [
                "autovacuum = off",
                "checkpoint_timeout = 1h",
                "archive_mode=on",
                "archive_command=''",
                "wal_level=logical",
            ]
        )
    )
    node.start()

    start_lsn, start_walfile = node.safe_psql(
        "SELECT pg_current_wal_insert_lsn(), "
        "pg_walfile_name(pg_current_wal_insert_lsn())"
    ).split("|")

    node.safe_psql(_WORKLOAD)

    tblspc_path = tmp_path / "tblspc"
    tblspc_path.mkdir()
    node.safe_psql(
        "CREATE TABLESPACE ts1 LOCATION '{}';\n"
        "DROP TABLESPACE ts1;".format(tblspc_path)
    )

    node.safe_psql(_FILL_SEGMENT)

    contrecord_lsn = node.safe_psql("SELECT pg_current_wal_insert_lsn()")
    node.safe_psql(
        "SELECT pg_logical_emit_message(true, 'test 026', repeat('xyzxz', 123456))"
    )

    end_lsn, end_walfile = node.safe_psql(
        "SELECT pg_current_wal_insert_lsn(), "
        "pg_walfile_name(pg_current_wal_insert_lsn())"
    ).split("|")

    default_ts_oid = node.safe_psql(
        "SELECT oid FROM pg_tablespace WHERE spcname = 'pg_default'"
    )
    postgres_db_oid = node.safe_psql(
        "SELECT oid FROM pg_database WHERE datname = 'postgres'"
    )
    rel_t1_oid = node.safe_psql("SELECT oid FROM pg_class WHERE relname = 't1'")
    rel_i1a_oid = node.safe_psql("SELECT oid FROM pg_class WHERE relname = 'i1a'")

    node.stop()

    _file_checks(pg_bin, node, start_walfile, end_walfile, tmp_path)

    oids = (default_ts_oid, postgres_db_oid, rel_t1_oid, rel_i1a_oid)
    lsns = (start_lsn, contrecord_lsn, end_lsn)
    tar = os.environ.get("TAR", "")
    tar_p_flags = _tar_portability_options(tar)
    have_libz = pg_bin.check_pg_config("#define HAVE_LIBZ 1")

    scenarios = [
        {"path": str(node.datadir), "is_archive": False, "enabled": True},
        {
            "path": str(tmp_path / "pg_wal.tar"),
            "compression_method": "none",
            "compression_flags": "-cf",
            "is_archive": True,
            "enabled": True,
        },
        {
            "path": str(tmp_path / "pg_wal.tar.gz"),
            "compression_method": "gzip",
            "compression_flags": "-czf",
            "is_archive": True,
            "enabled": have_libz,
        },
    ]

    for scenario in scenarios:
        path = str(scenario["path"])
        if scenario["is_archive"] and not tar:
            continue
        if scenario["is_archive"] and not scenario["enabled"]:
            continue
        if scenario["is_archive"]:
            _generate_archive(
                tar,
                tar_p_flags,
                path,
                os.path.join(node.datadir, "pg_wal"),
                scenario["compression_flags"],
            )
        _scenario_checks(pg_bin, path, oids, lsns)
        if scenario["is_archive"] and os.path.exists(path):
            os.unlink(path)

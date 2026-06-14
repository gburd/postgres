# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/bin/pg_dump/t/006_pg_dump_compress.pl.

Compression-focused pg_dump/pg_restore matrix: every supported compression
method (none/gzip/lz4/zstd) in custom, directory and plain formats, with
coverage for manually (de)compressed TOC/data files and verification that the
dumped SQL matches the expected per-object regexes. Runs that need a build
option absent from this build are skipped exactly as the Perl original does.
"""

import glob as globmod
import os
import re
import subprocess
from typing import Any, Dict

import pypg


def _supports(option):
    """Return True if the build was compiled with the given option."""
    defines = {
        "gzip": r"#define HAVE_LIBZ 1",
        "lz4": r"#define USE_LZ4 1",
        "zstd": r"#define USE_ZSTD 1",
    }
    return pypg.check_pg_config(defines[option])


# Object-creation SQL and the per-object dump regexes (the %tests matrix).
_TESTS: Dict[str, Dict[str, Any]] = {
    "matview_compression_lz4": {
        "create_order": 20,
        "create_sql": (
            "CREATE MATERIALIZED VIEW\n"
            "    matview_compression_lz4 (col2) AS\n"
            "    SELECT repeat('xyzzy', 10000);\n"
            "ALTER MATERIALIZED VIEW matview_compression_lz4\n"
            "ALTER COLUMN col2 SET COMPRESSION lz4;"
        ),
        "regexp": re.compile(
            r"^"
            + re.escape("CREATE MATERIALIZED VIEW public.matview_compression_lz4 AS")
            + r"\n\s+"
            + re.escape("SELECT repeat('xyzzy'::text, 10000) AS col2")
            + r"\n\s+"
            + re.escape("WITH NO DATA;")
            + r".*"
            + re.escape(
                "ALTER TABLE ONLY public.matview_compression_lz4 ALTER COLUMN "
                "col2 SET COMPRESSION lz4;"
            )
            + r"\n",
            re.DOTALL | re.MULTILINE,
        ),
        "compile_option": "lz4",
        "like": True,
    },
    "test_compression_method_create": {
        "create_order": 110,
        "create_sql": "CREATE TABLE test_compression_method (\n    col1 text\n);",
        "regexp": re.compile(
            r"^"
            + re.escape("CREATE TABLE public.test_compression_method (")
            + r"\n\s+"
            + re.escape("col1 text")
            + r"\n"
            + re.escape(");"),
            re.MULTILINE,
        ),
        "like": True,
    },
    "test_compression_method_copy": {
        "create_order": 111,
        "create_sql": (
            "INSERT INTO test_compression_method (col1) "
            "SELECT string_agg(a::text, '') FROM generate_series(1,65536) a;"
        ),
        "regexp": re.compile(
            r"^"
            + re.escape("COPY public.test_compression_method (col1) FROM stdin;")
            + r"\n(?:(?:\d\d\d\d\d\d\d\d\d\d){31657}\d\d\d\d\n){1}\\\.\n",
            re.MULTILINE,
        ),
        "like": True,
    },
    "test_compression_create": {
        "create_order": 3,
        "create_sql": (
            "CREATE TABLE test_compression (\n"
            "    col1 int,\n"
            "    col2 text COMPRESSION lz4\n"
            ");"
        ),
        "regexp": re.compile(
            r"^"
            + re.escape("CREATE TABLE public.test_compression (")
            + r"\n\s+"
            + re.escape("col1 integer,")
            + r"\n\s+"
            + re.escape("col2 text")
            + r"\n"
            + re.escape(");")
            + r"\n.*"
            + re.escape(
                "ALTER TABLE ONLY public.test_compression ALTER COLUMN "
                "col2 SET COMPRESSION lz4;"
            )
            + r"\n",
            re.DOTALL | re.MULTILINE,
        ),
        "compile_option": "lz4",
        "like": True,
    },
    "lo_create": {
        "create_order": 50,
        "create_sql": (
            "SELECT pg_catalog.lo_from_bytea(0, "
            "'\\x310a320a330a340a350a360a370a380a390a');"
        ),
        "regexp": re.compile(r"^SELECT pg_catalog\.lo_create\('\d+'\);", re.MULTILINE),
        "like": True,
    },
    "lo_load": {
        "regexp": re.compile(
            r"^"
            + re.escape("SELECT pg_catalog.lo_open")
            + r"\('\d+', \d+\);\n"
            + re.escape("SELECT pg_catalog.lowrite(0, ")
            + re.escape("'\\x310a320a330a340a350a360a370a380a390a');")
            + r"\n"
            + re.escape("SELECT pg_catalog.lo_close(0);"),
            re.MULTILINE,
        ),
        "like": True,
    },
}


def _setup_objects(node):
    """Create dumped objects in create_order, skipping unsupported options."""
    creatable = [
        (name, spec) for name, spec in _TESTS.items() if spec.get("create_sql")
    ]
    creatable.sort(key=lambda item: item[1].get("create_order", 1 << 30))
    create_sql = ""
    for _name, spec in creatable:
        option = spec.get("compile_option")
        if option and not _supports(option):
            continue
        sql = str(spec["create_sql"]).rstrip("\n")
        if not sql.endswith(";"):
            sql += ";"
        create_sql += sql + "\n\n"
    node.safe_psql(create_sql)


def _dump_runs(tempdir):
    """Return the %pgdump_runs matrix (dump/restore/compress/glob/command)."""
    runs: Dict[str, Dict[str, Any]] = {}
    runs["compression_none_custom"] = {
        "dump_cmd": [
            "pg_dump",
            "--no-sync",
            "--format",
            "custom",
            "--compress",
            "none",
            "--file",
            tempdir + "/compression_none_custom.dump",
            "--statistics",
            "postgres",
        ],
        "restore_cmd": [
            "pg_restore",
            "--file",
            tempdir + "/compression_none_custom.sql",
            "--statistics",
            tempdir + "/compression_none_custom.dump",
        ],
    }
    for method, ext, prog_env in (
        ("gzip", "gz", "GZIP_PROGRAM"),
        ("lz4", "lz4", "LZ4"),
        ("zstd", "zst", "ZSTD"),
    ):
        runs.update(_method_runs(tempdir, method, ext, prog_env))
    return runs


def _method_runs(tempdir, method, ext, prog_env):
    """The custom/dir/plain runs for one compression method."""
    base = tempdir + "/compression_" + method
    custom_compress = "1" if method == "gzip" else method
    dir_compress = {"gzip": "gzip:1", "lz4": "lz4:1", "zstd": "zstd:1"}[method]
    plain_compress = {"gzip": "1", "lz4": "lz4", "zstd": "zstd:long"}[method]
    return {
        "compression_{}_custom".format(method): {
            "compile_option": method,
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                "--format",
                "custom",
                "--compress",
                custom_compress,
                "--file",
                base + "_custom.dump",
                "--statistics",
                "postgres",
            ],
            "restore_cmd": [
                "pg_restore",
                "--file",
                base + "_custom.sql",
                "--statistics",
                base + "_custom.dump",
            ],
            "command_like": {
                "command": ["pg_restore", "--list", base + "_custom.dump"],
                "expected": r"Compression: {}".format(method),
                "name": "data content is {} compressed".format(method),
            },
        },
        "compression_{}_dir".format(method): {
            "compile_option": method,
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                "--jobs",
                "2",
                "--format",
                "directory",
                "--compress",
                dir_compress,
                "--file",
                base + "_dir",
                "--statistics",
                "postgres",
            ],
            "compress_cmd": {
                "program_env": prog_env,
                "args": _dir_compress_args(method, base + "_dir"),
            },
            "glob_patterns": [
                base + "_dir/toc.dat." + ext,
                base + "_dir/*.dat." + ext,
            ],
            "restore_cmd": [
                "pg_restore",
                "--jobs",
                "2",
                "--file",
                base + "_dir.sql",
                "--statistics",
                base + "_dir",
            ],
        },
        "compression_{}_plain".format(method): {
            "compile_option": method,
            "dump_cmd": [
                "pg_dump",
                "--no-sync",
                "--format",
                "plain",
                "--compress",
                plain_compress,
                "--file",
                base + "_plain.sql." + ext,
                "--statistics",
                "postgres",
            ],
            "compress_cmd": {
                "program_env": prog_env,
                "args": _plain_decompress_args(method, base + "_plain.sql." + ext),
            },
        },
    }


def _dir_compress_args(method, dirpath):
    """Arguments to manually compress a directory dump's TOC files."""
    toc = dirpath + "/toc.dat"
    blobs = dirpath + "/blobs_*.toc"
    if method == "gzip":
        return ["-f", toc, blobs]
    if method == "lz4":
        return ["-z", "-f", "-m", "--rm", toc, blobs]
    return ["-z", "-f", "--rm", toc, blobs]


def _plain_decompress_args(method, path):
    """Arguments to decompress a plain dump back to a .sql we can scan."""
    out = path.rsplit(".", 1)[0]
    if method == "gzip":
        return ["-d", path]
    if method == "lz4":
        return ["-d", "-f", path, out]
    return ["-d", "-f", path, "-o", out]


def _run_compress_cmd(node, run, spec):
    """Run the manual (de)compression command; return False to skip the run."""
    compress = spec.get("compress_cmd")
    if not compress:
        return True
    program = os.environ.get(compress["program_env"], "")
    if not program:
        return False
    full = [program]
    for arg in compress["args"]:
        matches = globmod.glob(arg)
        full += matches if matches else [arg]
    result = subprocess.run(full, capture_output=True, check=False)
    assert result.returncode == 0, "{}: compression commands\n{}".format(
        run, result.stderr.decode("utf-8", "replace")
    )
    return True


def _check_glob_patterns(run, spec):
    """Assert each glob pattern matched at least one real file."""
    for pattern in spec.get("glob_patterns", []):
        matches = globmod.glob(pattern)
        ok = len(matches) > 1 or (len(matches) == 1 and os.path.isfile(matches[0]))
        assert ok, "{}: glob check for {}".format(run, pattern)


def _check_dump_output(node, run, spec, tempdir):
    """Match each enabled %tests regexp as a like/unlike against the dump."""
    output = pypg.slurp_file("{}/{}.sql".format(tempdir, run))
    for test, tspec in sorted(_TESTS.items()):
        option = tspec.get("compile_option")
        if option and not _supports(option):
            continue
        if tspec.get("like"):
            assert tspec["regexp"].search(output), "{}: should dump {}".format(
                run, test
            )
        else:
            assert not tspec["regexp"].search(output), "{}: should not dump {}".format(
                run, test
            )


def test_006_pg_dump_compress(create_pg, tmp_path):
    """pg_dump/pg_restore compression matrix matches expected dump output."""
    tempdir = str(tmp_path)
    node = create_pg("main")
    _setup_objects(node)

    runs = _dump_runs(tempdir)
    for run in sorted(runs):
        spec = runs[run]
        option = spec.get("compile_option")
        if option and not _supports(option):
            continue
        node.command_ok(spec["dump_cmd"], "{}: pg_dump runs".format(run))
        if not _run_compress_cmd(node, run, spec):
            continue
        _check_glob_patterns(run, spec)
        cmd_like = spec.get("command_like")
        if cmd_like:
            node.command_like(
                cmd_like["command"],
                cmd_like["expected"],
                "{}: {}".format(run, cmd_like["name"]),
            )
        if spec.get("restore_cmd"):
            node.command_ok(spec["restore_cmd"], "{}: pg_restore runs".format(run))
        _check_dump_output(node, run, spec, tempdir)

    node.stop("fast")

# Copyright (c) 2021-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long
"""Port of src/bin/pg_dump/t/003_pg_dump_with_server.pl.

pg_dump --include-foreign-data against a live server: dumping a foreign table
whose FDW ('dummy') has no handler fails with a clear error naming the table,
while dumping a foreign server that has no tables (data-only) succeeds.
"""


def test_003_pg_dump_with_server(create_pg):
    """pg_dump --include-foreign-data error and empty-server success paths."""
    node = create_pg("main")
    port = node.port
    node.safe_psql("CREATE FOREIGN DATA WRAPPER dummy")
    node.safe_psql("CREATE SERVER s0 FOREIGN DATA WRAPPER dummy")
    node.safe_psql("CREATE SERVER s1 FOREIGN DATA WRAPPER dummy")
    node.safe_psql("CREATE SERVER s2 FOREIGN DATA WRAPPER dummy")
    node.safe_psql("CREATE FOREIGN TABLE t0 (a int) SERVER s0")
    node.safe_psql("CREATE FOREIGN TABLE t1 (a int) SERVER s1")
    pg_bin = node.bin
    pg_bin.command_fails_like(
        ["pg_dump", "--port", str(port), "--include-foreign-data", "s0", "postgres"],
        r'foreign-data wrapper "dummy" has no handler\r?\npg_dump: detail: Query was: .*t0',
        "correctly fails to dump a foreign table from a dummy FDW",
    )
    pg_bin.command_ok(
        [
            "pg_dump",
            "--port",
            str(port),
            "--data-only",
            "--include-foreign-data",
            "s2",
            "postgres",
        ],
        "dump foreign server with no tables",
    )

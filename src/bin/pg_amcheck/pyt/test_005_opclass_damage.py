# Copyright (c) 2022-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long,too-many-statements
"""Port of src/bin/pg_amcheck/t/005_opclass_damage.pl.

pg_amcheck detects btree indexes whose ordering depends on a fickle (non-deterministic) operator class, and validates unique-constraint checking.
Generated from the Perl original via .agent/gen_golden.py.
"""


def test_005_opclass_damage(create_pg):
    """pg_amcheck detects btree indexes whose ordering depends on a fickle (non-deterministic) operator class."""
    node = create_pg("test", start=False)
    node.start()
    node.safe_psql(
        "CREATE EXTENSION amcheck;\n\n\tCREATE FUNCTION int4_asc_cmp (a int4, b int4) RETURNS int LANGUAGE sql AS $$\n\t\tSELECT CASE WHEN $1 = $2 THEN 0 WHEN $1 > $2 THEN 1 ELSE -1 END; $$;\n\n\tCREATE FUNCTION ok_cmp (int4, int4)\n\tRETURNS int LANGUAGE sql AS\n\t$$\n\t\tSELECT\n\t\t\tCASE WHEN $1 < $2 THEN -1\n\t\t\t\t WHEN $1 > $2 THEN  1\n\t\t\t\t ELSE 0\n\t\t\tEND;\n\t$$;\n\n\tCREATE OPERATOR CLASS int4_fickle_ops FOR TYPE int4 USING btree AS\n\t    OPERATOR 1 < (int4, int4), OPERATOR 2 <= (int4, int4),\n\t    OPERATOR 3 = (int4, int4), OPERATOR 4 >= (int4, int4),\n\t    OPERATOR 5 > (int4, int4), FUNCTION 1 int4_asc_cmp(int4, int4);\n\n\tCREATE OPERATOR CLASS int4_unique_ops FOR TYPE int4 USING btree AS\n\t\tOPERATOR 1 < (int4, int4), OPERATOR 2 <= (int4, int4),\n\t\tOPERATOR 3 = (int4, int4), OPERATOR 4 >= (int4, int4),\n\t\tOPERATOR 5 > (int4, int4), FUNCTION 1 ok_cmp(int4, int4);\n\n\tCREATE TABLE int4tbl (i int4);\n\tINSERT INTO int4tbl (SELECT * FROM generate_series(1,1000) gs);\n\tCREATE INDEX fickleidx ON int4tbl USING btree (i int4_fickle_ops);\n\tCREATE UNIQUE INDEX bttest_unique_idx\n\t\t\t\t\t\tON int4tbl\n\t\t\t\t\t\tUSING btree (i int4_unique_ops)\n\t\t\t\t\t\tWITH (deduplicate_items = off);"
    )
    node.command_like(
        ["pg_amcheck", "--port", str(node.port), "postgres"],
        r"""^$""",
        "pg_amcheck all schemas, tables and indexes reports no corruption",
    )
    node.safe_psql(
        "CREATE FUNCTION int4_desc_cmp (int4, int4) RETURNS int LANGUAGE sql AS $$\n\t\tSELECT CASE WHEN $1 = $2 THEN 0 WHEN $1 > $2 THEN -1 ELSE 1 END; $$;\n\tUPDATE pg_catalog.pg_amproc\n\t\tSET amproc = 'int4_desc_cmp'::regproc\n\t\tWHERE amproc = 'int4_asc_cmp'::regproc"
    )
    node.command_checks_all(
        ["pg_amcheck", "--port", str(node.port), "postgres"],
        2,
        [r'''item order invariant violated for index "fickleidx"'''],
        [],
        "pg_amcheck all schemas, tables and indexes reports fickleidx corruption",
    )
    node.safe_psql(
        "UPDATE pg_catalog.pg_amproc\n\t\tSET amproc = 'int4_asc_cmp'::regproc\n\t\tWHERE amproc = 'int4_desc_cmp'::regproc"
    )
    node.command_like(
        ["pg_amcheck", "--checkunique", "--port", str(node.port), "postgres"],
        r"""^$""",
        "pg_amcheck all schemas, tables and indexes reports no corruption",
    )
    node.safe_psql(
        "CREATE FUNCTION bad_cmp (int4, int4)\n\tRETURNS int LANGUAGE sql AS\n\t$$\n\t\tSELECT\n\t\t\tCASE WHEN ($1 = 768 AND $2 = 769) OR\n\t\t\t\t\t  ($1 = 769 AND $2 = 768) THEN 0\n\t\t\t\t WHEN $1 < $2 THEN -1\n\t\t\t\t WHEN $1 > $2 THEN  1\n\t\t\t\t ELSE 0\n\t\t\tEND;\n\t$$;\n\n\tUPDATE pg_catalog.pg_amproc\n\t\tSET amproc = 'bad_cmp'::regproc\n\t\tWHERE amproc = 'ok_cmp'::regproc"
    )
    node.command_checks_all(
        ["pg_amcheck", "--checkunique", "--port", str(node.port), "postgres"],
        2,
        [r'''index uniqueness is violated for index "bttest_unique_idx"'''],
        [],
        "pg_amcheck all schemas, tables and indexes reports bttest_unique_idx corruption",
    )

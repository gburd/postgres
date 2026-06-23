# Copyright (c) 2022-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long,too-many-statements
"""Port of contrib/amcheck/t/004_verify_nbtree_unique.pl.

amcheck bt_index_check uniqueness verification: a corrupted comparator that
makes distinct values compare equal is detected as a unique-index violation
(with and without deduplication).
Generated from the Perl original via .agent/gen_golden.py.
"""

import re


def test_004_verify_nbtree_unique(create_pg):
    """amcheck bt_index_check uniqueness verification: a corrupted comparator that."""
    node = create_pg("test", start=False)
    node.append_conf("autovacuum = off")
    node.start()
    node.safe_psql(
        "CREATE EXTENSION amcheck;\n\n\tCREATE FUNCTION ok_cmp (int4, int4)\n\tRETURNS int LANGUAGE sql AS\n\t$$\n\t\tSELECT\n\t\t\tCASE WHEN $1 < $2 THEN -1\n\t\t\t\t WHEN $1 > $2 THEN  1\n\t\t\t\t ELSE 0\n\t\t\tEND;\n\t$$;\n\n\t---\n\t--- Check 1: uniqueness violation.\n\t---\n\tCREATE FUNCTION ok_cmp1 (int4, int4)\n\tRETURNS int LANGUAGE sql AS\n\t$$\n\t\tSELECT public.ok_cmp($1, $2);\n\t$$;\n\n\t---\n\t--- Make values 768 and 769 look equal.\n\t---\n\tCREATE FUNCTION bad_cmp1 (int4, int4)\n\tRETURNS int LANGUAGE sql AS\n\t$$\n\t\tSELECT\n\t\t\tCASE WHEN ($1 = 768 AND $2 = 769) OR\n\t\t\t\t\t  ($1 = 769 AND $2 = 768) THEN 0\n\t\t\t\t ELSE public.ok_cmp($1, $2)\n\t\t\tEND;\n\t$$;\n\n\t---\n\t--- Check 2: uniqueness violation without deduplication.\n\t---\n\tCREATE FUNCTION ok_cmp2 (int4, int4)\n\tRETURNS int LANGUAGE sql AS\n\t$$\n\t\tSELECT public.ok_cmp($1, $2);\n\t$$;\n\n\tCREATE FUNCTION bad_cmp2 (int4, int4)\n\tRETURNS int LANGUAGE sql AS\n\t$$\n\t\tSELECT\n\t\t\tCASE WHEN $1 = $2 AND $1 = 400 THEN -1\n\t\t\tELSE public.ok_cmp($1, $2)\n\t\tEND;\n\t$$;\n\n\t---\n\t--- Check 3: uniqueness violation with deduplication.\n\t---\n\tCREATE FUNCTION ok_cmp3 (int4, int4)\n\tRETURNS int LANGUAGE sql AS\n\t$$\n\t\tSELECT public.ok_cmp($1, $2);\n\t$$;\n\n\tCREATE FUNCTION bad_cmp3 (int4, int4)\n\tRETURNS int LANGUAGE sql AS\n\t$$\n\t\tSELECT public.bad_cmp2($1, $2);\n\t$$;\n\n\t---\n\t--- Create data.\n\t---\n\tCREATE TABLE bttest_unique1 (i int4);\n\tINSERT INTO bttest_unique1\n\t\t(SELECT * FROM generate_series(1, 1024) gs);\n\n\tCREATE TABLE bttest_unique2 (i int4);\n\tINSERT INTO bttest_unique2(i)\n\t\t(SELECT * FROM generate_series(1, 400) gs);\n\tINSERT INTO bttest_unique2\n\t\t(SELECT * FROM generate_series(400, 1024) gs);\n\n\tCREATE TABLE bttest_unique3 (i int4);\n\tINSERT INTO bttest_unique3\n\t\tSELECT * FROM bttest_unique2;\n\n\tCREATE OPERATOR CLASS int4_custom_ops1 FOR TYPE int4 USING btree AS\n\t\tOPERATOR 1 < (int4, int4), OPERATOR 2 <= (int4, int4),\n\t\tOPERATOR 3 = (int4, int4), OPERATOR 4 >= (int4, int4),\n\t\tOPERATOR 5 > (int4, int4), FUNCTION 1 ok_cmp1(int4, int4);\n\tCREATE OPERATOR CLASS int4_custom_ops2 FOR TYPE int4 USING btree AS\n\t\tOPERATOR 1 < (int4, int4), OPERATOR 2 <= (int4, int4),\n\t\tOPERATOR 3 = (int4, int4), OPERATOR 4 >= (int4, int4),\n\t\tOPERATOR 5 > (int4, int4), FUNCTION 1 bad_cmp2(int4, int4);\n\tCREATE OPERATOR CLASS int4_custom_ops3 FOR TYPE int4 USING btree AS\n\t\tOPERATOR 1 < (int4, int4), OPERATOR 2 <= (int4, int4),\n\t\tOPERATOR 3 = (int4, int4), OPERATOR 4 >= (int4, int4),\n\t\tOPERATOR 5 > (int4, int4), FUNCTION 1 bad_cmp3(int4, int4);\n\n\tCREATE UNIQUE INDEX bttest_unique_idx1\n\t\t\t\t\t\tON bttest_unique1\n\t\t\t\t\t\tUSING btree (i int4_custom_ops1)\n\t\t\t\t\t\tWITH (deduplicate_items = off);\n\tCREATE UNIQUE INDEX bttest_unique_idx2\n\t\t\t\t\t\tON bttest_unique2\n\t\t\t\t\t\tUSING btree (i int4_custom_ops2)\n\t\t\t\t\t\tWITH (deduplicate_items = off);\n\tCREATE UNIQUE INDEX bttest_unique_idx3\n\t\t\t\t\t\tON bttest_unique3\n\t\t\t\t\t\tUSING btree (i int4_custom_ops3)\n\t\t\t\t\t\tWITH (deduplicate_items = on);"
    )
    result = node.safe_psql("SELECT bt_index_check('bttest_unique_idx1', true, true);")
    assert result == "", "run amcheck on non-broken bttest_unique_idx1"
    node.safe_psql(
        "UPDATE pg_catalog.pg_amproc SET\n\t\t   amproc = 'bad_cmp1'::regproc\n\tWHERE amproc = 'ok_cmp1'::regproc;"
    )
    result = node.psql_capture(
        "SELECT bt_index_check('bttest_unique_idx1', true, true);"
    )
    assert re.search(
        r'''index uniqueness is violated for index "bttest_unique_idx1"''',
        result.stderr,
    ), 'detected uniqueness violation for index "bttest_unique_idx1"'
    result = node.psql_capture(
        "SELECT bt_index_check('bttest_unique_idx2', true, true);"
    )
    assert re.search(
        r'''item order invariant violated for index "bttest_unique_idx2"''',
        result.stderr,
    ), 'detected item order invariant violation for index "bttest_unique_idx2"'
    node.safe_psql(
        "UPDATE pg_catalog.pg_amproc SET\n\t\t   amproc = 'ok_cmp2'::regproc\n\tWHERE amproc = 'bad_cmp2'::regproc;"
    )
    result = node.psql_capture(
        "SELECT bt_index_check('bttest_unique_idx2', true, true);"
    )
    assert re.search(
        r'''index uniqueness is violated for index "bttest_unique_idx2"''',
        result.stderr,
    ), 'detected uniqueness violation for index "bttest_unique_idx2"'
    result = node.psql_capture(
        "SELECT bt_index_check('bttest_unique_idx3', true, true);"
    )
    assert re.search(
        r'''item order invariant violated for index "bttest_unique_idx3"''',
        result.stderr,
    ), 'detected item order invariant violation for index "bttest_unique_idx3"'
    node.safe_psql(
        "DELETE FROM bttest_unique3 WHERE 380 <= i AND i <= 420;\n\tINSERT INTO bttest_unique3 (SELECT * FROM generate_series(380, 420));\n\tINSERT INTO bttest_unique3 VALUES (400);\n\tDELETE FROM bttest_unique3 WHERE 380 <= i AND i <= 420;\n\tINSERT INTO bttest_unique3 (SELECT * FROM generate_series(380, 420));\n\tINSERT INTO bttest_unique3 VALUES (400);\n\tDELETE FROM bttest_unique3 WHERE 380 <= i AND i <= 420;\n\tINSERT INTO bttest_unique3 (SELECT * FROM generate_series(380, 420));\n\tINSERT INTO bttest_unique3 VALUES (400);"
    )
    node.safe_psql(
        "UPDATE pg_catalog.pg_amproc SET\n\t\t   amproc = 'ok_cmp3'::regproc\n\tWHERE amproc = 'bad_cmp3'::regproc;"
    )
    result = node.psql_capture(
        "SELECT bt_index_check('bttest_unique_idx3', true, true);"
    )
    assert re.search(
        r'''index uniqueness is violated for index "bttest_unique_idx3"''',
        result.stderr,
    ), 'detected uniqueness violation for index "bttest_unique_idx3"'
    node.stop()

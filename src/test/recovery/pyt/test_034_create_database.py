# Copyright (c) 2023-2026, PostgreSQL Global Development Group

"""Port of src/test/recovery/t/034_create_database.pl.

Test WAL replay for CREATE DATABASE .. STRATEGY WAL_LOG.
"""


def test_create_database(create_pg):
    """DDL on the template persists; the new database has no template tables."""
    node = create_pg("node")

    db_template = "template1"
    db_new = "test_db_1"

    # DDLs on the template database that modify pg_class must persist after
    # creating a database from it with the WAL_LOG strategy (a direct copy of
    # the template's pg_class is used).
    node.safe_psql(
        "CREATE DATABASE {} STRATEGY WAL_LOG TEMPLATE {};".format(db_new, db_template)
    )
    node.safe_psql("CREATE TABLE tab_db_after_create_1 (a INT);", dbname=db_template)

    # Flush the changes affecting the template database, then replay them.
    node.safe_psql("CHECKPOINT;")
    node.stop("immediate")
    node.start()

    assert (
        node.safe_psql(
            "SELECT count(*) FROM pg_class WHERE relname LIKE 'tab_db_%';",
            dbname=db_template,
        )
        == "1"
    ), "table exists on template after crash, with checkpoint"

    assert (
        node.safe_psql(
            "SELECT count(*) FROM pg_class WHERE relname LIKE 'tab_db_%';",
            dbname=db_new,
        )
        == "0"
    ), "no tables from template on new database after crash"

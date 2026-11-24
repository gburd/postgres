# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of contrib/pg_visibility/t/002_corrupt_vm.pl.

A stale visibility map exposes corruption: after freezing a table, the _vm fork
is saved, more rows are deleted (updating the real VM), then the old _vm is
restored so it disagrees with the heap. pg_check_visible and pg_check_frozen
must then report exactly the tuples whose visibility/frozen bits are now wrong.
"""

import shutil


def test_002_corrupt_vm(create_pg):
    """pg_check_visible/pg_check_frozen detect a restored, stale visibility map."""
    node = create_pg("main", start=False)
    node.append_conf("autovacuum=off")
    node.start()
    blck_size = node.safe_psql("SHOW block_size;")
    node.safe_psql(
        f"""
                CREATE EXTENSION pg_visibility;
                CREATE TABLE corruption_test
                        WITH (autovacuum_enabled = false) AS
                        SELECT
                                i,
                                repeat('a', 10) AS data
                        FROM
                                generate_series(1, {blck_size}) i;
                VACUUM (FREEZE, DISABLE_PAGE_SKIPPING) corruption_test;
        """
    )
    npages = node.safe_psql(
        "SELECT relpages FROM pg_class WHERE relname = 'corruption_test';"
    )
    assert int(npages) >= 10, "table has at least 10 pages"
    relfile = node.safe_psql("SELECT pg_relation_filepath('corruption_test');")
    node.safe_psql("DELETE FROM corruption_test WHERE (ctid::text::point)[0] = 0;")
    node.stop()
    vm_file = f"{node.datadir}/{relfile}_vm"
    shutil.copy(vm_file, f"{vm_file}_temp")
    node.start()
    tuples = node.safe_psql(
        """SELECT ctid FROM (
                SELECT ctid FROM corruption_test
                        WHERE (ctid::text::point)[0] != 0
                        ORDER BY random() LIMIT 5)
                ORDER BY ctid ASC;"""
    )
    # Perl: s/\n/,/g; s/\(/'(/g; s/\)/)'/g -- build a quoted ctid IN-list.
    tuples_query = tuples.replace("\n", ",").replace("(", "'(").replace(")", ")'")
    node.safe_psql(f"DELETE FROM corruption_test WHERE ctid in ({tuples_query});")
    node.stop()
    shutil.move(f"{vm_file}_temp", vm_file)
    node.start()
    result = node.safe_psql(
        """SELECT DISTINCT t_ctid
                FROM pg_check_visible('corruption_test')
                ORDER BY t_ctid ASC;"""
    )
    assert result == tuples, "pg_check_visible must report tuples as corrupted"
    result = node.safe_psql(
        """SELECT DISTINCT t_ctid
                FROM pg_check_frozen('corruption_test')
                ORDER BY t_ctid ASC;"""
    )
    assert result == tuples, "pg_check_frozen must report tuples as corrupted"
    node.stop()

# Copyright (c) 2023-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long
"""Port of src/test/modules/xid_wraparound/t/001_emergency_vacuum.pl.

Emergency (failsafe) autovacuum: with a long-running transaction pinning the
xmin horizon, consume XIDs until every database ages past vacuum_failsafe_age.
Once the old transaction commits, autovacuum must vacuum every table and log the
failsafe "bypassing nonessential maintenance" message for each table. Gated on
PG_TEST_EXTRA=xid_wraparound (slow: consumes ~2 billion XIDs).
"""

import re

import pypg

pytestmark = pypg.require_test_extras("xid_wraparound")


def test_001_emergency_vacuum(create_pg):
    """Failsafe autovacuum triggers and vacuums all tables past failsafe age."""
    node = create_pg("main", start=False)
    node.append_conf(
        "\n"
        "autovacuum_naptime = 1s\n"
        "autovacuum_max_workers = 1\n"
        "log_autovacuum_min_duration = 0\n"
    )
    node.start()
    node.safe_psql("CREATE EXTENSION xid_wraparound")
    node.safe_psql(
        """
CREATE TABLE large(id serial primary key, data text, filler text default repeat(random()::text, 10))
   WITH (autovacuum_enabled = off);
INSERT INTO large(data) SELECT generate_series(1,30000);
CREATE TABLE large_trunc(id serial primary key, data text, filler text default repeat(random()::text, 10))
   WITH (autovacuum_enabled = off);
INSERT INTO large_trunc(data) SELECT generate_series(1,30000);
CREATE TABLE small(id serial primary key, data text, filler text default repeat(random()::text, 10))
   WITH (autovacuum_enabled = off);
INSERT INTO small(data) SELECT generate_series(1,15000);
CREATE TABLE small_trunc(id serial primary key, data text, filler text default repeat(random()::text, 10))
   WITH (autovacuum_enabled = off);
INSERT INTO small_trunc(data) SELECT generate_series(1,15000);
"""
    )
    psql_timeout_secs = 4 * pypg.test_timeout_default()
    background_psql = node.background_psql(
        "postgres", on_error_stop=False, timeout=psql_timeout_secs
    )
    background_psql.set_query_timer_restart()
    background_psql.query_safe(
        """
	BEGIN;
	DELETE FROM large WHERE id % 2 = 0;
	DELETE FROM large_trunc WHERE id > 10000;
	DELETE FROM small WHERE id % 2 = 0;
	DELETE FROM small_trunc WHERE id > 1000;
"""
    )
    node.safe_psql("SELECT consume_xids_until('2000000000'::xid8)")
    node.safe_psql("INSERT INTO small(data) SELECT 1")
    ret = node.safe_psql(
        """
SELECT datname,
       age(datfrozenxid) > current_setting('vacuum_failsafe_age')::int as old
FROM pg_database ORDER BY 1
"""
    )
    assert ret == "postgres|t\ntemplate0|t\ntemplate1|t", "all tables became old"
    log_offset = node.current_log_position()
    background_psql.query_safe("COMMIT")
    background_psql.quit()
    assert node.poll_query_until(
        """
SELECT NOT EXISTS (
	SELECT *
	FROM pg_database
	WHERE age(datfrozenxid) > current_setting('autovacuum_freeze_max_age')::int)
"""
    ), "timeout waiting for all databases to be vacuumed"
    ret = node.safe_psql(
        """
SELECT relname, age(relfrozenxid) > current_setting('autovacuum_freeze_max_age')::int
FROM pg_class
WHERE relname IN ('large', 'large_trunc', 'small', 'small_trunc')
ORDER BY 1
"""
    )
    assert ret == (
        "large|f\nlarge_trunc|f\nsmall|f\nsmall_trunc|f"
    ), "all tables are vacuumed"
    log_contents = pypg.slurp_file(node.log, log_offset)
    for tablename in ("large", "large_trunc", "small", "small_trunc"):
        assert re.search(
            r'bypassing nonessential maintenance of table "postgres\.public\.'
            + tablename
            + r'" as a failsafe after \d+ index scans',
            log_contents,
        ), (
            "failsafe vacuum triggered for " + tablename
        )
    node.stop()

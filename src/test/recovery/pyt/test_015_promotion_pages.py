# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/recovery/t/015_promotion_pages.pl.

Promotion handling with WAL records generated post-promotion before the first
checkpoint, checking for invalid page references against minRecoveryPoint.
"""


def test_promotion_pages(create_pg):
    """A promoted standby survives crash recovery without invalid page refs."""
    alpha = create_pg("alpha", allows_streaming=True, start=False)
    # wal_log_hints=off is important to get invalid page references.
    alpha.append_conf("wal_log_hints = off")
    alpha.start()

    alpha.backup("bkp")
    bravo = create_pg(
        "bravo", from_backup=(alpha, "bkp"), has_streaming=True, start=False
    )
    bravo.append_conf("checkpoint_timeout=1h")
    bravo.start()

    alpha.safe_psql("create table test1 (a int)")
    alpha.safe_psql("insert into test1 select generate_series(1, 10000)")
    alpha.safe_psql("checkpoint")
    # This vacuum sets visibility map bits and creates problematic WAL records.
    alpha.safe_psql("vacuum verbose test1")
    alpha.wait_for_catchup(bravo)

    # Force a checkpoint on the standby so redo does not start from an older
    # point that would include the initial table/page additions.
    bravo.safe_psql("checkpoint")

    # Move minRecoveryPoint beyond the previous vacuum with a dummy table.
    alpha.safe_psql("create table test2 (a int, b bytea)")
    alpha.safe_psql(
        "insert into test2 select generate_series(1,10000), "
        "sha256(random()::text::bytea)"
    )
    alpha.safe_psql("truncate test2")
    alpha.wait_for_catchup(bravo)

    # Promote: minRecoveryPoint is reinitialized so WAL replays to the end.
    bravo.promote()

    # New page references on the promoted standby before its first checkpoint.
    bravo.safe_psql("truncate test1")
    bravo.safe_psql("vacuum verbose test1")
    bravo.safe_psql("insert into test1 select generate_series(1,1000)")

    # Crash-stop and restart: replay must not see invalid page references.
    bravo.stop("immediate")
    bravo.start()

    assert (
        bravo.safe_psql("SELECT count(*) FROM test1") == "1000"
    ), "Check that table state is correct"

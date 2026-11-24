# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/bin/pg_walsummary/t/002_blocks.pl."""

import re


def test_blocks(pg_bin, create_pg):
    """WAL summarization produces a summary that pg_walsummary can read."""
    node1 = create_pg("node1", start=False, allows_streaming=True, has_archiving=True)
    node1.append_conf("summarize_wal = on")
    node1.start()

    # Create a table and insert rows, VACUUM FREEZE so autovacuum won't induce
    # future modifications, then checkpoint.
    node1.safe_psql(
        "CREATE TABLE mytable (a int, b text);\n"
        "INSERT INTO mytable\n"
        "SELECT g, random()::text||random()::text||random()::text||random()::text\n"
        "FROM generate_series(1, 400) g;\n"
        "VACUUM FREEZE;\n"
    )

    base_lsn = node1.safe_psql("SELECT pg_current_wal_insert_lsn()")
    node1.safe_psql("CHECKPOINT;")

    assert node1.poll_query_until(
        "SELECT EXISTS (SELECT * from pg_available_wal_summaries() "
        "WHERE end_lsn >= '{}')".format(base_lsn)
    ), "WAL summarization caught up after insert"

    assert node1.poll_query_until(
        "SELECT sum(reads) > 0 FROM pg_stat_io "
        "WHERE backend_type = 'walsummarizer' AND object = 'wal'"
    ), "WAL summarizer generated IO statistics"

    summarized_lsn = node1.safe_psql(
        "SELECT MAX(end_lsn) AS summarized_lsn FROM pg_available_wal_summaries()"
    )

    # Update a row in the first block of the table and trigger a checkpoint.
    node1.safe_psql(
        "UPDATE mytable SET b = 'abcdefghijklmnopqrstuvwxyz' || b || '01234567890'\n"
        "WHERE a = 2;\n"
        "CHECKPOINT;\n"
    )

    assert node1.poll_query_until(
        "SELECT EXISTS (SELECT * from pg_available_wal_summaries() "
        "WHERE end_lsn > '{}')".format(summarized_lsn)
    ), "got new WAL summary after update"

    details = node1.safe_psql(
        "SELECT tli, start_lsn, end_lsn from pg_available_wal_summaries() "
        "WHERE end_lsn > '{}'".format(summarized_lsn)
    )
    lines = details.split("\n")
    assert len(lines) == 1, "got exactly one new WAL summary"
    tli, start_lsn, end_lsn = lines[0].split("|")

    # Reconstruct the WAL summary file path.
    start_hi, start_lo = start_lsn.split("/")
    end_hi, end_lo = end_lsn.split("/")
    filename = (
        node1.datadir
        / "pg_wal"
        / "summaries"
        / (
            "{:0>8}{:0>8}{:0>8}{:0>8}{:0>8}.summary".format(
                tli, start_hi, start_lo, end_hi, end_lo
            )
        )
    )
    assert filename.is_file(), "WAL summary file exists"

    # Run pg_walsummary: we expect exactly two modified blocks, block 0 and one
    # other.
    result = pg_bin.result(["pg_walsummary", "-i", filename])
    lines = result.stdout.split("\n")
    assert re.search(
        r"(?m)FORK main: block 0$", result.stdout
    ), "stdout shows block 0 modified"
    assert result.stderr == "", "stderr is empty"
    # stdout has a trailing newline, so splitting yields a final empty element.
    assert len([line for line in lines if line]) == 2, "UPDATE modified 2 blocks"

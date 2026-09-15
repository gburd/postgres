# Copyright (c) 2023-2026, PostgreSQL Global Development Group

"""Port of src/test/modules/test_slru/t/002_multixact_wraparound.pl.

Forces the multixact ID space close to wraparound (pg_resetwal sets the next
multixact to 0xFFFFFFF8 and the offsets SLRU is pre-extended/zeroed at the
matching segment) and then creates 16 multixacts via the test_slru extension.
Wraparound occurs (the last ID is less than the first) and every created
multixact remains readable afterward.
"""

import os
import re


def test_002_multixact_wraparound(create_pg):
    """Multixacts created across a forced wraparound stay readable."""
    node = create_pg("main", start=False)
    node.append_conf("shared_preload_libraries = 'test_slru'")
    pgdata = str(node.datadir)
    node.command_ok(
        ["pg_resetwal", "--multixact-ids", "0xFFFFFFF8,0xFFFFFFF8", pgdata],
        "set the cluster's next multitransaction to 0xFFFFFFF8",
    )
    out = node.bin.run_command(["pg_resetwal", "--dry-run", pgdata]).stdout
    blcksz = int(re.search(r"^Database block size: *(\d+)$", out, re.M).group(1))
    slru_pages = int(re.search(r"^Pages per SLRU segment: *(\d+)$", out, re.M).group(1))
    offsets_per_page = blcksz // 8  # sizeof(MultiXactOffset) == 8
    segno = int(0xFFFFFFF8 / offsets_per_page / slru_pages)
    slru_file = os.path.join(pgdata, "pg_multixact", "offsets", "{:04X}".format(segno))
    bytes_per_seg = slru_pages * blcksz
    with open(slru_file, "wb") as fh:
        fh.write(b"\0" * bytes_per_seg)
    os.unlink(os.path.join(pgdata, "pg_multixact", "offsets", "0000"))
    node.start()
    node.safe_psql("CREATE EXTENSION test_slru")
    multixact_ids = [
        node.safe_psql("SELECT test_create_multixact();") for _ in range(16)
    ]
    first_multi, last_multi = int(multixact_ids[0]), int(multixact_ids[-1])
    assert (
        last_multi < first_multi
    ), "multixact wraparound occurred (first: {}, last: {})".format(
        first_multi, last_multi
    )
    for i, multi in enumerate(multixact_ids):
        assert (
            node.safe_psql("SELECT test_read_multixact('{}');".format(multi)) == ""
        ), "multixact {} (ID: {}) is readable after wraparound".format(i, multi)

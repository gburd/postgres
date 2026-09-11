# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/bin/pg_rewind/t/005_same_timeline.pl.

pg_rewind succeeds (does nothing harmful) when the source standby and the
target primary are still on the same timeline -- i.e. the standby was never
promoted, so there is no divergence to rewind.
"""


def test_005_same_timeline(rewind_test):
    """pg_rewind with source and target on the same timeline."""
    rewind_test.setup_cluster()
    rewind_test.start_primary()
    rewind_test.create_standby()
    rewind_test.run_pg_rewind("local")
    rewind_test.clean_rewind_test()

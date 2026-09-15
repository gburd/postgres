# Copyright (c) 2024-2026, PostgreSQL Global Development Group

"""Port of src/test/modules/test_aio/t/003_initdb.pl.

Test initdb for each IO method. This is done separately from 001_aio.pl, as it
isn't fast. This way the more commonly failing / hacked-on 001_aio.pl can be
iterated on more quickly.
"""

import os

import testaio  # pyrefly: ignore


def _test_create_node(io_method, create_pg):
    # Want to test initdb for each IO method, otherwise we could just reuse the
    # cluster.
    #
    # Unfortunately, when PG_TEST_INITDB_EXTRA_OPTS contains -c io_method=xyz it
    # is applied after our own ->extra options and would break this test. Fix
    # that up if we detect it, mirroring the Perl test's local-env override.
    extra_opts = os.environ.get("PG_TEST_INITDB_EXTRA_OPTS")
    saved = extra_opts
    try:
        if extra_opts is not None and "io_method=" in extra_opts:
            os.environ["PG_TEST_INITDB_EXTRA_OPTS"] = (
                extra_opts + " -c io_method={}".format(io_method)
            )

        node = create_pg(
            io_method, extra=["-c", "io_method={}".format(io_method)], start=False
        )

        testaio.configure(node)

        # Even though we used -c io_method=... above, if TEMP_CONFIG sets
        # io_method, it'd override the setting persisted at initdb time. While
        # using (and later verifying) the setting from initdb provides some
        # verification of having used the io_method during initdb, it's probably
        # not worth the complication of only appending if the variable is set in
        # TEMP_CONFIG.
        node.append_conf("\nio_method={}\n".format(io_method))

        # io_method: initdb
        node.start()
        node.stop()
        # io_method: start & stop
        return node
    finally:
        if saved is None:
            os.environ.pop("PG_TEST_INITDB_EXTRA_OPTS", None)
        else:
            os.environ["PG_TEST_INITDB_EXTRA_OPTS"] = saved


def test_003_initdb(create_pg):
    """Run initdb + start/stop once per supported io_method."""
    for method in testaio.supported_io_methods():
        _test_create_node(method, create_pg)

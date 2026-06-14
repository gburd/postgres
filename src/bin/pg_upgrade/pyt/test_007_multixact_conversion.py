# Copyright (c) 2025-2026, PostgreSQL Global Development Group

"""Port of src/bin/pg_upgrade/t/007_multixact_conversion.pl.

Version 19 expanded MultiXactOffset from 32 to 64 bits.  Upgrading across that
requires rewriting the SLRU files to the new format.  This file contains tests
for the conversion.

To run, set 'oldinstall' ENV variable to point to a pre-v19 installation.  If
it's not set, or if it points to a v19 or above installation, this still
performs a very basic test, upgrading a cluster with some multixacts.  It's not
very interesting, however, because there's no conversion involved in that case.

This is a same-version port: old and new clusters are both built from this tree
(v19+), so only the "basic" scenario runs; the "wraparound" scenario requires a
pre-v19 'oldinstall' and is skipped (see test_007_wraparound).
"""

import os
import re

import pytest

import pypg

_NCLIENTS = 20
_UPDATE_EVERY = 13
_ABORT_EVERY = 11


def _read_multixid_fields(pg_bin, node):
    """Read multixid related fields from the control file.

    Returns (oldest_multi_xid, next_multi_xid, next_multi_offset) as strings.
    """
    result = pg_bin.run_command(["pg_controldata", str(node.datadir)])
    stdout = result.stdout
    match = re.search(
        r"^Latest checkpoint's oldestMultiXid:\s*(.*)$", stdout, re.MULTILINE
    )
    assert match, "could not read oldestMultiXid from pg_controldata"
    oldest_multi_xid = match.group(1)
    match = re.search(
        r"^Latest checkpoint's NextMultiXactId:\s*(.*)$", stdout, re.MULTILINE
    )
    assert match, "could not read NextMultiXactId from pg_controldata"
    next_multi_xid = match.group(1)
    match = re.search(
        r"^Latest checkpoint's NextMultiOffset:\s*(.*)$", stdout, re.MULTILINE
    )
    assert match, "could not read NextMultiOffset from pg_controldata"
    next_multi_offset = match.group(1)
    return (oldest_multi_xid, next_multi_xid, next_multi_offset)


def _open_workload_connections(node, binnode, connection_timeout_secs):
    """Open _NCLIENTS+1 background psql connections, each in a transaction.

    The Perl original borrows the new installation's psql *binary* for
    BackgroundPsql feature support but connects every session to the old node
    (``connstr => node->connstr``).  In this same-version port both binaries are
    identical, so the connections are opened against ``node`` directly; binnode
    is accepted only to preserve the helper's signature.
    """
    del binnode  # same-version: node's own psql is used, see docstring
    connections = []
    for _ in range(_NCLIENTS + 1):
        conn = node.background_psql(timeout=connection_timeout_secs)
        conn.query_safe("SET log_statement=none")
        conn.query_safe("SET enable_seqscan=off")
        conn.query_safe("BEGIN")
        connections.append(conn)
    return connections


def _workload_step_sql(i):
    """Return the SQL for one round of the multixid-generating workload."""
    if i % _UPDATE_EVERY == 0:
        return (
            "UPDATE mxofftest SET n_updated = n_updated + 1 "
            "WHERE id = {} % 50;".format(i)
        )
    threshold = int(i / 3000 * 50)
    return (
        "select count(*) from (\n"
        "  SELECT * FROM mxofftest WHERE id >= {} FOR KEY SHARE\n"
        ") as x".format(threshold)
    )


def _mxact_workload(node, binnode):
    """A workload that consumes multixids.

    The purpose of this is to generate some multixids in the old cluster, so
    that we can test upgrading them.  The workload is a mix of KEY SHARE locking
    queries and UPDATEs, and commits and aborts, to generate a mix of multixids
    with different statuses.  It consumes around 3000 multixids with 60000
    members in total.  That's enough to span more than one multixids 'offsets'
    page, and more than one 'members' segment with the default block size.

    The workload leaves behind a table called 'mxofftest' containing a small
    number of rows referencing some of the generated multixids.
    """
    node.start()
    node.safe_psql(
        "CREATE TABLE mxofftest (id INT PRIMARY KEY, n_updated INT)"
        "  WITH (AUTOVACUUM_ENABLED=FALSE);\n"
        "INSERT INTO mxofftest SELECT G, 0 FROM GENERATE_SERIES(1, 50) G;"
    )

    # Bump the timeout on the connections to avoid false negatives on slow test
    # systems.  The timeout covers the whole duration that the connections are
    # open rather than the individual queries.
    connection_timeout_secs = 4 * pypg.test_timeout_default()
    connections = _open_workload_connections(node, binnode, connection_timeout_secs)

    # Run queries cycling through the connections in a round-robin fashion.  We
    # keep a transaction open in each connection at all times, and lock/update
    # the rows.  With 20 connections, each SELECT FOR KEY SHARE query generates
    # a new multixid, containing the XIDs of all the transactions running at the
    # time, ie. around 20 XIDs.
    for i in range(3000):
        conn = connections[i % _NCLIENTS]
        conn.query_safe("ABORT" if i % _ABORT_EVERY == 0 else "COMMIT")
        conn.query_safe("BEGIN")
        conn.query_safe(_workload_step_sql(i))

    for conn in connections:
        conn.quit()

    node.stop()


def _get_test_table_contents(node, tempdir, filename):
    """Write the 'mxofftest' table contents to a file; return its path."""
    contents = node.safe_psql("SELECT ctid, xmin, xmax, * FROM mxofftest")
    path = os.path.join(tempdir, filename)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(contents)
    return path


def _get_updating_multixact_members(node, from_, to, tempdir, filename):
    """Write the members of all updating multixids in the given range to a file.

    Returns the file path.
    """
    path = os.path.join(tempdir, filename)
    with open(path, "w", encoding="utf-8") as fh:
        if to >= from_:
            res = node.safe_psql(
                "SELECT multi, mode, xid\n"
                "FROM generate_series({from_}, {to} - 1) as multi,\n"
                "     pg_get_multixact_members(multi::text::xid)\n"
                "WHERE mode not in ('keysh', 'sh');".format(from_=from_, to=to)
            )
            fh.write(res)
        else:
            # Multixids wrapped around.  Split the query into two parts, before
            # and after the wraparound.
            res = node.safe_psql(
                "SELECT multi, mode, xid\n"
                "FROM generate_series({from_}, 4294967295) as multi,\n"
                "     pg_get_multixact_members(multi::text::xid)\n"
                "WHERE mode not in ('keysh', 'sh');".format(from_=from_)
            )
            fh.write(res)
            res = node.safe_psql(
                "SELECT multi, mode, xid\n"
                "FROM generate_series(1, {to} - 1) as multi,\n"
                "     pg_get_multixact_members(multi::text::xid)\n"
                "WHERE mode not in ('keysh', 'sh');".format(to=to)
            )
            fh.write(res)
    return path


def _build_pg_upgrade_cmd(old, new):
    """Build the same-version pg_upgrade command (no mode, matching the .pl)."""
    return [
        "pg_upgrade",
        "--no-sync",
        "--old-datadir",
        str(old.datadir),
        "--new-datadir",
        str(new.datadir),
        "--old-bindir",
        old.config_data("--bindir"),
        "--new-bindir",
        new.config_data("--bindir"),
        "--socketdir",
        str(new.host),
        "--old-port",
        str(old.port),
        "--new-port",
        str(new.port),
    ]


def _upgrade_and_compare(pg_bin, tag, oldnode, newnode, tempdir):
    """Dump data on old version, run pg_upgrade, compare data after upgrade."""
    pg_bin.command_ok(
        _build_pg_upgrade_cmd(oldnode, newnode),
        "run of pg_upgrade for new instance",
    )

    # Dump contents of the test table, and the status of all updating multixids
    # from the old cluster.  (Locking-only multixids don't need to be preserved
    # so we ignore those.)
    #
    # Note: we do this *after* running pg_upgrade, to ensure that we don't set
    # all the hint bits before upgrade by doing the SELECT on the table.
    multixids_start, multixids_end, _ = _read_multixid_fields(pg_bin, oldnode)
    multixids_start = int(multixids_start)
    multixids_end = int(multixids_end)
    oldnode.start()
    old_table_contents = _get_test_table_contents(
        oldnode, tempdir, "oldnode_{}_table_contents".format(tag)
    )
    old_multixacts = _get_updating_multixact_members(
        oldnode,
        multixids_start,
        multixids_end,
        tempdir,
        "oldnode_{}_multixacts".format(tag),
    )
    oldnode.stop()

    # Compare them with the upgraded cluster
    newnode.start()
    new_table_contents = _get_test_table_contents(
        newnode, tempdir, "newnode_{}_table_contents".format(tag)
    )
    new_multixacts = _get_updating_multixact_members(
        newnode,
        multixids_start,
        multixids_end,
        tempdir,
        "newnode_{}_multixacts".format(tag),
    )
    newnode.stop()

    pypg.compare_files(
        old_table_contents,
        new_table_contents,
        "test table contents from original and upgraded clusters match",
    )
    pypg.compare_files(
        old_multixacts,
        new_multixacts,
        "multixact members from original and upgraded clusters match",
    )


def test_007_basic(create_pg, pg_bin, tmp_check, tmp_path, monkeypatch):
    """Basic scenario: create a cluster, run a multixid workload, then upgrade.

    This works even if the old and new version is the same, although it's not
    very interesting as the conversion routines only run when upgrading from a
    pre-v19 cluster.
    """
    tag = "basic"
    old = create_pg("{}_oldnode".format(tag), start=False, extra=["-k"])
    new = create_pg("{}_newnode".format(tag), start=False)

    # In a VPATH build, we'll be started in the source directory, but we want to
    # run pg_upgrade in the build directory so that any files generated finish
    # in it, like delete_old_cluster.{sh,bat}.
    monkeypatch.chdir(tmp_check)

    _mxact_workload(old, new)
    _upgrade_and_compare(pg_bin, tag, old, new, str(tmp_path))


def test_007_wraparound():
    """Wraparound scenario: requires a pre-v19 'oldinstall' to reset the old
    cluster to just before 32-bit offset wraparound using the old file format.

    The same-version port has no pre-v19 old install available, so the SLRU
    conversion path cannot be exercised; the Perl original likewise skips this
    when the old version is >= 19devel.
    """
    if os.environ.get("oldinstall"):
        pytest.fail(
            "oldinstall is set; wraparound conversion is not ported "
            "(needs the pre-v19 file-format reset hacks)"
        )
    pytest.skip(
        "skipping mxoffset conversion tests because upgrading from the old "
        "version does not require conversion"
    )

#!/usr/bin/perl

# Copyright (c) 2021-2026, PostgreSQL Global Development Group

# Deterministic regression test for the 8th RECNO crash: a live heap row loses
# its btree index entry (and, downstream, an IndexOnlyScan is handed a bogus
# TID that trips the recno_tuple.c deform-bounds oracle), because the index
# fetch reported all_dead for a LIVE tuple.
#
# ROOT CAUSE.  recno_index_fetch_tuple() must report *all_dead per-fetch: true
# iff the tuple at *this* tid is guaranteed dead to every backend (the
# tableam.h index_fetch_tuple contract).  RECNO has no version chain to walk
# within one tid (it never sets *call_again), so each call resolves exactly one
# TID.  The AM kept a per-scan sticky flag (scan->all_dead) that latched true on
# the first dead-to-all tuple and was seeded into the caller's per-fetch
# *all_dead on every subsequent call -- never cleared.  index_fetch_heap()
# (indexam.c) then sets scan->kill_prior_tuple = all_dead unconditionally per
# fetch, so once the flag latched, a subsequently-fetched LIVE tuple was
# reported all_dead, nbtree recorded its correct (key -> tid) index entry in
# so->killedItems, and _bt_killitems() set that live entry LP_DEAD and removed
# it.  amcheck's bt_index_check then reports "heap tuple ... lacks matching
# index tuple".  Under churn a surviving stale entry pointing at a recycled TID
# feeds IndexOnlyScan a bogus tuple whose bytes deform as a garbage varlena,
# tripping the deform-bounds Assert -- the field crash, hitting many backends
# at once because the LP_DEAD kill is persistent on the shared index page.
#
# FIX.  recno_index_fetch_tuple() clears scan->all_dead (and the caller's
# *all_dead) on entry, so all_dead reflects only the current TID.
#
# DETERMINISM.  Single connection, no injection point, no timing.  Four rows
# share one indexed key; the lowest-TID row is deleted and the global xmin
# horizon is advanced past its deleter (a few autocommit txns) so it is
# dead-to-all while its NORMAL line pointer -- and its index entry -- linger.
# One forward index scan on that key then visits the dead entry (latching the
# buggy flag) before the three LIVE entries, which the buggy build marks
# LP_DEAD.  bt_index_check reports the missing entry every run without the fix
# and is clean with it.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Utils;
use PostgreSQL::Test::Cluster;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('recno_stale_alldead');
$node->init;
$node->append_conf('postgresql.conf', qq{
autovacuum = off
shared_buffers = 32MB
fsync = off
});
$node->start;

$node->safe_psql('postgres', 'CREATE EXTENSION amcheck');

# Four rows share indexed key k=5.  The first (lowest heap TID) will be deleted
# and aged past the xmin horizon; the other three stay LIVE.
$node->safe_psql('postgres', q{
    CREATE TABLE t (k int, v int) USING recno WITH (fillfactor = 90);
    CREATE INDEX t_idx ON t (k);
    INSERT INTO t VALUES (5, 100), (5, 200), (5, 300), (5, 400);
});

# Delete the lowest-TID row and advance the global non-removable-xid horizon
# past its deleter with a few committed autocommit transactions, so it becomes
# dead-to-all.  No VACUUM: its NORMAL line pointer and its (k=5 -> tid) index
# entry both linger, so a scan on k=5 still visits the dead entry first.
$node->safe_psql('postgres', q{
    DELETE FROM t WHERE k = 5 AND v = 100;
    SELECT txid_current();
    SELECT txid_current();
    SELECT txid_current();
});

# Force forward index scans on k=5.  With the sticky-all_dead bug the dead
# entry latches the flag and the three LIVE entries are reported all_dead and
# LP_DEAD-killed by _bt_killitems.  Repeated scans flush the kills.
$node->safe_psql('postgres', q{
    SET enable_seqscan = off;
    SET enable_bitmapscan = off;
    SELECT k FROM t WHERE k = 5;
    SELECT k FROM t WHERE k = 5;
    SELECT k FROM t WHERE k = 5;
    SELECT k FROM t WHERE k = 5;
});

# ORACLE: bt_index_check must not find a live heap row lacking its index entry.
# Without the fix this errors with "heap tuple (0,2) ... lacks matching index
# tuple within index t_idx"; with the fix it returns cleanly.
my ($rc, $out, $err) = $node->psql('postgres',
	q{SELECT bt_index_check('t_idx'::regclass, true)});
is($rc, 0, 'bt_index_check: no live heap row lost its index entry')
  or diag("bt_index_check failed: rc=$rc err=$err");
unlike($err, qr/lacks matching index tuple/,
	'no "lacks matching index tuple" corruption');

# The three live rows must still be findable by BOTH an index scan and a seq
# scan (a kill of a live entry would make the index scan undercount).
is( $node->safe_psql(
		'postgres',
		'SET enable_seqscan=off; SET enable_bitmapscan=off; '
	  . 'SELECT count(*) FROM t WHERE k = 5'),
	'3', 'index scan returns all three live k=5 rows');
is( $node->safe_psql(
		'postgres',
		'SET enable_seqscan=on; SET enable_indexscan=off; SET enable_bitmapscan=off; '
	  . 'SELECT count(*) FROM t WHERE k = 5'),
	'3', 'seq scan returns all three live k=5 rows (ground truth)');

# The cluster must still be up (the field symptom was a deform-oracle crash).
is($node->safe_psql('postgres', 'SELECT 1'), '1', 'server still up');

my $log = slurp_file($node->logfile);
unlike(
	$log,
	qr/terminated by signal|TRAP: failed Assert|server closed the connection unexpectedly/,
	'no backend crash / deform-bounds Assert logged');

$node->safe_psql('postgres', 'DROP TABLE t');

done_testing();

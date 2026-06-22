#!/usr/bin/perl

# Copyright (c) 2021-2026, PostgreSQL Global Development Group

# Deterministic regression test for the 7th RECNO crash: duplicate (key, TID)
# btree entries produced by in-place UPDATEs of an indexed variable-length
# column, and the deform-bounds Assert an IndexOnlyScan then trips on a corrupt
# posting list.
#
# ROOT CAUSE.  RECNO updates rows IN PLACE and keeps the same heap TID
# (am_inplace_update_keeps_tid).  When an indexed column changes the executor
# inserts a new (newkey -> TID) btree entry; the prior (oldkey -> TID) entry is
# left in place (dropped lazily at read time by the stale-entry recheck, and by
# VACUUM only for genuinely dead TIDs).  nbtree tolerates the SAME heap TID
# under DIFFERENT keys, but it forbids a DUPLICATE (key, TID) pair: posting-list
# dedup (_bt_posting_valid) and page-split (nbtutils.c) both require strictly
# increasing heap TIDs within one key.  An indexed column that oscillates back
# to a value the row previously held -- e.g. A -> B -> A -- re-inserts a
# (key = A, TID) pair that is still physically present, creating the forbidden
# duplicate.  With dedup on this trips
#     TRAP: failed Assert("_bt_posting_valid(itup)")            (nbtdedup.c)
# and a corrupt posting list then hands IndexOnlyScan a bogus TID whose bytes
# deform as a garbage ~page-sized varlena length, tripping the RECNO
# deform-bounds oracle (recno_tuple.c / recno_slot.c).  Many backends trip at
# once because the corruption is persistent on-page in the shared btree.
#
# FIX.  In ExecInsertIndexTuples(), for a keeps-TID AM whose key changed by this
# UPDATE, probe the btree for an existing (key, TID) entry and skip the insert
# when it already exists.  This keeps at most one entry per distinct (key, TID)
# -- the only invariant nbtree requires -- while preserving the existing design
# (distinct stale keys tolerated + dropped at read time).
#
# DETERMINISM.  No injection point needed: a single-connection oscillation of an
# indexed varchar reproduces the duplicate (key, TID) and the btree Assert every
# run.  A concurrent ANALYZE + IndexOnlyScan phase exercises the downstream
# deform-oracle path that the field crash reported.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Utils;
use PostgreSQL::Test::Cluster;
use Test::More;
use Time::HiRes qw(time usleep);
use POSIX qw(_exit);

my $node = PostgreSQL::Test::Cluster->new('recno_dup_keytid');
$node->init;
$node->append_conf('postgresql.conf', qq{
autovacuum = off
shared_buffers = 32MB
fsync = off
});
$node->start;

# ---------------------------------------------------------------------------
# Phase 1: deterministic single-connection oscillation.
#
# An indexed varchar that cycles through a small set of values makes each value
# recur, so a stale (value, TID) entry is always physically present when the
# same value comes back around.  Without the fix the second insert of an
# already-present (key, TID) corrupts the posting list and trips
# _bt_posting_valid; with the fix the redundant insert is skipped.
# ---------------------------------------------------------------------------
$node->safe_psql('postgres', q{
    CREATE TABLE t (id int, nm varchar(20)) USING recno;
    CREATE INDEX t_idx ON t (id, nm);
    INSERT INTO t SELECT g, 'v'||g FROM generate_series(1, 50) g;
});

my ($rc, $out, $err) = $node->psql('postgres', q{
    DO $$
    BEGIN
      FOR k IN 1..5000 LOOP
        UPDATE t SET nm = 'x' || (k % 17) WHERE id = (k % 50) + 1;
      END LOOP;
    END $$;
});

is($rc, 0, 'single-connection indexed-varchar oscillation does not crash')
  or diag("psql rc=$rc err=$err");

# The server must still be up (a crash takes the whole cluster down).
is($node->safe_psql('postgres', 'SELECT 1'), '1',
	'server up after oscillation churn');

# Correctness: exactly the 50 original rows, one live version each, and an
# index scan must return each id exactly once (no duplicate rows from stale
# (oldkey, TID) entries).
is($node->safe_psql('postgres', 'SELECT count(*) FROM t'),
	'50', 'all 50 rows present after churn');

is( $node->safe_psql(
		'postgres',
		'SET enable_seqscan=off; '
	  . 'SELECT count(*) FROM (SELECT id FROM t WHERE id > 0) s'),
	'50',
	'index scan returns each row exactly once (no duplicate (key,TID) rows)');

# ---------------------------------------------------------------------------
# Phase 2: concurrent size-changing UPDATE + ANALYZE + VACUUM + IndexOnlyScan.
#
# This mirrors the field crash context (concurrent ANALYZE, IndexOnlyScan
# deforming the row).  Without the fix it trips the btree Asserts and/or the
# deform-bounds oracle across many backends; with the fix it completes cleanly.
# ---------------------------------------------------------------------------
$node->safe_psql('postgres', q{
    CREATE TABLE district (
      d_id int, d_w_id int, d_name varchar(10),
      d_street_1 varchar(20), d_street_2 varchar(20), d_city varchar(20),
      d_state char(2), d_zip char(9), d_tax numeric, d_ytd numeric, d_next_o_id int
    ) USING recno WITH (fillfactor = 30);
    CREATE INDEX district_idx ON district (d_id, d_name);
    INSERT INTO district
      SELECT g, 1, 'n'||g, 's1', 's2', 'city', 'CA', '90210', 0.1, 100000.99, 3001
      FROM generate_series(1, 20) g;
});

my $run_secs = 12;
my $nrows    = 20;
my $connstr  = $node->connstr('postgres');
my @pids;

# Feed a stream of SQL to a fresh psql for the given wall-clock duration.
# Uses a raw psql pipe (not $node->background_psql) so the forked child never
# touches the $node Perl object -- a child that did would run $node's DESTROY
# on exit and stop the shared postmaster out from under the parent.
sub run_child_loop
{
	my ($deadline, $gen) = @_;
	my $ok = open(my $fh, '|-', 'psql', '-X', '-q', '-d', $connstr,
		'-v', 'ON_ERROR_STOP=0');
	_exit(0) unless $ok;
	my $n = 0;
	while (time < $deadline)
	{
		$n++;
		print $fh $gen->($n);
	}
	close($fh);
	_exit(0);
}

# Size-changing in-place UPDATE writers: d_name and d_street_1 lengths (and
# hence the on-disk key value) oscillate, so an indexed d_name value recurs.
for my $id (1 .. $nrows)
{
	my $pid = fork;
	die "fork failed: $!" unless defined $pid;
	if ($pid == 0)
	{
		my $deadline = time + $run_secs;
		run_child_loop(
			$deadline,
			sub {
				my ($n) = @_;
				my $len = ($n % 10) + 1;
				my $nm  = substr(('X' x 10), 0, $len);
				my $sl  = (($n * 3) % 20) + 1;
				my $st  = substr(('Y' x 20), 0, $sl);
				my $v   = (($n * 7919) % 900000) + 100000;
				return
					"UPDATE district SET d_name='$nm', d_street_1='$st', "
				  . "d_ytd=$v.99 WHERE d_id=$id;\n";
			});
	}
	push @pids, $pid;
}

# IndexOnlyScan readers (SELECT only indexed columns) + full-deform readers.
for my $r (1 .. 6)
{
	my $pid = fork;
	die "fork failed: $!" unless defined $pid;
	if ($pid == 0)
	{
		my $deadline = time + $run_secs;
		run_child_loop(
			$deadline,
			sub {
				return
					'SELECT d_id, d_name FROM district '
				  . "WHERE d_id BETWEEN 1 AND 20 ORDER BY d_id;\n"
				  . 'SELECT count(md5(d_name||d_street_1)) FROM district '
				  . "WHERE d_id > 0;\n";
			});
	}
	push @pids, $pid;
}

# ANALYZE + VACUUM loop (ShareUpdateExclusiveLock; the field-crash context).
for my $a (1 .. 3)
{
	my $pid = fork;
	die "fork failed: $!" unless defined $pid;
	if ($pid == 0)
	{
		my $deadline = time + $run_secs;
		run_child_loop($deadline,
			sub { return "ANALYZE district;\nVACUUM district;\n"; });
	}
	push @pids, $pid;
}

waitpid($_, 0) for @pids;

# The cluster must still be up after the concurrent phase.
is($node->safe_psql('postgres', 'SELECT 1'), '1',
	'server up after concurrent UPDATE/ANALYZE/VACUUM/IndexOnlyScan');

# All 20 rows still deform cleanly through a full scan and an index-only scan.
is($node->safe_psql('postgres', 'SELECT count(*) FROM district'),
	'20', 'all 20 district rows present after concurrent phase');

is( $node->safe_psql(
		'postgres',
		'SET enable_seqscan=off; '
	  . 'SELECT count(*) FROM (SELECT d_id FROM district WHERE d_id > 0) s'),
	'20',
	'index-only scan returns each district row exactly once');

# No crash / Assert of any kind logged.
my $log = slurp_file($node->logfile);
unlike(
	$log,
	qr/terminated by signal|TRAP: failed Assert|server closed the connection unexpectedly/,
	'no backend crash, btree Assert, or deform-bounds Assert logged');

$node->safe_psql('postgres', 'DROP TABLE t; DROP TABLE district;');

done_testing();

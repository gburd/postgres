
# Copyright (c) 2026, PostgreSQL Global Development Group

# Deleting a skinny subtree (bark_delete_subtree) under recovery.
#
# 1. A standby with wal_consistency_checking = bark replays deletions that
#    take zero, one and two internal levels with their leaf (one
#    UNLINK_PAGE record each), and its index then passes amcheck and
#    answers every query as the primary's does.
# 2. A standby query keeps a cursor in a deleted range, and the reuse of
#    the deleted pages cancels it (Bark/REUSE_PAGE).
# 3. A standby backend whose level-1 entry table names a level-1 page that
#    the primary deletes and then reuses as a level-1 page for other keys:
#    replay of the deletion advances the standby's level-1 epoch, so the
#    backend drops its table and finds the row.  The backend runs inside a
#    transaction, through a function whose plans and locks it holds, so it
#    accepts no invalidation message in between, as in the isolation test
#    bark_subtree_concurrent.
# 4. A crash after a deletion is logged and before the removed downlink's
#    overflow chain is freed (bark-delete-subtree-unlinked): recovery
#    replays the deletion, the chain is left with no reference, and the
#    parent check reports it as an orphan and nothing else.
# 5. Sibling links and a downlink corrupted on disk, as nbtree's page
#    deletion meets them in the field: a right sibling that does not link
#    back, a left sibling whose right links never reach the page (they end
#    the level, name a deleted page, or loop), a subtree parent whose next
#    downlink is not the chain's right sibling, and a one-child parent at
#    the edge of its level.
#    Each deletion logs the corruption and declines, and VACUUM goes on.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init(allows_streaming => 1);
$node->append_conf(
	'postgresql.conf', q[
autovacuum = off
wal_consistency_checking = 'bark'
max_standby_streaming_delay = 2s
log_recovery_conflict_waits = on
deadlock_timeout = 10ms
]);
$node->start;
my $db = 'postgres';
$node->safe_psql(
	$db, q[
CREATE EXTENSION injection_points;
CREATE EXTENSION amcheck;
CREATE EXTENSION pageinspect;
CREATE FUNCTION sdk(int) RETURNS text LANGUAGE sql IMMUTABLE AS
  $$ SELECT lpad($1::text, 8, '0') || repeat('s', 1000) $$;
]);
$node->backup('backup');
my $standby = PostgreSQL::Test::Cluster->new('standby');
$standby->init_from_backup($node, 'backup', has_streaming => 1);
$standby->start;

# The rows of `tab` through an index scan, as one string.
sub rows
{
	my ($n, $tab) = @_;
	return $n->safe_psql($db,
		"SET enable_seqscan = off; SET enable_bitmapscan = off;
		 SELECT md5(string_agg(n::text, ',')) FROM (SELECT n FROM $tab ORDER BY k) s");
}

## 1: replay of deletions of zero, one and two internal levels
my $sect = 'replay';
$node->safe_psql(
	$db, q[
CREATE TABLE sd (k text COLLATE "C", n int) WITH (autovacuum_enabled = off);
ALTER TABLE sd ALTER COLUMN k SET STORAGE plain;
INSERT INTO sd SELECT sdk(g), g FROM generate_series(1, 3000) g;
CREATE INDEX sd_idx ON sd USING bark (k);
]);
is($node->safe_psql($db, q[SELECT level FROM bark_metap('sd_idx')]),
	'4', "$sect: a tree of height 4");
my $lsn = $node->safe_psql($db, 'SELECT pg_current_wal_insert_lsn()');
$node->safe_psql($db, 'DELETE FROM sd WHERE n BETWEEN 400 AND 2600');
$node->safe_psql($db, 'VACUUM sd');
$node->safe_psql($db, 'CHECKPOINT');
my $end = $node->safe_psql($db, 'SELECT pg_current_wal_flush_lsn()');
my ($out) = run_command(
	[ 'pg_waldump', '-p', $node->data_dir . '/pg_wal', '-s', $lsn, '-e', $end,
	  '-r', 'Bark' ]);
my %levels;
$levels{ $_ =~ /internal levels: (\d+)/ ? $1 : 0 }++
  for grep { /UNLINK_PAGE/ && !/lower separator/ } split /\n/, $out;
ok($levels{0} && $levels{1} && $levels{2},
	"$sect: deletions of 0, 1 and 2 internal levels were logged")
  or diag(explain \%levels);
$node->wait_for_replay_catchup($standby);
is($standby->safe_psql($db, q[SELECT bark_index_check('sd_idx')]),
	'', "$sect: bark_index_check on the standby");
is(rows($standby, 'sd'), rows($node, 'sd'),
	"$sect: the standby's index scan returns the primary's rows");
is($standby->safe_psql($db, q[SELECT count(*) FROM bark_multi_page_stats('sd_idx', 1, -1) WHERE type = 'deleted']),
	$node->safe_psql($db, q[SELECT count(*) FROM bark_multi_page_stats('sd_idx', 1, -1) WHERE type = 'deleted']),
	"$sect: the standby has the primary's deleted pages");

## 2: reuse of a deleted page cancels a standby query that may reach it
$sect = 'reuse conflict';
my $psql = $standby->background_psql($db, on_error_stop => 0);
$psql->query_safe(
	q[
BEGIN ISOLATION LEVEL REPEATABLE READ;
SET enable_seqscan = off; SET enable_bitmapscan = off;
DECLARE c CURSOR FOR SELECT n FROM sd WHERE k >= sdk(300) ORDER BY k;
FETCH 50 FROM c;
]);
my $log_location = -s $standby->logfile;
$node->safe_psql($db, 'DELETE FROM sd WHERE n BETWEEN 2700 AND 2900');
$node->safe_psql($db, 'VACUUM sd');
$node->safe_psql($db, 'SELECT pg_current_xact_id()');
$node->safe_psql($db, 'VACUUM sd');
$node->safe_psql($db, 'INSERT INTO sd SELECT sdk(g) || \'r\', g FROM generate_series(400, 2600, 2) g');
$node->wait_for_replay_catchup($standby);
ok( $standby->wait_for_log(
		qr/User query might have needed to see row versions that must be removed/,
		$log_location),
	"$sect: the standby query was cancelled");
$psql->reconnect_and_clear;
is($standby->safe_psql($db, q[SELECT bark_index_check('sd_idx')]),
	'', "$sect: bark_index_check on the standby");
is(rows($standby, 'sd'), rows($node, 'sd'),
	"$sect: the standby's index scan returns the primary's rows");

## 3: a warm level-1 entry table on the standby across the reuse of a
## level-1 page's block (the setup of bark_subtree_concurrent's 6th
## permutation, on the primary)
$sect = 'standby level-1 cache';
$node->safe_psql(
	$db, q[
CREATE FUNCTION rck(int) RETURNS text LANGUAGE sql IMMUTABLE AS
  $$ SELECT lpad($1::text, 8, '0') || repeat('r', 1000) $$;
-- no truncation: its AccessExclusiveLock would cancel the standby backend
CREATE TABLE rc (k text COLLATE "C", n int)
  WITH (autovacuum_enabled = off, vacuum_truncate = off);
ALTER TABLE rc ALTER COLUMN k SET STORAGE plain;
INSERT INTO rc SELECT rck(g * 10), g * 10 FROM generate_series(1, 200) g;
CREATE INDEX rc_idx ON rc USING bark (k);
CREATE FUNCTION rc_down(p int8, j int) RETURNS int8 LANGUAGE sql AS $$
  SELECT downlink FROM bark_page_items('rc_idx', p) WHERE downlink IS NOT NULL
   ORDER BY CASE WHEN j > 0 THEN itemoffset ELSE -itemoffset END
   OFFSET abs(j) - 1 LIMIT 1 $$;
CREATE TABLE rc_saved AS
  SELECT rc_down(m.root, 2) AS c1, rc_down(rc_down(m.root, 2), -1) AS t
    FROM bark_metap('rc_idx') m;
-- every leaf of C1 (the root's second child) but its last, T
DELETE FROM rc WHERE ctid IN
  (SELECT i.htid FROM rc_saved, generate_series(1, 7) j,
          LATERAL bark_page_items('rc_idx', rc_down(c1, j)) i
    WHERE rc_down(c1, j) <> t);
ALTER TABLE rc_saved ADD tlo int, ADD thi int, ADD r2hi int;
UPDATE rc_saved SET
  tlo = (SELECT min(n) FROM rc r JOIN bark_page_items('rc_idx', t) i ON r.ctid = i.htid),
  thi = (SELECT max(n) FROM rc r JOIN bark_page_items('rc_idx', t) i ON r.ctid = i.htid),
  r2hi = (SELECT max(n) FROM rc r, bark_metap('rc_idx') m,
                 LATERAL bark_page_items('rc_idx', rc_down(rc_down(m.root, 3), 2)) i
           WHERE r.ctid = i.htid);
CREATE FUNCTION rc_probe(suffix text) RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE t int; c bigint;
BEGIN
  SELECT tlo INTO t FROM rc_saved;
  SELECT count(*) INTO c FROM rc WHERE k = rck(t) || suffix;
  RETURN c;
END $$;
CREATE FUNCTION rc_warm() RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE c bigint := 0;
BEGIN
  PERFORM set_config('enable_seqscan', 'off', true);
  PERFORM set_config('enable_bitmapscan', 'off', true);
  FOR g IN 1..200 LOOP
    c := c + (SELECT count(*) FROM rc WHERE k = rck(g * 10));
  END LOOP;
  RETURN c;
END $$;
]);
$node->safe_psql($db, 'VACUUM rc');
$node->safe_psql($db, 'SELECT pg_current_xact_id()');
$node->safe_psql($db, 'VACUUM rc');
$node->safe_psql($db, q[INSERT INTO rc SELECT rck(5000 + g), 5000 + g FROM generate_series(1, 60) g]);
$node->wait_for_replay_catchup($standby);

# The standby backend warms its table and caches rc_probe's plans.
$psql = $standby->background_psql($db, on_error_stop => 1);
$psql->query_safe(
	q[
BEGIN ISOLATION LEVEL READ COMMITTED;
SELECT rc_warm();
SELECT rc_probe('y');
]);
# R and R2, C1R's first two children, are left mid-split; T goes with C1,
# and C1's block is reused for C1R's new right half.
$node->safe_psql(
	$db, q[
SELECT injection_points_attach('bark-leave-leaf-split-incomplete', 'error');
]);
$node->psql($db, q[INSERT INTO rc SELECT rck(thi + 10) || 'a', 0 FROM rc_saved]);
$node->psql($db, q[INSERT INTO rc SELECT rck(r2hi) || 'a', 0 FROM rc_saved]);
$node->safe_psql($db,
	q[SELECT injection_points_detach('bark-leave-leaf-split-incomplete')]);
$node->safe_psql($db, q[DELETE FROM rc USING rc_saved WHERE n BETWEEN tlo AND thi]);
$node->safe_psql($db, 'VACUUM rc');
$node->safe_psql($db, 'SELECT pg_current_xact_id()');
$node->safe_psql($db, 'VACUUM rc');
$node->safe_psql(
	$db, q[
INSERT INTO rc SELECT rck(tlo) || 'y', 0 FROM rc_saved;
INSERT INTO rc SELECT rck(r2hi) || 'b', 0 FROM rc_saved;
]);
is( $node->safe_psql(
		$db, q[SELECT s.type || ' ' || s.bark_level FROM rc_saved, bark_page_stats('rc_idx', c1) s]),
	'internal 1', "$sect: C1's block is a level-1 page again");
$node->wait_for_replay_catchup($standby);
like($psql->query_safe(q[SELECT rc_probe('y');]),
	qr/^1$/m, "$sect: the standby backend finds the new row");
$psql->query_safe('COMMIT');
$psql->quit;
is($standby->safe_psql($db, q[SELECT bark_index_check('rc_idx')]),
	'', "$sect: bark_index_check on the standby");

## 4: crash after the deletion is logged, before the chain is freed
$sect = 'crash';
$node->safe_psql(
	$db, q[
CREATE TABLE so (k text COLLATE "C", n int) WITH (autovacuum_enabled = off);
ALTER TABLE so ALTER COLUMN k SET STORAGE plain;
INSERT INTO so SELECT lpad(g::text, 8, '0') || repeat('o', 3000), g
  FROM generate_series(1, 3000) g;
CREATE INDEX so_idx ON so USING bark (k) WITH (fillfactor = 10);
DELETE FROM so WHERE n BETWEEN 50 AND 2850;
]);
my $vac = $node->background_psql($db, on_error_stop => 0);
$vac->query_safe(
	q[
SELECT injection_points_set_local();
SELECT injection_points_attach('bark-delete-subtree-unlinked', 'wait');
]);
$vac->query_until(qr/start/, q[
\echo start
VACUUM so;
]);
$node->wait_for_event('client backend', 'bark-delete-subtree-unlinked');
# Flush the deletion's WAL, then kill the server.
$node->safe_psql($db, 'CREATE TABLE flush_wal ()');
my $pid = $node->safe_psql($db,
	"SELECT pid FROM pg_stat_activity WHERE wait_event = 'bark-delete-subtree-unlinked'");
is(PostgreSQL::Test::Utils::system_log('pg_ctl', 'kill', 'KILL', $pid),
	0, "$sect: killed VACUUM");
$vac->{run}->finish;
$node->stop('immediate', fail_ok => 1);
$node->start;
ok($node->log_contains('database system was interrupted'),
	"$sect: the server went through crash recovery");
my ($rc, $stdout, $stderr) =
  $node->psql($db, q[SELECT bark_index_parent_check('so_idx', true)]);
is($rc, 0, "$sect: the parent check reports no corruption");
my @w = $stderr =~ /WARNING:  (.*)$/mg;
is(scalar @w, 1, "$sect: one warning") or diag(join("\n", @w));
like($w[0] // '', qr/has an orphaned overflow chain of \d+ pages? at block \d+/,
	"$sect: the removed downlink's chain, left with no reference");
is( $node->safe_psql(
		$db,
		q[SET enable_seqscan = off; SET enable_bitmapscan = off;
		  SELECT count(*), sum(n) FROM so WHERE k > '']),
	'199|440050', "$sect: an index scan returns the live rows");

# A clean stop, so that coverage builds keep the startup processes' counts.
$standby->stop;
$node->stop;

## 5: corrupt sibling links and downlinks: the deletion logs and declines
## (as nbtree's does), and VACUUM goes on
$sect = 'corruption';
my $cn = PostgreSQL::Test::Cluster->new('corrupt');
$cn->init(no_data_checksums => 1);
$cn->append_conf('postgresql.conf', "autovacuum = off\n");
$cn->start;
my $blksz = $cn->safe_psql($db, 'SHOW block_size');
$cn->safe_psql(
	$db, q[
CREATE EXTENSION pageinspect;
CREATE FUNCTION rck(int) RETURNS text LANGUAGE sql IMMUTABLE AS
  $$ SELECT lpad($1::text, 8, '0') || repeat('r', 1000) $$;
CREATE FUNCTION down(idx regclass, p int8, j int) RETURNS int8 LANGUAGE sql AS $$
  SELECT downlink FROM bark_page_items(idx::text, p) WHERE downlink IS NOT NULL
   ORDER BY CASE WHEN j > 0 THEN itemoffset ELSE -itemoffset END
   OFFSET abs(j) - 1 LIMIT 1 $$;
]);

# A two-level index of 200 rows.  With $chain, every leaf of the root's
# second child C1 is emptied, so that VACUUM deletes the last, T, with C1;
# else only C1's fourth leaf, T, whose neighbours stay live.  Returns the
# index's file, T, its left sibling L, its right sibling R, and the top of
# the chain (C1, or T).
sub victim
{
	my ($t, $chain, $before) = @_;
	my $from = $chain ? 1 : 4;
	my $to = $chain ? 8 : 4;
	my $which = $chain ? -1 : 4;
	$cn->safe_psql(
		$db, qq[
CREATE TABLE $t (k text COLLATE "C", n int) WITH (autovacuum_enabled = off);
ALTER TABLE $t ALTER COLUMN k SET STORAGE plain;
INSERT INTO $t SELECT rck(g * 10), g * 10 FROM generate_series(1, 200) g;
CREATE INDEX ${t}_idx ON $t USING bark (k);
]);
	$before->() if $before;
	$cn->safe_psql(
		$db, qq[
DELETE FROM $t WHERE ctid IN
  (SELECT i.htid FROM bark_metap('${t}_idx') m, generate_series($from, $to) j,
          LATERAL bark_page_items('${t}_idx', down('${t}_idx', down('${t}_idx', m.root, 2), j)) i
    WHERE i.htid IS NOT NULL);
]);
	my ($c1, $tb) = split /\|/, $cn->safe_psql($db,
		qq[SELECT down('${t}_idx', root, 2),
		          down('${t}_idx', down('${t}_idx', root, 2), $which)
		     FROM bark_metap('${t}_idx')]);
	my ($l, $r) = split /\|/, $cn->safe_psql($db,
		qq[SELECT bark_prev, bark_next FROM bark_page_stats('${t}_idx', $tb)]);
	my $file = $cn->data_dir . '/'
	  . $cn->safe_psql($db, qq[SELECT pg_relation_filepath('${t}_idx')]);
	return ($file, $tb, $l, $r, $chain ? $c1 : $tb);
}

# Read-modify-write one page of a stopped cluster's file.  A build with
# small segments (--with-segsize-blocks) keeps block $blk in segment file
# $file.N, at its offset within that segment.
my $segblocks = $cn->safe_psql($db,
	"SELECT setting FROM pg_settings WHERE name = 'segment_size'");
sub patch_page
{
	my ($file, $blk, $f) = @_;
	my $seg = int($blk / $segblocks);
	$file .= ".$seg" if $seg > 0;
	$blk -= $seg * $segblocks;
	open(my $fh, '+<:raw', $file) or die "open $file: $!";
	seek($fh, $blk * $blksz, 0) or die "seek: $!";
	read($fh, my $p, $blksz) == $blksz or die "short read of $file";
	$f->(\$p);
	seek($fh, $blk * $blksz, 0) or die "seek: $!";
	print $fh $p;
	close $fh;
}
# BarkPageOpaqueData is the page's last 16 bytes: bark_prev, then bark_next.
sub set_prev { substr(${ $_[0] }, $blksz - 16, 4) = pack('L', $_[1]) }
sub set_next { substr(${ $_[0] }, $blksz - 12, 4) = pack('L', $_[1]) }

my @v1 = victim('cr1', 0);	# R links back to L instead of T
my @v2 = victim('cr2', 0);	# L's right link skips T, and the level never reaches it
my @v3 = victim('cr3', 1);	# the root's downlink after C1's is not to C1R
# L's right link names a deleted page, which a VACUUM made first
my @v4 = victim(
	'cr4', 0,
	sub {
		$cn->safe_psql($db, q[DELETE FROM cr4 WHERE n BETWEEN 1500 AND 1600]);
		$cn->safe_psql($db, q[VACUUM cr4]);
	});
my $dead4 = $cn->safe_psql($db,
	q[SELECT min(blkno) FROM bark_multi_page_stats('cr4_idx', 1, -1) WHERE type = 'deleted']);
my @v5 = victim('cr5', 0);	# L's right link names L
my @v6 = victim('cr6', 1);	# C1, T's only parent, has no left link
my ($root3, $off3, $c1r3, $c2r3) = split /\|/, $cn->safe_psql($db,
	q[SELECT m.root, i.itemoffset, down('cr3_idx', m.root, 3), down('cr3_idx', m.root, 4)
	    FROM bark_metap('cr3_idx') m, bark_page_items('cr3_idx', m.root) i
	   WHERE i.downlink = down('cr3_idx', m.root, 3)]);
my $little_endian = pack('L', 1) eq pack('V', 1);
$cn->stop;
patch_page($v1[0], $v1[3], sub { set_prev($_[0], $v1[2]) });
patch_page($v2[0], $v2[2], sub { set_next($_[0], $v2[3]) });
patch_page($v4[0], $v4[2], sub { set_next($_[0], $dead4) });
patch_page($v5[0], $v5[2], sub { set_next($_[0], $v5[2]) });
patch_page($v6[0], $v6[4], sub { set_prev($_[0], 0) });
patch_page(
	$v3[0], $root3,
	sub {
		my $p = $_[0];
		# ItemIdData's lp_off: its first 15 bits
		my $w = unpack('L', substr($$p, 24 + 4 * ($off3 - 1), 4));
		my $lpoff = $little_endian ? $w & 0x7fff : $w >> 17;
		# a plain pivot's downlink is its t_tid block: bi_hi, bi_lo
		substr($$p, $lpoff, 4) = pack('SS', $c2r3 >> 16, $c2r3 & 0xffff);
	});
$cn->start;
my $log = -s $cn->logfile;
$cn->safe_psql($db, 'VACUUM cr1');
$cn->safe_psql($db, 'VACUUM cr2');
$cn->safe_psql($db, 'VACUUM cr3');
$cn->safe_psql($db, 'VACUUM cr4');
$cn->safe_psql($db, 'VACUUM cr5');
$cn->safe_psql($db, 'VACUUM cr6');
ok( $cn->log_contains(
		qr/right sibling $v1[3] of block $v1[1] links back to $v1[2] at level 0 of BARK index "cr1_idx"/,
		$log),
	"$sect: a right sibling that does not link back is reported");
ok( $cn->log_contains(
		qr/left sibling of block $v2[1] could not be located: the walk right ended at block \d+ in BARK index "cr2_idx"/,
		$log),
	"$sect: a left sibling whose right links miss the page is reported");
ok( $cn->log_contains(
		qr/downlink after block $v3[4]'s on page $root3 of BARK index "cr3_idx" is not to its right sibling $c1r3/,
		$log),
	"$sect: a subtree parent whose next downlink is not the right sibling's is reported");
ok( $cn->log_contains(
		qr/left sibling of block $v4[1] could not be located: the walk right ended at block $dead4 in BARK index "cr4_idx"/,
		$log),
	"$sect: a left sibling whose right link names a deleted page is reported");
ok( $cn->log_contains(
		qr/left sibling of block $v5[1] could not be located: the walk right ended at block $v5[2] in BARK index "cr5_idx"/,
		$log),
	"$sect: a left sibling whose right link loops is reported");
ok( $cn->log_contains(
		qr/block $v6[4] of BARK index "cr6_idx" has block $v6[1] as its only child but is the root or at an edge of its level/,
		$log),
	"$sect: an only child's parent at the edge of its level is reported");
is( $cn->safe_psql(
		$db,
		qq[SELECT string_agg((SELECT type FROM bark_page_stats('cr' || i || '_idx', b)), ',' ORDER BY i)
		     FROM (VALUES (1, $v1[1]), (2, $v2[1]), (3, $v3[1]), (4, $v4[1]), (5, $v5[1]),
		                  (6, $v6[1])) v(i, b)]),
	'leaf,leaf,leaf,leaf,leaf,leaf', "$sect: each deletion was declined");
$cn->stop;

done_testing();

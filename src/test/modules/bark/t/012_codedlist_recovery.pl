# Copyright (c) 2026, PostgreSQL Global Development Group

# Crash recovery and standby replay of coded LIST entries (V6).
#
# A coded LIST stores its heap TIDs as the first raw, then varbyte deltas.
# Redo repeats the primary's re-coding from the record's TID alone: ADD_TID
# (decode, insert in order, re-code), INSERT_SWAP (divide the coded entry
# around the TID), VACUUM (re-form the survivors, kept coded) and a split
# that carries a re-formed entry.  Each is redone after a crash from a
# checkpoint taken before the work, so every record is replayed; a streaming
# standby replays the same records, and its scans must equal the primary's.
# After each step the index scans equal a sequential scan, the coded entries
# decode to the heap's TIDs, and bark_index_check accepts the index.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init(allows_streaming => 1);
$node->append_conf(
	'postgresql.conf', qq[
autovacuum = off
wal_keep_size = 1GB
checkpoint_timeout = 1h
max_wal_size = 1GB
]);
$node->start;
$node->safe_psql(
	'postgres', q[
CREATE EXTENSION amcheck;
CREATE EXTENSION pageinspect;
CREATE TABLE cl (k int, v int) WITH (fillfactor = 50, autovacuum_enabled = off);
INSERT INTO cl SELECT g % 40, g FROM generate_series(1, 40000) g;
CREATE INDEX cl_k ON cl USING bark (k) WITH (fillfactor = 100);
CREATE TABLE clp (k text, v int) WITH (fillfactor = 50, autovacuum_enabled = off);
INSERT INTO clp SELECT 'https://www.example.com/item/' || lpad((g % 6)::text, 8, '0'), g
  FROM generate_series(1, 60000) g;
CREATE INDEX clp_k ON clp USING bark (k) WITH (prefix_compression = on, fillfactor = 100);
]);
$node->backup('bk');
my $standby = PostgreSQL::Test::Cluster->new('standby');
$standby->init_from_backup($node, 'bk', has_streaming => 1);
$standby->start;

# Scans by the index and by the heap agree; coded entries exist.
sub check
{
	my ($n, $what) = @_;
	my $q =
	  'SELECT k, count(*), sum(v) FROM cl WHERE k BETWEEN 0 AND 39 GROUP BY k ORDER BY k';
	my $seq = $n->safe_psql('postgres',
		"SET enable_indexscan = off; SET enable_indexonlyscan = off; SET enable_bitmapscan = off; $q"
	);
	my $idx = $n->safe_psql('postgres',
		"SET enable_seqscan = off; SET enable_bitmapscan = off; $q");
	is($idx, $seq, "$what: index scan matches seqscan");
	# Every live row's TID is in an entry (before VACUUM the entries also
	# hold the dead versions' TIDs).
	is( $n->safe_psql(
			'postgres', q[
SELECT count(*) FROM (SELECT ctid FROM cl
                      EXCEPT
                      SELECT t FROM generate_series(1, pg_relation_size('cl_k') / 8192 - 1) b,
                                    bark_page_items('cl_k', b) i, unnest(i.tids) t
                       WHERE i.shape IN ('LIST', 'POSTING', 'SINGLE')) x]),
		'0',
		"$what: every heap TID is in an entry");
	$n->safe_psql('postgres', "SELECT bark_index_check('cl_k', true)");
	pass("$what: bark_index_check");
	return $seq;
}

my $redo = sub {
	$node->safe_psql('postgres', 'CHECKPOINT');
	return $node->safe_psql('postgres',
		'SELECT redo_lsn FROM pg_control_checkpoint()');
};

ok( $node->safe_psql(
		'postgres', q[
SELECT count(*) FILTER (WHERE itemlen < 16 + 6 * ntids) > 0
  FROM generate_series(1, pg_relation_size('cl_k') / 8192 - 1) b,
       bark_page_items('cl_k', b) WHERE shape = 'LIST']) eq 't',
	'the index has coded LIST entries');

# 1. TIDs inside coded runs: ADD_TID re-codes, INSERT_SWAP divides; on the
# prefix-coded index the divided halves are stored prefix-coded.
my $r = $redo->();
$node->safe_psql('postgres',
	'UPDATE cl SET k = 1 WHERE k = 2 AND v % 4 = 2; UPDATE cl SET k = 3 WHERE k = 4 AND v % 2 = 0;'
);
$node->safe_psql('postgres', q[
UPDATE clp SET k = 'https://www.example.com/item/00000001'
  WHERE k = 'https://www.example.com/item/00000002' AND v % 4 = 2;
REINDEX INDEX clp_k;
UPDATE clp SET k = 'https://www.example.com/item/00000001'
  WHERE k = 'https://www.example.com/item/00000003' AND v % 4 = 3;
]);
my $pq = q[SELECT count(*), sum(v) FROM clp WHERE k = 'https://www.example.com/item/00000001'];
my $pwant = $node->safe_psql('postgres', "SET enable_seqscan = off; SET enable_bitmapscan = off; $pq");
is($pwant, $node->safe_psql('postgres', $pq), 'prefix index matches seqscan before crash 1');
my $want = check($node, 'before crash 1');
$node->stop('immediate');
$node->start;
ok($node->log_contains("redo starts at $r"), 'crash 1 replays the inserts');
is(check($node, 'after crash 1'), $want, 'crash 1: same rows');
is($node->safe_psql('postgres', "SET enable_seqscan = off; SET enable_bitmapscan = off; $pq"),
	$pwant, 'crash 1: prefix index same rows');
$node->safe_psql('postgres', "SELECT bark_index_check('clp_k', true)");

# 2. VACUUM re-forms coded entries from their survivors.
$r = $redo->();
$node->safe_psql('postgres',
	'DELETE FROM cl WHERE k IN (1, 3) AND v % 3 = 0; VACUUM cl;');
$want = check($node, 'before crash 2');
$node->stop('immediate');
$node->start;
ok($node->log_contains("redo starts at $r"), 'crash 2 replays VACUUM');
is(check($node, 'after crash 2'), $want, 'crash 2: same rows');

# 3. The standby replayed all of it.
$node->wait_for_catchup($standby);
is(check($standby, 'standby'), $want, 'standby: same rows as the primary');

$standby->stop;
$node->stop;
done_testing();


# Copyright (c) 2026, PostgreSQL Global Development Group

# BARK indexes of a table whose locator offsets exceed MaxHeapTuplesPerPage
# (bark_test_wideblock) through a standby's replay, with
# wal_consistency_checking comparing every page redo rebuilds with the
# primary's, and through crash recovery.  ADD_TID and INSERT_SWAP records
# recompute their entry from the logged TID, so they carry the index's TID
# stride; a redo with heap's stride would code the entries differently and
# fail the consistency check, or put TIDs above 291 on another block.

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
wal_consistency_checking = 'Bark'
]);
$node->start;
$node->backup('backup');
my $standby = PostgreSQL::Test::Cluster->new('standby');
$standby->init_from_backup($node, 'backup', has_streaming => 1);
$standby->start;

# An index of every shape, then inserts that coalesce into them in order
# (ADD_TID) and inside their TID ranges (INSERT_SWAP, after the DELETE and
# VACUUM free slots below the end), and UPDATE, DELETE and VACUUM.
$node->safe_psql(
	'postgres', q[
CREATE EXTENSION amcheck;
CREATE EXTENSION bark_multikey;
CREATE TABLE w (id int, k int) USING bark_test_wideblock
  WITH (autovacuum_enabled = off);
INSERT INTO w SELECT g, CASE WHEN g <= 3000 THEN -1 ELSE g % 300 END
  FROM generate_series(1, 9000) g;
CREATE INDEX w_k ON w USING bark (k);
INSERT INTO w SELECT g, CASE WHEN g % 3 = 0 THEN -1 ELSE g % 300 END
  FROM generate_series(9001, 12000) g;
DELETE FROM w WHERE id % 5 = 0;
VACUUM w;
INSERT INTO w SELECT g, CASE WHEN g % 3 = 0 THEN -1 ELSE g % 300 END
  FROM generate_series(12001, 15000) g;
UPDATE w SET k = k + 1 WHERE id % 7 = 0 AND k >= 0;
]);
$node->wait_for_replay_catchup($standby);

my $wide = $node->safe_psql('postgres',
	"SELECT count(*) > 5000 FROM w WHERE (ctid::text::point)[1] > 291");
is($wide, 't', 'most rows have offsets above MaxHeapTuplesPerPage');
is( $node->safe_psql('postgres', "SELECT bark_test_tid_stride('w_k') > 291"),
	't', 'the index has the wide stride');

my $count = q[
SELECT count(*) || ':' || sum(hashtext(k::text || ctid::text))
  FROM w WHERE k = -1 OR k BETWEEN 10 AND 20];
my $idx = "SET enable_seqscan = off; SET enable_bitmapscan = off; $count";
my $want = $node->safe_psql('postgres', $count);
is($node->safe_psql('postgres', $idx), $want, 'primary index scan = seqscan');
is($standby->safe_psql('postgres', $idx),
	$want, 'standby index scan = primary seqscan');
is($standby->safe_psql('postgres', "SELECT bark_index_check('w_k', true)"),
	'', 'standby index passes bark_index_check');

# Crash: the inserts since the last checkpoint are replayed at startup.
$node->safe_psql('postgres', 'CHECKPOINT');
$node->safe_psql(
	'postgres', q[
INSERT INTO w SELECT g, CASE WHEN g % 2 = 0 THEN -1 ELSE g % 300 END
  FROM generate_series(15001, 18000) g;
DELETE FROM w WHERE id % 11 = 0;
VACUUM w;
INSERT INTO w SELECT g, -1 FROM generate_series(18001, 19000) g;
]);
$want = $node->safe_psql('postgres', $count);
$node->stop('immediate');
$node->start;
is($node->safe_psql('postgres', $idx),
	$want, 'after crash recovery, index scan = the seqscan before the crash');
is($node->safe_psql('postgres', "SELECT bark_index_check('w_k', true)"),
	'', 'after crash recovery the index passes bark_index_check');

$node->wait_for_replay_catchup($standby);
is($standby->safe_psql('postgres', $idx),
	$want, 'standby index scan after the primary\'s crash');
ok($standby->safe_psql('postgres', 'SELECT 1') eq '1',
	'standby is alive after replaying with wal_consistency_checking');

$standby->stop;
$node->stop;

done_testing();

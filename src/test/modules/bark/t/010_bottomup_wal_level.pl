# Copyright (c) 2026, PostgreSQL Global Development Group

# Bottom-up deletion and the merge pass at the WAL levels the regression
# tests do not run at.  At wal_level = minimal an index created or
# truncated in the transaction is not WAL-logged and the deletion record has
# no conflict horizon; at wal_level = logical the record says whether the
# table is a catalog one.  Each step leaves dead versions (a rolled-back
# subtransaction's, or a committed UPDATE's) for the next UPDATE's inserts,
# which are new versions of rows whose key did not change, to delete.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->append_conf('postgresql.conf',
	"wal_level = minimal\nmax_wal_senders = 0\nautovacuum = off\n");
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION amcheck');

my $fill = q{
INSERT INTO t SELECT g, g / 4, 0 FROM generate_series(1, 20000) g;
SAVEPOINT s;
UPDATE t SET v = v + 1;
ROLLBACK TO s;
UPDATE t SET v = v + 1;
};

# The index's answer agrees with a seqscan, and the indexes check.
sub check
{
	my ($what) = @_;
	my $q = 'SELECT count(*), sum(id), sum(k), sum(v) FROM t WHERE k >= 0';
	my $seq = $node->safe_psql('postgres',
		"SET enable_indexscan = off; SET enable_indexonlyscan = off; SET enable_bitmapscan = off; $q"
	);
	my $idx = $node->safe_psql('postgres',
		"SET enable_seqscan = off; SET enable_bitmapscan = off; $q");
	my $bmp = $node->safe_psql('postgres',
		"SET enable_seqscan = off; SET enable_indexscan = off; $q");
	is($idx, $seq, "$what: index scan matches seqscan");
	is($bmp, $seq, "$what: bitmap scan matches seqscan");
	$node->safe_psql('postgres',
		"SELECT bark_index_check('t_k', true), bark_index_check('t_v', true)");
	pass("$what: bark_index_check");
}

# Created in the transaction: not WAL-logged.
$node->safe_psql(
	'postgres', qq{
BEGIN;
CREATE TABLE t (id int, k int, v int);
CREATE INDEX t_k ON t USING bark (k) WITH (fillfactor = 100);
CREATE INDEX t_v ON t USING bark (v);
$fill
COMMIT;
});
check('created in the transaction');

# An existing relation at wal_level = minimal: WAL-logged.
$node->safe_psql('postgres', 'UPDATE t SET v = v + 1');
$node->safe_psql('postgres', 'UPDATE t SET v = v + 1');
check('existing');

# Truncated in the transaction: a new relfilenode, not WAL-logged.
$node->safe_psql(
	'postgres', qq{
BEGIN;
TRUNCATE t;
$fill
COMMIT;
});
check('truncated in the transaction');

# wal_level = logical.
$node->append_conf('postgresql.conf', "wal_level = logical\nmax_wal_senders = 10\n");
$node->restart;
$node->safe_psql('postgres', 'UPDATE t SET v = v + 1');
$node->safe_psql('postgres', 'UPDATE t SET v = v + 1');
check('wal_level = logical');

$node->stop;
done_testing();

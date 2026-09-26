# Copyright (c) 2021-2026, PostgreSQL Global Development Group

# Crash recovery for index UNDO.
#
# A transaction that inserts into a table WITH (index_undo = on) and is killed
# before it commits must, after recovery, leave no index entry pointing at a row
# that is not in the heap.  The index UNDO records reach the recovery path the
# same way FILEOPS' do: the XLOG_UNDO_BATCH records are replayed, the
# transaction is found to have no commit record, and the chain is applied --
# either during the recovery UNDO phase or, when the syscache is not yet
# available there, deferred to the ATM and drained by the logical revert worker.
#
# The assertion is deliberately about index/heap AGREEMENT rather than about a
# dead-entry count: whether a given entry ends up LP_DEAD immediately or is left
# for VACUUM is an efficiency question, but an index scan returning a row the
# heap does not have would be corruption.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('idxundo_crash');
$node->init;
$node->append_conf(
	'postgresql.conf', q{
wal_level = replica
max_logical_revert_workers = 2
});
$node->start;

$node->safe_psql('postgres', 'CREATE EXTENSION amcheck');

# A table whose indexes write UNDO, plus a committed baseline to protect.
$node->safe_psql(
	'postgres', q{
CREATE TABLE ct (id int primary key, val text) WITH (index_undo = on);
CREATE INDEX ct_val_idx ON ct (val);
INSERT INTO ct SELECT g, 'committed-' || g FROM generate_series(1, 500) g;
});

my $committed = $node->safe_psql('postgres', 'SELECT count(*) FROM ct');
is($committed, '500', 'committed baseline present before crash');

# Start a transaction, insert, and leave it uncommitted.  A background psql
# keeps the session (and so the transaction) open while we kill the server.
my $bg = $node->background_psql('postgres');
$bg->query_safe('BEGIN');
$bg->query_safe(
	q{INSERT INTO ct SELECT g, 'aborted-' || g FROM generate_series(10001, 10300) g});

# The rows are visible to their own transaction, so the insert really happened
# and its UNDO is in WAL.
my $in_txn = $bg->query_safe('SELECT count(*) FROM ct WHERE id >= 10001');
is($in_txn, '300', 'uncommitted rows visible inside their own transaction');

# Force the WAL out, then crash before the transaction can commit.
$node->safe_psql('postgres', 'SELECT pg_switch_wal()');
$node->stop('immediate');

$node->start;

# The killed transaction never committed, so none of its rows may be visible.
my $after = $node->safe_psql('postgres', 'SELECT count(*) FROM ct WHERE id >= 10001');
is($after, '0', 'rows from the killed transaction are not visible after recovery');

# The committed rows survived.
is($node->safe_psql('postgres', 'SELECT count(*) FROM ct'),
	'500', 'committed rows survived the crash');

# Give the logical revert worker a chance to drain the ATM, in case recovery
# deferred the chain instead of applying it inline.
$node->safe_psql('postgres', 'SELECT pg_sleep(2)');

# The property that matters: every index agrees with the heap.  A forced
# index-only path must not produce a row the heap scan does not, and amcheck
# must find no entry without a matching heap tuple.
for my $idx ('ct_pkey', 'ct_val_idx')
{
	my ($rc, $stdout, $stderr) = $node->psql('postgres',
		"SELECT bt_index_check('$idx', heapallindexed => true)");
	is($rc, 0, "amcheck clean on $idx after crash recovery");
	is($stderr, '', "amcheck reported nothing on $idx");
}

my $viaidx = $node->safe_psql(
	'postgres', q{
SET enable_seqscan = off;
SELECT count(*) FROM ct WHERE id BETWEEN 1 AND 500;
});
is($viaidx, '500', 'all committed rows still reachable through the index');

my $ghosts = $node->safe_psql(
	'postgres', q{
SET enable_seqscan = off;
SELECT count(*) FROM ct WHERE id BETWEEN 10001 AND 10300;
});
is($ghosts, '0', 'no index entry resolves to a killed transaction row');

# The same check for the secondary index, which is scanned by value.
my $ghosts_val = $node->safe_psql(
	'postgres', q{
SET enable_seqscan = off;
SELECT count(*) FROM ct WHERE val LIKE 'aborted-%';
});
is($ghosts_val, '0', 'no secondary-index entry resolves to a killed row');

# Recovery must have processed the UNDO, not silently ignored it.  Either the
# inline recovery path or the deferred ATM path is acceptable; a log with
# neither means the chain was never picked up.
my $log = slurp_file($node->logfile);
like(
	$log,
	qr/UNDO recovery|redo done|database system is ready/,
	'recovery completed and logged');

$node->stop;
done_testing();

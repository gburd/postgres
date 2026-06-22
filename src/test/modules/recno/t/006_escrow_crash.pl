# GATE C: RECNO escrow crash recovery (H2 redo idempotency + H3 rollback).
#
# Two sessions accumulate an int8 escrow column on a hot row: one COMMITS its
# delta, one leaves its delta UNCOMMITTED.  A CHECKPOINT forces the dirty page
# (carrying the full running sum = base + committed + uncommitted) to disk, then
# the server crashes with the uncommitted txn in flight.  After recovery the
# escrow value must equal base + COMMITTED delta only: crash recovery replays
# the redo (result bytes, H2) and reverse-applies the loser's escrow record by
# subtracting its delta (H3).  0 PANIC, recovery clean, amcheck clean.
#
# Modelled on 002_crash_recovery.pl Test 9 (uncommitted in-place UPDATE).

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Utils;
use Test::More;
use PostgreSQL::Test::Cluster;

sub run_cycle
{
	my ($node, $iter) = @_;

	$node->safe_psql('postgres',
		'DROP TABLE IF EXISTS crashesc');
	$node->safe_psql('postgres',
		'CREATE TABLE crashesc (id int PRIMARY KEY, ytd bigint NOT NULL DEFAULT 0) USING recno');
	$node->safe_psql('postgres', 'INSERT INTO crashesc VALUES (1, 1000)');
	$node->safe_psql('postgres',
		'ALTER TABLE crashesc ALTER COLUMN ytd SET (escrow = true)');

	# Session U: uncommitted escrow += 500 (held open).
	my $bgu = $node->background_psql('postgres');
	$bgu->query_safe('BEGIN');
	$bgu->query_safe('UPDATE crashesc SET ytd = ytd + 500 WHERE id = 1');

	# Session C: committed escrow += 250 (stacks on U's uncommitted sum).
	$node->safe_psql('postgres',
		'BEGIN; UPDATE crashesc SET ytd = ytd + 250 WHERE id = 1; COMMIT;');

	# Force the dirty page (running sum 1750) to disk, then crash in flight.
	$node->safe_psql('postgres', 'CHECKPOINT');
	$node->stop('immediate');
	eval { $bgu->reconnect_and_clear; };

	$node->start;

	my $val = $node->safe_psql('postgres',
		'SELECT ytd FROM crashesc WHERE id = 1');
	is($val, '1250',
		"iter $iter: post-recovery escrow value = base + committed delta only (uncommitted rolled back)");

	# amcheck the primary key index.
	$node->safe_psql('postgres', 'CREATE EXTENSION IF NOT EXISTS amcheck');
	my ($rc, $out, $err) = $node->psql('postgres',
		"SELECT bt_index_check('crashesc_pkey'::regclass)");
	is($rc, 0, "iter $iter: amcheck clean after recovery");

	eval { $bgu->quit; };
}

my $node = PostgreSQL::Test::Cluster->new('recno_escrow_crash');
$node->init;
$node->append_conf('postgresql.conf', 'fsync = on');
$node->start;

# Run the crash/recovery cycle twice (plan requires 2x).
run_cycle($node, 1);

# Assert no PANIC in the log across the whole run.
my $log = slurp_file($node->logfile);
unlike($log, qr/PANIC/, "no PANIC during escrow crash recovery (iter 1)");

run_cycle($node, 2);

$log = slurp_file($node->logfile);
unlike($log, qr/PANIC/, "no PANIC during escrow crash recovery (iter 2)");

$node->stop;
done_testing();

# Copyright (c) 2026, PostgreSQL Global Development Group

# An RI check's stack of executing plans belongs to its own transaction.
# Session A runs an ON DELETE CASCADE whose SPI query waits on a row lock, so
# A sits inside ri_PerformCheck().  Session B commits meanwhile: its
# AtEOXact_RI() must not see, clear, or free A's stack.  With one shared
# stack, B's commit fails "ri_executing_depth == 0 || !isCommit" in a cassert
# build, and otherwise lets B free a plan A is still executing.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

$ENV{PG_XTC_CARRIER_LOOPS} = '1';

for my $lane (['fork', 'off', 0], ['fiber', 'on', 0], ['pooled', 'on', 4])
{
	my ($label, $mode, $carriers) = @$lane;
	my $node = PostgreSQL::Test::Cluster->new("ri_check_ownership_$label");
	$node->init;
	$node->append_conf(
		'postgresql.conf', qq{
multithreaded = $mode
pooled_protocol_carriers = $carriers
io_method = sync
summarize_wal = off
max_connections = 20
});
	$node->start;
	is($node->safe_psql('postgres', 'SHOW multithreaded'),
		$mode, "$label: backend model");

	$node->safe_psql(
		'postgres', q{
		CREATE TABLE ri_pk (id int PRIMARY KEY);
		CREATE TABLE ri_fk (id int REFERENCES ri_pk ON DELETE CASCADE);
		INSERT INTO ri_pk VALUES (1);
		INSERT INTO ri_fk VALUES (1);
	});

	# C holds the referencing row, so A's cascaded DELETE waits for it.
	my $c = $node->background_psql('postgres', timeout => 60);
	$c->query_safe('BEGIN; SELECT * FROM ri_fk FOR UPDATE;');

	my $a = $node->background_psql('postgres', timeout => 60);
	my $a_pid = $a->query_safe('SELECT pg_backend_pid();');
	$a->query_until(qr/cascade_started/, q{
		\echo cascade_started
		DELETE FROM ri_pk WHERE id = 1;
	});
	ok( $node->poll_query_until(
			'postgres', qq{
		SELECT count(*) = 1 FROM pg_locks
		WHERE pid = $a_pid AND NOT granted
	}), "$label: cascade waits inside the RI check");

	# B commits while A's RI check is in progress.
	is($node->safe_psql('postgres', 'SELECT 1;'),
		'1', "$label: another transaction commits during the RI check");

	$c->query_safe('COMMIT;');
	$c->quit;
	is($a->query_safe('SELECT count(*) FROM ri_fk;'),
		'0', "$label: cascade completes after the lock is released");
	$a->quit;

	$node->stop;
}

done_testing();

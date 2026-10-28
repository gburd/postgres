# Copyright (c) 2026, PostgreSQL Global Development Group

# Keep both sessions alive across SET, transaction rollback, RESET and reconnect.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use JSON::PP;

$ENV{PG_XTC_CARRIER_LOOPS} = '1';

my $settings = q{SELECT current_setting('enable_groupagg'),
	current_setting('log_statement_max_length'),
	current_setting('output_plugin_libraries')};
my $explain = q{EXPLAIN (FORMAT JSON, COSTS OFF)
	SELECT i, count(i ORDER BY i) FROM generate_series(1, 10) i GROUP BY i ORDER BY i};

for my $lane (['fork', 'off', 0], ['fiber', 'on', 0], ['pooled', 'on', 4])
{
	my ($label, $mode, $carriers) = @$lane;
	my $node = PostgreSQL::Test::Cluster->new("guc_owners_$label");
	$node->init(allows_streaming => 'logical');
	$node->append_conf(
		'postgresql.conf', qq{
multithreaded = $mode
pooled_protocol_carriers = $carriers
io_method = sync
summarize_wal = off
max_connections = 20
log_statement = 'all'
});
	$node->start;
	is($node->safe_psql('postgres', 'SHOW multithreaded'),
		$mode, "$label: backend model");
	is($node->safe_psql('postgres', 'SHOW pooled_protocol_carriers'),
		"$carriers", "$label: protocol carrier count");

	my $a = $node->background_psql('postgres', timeout => 30);
	my $b = $node->background_psql('postgres', timeout => 30);
	$a->set_query_timer_restart;
	$b->set_query_timer_restart;
	# Five live sessions exceed the four-carrier protocol pool.
	my @idle = map { $node->background_psql('postgres', timeout => 30) } 1 .. 3;
	my $defaults = 'on|-1|pgoutput, test_decoding';
	is($a->query_safe($settings), $defaults, "$label: A boot defaults");
	is($b->query_safe($settings), $defaults, "$label: B boot defaults");
	$a->query_safe(q{SET enable_groupagg = off;
		SET log_statement_max_length = 17;
		SET output_plugin_libraries = 'pgoutput'});
	is($b->query_safe($settings), $defaults, "$label: A SET does not change B");
	$b->query_safe(q{SET enable_groupagg = on;
		SET log_statement_max_length = 31;
		SET output_plugin_libraries = 'test_decoding'});
	is($a->query_safe($settings), 'off|17B|pgoutput',
		"$label: B SET does not change A");

	# An ordered aggregate requires sorted aggregation even when disabled.
	# Inspect the actual plan rather than inferring planner behavior from SHOW.
	my $plan_a = decode_json($a->query_safe($explain))->[0]->{Plan};
	my $plan_b = decode_json($b->query_safe($explain))->[0]->{Plan};
	is($plan_a->{'Node Type'}, 'Aggregate', "$label: A aggregate plan");
	is($plan_b->{'Node Type'}, 'Aggregate', "$label: B aggregate plan");
	is($plan_a->{Strategy}, 'Sorted', "$label: A sorted aggregate");
	is($plan_b->{Strategy}, 'Sorted', "$label: B sorted aggregate");
	ok($plan_a->{Disabled}, "$label: A disables grouped aggregation");
	ok(!$plan_b->{Disabled}, "$label: B enables grouped aggregation");

	my $offset = -s $node->logfile;
	my $query_a = "/*guc_a*/ SELECT 'abcdefghijklmnopqrstuvwxyz'";
	my $query_b = "/*guc_b*/ SELECT 'abcdefghijklmnopqrstuvwxyz'";
	$a->query_safe($query_a);
	$b->query_safe($query_b);
	my $log = slurp_file($node->logfile, $offset);
	my $logged_a = substr($query_a, 0, 17) . '...';
	my $logged_b = substr($query_b, 0, 31) . '...';
	like($log, qr/statement: \Q$logged_a\E\r?$/m,
		"$label: A statement truncation uses A's limit");
	like($log, qr/statement: \Q$logged_b\E\r?$/m,
		"$label: B statement truncation uses B's limit");

	$b->query_safe(q{BEGIN; SET LOCAL enable_groupagg = off;
		SET LOCAL log_statement_max_length = 63;
		SET LOCAL output_plugin_libraries = ''});
	is($b->query_safe($settings), 'off|63B|""', "$label: B SET LOCAL values");
	is($a->query_safe($settings), 'off|17B|pgoutput',
		"$label: B SET LOCAL leaves A alone");
	$b->query_safe('ROLLBACK');
	is($b->query_safe($settings), 'on|31B|test_decoding',
		"$label: rollback restores B");
	is($a->query_safe($settings), 'off|17B|pgoutput',
		"$label: B rollback leaves A alone");
	$b->query_safe(q{RESET enable_groupagg; RESET log_statement_max_length;
		RESET output_plugin_libraries});
	is($b->query_safe($settings), $defaults, "$label: B RESET restores defaults");
	is($a->query_safe($settings), 'off|17B|pgoutput',
		"$label: B RESET leaves A alone");
	$b->quit;

	# Role defaults exercise startup adoption and a non-superuser policy reader.
	$a->query_safe(q{CREATE ROLE guc_replication LOGIN REPLICATION;
		GRANT pg_read_all_settings TO guc_replication;
		ALTER ROLE guc_replication SET enable_groupagg = off;
		ALTER ROLE guc_replication SET log_statement_max_length = 47;
		ALTER ROLE guc_replication SET output_plugin_libraries = 'pgoutput';
		SET output_plugin_libraries = ''});
	$b = $node->background_psql('postgres', timeout => 30,
		extra_params => ['-U', 'guc_replication'], on_error_stop => 0);
	$b->set_query_timer_restart;
	is($b->query_safe($settings), 'off|47B|pgoutput',
		"$label: reconnect applies role defaults");
	is($a->query_safe($settings), 'off|17B|""',
		"$label: B startup leaves A alone");
	$a->query_safe("SET output_plugin_libraries = ''");
	my $allowed = $b->query(q{SELECT slot_name FROM
		pg_create_logical_replication_slot('guc_owner_slot', 'pgoutput', true);
		\echo :SQLSTATE});
	if ($mode eq 'off')
	{
		is($allowed, "guc_owner_slot\n00000",
			"$label: replication role can use its allowed plugin")
		  or diag($b->{stderr});
	}
	else
	{
		# Policy allows pgoutput, but its process-only guard must still reject
		# it. Phase 16/19 owns module admission/fallback, not this owner fix.
		is($allowed, 'XX000', "$label: allowed plugin reaches loader guard");
		like($b->{stderr},
			qr/ERROR:  library "[^"\r\n]*pgoutput[^"\r\n]*" is not supported in the threaded backend runtime\r?$/m,
			"$label: pgoutput is rejected by the threaded loader");
		like($b->{stderr},
			qr/DETAIL:  The active backend model is "[^"\r\n]+", but the library is process-only\.\r?$/m,
			"$label: rejection identifies the process-only boundary");
	}
	$b->{stderr} = '';
	if ($allowed eq "guc_owner_slot\n00000")
	{
		$b->query_safe(q{SELECT pg_drop_replication_slot('guc_owner_slot')});
	}
	is($b->query_safe(q{SELECT count(*) FROM pg_replication_slots
		WHERE slot_name = 'guc_owner_slot'}), '0',
		"$label: no slot remains after allowed-plugin check");
	my $set_denied = $b->query(q{SET output_plugin_libraries = '';
		\echo :SQLSTATE});
	is($set_denied, '42501', "$label: output-plugin SET requires privilege");
	like($b->{stderr}, qr/permission denied to set parameter "output_plugin_libraries"/,
		"$label: replication privilege cannot change plugin policy");
	$b->{stderr} = '';
	$b->query_safe('RESET ALL');
	is($b->query_safe($settings), 'off|47B|pgoutput',
		"$label: RESET ALL keeps role defaults");
	$b->quit;

	$a->query_safe(q{ALTER ROLE guc_replication SET output_plugin_libraries = '';
		SET output_plugin_libraries = 'pgoutput'});
	$b = $node->background_psql('postgres', timeout => 30,
		extra_params => ['-U', 'guc_replication'], on_error_stop => 0);
	$b->set_query_timer_restart;
	is($b->query_safe($settings), 'off|47B|""',
		"$label: next reconnect installs restricted policy");
	$a->query_safe("SET output_plugin_libraries = 'pgoutput'");
	my $denied = $b->query(q{SELECT slot_name FROM
		pg_create_logical_replication_slot('guc_owner_denied', 'pgoutput', true);
		\echo :SQLSTATE});
	is($denied, '42501', "$label: B denies plugin despite A allowing it");
	like($b->{stderr}, qr/library "pgoutput" may not be used as an output plugin/,
		"$label: deny occurs at the output-plugin policy boundary");
	$b->{stderr} = '';
	$b->quit;
	$a->quit;
	$_->quit for @idle;
	$node->stop;
}

done_testing();

# Copyright (c) 2026, PostgreSQL Global Development Group

# A client's LOAD must not reserve prefixes in another client's GUC registry.
# Genuine shared preload must reserve them, even without custom descriptors.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

sub connect_session
{
	my ($node) = @_;
	my $session;
	{
		local $ENV{PGOPTIONS} = '';
		$session = $node->background_psql('postgres',
			on_error_stop => 0, timeout => 15);
	}
	$session->set_query_timer_restart;
	return $session;
}

# Keep deliberate SQL errors on the same connection, and delimit stderr so an
# earlier expected error cannot contaminate a later LOAD warning or success.
sub command
{
	my ($session, $sql, $state, $label, $warning) = @_;
	my $result = $session->query("$sql;\n\\echo :SQLSTATE");
	my $stderr = $session->{stderr};
	$session->{stderr} = '';
	is($result, $state, $label) or diag($stderr);
	if (defined $warning)
	{
		like($stderr, $warning, "$label: diagnostic");
	}
	elsif ($state eq '00000')
	{
		is($stderr, '', "$label: no diagnostic");
	}
}

# Activity removal precedes GUC/context reclamation; this is not a memory barrier.
sub disconnect_and_wait_activity_removal
{
	my ($node, $session, $label) = @_;
	my $pid = $session->query_safe('SELECT pg_backend_pid()');
	$session->quit;
	ok($node->poll_query_until('postgres',
		"SELECT NOT EXISTS (SELECT FROM pg_stat_activity WHERE pid = $pid)"),
		"$label: A removed from activity");
}

sub placeholder_sequence
{
	my ($session, $label) = @_;
	is($session->query_safe(q{
		SELECT count(*) FROM pg_settings WHERE name = 'plpgsql.extra_warnings'
	}), '0', "$label: no descriptor before own load");
	command($session, q{SET plpgsql.extra_warnings = 'all'}, '00000',
		"$label: valid placeholder accepted");
	command($session, q{SET plpgsql.extra_foo_warnings = 'all'}, '00000',
		"$label: typo placeholder accepted before own load");
	command($session, q{LOAD 'plpgsql'}, '00000', "$label: own load",
		qr/WARNING:.*invalid configuration parameter name "plpgsql.extra_foo_warnings", removing it.*"plpgsql" is now a reserved prefix/s);
	is($session->query_safe('SHOW plpgsql.extra_warnings'), 'all',
		"$label: placeholder value survives conversion");
	command($session, q{SET plpgsql.extra_foo_warnings = 'all'}, '42602',
		"$label: typo rejected after own load", qr/"plpgsql" is a reserved prefix/);
	command($session, 'RESET ALL', '00000', "$label: reset all");
	command($session, q{SET plpgsql.extra_foo_warnings = 'all'}, '42602',
		"$label: reset does not unreserve", qr/"plpgsql" is a reserved prefix/);
}

for my $lane ('fork', 'fiber1', 'fiber2')
{
	my $mode = $lane eq 'fork' ? 'off' : 'on';
	my $loops = $lane eq 'fiber2' ? 2 : 1;
	local $ENV{PG_XTC_CARRIER_LOOPS} = $loops;

	for my $preload ('none', 'prefix', 'descriptors')
	{
		my $label = "$lane/$preload";
		my $library = $preload eq 'prefix' ? 'test_backend_runtime_prefix'
		  : $preload eq 'descriptors' ? 'pg_stat_statements' : '';
		my $node = PostgreSQL::Test::Cluster->new("prefix_${lane}_$preload");
		$node->init;
		$node->append_conf('postgresql.conf', qq{
multithreaded = $mode
pooled_protocol_carriers = 0
io_method = sync
summarize_wal = off
max_connections = 20
max_worker_processes = 8
max_logical_replication_workers = 4
shared_preload_libraries = '$library'
session_preload_libraries = ''
local_preload_libraries = ''
});
		$node->append_conf('postgresql.conf', 'pg_stat_statements.track = all')
		  if $preload eq 'descriptors';
		$node->start;
		is($node->safe_psql('postgres', 'SHOW multithreaded'), $mode,
			"$label: requested backend model");
		is($node->safe_psql('postgres', 'SHOW pooled_protocol_carriers'), '0',
			"$label: stackless pool disabled");
		if ($mode eq 'on')
		{
			like(slurp_file($node->logfile),
				qr/xtc: carrier scheduler thread up \($loops loops?, $loops supervisors?, thread-per-session backend pool\)/,
				"$label: requested native loop count");
		}

		if ($preload eq 'none')
		{
			# This first connection is the B-only control.  Do not use a PL
			# helper to inspect state before the pre-LOAD SET.
			my $a = connect_session($node);
			is($a->query_safe(q{
				SELECT current_setting('shared_preload_libraries') = '' AND
					current_setting('session_preload_libraries') = '' AND
					current_setting('local_preload_libraries') = ''
			}), 't', "$label: no preload in control");
			placeholder_sequence($a, "$label/control");
			command($a, q{SET plpgsql.extra_warnings = 'shadowed_variables'},
				'00000', "$label: A value");

			my $b = connect_session($node);
			placeholder_sequence($b, "$label/A-live");
			is($a->query_safe('SHOW plpgsql.extra_warnings'), 'shadowed_variables',
				"$label: B load and reset leave A unchanged");
			$b->quit;

			# Startup-option replay must also accept a valid placeholder after
			# another session has loaded the module.
			{
				local $ENV{PGOPTIONS} = '-c plpgsql.extra_warnings=all';
				my ($stdout, $stderr, $timed_out);
				my $ret = $node->psql('postgres',
					q{LOAD 'plpgsql'; SHOW plpgsql.extra_warnings},
					stdout => \$stdout, stderr => \$stderr,
					timeout => 15, timed_out => \$timed_out);
				ok(!$timed_out && defined($ret) && $ret == 0 && $stdout eq 'all',
					"$label: startup placeholder survives conversion")
				  or diag("stdout=$stdout, stderr=$stderr");
			}
			disconnect_and_wait_activity_removal($node, $a, $label);
			$b = connect_session($node);
			placeholder_sequence($b, "$label/A-removed-from-activity");
			$b->quit;
		}
		else
		{
			# No explicit LOAD in these clients: reservation comes from startup.
			my $prefix = $library;
			my $a = connect_session($node);
			command($a, "SET $prefix.no_such_setting = 'on'", '42602',
				"$label: preloaded prefix reserved", qr/"$prefix" is a reserved prefix/);
			if ($preload eq 'descriptors')
			{
				is($a->query_safe('SHOW pg_stat_statements.track_utility'), 'on',
					"$label: boot-default descriptor available");
				is($a->query_safe('SHOW pg_stat_statements.track'), 'all',
					"$label: configured value replayed");
				command($a, 'SET pg_stat_statements.track = none', '00000',
					"$label: A changes its value");
			}
			else
			{
				is($a->query_safe(q{
					SELECT count(*) FROM pg_settings WHERE category = 'Customized Options'
				}), '0', "$label: no custom descriptors captured");
			}
			my $b = connect_session($node);
			if ($preload eq 'descriptors')
			{
				is($b->query_safe('SHOW pg_stat_statements.track'), 'all',
					"$label: B value independent");
			}
			disconnect_and_wait_activity_removal($node, $a, $label);
			command($b, 'RESET ALL', '00000', "$label: B reset after A activity removal");
			command($b, "SET $prefix.no_such_setting = 'on'", '42602',
				"$label: reservation survives A activity removal and reset", qr/"$prefix" is a reserved prefix/);
			$b->quit;
			$b = connect_session($node);
			command($b, "SET $prefix.no_such_setting = 'on'", '42602',
				"$label: snapshot available on reconnect", qr/"$prefix" is a reserved prefix/);
			$b->quit;
		}

		$node->stop;
		unlike(slurp_file($node->logfile),
			qr/retained .*TopMemoryContext|retained runtime-owned roots|PANIC:|segmentation fault|TRAP:/i,
			"$label: no retained roots or crash diagnostics");

		# Prefix repair must not admit writable preload descriptors without
		# session accessors.  PL/pgSQL shared preload remains unsupported.
		if ($preload eq 'none' && $mode eq 'on')
		{
			$node->append_conf('postgresql.conf', q{shared_preload_libraries = 'plpgsql'});
			ok(!$node->start(fail_ok => 1), "$label: unsafe preload rejected");
			like(slurp_file($node->logfile),
				qr/custom configuration parameter "plpgsql\.[^"]+" is not safe under multithreaded=on/,
				"$label: rejection is the custom-GUC safety check");
			$node->stop('immediate', fail_ok => 1);
		}
	}
}

done_testing();

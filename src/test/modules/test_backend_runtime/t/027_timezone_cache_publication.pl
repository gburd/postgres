# Copyright (c) 2026, PostgreSQL Global Development Group

# A HASH_FIND must not return the publisher's still-uninitialized pg_tz.
# The fork reader owns a different cache, so an early return there is valid.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'timezone publication coverage requires injection points'
  unless ($ENV{enable_injection_points} // '') eq 'yes';

for my $lane ('fork', 'fiber')
{
	local $ENV{PGOPTIONS} = '';
	my $mode = $lane eq 'fork' ? 'off' : 'on';
	my $node = PostgreSQL::Test::Cluster->new("timezone_publication_$lane");
	$node->init;
	$node->append_conf('postgresql.conf', qq{
multithreaded = $mode
carrier_loops = 4
io_method = sync
autovacuum = off
summarize_wal = off
max_connections = 20
max_worker_processes = 8
max_logical_replication_workers = 0
statement_timeout = 0
});
	$node->start;
	$node->safe_psql('postgres',
		'CREATE EXTENSION test_backend_runtime_threaded');
	is($node->safe_psql('postgres', 'SHOW multithreaded'), $mode,
		"$lane: backend mode");
	if ($lane eq 'fiber')
	{
		like(slurp_file($node->logfile),
			qr/xtc: carrier up \(4 loops, /,
			"$lane: four native carrier loops");
	}

	my ($keeper, $publisher, $reader, $handle);
	my $completed = eval {
		$keeper = $node->background_psql('postgres', timeout => 20);
		$publisher = $node->background_psql('postgres', timeout => 20);
		$reader = $node->background_psql('postgres', timeout => 20);
		$_->set_query_timer_restart for ($keeper, $publisher, $reader);
		my $publisher_id = $publisher->query_safe('SELECT pg_backend_pid()');
		my $reader_id = $reader->query_safe('SELECT pg_backend_pid()');
		isnt($publisher_id, $reader_id, "$lane: distinct logical backends");
		$handle = $keeper->query_safe(
			q{SELECT test_backend_runtime_timezone_publication('create', 0)});
		die "invalid DSM handle: $handle" unless $handle =~ /^\d+$/;
		is($publisher->query_safe(
			"SELECT test_backend_runtime_timezone_publication('prepare_publisher', $handle)"),
			'ready', "$lane: publisher mapping and callback preloaded");
		is($reader->query_safe(
			"SELECT test_backend_runtime_timezone_publication('prepare_reader', $handle)"),
			'ready', "$lane: reader mapping and callback preloaded");

		# No third-session command is needed while either carrier is occupied.
		# The publisher releases autonomously after seeing the reader attempt.
		$publisher->query_until(qr/tzpub_start\r?\n/, qq{
\\echo tzpub_start
SELECT test_backend_runtime_timezone_publication('publish', $handle);
\\echo tzpub_done
});
		my $b = $reader->query_safe(
			"SELECT test_backend_runtime_timezone_publication('lookup', $handle)");
		my $a = $publisher->query_until(qr/tzpub_done\r?\n/, '');
		die "publisher error: $publisher->{stderr}" if $publisher->{stderr} ne '';
		$a =~ s/\r?\ntzpub_done\r?\n.*//s;
		$a =~ s/^\s+|\s+$//g;
		my %a = map { split /=/, $_, 2 } split /;/, $a;
		my %b = map { split /=/, $_, 2 } split /;/, $b;
		for my $result (\%a, \%b)
		{
			is($result->{held}, '1', "$lane: publisher held after hash insertion");
			is($result->{attempted}, '1', "$lane: reader attempted during hold");
			is($result->{completed}, '1', "$lane: publisher completed");
			is($result->{offset}, '3600', "$lane: complete zone is UTC+01:00");
			is($result->{fiber}, $lane eq 'fiber' ? '1' : '0',
				"$lane: actual participant carrier model");
		}
		if ($lane eq 'fiber')
		{
			is($a{pid}, $b{pid}, "$lane: cache shared in one process");
			is($b{same_carrier}, '0', "$lane: reader entered on another OS carrier");
			# The tests-only old-code control must fail HERE with actual 1.
			is($b{early}, '0', "$lane: lookup cannot return before publication");
		}
		else
		{
			isnt($a{pid}, $b{pid}, "$lane: independent process caches");
			like($b{early}, qr/^[01]$/, "$lane: own cache may return before release");
		}
		is($publisher->query_safe('SELECT pg_backend_pid()'), $publisher_id,
			"$lane: original publisher remains usable");
		is($reader->query_safe('SELECT pg_backend_pid()'), $reader_id,
			"$lane: original reader remains usable");
		for my $session ($publisher, $reader)
		{
			is($session->query_safe(q{
SELECT '2000-03-15 13:14:02 TZPUBCACHE-1'::timestamptz =
       '2000-03-15 13:14:02+01:00'::timestamptz
}), 't', "$lane: published zone agrees with numeric input");
		}
		$publisher->quit;
		$publisher = undef;
		$reader->quit;
		$reader = undef;
		is($keeper->query_safe(
			"SELECT test_backend_runtime_timezone_publication('cleanup', $handle)"),
			'cleaned', "$lane: registration and keeper mapping removed after quiescence");
		$keeper->quit;
		$keeper = undef;
		1;
	};
	if (!$completed)
	{
		fail("$lane: publication fixture completed (scheduling miss is not green)");
		diag($@);
	}
	my $stopped = $node->stop('fast', timeout => 10, fail_ok => 1);
	ok($stopped, "$lane: clean shutdown");
	$node->stop('immediate', timeout => 5) unless $stopped;
	for my $session ($publisher, $reader, $keeper)
	{
		next unless defined $session;
		eval { $session->{run}->finish; };
		diag($@) if $@;
	}
	unlike(slurp_file($node->logfile),
		qr/timezone (?:test missed|publisher did not)|retained .*TopMemoryContext|retained runtime-owned roots|PANIC:|segmentation fault|TRAP:|was terminated by signal/i,
		"$lane: no missed interleaving, retained roots or crash diagnostics");
}

done_testing();

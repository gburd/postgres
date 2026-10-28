# Copyright (c) 2026, PostgreSQL Global Development Group

# An exec fallback must replay postmaster GUCs even after restoring
# multithreaded=on.  A value equal to its boot default must retain its source,
# too: otherwise max_wal_senders=10 can conceal a skipped assignment.
use strict;
use warnings FATAL => 'all';

use File::Copy qw(copy);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'process fallback requires Unix and SysV shared memory'
  if $windows_os;

$ENV{PGCTLTIMEOUT} = 30;
$ENV{PGCONNECT_TIMEOUT} = 5;
$ENV{PG_XTC_CARRIER_LOOPS} = 1;

# Capture connection failures and timeouts without aborting the other cases.
sub query_ok
{
	my ($node, $sql, $label) = @_;
	my ($out, $err, $timed_out);
	my $ret = $node->psql('postgres', $sql,
		connstr => $node->connstr('postgres')
		  . " connect_timeout=5 options='-c statement_timeout=10000'",
		stdout => \$out, stderr => \$err,
		timeout => 15, timed_out => \$timed_out);
	my $completed = !$timed_out && defined($ret) && $ret == 0;
	ok($completed, $label)
	  or diag('psql status=' . (defined($ret) ? $ret : 'timeout')
		  . ", stdout=$out, stderr=$err");
	return $completed ? $out : undef;
}

# A failed connection ends only this case's dependent assertions.  The failed
# success assertion remains red, and the caller still checks fast shutdown.
sub check_bootstrap
{
	my ($node, $fallback, $senders, $before) = @_;
	my $mode = $fallback ? 'on' : 'off';
	my @settings = (
		['xtc_force_process_fallback', $mode],
		['shared_memory_type', 'sysv'],
		['pooled_protocol_carriers', '4'],
		['max_connections', '4'],
		['superuser_reserved_connections', '0'],
		['reserved_connections', '0'],
		['io_method', 'sync'],
		['wal_level', 'replica'],
		['autovacuum', 'off'],
		['summarize_wal', 'off'],
		['ssl', 'off']);
	push @settings, ['max_wal_senders', $senders] if $before;
	push @settings, ['multithreaded', $mode];
	push @settings, ['max_wal_senders', $senders] unless $before;

	# Explicit final assignments discard Cluster.pm/TEMP_CONFIG duplicates.
	# Only max_wal_senders is allowed after multithreaded in this fixture.
	$node->append_conf('postgresql.conf',
		join("\n", map { "$_->[0] = $_->[1]" } @settings));
	my $conf = slurp_file($node->data_dir . '/postgresql.conf');
	my %source_line;
	my @lines = split /\n/, $conf;
	for my $i (0 .. $#lines)
	{
		$source_line{$1} = $i + 1 if $lines[$i] =~ /^([a-z_]+) = /;
	}

	# Logs live outside pgdata; keep the exact inputs there even on green runs.
	copy($node->data_dir . '/postgresql.conf',
		$node->logfile . '.postgresql.conf') or die "copy config: $!";
	return unless ok($node->start(fail_ok => 1), 'postmaster starts');

	if ($fallback)
	{
		my $path = $node->data_dir . '/global/config_exec_params';
		return unless ok(-f $path, 'exec GUC input exists');
		copy($path, $node->logfile . '.config_exec_params')
		  or die "copy exec GUC input: $!";
		my $params = slurp_file($path);
		my $multi_pos = index($params, "multithreaded\0true\0");
		my $sender_pos = index($params, "max_wal_senders\0$senders\0");
		ok($multi_pos >= 0, 'saved multithreaded is true');
		ok($sender_pos >= 0, 'saved max_wal_senders matches the request');
		ok($multi_pos >= 0 && $sender_pos >= 0
			  && ($before ? $sender_pos < $multi_pos : $sender_pos > $multi_pos),
			'saved records have the intended replay order');
	}

	my $out = query_ok($node, 'SELECT 1', 'first connection succeeds');
	return unless defined $out;
	is($out, '1', 'first query returns its result');

	my $names = join(', ', map { "'$_->[0]'" } @settings);
	$out = query_ok($node, qq{
SELECT name, setting, source, sourcefile, sourceline
FROM pg_settings WHERE name IN ($names) ORDER BY name;
}, 'settings query succeeds');
	return unless defined $out;
	my %observed;
	for my $row (split /\n/, $out)
	{
		my ($name, @fields) = split /\|/, $row, -1;
		$observed{$name} = \@fields;
	}
	is(scalar(keys %observed), scalar(@settings), 'all requested GUCs returned');
	for my $setting (@settings)
	{
		my ($name, $value) = @$setting;
		my ($actual, $source, $file, $line) = @{ $observed{$name} // [] };
		is($actual, $value, "$name value restored");
		is($source, 'configuration file', "$name source restored");
		like($file // '', qr{(?:^|/)postgresql\.conf$}, "$name source file restored");
		is($line, $source_line{$name}, "$name final source line restored");
	}

	$out = query_ok($node, 'CREATE EXTENSION test_backend_runtime_threaded',
		'dynamic fixture loads');
	return unless defined $out;
	$out = query_ok($node, q{
SELECT pg_backend_pid(), test_backend_runtime_model_snapshot(),
       (SELECT size FROM pg_shmem_allocations WHERE name = 'Wal Sender Ctl');
}, 'runtime and shared memory query succeeds');
	return unless defined $out;
	my ($pid, $runtime, $model, $limit, $requested, $size) = split /\|/, $out, -1;
	like($pid // '', qr/^[1-9][0-9]*$/, 'backend has a real PID');
	my $postmaster_pid = slurp_file($node->data_dir . '/postmaster.pid');
	$postmaster_pid =~ s/\n.*//s;
	isnt($pid, $postmaster_pid, 'backend PID differs from postmaster');
	is($runtime, 'process', 'actual runtime is process');
	is($model, 'process', 'actual extension model is process');
	like($size // '', qr/^[1-9][0-9]*$/, 'WAL sender allocation has positive size');
	diag("max_wal_senders=$senders, Wal Sender Ctl size=" . ($size // 'missing'));

	$out = query_ok($node, 'SELECT 42', 'fresh connection succeeds');
	return unless defined $out;
	is($out, '42', 'fresh connection is usable');
}

for my $case (
	['fork_zero_after', 0, 0, 0],
	['fork_ten_after', 0, 10, 0],
	['fallback_zero_after', 1, 0, 0],
	['fallback_ten_after', 1, 10, 0],
	['fallback_zero_before', 1, 0, 1])
{
	my ($name, $fallback, $senders, $before) = @$case;
	subtest $name => sub {
		my $node = PostgreSQL::Test::Cluster->new("fallback_guc_$name");
		$node->init;
		my $completed = eval {
			check_bootstrap($node, $fallback, $senders, $before);
			1;
		};
		if (!$completed)
		{
			my $error = $@;
			fail('bootstrap checks completed without a harness exception');
			diag($error);
		}
		diag('postmaster log and input snapshots: ' . $node->logfile);
		my $stopped = $node->stop('fast', timeout => 30, fail_ok => 1);
		ok($stopped, 'clean fast shutdown');
		done_testing();
	};
}

done_testing();

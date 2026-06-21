#!/usr/bin/env perl

use strict;
use warnings FATAL => 'all';

use Cwd qw(abs_path);
use File::Path qw(make_path);
use File::Spec;
use FindBin;
use Getopt::Long qw(GetOptions);
use POSIX qw(strftime);

my $repo_root = abs_path(File::Spec->catdir($FindBin::Bin, '..', '..', '..'));
my $matrix_script =
  File::Spec->catfile($repo_root, 'src', 'tools', 'benchmark',
	'mtpg_pgbench_matrix.pl');

my $vanilla_install = '/home/sam/codex-work/vanilla-pg19/tmp_install';
my $branch_install = File::Spec->catdir($repo_root, 'tmp_install');
my $client_install = $vanilla_install;
my $out_dir = File::Spec->catdir('/tmp',
	sprintf('mtpg_phase15_benchmark_suite_%s',
		strftime('%Y%m%d_%H%M%S', localtime)));
my $profiles = 'pinned_hot,pool_realish_100ms,pool_realish_1000ms';
my $quick = 0;
my $override_runs;
my $override_duration;
my $override_warmup;
my @extra_matrix_args;
my $help = 0;

GetOptions(
	'vanilla-install=s' => \$vanilla_install,
	'branch-install=s'  => \$branch_install,
	'client-install=s'  => \$client_install,
	'matrix-script=s'   => \$matrix_script,
	'out-dir=s'         => \$out_dir,
	'profiles=s'        => \$profiles,
	'quick'             => \$quick,
	'runs=i'            => \$override_runs,
	'duration=i'        => \$override_duration,
	'warmup=i'          => \$override_warmup,
	'matrix-arg=s'      => \@extra_matrix_args,
	'help'              => \$help,
) or die usage();

if ($help)
{
	print usage();
	exit 0;
}

die "--runs must be positive\n"
  if defined $override_runs && $override_runs <= 0;
die "--duration must be positive\n"
  if defined $override_duration && $override_duration <= 0;
die "--warmup must be non-negative\n"
  if defined $override_warmup && $override_warmup < 0;
die "matrix script is not executable: $matrix_script\n"
  unless -x $matrix_script;

my @profile_order = qw(
  pinned_hot
  pool_realish_100ms
  pool_realish_1000ms
  pool_stateful_1000ms
  pool_scale_1000_realish
  connection_churn_realish
  pool_idle_100ms
  pool_idle_1000ms
  pool_burst_10ms
  pool_scale_1000_idle
  connection_memory_idle
  connection_churn
);

my %profile_specs = (
	pinned_hot => {
		description =>
		  'Hot-path parity check for process, thread-per-session, and vanilla.',
		lanes => 'vanilla,branch_process,branch_threaded',
		workloads =>
		  'builtin_select_prepared,select1_prepared,bench_one_prepared,kv_read_prepared',
		clients => 32,
		threads => 8,
		max_connections => 96,
		scale => 10,
		duration => 20,
		warmup => 4,
		runs => 3,
		restart_per_workload => 1,
		sample_server_resources => 0,
	},
	pool_realish_100ms => {
		description =>
		  'Mostly idle c200 profile with indexed reads, writes, WAL, and client think time.',
		lanes => 'vanilla,branch_process,branch_threaded,branch_pool',
		pool_sizes => '32,64,128,192',
		workloads =>
		  'kv_read_sleep_wake_100ms_prepared,app_txn_sleep_wake_100ms_prepared,app_mixed_sleep_wake_100ms_prepared',
		clients => 200,
		threads => 32,
		max_connections => 260,
		scale => 10,
		duration => 20,
		warmup => 4,
		runs => 3,
		sample_server_resources => 1,
		resource_sample_interval_ms => 250,
	},
	pool_realish_1000ms => {
		description =>
		  'Long-idle c200 profile with real table/index work and protocol-read parks.',
		lanes => 'vanilla,branch_process,branch_threaded,branch_pool',
		pool_sizes => '16,32,64,128',
		workloads =>
		  'kv_read_sleep_wake_1000ms_prepared,app_txn_sleep_wake_1000ms_prepared',
		clients => 200,
		threads => 32,
		max_connections => 260,
		scale => 10,
		duration => 25,
		warmup => 4,
		runs => 3,
		sample_server_resources => 1,
		resource_sample_interval_ms => 250,
	},
	pool_stateful_1000ms => {
		description =>
		  'Stateful c100 temp-table session diagnostic profile.',
		lanes => 'vanilla,branch_process,branch_threaded,branch_pool',
		pool_sizes => '16,32,64',
		workloads => 'stateful_temp_sleep_wake_1000ms_prepared',
		clients => 100,
		threads => 16,
		max_connections => 160,
		scale => 10,
		duration => 30,
		warmup => 5,
		runs => 2,
		sample_server_resources => 1,
		sample_memory_detail => 1,
		log_protocol_park_memory => 1,
		resource_sample_interval_ms => 500,
	},
	pool_scale_1000_realish => {
		description =>
		  'Large c1000 indexed-read idle population: vanilla and pinned threads versus bounded carrier pools.',
		lanes => 'vanilla,branch_threaded,branch_pool',
		pool_sizes => '64,128,256,512',
		workloads => 'kv_read_sleep_wake_1000ms_prepared',
		clients => 1000,
		threads => 64,
		max_connections => 1100,
		scale => 10,
		duration => 20,
		warmup => 3,
		runs => 2,
		sample_server_resources => 1,
		resource_sample_interval_ms => 500,
	},
	connection_churn_realish => {
		description =>
		  'One database-touching transaction per connection for reconnect-heavy client patterns.',
		lanes => 'vanilla,branch_process,branch_threaded,branch_pool',
		pool_sizes => '32,64,128',
		workloads => 'app_txn_connect_prepared',
		clients => 64,
		threads => 16,
		max_connections => 160,
		scale => 10,
		duration => 20,
		warmup => 4,
		runs => 3,
		sample_server_resources => 1,
		resource_sample_interval_ms => 250,
	},
	pool_idle_100ms => {
		description =>
		  'Mostly idle c200 profile: pool should preserve throughput with fewer server threads/processes.',
		lanes => 'vanilla,branch_process,branch_threaded,branch_pool',
		pool_sizes => '32,64,128,192',
		workloads => 'select1_sleep_wake_100ms_prepared',
		clients => 200,
		threads => 32,
		max_connections => 260,
		scale => 1,
		duration => 15,
		warmup => 3,
		runs => 3,
		sample_server_resources => 1,
		resource_sample_interval_ms => 250,
	},
	pool_idle_1000ms => {
		description =>
		  'Long-idle c200 profile: pool should cap carriers while sessions mostly wait on clients.',
		lanes => 'vanilla,branch_process,branch_threaded,branch_pool',
		pool_sizes => '16,32,64,128',
		workloads => 'select1_sleep_wake_1000ms_prepared',
		clients => 200,
		threads => 32,
		max_connections => 260,
		scale => 1,
		duration => 20,
		warmup => 3,
		runs => 3,
		sample_server_resources => 1,
		resource_sample_interval_ms => 250,
	},
	pool_burst_10ms => {
		description =>
		  'Short-idle diagnostic profile for parking overhead and bursty wakeups.',
		lanes => 'branch_threaded,branch_pool',
		pool_sizes => '64,128,192',
		workloads => 'select1_sleep_wake_10ms_prepared',
		clients => 200,
		threads => 32,
		max_connections => 260,
		scale => 1,
		duration => 15,
		warmup => 3,
		runs => 3,
		sample_server_resources => 1,
		resource_sample_interval_ms => 250,
	},
	pool_scale_1000_idle => {
		description =>
		  'Large mostly-idle connection profile: compare pinned threads with large carrier pools.',
		lanes => 'branch_threaded,branch_pool',
		pool_sizes => '64,128,256,512',
		workloads => 'select1_sleep_wake_1000ms_prepared',
		clients => 1000,
		threads => 64,
		max_connections => 1100,
		scale => 1,
		duration => 30,
		warmup => 5,
		runs => 3,
		sample_server_resources => 1,
		resource_sample_interval_ms => 500,
	},
	connection_memory_idle => {
		description =>
		  'Large idle connection memory profile: process, pinned thread, and pooled carrier footprint.',
		lanes => 'vanilla,branch_process,branch_threaded,branch_pool',
		pool_sizes => '64,128,256,512',
		workloads => 'select1_sleep_wake_1000ms_prepared',
		clients => 1000,
		threads => 64,
		max_connections => 1100,
		scale => 1,
		duration => 20,
		warmup => 3,
		runs => 2,
		sample_server_resources => 1,
		sample_memory_detail => 1,
		log_protocol_park_memory => 1,
		resource_sample_interval_ms => 500,
	},
	connection_churn => {
		description =>
		  'One transaction per connection profile for reconnect-heavy client patterns.',
		lanes => 'vanilla,branch_process,branch_threaded,branch_pool',
		pool_sizes => '32,64,128',
		workloads => 'select1_connect_prepared',
		clients => 64,
		threads => 16,
		max_connections => 160,
		scale => 10,
		duration => 15,
		warmup => 3,
		runs => 3,
		sample_server_resources => 1,
		resource_sample_interval_ms => 250,
	},
);

my @selected_profiles = expand_profiles($profiles, \@profile_order, \%profile_specs);

make_path($out_dir);
my $commands_path = File::Spec->catfile($out_dir, 'commands.sh');
open my $commands_fh, '>', $commands_path
  or die "could not write $commands_path: $!";
print $commands_fh "#!/bin/sh\nset -eu\n\n";

my @completed;
for my $profile_name (@selected_profiles)
{
	my $profile = profile_with_overrides($profile_specs{$profile_name});
	my $profile_dir = File::Spec->catdir($out_dir, $profile_name);
	my @cmd = matrix_command($profile, $profile_dir);

	print "==> running $profile_name\n";
	print "    $profile->{description}\n";
	print $commands_fh shell_join(@cmd), "\n\n";

	my $rc = system @cmd;
	if ($rc != 0)
	{
		close $commands_fh;
		die "$profile_name failed with exit code " . ($rc >> 8) . "\n";
	}
	push @completed, $profile_name;
}

close $commands_fh;
chmod 0755, $commands_path;
write_index($out_dir, \@completed);

print "wrote $out_dir\n";
print "wrote ", File::Spec->catfile($out_dir, 'index.md'), "\n";

sub usage
{
	return <<'USAGE';
Usage: src/tools/benchmark/mtpg_phase15_benchmark_suite.pl [options]

Runs named Phase 15 benchmark profiles through mtpg_pgbench_matrix.pl.

Options:
  --profiles=LIST          comma-separated profile names, showcase, or all
                           default: pinned_hot,pool_realish_100ms,pool_realish_1000ms
  --quick                  use short one-run profiles for smoke validation
  --runs=N                 override measured repetitions for every profile
  --duration=SECONDS       override measured duration for every profile
  --warmup=SECONDS         override warmup duration for every profile
  --out-dir=DIR            output directory
  --vanilla-install=DIR    vanilla PostgreSQL install tree
  --branch-install=DIR     branch PostgreSQL install tree
  --client-install=DIR     client binary install tree
  --matrix-script=PATH     pgbench matrix runner
  --matrix-arg=ARG         extra argument passed through to each matrix run

Profiles:
  pinned_hot               hot-path vanilla/process/threaded parity
  pool_realish_100ms       c200 indexed reads/writes/ranges with 100ms think time
  pool_realish_1000ms      c200 real-ish table/index work plus long parks
  pool_stateful_1000ms     c100 temp-table session diagnostic profile
  pool_scale_1000_realish  c1000 pinned thread vs pooled real-ish scale profile
  connection_churn_realish reconnect-heavy profile with real table/index/WAL work
  pool_idle_100ms          c200 mostly-idle scale profile, all lanes
  pool_idle_1000ms         c200 long-idle scale profile, all lanes
  pool_burst_10ms          short-idle pooled diagnostic profile
  pool_scale_1000_idle     c1000 long-idle pinned vs pooled scale profile
  connection_memory_idle   c1000 idle connection memory footprint profile
  connection_churn         reconnect-heavy profile
USAGE
}

sub expand_profiles
{
	my ($profiles_arg, $profile_order, $profile_specs) = @_;
	my @names;

	for my $name (grep { length($_) } split /,/, $profiles_arg)
	{
		if ($name eq 'all')
		{
			push @names, @$profile_order;
			next;
		}
		if ($name eq 'showcase')
		{
			push @names, qw(
			  pinned_hot
			  pool_realish_100ms
			  pool_realish_1000ms
			  pool_scale_1000_realish
			  connection_churn_realish
			);
			next;
		}
		die "unknown profile: $name\n" unless exists $profile_specs->{$name};
		push @names, $name;
	}

	die "no profiles selected\n" unless @names;
	return deduplicate(@names);
}

sub deduplicate
{
	my @names = @_;
	my @deduplicated;
	my %seen;

	for my $name (@names)
	{
		next if $seen{$name}++;
		push @deduplicated, $name;
	}
	return @deduplicated;
}

sub profile_with_overrides
{
	my ($profile) = @_;
	my %copy = %$profile;

	if ($quick)
	{
		$copy{duration} = 5;
		$copy{warmup} = 1;
		$copy{runs} = 1;
		$copy{resource_sample_interval_ms} = 500
		  if $copy{sample_server_resources};
	}

	$copy{runs} = $override_runs if defined $override_runs;
	$copy{duration} = $override_duration if defined $override_duration;
	$copy{warmup} = $override_warmup if defined $override_warmup;

	return \%copy;
}

sub matrix_command
{
	my ($profile, $profile_dir) = @_;

	my @cmd = (
		$^X,
		$matrix_script,
		"--out-dir=$profile_dir",
		"--vanilla-install=$vanilla_install",
		"--branch-install=$branch_install",
		"--client-install=$client_install",
		"--duration=$profile->{duration}",
		"--warmup=$profile->{warmup}",
		"--runs=$profile->{runs}",
		"--clients=$profile->{clients}",
		"--threads=$profile->{threads}",
		"--max-connections=$profile->{max_connections}",
		"--scale=$profile->{scale}",
		"--lanes=$profile->{lanes}",
		"--workloads=$profile->{workloads}",
	);

	push @cmd, "--pool-sizes=$profile->{pool_sizes}"
	  if defined $profile->{pool_sizes};
	push @cmd, '--restart-per-workload'
	  if $profile->{restart_per_workload};
	if ($profile->{sample_server_resources})
	{
		push @cmd, '--sample-server-resources';
		push @cmd,
		  "--resource-sample-interval-ms=$profile->{resource_sample_interval_ms}";
	}
	push @cmd, '--sample-memory-detail'
	  if $profile->{sample_memory_detail};
	push @cmd, '--log-protocol-park-memory'
	  if $profile->{log_protocol_park_memory};
	push @cmd, @extra_matrix_args;

	return @cmd;
}

sub write_index
{
	my ($dir, $completed) = @_;
	my $path = File::Spec->catfile($dir, 'index.md');

	open my $fh, '>', $path or die "could not write $path: $!";
	print $fh "# Phase 15 benchmark suite\n\n";
	print $fh "- branch install: `$branch_install`\n";
	print $fh "- vanilla install: `$vanilla_install`\n";
	print $fh "- client install: `$client_install`\n";
	print $fh "- commands: `commands.sh`\n\n";
	print $fh "| Profile | Purpose | Results |\n";
	print $fh "| --- | --- | --- |\n";
	for my $profile_name (@$completed)
	{
		my $description = $profile_specs{$profile_name}{description};
		print $fh "| `$profile_name` | $description | ",
		  "`$profile_name/summary.md` |\n";
	}
	close $fh;
}

sub shell_join
{
	return join ' ', map { shell_quote($_) } @_;
}

sub shell_quote
{
	my ($arg) = @_;
	return "''" if $arg eq '';
	return $arg if $arg =~ /^[A-Za-z0-9_.,:\/=+-]+$/;
	$arg =~ s/'/'"'"'/g;
	return "'$arg'";
}

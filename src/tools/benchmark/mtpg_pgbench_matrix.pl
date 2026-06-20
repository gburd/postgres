#!/usr/bin/env perl

use strict;
use warnings FATAL => 'all';

use Cwd qw(abs_path);
use File::Path qw(make_path remove_tree);
use File::Spec;
use FindBin;
use Getopt::Long qw(GetOptions);
use IO::Socket::INET;
use POSIX qw(WNOHANG strftime);

my $repo_root = abs_path(File::Spec->catdir($FindBin::Bin, '..', '..', '..'));

my $vanilla_install = '/home/sam/codex-work/vanilla-pg19/tmp_install';
my $branch_install = File::Spec->catdir($repo_root, 'tmp_install');
my $client_install = $vanilla_install;
my $out_dir = File::Spec->catdir('/tmp',
	sprintf('mtpg_pgbench_matrix_%s', strftime('%Y%m%d_%H%M%S', localtime)));
my $duration = 35;
my $warmup = 5;
my $clients = 8;
my $threads = 8;
my $scale = 10;
my $max_connections = 100;
my $shared_buffers = '128MB';
my $pool_sizes = '4,8,16';
my $runs = 1;
my $workloads =
  'builtin_select_simple,builtin_select_prepared,select1_prepared,bench_one_prepared,kv_read_prepared';
my $lanes = 'vanilla,branch_process,branch_threaded,branch_pool';
my $reuse = 0;
my $restart_per_workload = 0;
my $sample_server_resources = 0;
my $resource_sample_interval_ms = 100;
my $help = 0;
my $socket_seq = 0;
my $default_max_files_per_process = 1000;

GetOptions(
	'vanilla-install=s' => \$vanilla_install,
	'branch-install=s'  => \$branch_install,
	'client-install=s'  => \$client_install,
	'out-dir=s'         => \$out_dir,
	'duration=i'        => \$duration,
	'warmup=i'          => \$warmup,
	'clients=i'         => \$clients,
	'threads=i'         => \$threads,
	'scale=i'           => \$scale,
	'max-connections=i' => \$max_connections,
	'shared-buffers=s'  => \$shared_buffers,
	'pool-sizes=s'      => \$pool_sizes,
	'runs=i'            => \$runs,
	'workloads=s'       => \$workloads,
	'lanes=s'           => \$lanes,
	'reuse'             => \$reuse,
	'restart-per-workload' => \$restart_per_workload,
	'sample-server-resources!' => \$sample_server_resources,
	'resource-sample-interval-ms=i' => \$resource_sample_interval_ms,
	'help'              => \$help,
) or die usage();

if ($help)
{
	print usage();
	exit 0;
}

die "--duration must be positive\n" if $duration <= 0;
die "--warmup must be non-negative\n" if $warmup < 0;
die "--clients must be positive\n" if $clients <= 0;
die "--threads must be positive\n" if $threads <= 0;
die "--scale must be positive\n" if $scale <= 0;
die "--max-connections must exceed --clients\n"
  if $max_connections <= $clients;
die "--runs must be positive\n" if $runs <= 0;
die "--resource-sample-interval-ms must be positive\n"
  if $resource_sample_interval_ms <= 0;

my @pool_sizes = grep { length($_) } split /,/, $pool_sizes;
for my $size (@pool_sizes)
{
	die "invalid pool size: $size\n" unless $size =~ /^\d+$/ && $size > 0;
}

my @requested_workloads = grep { length($_) } split /,/, $workloads;
my @requested_lanes = grep { length($_) } split /,/, $lanes;

my %workload_specs = (
	builtin_select_simple => {
		args => [ '-S', '-M', 'simple' ],
		needs_extra_setup => 0,
	},
	builtin_select_prepared => {
		args => [ '-S', '-M', 'prepared' ],
		needs_extra_setup => 0,
	},
	select1_prepared => {
		args => [ '-M', 'prepared', '-f', undef ],
		script => 'select1.sql',
		needs_extra_setup => 0,
	},
	bench_one_prepared => {
		args => [ '-M', 'prepared', '-f', undef ],
		script => 'bench_one.sql',
		needs_extra_setup => 1,
	},
	kv_read_prepared => {
		args => [ '-M', 'prepared', '-f', undef ],
		script => 'kv_read.sql',
		needs_extra_setup => 1,
	},
	select1_sleep_1ms_prepared => {
		args => [ '-M', 'prepared', '-f', undef ],
		script => 'select1_sleep_1ms.sql',
		needs_extra_setup => 0,
	},
	select1_sleep_10ms_prepared => {
		args => [ '-M', 'prepared', '-f', undef ],
		script => 'select1_sleep_10ms.sql',
		needs_extra_setup => 0,
	},
	select1_sleep_100ms_prepared => {
		args => [ '-M', 'prepared', '-f', undef ],
		script => 'select1_sleep_100ms.sql',
		needs_extra_setup => 0,
	},
	select1_sleep_1000ms_prepared => {
		args => [ '-M', 'prepared', '-f', undef ],
		script => 'select1_sleep_1000ms.sql',
		needs_extra_setup => 0,
	},
	select1_connect_prepared => {
		args => [ '-C', '-M', 'prepared', '-f', undef ],
		script => 'select1.sql',
		needs_extra_setup => 0,
	},
);

for my $workload (@requested_workloads)
{
	die "unknown workload: $workload\n" unless exists $workload_specs{$workload};
}

my @lane_specs;
for my $lane (@requested_lanes)
{
	if ($lane eq 'vanilla')
	{
		push @lane_specs, {
			name => 'vanilla',
			install => $vanilla_install,
			config => [],
			branch => 0,
		};
	}
	elsif ($lane eq 'branch_process')
	{
		push @lane_specs, {
			name => 'branch_process',
			install => $branch_install,
			config => [],
			branch => 1,
		};
	}
	elsif ($lane eq 'branch_threaded')
	{
		push @lane_specs, {
			name => 'branch_threaded',
			install => $branch_install,
			config => [ 'multithreaded = on', 'pooled_protocol_carriers = 0' ],
			branch => 1,
		};
	}
	elsif ($lane eq 'branch_pool')
	{
		for my $size (@pool_sizes)
		{
			push @lane_specs, {
				name => "branch_pool_$size",
				install => $branch_install,
				config => [
					'multithreaded = on',
					"pooled_protocol_carriers = $size",
				],
				branch => 1,
			};
		}
	}
	else
	{
		die "unknown lane: $lane\n";
	}
}

die "no lanes selected\n" unless @lane_specs;

verify_install($client_install, 'client');
verify_install($vanilla_install, 'vanilla') if lane_selected('vanilla', \@lane_specs);
verify_install($branch_install, 'branch') if grep { $_->{branch} } @lane_specs;
install_library_paths($client_install, $vanilla_install, $branch_install);

if (-e $out_dir && !$reuse)
{
	die "output directory already exists: $out_dir\n";
}

make_path($out_dir);
my $script_dir = File::Spec->catdir($out_dir, 'scripts');
make_path($script_dir);
write_workload_scripts($script_dir);

my $tps_path = File::Spec->catfile($out_dir, 'tps.tsv');
open my $tps_fh, '>', $tps_path or die "could not write $tps_path: $!";
print $tps_fh join("\t", qw(lane workload tps latency_ms failed_transactions)), "\n";

my $samples_path = File::Spec->catfile($out_dir, 'samples.tsv');
open my $samples_fh, '>', $samples_path
  or die "could not write $samples_path: $!";
print $samples_fh
  join("\t", qw(lane workload run tps latency_ms failed_transactions)), "\n";

my $resources_path = File::Spec->catfile($out_dir, 'server_resources.tsv');
open my $resources_fh, '>', $resources_path
  or die "could not write $resources_path: $!";
print $resources_fh
  join("\t", qw(lane workload max_server_processes max_server_threads
	  max_server_rss_kb max_server_pss_kb max_server_private_kb samples)),
  "\n";

my %results;
if ($restart_per_workload)
{
	for my $lane (@lane_specs)
	{
		for my $workload (@requested_workloads)
		{
			run_lane($lane, [ $workload ], $script_dir, $tps_fh, $samples_fh,
				$resources_fh, \%results, $workload);
		}
	}
}
else
{
	for my $lane (@lane_specs)
	{
		run_lane($lane, \@requested_workloads, $script_dir, $tps_fh, $samples_fh,
			$resources_fh, \%results, undef);
	}
}

close $tps_fh;
close $samples_fh;
close $resources_fh;

write_ratios($out_dir, \@requested_workloads, \@lane_specs, \%results);
write_summary($out_dir, \@requested_workloads, \@lane_specs, \%results);

print "wrote $tps_path\n";
print "wrote $samples_path\n";
print "wrote $resources_path\n";
print "wrote ", File::Spec->catfile($out_dir, 'ratios.tsv'), "\n";
print "wrote ", File::Spec->catfile($out_dir, 'summary.md'), "\n";

sub usage
{
	return <<'USAGE';
Usage: src/tools/benchmark/mtpg_pgbench_matrix.pl [options]

Runs the multithreaded branch pgbench comparison matrix:
  vanilla
  branch_process
  branch_threaded
  branch_pool_<N> for each --pool-sizes value

Key options:
  --vanilla-install=DIR   vanilla PostgreSQL install tree
  --branch-install=DIR    branch PostgreSQL install tree
  --client-install=DIR    client binary install tree, defaults to vanilla
  --out-dir=DIR           result directory
  --duration=SECONDS      measured pgbench duration, default 35
  --warmup=SECONDS        warmup duration per workload, default 5
  --clients=N             pgbench clients, default 8
  --threads=N             pgbench threads, default 8
  --scale=N               pgbench initialization scale, default 10
  --pool-sizes=LIST       comma-separated pooled carrier counts, default 4,8,16
  --runs=N                measured repetitions per lane/workload, default 1
  --lanes=LIST            vanilla,branch_process,branch_threaded,branch_pool
  --workloads=LIST        workload names to run
  --restart-per-workload  restart each lane for each workload
  --sample-server-resources
                           sample server process/thread counts while measuring
  --resource-sample-interval-ms=N
                           server resource sample interval, default 100

Additional non-default workloads useful for pooled connection-shape profiles:
  select1_sleep_1ms_prepared
  select1_sleep_10ms_prepared
  select1_sleep_100ms_prepared
  select1_sleep_1000ms_prepared
  select1_connect_prepared

Output:
  tps.tsv                 summary TPS and latency per lane/workload
  samples.tsv             per-run TPS and latency samples
  server_resources.tsv    max server process/thread counts sampled per workload
  ratios.tsv              per-lane ratios against vanilla
  summary.md              Markdown table for quick comparison
USAGE
}

sub lane_selected
{
	my ($name, $lane_specs) = @_;

	for my $lane (@$lane_specs)
	{
		return 1 if $lane->{name} eq $name;
	}
	return 0;
}

sub verify_install
{
	my ($install, $label) = @_;

	for my $bin (qw(postgres initdb pg_ctl psql pgbench))
	{
		my $path = File::Spec->catfile($install, 'bin', $bin);
		die "$label install is missing $path\n" unless -x $path;
	}

	my $tzdir = File::Spec->catdir($install, 'share', 'postgresql', 'timezonesets');
	die "$label install is missing $tzdir\n" unless -d $tzdir;
}

sub install_library_paths
{
	my @installs = @_;
	my @paths;
	my %seen;

	for my $install (@installs)
	{
		my $libdir = File::Spec->catdir($install, 'lib');
		next unless -d $libdir;
		next if $seen{$libdir}++;
		push @paths, $libdir;
	}

	if (defined $ENV{LD_LIBRARY_PATH} && length $ENV{LD_LIBRARY_PATH})
	{
		for my $libdir (split /:/, $ENV{LD_LIBRARY_PATH})
		{
			next if $seen{$libdir}++;
			push @paths, $libdir;
		}
	}

	$ENV{LD_LIBRARY_PATH} = join ':', @paths if @paths;
}

sub write_workload_scripts
{
	my ($dir) = @_;

	write_file(File::Spec->catfile($dir, 'select1.sql'), "SELECT 1;\n");
	write_file(File::Spec->catfile($dir, 'select1_sleep_1ms.sql'),
		"SELECT 1;\n"
	  . "\\sleep 1 ms\n");
	write_file(File::Spec->catfile($dir, 'select1_sleep_10ms.sql'),
		"SELECT 1;\n"
	  . "\\sleep 10 ms\n");
	write_file(File::Spec->catfile($dir, 'select1_sleep_100ms.sql'),
		"SELECT 1;\n"
	  . "\\sleep 100 ms\n");
	write_file(File::Spec->catfile($dir, 'select1_sleep_1000ms.sql'),
		"SELECT 1;\n"
	  . "\\sleep 1000 ms\n");
	write_file(File::Spec->catfile($dir, 'bench_one.sql'),
		"SELECT payload FROM bench_one WHERE id = 1;\n");
	write_file(File::Spec->catfile($dir, 'kv_read.sql'),
		"\\set id random(1, 100000)\n"
	  . "SELECT v, payload FROM bench_kv WHERE id = :id;\n");
	write_file(File::Spec->catfile($dir, 'setup_extra.sql'),
		"DROP TABLE IF EXISTS bench_one;\n"
	  . "CREATE TABLE bench_one(id int primary key, payload text not null);\n"
	  . "INSERT INTO bench_one VALUES (1, repeat('x', 128));\n"
	  . "DROP TABLE IF EXISTS bench_kv;\n"
	  . "CREATE TABLE bench_kv(id int primary key, v int not null, payload text not null);\n"
	  . "INSERT INTO bench_kv SELECT g, 0, repeat(md5(g::text), 4) FROM generate_series(1, 100000) g;\n"
	  . "VACUUM ANALYZE bench_one;\n"
	  . "VACUUM ANALYZE bench_kv;\n"
	  . "VACUUM ANALYZE pgbench_accounts;\n"
	  . "CHECKPOINT;\n");
}

sub write_file
{
	my ($path, $contents) = @_;

	open my $fh, '>', $path or die "could not write $path: $!";
	print $fh $contents;
	close $fh;
}

sub run_lane
{
	my ($lane, $workloads, $script_dir, $tps_fh, $samples_fh, $resources_fh,
		$results, $lane_dir_suffix) = @_;

	my $lane_dir_name = defined $lane_dir_suffix ?
		"$lane->{name}_$lane_dir_suffix" : $lane->{name};
	my $lane_dir = File::Spec->catdir($out_dir, $lane_dir_name);
	my $data_dir = File::Spec->catdir($lane_dir, 'data');
	my $socket_dir = File::Spec->catdir('/tmp',
		sprintf('mtpg_sock_%d_%d', $$, ++$socket_seq));
	my $server_log = File::Spec->catfile($lane_dir, 'server.log');
	my $port = pick_free_port();

	remove_tree($lane_dir) if -e $lane_dir;
	make_path($data_dir);
	make_path($socket_dir);

	my $server_bin = bin_path($lane->{install}, 'postgres');
	my $initdb_bin = bin_path($lane->{install}, 'initdb');
	my $pg_ctl_bin = bin_path($lane->{install}, 'pg_ctl');
	my $psql_bin = bin_path($client_install, 'psql');
	my $pgbench_bin = bin_path($client_install, 'pgbench');

	print "==> initializing $lane->{name} on port $port\n";
	run_cmd([ $initdb_bin, '-A', 'trust', '--no-sync', '-D', $data_dir ],
		"$lane->{name} initdb");

	append_config($data_dir, $port, $socket_dir, $lane->{config});

	my $started = 0;
	eval {
		run_cmd([
				$pg_ctl_bin, '-D', $data_dir, '-l', $server_log,
				'-o', "-k $socket_dir",
				'-w', 'start'
			],
			"$lane->{name} start");
		$started = 1;

		run_cmd([
				$pgbench_bin, '-i', '-s', $scale,
				'-h', $socket_dir, '-p', $port, 'postgres'
			],
			"$lane->{name} pgbench init");

		run_cmd([
				$psql_bin, '-X', '-v', 'ON_ERROR_STOP=1',
				'-h', $socket_dir, '-p', $port, '-d', 'postgres',
				'-f', File::Spec->catfile($script_dir, 'setup_extra.sql')
			],
			"$lane->{name} extra setup");

		for my $workload (@$workloads)
		{
			my @samples;
			my $resources = new_resource_summary();

			for my $run_index (1 .. $runs)
			{
				my ($sample_tps, $sample_latency, $sample_failed,
					$sample_resources) =
				  run_workload($lane, $workload, $script_dir, $socket_dir,
					$port, $pgbench_bin, $data_dir);

				push @samples, {
					tps => $sample_tps,
					latency => $sample_latency,
					failed => $sample_failed,
				};
				merge_resource_summary($resources, $sample_resources);
				print $samples_fh join("\t", $lane->{name}, $workload,
					$run_index, $sample_tps, $sample_latency, $sample_failed),
				  "\n";
			}

			my $summary = summarize_workload_samples(\@samples);
			$results->{$lane->{name}}{$workload} = {
				tps => $summary->{tps},
				latency => $summary->{latency},
				failed => $summary->{failed},
				resources => $resources,
			};
			print $tps_fh join("\t", $lane->{name}, $workload,
				$summary->{tps}, $summary->{latency}, $summary->{failed}),
			  "\n";
			print $resources_fh join("\t", $lane->{name}, $workload,
				resource_value($resources, 'max_server_processes'),
				resource_value($resources, 'max_server_threads'),
				resource_value($resources, 'max_server_rss_kb'),
				resource_value($resources, 'max_server_pss_kb'),
				resource_value($resources, 'max_server_private_kb'),
				resource_value($resources, 'samples')), "\n";
			print "    $workload: $summary->{tps} TPS, $summary->{latency} ms";
			print " (median of $runs runs)" if $runs > 1;
			print "\n";
		}
	};
	my $err = $@;

	if ($started)
	{
		system $pg_ctl_bin, '-D', $data_dir, '-m', 'fast', '-w', 'stop';
	}
	remove_tree($socket_dir) if -e $socket_dir;

	die $err if $err;
}

sub append_config
{
	my ($data_dir, $port, $socket_dir, $extra_config) = @_;
	my $conf = File::Spec->catfile($data_dir, 'postgresql.conf');
	my $max_files_per_process =
	  benchmark_max_files_per_process($max_connections);

	open my $fh, '>>', $conf or die "could not append $conf: $!";
	print $fh "\n# mtpg pgbench matrix\n";
	print $fh "listen_addresses = '127.0.0.1'\n";
	print $fh "port = $port\n";
	print $fh "unix_socket_directories = '$socket_dir'\n";
	print $fh "max_connections = $max_connections\n";
	print $fh "shared_buffers = $shared_buffers\n";
	if ($max_files_per_process > $default_max_files_per_process)
	{
		# Threaded lanes keep all client sockets in one server process.
		print $fh "max_files_per_process = $max_files_per_process\n";
	}
	for my $line (@$extra_config)
	{
		print $fh "$line\n";
	}
	close $fh;
}

sub benchmark_max_files_per_process
{
	my ($connections) = @_;
	my $fd_budget = $connections * 8;

	return $fd_budget > $default_max_files_per_process ?
	  $fd_budget : $default_max_files_per_process;
}

sub run_workload
{
	my ($lane, $workload, $script_dir, $socket_dir, $port, $pgbench_bin,
		$data_dir) = @_;

	my $spec = $workload_specs{$workload};
	my @args = @{ $spec->{args} };
	for my $arg (@args)
	{
		if (!defined $arg)
		{
			$arg = File::Spec->catfile($script_dir, $spec->{script});
		}
	}

	my @base_cmd = (
		$pgbench_bin,
		'-n',
		'-c', $clients,
		'-j', $threads,
		'-h', $socket_dir,
		'-p', $port,
		@args,
	);

	if ($warmup > 0)
	{
		my $warm = File::Spec->catfile($out_dir,
			"$lane->{name}_${workload}.warm");
		run_capture([ @base_cmd, '-T', $warmup, 'postgres' ], "$workload warmup",
			"$warm.out", "$warm.err");
	}

	my $bench = File::Spec->catfile($out_dir, "$lane->{name}_${workload}.bench");
	my $resources = new_server_resource_sample($data_dir);
	my $output = run_capture([ @base_cmd, '-T', $duration, 'postgres' ],
		"$lane->{name} $workload", $bench, "$bench.err", $resources);

	my ($tps) = $output =~ /^tps = ([0-9.]+) /m;
	my ($latency) = $output =~ /^latency average = ([0-9.]+) ms/m;
	my ($failed) = $output =~ /^number of failed transactions: ([0-9]+)/m;

	die "could not parse TPS for $lane->{name} $workload\n$output\n"
	  unless defined $tps && defined $latency && defined $failed;

	return ($tps, $latency, $failed, $resources);
}

sub summarize_workload_samples
{
	my ($samples) = @_;
	my @tps_values = map { $_->{tps} } @$samples;
	my @latency_values = map { $_->{latency} } @$samples;
	my $failed_total = 0;

	for my $sample (@$samples)
	{
		$failed_total += $sample->{failed};
	}

	return {
		tps => median(@tps_values),
		latency => median(@latency_values),
		failed => $failed_total,
	};
}

sub median
{
	my @values = sort { $a <=> $b } @_;
	my $count = scalar @values;

	die "cannot compute median of no samples\n" if $count == 0;

	if ($count % 2)
	{
		return $values[int($count / 2)];
	}

	return ($values[$count / 2 - 1] + $values[$count / 2]) / 2;
}

sub new_resource_summary
{
	return {
		max_server_processes => undef,
		max_server_threads => undef,
		max_server_rss_kb => undef,
		max_server_pss_kb => undef,
		max_server_private_kb => undef,
		samples => 0,
	};
}

sub merge_resource_summary
{
	my ($summary, $sample) = @_;

	return unless defined $summary && defined $sample;

	update_resource_max($summary, max_server_processes =>
		$sample->{max_server_processes});
	update_resource_max($summary, max_server_threads =>
		$sample->{max_server_threads});
	update_resource_max($summary, max_server_rss_kb =>
		$sample->{max_server_rss_kb});
	update_resource_max($summary, max_server_pss_kb =>
		$sample->{max_server_pss_kb});
	update_resource_max($summary, max_server_private_kb =>
		$sample->{max_server_private_kb});
	$summary->{samples} += $sample->{samples}
	  if defined $sample->{samples};
}

sub bin_path
{
	my ($install, $bin) = @_;
	return File::Spec->catfile($install, 'bin', $bin);
}

sub run_cmd
{
	my ($cmd, $label) = @_;

	my $rc = system @$cmd;
	if ($rc != 0)
	{
		die "$label failed with exit code " . ($rc >> 8) . ": @$cmd\n";
	}
}

sub run_capture
{
	my ($cmd, $label, $stdout_path, $stderr_path, $resource_sample) = @_;

	open my $out, '>', $stdout_path or die "could not write $stdout_path: $!";
	open my $err, '>', $stderr_path or die "could not write $stderr_path: $!";

	my $pid = fork();
	die "fork failed for $label: $!" unless defined $pid;
	if ($pid == 0)
	{
		open STDOUT, '>&', $out or die "dup stdout failed: $!";
		open STDERR, '>&', $err or die "dup stderr failed: $!";
		exec @$cmd or die "exec failed for $label: $!";
	}

	if (defined $resource_sample && $resource_sample->{enabled})
	{
		for (;;)
		{
			my $waited = waitpid($pid, WNOHANG);
			if ($waited == $pid)
			{
				last;
			}
			elsif ($waited == 0)
			{
				sample_server_resources($resource_sample);
				select(undef, undef, undef,
					$resource_sample->{interval_seconds});
			}
			else
			{
				last;
			}
		}
	}
	else
	{
		waitpid($pid, 0);
	}
	my $rc = $?;
	close $out;
	close $err;

	my $output = slurp($stdout_path);
	if ($rc != 0)
	{
		my $stderr = slurp($stderr_path);
		die "$label failed with exit code "
		  . ($rc >> 8)
		  . ": @$cmd\n$output\n$stderr\n";
	}

	return $output;
}

sub new_server_resource_sample
{
	my ($data_dir) = @_;
	my $pid = $sample_server_resources ?
		read_postmaster_pid($data_dir) : undef;

	return {
		enabled => $sample_server_resources,
		interval_seconds => $resource_sample_interval_ms / 1000,
		postmaster_pid => $pid,
		max_server_processes => undef,
		max_server_threads => undef,
		max_server_rss_kb => undef,
		max_server_pss_kb => undef,
		max_server_private_kb => undef,
		samples => 0,
	};
}

sub read_postmaster_pid
{
	my ($data_dir) = @_;
	my $pidfile = File::Spec->catfile($data_dir, 'postmaster.pid');

	open my $fh, '<', $pidfile or return undef;
	my $line = <$fh>;
	close $fh;
	chomp $line if defined $line;
	return $line =~ /^\d+$/ ? int($line) : undef;
}

sub sample_server_resources
{
	my ($sample) = @_;

	return unless defined $sample;
	return unless defined $sample->{postmaster_pid};
	return unless -d '/proc';

	my @pids = linux_process_tree($sample->{postmaster_pid});
	return unless @pids;

	my $threads = 0;
	my $rss_kb = 0;
	my $pss_kb = 0;
	my $private_kb = 0;
	my $saw_rss = 0;
	my $saw_pss = 0;
	my $saw_private = 0;

	for my $pid (@pids)
	{
		$threads += linux_thread_count($pid);
		my $memory = linux_process_memory_kb($pid);
		next unless defined $memory;

		if (defined $memory->{rss_kb})
		{
			$rss_kb += $memory->{rss_kb};
			$saw_rss = 1;
		}
		if (defined $memory->{pss_kb})
		{
			$pss_kb += $memory->{pss_kb};
			$saw_pss = 1;
		}
		if (defined $memory->{private_kb})
		{
			$private_kb += $memory->{private_kb};
			$saw_private = 1;
		}
	}

	update_resource_max($sample, max_server_processes => scalar @pids);
	update_resource_max($sample, max_server_threads => $threads);
	update_resource_max($sample, max_server_rss_kb => $rss_kb) if $saw_rss;
	update_resource_max($sample, max_server_pss_kb => $pss_kb) if $saw_pss;
	update_resource_max($sample, max_server_private_kb => $private_kb)
	  if $saw_private;
	$sample->{samples}++;
}

sub linux_process_tree
{
	my ($root_pid) = @_;
	my %children;

	return () unless defined $root_pid && -d "/proc/$root_pid";

	opendir my $dh, '/proc' or return ();
	while (defined(my $entry = readdir $dh))
	{
		next unless $entry =~ /^\d+$/;
		my $ppid = linux_ppid($entry);
		next unless defined $ppid;
		push @{ $children{$ppid} }, int($entry);
	}
	closedir $dh;

	my @tree;
	my @queue = (int($root_pid));
	my %seen;
	while (@queue)
	{
		my $pid = shift @queue;
		next if $seen{$pid}++;
		next unless -d "/proc/$pid";
		push @tree, $pid;
		push @queue, @{ $children{$pid} || [] };
	}

	return @tree;
}

sub linux_ppid
{
	my ($pid) = @_;
	my $status = "/proc/$pid/status";

	open my $fh, '<', $status or return undef;
	while (defined(my $line = <$fh>))
	{
		if ($line =~ /^PPid:\s+(\d+)/)
		{
			close $fh;
			return int($1);
		}
	}
	close $fh;
	return undef;
}

sub linux_thread_count
{
	my ($pid) = @_;
	my $task_dir = "/proc/$pid/task";

	opendir my $dh, $task_dir or return -d "/proc/$pid" ? 1 : 0;
	my $count = grep { /^\d+$/ } readdir $dh;
	closedir $dh;
	return $count;
}

sub linux_process_memory_kb
{
	my ($pid) = @_;
	my $rollup = "/proc/$pid/smaps_rollup";

	if (open my $fh, '<', $rollup)
	{
		my %memory;
		while (defined(my $line = <$fh>))
		{
			if ($line =~ /^Rss:\s+(\d+)\s+kB/)
			{
				$memory{rss_kb} = int($1);
			}
			elsif ($line =~ /^Pss:\s+(\d+)\s+kB/)
			{
				$memory{pss_kb} = int($1);
			}
			elsif ($line =~ /^Private_(?:Clean|Dirty|Hugetlb):\s+(\d+)\s+kB/)
			{
				$memory{private_kb} += int($1);
			}
		}
		close $fh;
		return \%memory if %memory;
	}

	my $status = "/proc/$pid/status";
	open my $fh, '<', $status or return undef;
	while (defined(my $line = <$fh>))
	{
		if ($line =~ /^VmRSS:\s+(\d+)\s+kB/)
		{
			close $fh;
			return { rss_kb => int($1) };
		}
	}
	close $fh;
	return undef;
}

sub update_resource_max
{
	my ($sample, $key, $value) = @_;

	return unless defined $value;
	if (!defined $sample->{$key} || $sample->{$key} < $value)
	{
		$sample->{$key} = $value;
	}
}

sub resource_value
{
	my ($sample, $key) = @_;

	return 'n/a' unless defined $sample && defined $sample->{$key};
	return $sample->{$key};
}

sub slurp
{
	my ($path) = @_;

	open my $fh, '<', $path or die "could not read $path: $!";
	local $/;
	my $contents = <$fh>;
	close $fh;
	return $contents;
}

sub pick_free_port
{
	my $socket = IO::Socket::INET->new(
		LocalAddr => '127.0.0.1',
		LocalPort => 0,
		Proto => 'tcp',
		Listen => 1,
		ReuseAddr => 0,
	) or die "could not allocate a free TCP port: $!";

	my $port = $socket->sockport();
	close $socket;
	return $port;
}

sub write_ratios
{
	my ($dir, $workloads, $lane_specs, $results) = @_;
	my $path = File::Spec->catfile($dir, 'ratios.tsv');

	open my $fh, '>', $path or die "could not write $path: $!";
	print $fh join("\t", qw(workload lane tps ratio_vs_vanilla)), "\n";
	for my $workload (@$workloads)
	{
		my $vanilla = $results->{vanilla}{$workload}{tps};
		for my $lane (@$lane_specs)
		{
			my $name = $lane->{name};
			next unless exists $results->{$name}{$workload};
			my $tps = $results->{$name}{$workload}{tps};
			my $ratio =
			  defined $vanilla && $vanilla > 0
			  ? sprintf('%.3f', $tps / $vanilla)
			  : 'n/a';
			print $fh join("\t", $workload, $name, $tps, $ratio), "\n";
		}
	}
	close $fh;
}

sub write_summary
{
	my ($dir, $workloads, $lane_specs, $results) = @_;
	my $path = File::Spec->catfile($dir, 'summary.md');

	open my $fh, '>', $path or die "could not write $path: $!";
	print $fh "# mtpg pgbench matrix\n\n";
	print $fh "- duration: ${duration}s\n";
	print $fh "- warmup: ${warmup}s\n";
	print $fh "- runs: $runs\n";
	print $fh "- clients: $clients\n";
	print $fh "- threads: $threads\n";
	print $fh "- max connections: $max_connections\n";
	if (benchmark_max_files_per_process($max_connections) >
		$default_max_files_per_process)
	{
		print $fh "- max files per process: ",
		  benchmark_max_files_per_process($max_connections), "\n";
	}
	print $fh "- pool sizes: $pool_sizes\n"
	  if grep { $_->{name} =~ /^branch_pool_/ } @$lane_specs;
	print $fh "- scale: $scale\n";
	print $fh "- branch install: `$branch_install`\n";
	print $fh "- vanilla install: `$vanilla_install`\n";
	print $fh "- client install: `$client_install`\n\n";

	print $fh "| Workload |";
	for my $lane (@$lane_specs)
	{
		print $fh " $lane->{name} TPS |";
		print $fh " $lane->{name} / vanilla |" unless $lane->{name} eq 'vanilla';
	}
	print $fh "\n| --- |";
	for my $lane (@$lane_specs)
	{
		print $fh " ---: |";
		print $fh " ---: |" unless $lane->{name} eq 'vanilla';
	}
	print $fh "\n";

	for my $workload (@$workloads)
	{
		my $vanilla = $results->{vanilla}{$workload}{tps};
		print $fh "| `$workload` |";
		for my $lane (@$lane_specs)
		{
			my $name = $lane->{name};
			my $tps = $results->{$name}{$workload}{tps};
			print $fh " ", sprintf('%.1f', $tps), " |";
			if ($name ne 'vanilla')
			{
				my $ratio =
				  defined $vanilla && $vanilla > 0
				  ? sprintf('%.3f', $tps / $vanilla)
				  : 'n/a';
				print $fh " $ratio |";
			}
		}
		print $fh "\n";
	}

	print $fh "\n## Server resource samples\n\n";
	print $fh "| Workload | Lane | Max server processes | Max server threads | Max RSS kB | Max PSS kB | Max private kB |\n";
	print $fh "| --- | --- | ---: | ---: | ---: | ---: | ---: |\n";
	for my $workload (@$workloads)
	{
		for my $lane (@$lane_specs)
		{
			my $name = $lane->{name};
			next unless exists $results->{$name}{$workload};
			my $resources = $results->{$name}{$workload}{resources};
			print $fh "| `$workload` | `$name` | ",
			  resource_value($resources, 'max_server_processes'), " | ",
			  resource_value($resources, 'max_server_threads'), " | ",
			  resource_value($resources, 'max_server_rss_kb'), " | ",
			  resource_value($resources, 'max_server_pss_kb'), " | ",
			  resource_value($resources, 'max_server_private_kb'), " |\n";
		}
	}
	close $fh;
}

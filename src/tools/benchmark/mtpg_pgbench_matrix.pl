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
use Time::HiRes qw(time);

my $repo_root = abs_path(File::Spec->catdir($FindBin::Bin, '..', '..', '..'));

# Install trees.  Defaults derive from this repo (branch = ./tmp_install); the
# vanilla/baseline tree has no portable default, so it defaults to the branch
# tree and MUST be overridden with --vanilla-install when a distinct baseline is
# wanted (e.g. a Phase 18 A/B of baseline-build vs candidate-build).
my $branch_install = File::Spec->catdir($repo_root, 'tmp_install');
my $vanilla_install = $ENV{MTPG_VANILLA_INSTALL} || $branch_install;
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
my $sample_memory_detail = 0;
my $resource_sample_interval_ms = 100;
my $resource_baseline_samples = 3;
my $log_protocol_park_memory = 0;
my $help = 0;
my $socket_seq = 0;
my $default_max_files_per_process = 1000;
my @branch_extra_config;

my @protocol_park_memory_fields = qw(
  pid backend_id generation
  top_total_bytes top_free_bytes top_used_bytes top_blocks
  message_total_bytes message_free_bytes message_used_bytes message_blocks
  cache_total_bytes cache_free_bytes cache_used_bytes cache_blocks
  top_xact_total_bytes top_xact_free_bytes top_xact_used_bytes top_xact_blocks
  cur_xact_total_bytes cur_xact_free_bytes cur_xact_used_bytes cur_xact_blocks
  portal_total_bytes portal_free_bytes portal_used_bytes portal_blocks
  error_total_bytes error_free_bytes error_used_bytes error_blocks
  current_total_bytes current_free_bytes current_used_bytes current_blocks
  row_description_total_bytes row_description_free_bytes row_description_used_bytes row_description_blocks
  client_info_total_bytes client_info_free_bytes client_info_used_bytes client_info_blocks
  legacy_session_total_bytes legacy_session_free_bytes legacy_session_used_bytes legacy_session_blocks
  dynamic_library_total_bytes dynamic_library_free_bytes dynamic_library_used_bytes dynamic_library_blocks
  sizeof_backend sizeof_session sizeof_connection sizeof_execution
  sizeof_logical_state sizeof_runtime_state
);

my @protocol_park_memory_summary_fields = qw(
  top_used_bytes message_used_bytes cache_used_bytes top_xact_used_bytes
  cur_xact_used_bytes portal_used_bytes error_used_bytes current_used_bytes
  row_description_used_bytes client_info_used_bytes legacy_session_used_bytes
  dynamic_library_used_bytes sizeof_backend sizeof_session sizeof_connection
  sizeof_execution sizeof_logical_state sizeof_runtime_state
);

my @protocol_park_guc_memory_fields = qw(
  pid backend_id generation
  context_total_bytes context_free_bytes context_used_bytes context_blocks
  builtin_count custom_count state_array_bytes
  cold_count cold_direct_bytes
  current_string_bytes reset_string_bytes
  last_reported_bytes sourcefile_bytes
  stack_count stack_direct_bytes custom_record_bytes
  attributed_bytes unattributed_used_bytes
);

my @protocol_park_guc_memory_summary_fields = qw(
  context_used_bytes context_blocks builtin_count custom_count state_array_bytes
  cold_count cold_direct_bytes current_string_bytes reset_string_bytes
  last_reported_bytes sourcefile_bytes stack_count stack_direct_bytes
  custom_record_bytes attributed_bytes unattributed_used_bytes
);

my @protocol_park_context_memory_fields = qw(
  pid backend_id generation context_index depth type name ident path
  local_total_bytes local_free_bytes local_used_bytes local_blocks local_free_chunks
  recursive_total_bytes recursive_free_bytes recursive_used_bytes recursive_blocks recursive_free_chunks
);

my @protocol_park_context_memory_summary_fields = qw(
  local_total_bytes local_free_bytes local_used_bytes local_blocks local_free_chunks
  recursive_total_bytes recursive_free_bytes recursive_used_bytes recursive_blocks recursive_free_chunks
);

my @protocol_park_catcache_memory_fields = qw(
  pid backend_id generation cache_id reloid indexoid relname
  ntup npositive nnegative nlist nbuckets nlbuckets
  cache_header_bytes bucket_bytes tuple_header_bytes tuple_data_bytes
  negative_key_bytes list_header_bytes list_key_bytes total_requested_bytes
);

my @protocol_park_catcache_memory_summary_fields = qw(
  ntup npositive nnegative nlist nbuckets nlbuckets
  cache_header_bytes bucket_bytes tuple_header_bytes tuple_data_bytes
  negative_key_bytes list_header_bytes list_key_bytes total_requested_bytes
);

my @protocol_park_relcache_memory_fields = qw(
  pid backend_id generation reloid relname isvalid isnailed islocaltemp refcnt
  has_index_context has_rules_context has_partition_context
  relation_data_bytes class_tuple_bytes tuple_desc_bytes tuple_constr_bytes
  index_tuple_bytes options_bytes pubdesc_bytes direct_payload_bytes
  private_context_total_bytes private_context_free_bytes private_context_used_bytes
);

my @protocol_park_relcache_memory_summary_fields = qw(
  isvalid isnailed islocaltemp refcnt has_index_context has_rules_context
  has_partition_context relation_data_bytes class_tuple_bytes tuple_desc_bytes
  tuple_constr_bytes index_tuple_bytes options_bytes pubdesc_bytes
  direct_payload_bytes private_context_total_bytes private_context_free_bytes
  private_context_used_bytes
);

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
	'branch-config=s@' => \@branch_extra_config,
	'sample-server-resources!' => \$sample_server_resources,
	'sample-memory-detail!' => \$sample_memory_detail,
	'resource-sample-interval-ms=i' => \$resource_sample_interval_ms,
	'resource-baseline-samples=i' => \$resource_baseline_samples,
	'log-protocol-park-memory!' => \$log_protocol_park_memory,
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
die "--resource-baseline-samples must be non-negative\n"
  if $resource_baseline_samples < 0;

$sample_server_resources = 1 if $sample_memory_detail;

raise_nofile_limit_for_benchmark(benchmark_max_files_per_process($max_connections));

my @pool_sizes = grep { length($_) } split /,/, $pool_sizes;
for my $size (@pool_sizes)
{
	die "invalid pool size: $size\n" unless $size =~ /^\d+$/ && $size > 0;
}

my @requested_workloads = grep { length($_) } split /,/, $workloads;
my @requested_lanes = grep { length($_) } split /,/, $lanes;
my @branch_diagnostic_config;

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
	select1_sleep_wake_1ms_prepared => {
		args => [ '-M', 'prepared', '-f', undef ],
		script => 'select1_sleep_wake_1ms.sql',
		needs_extra_setup => 0,
	},
	select1_sleep_10ms_prepared => {
		args => [ '-M', 'prepared', '-f', undef ],
		script => 'select1_sleep_10ms.sql',
		needs_extra_setup => 0,
	},
	select1_sleep_wake_10ms_prepared => {
		args => [ '-M', 'prepared', '-f', undef ],
		script => 'select1_sleep_wake_10ms.sql',
		needs_extra_setup => 0,
	},
	select1_sleep_100ms_prepared => {
		args => [ '-M', 'prepared', '-f', undef ],
		script => 'select1_sleep_100ms.sql',
		needs_extra_setup => 0,
	},
	select1_sleep_wake_100ms_prepared => {
		args => [ '-M', 'prepared', '-f', undef ],
		script => 'select1_sleep_wake_100ms.sql',
		needs_extra_setup => 0,
	},
	select1_sleep_1000ms_prepared => {
		args => [ '-M', 'prepared', '-f', undef ],
		script => 'select1_sleep_1000ms.sql',
		needs_extra_setup => 0,
	},
	select1_sleep_wake_1000ms_prepared => {
		args => [ '-M', 'prepared', '-f', undef ],
		script => 'select1_sleep_wake_1000ms.sql',
		needs_extra_setup => 0,
	},
	select1_connect_prepared => {
		args => [ '-C', '-M', 'prepared', '-f', undef ],
		script => 'select1.sql',
		needs_extra_setup => 0,
	},
	kv_read_sleep_wake_100ms_prepared => {
		args => [ '-M', 'prepared', '-f', undef ],
		script => 'kv_read_sleep_wake_100ms.sql',
		needs_extra_setup => 1,
	},
	kv_read_sleep_wake_1000ms_prepared => {
		args => [ '-M', 'prepared', '-f', undef ],
		script => 'kv_read_sleep_wake_1000ms.sql',
		needs_extra_setup => 1,
	},
	app_txn_sleep_wake_100ms_prepared => {
		args => [ '-M', 'prepared', '-f', undef ],
		script => 'app_txn_sleep_wake_100ms.sql',
		needs_extra_setup => 1,
	},
	app_txn_sleep_wake_1000ms_prepared => {
		args => [ '-M', 'prepared', '-f', undef ],
		script => 'app_txn_sleep_wake_1000ms.sql',
		needs_extra_setup => 1,
	},
	app_mixed_sleep_wake_100ms_prepared => {
		args => [ '-M', 'prepared', '-f', undef ],
		script => 'app_mixed_sleep_wake_100ms.sql',
		needs_extra_setup => 1,
	},
	stateful_temp_sleep_wake_1000ms_prepared => {
		args => [ '-M', 'prepared', '-f', undef ],
		script => 'stateful_temp_sleep_wake_1000ms.sql',
		needs_extra_setup => 1,
	},
	app_txn_connect_prepared => {
		args => [ '-C', '-M', 'prepared', '-f', undef ],
		script => 'app_txn.sql',
		needs_extra_setup => 1,
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
			config => [ @branch_extra_config, @branch_diagnostic_config ],
			branch => 1,
		};
	}
	elsif ($lane eq 'branch_threaded')
	{
		push @lane_specs, {
			name => 'branch_threaded',
			install => $branch_install,
			config => [
				'multithreaded = on',
				'pooled_protocol_carriers = 0',
				@branch_extra_config,
				@branch_diagnostic_config,
			],
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
					@branch_extra_config,
					@branch_diagnostic_config,
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
	  max_server_rss_kb max_server_vm_rss_kb max_server_pss_kb
	  max_server_shared_kb max_server_private_kb
	  max_smaps_rollup_readable max_smaps_rollup_unreadable samples)),
  "\n";

my $resource_samples_path =
  File::Spec->catfile($out_dir, 'server_resource_samples.tsv');
open my $resource_samples_fh, '>', $resource_samples_path
  or die "could not write $resource_samples_path: $!";
print $resource_samples_fh
  join("\t", qw(lane workload run sample_index server_processes server_threads
	  server_rss_kb server_vm_rss_kb server_pss_kb server_shared_kb
	  server_private_kb smaps_rollup_readable smaps_rollup_unreadable)),
  "\n";

my $resource_baselines_path =
  File::Spec->catfile($out_dir, 'server_resource_baselines.tsv');
open my $resource_baselines_fh, '>', $resource_baselines_path
  or die "could not write $resource_baselines_path: $!";
print $resource_baselines_fh
  join("\t", qw(lane workload_scope max_server_processes max_server_threads
	  max_server_rss_kb max_server_vm_rss_kb max_server_pss_kb
	  max_server_shared_kb max_server_private_kb
	  max_smaps_rollup_readable max_smaps_rollup_unreadable samples)),
  "\n";

my $process_rollups_path =
  File::Spec->catfile($out_dir, 'server_process_rollups.tsv');
open my $process_rollups_fh, '>', $process_rollups_path
  or die "could not write $process_rollups_path: $!";
print $process_rollups_fh
  join("\t", qw(lane workload run sample_index process_index pid ppid
	  comm threads rss_kb vm_rss_kb pss_kb shared_kb private_kb
	  smaps_rollup_readable smaps_rollup_unreadable)),
  "\n";

my $memory_map_summary_path =
  File::Spec->catfile($out_dir, 'server_memory_map_summary.tsv');
open my $memory_map_summary_fh, '>', $memory_map_summary_path
  or die "could not write $memory_map_summary_path: $!";
print $memory_map_summary_fh
  join("\t", qw(lane workload run snapshot_index sample_index category
	  mappings size_kb rss_kb pss_kb shared_kb private_kb)),
  "\n";

my $memory_map_path_summary_path =
  File::Spec->catfile($out_dir, 'server_memory_map_path_summary.tsv');
open my $memory_map_path_summary_fh, '>', $memory_map_path_summary_path
  or die "could not write $memory_map_path_summary_path: $!";
print $memory_map_path_summary_fh
  join("\t", qw(lane workload run snapshot_index sample_index category
	  path mappings size_kb rss_kb pss_kb shared_kb private_kb)),
  "\n";

my $thread_stacks_path =
  File::Spec->catfile($out_dir, 'server_thread_stacks.tsv');
open my $thread_stacks_fh, '>', $thread_stacks_path
  or die "could not write $thread_stacks_path: $!";
print $thread_stacks_fh
  join("\t", qw(lane workload run snapshot_index sample_index pid tid name
	  state vmstk_kb stack_map_found stack_map_kb)),
  "\n";

my $memory_accounting_path =
  File::Spec->catfile($out_dir, 'server_memory_accounting.tsv');
open my $memory_accounting_fh, '>', $memory_accounting_path
  or die "could not write $memory_accounting_path: $!";
print $memory_accounting_fh
  join("\t", qw(lane workload run snapshot_index sample_index processes
	  threads rollup_rss_kb rollup_pss_kb rollup_shared_kb
	  rollup_private_kb map_rss_kb map_pss_kb map_shared_kb
	  map_private_kb rss_diff_kb pss_diff_kb shared_diff_kb
	  private_diff_kb smaps_pids_readable smaps_pids_unreadable)),
  "\n";

my $protocol_park_memory_path =
  File::Spec->catfile($out_dir, 'protocol_park_memory.tsv');
open my $protocol_park_memory_fh, '>', $protocol_park_memory_path
  or die "could not write $protocol_park_memory_path: $!";
print $protocol_park_memory_fh
  join("\t", 'lane', 'workload', 'run', 'sample_index',
	@protocol_park_memory_fields),
  "\n";

my $protocol_park_guc_memory_path =
  File::Spec->catfile($out_dir, 'protocol_park_guc_memory.tsv');
open my $protocol_park_guc_memory_fh, '>', $protocol_park_guc_memory_path
  or die "could not write $protocol_park_guc_memory_path: $!";
print $protocol_park_guc_memory_fh
  join("\t", 'lane', 'workload', 'run', 'sample_index',
	@protocol_park_guc_memory_fields),
  "\n";

my $protocol_park_context_memory_path =
  File::Spec->catfile($out_dir, 'protocol_park_context_memory.tsv');
open my $protocol_park_context_memory_fh, '>', $protocol_park_context_memory_path
  or die "could not write $protocol_park_context_memory_path: $!";
print $protocol_park_context_memory_fh
  join("\t", 'lane', 'workload', 'run', 'sample_index',
	@protocol_park_context_memory_fields),
  "\n";

my $protocol_park_catcache_memory_path =
  File::Spec->catfile($out_dir, 'protocol_park_catcache_memory.tsv');
open my $protocol_park_catcache_memory_fh, '>',
  $protocol_park_catcache_memory_path
  or die "could not write $protocol_park_catcache_memory_path: $!";
print $protocol_park_catcache_memory_fh
  join("\t", 'lane', 'workload', 'run', 'sample_index',
	@protocol_park_catcache_memory_fields),
  "\n";

my $protocol_park_relcache_memory_path =
  File::Spec->catfile($out_dir, 'protocol_park_relcache_memory.tsv');
open my $protocol_park_relcache_memory_fh, '>',
  $protocol_park_relcache_memory_path
  or die "could not write $protocol_park_relcache_memory_path: $!";
print $protocol_park_relcache_memory_fh
  join("\t", 'lane', 'workload', 'run', 'sample_index',
	@protocol_park_relcache_memory_fields),
  "\n";

my %results;
if ($restart_per_workload)
{
	for my $lane (@lane_specs)
	{
		for my $workload (@requested_workloads)
		{
			run_lane($lane, [ $workload ], $script_dir, $tps_fh,
				$samples_fh, $resources_fh, $resource_samples_fh,
				$resource_baselines_fh, $protocol_park_memory_fh,
				$protocol_park_guc_memory_fh,
				$protocol_park_context_memory_fh,
				$protocol_park_catcache_memory_fh,
				$protocol_park_relcache_memory_fh, \%results, $workload);
		}
	}
}
else
{
	for my $lane (@lane_specs)
	{
		run_lane($lane, \@requested_workloads, $script_dir, $tps_fh,
			$samples_fh, $resources_fh, $resource_samples_fh,
			$resource_baselines_fh, $protocol_park_memory_fh,
			$protocol_park_guc_memory_fh,
			$protocol_park_context_memory_fh,
			$protocol_park_catcache_memory_fh,
			$protocol_park_relcache_memory_fh, \%results, undef);
	}
}

close $tps_fh;
close $samples_fh;
close $resources_fh;
close $resource_samples_fh;
close $resource_baselines_fh;
close $process_rollups_fh;
close $memory_map_summary_fh;
close $memory_map_path_summary_fh;
close $thread_stacks_fh;
close $memory_accounting_fh;
close $protocol_park_memory_fh;
close $protocol_park_guc_memory_fh;
close $protocol_park_context_memory_fh;
close $protocol_park_catcache_memory_fh;
close $protocol_park_relcache_memory_fh;

write_ratios($out_dir, \@requested_workloads, \@lane_specs, \%results);
write_resource_efficiency($out_dir, \@requested_workloads, \@lane_specs,
	\%results);
write_memory_footprint($out_dir, \@requested_workloads, \@lane_specs,
	\%results);
write_protocol_park_memory_summary($out_dir, $protocol_park_memory_path);
write_protocol_park_guc_memory_summary($out_dir,
	$protocol_park_guc_memory_path);
write_protocol_park_context_memory_summary($out_dir,
	$protocol_park_context_memory_path);
write_protocol_park_catcache_memory_summary($out_dir,
	$protocol_park_catcache_memory_path);
write_protocol_park_relcache_memory_summary($out_dir,
	$protocol_park_relcache_memory_path);
write_memory_detail_summaries($out_dir);
write_summary($out_dir, \@requested_workloads, \@lane_specs, \%results);

print "wrote $tps_path\n";
print "wrote $samples_path\n";
print "wrote $resources_path\n";
print "wrote $resource_samples_path\n";
print "wrote $resource_baselines_path\n";
print "wrote $process_rollups_path\n";
print "wrote $memory_map_summary_path\n";
print "wrote $memory_map_path_summary_path\n";
print "wrote $thread_stacks_path\n";
print "wrote $memory_accounting_path\n";
print "wrote $protocol_park_memory_path\n";
print "wrote $protocol_park_guc_memory_path\n";
print "wrote $protocol_park_context_memory_path\n";
print "wrote $protocol_park_catcache_memory_path\n";
print "wrote $protocol_park_relcache_memory_path\n";
print "wrote ", File::Spec->catfile($out_dir, 'ratios.tsv'), "\n";
print "wrote ", File::Spec->catfile($out_dir, 'resource_efficiency.tsv'), "\n";
print "wrote ", File::Spec->catfile($out_dir, 'memory_footprint.tsv'), "\n";
print "wrote ", File::Spec->catfile($out_dir, 'protocol_park_memory_summary.tsv'), "\n";
print "wrote ", File::Spec->catfile($out_dir, 'protocol_park_guc_memory_summary.tsv'), "\n";
print "wrote ", File::Spec->catfile($out_dir, 'protocol_park_context_memory_summary.tsv'), "\n";
print "wrote ", File::Spec->catfile($out_dir, 'protocol_park_catcache_memory_summary.tsv'), "\n";
print "wrote ", File::Spec->catfile($out_dir, 'protocol_park_relcache_memory_summary.tsv'), "\n";
print "wrote ", File::Spec->catfile($out_dir, 'server_process_rollup_summary.tsv'), "\n";
print "wrote ", File::Spec->catfile($out_dir, 'server_memory_map_category_summary.tsv'), "\n";
print "wrote ", File::Spec->catfile($out_dir, 'server_memory_map_path_top.tsv'), "\n";
print "wrote ", File::Spec->catfile($out_dir, 'server_thread_stack_summary.tsv'), "\n";
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
  --branch-config=LINE    append a postgresql.conf line to branch lanes;
                           may be specified more than once
  --sample-server-resources
                           sample server process/thread counts while measuring
  --sample-memory-detail
                           write per-process rollups, one detailed smaps
                           category snapshot per run, and per-thread stack
                           visibility; implies --sample-server-resources
  --resource-sample-interval-ms=N
                           server resource sample interval, default 100
  --resource-baseline-samples=N
                           idle server samples before each measured workload,
                           default 3
  --log-protocol-park-memory
                           enable branch server log attribution at committed
                           protocol-read parks and write protocol_park_memory.tsv

Additional non-default workloads useful for pooled connection-shape profiles:
  select1_sleep_1ms_prepared
  select1_sleep_10ms_prepared
  select1_sleep_100ms_prepared
  select1_sleep_1000ms_prepared
  select1_sleep_wake_1ms_prepared
  select1_sleep_wake_10ms_prepared
  select1_sleep_wake_100ms_prepared
  select1_sleep_wake_1000ms_prepared
  select1_connect_prepared
  kv_read_sleep_wake_100ms_prepared
  kv_read_sleep_wake_1000ms_prepared
  app_txn_sleep_wake_100ms_prepared
  app_txn_sleep_wake_1000ms_prepared
  app_mixed_sleep_wake_100ms_prepared
  stateful_temp_sleep_wake_1000ms_prepared
  app_txn_connect_prepared

Output:
  tps.tsv                 summary TPS and latency per lane/workload
  samples.tsv             per-run TPS and latency samples
  server_resources.tsv    max server process/thread counts sampled per workload
  server_resource_samples.tsv
                           raw per-sample process-tree memory observations
  server_resource_baselines.tsv
                           idle server resource samples before workload clients
  server_process_rollups.tsv
                           per-process smaps_rollup rows for sampled server
                           process trees when --sample-memory-detail is used
  server_memory_map_summary.tsv
                           one detailed smaps category snapshot per run when
                           --sample-memory-detail is used
  server_memory_map_path_summary.tsv
                           detailed smaps totals by category and mapped path
  server_memory_accounting.tsv
                           detailed smaps category totals checked against
                           process smaps_rollup totals
  server_thread_stacks.tsv
                           per-thread stack visibility for detailed snapshots
  protocol_park_memory.tsv
                           parsed per-park memory-context attribution rows
  protocol_park_guc_memory.tsv
                           parsed per-park GUC memory attribution rows
  protocol_park_context_memory.tsv
                           bounded per-backend memory-context tree rows
                           emitted at committed protocol-read parks
  protocol_park_memory_summary.tsv
                           median per-park memory attribution by lane/workload
  protocol_park_guc_memory_summary.tsv
                           median per-park GUC memory attribution by lane/workload
  protocol_park_context_memory_summary.tsv
                           median per-context retained/used memory by path
  ratios.tsv              per-lane ratios against vanilla, or the first selected lane
  resource_efficiency.tsv derived TPS/thread and memory/client metrics
  memory_footprint.tsv    baseline-adjusted memory footprint estimates
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

sub ratio_baseline_lane
{
	my ($lane_specs) = @_;

	return 'vanilla' if lane_selected('vanilla', $lane_specs);
	return $lane_specs->[0]{name};
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
	write_file(File::Spec->catfile($dir, 'select1_sleep_wake_1ms.sql'),
		"SELECT 1;\n"
	  . "\\sleep 1 ms\n"
	  . "SELECT 1;\n");
	write_file(File::Spec->catfile($dir, 'select1_sleep_10ms.sql'),
		"SELECT 1;\n"
	  . "\\sleep 10 ms\n");
	write_file(File::Spec->catfile($dir, 'select1_sleep_wake_10ms.sql'),
		"SELECT 1;\n"
	  . "\\sleep 10 ms\n"
	  . "SELECT 1;\n");
	write_file(File::Spec->catfile($dir, 'select1_sleep_100ms.sql'),
		"SELECT 1;\n"
	  . "\\sleep 100 ms\n");
	write_file(File::Spec->catfile($dir, 'select1_sleep_wake_100ms.sql'),
		"SELECT 1;\n"
	  . "\\sleep 100 ms\n"
	  . "SELECT 1;\n");
	write_file(File::Spec->catfile($dir, 'select1_sleep_1000ms.sql'),
		"SELECT 1;\n"
	  . "\\sleep 1000 ms\n");
	write_file(File::Spec->catfile($dir, 'select1_sleep_wake_1000ms.sql'),
		"SELECT 1;\n"
	  . "\\sleep 1000 ms\n"
	  . "SELECT 1;\n");
	write_file(File::Spec->catfile($dir, 'bench_one.sql'),
		"SELECT payload FROM bench_one WHERE id = 1;\n");
	write_file(File::Spec->catfile($dir, 'kv_read.sql'),
		"\\set id random(1, 100000)\n"
	  . "SELECT v, payload FROM bench_kv WHERE id = :id;\n");
	write_file(File::Spec->catfile($dir, 'kv_read_sleep_wake_100ms.sql'),
		"\\set id random_zipfian(1, 100000, 1.07)\n"
	  . "SELECT v, payload FROM bench_kv WHERE id = :id;\n"
	  . "\\sleep 100 ms\n"
	  . "SELECT v, payload FROM bench_kv WHERE id = :id;\n");
	write_file(File::Spec->catfile($dir, 'kv_read_sleep_wake_1000ms.sql'),
		"\\set id random_zipfian(1, 100000, 1.07)\n"
	  . "SELECT v, payload FROM bench_kv WHERE id = :id;\n"
	  . "\\sleep 1000 ms\n"
	  . "SELECT v, payload FROM bench_kv WHERE id = :id;\n");
	write_file(File::Spec->catfile($dir, 'app_txn.sql'),
		"\\set aid random(1, 100000 * :scale)\n"
	  . "\\set delta random(-10, 10)\n"
	  . "BEGIN;\n"
	  . "SELECT abalance FROM pgbench_accounts WHERE aid = :aid;\n"
	  . "UPDATE pgbench_accounts SET abalance = abalance + :delta WHERE aid = :aid;\n"
	  . "UPDATE bench_client_state SET v = v + :delta, last_aid = :aid WHERE client_id = :client_id;\n"
	  . "COMMIT;\n");
	write_file(File::Spec->catfile($dir, 'app_txn_sleep_wake_100ms.sql'),
		"\\set aid random(1, 100000 * :scale)\n"
	  . "\\set delta random(-10, 10)\n"
	  . "SELECT abalance FROM pgbench_accounts WHERE aid = :aid;\n"
	  . "\\sleep 100 ms\n"
	  . "BEGIN;\n"
	  . "UPDATE pgbench_accounts SET abalance = abalance + :delta WHERE aid = :aid;\n"
	  . "UPDATE bench_client_state SET v = v + :delta, last_aid = :aid WHERE client_id = :client_id;\n"
	  . "COMMIT;\n"
	  . "SELECT abalance FROM pgbench_accounts WHERE aid = :aid;\n");
	write_file(File::Spec->catfile($dir, 'app_txn_sleep_wake_1000ms.sql'),
		"\\set aid random(1, 100000 * :scale)\n"
	  . "\\set delta random(-10, 10)\n"
	  . "SELECT abalance FROM pgbench_accounts WHERE aid = :aid;\n"
	  . "\\sleep 1000 ms\n"
	  . "BEGIN;\n"
	  . "UPDATE pgbench_accounts SET abalance = abalance + :delta WHERE aid = :aid;\n"
	  . "UPDATE bench_client_state SET v = v + :delta, last_aid = :aid WHERE client_id = :client_id;\n"
	  . "COMMIT;\n"
	  . "SELECT abalance FROM pgbench_accounts WHERE aid = :aid;\n");
	write_file(File::Spec->catfile($dir, 'app_mixed_sleep_wake_100ms.sql'),
		"\\set id random_zipfian(1, 100000, 1.10)\n"
	  . "\\set delta random(1, 3)\n"
	  . "SELECT v, payload FROM bench_kv WHERE id = :id;\n"
	  . "\\sleep 50 ms\n"
	  . "BEGIN;\n"
	  . "UPDATE bench_client_state SET v = v + :delta, last_aid = :id WHERE client_id = :client_id;\n"
	  . "COMMIT;\n"
	  . "\\sleep 50 ms\n"
	  . "SELECT count(*), sum(v) FROM bench_kv WHERE id BETWEEN greatest(1, :id - 10) AND least(100000, :id + 10);\n");
	write_file(File::Spec->catfile($dir, 'stateful_temp_sleep_wake_1000ms.sql'),
		"SELECT set_config('application_name', 'mtpg_stateful_realish', false);\n"
	  . "CREATE TEMP TABLE IF NOT EXISTS session_cache(k int primary key, v text not null, seen int not null default 0) ON COMMIT PRESERVE ROWS;\n"
	  . "INSERT INTO session_cache(k, v, seen) VALUES (:client_id, md5((:client_id)::text), 0) ON CONFLICT (k) DO NOTHING;\n"
	  . "UPDATE session_cache SET seen = seen + 1 WHERE k = :client_id;\n"
	  . "\\sleep 1000 ms\n"
	  . "SELECT v, seen FROM session_cache WHERE k = :client_id;\n");
	write_file(File::Spec->catfile($dir, 'setup_extra.sql'),
		"DROP TABLE IF EXISTS bench_one;\n"
	  . "CREATE TABLE bench_one(id int primary key, payload text not null);\n"
	  . "INSERT INTO bench_one VALUES (1, repeat('x', 128));\n"
	  . "DROP TABLE IF EXISTS bench_kv;\n"
	  . "CREATE TABLE bench_kv(id int primary key, v int not null, payload text not null);\n"
	  . "INSERT INTO bench_kv SELECT g, 0, repeat(md5(g::text), 4) FROM generate_series(1, 100000) g;\n"
	  . "DROP TABLE IF EXISTS bench_client_state;\n"
	  . "CREATE TABLE bench_client_state(client_id int primary key, v bigint not null, last_aid int not null, payload text not null) WITH (fillfactor = 50);\n"
	  . "INSERT INTO bench_client_state SELECT g, 0, 0, repeat(md5(g::text), 2) FROM generate_series(0, 20000) g;\n"
	  . "VACUUM ANALYZE bench_one;\n"
	  . "VACUUM ANALYZE bench_kv;\n"
	  . "VACUUM ANALYZE bench_client_state;\n"
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
		$resource_samples_fh, $resource_baselines_fh,
		$protocol_park_memory_fh, $protocol_park_guc_memory_fh,
		$protocol_park_context_memory_fh,
		$protocol_park_catcache_memory_fh,
		$protocol_park_relcache_memory_fh, $results,
		$lane_dir_suffix) = @_;

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

		if ($log_protocol_park_memory && $lane->{branch})
		{
			run_cmd([
					$pg_ctl_bin, '-D', $data_dir, '-m', 'fast',
					'-w', 'stop'
				],
				"$lane->{name} stop before protocol park logging");
			$started = 0;
			append_postmaster_config($data_dir,
				'log_protocol_park_memory = on');
			run_cmd([
					$pg_ctl_bin, '-D', $data_dir, '-l', $server_log,
					'-o', "-k $socket_dir",
					'-w', 'start'
				],
				"$lane->{name} restart with protocol park logging");
			$started = 1;
		}

		my $baseline_resources = sample_server_resource_baseline($data_dir);
		print $resource_baselines_fh join("\t", $lane->{name},
			defined $lane_dir_suffix ? $lane_dir_suffix : 'all',
			resource_value($baseline_resources, 'max_server_processes'),
			resource_value($baseline_resources, 'max_server_threads'),
			resource_value($baseline_resources, 'max_server_rss_kb'),
			resource_value($baseline_resources, 'max_server_vm_rss_kb'),
			resource_value($baseline_resources, 'max_server_pss_kb'),
			resource_value($baseline_resources, 'max_server_shared_kb'),
			resource_value($baseline_resources, 'max_server_private_kb'),
			resource_value($baseline_resources, 'max_smaps_rollup_readable'),
			resource_value($baseline_resources, 'max_smaps_rollup_unreadable'),
			resource_value($baseline_resources, 'samples')), "\n";

		for my $workload (@$workloads)
		{
			my @samples;
			my $resources = new_resource_summary();

			for my $run_index (1 .. $runs)
			{
				my ($sample_tps, $sample_latency, $sample_failed,
					$sample_resources) =
				  run_workload($lane, $workload, $script_dir, $socket_dir,
					$port, $pgbench_bin, $data_dir, $server_log, $run_index,
					$resource_samples_fh, $protocol_park_memory_fh,
					$protocol_park_guc_memory_fh,
					$protocol_park_context_memory_fh,
					$protocol_park_catcache_memory_fh,
					$protocol_park_relcache_memory_fh);

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
				baseline_resources => $baseline_resources,
			};
			print $tps_fh join("\t", $lane->{name}, $workload,
				$summary->{tps}, $summary->{latency}, $summary->{failed}),
			  "\n";
			print $resources_fh join("\t", $lane->{name}, $workload,
				resource_value($resources, 'max_server_processes'),
				resource_value($resources, 'max_server_threads'),
				resource_value($resources, 'max_server_rss_kb'),
				resource_value($resources, 'max_server_vm_rss_kb'),
				resource_value($resources, 'max_server_pss_kb'),
				resource_value($resources, 'max_server_shared_kb'),
				resource_value($resources, 'max_server_private_kb'),
				resource_value($resources, 'max_smaps_rollup_readable'),
				resource_value($resources, 'max_smaps_rollup_unreadable'),
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

sub append_postmaster_config
{
	my ($data_dir, $line) = @_;
	my $conf = File::Spec->catfile($data_dir, 'postgresql.conf');

	open my $fh, '>>', $conf or die "could not append $conf: $!";
	print $fh "$line\n";
	close $fh;
}

sub benchmark_max_files_per_process
{
	my ($connections) = @_;
	my $fd_budget = $connections * 16;

	return $fd_budget > $default_max_files_per_process ?
	  $fd_budget : $default_max_files_per_process;
}

sub raise_nofile_limit_for_benchmark
{
	my ($needed) = @_;

	return if $^O ne 'linux';
	return if $needed <= 0;

	my $ok = eval {
		require 'sys/syscall.ph';
		1;
	};
	if (!$ok || !defined &SYS_prlimit64)
	{
		warn "could not inspect RLIMIT_NOFILE for benchmark: $@\n";
		return;
	}

	my $old_limit = pack('QQ', 0, 0);
	if (syscall(&SYS_prlimit64, 0, 7, 0, $old_limit) != 0)
	{
		warn "could not inspect RLIMIT_NOFILE for benchmark: $!\n";
		return;
	}

	my ($soft, $hard) = unpack('QQ', $old_limit);
	return if $soft >= $needed;

	if ($hard < $needed)
	{
		warn "benchmark needs RLIMIT_NOFILE >= $needed, but hard limit is $hard\n";
		return;
	}

	my $new_limit = pack('QQ', $needed, $hard);
	if (syscall(&SYS_prlimit64, 0, 7, $new_limit, 0) != 0)
	{
		warn "could not raise RLIMIT_NOFILE to $needed for benchmark: $!\n";
	}
}

sub run_workload
{
	my ($lane, $workload, $script_dir, $socket_dir, $port, $pgbench_bin,
		$data_dir, $server_log, $run_index, $resource_samples_fh,
		$protocol_park_memory_fh, $protocol_park_guc_memory_fh,
		$protocol_park_context_memory_fh,
		$protocol_park_catcache_memory_fh,
		$protocol_park_relcache_memory_fh) = @_;

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
			"$warm.out", "$warm.err", undef, pgbench_timeout($warmup));
	}

	my $bench = File::Spec->catfile($out_dir, "$lane->{name}_${workload}.bench");
	my $resources =
	  new_server_resource_sample($data_dir, $lane->{name}, $workload,
		$run_index, $resource_samples_fh);
	my $protocol_park_log_offset = -e $server_log ? (-s $server_log) : 0;
	my $output = run_capture([ @base_cmd, '-T', $duration, 'postgres' ],
		"$lane->{name} $workload", $bench, "$bench.err", $resources,
		pgbench_timeout($duration));

	parse_protocol_park_memory_log($server_log, $protocol_park_log_offset,
		$lane->{name}, $workload, $run_index, $protocol_park_memory_fh,
		$protocol_park_guc_memory_fh,
		$protocol_park_context_memory_fh,
		$protocol_park_catcache_memory_fh,
		$protocol_park_relcache_memory_fh)
	  if $log_protocol_park_memory;

	my ($tps) = $output =~ /^tps = ([0-9.]+) /m;
	my ($latency) = $output =~ /^latency average = ([0-9.]+) ms/m;
	my ($failed) = $output =~ /^number of failed transactions: ([0-9]+)/m;

	die "could not parse TPS for $lane->{name} $workload\n$output\n"
	  unless defined $tps && defined $latency && defined $failed;

	return ($tps, $latency, $failed, $resources);
}

sub pgbench_timeout
{
	my ($seconds) = @_;
	my $timeout = int($seconds * 4 + 120);

	return $timeout < 180 ? 180 : $timeout;
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
		max_server_vm_rss_kb => undef,
		max_server_pss_kb => undef,
		max_server_shared_kb => undef,
		max_server_private_kb => undef,
		max_smaps_rollup_readable => undef,
		max_smaps_rollup_unreadable => undef,
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
	update_resource_max($summary, max_server_vm_rss_kb =>
		$sample->{max_server_vm_rss_kb});
	update_resource_max($summary, max_server_pss_kb =>
		$sample->{max_server_pss_kb});
	update_resource_max($summary, max_server_shared_kb =>
		$sample->{max_server_shared_kb});
	update_resource_max($summary, max_server_private_kb =>
		$sample->{max_server_private_kb});
	update_resource_max($summary, max_smaps_rollup_readable =>
		$sample->{max_smaps_rollup_readable});
	update_resource_max($summary, max_smaps_rollup_unreadable =>
		$sample->{max_smaps_rollup_unreadable});
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
	my ($cmd, $label, $stdout_path, $stderr_path, $resource_sample,
		$timeout_seconds) = @_;

	open my $out, '>', $stdout_path or die "could not write $stdout_path: $!";
	open my $err, '>', $stderr_path or die "could not write $stderr_path: $!";

	my $pid = fork();
	die "fork failed for $label: $!" unless defined $pid;
	if ($pid == 0)
	{
		setpgrp(0, 0);
		open STDOUT, '>&', $out or die "dup stdout failed: $!";
		open STDERR, '>&', $err or die "dup stderr failed: $!";
		exec @$cmd or die "exec failed for $label: $!";
	}

	my $started_at = time();
	my $next_sample_at = $started_at;
	my $timed_out = 0;
	for (;;)
	{
		my $waited = waitpid($pid, WNOHANG);
		last if $waited == $pid;
		last if $waited < 0;

		my $now = time();
		if (defined $timeout_seconds &&
			$timeout_seconds > 0 &&
			$now - $started_at > $timeout_seconds)
		{
			$timed_out = 1;
			kill 'TERM', -$pid;
			kill 'TERM', $pid;
			for (1 .. 50)
			{
				$waited = waitpid($pid, WNOHANG);
				last if $waited == $pid || $waited < 0;
				select(undef, undef, undef, 0.1);
			}
			if ($waited == 0)
			{
				kill 'KILL', -$pid;
				kill 'KILL', $pid;
				waitpid($pid, 0);
				last;
			}
			last;
		}

		if (defined $resource_sample && $resource_sample->{enabled} &&
			$now >= $next_sample_at)
		{
			sample_server_resources($resource_sample);
			$next_sample_at = $now + $resource_sample->{interval_seconds};
		}

		my $sleep_seconds = 0.1;
		if (defined $resource_sample && $resource_sample->{enabled})
		{
			my $until_sample = $next_sample_at - $now;
			$sleep_seconds = $until_sample
			  if $until_sample > 0 && $until_sample < $sleep_seconds;
		}
		select(undef, undef, undef, $sleep_seconds);
	}
	my $rc = $?;
	close $out;
	close $err;

	my $output = slurp($stdout_path);
	if ($timed_out)
	{
		my $stderr = slurp($stderr_path);
		die "$label timed out after ${timeout_seconds}s: @$cmd\n$output\n$stderr\n";
	}
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
	my ($data_dir, $lane, $workload, $run_index, $raw_fh) = @_;
	my $pid = $sample_server_resources ?
		read_postmaster_pid($data_dir) : undef;
	my $detail_at = defined $lane ? time() + int($duration / 2) : undef;

	return {
		enabled => $sample_server_resources,
		interval_seconds => $resource_sample_interval_ms / 1000,
		postmaster_pid => $pid,
		data_dir => $data_dir,
		lane => $lane,
		workload => $workload,
		run_index => $run_index,
		raw_fh => $raw_fh,
		detail_enabled => $sample_memory_detail && defined $lane,
		detail_at_epoch => $detail_at,
		detail_snapshot_index => 0,
		max_server_processes => undef,
		max_server_threads => undef,
		max_server_rss_kb => undef,
		max_server_vm_rss_kb => undef,
		max_server_pss_kb => undef,
		max_server_shared_kb => undef,
		max_server_private_kb => undef,
		max_smaps_rollup_readable => undef,
		max_smaps_rollup_unreadable => undef,
		samples => 0,
	};
}

sub sample_server_resource_baseline
{
	my ($data_dir) = @_;
	my $resources = new_server_resource_sample($data_dir);

	return $resources unless $resources->{enabled};

	for my $sample_index (1 .. $resource_baseline_samples)
	{
		sample_server_resources($resources);
		select(undef, undef, undef, $resources->{interval_seconds})
		  if $sample_index < $resource_baseline_samples;
	}

	return $resources;
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
	my $vm_rss_kb = 0;
	my $pss_kb = 0;
	my $shared_kb = 0;
	my $private_kb = 0;
	my $smaps_rollup_readable = 0;
	my $smaps_rollup_unreadable = 0;
	my $saw_rss = 0;
	my $saw_vm_rss = 0;
	my $saw_pss = 0;
	my $saw_shared = 0;
	my $saw_private = 0;
	my @process_rollups;
	my $sample_index = $sample->{samples} + 1;
	my $process_index = 0;

	for my $pid (@pids)
	{
		my $pid_threads = linux_thread_count($pid);
		my $process_info = linux_process_info($pid);
		$threads += $pid_threads;
		my $memory = linux_process_memory_kb($pid);
		next unless defined $memory;
		$process_index++;

		if (defined $memory->{rss_kb})
		{
			$rss_kb += $memory->{rss_kb};
			$saw_rss = 1;
		}
		if (defined $memory->{vm_rss_kb})
		{
			$vm_rss_kb += $memory->{vm_rss_kb};
			$saw_vm_rss = 1;
		}
		if (defined $memory->{pss_kb})
		{
			$pss_kb += $memory->{pss_kb};
			$saw_pss = 1;
		}
		if (defined $memory->{shared_kb})
		{
			$shared_kb += $memory->{shared_kb};
			$saw_shared = 1;
		}
		if (defined $memory->{private_kb})
		{
			$private_kb += $memory->{private_kb};
			$saw_private = 1;
		}
		$smaps_rollup_readable += $memory->{smaps_rollup_readable} || 0;
		$smaps_rollup_unreadable += $memory->{smaps_rollup_unreadable} || 0;

		if ($sample->{detail_enabled})
		{
			my $rollup = {
				pid => $pid,
				ppid => defined $process_info->{ppid} ? $process_info->{ppid} : 'n/a',
				comm => defined $process_info->{comm} ? $process_info->{comm} : 'n/a',
				threads => $pid_threads,
				rss_kb => $memory->{rss_kb},
				vm_rss_kb => $memory->{vm_rss_kb},
				pss_kb => $memory->{pss_kb},
				shared_kb => $memory->{shared_kb},
				private_kb => $memory->{private_kb},
				smaps_rollup_readable => $memory->{smaps_rollup_readable} || 0,
				smaps_rollup_unreadable => $memory->{smaps_rollup_unreadable} || 0,
			};

			push @process_rollups, $rollup;
			write_server_process_rollup($sample, $sample_index,
				$process_index, $rollup);
		}
	}

	update_resource_max($sample, max_server_processes => scalar @pids);
	update_resource_max($sample, max_server_threads => $threads);
	update_resource_max($sample, max_server_rss_kb => $rss_kb) if $saw_rss;
	update_resource_max($sample, max_server_vm_rss_kb => $vm_rss_kb)
	  if $saw_vm_rss;
	update_resource_max($sample, max_server_pss_kb => $pss_kb) if $saw_pss;
	update_resource_max($sample, max_server_shared_kb => $shared_kb)
	  if $saw_shared;
	update_resource_max($sample, max_server_private_kb => $private_kb)
	  if $saw_private;
	update_resource_max($sample, max_smaps_rollup_readable =>
		$smaps_rollup_readable);
	update_resource_max($sample, max_smaps_rollup_unreadable =>
		$smaps_rollup_unreadable);
	$sample->{samples}++;

	write_server_resource_sample($sample, scalar @pids, $threads,
		$saw_rss ? $rss_kb : undef,
		$saw_vm_rss ? $vm_rss_kb : undef,
		$saw_pss ? $pss_kb : undef,
		$saw_shared ? $shared_kb : undef,
		$saw_private ? $private_kb : undef,
		$smaps_rollup_readable,
		$smaps_rollup_unreadable);

	if ($sample->{detail_enabled} &&
		$sample->{detail_snapshot_index} == 0 &&
		(!defined $sample->{detail_at_epoch} ||
		 time() >= $sample->{detail_at_epoch}))
	{
		write_memory_detail_snapshot($sample, $sample_index, \@pids,
			\@process_rollups, $threads);
	}
}

sub write_server_resource_sample
{
	my ($sample, $processes, $threads, $rss_kb, $vm_rss_kb, $pss_kb,
		$shared_kb, $private_kb, $smaps_rollup_readable,
		$smaps_rollup_unreadable) = @_;
	my $fh = $sample->{raw_fh};

	return unless defined $fh;
	return unless defined $sample->{lane} && defined $sample->{workload};

	print $fh join("\t",
		$sample->{lane},
		$sample->{workload},
		defined $sample->{run_index} ? $sample->{run_index} : 'n/a',
		$sample->{samples},
		$processes,
		$threads,
		defined $rss_kb ? $rss_kb : 'n/a',
		defined $vm_rss_kb ? $vm_rss_kb : 'n/a',
		defined $pss_kb ? $pss_kb : 'n/a',
		defined $shared_kb ? $shared_kb : 'n/a',
		defined $private_kb ? $private_kb : 'n/a',
		$smaps_rollup_readable,
		$smaps_rollup_unreadable), "\n";
}

sub write_server_process_rollup
{
	my ($sample, $sample_index, $process_index, $rollup) = @_;

	return unless defined $process_rollups_fh;
	return unless defined $sample->{lane} && defined $sample->{workload};

	print $process_rollups_fh join("\t",
		$sample->{lane},
		$sample->{workload},
		defined $sample->{run_index} ? $sample->{run_index} : 'n/a',
		$sample_index,
		$process_index,
		$rollup->{pid},
		$rollup->{ppid},
		$rollup->{comm},
		$rollup->{threads},
		defined $rollup->{rss_kb} ? $rollup->{rss_kb} : 'n/a',
		defined $rollup->{vm_rss_kb} ? $rollup->{vm_rss_kb} : 'n/a',
		defined $rollup->{pss_kb} ? $rollup->{pss_kb} : 'n/a',
		defined $rollup->{shared_kb} ? $rollup->{shared_kb} : 'n/a',
		defined $rollup->{private_kb} ? $rollup->{private_kb} : 'n/a',
		$rollup->{smaps_rollup_readable},
		$rollup->{smaps_rollup_unreadable}), "\n";
}

sub write_memory_detail_snapshot
{
	my ($sample, $sample_index, $pids, $process_rollups, $threads) = @_;
	my %categories;
	my %paths;
	my %rollup_totals = (
		rss_kb => 0,
		pss_kb => 0,
		shared_kb => 0,
		private_kb => 0,
	);
	my $rollup_readable = 0;
	my $rollup_unreadable = 0;
	my $map_readable = 0;
	my $map_unreadable = 0;

	$sample->{detail_snapshot_index}++;
	my $snapshot_index = $sample->{detail_snapshot_index};

	for my $rollup (@$process_rollups)
	{
		for my $field (qw(rss_kb pss_kb shared_kb private_kb))
		{
			$rollup_totals{$field} += $rollup->{$field}
			  if defined $rollup->{$field};
		}
		$rollup_readable += $rollup->{smaps_rollup_readable} || 0;
		$rollup_unreadable += $rollup->{smaps_rollup_unreadable} || 0;
	}

	for my $pid (@$pids)
	{
		my $pid_smaps =
		  linux_process_smaps_categories($pid, $sample->{data_dir});

		if (!defined $pid_smaps)
		{
			$map_unreadable++;
			next;
		}

		$map_readable++;
		for my $category (keys %{ $pid_smaps->{categories} })
		{
			for my $field (qw(mappings size_kb rss_kb pss_kb shared_kb private_kb))
			{
				$categories{$category}{$field} +=
				  $pid_smaps->{categories}{$category}{$field} || 0;
			}
		}
		for my $path_key (keys %{ $pid_smaps->{paths} })
		{
			for my $field (qw(mappings size_kb rss_kb pss_kb shared_kb private_kb))
			{
				$paths{$path_key}{$field} +=
				  $pid_smaps->{paths}{$path_key}{$field} || 0;
			}
			$paths{$path_key}{category} = $pid_smaps->{paths}{$path_key}{category};
			$paths{$path_key}{path} = $pid_smaps->{paths}{$path_key}{path};
		}
	}

	for my $category (sort keys %categories)
	{
		my $entry = $categories{$category};

		print $memory_map_summary_fh join("\t",
			$sample->{lane},
			$sample->{workload},
			$sample->{run_index},
			$snapshot_index,
			$sample_index,
			$category,
			map { metric_value($entry->{$_} || 0, 1) }
			  qw(mappings size_kb rss_kb pss_kb shared_kb private_kb)),
		  "\n";
	}

	for my $path_key (sort keys %paths)
	{
		my $entry = $paths{$path_key};

		print $memory_map_path_summary_fh join("\t",
			$sample->{lane},
			$sample->{workload},
			$sample->{run_index},
			$snapshot_index,
			$sample_index,
			$entry->{category},
			$entry->{path},
			map { metric_value($entry->{$_} || 0, 1) }
			  qw(mappings size_kb rss_kb pss_kb shared_kb private_kb)),
		  "\n";
	}

	my %map_totals = (
		rss_kb => category_sum(\%categories, 'rss_kb'),
		pss_kb => category_sum(\%categories, 'pss_kb'),
		shared_kb => category_sum(\%categories, 'shared_kb'),
		private_kb => category_sum(\%categories, 'private_kb'),
	);
	my @memory_fields = qw(rss_kb pss_kb shared_kb private_kb);
	my @rollup_values =
	  map { metric_value($rollup_totals{$_}, 1) } @memory_fields;
	my @map_values =
	  map { metric_value($map_totals{$_}, 1) } @memory_fields;
	my @diff_values =
	  map {
		  metric_value(number_or_zero($rollup_totals{$_}) -
			  number_or_zero($map_totals{$_}), 1)
	  } @memory_fields;

	print $memory_accounting_fh join("\t",
		$sample->{lane},
		$sample->{workload},
		$sample->{run_index},
		$snapshot_index,
		$sample_index,
		scalar @$pids,
		$threads,
		@rollup_values,
		@map_values,
		@diff_values,
		$map_readable,
		$map_unreadable),
	  "\n";

	for my $pid (@$pids)
	{
		next unless linux_thread_count($pid) > 1;
		write_thread_stack_snapshot($sample, $snapshot_index, $sample_index,
			$pid);
	}
}

sub category_sum
{
	my ($categories, $field) = @_;
	my $sum = 0;

	for my $category (keys %$categories)
	{
		$sum += $categories->{$category}{$field} || 0;
	}
	return $sum;
}

sub number_or_zero
{
	my ($value) = @_;

	return defined $value ? $value : 0;
}

sub parse_protocol_park_memory_log
{
	my ($server_log, $offset, $lane, $workload, $run_index, $fh,
		$guc_fh, $context_fh, $catcache_fh, $relcache_fh) = @_;
	my $sample_index = 0;
	my $guc_sample_index = 0;
	my $context_sample_index = 0;
	my $catcache_sample_index = 0;
	my $relcache_sample_index = 0;

	return unless defined $fh;
	return unless -e $server_log;

	open my $log_fh, '<', $server_log
	  or die "could not read $server_log: $!";
	seek $log_fh, $offset, 0
	  or die "could not seek $server_log: $!";

	while (defined(my $line = <$log_fh>))
	{
		my %fields;
		my $payload;

		if ($line =~ /protocol_park_guc_memory\s+(.*)$/)
		{
			next unless defined $guc_fh;
			$payload = $1;
			while ($payload =~ /([A-Za-z0-9_]+)=([^\s]+)/g)
			{
				$fields{$1} = $2;
			}

			$guc_sample_index++;
			print $guc_fh join("\t",
				$lane,
				$workload,
				$run_index,
				$guc_sample_index,
				map { defined $fields{$_} ? $fields{$_} : 'n/a' }
				  @protocol_park_guc_memory_fields), "\n";
			next;
		}

		if ($line =~ /protocol_park_context_memory\s+(.*)$/)
		{
			next unless defined $context_fh;
			$payload = $1;
			while ($payload =~ /([A-Za-z0-9_]+)=([^\s]+)/g)
			{
				$fields{$1} = $2;
			}

			$context_sample_index++;
			print $context_fh join("\t",
				$lane,
				$workload,
				$run_index,
				$context_sample_index,
				map { defined $fields{$_} ? $fields{$_} : 'n/a' }
				  @protocol_park_context_memory_fields), "\n";
			next;
		}

		if ($line =~ /protocol_park_catcache_memory\s+(.*)$/)
		{
			next unless defined $catcache_fh;
			$payload = $1;
			while ($payload =~ /([A-Za-z0-9_]+)=([^\s]+)/g)
			{
				$fields{$1} = $2;
			}

			$catcache_sample_index++;
			print $catcache_fh join("\t",
				$lane,
				$workload,
				$run_index,
				$catcache_sample_index,
				map { defined $fields{$_} ? $fields{$_} : 'n/a' }
				  @protocol_park_catcache_memory_fields), "\n";
			next;
		}

		if ($line =~ /protocol_park_relcache_memory\s+(.*)$/)
		{
			next unless defined $relcache_fh;
			$payload = $1;
			while ($payload =~ /([A-Za-z0-9_]+)=([^\s]+)/g)
			{
				$fields{$1} = $2;
			}

			$relcache_sample_index++;
			print $relcache_fh join("\t",
				$lane,
				$workload,
				$run_index,
				$relcache_sample_index,
				map { defined $fields{$_} ? $fields{$_} : 'n/a' }
				  @protocol_park_relcache_memory_fields), "\n";
			next;
		}

		next unless $line =~ /protocol_park_memory\s+(.*)$/;
		$payload = $1;
		while ($payload =~ /([A-Za-z0-9_]+)=([^\s]+)/g)
		{
			$fields{$1} = $2;
		}

		$sample_index++;
		print $fh join("\t",
			$lane,
			$workload,
			$run_index,
			$sample_index,
			map { defined $fields{$_} ? $fields{$_} : 'n/a' }
			  @protocol_park_memory_fields), "\n";
	}

	close $log_fh;
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

sub linux_process_info
{
	my ($pid) = @_;
	my %info;
	my $status = "/proc/$pid/status";

	open my $fh, '<', $status or return {};
	while (defined(my $line = <$fh>))
	{
		if ($line =~ /^Name:\s+(.+?)\s*$/)
		{
			$info{comm} = $1;
		}
		elsif ($line =~ /^PPid:\s+(\d+)/)
		{
			$info{ppid} = int($1);
		}
	}
	close $fh;
	return \%info;
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

sub linux_process_smaps_categories
{
	my ($pid, $data_dir) = @_;
	my $smaps = "/proc/$pid/smaps";
	my %categories;
	my %paths;
	my $current;

	open my $fh, '<', $smaps or return undef;
	while (defined(my $line = <$fh>))
	{
		if ($line =~ /^([0-9a-f]+)-([0-9a-f]+)\s+(\S+)\s+([0-9a-f]+)\s+(\S+)\s+(\d+)\s*(.*)$/)
		{
			finish_smaps_mapping(\%categories, \%paths, $current,
				$data_dir)
			  if defined $current;
			$current = {
				start => hex_address($1),
				end => hex_address($2),
				perms => $3,
				path => $7,
				rss_kb => 0,
				pss_kb => 0,
				shared_kb => 0,
				private_kb => 0,
			};
			$current->{size_kb} =
			  ($current->{end} - $current->{start}) / 1024;
			$current->{path} =~ s/^\s+|\s+$//g;
			next;
		}

		next unless defined $current;
		if ($line =~ /^Rss:\s+(\d+)\s+kB/)
		{
			$current->{rss_kb} = int($1);
		}
		elsif ($line =~ /^Pss:\s+(\d+)\s+kB/)
		{
			$current->{pss_kb} = int($1);
		}
		elsif ($line =~ /^Shared_(?:Clean|Dirty|Hugetlb):\s+(\d+)\s+kB/)
		{
			$current->{shared_kb} += int($1);
		}
		elsif ($line =~ /^Private_(?:Clean|Dirty|Hugetlb):\s+(\d+)\s+kB/)
		{
			$current->{private_kb} += int($1);
		}
	}
	finish_smaps_mapping(\%categories, \%paths, $current, $data_dir)
	  if defined $current;
	close $fh;

	return {
		categories => \%categories,
		paths => \%paths,
	};
}

sub finish_smaps_mapping
{
	my ($categories, $paths, $mapping, $data_dir) = @_;

	return unless defined $mapping;

	my $category =
	  linux_smaps_mapping_category($mapping->{path}, $mapping->{perms},
		$mapping->{size_kb}, $data_dir);
	my $path = smaps_path_label($mapping->{path});
	my $entry = $categories->{$category} ||= {
		mappings => 0,
		size_kb => 0,
		rss_kb => 0,
		pss_kb => 0,
		shared_kb => 0,
		private_kb => 0,
	};
	my $path_key = "$category\t$path";
	my $path_entry = $paths->{$path_key} ||= {
		category => $category,
		path => $path,
		mappings => 0,
		size_kb => 0,
		rss_kb => 0,
		pss_kb => 0,
		shared_kb => 0,
		private_kb => 0,
	};

	$entry->{mappings}++;
	$path_entry->{mappings}++;
	for my $field (qw(size_kb rss_kb pss_kb shared_kb private_kb))
	{
		$entry->{$field} += $mapping->{$field} || 0;
		$path_entry->{$field} += $mapping->{$field} || 0;
	}
}

sub smaps_path_label
{
	my ($path) = @_;

	$path = '[anonymous]' unless defined $path && length $path;
	$path =~ s/[\t\r\n]+/ /g;
	return $path;
}

sub linux_smaps_mapping_category
{
	my ($path, $perms, $size_kb, $data_dir) = @_;

	$path = '' unless defined $path;
	$perms = '' unless defined $perms;

	return 'heap' if $path eq '[heap]';
	return 'labeled_stack' if $path =~ /^\[stack(?::\d+)?\]$/;
	return 'vvar_vdso_vsyscall' if $path =~ /^\[(?:vvar|vdso|vsyscall)\]$/;
	return 'dev_zero_deleted' if $path =~ m{(?:^|/)dev/zero\s+\(deleted\)$};
	return 'shared_memory'
	  if $path =~ /^\[anon_shmem:/ ||
		 $path =~ m{/(?:dev/)?shm/} ||
		 $path =~ /SYSV/;
	return 'postgres_binary' if $path =~ m{/bin/postgres(?:\s|\z)};
	return 'data_directory_file'
	  if defined $data_dir && length($data_dir) && index($path, $data_dir) == 0;
	return 'thread_stack_candidate'
	  if $path eq '' && $perms =~ /^rw/ && $size_kb >= 7000 &&
		 $size_kb <= 9000;
	return 'anonymous_rw' if $path eq '' && $perms =~ /^rw/;
	return 'anonymous_guard' if $path eq '' && $perms =~ /^---/;
	return 'anonymous_exec' if $path eq '' && $perms =~ /x/;
	return 'anonymous_other' if $path eq '';
	return 'deleted_file' if $path =~ /\(deleted\)$/;
	return 'shared_library'
	  if $path =~ /\.so(?:[.\d]*)?(?:\s|\z)/ ||
		 $path =~ m{/(?:lib|lib64|usr/lib|usr/lib64)/};
	return 'locale_or_timezone' if $path =~ m{/locale/|/timezonesets/};
	return 'other_file';
}

sub write_thread_stack_snapshot
{
	my ($sample, $snapshot_index, $sample_index, $pid) = @_;
	my $task_dir = "/proc/$pid/task";

	opendir my $dh, $task_dir or return;
	my @tids = sort { $a <=> $b } grep { /^\d+$/ } readdir $dh;
	closedir $dh;

	for my $tid (@tids)
	{
		my $info = linux_thread_status_info($pid, $tid);
		my $stack_map_kb = linux_thread_stack_map_kb($pid, $tid);

		print $thread_stacks_fh join("\t",
			$sample->{lane},
			$sample->{workload},
			$sample->{run_index},
			$snapshot_index,
			$sample_index,
			$pid,
			$tid,
			defined $info->{name} ? $info->{name} : 'n/a',
			defined $info->{state} ? $info->{state} : 'n/a',
			defined $info->{vmstk_kb} ? $info->{vmstk_kb} : 'n/a',
			defined $stack_map_kb ? 1 : 0,
			defined $stack_map_kb ? $stack_map_kb : 'n/a'),
		  "\n";
	}
}

sub linux_thread_status_info
{
	my ($pid, $tid) = @_;
	my %info;
	my $status = "/proc/$pid/task/$tid/status";

	open my $fh, '<', $status or return {};
	while (defined(my $line = <$fh>))
	{
		if ($line =~ /^Name:\s+(.+?)\s*$/)
		{
			$info{name} = $1;
		}
		elsif ($line =~ /^State:\s+(.+?)\s*$/)
		{
			$info{state} = $1;
		}
		elsif ($line =~ /^VmStk:\s+(\d+)\s+kB/)
		{
			$info{vmstk_kb} = int($1);
		}
	}
	close $fh;
	return \%info;
}

sub linux_thread_stack_map_kb
{
	my ($pid, $tid) = @_;
	my $maps = "/proc/$pid/task/$tid/maps";

	open my $fh, '<', $maps or return undef;
	while (defined(my $line = <$fh>))
	{
		if ($line =~ /^([0-9a-f]+)-([0-9a-f]+)\s+\S+\s+\S+\s+\S+\s+\d+\s+\[stack(?::\d+)?\]\s*$/)
		{
			close $fh;
			return (hex_address($2) - hex_address($1)) / 1024;
		}
	}
	close $fh;
	return undef;
}

sub hex_address
{
	my ($value) = @_;

	no warnings 'portable';
	return hex($value);
}

sub linux_process_memory_kb
{
	my ($pid) = @_;
	my $rollup = "/proc/$pid/smaps_rollup";
	my $vm_rss_kb = linux_process_vm_rss_kb($pid);
	my %memory;

	if (open my $fh, '<', $rollup)
	{
		$memory{smaps_rollup_readable} = 1;
		$memory{smaps_rollup_unreadable} = 0;
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
			elsif ($line =~ /^Shared_(?:Clean|Dirty|Hugetlb):\s+(\d+)\s+kB/)
			{
				$memory{shared_kb} += int($1);
			}
			elsif ($line =~ /^Private_(?:Clean|Dirty|Hugetlb):\s+(\d+)\s+kB/)
			{
				$memory{private_kb} += int($1);
			}
		}
		close $fh;
		$memory{vm_rss_kb} = $vm_rss_kb if defined $vm_rss_kb;
		return \%memory if %memory;
	}

	return undef unless defined $vm_rss_kb;

	return {
		vm_rss_kb => $vm_rss_kb,
		smaps_rollup_readable => 0,
		smaps_rollup_unreadable => 1,
	};
}

sub linux_process_vm_rss_kb
{
	my ($pid) = @_;
	my $status = "/proc/$pid/status";
	open my $fh, '<', $status or return undef;
	while (defined(my $line = <$fh>))
	{
		if ($line =~ /^VmRSS:\s+(\d+)\s+kB/)
		{
			close $fh;
			return int($1);
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

sub resource_number
{
	my ($sample, $key) = @_;

	return undef unless defined $sample && defined $sample->{$key};
	return undef unless $sample->{$key} =~ /^-?\d+(?:\.\d+)?$/;
	return 0 + $sample->{$key};
}

sub resource_delta
{
	my ($resources, $baseline, $key) = @_;
	my $value = resource_number($resources, $key);
	my $base = resource_number($baseline, $key);

	return undef unless defined $value && defined $base;
	return $value - $base;
}

sub metric_value
{
	my ($value, $digits) = @_;

	return 'n/a' unless defined $value;
	return sprintf("%.${digits}f", $value);
}

sub metric_ratio
{
	my ($value, $baseline, $digits) = @_;

	return 'n/a'
	  unless defined $value && defined $baseline && $baseline > 0;
	return sprintf("%.${digits}f", $value / $baseline);
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
	my $baseline_lane = ratio_baseline_lane($lane_specs);
	my $ratio_header = "ratio_vs_$baseline_lane";

	$ratio_header =~ s/[^A-Za-z0-9_]/_/g;

	open my $fh, '>', $path or die "could not write $path: $!";
	print $fh join("\t", 'workload', 'lane', 'tps', $ratio_header), "\n";
	for my $workload (@$workloads)
	{
		my $baseline =
		  exists $results->{$baseline_lane}{$workload}
		  ? $results->{$baseline_lane}{$workload}{tps}
		  : undef;
		for my $lane (@$lane_specs)
		{
			my $name = $lane->{name};
			next unless exists $results->{$name}{$workload};
			my $tps = $results->{$name}{$workload}{tps};
			my $ratio =
			  defined $baseline && $baseline > 0
			  ? sprintf('%.3f', $tps / $baseline)
			  : 'n/a';
			print $fh join("\t", $workload, $name, $tps, $ratio), "\n";
		}
	}

	close $fh;
}

sub write_resource_efficiency
{
	my ($dir, $workloads, $lane_specs, $results) = @_;
	my $path = File::Spec->catfile($dir, 'resource_efficiency.tsv');
	my $baseline_lane = ratio_baseline_lane($lane_specs);

	open my $fh, '>', $path or die "could not write $path: $!";
	print $fh join("\t", qw(workload lane tps max_server_threads
		  tps_per_server_thread clients_per_server_thread pss_kb_per_client
		  private_kb_per_client
		  tps_per_thread_vs_baseline server_threads_vs_baseline
		  pss_kb_vs_baseline private_kb_vs_baseline)), "\n";

	for my $workload (@$workloads)
	{
		my $baseline = $results->{$baseline_lane}{$workload};
		my $baseline_threads =
		  resource_number($baseline->{resources}, 'max_server_threads');
		my $baseline_private =
		  resource_number($baseline->{resources}, 'max_server_private_kb');
		my $baseline_pss =
		  resource_number($baseline->{resources}, 'max_server_pss_kb');
		my $baseline_tps_per_thread =
		  defined $baseline_threads && $baseline_threads > 0
		  ? $baseline->{tps} / $baseline_threads
		  : undef;

		for my $lane (@$lane_specs)
		{
			my $name = $lane->{name};
			next unless exists $results->{$name}{$workload};

			my $result = $results->{$name}{$workload};
			my $resources = $result->{resources};
			my $threads = resource_number($resources, 'max_server_threads');
			my $private = resource_number($resources, 'max_server_private_kb');
			my $pss = resource_number($resources, 'max_server_pss_kb');
			my $tps_per_thread =
			  defined $threads && $threads > 0 ? $result->{tps} / $threads : undef;
			my $clients_per_thread =
			  defined $threads && $threads > 0 ? $clients / $threads : undef;
			my $pss_per_client =
			  defined $pss && $clients > 0 ? $pss / $clients : undef;
			my $private_per_client =
			  defined $private && $clients > 0 ? $private / $clients : undef;

			print $fh join("\t",
				$workload,
				$name,
				$result->{tps},
				defined $threads ? $threads : 'n/a',
				metric_value($tps_per_thread, 3),
				metric_value($clients_per_thread, 3),
				metric_value($pss_per_client, 1),
				metric_value($private_per_client, 1),
				metric_ratio($tps_per_thread, $baseline_tps_per_thread, 3),
				metric_ratio($threads, $baseline_threads, 3),
				metric_ratio($pss, $baseline_pss, 3),
				metric_ratio($private, $baseline_private, 3)),
			  "\n";
		}
	}
	close $fh;
}

sub write_memory_footprint
{
	my ($dir, $workloads, $lane_specs, $results) = @_;
	my $path = File::Spec->catfile($dir, 'memory_footprint.tsv');

	open my $fh, '>', $path or die "could not write $path: $!";
	print $fh join("\t", qw(workload lane clients max_server_processes
		  max_server_threads baseline_server_processes baseline_server_threads
		  private_delta_kb pss_delta_kb rss_delta_kb
		  vm_rss_delta_kb shared_delta_kb
		  private_delta_per_client_kb pss_delta_per_client_kb
		  private_delta_per_server_thread_kb pss_delta_per_server_thread_kb
		  pooled_idle_session_private_kb pooled_carrier_private_kb
		  pooled_idle_session_pss_kb pooled_carrier_pss_kb
		  pooled_fit_points)),
	  "\n";

	for my $workload (@$workloads)
	{
		my $private_fit =
		  pooled_memory_fit($workload, $lane_specs, $results,
			'max_server_private_kb');
		my $pss_fit =
		  pooled_memory_fit($workload, $lane_specs, $results,
			'max_server_pss_kb');

		for my $lane (@$lane_specs)
		{
			my $name = $lane->{name};
			next unless exists $results->{$name}{$workload};

			my $result = $results->{$name}{$workload};
			my $resources = $result->{resources};
			my $baseline = $result->{baseline_resources};
			my $processes = resource_number($resources, 'max_server_processes');
			my $threads = resource_number($resources, 'max_server_threads');
			my $baseline_processes =
			  resource_number($baseline, 'max_server_processes');
			my $baseline_threads =
			  resource_number($baseline, 'max_server_threads');
			my $thread_delta =
			  resource_delta($resources, $baseline, 'max_server_threads');
			my $private_delta =
			  resource_delta($resources, $baseline, 'max_server_private_kb');
			my $pss_delta =
			  resource_delta($resources, $baseline, 'max_server_pss_kb');
			my $rss_delta =
			  resource_delta($resources, $baseline, 'max_server_rss_kb');
			my $vm_rss_delta =
			  resource_delta($resources, $baseline, 'max_server_vm_rss_kb');
			my $shared_delta =
			  resource_delta($resources, $baseline, 'max_server_shared_kb');
			my $private_per_client =
			  defined $private_delta && $clients > 0
			  ? $private_delta / $clients
			  : undef;
			my $pss_per_client =
			  defined $pss_delta && $clients > 0 ? $pss_delta / $clients : undef;
			my $private_per_thread =
			  defined $private_delta && defined $thread_delta && $thread_delta > 0
			  ? $private_delta / $thread_delta
			  : undef;
			my $pss_per_thread =
			  defined $pss_delta && defined $thread_delta && $thread_delta > 0
			  ? $pss_delta / $thread_delta
			  : undef;

			print $fh join("\t",
				$workload,
				$name,
				$clients,
				defined $processes ? $processes : 'n/a',
				defined $threads ? $threads : 'n/a',
				defined $baseline_processes ? $baseline_processes : 'n/a',
				defined $baseline_threads ? $baseline_threads : 'n/a',
				metric_value($private_delta, 1),
				metric_value($pss_delta, 1),
				metric_value($rss_delta, 1),
				metric_value($vm_rss_delta, 1),
				metric_value($shared_delta, 1),
				metric_value($private_per_client, 2),
				metric_value($pss_per_client, 2),
				metric_value($private_per_thread, 2),
				metric_value($pss_per_thread, 2),
				pooled_fit_session_value($private_fit),
				pooled_fit_carrier_value($private_fit),
				pooled_fit_session_value($pss_fit),
				pooled_fit_carrier_value($pss_fit),
				pooled_fit_points($private_fit, $pss_fit)),
			  "\n";
		}
	}

	close $fh;
}

sub append_protocol_park_memory_summary
{
	my ($fh, $dir) = @_;
	my $path = File::Spec->catfile($dir, 'protocol_park_memory_summary.tsv');
	my %field_index;
	my @rows;

	return unless -e $path;
	open my $summary_fh, '<', $path or return;
	my $header = <$summary_fh>;
	chomp $header if defined $header;
	my @header = defined $header ? split /\t/, $header : ();
	for my $i (0 .. $#header)
	{
		$field_index{$header[$i]} = $i;
	}

	while (defined(my $line = <$summary_fh>))
	{
		chomp $line;
		next if $line eq '';
		push @rows, [ split /\t/, $line, -1 ];
	}
	close $summary_fh;
	return unless @rows;

	print $fh "\n## Protocol Park Memory Attribution\n\n";
	print $fh "| Workload | Lane | Park samples | Top used kB | Message used kB | Cache used kB | Current used kB | RowDescription used kB | Client info used kB | Legacy session used kB | Logical struct kB | Runtime struct kB |\n";
	print $fh "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n";
	for my $row (@rows)
	{
		print $fh "| `", protocol_summary_value($row, \%field_index, 'workload'), "` | `",
		  protocol_summary_value($row, \%field_index, 'lane'), "` | ",
		  protocol_summary_value($row, \%field_index, 'park_samples'), " | ",
		  protocol_summary_value($row, \%field_index,
			'top_used_bytes_median_kb'), " | ",
		  protocol_summary_value($row, \%field_index,
			'message_used_bytes_median_kb'), " | ",
		  protocol_summary_value($row, \%field_index,
			'cache_used_bytes_median_kb'), " | ",
		  protocol_summary_value($row, \%field_index,
			'current_used_bytes_median_kb'), " | ",
		  protocol_summary_value($row, \%field_index,
			'row_description_used_bytes_median_kb'), " | ",
		  protocol_summary_value($row, \%field_index,
			'client_info_used_bytes_median_kb'), " | ",
		  protocol_summary_value($row, \%field_index,
			'legacy_session_used_bytes_median_kb'), " | ",
		  protocol_summary_value($row, \%field_index,
			'sizeof_logical_state_median_kb'), " | ",
		  protocol_summary_value($row, \%field_index,
			'sizeof_runtime_state_median_kb'), " |\n";
	}
}

sub append_protocol_park_context_memory_summary
{
	my ($fh, $dir) = @_;
	my $path = File::Spec->catfile($dir,
		'protocol_park_context_memory_summary.tsv');
	my ($header, $rows) = read_tsv_rows($path);
	my %emitted;

	return unless @$rows;

	print $fh "\n## Protocol Park Context Attribution\n\n";
	print $fh "| Workload | Lane | Context path | Samples | Local total kB | Local used kB | Local free kB | Recursive total kB | Recursive used kB |\n";
	print $fh "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |\n";
	for my $row (@$rows)
	{
		my $lane = tsv_row_value($row, $header, 'lane');
		my $workload = tsv_row_value($row, $header, 'workload');
		my $key = "$lane\t$workload";

		$emitted{$key} ||= 0;
		next if $emitted{$key} >= 12;
		$emitted{$key}++;

		print $fh "| `$workload` | `$lane` | `",
		  tsv_row_value($row, $header, 'path'), "` | ",
		  tsv_row_value($row, $header, 'context_samples'), " | ",
		  tsv_row_value($row, $header, 'local_total_bytes_median_kb'), " | ",
		  tsv_row_value($row, $header, 'local_used_bytes_median_kb'), " | ",
		  tsv_row_value($row, $header, 'local_free_bytes_median_kb'), " | ",
		  tsv_row_value($row, $header, 'recursive_total_bytes_median_kb'), " | ",
		  tsv_row_value($row, $header, 'recursive_used_bytes_median_kb'), " |\n";
	}
}

sub append_memory_detail_summary
{
	my ($fh, $dir) = @_;

	append_process_rollup_summary($fh, $dir);
	append_memory_accounting_summary($fh, $dir);
	append_memory_map_category_summary($fh, $dir);
	append_memory_map_path_top($fh, $dir);
	append_thread_stack_summary($fh, $dir);
}

sub append_process_rollup_summary
{
	my ($fh, $dir) = @_;
	my $path = File::Spec->catfile($dir, 'server_process_rollup_summary.tsv');
	my ($header, $rows) = read_tsv_rows($path);

	return unless @$rows;

	print $fh "\n## Process Rollup Detail\n\n";
	print $fh "| Workload | Lane | Processes | Threads | Total PSS kB | Total private kB | Median process PSS kB | Max process PSS kB | Median process private kB | Max process private kB |\n";
	print $fh "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n";
	for my $row (@$rows)
	{
		print $fh "| `", tsv_row_value($row, $header, 'workload'), "` | `",
		  tsv_row_value($row, $header, 'lane'), "` | ",
		  tsv_row_value($row, $header, 'processes'), " | ",
		  tsv_row_value($row, $header, 'threads'), " | ",
		  tsv_row_value($row, $header, 'total_pss_kb'), " | ",
		  tsv_row_value($row, $header, 'total_private_kb'), " | ",
		  tsv_row_value($row, $header, 'median_process_pss_kb'), " | ",
		  tsv_row_value($row, $header, 'max_process_pss_kb'), " | ",
		  tsv_row_value($row, $header, 'median_process_private_kb'), " | ",
		  tsv_row_value($row, $header, 'max_process_private_kb'), " |\n";
	}
}

sub append_memory_accounting_summary
{
	my ($fh, $dir) = @_;
	my $path = File::Spec->catfile($dir, 'server_memory_accounting.tsv');
	my ($header, $rows) = read_tsv_rows($path);

	return unless @$rows;

	print $fh "\n## Memory Accounting Check\n\n";
	print $fh "| Workload | Lane | Processes | Threads | Rollup PSS kB | Map PSS kB | PSS diff kB | Rollup private kB | Map private kB | Private diff kB | smaps pids read | smaps pids missed |\n";
	print $fh "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n";
	for my $row (@$rows)
	{
		print $fh "| `", tsv_row_value($row, $header, 'workload'), "` | `",
		  tsv_row_value($row, $header, 'lane'), "` | ",
		  tsv_row_value($row, $header, 'processes'), " | ",
		  tsv_row_value($row, $header, 'threads'), " | ",
		  tsv_row_value($row, $header, 'rollup_pss_kb'), " | ",
		  tsv_row_value($row, $header, 'map_pss_kb'), " | ",
		  tsv_row_value($row, $header, 'pss_diff_kb'), " | ",
		  tsv_row_value($row, $header, 'rollup_private_kb'), " | ",
		  tsv_row_value($row, $header, 'map_private_kb'), " | ",
		  tsv_row_value($row, $header, 'private_diff_kb'), " | ",
		  tsv_row_value($row, $header, 'smaps_pids_readable'), " | ",
		  tsv_row_value($row, $header, 'smaps_pids_unreadable'), " |\n";
	}
}

sub append_memory_map_category_summary
{
	my ($fh, $dir) = @_;
	my $path =
	  File::Spec->catfile($dir, 'server_memory_map_category_summary.tsv');
	my ($header, $rows) = read_tsv_rows($path);
	my %emitted;

	return unless @$rows;

	print $fh "\n## Memory Map Categories\n\n";
	print $fh "| Workload | Lane | Category | Mappings | PSS kB | Private kB | PSS share | Private share |\n";
	print $fh "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |\n";
	for my $row (@$rows)
	{
		my $lane = tsv_row_value($row, $header, 'lane');
		my $workload = tsv_row_value($row, $header, 'workload');
		my $key = "$lane\t$workload";

		$emitted{$key} ||= 0;
		next if $emitted{$key} >= 6;
		$emitted{$key}++;

		print $fh "| `$workload` | `$lane` | `",
		  tsv_row_value($row, $header, 'category'), "` | ",
		  tsv_row_value($row, $header, 'mappings'), " | ",
		  tsv_row_value($row, $header, 'pss_kb'), " | ",
		  tsv_row_value($row, $header, 'private_kb'), " | ",
		  tsv_row_value($row, $header, 'pss_pct'), " | ",
		  tsv_row_value($row, $header, 'private_pct'), " |\n";
	}
}

sub append_memory_map_path_top
{
	my ($fh, $dir) = @_;
	my $path = File::Spec->catfile($dir, 'server_memory_map_path_top.tsv');
	my ($header, $rows) = read_tsv_rows($path);

	return unless @$rows;

	print $fh "\n## Memory Map Top Paths\n\n";
	print $fh "| Workload | Lane | Category | Path | Mappings | PSS kB | Private kB |\n";
	print $fh "| --- | --- | --- | --- | ---: | ---: | ---: |\n";
	for my $row (@$rows)
	{
		print $fh "| `", tsv_row_value($row, $header, 'workload'), "` | `",
		  tsv_row_value($row, $header, 'lane'), "` | `",
		  tsv_row_value($row, $header, 'category'), "` | `",
		  tsv_row_value($row, $header, 'path'), "` | ",
		  tsv_row_value($row, $header, 'mappings'), " | ",
		  tsv_row_value($row, $header, 'pss_kb'), " | ",
		  tsv_row_value($row, $header, 'private_kb'), " |\n";
	}
}

sub append_thread_stack_summary
{
	my ($fh, $dir) = @_;
	my $path = File::Spec->catfile($dir, 'server_thread_stack_summary.tsv');
	my ($header, $rows) = read_tsv_rows($path);

	return unless @$rows;

	print $fh "\n## Thread Stack Visibility\n\n";
	print $fh "| Workload | Lane | Threads | Stack maps found | Total VmStk kB | Median VmStk kB | Max VmStk kB | Total stack map kB | Median stack map kB |\n";
	print $fh "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n";
	for my $row (@$rows)
	{
		print $fh "| `", tsv_row_value($row, $header, 'workload'), "` | `",
		  tsv_row_value($row, $header, 'lane'), "` | ",
		  tsv_row_value($row, $header, 'threads'), " | ",
		  tsv_row_value($row, $header, 'stack_map_found'), " | ",
		  tsv_row_value($row, $header, 'total_vmstk_kb'), " | ",
		  tsv_row_value($row, $header, 'median_vmstk_kb'), " | ",
		  tsv_row_value($row, $header, 'max_vmstk_kb'), " | ",
		  tsv_row_value($row, $header, 'total_stack_map_kb'), " | ",
		  tsv_row_value($row, $header, 'median_stack_map_kb'), " |\n";
	}
}

sub read_tsv_rows
{
	my ($path) = @_;
	my %field_index;
	my @rows;

	return (\%field_index, \@rows) unless -e $path;
	open my $fh, '<', $path or return (\%field_index, \@rows);
	my $header = <$fh>;
	chomp $header if defined $header;
	my @header = defined $header ? split /\t/, $header : ();
	for my $i (0 .. $#header)
	{
		$field_index{$header[$i]} = $i;
	}
	while (defined(my $line = <$fh>))
	{
		chomp $line;
		next if $line eq '';
		push @rows, [ split /\t/, $line, -1 ];
	}
	close $fh;
	return (\%field_index, \@rows);
}

sub tsv_row_value
{
	my ($row, $field_index, $name) = @_;

	return 'n/a' unless exists $field_index->{$name};
	return defined $row->[$field_index->{$name}] &&
	  $row->[$field_index->{$name}] ne ''
	  ? $row->[$field_index->{$name}]
	  : 'n/a';
}

sub protocol_summary_value
{
	my ($row, $field_index, $name) = @_;

	return 'n/a' unless exists $field_index->{$name};
	return defined $row->[$field_index->{$name}] &&
	  $row->[$field_index->{$name}] ne ''
	  ? $row->[$field_index->{$name}]
	  : 'n/a';
}

sub write_protocol_park_memory_summary
{
	my ($dir, $raw_path) = @_;
	my $path = File::Spec->catfile($dir, 'protocol_park_memory_summary.tsv');
	my %field_index;
	my %groups;

	open my $raw_fh, '<', $raw_path or die "could not read $raw_path: $!";
	my $header = <$raw_fh>;
	chomp $header if defined $header;
	my @header = defined $header ? split /\t/, $header : ();
	for my $i (0 .. $#header)
	{
		$field_index{$header[$i]} = $i;
	}

	while (defined(my $line = <$raw_fh>))
	{
		chomp $line;
		next if $line eq '';
		my @cols = split /\t/, $line, -1;
		my $lane = $cols[$field_index{lane}];
		my $workload = $cols[$field_index{workload}];
		my $key = "$lane\t$workload";

		$groups{$key}{lane} = $lane;
		$groups{$key}{workload} = $workload;
		$groups{$key}{count}++;
		for my $field (@protocol_park_memory_summary_fields)
		{
			next unless exists $field_index{$field};
			my $value = $cols[$field_index{$field}];

			next unless defined $value && $value =~ /^-?\d+(?:\.\d+)?$/;
			push @{ $groups{$key}{values}{$field} }, $value + 0;
		}
	}
	close $raw_fh;

	open my $fh, '>', $path or die "could not write $path: $!";
	print $fh join("\t",
		'workload',
		'lane',
		'park_samples',
		map { "${_}_median_kb" } @protocol_park_memory_summary_fields),
	  "\n";

	for my $key (sort keys %groups)
	{
		my $group = $groups{$key};

		print $fh join("\t",
			$group->{workload},
			$group->{lane},
			$group->{count},
			map {
				my $values = $group->{values}{$_};
				defined $values && @$values
				  ? metric_value(median(@$values) / 1024, 2)
				  : 'n/a'
			} @protocol_park_memory_summary_fields),
		  "\n";
	}

	close $fh;
}

sub write_protocol_park_guc_memory_summary
{
	my ($dir, $raw_path) = @_;
	my $path =
	  File::Spec->catfile($dir, 'protocol_park_guc_memory_summary.tsv');
	my %field_index;
	my %groups;

	open my $raw_fh, '<', $raw_path or die "could not read $raw_path: $!";
	my $header = <$raw_fh>;
	chomp $header if defined $header;
	my @header = defined $header ? split /\t/, $header : ();
	for my $i (0 .. $#header)
	{
		$field_index{$header[$i]} = $i;
	}

	while (defined(my $line = <$raw_fh>))
	{
		chomp $line;
		next if $line eq '';
		my @cols = split /\t/, $line, -1;
		my $lane = $cols[$field_index{lane}];
		my $workload = $cols[$field_index{workload}];
		my $key = "$lane\t$workload";

		$groups{$key}{lane} = $lane;
		$groups{$key}{workload} = $workload;
		$groups{$key}{count}++;
		for my $field (@protocol_park_guc_memory_summary_fields)
		{
			next unless exists $field_index{$field};
			my $value = $cols[$field_index{$field}];

			next unless defined $value && $value =~ /^-?\d+(?:\.\d+)?$/;
			push @{ $groups{$key}{values}{$field} }, $value + 0;
		}
	}
	close $raw_fh;

	open my $fh, '>', $path or die "could not write $path: $!";
	print $fh join("\t",
		'workload',
		'lane',
		'guc_samples',
		map {
			$_ =~ /bytes\z/ ? "${_}_median_kb" : "${_}_median"
		} @protocol_park_guc_memory_summary_fields),
	  "\n";

	for my $key (sort keys %groups)
	{
		my $group = $groups{$key};

		print $fh join("\t",
			$group->{workload},
			$group->{lane},
			$group->{count},
			map {
				my $values = $group->{values}{$_};

				if (!defined $values || !@$values)
				{
					'n/a';
				}
				elsif ($_ =~ /bytes\z/)
				{
					metric_value(median(@$values) / 1024, 2);
				}
				else
				{
					metric_value(median(@$values), 2);
				}
			} @protocol_park_guc_memory_summary_fields),
		  "\n";
	}

	close $fh;
}

sub write_protocol_park_context_memory_summary
{
	my ($dir, $raw_path) = @_;
	my $path =
	  File::Spec->catfile($dir, 'protocol_park_context_memory_summary.tsv');
	my %field_index;
	my %groups;

	open my $raw_fh, '<', $raw_path or die "could not read $raw_path: $!";
	my $header = <$raw_fh>;
	chomp $header if defined $header;
	my @header = defined $header ? split /\t/, $header : ();
	for my $i (0 .. $#header)
	{
		$field_index{$header[$i]} = $i;
	}

	while (defined(my $line = <$raw_fh>))
	{
		chomp $line;
		next if $line eq '';
		my @cols = split /\t/, $line, -1;
		my $lane = $cols[$field_index{lane}];
		my $workload = $cols[$field_index{workload}];
		my $path_value = $cols[$field_index{path}];
		my $type = $cols[$field_index{type}];
		my $name = $cols[$field_index{name}];
		my $depth = $cols[$field_index{depth}];
		my $key = join "\t", $lane, $workload, $path_value, $type, $name,
		  $depth;

		$groups{$key}{lane} = $lane;
		$groups{$key}{workload} = $workload;
		$groups{$key}{path} = $path_value;
		$groups{$key}{type} = $type;
		$groups{$key}{name} = $name;
		$groups{$key}{depth} = $depth;
		$groups{$key}{count}++;
		for my $field (@protocol_park_context_memory_summary_fields)
		{
			next unless exists $field_index{$field};
			my $value = $cols[$field_index{$field}];

			next unless defined $value && $value =~ /^-?\d+(?:\.\d+)?$/;
			push @{ $groups{$key}{values}{$field} }, $value + 0;
		}
	}
	close $raw_fh;

	open my $fh, '>', $path or die "could not write $path: $!";
	print $fh join("\t",
		'workload',
		'lane',
		'path',
		'type',
		'name',
		'depth',
		'context_samples',
		map { "${_}_median_kb" }
		  @protocol_park_context_memory_summary_fields),
	  "\n";

	my @groups =
	  sort {
		  protocol_context_summary_sort_value($groups{$b}, 'local_total_bytes') <=>
			protocol_context_summary_sort_value($groups{$a}, 'local_total_bytes') ||
		  protocol_context_summary_sort_value($groups{$b}, 'local_used_bytes') <=>
			protocol_context_summary_sort_value($groups{$a}, 'local_used_bytes') ||
		  $groups{$a}{path} cmp $groups{$b}{path}
	  } keys %groups;

	for my $key (@groups)
	{
		my $group = $groups{$key};

		print $fh join("\t",
			$group->{workload},
			$group->{lane},
			$group->{path},
			$group->{type},
			$group->{name},
			$group->{depth},
			$group->{count},
			map {
				my $values = $group->{values}{$_};
				defined $values && @$values
				  ? metric_value(median(@$values) / 1024, 2)
				  : 'n/a'
			} @protocol_park_context_memory_summary_fields),
		  "\n";
	}

	close $fh;
}

sub protocol_context_summary_sort_value
{
	my ($group, $field) = @_;
	my $values = $group->{values}{$field};

	return 0 unless defined $values && @$values;
	return median(@$values);
}

sub write_protocol_park_catcache_memory_summary
{
	my ($dir, $raw_path) = @_;
	my $path =
	  File::Spec->catfile($dir, 'protocol_park_catcache_memory_summary.tsv');
	my %field_index;
	my %groups;

	open my $raw_fh, '<', $raw_path or die "could not read $raw_path: $!";
	my $header = <$raw_fh>;
	chomp $header if defined $header;
	my @header = defined $header ? split /\t/, $header : ();
	for my $i (0 .. $#header)
	{
		$field_index{$header[$i]} = $i;
	}

	while (defined(my $line = <$raw_fh>))
	{
		chomp $line;
		next if $line eq '';
		my @cols = split /\t/, $line, -1;
		my $lane = $cols[$field_index{lane}];
		my $workload = $cols[$field_index{workload}];
		my $cache_id = $cols[$field_index{cache_id}];
		my $relname = $cols[$field_index{relname}];
		my $reloid = $cols[$field_index{reloid}];
		my $indexoid = $cols[$field_index{indexoid}];
		my $key = join "\t", $lane, $workload, $cache_id, $relname,
		  $reloid, $indexoid;

		$groups{$key}{lane} = $lane;
		$groups{$key}{workload} = $workload;
		$groups{$key}{cache_id} = $cache_id;
		$groups{$key}{relname} = $relname;
		$groups{$key}{reloid} = $reloid;
		$groups{$key}{indexoid} = $indexoid;
		$groups{$key}{count}++;
		for my $field (@protocol_park_catcache_memory_summary_fields)
		{
			next unless exists $field_index{$field};
			my $value = $cols[$field_index{$field}];

			next unless defined $value && $value =~ /^-?\d+(?:\.\d+)?$/;
			push @{ $groups{$key}{values}{$field} }, $value + 0;
		}
	}
	close $raw_fh;

	open my $fh, '>', $path or die "could not write $path: $!";
	print $fh join("\t",
		'workload',
		'lane',
		'cache_id',
		'relname',
		'reloid',
		'indexoid',
		'cache_samples',
		map {
			$_ =~ /bytes\z/ ? "${_}_median_kb" : "${_}_median"
		} @protocol_park_catcache_memory_summary_fields),
	  "\n";

	my @groups =
	  sort {
		  protocol_context_summary_sort_value($groups{$b},
			  'total_requested_bytes') <=>
			protocol_context_summary_sort_value($groups{$a},
				'total_requested_bytes') ||
		  $groups{$a}{relname} cmp $groups{$b}{relname}
	  } keys %groups;

	for my $key (@groups)
	{
		my $group = $groups{$key};

		print $fh join("\t",
			$group->{workload},
			$group->{lane},
			$group->{cache_id},
			$group->{relname},
			$group->{reloid},
			$group->{indexoid},
			$group->{count},
			map {
				my $values = $group->{values}{$_};
				defined $values && @$values
				  ? ($_ =~ /bytes\z/
					  ? metric_value(median(@$values) / 1024, 2)
					  : metric_value(median(@$values), 2))
				  : 'n/a'
			} @protocol_park_catcache_memory_summary_fields),
		  "\n";
	}

	close $fh;
}

sub write_protocol_park_relcache_memory_summary
{
	my ($dir, $raw_path) = @_;
	my $path =
	  File::Spec->catfile($dir, 'protocol_park_relcache_memory_summary.tsv');
	my %field_index;
	my %groups;

	open my $raw_fh, '<', $raw_path or die "could not read $raw_path: $!";
	my $header = <$raw_fh>;
	chomp $header if defined $header;
	my @header = defined $header ? split /\t/, $header : ();
	for my $i (0 .. $#header)
	{
		$field_index{$header[$i]} = $i;
	}

	while (defined(my $line = <$raw_fh>))
	{
		chomp $line;
		next if $line eq '';
		my @cols = split /\t/, $line, -1;
		my $lane = $cols[$field_index{lane}];
		my $workload = $cols[$field_index{workload}];
		my $reloid = $cols[$field_index{reloid}];
		my $relname = $cols[$field_index{relname}];
		my $key = join "\t", $lane, $workload, $reloid, $relname;

		$groups{$key}{lane} = $lane;
		$groups{$key}{workload} = $workload;
		$groups{$key}{reloid} = $reloid;
		$groups{$key}{relname} = $relname;
		$groups{$key}{count}++;
		for my $field (@protocol_park_relcache_memory_summary_fields)
		{
			next unless exists $field_index{$field};
			my $value = $cols[$field_index{$field}];

			next unless defined $value && $value =~ /^-?\d+(?:\.\d+)?$/;
			push @{ $groups{$key}{values}{$field} }, $value + 0;
		}
	}
	close $raw_fh;

	open my $fh, '>', $path or die "could not write $path: $!";
	print $fh join("\t",
		'workload',
		'lane',
		'reloid',
		'relname',
		'relcache_samples',
		map {
			$_ =~ /bytes\z/ ? "${_}_median_kb" : "${_}_median"
		} @protocol_park_relcache_memory_summary_fields),
	  "\n";

	my @groups =
	  sort {
		  protocol_context_summary_sort_value($groups{$b},
			  'private_context_used_bytes') <=>
			protocol_context_summary_sort_value($groups{$a},
				'private_context_used_bytes') ||
		  $groups{$a}{relname} cmp $groups{$b}{relname}
	  } keys %groups;

	for my $key (@groups)
	{
		my $group = $groups{$key};

		print $fh join("\t",
			$group->{workload},
			$group->{lane},
			$group->{reloid},
			$group->{relname},
			$group->{count},
			map {
				my $values = $group->{values}{$_};
				defined $values && @$values
				  ? ($_ =~ /bytes\z/
					  ? metric_value(median(@$values) / 1024, 2)
					  : metric_value(median(@$values), 2))
				  : 'n/a'
			} @protocol_park_relcache_memory_summary_fields),
		  "\n";
	}

	close $fh;
}

sub write_memory_detail_summaries
{
	my ($dir) = @_;

	write_process_rollup_summary($dir);
	write_memory_map_category_summary($dir);
	write_memory_map_path_top($dir);
	write_thread_stack_summary($dir);
}

sub write_process_rollup_summary
{
	my ($dir) = @_;
	my $raw_path = File::Spec->catfile($dir, 'server_process_rollups.tsv');
	my $summary_path =
	  File::Spec->catfile($dir, 'server_process_rollup_summary.tsv');
	my %field_index;
	my %samples;

	open my $raw_fh, '<', $raw_path or die "could not read $raw_path: $!";
	my $header = <$raw_fh>;
	chomp $header if defined $header;
	my @header = defined $header ? split /\t/, $header : ();
	for my $i (0 .. $#header)
	{
		$field_index{$header[$i]} = $i;
	}

	while (defined(my $line = <$raw_fh>))
	{
		chomp $line;
		next if $line eq '';
		my @cols = split /\t/, $line, -1;
		my $lane = $cols[$field_index{lane}];
		my $workload = $cols[$field_index{workload}];
		my $run = $cols[$field_index{run}];
		my $sample_index = $cols[$field_index{sample_index}];
		my $key = join "\t", $lane, $workload, $run, $sample_index;
		my $sample = $samples{$key} ||= {
			lane => $lane,
			workload => $workload,
			run => $run,
			sample_index => $sample_index,
			processes => 0,
			threads => 0,
			rss_kb => 0,
			pss_kb => 0,
			shared_kb => 0,
			private_kb => 0,
			pss_values => [],
			private_values => [],
		};
		my $threads = tsv_number(\@cols, \%field_index, 'threads');
		my $rss = tsv_number(\@cols, \%field_index, 'rss_kb');
		my $pss = tsv_number(\@cols, \%field_index, 'pss_kb');
		my $shared = tsv_number(\@cols, \%field_index, 'shared_kb');
		my $private = tsv_number(\@cols, \%field_index, 'private_kb');

		$sample->{processes}++;
		$sample->{threads} += $threads if defined $threads;
		$sample->{rss_kb} += $rss if defined $rss;
		$sample->{pss_kb} += $pss if defined $pss;
		$sample->{shared_kb} += $shared if defined $shared;
		$sample->{private_kb} += $private if defined $private;
		push @{ $sample->{pss_values} }, $pss if defined $pss;
		push @{ $sample->{private_values} }, $private if defined $private;
	}
	close $raw_fh;

	my %best;
	for my $sample (values %samples)
	{
		my $key = join "\t", $sample->{lane}, $sample->{workload};
		if (!defined $best{$key} ||
			$sample->{pss_kb} > $best{$key}{pss_kb})
		{
			$best{$key} = $sample;
		}
	}

	open my $fh, '>', $summary_path
	  or die "could not write $summary_path: $!";
	print $fh join("\t", qw(workload lane run sample_index processes threads
		  total_rss_kb total_pss_kb total_shared_kb total_private_kb
		  median_process_pss_kb max_process_pss_kb
		  median_process_private_kb max_process_private_kb)),
	  "\n";
	for my $key (sort keys %best)
	{
		my $sample = $best{$key};
		my @pss_values = @{ $sample->{pss_values} };
		my @private_values = @{ $sample->{private_values} };
		my @total_values =
		  map { metric_value($sample->{$_}, 1) }
		  qw(rss_kb pss_kb shared_kb private_kb);

		print $fh join("\t",
			$sample->{workload},
			$sample->{lane},
			$sample->{run},
			$sample->{sample_index},
			$sample->{processes},
			$sample->{threads},
			@total_values,
			@pss_values ? metric_value(median(@pss_values), 1) : 'n/a',
			@pss_values ? metric_value(max_value(@pss_values), 1) : 'n/a',
			@private_values ? metric_value(median(@private_values), 1) : 'n/a',
			@private_values ? metric_value(max_value(@private_values), 1) : 'n/a'),
		  "\n";
	}
	close $fh;
}

sub write_memory_map_category_summary
{
	my ($dir) = @_;
	my $raw_path = File::Spec->catfile($dir, 'server_memory_map_summary.tsv');
	my $summary_path =
	  File::Spec->catfile($dir, 'server_memory_map_category_summary.tsv');
	my %field_index;
	my %snapshots;

	open my $raw_fh, '<', $raw_path or die "could not read $raw_path: $!";
	my $header = <$raw_fh>;
	chomp $header if defined $header;
	my @header = defined $header ? split /\t/, $header : ();
	for my $i (0 .. $#header)
	{
		$field_index{$header[$i]} = $i;
	}

	while (defined(my $line = <$raw_fh>))
	{
		chomp $line;
		next if $line eq '';
		my @cols = split /\t/, $line, -1;
		my $lane = $cols[$field_index{lane}];
		my $workload = $cols[$field_index{workload}];
		my $run = $cols[$field_index{run}];
		my $snapshot_index = $cols[$field_index{snapshot_index}];
		my $key = join "\t", $lane, $workload, $run, $snapshot_index;
		my $snapshot = $snapshots{$key} ||= {
			lane => $lane,
			workload => $workload,
			run => $run,
			snapshot_index => $snapshot_index,
			total_pss_kb => 0,
			total_private_kb => 0,
			rows => [],
		};
		my $row = {
			category => $cols[$field_index{category}],
			mappings => tsv_number(\@cols, \%field_index, 'mappings') || 0,
			size_kb => tsv_number(\@cols, \%field_index, 'size_kb') || 0,
			rss_kb => tsv_number(\@cols, \%field_index, 'rss_kb') || 0,
			pss_kb => tsv_number(\@cols, \%field_index, 'pss_kb') || 0,
			shared_kb => tsv_number(\@cols, \%field_index, 'shared_kb') || 0,
			private_kb => tsv_number(\@cols, \%field_index, 'private_kb') || 0,
		};

		$snapshot->{total_pss_kb} += $row->{pss_kb};
		$snapshot->{total_private_kb} += $row->{private_kb};
		push @{ $snapshot->{rows} }, $row;
	}
	close $raw_fh;

	my %best;
	for my $snapshot (values %snapshots)
	{
		my $key = join "\t", $snapshot->{lane}, $snapshot->{workload};
		if (!defined $best{$key} ||
			$snapshot->{total_pss_kb} > $best{$key}{total_pss_kb})
		{
			$best{$key} = $snapshot;
		}
	}

	open my $fh, '>', $summary_path
	  or die "could not write $summary_path: $!";
	print $fh join("\t", qw(workload lane run snapshot_index category
		  mappings size_kb rss_kb pss_kb shared_kb private_kb pss_pct
		  private_pct)),
	  "\n";
	for my $key (sort keys %best)
	{
		my $snapshot = $best{$key};
		my @rows =
		  sort { $b->{private_kb} <=> $a->{private_kb} ||
				 $b->{pss_kb} <=> $a->{pss_kb} }
		  @{ $snapshot->{rows} };

		for my $row (@rows)
		{
			print $fh join("\t",
				$snapshot->{workload},
				$snapshot->{lane},
				$snapshot->{run},
				$snapshot->{snapshot_index},
				$row->{category},
				metric_value($row->{mappings}, 1),
				metric_value($row->{size_kb}, 1),
				metric_value($row->{rss_kb}, 1),
				metric_value($row->{pss_kb}, 1),
				metric_value($row->{shared_kb}, 1),
				metric_value($row->{private_kb}, 1),
				metric_ratio($row->{pss_kb}, $snapshot->{total_pss_kb}, 3),
				metric_ratio($row->{private_kb},
					$snapshot->{total_private_kb}, 3)),
			  "\n";
		}
	}
	close $fh;
}

sub write_memory_map_path_top
{
	my ($dir) = @_;
	my $raw_path = File::Spec->catfile($dir, 'server_memory_map_path_summary.tsv');
	my $top_path = File::Spec->catfile($dir, 'server_memory_map_path_top.tsv');
	my %field_index;
	my %snapshots;

	open my $raw_fh, '<', $raw_path or die "could not read $raw_path: $!";
	my $header = <$raw_fh>;
	chomp $header if defined $header;
	my @header = defined $header ? split /\t/, $header : ();
	for my $i (0 .. $#header)
	{
		$field_index{$header[$i]} = $i;
	}

	while (defined(my $line = <$raw_fh>))
	{
		chomp $line;
		next if $line eq '';
		my @cols = split /\t/, $line, -1;
		my $lane = $cols[$field_index{lane}];
		my $workload = $cols[$field_index{workload}];
		my $run = $cols[$field_index{run}];
		my $snapshot_index = $cols[$field_index{snapshot_index}];
		my $key = join "\t", $lane, $workload, $run, $snapshot_index;
		my $snapshot = $snapshots{$key} ||= {
			lane => $lane,
			workload => $workload,
			run => $run,
			snapshot_index => $snapshot_index,
			total_private_kb => 0,
			rows => [],
		};
		my $row = {
			category => $cols[$field_index{category}],
			path => $cols[$field_index{path}],
			mappings => tsv_number(\@cols, \%field_index, 'mappings') || 0,
			size_kb => tsv_number(\@cols, \%field_index, 'size_kb') || 0,
			rss_kb => tsv_number(\@cols, \%field_index, 'rss_kb') || 0,
			pss_kb => tsv_number(\@cols, \%field_index, 'pss_kb') || 0,
			shared_kb => tsv_number(\@cols, \%field_index, 'shared_kb') || 0,
			private_kb => tsv_number(\@cols, \%field_index, 'private_kb') || 0,
		};

		$snapshot->{total_private_kb} += $row->{private_kb};
		push @{ $snapshot->{rows} }, $row;
	}
	close $raw_fh;

	my %best;
	for my $snapshot (values %snapshots)
	{
		my $key = join "\t", $snapshot->{lane}, $snapshot->{workload};
		if (!defined $best{$key} ||
			$snapshot->{total_private_kb} > $best{$key}{total_private_kb})
		{
			$best{$key} = $snapshot;
		}
	}

	open my $fh, '>', $top_path or die "could not write $top_path: $!";
	print $fh join("\t", qw(workload lane run snapshot_index category path
		  mappings size_kb rss_kb pss_kb shared_kb private_kb)),
	  "\n";
	for my $key (sort keys %best)
	{
		my $snapshot = $best{$key};
		my @rows =
		  sort { $b->{private_kb} <=> $a->{private_kb} ||
				 $b->{pss_kb} <=> $a->{pss_kb} }
		  @{ $snapshot->{rows} };
		my $limit = @rows < 8 ? scalar @rows : 8;

		for my $i (0 .. $limit - 1)
		{
			my $row = $rows[$i];

			print $fh join("\t",
				$snapshot->{workload},
				$snapshot->{lane},
				$snapshot->{run},
				$snapshot->{snapshot_index},
				$row->{category},
				$row->{path},
				metric_value($row->{mappings}, 1),
				metric_value($row->{size_kb}, 1),
				metric_value($row->{rss_kb}, 1),
				metric_value($row->{pss_kb}, 1),
				metric_value($row->{shared_kb}, 1),
				metric_value($row->{private_kb}, 1)),
			  "\n";
		}
	}
	close $fh;
}

sub write_thread_stack_summary
{
	my ($dir) = @_;
	my $raw_path = File::Spec->catfile($dir, 'server_thread_stacks.tsv');
	my $summary_path =
	  File::Spec->catfile($dir, 'server_thread_stack_summary.tsv');
	my %field_index;
	my %snapshots;

	open my $raw_fh, '<', $raw_path or die "could not read $raw_path: $!";
	my $header = <$raw_fh>;
	chomp $header if defined $header;
	my @header = defined $header ? split /\t/, $header : ();
	for my $i (0 .. $#header)
	{
		$field_index{$header[$i]} = $i;
	}

	while (defined(my $line = <$raw_fh>))
	{
		chomp $line;
		next if $line eq '';
		my @cols = split /\t/, $line, -1;
		my $lane = $cols[$field_index{lane}];
		my $workload = $cols[$field_index{workload}];
		my $run = $cols[$field_index{run}];
		my $snapshot_index = $cols[$field_index{snapshot_index}];
		my $key = join "\t", $lane, $workload, $run, $snapshot_index;
		my $snapshot = $snapshots{$key} ||= {
			lane => $lane,
			workload => $workload,
			run => $run,
			snapshot_index => $snapshot_index,
			threads => 0,
			stack_map_found => 0,
			vmstk_values => [],
			stack_map_values => [],
		};
		my $vmstk = tsv_number(\@cols, \%field_index, 'vmstk_kb');
		my $stack_map = tsv_number(\@cols, \%field_index, 'stack_map_kb');
		my $found = tsv_number(\@cols, \%field_index, 'stack_map_found');

		$snapshot->{threads}++;
		$snapshot->{stack_map_found}++ if defined $found && $found > 0;
		push @{ $snapshot->{vmstk_values} }, $vmstk if defined $vmstk;
		push @{ $snapshot->{stack_map_values} }, $stack_map
		  if defined $stack_map;
	}
	close $raw_fh;

	my %best;
	for my $snapshot (values %snapshots)
	{
		my $key = join "\t", $snapshot->{lane}, $snapshot->{workload};
		if (!defined $best{$key} ||
			$snapshot->{threads} > $best{$key}{threads})
		{
			$best{$key} = $snapshot;
		}
	}

	open my $fh, '>', $summary_path
	  or die "could not write $summary_path: $!";
	print $fh join("\t", qw(workload lane run snapshot_index threads
		  stack_map_found total_vmstk_kb median_vmstk_kb max_vmstk_kb
		  total_stack_map_kb median_stack_map_kb max_stack_map_kb)),
	  "\n";
	for my $key (sort keys %best)
	{
		my $snapshot = $best{$key};
		my @vmstk = @{ $snapshot->{vmstk_values} };
		my @stack_map = @{ $snapshot->{stack_map_values} };

		print $fh join("\t",
			$snapshot->{workload},
			$snapshot->{lane},
			$snapshot->{run},
			$snapshot->{snapshot_index},
			$snapshot->{threads},
			$snapshot->{stack_map_found},
			metric_value(sum_values(@vmstk), 1),
			@vmstk ? metric_value(median(@vmstk), 1) : 'n/a',
			@vmstk ? metric_value(max_value(@vmstk), 1) : 'n/a',
			metric_value(sum_values(@stack_map), 1),
			@stack_map ? metric_value(median(@stack_map), 1) : 'n/a',
			@stack_map ? metric_value(max_value(@stack_map), 1) : 'n/a'),
		  "\n";
	}
	close $fh;
}

sub tsv_number
{
	my ($cols, $field_index, $field) = @_;

	return undef unless exists $field_index->{$field};
	my $value = $cols->[$field_index->{$field}];
	return undef unless defined $value && $value =~ /^-?\d+(?:\.\d+)?$/;
	return 0 + $value;
}

sub max_value
{
	my @values = @_;
	my $max;

	for my $value (@values)
	{
		$max = $value if !defined $max || $value > $max;
	}
	return $max;
}

sub sum_values
{
	my @values = @_;
	my $sum = 0;

	for my $value (@values)
	{
		$sum += $value;
	}
	return $sum;
}

sub pooled_memory_fit
{
	my ($workload, $lane_specs, $results, $memory_key) = @_;
	my @points;

	for my $lane (@$lane_specs)
	{
		my $name = $lane->{name};
		next unless $name =~ /^branch_pool_/;
		next unless exists $results->{$name}{$workload};

		my $result = $results->{$name}{$workload};
		my $thread_delta =
		  resource_delta($result->{resources}, $result->{baseline_resources},
			'max_server_threads');
		my $memory_delta =
		  resource_delta($result->{resources}, $result->{baseline_resources},
			$memory_key);

		next
		  unless defined $thread_delta && $thread_delta > 0 &&
		  defined $memory_delta;
		push @points, [ $thread_delta, $memory_delta ];
	}

	return undef unless @points >= 2;

	my ($sum_x, $sum_y, $sum_xx, $sum_xy) = (0, 0, 0, 0);
	for my $point (@points)
	{
		my ($x, $y) = @$point;
		$sum_x += $x;
		$sum_y += $y;
		$sum_xx += $x * $x;
		$sum_xy += $x * $y;
	}

	my $n = scalar @points;
	my $denominator = $n * $sum_xx - $sum_x * $sum_x;
	return undef if $denominator == 0;

	my $slope = ($n * $sum_xy - $sum_x * $sum_y) / $denominator;
	my $intercept = ($sum_y - $slope * $sum_x) / $n;

	return {
		points => $n,
		carrier_kb => $slope,
		session_kb => $clients > 0 ? $intercept / $clients : undef,
	};
}

sub pooled_fit_session_value
{
	my ($fit) = @_;

	return 'n/a' unless defined $fit;
	return metric_value($fit->{session_kb}, 2);
}

sub pooled_fit_carrier_value
{
	my ($fit) = @_;

	return 'n/a' unless defined $fit;
	return metric_value($fit->{carrier_kb}, 2);
}

sub pooled_fit_points
{
	my (@fits) = @_;
	my $points;

	for my $fit (@fits)
	{
		next unless defined $fit;
		$points = $fit->{points}
		  if !defined $points || $fit->{points} > $points;
	}

	return defined $points ? $points : 'n/a';
}

sub write_summary
{
	my ($dir, $workloads, $lane_specs, $results) = @_;
	my $path = File::Spec->catfile($dir, 'summary.md');
	my $baseline_lane = ratio_baseline_lane($lane_specs);

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
	print $fh "- ratio baseline: `$baseline_lane`\n\n";

	print $fh "| Workload |";
	for my $lane (@$lane_specs)
	{
		print $fh " $lane->{name} TPS |";
		print $fh " $lane->{name} / $baseline_lane |"
		  unless $lane->{name} eq $baseline_lane;
	}
	print $fh "\n| --- |";
	for my $lane (@$lane_specs)
	{
		print $fh " ---: |";
		print $fh " ---: |" unless $lane->{name} eq $baseline_lane;
	}
	print $fh "\n";

	for my $workload (@$workloads)
	{
		my $baseline =
		  exists $results->{$baseline_lane}{$workload}
		  ? $results->{$baseline_lane}{$workload}{tps}
		  : undef;
		print $fh "| `$workload` |";
		for my $lane (@$lane_specs)
		{
			my $name = $lane->{name};
			my $tps = $results->{$name}{$workload}{tps};
			print $fh " ", sprintf('%.1f', $tps), " |";
			if ($name ne $baseline_lane)
			{
				my $ratio =
				  defined $baseline && $baseline > 0
				  ? sprintf('%.3f', $tps / $baseline)
				  : 'n/a';
				print $fh " $ratio |";
			}
		}
		print $fh "\n";
	}

	print $fh "\n## Idle server resource baselines\n\n";
	print $fh "| Workload | Lane | Baseline server processes | Baseline server threads | Baseline smaps RSS kB | Baseline VmRSS kB | Baseline PSS kB | Baseline shared kB | Baseline private kB | smaps readable | smaps unreadable |\n";
	print $fh "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n";
	for my $workload (@$workloads)
	{
		for my $lane (@$lane_specs)
		{
			my $name = $lane->{name};
			next unless exists $results->{$name}{$workload};
			my $baseline = $results->{$name}{$workload}{baseline_resources};
			print $fh "| `$workload` | `$name` | ",
			  resource_value($baseline, 'max_server_processes'), " | ",
			  resource_value($baseline, 'max_server_threads'), " | ",
			  resource_value($baseline, 'max_server_rss_kb'), " | ",
			  resource_value($baseline, 'max_server_vm_rss_kb'), " | ",
			  resource_value($baseline, 'max_server_pss_kb'), " | ",
			  resource_value($baseline, 'max_server_shared_kb'), " | ",
			  resource_value($baseline, 'max_server_private_kb'), " | ",
			  resource_value($baseline, 'max_smaps_rollup_readable'), " | ",
			  resource_value($baseline, 'max_smaps_rollup_unreadable'), " |\n";
		}
	}

	print $fh "\n## Server resource samples\n\n";
	print $fh "| Workload | Lane | Max server processes | Max server threads | Max smaps RSS kB | Max VmRSS kB | Max PSS kB | Max shared kB | Max private kB | smaps readable | smaps unreadable |\n";
	print $fh "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n";
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
			  resource_value($resources, 'max_server_vm_rss_kb'), " | ",
			  resource_value($resources, 'max_server_pss_kb'), " | ",
			  resource_value($resources, 'max_server_shared_kb'), " | ",
			  resource_value($resources, 'max_server_private_kb'), " | ",
			  resource_value($resources, 'max_smaps_rollup_readable'), " | ",
			  resource_value($resources, 'max_smaps_rollup_unreadable'), " |\n";
		}
	}

	print $fh "\n## Server Memory Footprint\n\n";
	print $fh "| Workload | Lane | PSS delta/client kB | Private delta/client kB | PSS delta/server thread kB | Private delta/server thread kB | Pool-est. idle session PSS kB | Pool-est. carrier PSS kB | Pool-est. idle session private kB | Pool-est. carrier private kB |\n";
	print $fh "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n";
	for my $workload (@$workloads)
	{
		my $private_fit =
		  pooled_memory_fit($workload, $lane_specs, $results,
			'max_server_private_kb');
		my $pss_fit =
		  pooled_memory_fit($workload, $lane_specs, $results,
			'max_server_pss_kb');

		for my $lane (@$lane_specs)
		{
			my $name = $lane->{name};
			next unless exists $results->{$name}{$workload};

			my $result = $results->{$name}{$workload};
			my $resources = $result->{resources};
			my $baseline = $result->{baseline_resources};
			my $thread_delta =
			  resource_delta($resources, $baseline, 'max_server_threads');
			my $private_delta =
			  resource_delta($resources, $baseline, 'max_server_private_kb');
			my $pss_delta =
			  resource_delta($resources, $baseline, 'max_server_pss_kb');
			my $private_per_client =
			  defined $private_delta && $clients > 0
			  ? $private_delta / $clients
			  : undef;
			my $pss_per_client =
			  defined $pss_delta && $clients > 0 ? $pss_delta / $clients : undef;
			my $private_per_thread =
			  defined $private_delta && defined $thread_delta && $thread_delta > 0
			  ? $private_delta / $thread_delta
			  : undef;
			my $pss_per_thread =
			  defined $pss_delta && defined $thread_delta && $thread_delta > 0
			  ? $pss_delta / $thread_delta
			  : undef;

			print $fh "| `$workload` | `$name` | ",
			  metric_value($pss_per_client, 2), " | ",
			  metric_value($private_per_client, 2), " | ",
			  metric_value($pss_per_thread, 2), " | ",
			  metric_value($private_per_thread, 2), " | ",
			  pooled_fit_session_value($pss_fit), " | ",
			  pooled_fit_carrier_value($pss_fit), " | ",
			  pooled_fit_session_value($private_fit), " | ",
			  pooled_fit_carrier_value($private_fit), " |\n";
		}
	}

	print $fh "\n## Server Resource Efficiency\n\n";
	print $fh "| Workload | Lane | TPS/server thread | Clients/server thread | PSS kB/client | Private kB/client | TPS/thread / $baseline_lane | Threads / $baseline_lane | PSS kB / $baseline_lane | Private kB / $baseline_lane |\n";
	print $fh "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n";
	for my $workload (@$workloads)
	{
		my $baseline = $results->{$baseline_lane}{$workload};
		my $baseline_threads =
		  resource_number($baseline->{resources}, 'max_server_threads');
		my $baseline_private =
		  resource_number($baseline->{resources}, 'max_server_private_kb');
		my $baseline_pss =
		  resource_number($baseline->{resources}, 'max_server_pss_kb');
		my $baseline_tps_per_thread =
		  defined $baseline_threads && $baseline_threads > 0
		  ? $baseline->{tps} / $baseline_threads
		  : undef;

		for my $lane (@$lane_specs)
		{
			my $name = $lane->{name};
			next unless exists $results->{$name}{$workload};

			my $result = $results->{$name}{$workload};
			my $resources = $result->{resources};
			my $threads = resource_number($resources, 'max_server_threads');
			my $private = resource_number($resources, 'max_server_private_kb');
			my $pss = resource_number($resources, 'max_server_pss_kb');
			my $tps_per_thread =
			  defined $threads && $threads > 0 ? $result->{tps} / $threads : undef;
			my $clients_per_thread =
			  defined $threads && $threads > 0 ? $clients / $threads : undef;
			my $pss_per_client =
			  defined $pss && $clients > 0 ? $pss / $clients : undef;
			my $private_per_client =
			  defined $private && $clients > 0 ? $private / $clients : undef;

			print $fh "| `$workload` | `$name` | ",
			  metric_value($tps_per_thread, 3), " | ",
			  metric_value($clients_per_thread, 3), " | ",
			  metric_value($pss_per_client, 1), " | ",
			  metric_value($private_per_client, 1), " | ",
			  metric_ratio($tps_per_thread, $baseline_tps_per_thread, 3), " | ",
			  metric_ratio($threads, $baseline_threads, 3), " | ",
			  metric_ratio($pss, $baseline_pss, 3), " | ",
			  metric_ratio($private, $baseline_private, 3), " |\n";
		}
	}

	append_memory_detail_summary($fh, $dir);
	append_protocol_park_memory_summary($fh, $dir);
	append_protocol_park_context_memory_summary($fh, $dir);

	close $fh;
}

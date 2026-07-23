# Copyright (c) 2026, PostgreSQL Global Development Group
#
# 016: worker-fiber executor thread-pool sizing regression.
#
# In pooled-protocol mode the pooled carrier pool owns client-backend
# parallelism, so the xtc fiber executor runs only a handful of long-lived
# worker/aux fibers.  A 2026-07-23 EC2 A/B caught the fiber executor sized to
# the raw core count (384 loops on a 384-vCPU box), which stacked on the pooled
# carriers and, because libxtc creates one io_uring ring per loop, lazily
# spawned hundreds of io-wq worker threads per ring -- 4634 OS threads for one
# process, with the kernel CFS load-balancer as the top CPU consumer.
#
# xtc_carrier_loop_count() now sizes the pooled-mode executor to the worker
# concurrency (small, fixed, capped at XTC_PG_MAX_WORKER_FIBER_LOOPS=16), never
# the core count.  This test asserts the executor comes up as the small
# "worker-fiber pool" and that the process thread count stays bounded, so a
# future change cannot silently re-inflate the pool back to ncpus.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('worker_fiber_pool_sizing');
$node->init;
# A small, explicit carrier count.  The worker-fiber executor must be small
# regardless of how many cores the test host has.
$node->append_conf(
	'postgresql.conf', qq(
multithreaded = on
pooled_protocol_carriers = 4
io_method = sync
autovacuum_max_workers = 3
max_worker_processes = 8
));
$node->start;

is($node->safe_psql('postgres', 'SHOW multithreaded'),
	'on', 'multithreaded is on');
is($node->safe_psql('postgres', 'SHOW pooled_protocol_carriers'),
	'4', 'pooled_protocol_carriers is 4');

# Drive some connections + I/O so the worker-fiber executor is actually
# started (it comes up lazily at the first fiber-eligible worker launch).
$node->safe_psql('postgres',
	'CREATE TABLE t AS SELECT generate_series(1, 50000) i;');
for (1 .. 8)
{
	$node->safe_psql('postgres', 'SELECT count(*) FROM t;');
}

# The startup LOG line names the pool mode and its loop count.
my $log = slurp_file($node->logfile);
like(
	$log,
	qr/xtc: carrier scheduler thread up \((\d+) loops?, \d+ supervisors?, worker-fiber pool/,
	'carrier scheduler came up as the worker-fiber pool in pooled mode');

# Extract the loop count and assert it is SMALL (the worker-fiber budget),
# not the machine core count.  The cap is XTC_PG_MAX_WORKER_FIBER_LOOPS (16),
# further clamped to the carrier count (4) here.  A core-sized pool on a
# many-core CI host would blow past this and fail -- exactly the regression we
# are guarding.
my ($loops) =
  ($log =~ qr/xtc: carrier scheduler thread up \((\d+) loops?/);
ok(defined $loops, 'parsed the worker-fiber loop count');
cmp_ok($loops, '<=', 4,
	"worker-fiber pool ($loops loops) is clamped to the carrier count, not core-sized");
cmp_ok($loops, '>=', 1, 'worker-fiber pool has at least one loop');

# Belt-and-suspenders: the whole process thread count must stay bounded.
# libxtc makes one io_uring ring per loop, so a re-inflated pool would also
# re-inflate rings and their io-wq workers.  Count OS threads directly.
# The postmaster pid is the first line of postmaster.pid.
my $pm = (split /\n/, slurp_file($node->data_dir . '/postmaster.pid'))[0];
my $ntasks;
if (-d "/proc/$pm/task")
{
	opendir(my $dh, "/proc/$pm/task") or die "opendir: $!";
	my @tasks = grep { /^\d+$/ } readdir($dh);
	closedir($dh);
	$ntasks = scalar @tasks;
	# With carriers=4 + a ~4-loop worker pool + supervisors + scheduler + a few
	# io-wq workers, a healthy process is well under 128 threads.  A re-inflated
	# core-sized pool on a big CI host would be hundreds/thousands.
	cmp_ok($ntasks, '<', 128,
		"process thread count ($ntasks) stays bounded in pooled mode");
}
else
{
	# Not Linux / no procfs: skip the direct thread count, the log assertions
	# above still guard the sizing.
	ok(1, 'thread-count check skipped (no /proc)');
}

$node->stop;
done_testing();

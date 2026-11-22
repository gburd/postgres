# Copyright (c) 2026, PostgreSQL Global Development Group
#
# 016: carrier loop count and thread-pool bound.
#
# Every client backend and fiber-eligible worker runs on one fixed pool of
# carrier loops, one OS thread each.  libxtc creates one io_uring ring per loop,
# and each ring lazily spawns io-wq worker threads, so an oversized pool
# multiplies into thousands of OS threads (a 2026-07-23 EC2 A/B measured 4634
# for one process, with the kernel CFS load balancer as the top CPU consumer).
#
# This test asserts that the server starts exactly the requested number of
# loops and that the process thread count stays bounded, so a future change
# cannot silently inflate the pool.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $loops = 4;

my $node = PostgreSQL::Test::Cluster->new('carrier_loop_count');
$node->init;
$node->append_conf(
	'postgresql.conf', qq(
multithreaded = on
carrier_loops = $loops
io_method = sync
autovacuum_max_workers = 3
max_worker_processes = 8
));
$node->start;

is($node->safe_psql('postgres', 'SHOW multithreaded'),
	'on', 'multithreaded is on');

# Drive some connections and I/O so the carrier is started and every loop
# has had work.
$node->safe_psql('postgres',
	'CREATE TABLE t AS SELECT generate_series(1, 50000) i;');
for (1 .. 8)
{
	$node->safe_psql('postgres', 'SELECT count(*) FROM t;');
}

like(
	slurp_file($node->logfile),
	qr/xtc: carrier up \($loops loops, /,
	"carrier scheduler came up with exactly $loops loops");

# The whole process thread count must stay bounded.  The postmaster pid is the
# first line of postmaster.pid.
my $pm = (split /\n/, slurp_file($node->data_dir . '/postmaster.pid'))[0];
if (-d "/proc/$pm/task")
{
	opendir(my $dh, "/proc/$pm/task") or die "opendir: $!";
	my @tasks = grep { /^\d+$/ } readdir($dh);
	closedir($dh);

	# Four loops, their supervisors, the scheduler thread, and a few io-wq
	# workers stay well under 128 threads.  A core-sized pool on a big CI
	# host would be hundreds or thousands.
	cmp_ok(scalar @tasks, '<', 128,
		'process thread count (' . scalar(@tasks) . ') stays bounded');
}
else
{
	# Not Linux / no procfs: the log assertion above still guards the sizing.
	ok(1, 'thread-count check skipped (no /proc)');
}

$node->stop;
done_testing();

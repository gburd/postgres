# Copyright (c) 2022-2026, PostgreSQL Global Development Group
#
# Regression: DROP BUFFER POOL concurrent with a running checkpoint must not
# crash the checkpointer.
#
# The checkpointer's BufferSync() writes each dynamic pool's dirty buffers in a
# loop, caching the pool's local DSM descriptor array.  CheckpointWriteDelay()
# inside that loop processes interrupts, including a pending
# PROCSIGNAL_BARRIER_BUFPOOL_DETACH emitted by a concurrent DROP BUFFER POOL --
# whose handler detaches this process from the pool's DSM, freeing the
# descriptor array out from under the loop.  Before the fix the next iteration
# dereferenced freed memory and the checkpointer took SIGSEGV, taking the whole
# cluster down.  The loop now re-checks bp_active each iteration and stops
# touching a pool that is being torn down.
#
# Driven deterministically with an injection point: park the checkpointer on
# the first dynamic-pool buffer (holding the stale descriptor pointer), fire a
# DROP BUFFER POOL (whose detach barrier frees that pointer), then wake the
# checkpointer.  The guard must make it stop cleanly instead of dereferencing
# freed memory.  Success = the cluster never reinitializes.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

if (($ENV{enable_injection_points} // 'no') ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

my $node = PostgreSQL::Test::Cluster->new('ckpt_drop');
$node->init;
$node->append_conf('postgresql.conf', <<'CONF');
shared_buffers = 256MB
checkpoint_timeout = 1h
CONF
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION injection_points;');

# A dynamic pool with a table full of dirty buffers -> the checkpointer's
# dynamic-pool write pass will have work to do for this pool.
$node->safe_psql('postgres', q{
	CREATE BUFFER POOL cp HANDLER clock_pool_handler SIZE '8388608';
	CREATE TABLE d (id int primary key, pad text) WITH (buffer_pool='cp');
	INSERT INTO d SELECT g, repeat('x', 80) FROM generate_series(1, 200000) g;
});

# Park the checkpointer inside the dynamic-pool write loop.
$node->safe_psql('postgres',
	"SELECT injection_points_attach('bufsync-dynamic-pool-loop', 'wait');");

# Issue a checkpoint from a background session; it will block at the injection
# point while writing pool "cp"'s dirty buffers.
my $ckpt = $node->background_psql('postgres');
$ckpt->query_until(qr/go/, "\\echo go\nCHECKPOINT;\n");
$node->wait_for_event('checkpointer', 'bufsync-dynamic-pool-loop');
pass('checkpointer parked inside the dynamic-pool write loop');

# Drop the table so its buffers are no longer pinned, then drop the pool.  The
# DROP emits PROCSIGNAL_BARRIER_BUFPOOL_DETACH; while the checkpointer is parked
# it cannot process the barrier, so WaitForProcSignalBarrier in the dropper
# would block -- run the DROP in another background session so the test can
# proceed to wake the checkpointer.
$node->safe_psql('postgres', 'DROP TABLE d;');
my $dropper = $node->background_psql('postgres');
$dropper->query_until(qr/dropping/,
	"\\echo dropping\nDROP BUFFER POOL cp;\n");

# Give the dropper a moment to mark the pool inactive and start waiting on the
# barrier, then wake the checkpointer.  Its next loop iteration must observe
# bp_active = false (or a detached mapping) and stop -- no use-after-free.
select(undef, undef, undef, 0.3);
$node->safe_psql('postgres',
	"SELECT injection_points_wakeup('bufsync-dynamic-pool-loop');");

# Both sessions should complete; the cluster must stay up.
$ckpt->query_safe('SELECT 1;');
$dropper->query_safe('SELECT 1;');
$ckpt->quit;
$dropper->quit;

ok($node->poll_query_until('postgres', 'SELECT 1;', '1'),
	'checkpointer and cluster alive after DROP raced the checkpoint');
is( $node->safe_psql('postgres',
		"SELECT count(*) FROM pg_bufferpool WHERE bpname = 'cp';"),
	'0', 'pool dropped');

$node->safe_psql('postgres',
	"SELECT injection_points_detach('bufsync-dynamic-pool-loop');");

my $log = slurp_file($node->logfile);
unlike($log, qr/was terminated by signal|Segmentation fault|PANIC|reinitializing/,
	'no checkpointer crash / cluster reinit in the server log');

# ---------------------------------------------------------------------------
# ABA variant: while the checkpointer is parked mid-loop, DROP the pool AND
# CREATE a new one in the same descriptor slot before waking it.  The
# checkpointer counted the OLD pool's dirty buffers; the slot now serves a
# DIFFERENT pool (new OID).  Writing with the stale buf_ids/counts would index
# past the new pool's descriptor array -- a use-after-free.  The OID guard must
# make the checkpointer skip the recycled slot.
# ---------------------------------------------------------------------------
$node->safe_psql('postgres', q{
	CREATE BUFFER POOL cp HANDLER clock_pool_handler SIZE '8388608';
	CREATE TABLE d (id int primary key, pad text) WITH (buffer_pool='cp');
	INSERT INTO d SELECT g, repeat('x', 80) FROM generate_series(1, 200000) g;
});
$node->safe_psql('postgres',
	"SELECT injection_points_attach('bufsync-dynamic-pool-loop', 'wait');");

my $ckpt2 = $node->background_psql('postgres');
$ckpt2->query_until(qr/go/, "\\echo go\nCHECKPOINT;\n");
$node->wait_for_event('checkpointer', 'bufsync-dynamic-pool-loop');

# Drop the counted pool and immediately recreate one of the same name (new OID)
# in the freed slot, dirtying it again, then wake the parked checkpointer.
$node->safe_psql('postgres', 'DROP TABLE d; DROP BUFFER POOL cp;');
$node->safe_psql('postgres', q{
	CREATE BUFFER POOL cp HANDLER clock_pool_handler SIZE '8388608';
	CREATE TABLE d (id int primary key, pad text) WITH (buffer_pool='cp');
	INSERT INTO d SELECT g, repeat('x', 80) FROM generate_series(1, 200000) g;
});
$node->safe_psql('postgres',
	"SELECT injection_points_wakeup('bufsync-dynamic-pool-loop');");
$ckpt2->query_safe('SELECT 1;');
$ckpt2->quit;

ok($node->poll_query_until('postgres', 'SELECT 1;', '1'),
	'checkpointer survives a pool slot recycled (ABA) mid-checkpoint');
$node->safe_psql('postgres',
	"SELECT injection_points_detach('bufsync-dynamic-pool-loop');");

$log = slurp_file($node->logfile);
unlike($log, qr/was terminated by signal|Segmentation fault|PANIC|reinitializing/,
	'no crash after pool-slot ABA during checkpoint');

# Clean restart proves no shared-memory corruption / orphaned DSM.
$node->restart;
is($node->safe_psql('postgres', 'SELECT 1;'), '1',
	'clean restart after checkpoint/DROP race');

$node->stop;
done_testing();

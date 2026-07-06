# Copyright (c) 2022-2026, PostgreSQL Global Development Group
#
# Isolation/injection test (plan R5, scenario 2): pool RESIZE under load.
#
# ALTER BUFFER POOL name SET SIZE is a barrier-quiesced destroy-and-recreate
# (ResizeDynamicBufferPool -> DestroyDynamicBufferPool -> CreateDynamicBufferPool).
# The destroy half emits PROCSIGNAL_BARRIER_BUFPOOL_DETACH and waits for every
# backend to drop the pool before the old DSM/reservation is freed and the new
# (larger/smaller) one is mapped.
#
# 006_algorithm_swap already proves the swap variant of this destroy+recreate
# with an idle reader.  This test proves the RESIZE variant with a reader that
# is driven WHILE the resize is PARKED mid-teardown at the
# bufpool-destroy-before-quiesce injection point -- the precise interleaving a
# quiescence bug needs: a backend actively touching the pool's DSM at the
# instant the resize would otherwise free it.  If the barrier were missing the
# concurrent reader would SIGSEGV/SIGBUS on the freed mapping when the resize
# resumes.
#
# It also proves resize correctness: grow then shrink, with reads/writes
# through each new size, and a clean restart afterward (no shared-memory
# corruption from the destroy/recreate cycle).

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

if (($ENV{enable_injection_points} // 'no') ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->append_conf('postgresql.conf',
	"shared_preload_libraries = 'pg_bp_lru'");
$node->start;

$node->safe_psql('postgres', 'CREATE EXTENSION injection_points;');
$node->safe_psql('postgres', 'CREATE EXTENSION pg_bp_lru;');

# 8MB pool + a table routed to it, populated so buffers are cached.
$node->safe_psql('postgres', <<'SQL');
CREATE BUFFER POOL rz_pool HANDLER lru_pool_handler SIZE '8388608';
CREATE TABLE t_rz (id int primary key, pad text) WITH (buffer_pool = 'rz_pool');
INSERT INTO t_rz SELECT g, repeat('r', 200) FROM generate_series(1, 20000) g;
SQL
is($node->safe_psql('postgres', 'SELECT count(*) FROM t_rz;'),
	'20000', 'pool usable before resize');

# --- Grow while a reader is parked concurrently at the teardown barrier. ---

# A long-lived reader attached to the pool DSM; it stays alive across the
# whole resize and must survive the mapping being freed and remapped.
my $reader = $node->background_psql('postgres');
is($reader->query_safe('SELECT count(*) FROM t_rz;'), '20000',
	'concurrent reader attached to pool before resize');

# Park the resize's teardown before quiescence.
$node->safe_psql('postgres',
	"SELECT injection_points_attach('bufpool-destroy-before-quiesce', 'wait');"
);

# Start the grow (8MB -> 24MB) in the background; it parks mid-teardown.
my $resizer = $node->background_psql('postgres');
$resizer->query_until(qr/start_resize/,
	"\\echo start_resize\nALTER BUFFER POOL rz_pool SET SIZE '25165824';\n");

$node->wait_for_event('client backend', 'bufpool-destroy-before-quiesce');
pass('RESIZE parked at teardown injection point (before quiescence)');

# While the resize is parked (old mapping still live, not yet freed), drive
# more traffic from the concurrent reader against the pool DSM.
$reader->query_safe('SELECT count(*) FROM t_rz;');

# Release the teardown -> barrier fires, reader must have detached, then the
# old mapping is freed and the new 24MB one is created.
$node->safe_psql('postgres',
	"SELECT injection_points_wakeup('bufpool-destroy-before-quiesce');");

# The resize completes.
$resizer->query_safe('SELECT 1;');
ok($node->poll_query_until('postgres', 'SELECT 1;', '1'),
	'server alive after barrier-quiesced resize');

# Reader survived the mapping being freed + remapped (no use-after-detach).
is($reader->query_safe('SELECT 55;'), '55',
	'concurrent reader survived resize teardown+recreate');

# Detach the injection point; remaining resizes run without parking.
$node->safe_psql('postgres',
	"SELECT injection_points_detach('bufpool-destroy-before-quiesce');");

# Catalog reflects the new size (24MB).
is( $node->safe_psql('postgres',
		"SELECT bpsize FROM pg_bufferpool WHERE bpname = 'rz_pool';"),
	'25165824', 'catalog size updated after grow');

# Pool works at the new (larger) size; cache repopulates from disk.
is($reader->query_safe('SELECT count(*) FROM t_rz;'), '20000',
	'pool usable at grown size (reader)');
$reader->query_safe(
	"UPDATE t_rz SET pad = repeat('g', 200) WHERE id <= 10000;");
is( $node->safe_psql('postgres',
		"SELECT count(*) FROM t_rz WHERE pad LIKE 'g%';"),
	'10000', 'writes through grown pool visible');

$reader->quit;
$resizer->quit;

# --- Shrink under concurrent readers (no injection park; plain load). ---
my @bg;
for my $c (1 .. 4)
{
	my $h = $node->background_psql('postgres');
	$h->query_safe('SELECT count(*) FROM t_rz;');
	push @bg, $h;
}
$node->safe_psql('postgres', "ALTER BUFFER POOL rz_pool SET SIZE '4194304';");
is( $node->safe_psql('postgres', 'SELECT count(*) FROM t_rz;'),
	'20000', 'pool correct after shrink under concurrent readers');
is( $node->safe_psql('postgres',
		"SELECT bpsize FROM pg_bufferpool WHERE bpname = 'rz_pool';"),
	'4194304', 'catalog size updated after shrink');
$_->query_safe('SELECT 1;') for @bg;    # concurrent readers survived
$_->quit for @bg;

# A no-op resize (same size) is accepted and does not tear down the pool.
$node->safe_psql('postgres', "ALTER BUFFER POOL rz_pool SET SIZE '4194304';");
is( $node->safe_psql('postgres', 'SELECT count(*) FROM t_rz;'),
	'20000', 'pool intact after no-op resize');

# Clean up + restart proves no shared-memory corruption from the resizes.
$node->safe_psql('postgres', 'DROP TABLE t_rz; DROP BUFFER POOL rz_pool;');
$node->restart;
is($node->safe_psql('postgres', 'SELECT 1;'), '1',
	'clean restart after resizes');

$node->stop;
done_testing();

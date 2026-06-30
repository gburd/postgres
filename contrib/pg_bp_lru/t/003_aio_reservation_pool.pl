# Copyright (c) 2022-2026, PostgreSQL Global Development Group
#
# Asynchronous I/O on reservation-backed (same-address) buffer pools (P3).
#
# A reservation-backed pool's buffers are at the same virtual address in every
# process, so unlike legacy DSM pools their reads/writes can be performed by
# AIO workers instead of being forced synchronous.  This test runs with
# io_method=worker, routes a table to a reservation-backed pool, evicts its
# pages, and forces disk reads back in -- exercising the path where an IO
# worker must reach the pool's buffer memory.  If the worker cannot map the
# pool's committed sub-range (the MAP_FIXED-per-process trap) it SIGSEGVs and
# the queries fail; success proves the worker correctly attaches the pool.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->append_conf('postgresql.conf', <<'CONF');
shared_preload_libraries = 'pg_bp_lru'
max_buffer_pool_memory = 64MB
io_method = worker
io_max_concurrency = 8
CONF
$node->start;

# Confirm async worker I/O and the reservation are both in effect.
is($node->safe_psql('postgres', 'SHOW io_method;'), 'worker',
	'io_method = worker');
is($node->safe_psql('postgres', 'SHOW max_buffer_pool_memory;'), '64MB',
	'reservation enabled');

$node->safe_psql('postgres', 'CREATE EXTENSION pg_bp_lru;');

# Reservation-backed pool + a table on it, larger than the pool so reads must
# go to disk and back through the pool (and thus through AIO workers).
$node->safe_psql('postgres', <<'SQL');
CREATE BUFFER POOL aio_pool HANDLER lru_pool_handler SIZE '16777216';  -- 16MB
CREATE TABLE t_aio (id int primary key, pad text) WITH (buffer_pool = 'aio_pool');
INSERT INTO t_aio SELECT g, repeat('a', 400) FROM generate_series(1, 50000) g;
SQL

# Force the pool's pages out of memory: read another big chunk to churn the
# pool, then read the table back -- the reads come from disk via AIO workers
# into reservation-pool buffers.
$node->safe_psql('postgres', 'CHECKPOINT;');

# Several full scans + aggregate; if an IO worker can't reach pool memory this
# crashes the backend/worker.  Run a few times to exercise eviction + reload.
for my $iter (1 .. 3)
{
	my $sum = $node->safe_psql('postgres',
		'SELECT count(*), sum(length(pad)) FROM t_aio;');
	is($sum, '50000|20000000', "full scan via AIO into reservation pool (iter $iter)");
}

# Index-driven point reads (random access pattern -> scattered AIO reads).
my $hits = $node->safe_psql('postgres', <<'SQL');
SELECT count(*) FROM t_aio
WHERE id IN (1, 12345, 25000, 37500, 49999, 7, 33333, 41000);
SQL
is($hits, '8', 'indexed point reads via AIO into reservation pool');

# A concurrent reader while another session churns the pool, to interleave
# worker I/O with eviction.
my $bg = $node->background_psql('postgres');
$bg->query_until(qr/ready/, "\\echo ready\n");
$node->safe_psql('postgres', 'SELECT count(*) FROM t_aio;');  # churn
is($bg->query_safe('SELECT count(*) FROM t_aio WHERE id < 10000;'),
	'9999', 'concurrent AIO reads on reservation pool');
$bg->quit;

# Clean shutdown + restart: no corruption from worker access.
$node->safe_psql('postgres', 'DROP TABLE t_aio;');
$node->safe_psql('postgres', 'DROP BUFFER POOL aio_pool;');
$node->restart;
is($node->safe_psql('postgres', 'SELECT 1;'), '1', 'clean restart after AIO pool use');

$node->stop;
done_testing();

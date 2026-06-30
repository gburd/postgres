# Copyright (c) 2022-2026, PostgreSQL Global Development Group
#
# End-to-end test for reservation-backed (same-address) buffer pools.
#
# With max_buffer_pool_memory set, a CREATE BUFFER POOL allocates its memory
# as a committed sub-range of the address-space reservation rather than a
# private DSM segment.  This test proves the pool is usable across backends
# that were forked BEFORE the pool existed (the case that needs per-backend
# re-mapping of the committed sub-range), survives reads/writes from several
# sessions, and tears down cleanly under the quiescence barrier.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->append_conf('postgresql.conf',
	"shared_preload_libraries = 'pg_bp_lru'");
# Enable same-address pools: reserve 64MB of address space at startup.
$node->append_conf('postgresql.conf', 'max_buffer_pool_memory = 64MB');
$node->start;

# Confirm the reservation is in effect.
is($node->safe_psql('postgres', 'SHOW max_buffer_pool_memory;'),
	'64MB', 'reservation enabled');

$node->safe_psql('postgres', 'CREATE EXTENSION pg_bp_lru;');

# A backend opened BEFORE the pool exists.  When it later touches the pooled
# table it must re-map the committed sub-range in its own address space; this
# is the path that SIGSEGVs if attach-on-access is missing.
my $pre = $node->background_psql('postgres');
is($pre->query_safe('SELECT 1;'), '1', 'pre-existing backend alive');

# Create a reservation-backed pool and a table on it, then load data from a
# DIFFERENT (post-pool) session.
$node->safe_psql('postgres', <<'SQL');
CREATE BUFFER POOL resv_pool HANDLER lru_pool_handler SIZE '8388608';
CREATE TABLE t_resv (id int primary key, pad text) WITH (buffer_pool = 'resv_pool');
INSERT INTO t_resv SELECT g, repeat('y', 200) FROM generate_series(1, 3000) g;
SQL

# The pre-existing backend now reads the pooled table -> exercises
# attach-on-access re-mapping of the committed sub-range.
is($pre->query_safe('SELECT count(*) FROM t_resv;'),
	'3000', 'pre-existing backend reads reservation-backed pool');

# Write traffic through the pool from the pre-existing backend.
$pre->query_safe('UPDATE t_resv SET pad = repeat(\'z\', 200) WHERE id <= 1500;');
is($pre->query_safe('SELECT count(*) FROM t_resv WHERE pad LIKE \'z%\';'),
	'1500', 'writes through reservation-backed pool visible');

# A fresh session also reads correctly (post-pool fork path).
is($node->safe_psql('postgres', 'SELECT count(*) FROM t_resv;'),
	'3000', 'fresh backend reads reservation-backed pool');

# Drop the table dependency, then drop the pool (decommit + barrier).
$node->safe_psql('postgres', 'DROP TABLE t_resv;');
$node->safe_psql('postgres', 'DROP BUFFER POOL resv_pool;');

# Pre-existing backend still usable after the pool's sub-range was
# decommitted (its stale mapping must not be touched again; barrier dropped
# its CurrentBufferPool).
is($pre->query_safe('SELECT 7;'), '7',
	'pre-existing backend survives pool decommit');

$pre->quit;

# A second create/drop cycle reuses the freed reservation extent.
$node->safe_psql('postgres', <<'SQL');
CREATE BUFFER POOL resv_pool2 HANDLER lru_pool_handler SIZE '8388608';
CREATE TABLE t_resv2 (id int) WITH (buffer_pool = 'resv_pool2');
INSERT INTO t_resv2 SELECT generate_series(1, 500);
SQL
is($node->safe_psql('postgres', 'SELECT count(*) FROM t_resv2;'),
	'500', 'reused reservation extent works');
$node->safe_psql('postgres', 'DROP TABLE t_resv2;');
$node->safe_psql('postgres', 'DROP BUFFER POOL resv_pool2;');

# Clean restart proves no shared-memory corruption.
$node->restart;
is($node->safe_psql('postgres', 'SELECT 1;'), '1', 'clean restart');

$node->stop;
done_testing();

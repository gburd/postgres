# Copyright (c) 2022-2026, PostgreSQL Global Development Group
#
# Per-pool huge pages (P4): CREATE BUFFER POOL ... WITH (huge_pages 'on').
#
# A reservation-backed pool can request huge pages for its committed
# sub-range.  Where huge pages are unavailable (e.g. HugePages_Total = 0 in
# CI), BufPoolCommit transparently falls back to normal pages, so the pool is
# still created and usable.  This test verifies the option is accepted, the
# pool works either way, and that an unknown option is rejected.

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
CONF
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION pg_bp_lru;');

# huge_pages = on: created (with or without actual huge pages) and usable.
$node->safe_psql('postgres', <<'SQL');
CREATE BUFFER POOL huge_pool HANDLER lru_pool_handler SIZE '8388608'
  WITH (huge_pages 'on');
CREATE TABLE t_huge (id int) WITH (buffer_pool = 'huge_pool');
INSERT INTO t_huge SELECT generate_series(1, 1000);
SQL
is($node->safe_psql('postgres', 'SELECT count(*) FROM t_huge;'),
	'1000', 'pool with huge_pages=on is usable (huge or fallback)');

# huge_pages = off: explicit normal pages.
$node->safe_psql('postgres', <<'SQL');
CREATE BUFFER POOL norm_pool HANDLER lru_pool_handler SIZE '8388608'
  WITH (huge_pages 'off');
CREATE TABLE t_norm (id int) WITH (buffer_pool = 'norm_pool');
INSERT INTO t_norm SELECT generate_series(1, 1000);
SQL
is($node->safe_psql('postgres', 'SELECT count(*) FROM t_norm;'),
	'1000', 'pool with huge_pages=off is usable');

# Unknown option is rejected.
my ($ret, $stdout, $stderr) = $node->psql('postgres',
	"CREATE BUFFER POOL bad_pool HANDLER lru_pool_handler SIZE '8388608' WITH (bogus 'on');"
);
isnt($ret, 0, 'unknown buffer pool option is rejected');
like($stderr, qr/unrecognized buffer pool option "bogus"/,
	'unknown option error message');

# Clean up and restart.
$node->safe_psql('postgres', 'DROP TABLE t_huge; DROP TABLE t_norm;');
$node->safe_psql('postgres', 'DROP BUFFER POOL huge_pool; DROP BUFFER POOL norm_pool;');
$node->restart;
is($node->safe_psql('postgres', 'SELECT 1;'), '1', 'clean restart');

$node->stop;
done_testing();

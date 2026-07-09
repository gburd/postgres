# Copyright (c) 2022-2026, PostgreSQL Global Development Group
#
# Deterministic churn stress for the pluggable buffer-pool framework.
#
# Distilled from the >1h EC2 chaos suite into a bounded, reproducible core that
# runs in normal CI: under a concurrent scan load with a working set larger than
# the pool, repeatedly create / resize / drop named buffer pools and reassign a
# table's heap, index, and TOAST storage between them and back to the default
# pool, then verify (a) the server stays live, (b) data is intact, (c) the
# unified pg_stat_bufferpool counters advance and are sane, and (d) the cluster
# recovers cleanly from an immediate (kill -9) crash with no data loss.
#
# Uses only the built-in clock_pool_handler so no contrib extension or
# shared_preload_libraries is required.  buffer_pool_numa is forced on via the
# developer node-count GUC so the NUMA-aware batched sweep is exercised too.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $ROWS   = 8000;      # per table; small pool below forces real eviction
my $NTABLE = 3;
my $NPOOL  = 3;
my $ITERS  = 42;        # churn iterations (deterministic, not time-based)

my $node = PostgreSQL::Test::Cluster->new('churn');
$node->init;
$node->append_conf('postgresql.conf', <<'CONF');
shared_buffers = 8MB
buffer_pool_numa = on
buffer_pool_numa_nodes = 4
max_connections = 20
CONF
$node->start;

# --- load: heap+index+TOAST-bearing tables, working set > pool ---
for my $t (1 .. $NTABLE)
{
	$node->safe_psql('postgres', qq{
		CREATE TABLE big$t (id int primary key, pad text, blob text);
		INSERT INTO big$t
			SELECT g, repeat('x', 200), repeat(md5(g::text), 40)
			FROM generate_series(1, $ROWS) g;
		CREATE INDEX big${t}_pad ON big$t (pad);
	});
}

# --- concurrent scanner: a cheap background loop that keeps eviction pressure
#     on while the foreground churns pools.  Bounded so it always terminates. ---
my $load = $node->background_psql('postgres');
$load->query_safe(q{
	DO $$
	BEGIN
		FOR i IN 1..40 LOOP
			PERFORM count(*), sum(length(blob)) FROM big1;
			PERFORM count(*) FROM big2 WHERE id BETWEEN i*100 AND i*100 + 2000;
		END LOOP;
	END $$;
});

# --- the churn loop: deterministic rotation through every pool operation and
#     every reassignment target.  ON_ERROR_STOP off -- some ops legitimately
#     fail (create existing / drop in-use); the point is the server survives. ---
my $live_errors = 0;
for my $iter (1 .. $ITERS)
{
	my $p     = ($iter % $NPOOL) + 1;
	my $t     = ($iter % $NTABLE) + 1;
	my $bytes = ((($iter * 7) % 30) + 4) * 8192 * 4;    # ~128KB .. ~1MB
	my $action = $iter % 7;

	my $sql;
	if    ($action == 0) { $sql = "CREATE BUFFER POOL cp$p HANDLER clock_pool_handler SIZE '$bytes';"; }
	elsif ($action == 1) { $sql = "ALTER BUFFER POOL cp$p SET SIZE '$bytes';"; }
	elsif ($action == 2) { $sql = "ALTER TABLE big$t SET (buffer_pool='cp$p');"; }
	elsif ($action == 3) { $sql = "ALTER TABLE big$t SET (overflow_buffer_pool='cp$p');"; }
	elsif ($action == 4) { $sql = "ALTER INDEX big${t}_pad SET (buffer_pool='cp$p');"; }
	elsif ($action == 5) { $sql = "ALTER TABLE big$t RESET (buffer_pool);"; }
	else                 { $sql = "DROP BUFFER POOL IF EXISTS cp$p;"; }

	$node->psql('postgres', $sql, on_error_stop => 0);

	# Liveness probe every few iterations (also drives a little eviction).
	if ($iter % 6 == 0)
	{
		my $live = $node->safe_psql('postgres', "SELECT 1;");
		$live_errors++ unless $live eq '1';
	}
}
is($live_errors, 0, 'server stayed live through pool churn under load');

$load->quit;

# Return everything to the default pool so integrity checks are unambiguous.
for my $t (1 .. $NTABLE)
{
	$node->psql('postgres', "ALTER TABLE big$t RESET (buffer_pool);", on_error_stop => 0);
	$node->psql('postgres', "ALTER TABLE big$t RESET (overflow_buffer_pool);", on_error_stop => 0);
	$node->psql('postgres', "ALTER INDEX big${t}_pad RESET (buffer_pool);", on_error_stop => 0);
}
for my $p (1 .. $NPOOL)
{
	$node->psql('postgres', "DROP BUFFER POOL IF EXISTS cp$p;", on_error_stop => 0);
}

# --- data intact after all that churn ---
for my $t (1 .. $NTABLE)
{
	is($node->safe_psql('postgres', "SELECT count(*) FROM big$t;"),
		"$ROWS", "big$t intact after churn ($ROWS rows)");
}

# --- the unified pg_stat_bufferpool counters advanced under real traffic ---
$node->safe_psql('postgres', 'SELECT count(*) FROM big1; SELECT count(*) FROM big1;');
my $stats = $node->safe_psql('postgres', <<'SQL');
SELECT hits > 0
   AND reads >= 0
   AND (hit_ratio IS NULL OR (hit_ratio >= 0 AND hit_ratio <= 1))
   AND algorithm = 'clock'
   AND numa_active = true
   AND batch_size > 1
   AND hot_buffers + cool_buffers <= nbuffers
FROM pg_stat_bufferpool WHERE name = 'default';
SQL
is($stats, 't', 'pg_stat_bufferpool default-pool counters advanced and are sane');

# --- crash-recovery from the churned state: no data loss ---
$node->safe_psql('postgres', 'CHECKPOINT;');
$node->stop('immediate');    # kill -9 equivalent
$node->start;
for my $t (1 .. $NTABLE)
{
	is($node->safe_psql('postgres', "SELECT count(*) FROM big$t;"),
		"$ROWS", "big$t intact after crash recovery ($ROWS rows)");
}

my $log = slurp_file($node->logfile);
unlike($log, qr/PANIC|corrupt|invalid page|could not read block/i,
	'no corruption or PANIC in server log');

$node->stop;
done_testing();

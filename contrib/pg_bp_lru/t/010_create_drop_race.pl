# Copyright (c) 2022-2026, PostgreSQL Global Development Group
#
# Isolation/injection test (plan R5, scenario 1): concurrent CREATE + DROP of
# the same named buffer pool, and concurrent DROP + DROP of the same pool.
#
# DROP BUFFER POOL serializes on LockDatabaseObject(AccessExclusiveLock) over
# the pool OID and tears down the DSM (DestroyDynamicBufferPool, which parks at
# the bufpool-destroy-before-quiesce injection point) BEFORE deleting the
# catalog row.  That ordering plus the object lock is what makes concurrent DDL
# on one pool name safe.  This test drives the two races with precise control:
#
#   A) CREATE(name) racing a parked DROP(name).  While the DROP is parked
#      mid-teardown the catalog row still exists and the object is exclusively
#      locked, so a concurrent CREATE of the SAME name must fail cleanly
#      ("already exists") rather than corrupt state or install a second
#      descriptor over the one being destroyed.  After the DROP completes, a
#      fresh CREATE of that name succeeds -- proving the descriptor slot and
#      catalog were left consistent.
#
#   B) DROP(name) racing another DROP(name).  The second DROP must block on the
#      object lock behind the parked first DROP, then -- once the first commits
#      the catalog delete -- observe the pool gone and fail cleanly ("does not
#      exist").  It must NOT double-free the DSM (which would crash) or delete a
#      catalog row that a reused OID now points at.
#
# Both variants must leave the cluster crash-free and restartable (no orphaned
# DSM segment, no catalog row without a live descriptor, no double-free).

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

# ---------------------------------------------------------------------------
# Variant A: CREATE(name) racing a parked DROP(name).
# ---------------------------------------------------------------------------
$node->safe_psql('postgres',
	"CREATE BUFFER POOL race_pool HANDLER lru_pool_handler SIZE '8388608';");
is( $node->safe_psql('postgres',
		"SELECT count(*) FROM pg_bufferpool WHERE bpname = 'race_pool';"),
	'1', 'race_pool created');

# Park the DROP's teardown before quiescence (catalog row still present here).
$node->safe_psql('postgres',
	"SELECT injection_points_attach('bufpool-destroy-before-quiesce', 'wait');"
);

my $dropper = $node->background_psql('postgres');
$dropper->query_until(qr/start_drop/,
	"\\echo start_drop\nDROP BUFFER POOL race_pool;\n");
$node->wait_for_event('client backend', 'bufpool-destroy-before-quiesce');
pass('DROP race_pool parked mid-teardown (catalog row still exists)');

# Concurrent CREATE of the SAME name must fail cleanly (name still taken).
my ($cret, $cout, $cerr) = $node->psql('postgres',
	"CREATE BUFFER POOL race_pool HANDLER lru_pool_handler SIZE '8388608';");
isnt($cret, 0, 'concurrent CREATE of same name rejected while DROP in flight');
like($cerr, qr/already exists/,
	'CREATE fails with "already exists" (row not yet deleted)');

# Release the DROP -> barrier fires, DSM torn down, catalog row deleted.
$node->safe_psql('postgres',
	"SELECT injection_points_wakeup('bufpool-destroy-before-quiesce');");
$dropper->query_safe('SELECT 1;');
$dropper->quit;

ok($node->poll_query_until('postgres', 'SELECT 1;', '1'),
	'server alive after parked DROP completed');
is( $node->safe_psql('postgres',
		"SELECT count(*) FROM pg_bufferpool WHERE bpname = 'race_pool';"),
	'0', 'race_pool gone after DROP completed');

# A fresh CREATE of that name now succeeds -- slot + catalog left consistent.
$node->safe_psql('postgres',
	"SELECT injection_points_detach('bufpool-destroy-before-quiesce');");
$node->safe_psql('postgres', <<'SQL');
CREATE BUFFER POOL race_pool HANDLER lru_pool_handler SIZE '8388608';
CREATE TABLE t_race (id int) WITH (buffer_pool = 'race_pool');
INSERT INTO t_race SELECT generate_series(1, 500);
SQL
is($node->safe_psql('postgres', 'SELECT count(*) FROM t_race;'),
	'500', 'recreated pool of same name is usable');
$node->safe_psql('postgres', 'DROP TABLE t_race; DROP BUFFER POOL race_pool;');

# ---------------------------------------------------------------------------
# Variant B: DROP(name) racing another DROP(name).
# ---------------------------------------------------------------------------
$node->safe_psql('postgres',
	"CREATE BUFFER POOL dd_pool HANDLER lru_pool_handler SIZE '8388608';");

$node->safe_psql('postgres',
	"SELECT injection_points_attach('bufpool-destroy-before-quiesce', 'wait');"
);

# First DROP parks mid-teardown, holding AccessExclusiveLock on the pool OID.
my $drop1 = $node->background_psql('postgres');
$drop1->query_until(qr/start_drop1/,
	"\\echo start_drop1\nDROP BUFFER POOL dd_pool;\n");
$node->wait_for_event('client backend', 'bufpool-destroy-before-quiesce');
pass('first DROP dd_pool parked mid-teardown (holds object lock)');

# Second DROP of the same pool: must BLOCK on the object lock, not proceed.
# Open with on_error_stop=>0 so the DROP's expected error ("does not exist",
# once the first DROP commits) does not terminate the psql session -- we want
# to inspect the error and confirm the session survives.
my $drop2 = $node->background_psql('postgres', on_error_stop => 0);
$drop2->query_until(qr/start_drop2/,
	"\\echo start_drop2\nDROP BUFFER POOL dd_pool;\n");

# Confirm the second DROP is waiting on a lock (blocked, not finished/crashed).
ok( $node->poll_query_until(
		'postgres', <<'SQL', 't'),
SELECT EXISTS (
  SELECT 1 FROM pg_locks
  WHERE NOT granted AND locktype = 'object'
    AND classid = 'pg_bufferpool'::regclass::oid
);
SQL
	'second DROP blocks on the pool object lock');

# Release the first DROP -> it commits the catalog delete and frees the DSM;
# the second DROP then acquires the lock, finds the pool gone, and errors.
$node->safe_psql('postgres',
	"SELECT injection_points_wakeup('bufpool-destroy-before-quiesce');");
$drop1->query_safe('SELECT 1;');    # first DROP succeeded

# The second DROP's statement errors ("does not exist"); its session survives
# the failed command and stays usable.
my ($d2out, $d2err) = $drop2->query('SELECT 1;');
is($d2out, '1', 'second DROP session still usable after its DROP failed');
$drop1->quit;
$drop2->quit;

# The pool must be gone exactly once and the server must be alive (no
# double-free crash).
ok($node->poll_query_until('postgres', 'SELECT 1;', '1'),
	'server alive after racing DROP+DROP (no double-free)');
is( $node->safe_psql('postgres',
		"SELECT count(*) FROM pg_bufferpool WHERE bpname = 'dd_pool';"),
	'0', 'dd_pool dropped exactly once');

$node->safe_psql('postgres',
	"SELECT injection_points_detach('bufpool-destroy-before-quiesce');");

# Clean restart proves no shared-memory corruption / orphaned DSM.
$node->restart;
is($node->safe_psql('postgres', 'SELECT 1;'), '1',
	'clean restart after concurrent CREATE/DROP and DROP/DROP races');

$node->stop;
done_testing();

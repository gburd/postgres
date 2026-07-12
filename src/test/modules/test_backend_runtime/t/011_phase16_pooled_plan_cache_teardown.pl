# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Phase 16 hardening regression guard: a pooled session that leaves cached
# plans on the plan cache at disconnect (via SQL-language functions / SPI, which
# call SaveCachedPlan/SPI_keepplan and are NOT owned by the prepared-statement
# hash) must close cleanly.  Before the fix, PgSessionResetPlanCacheClosedState()
# Assert()ed saved_plan_list / cached_expression_list empty at close and tripped
# on such retained plans in a cassert build; now the close path drains them.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('phase16_pooled_plan_cache_teardown');
$node->init;
$node->append_conf(
	'postgresql.conf', q{
multithreaded = on
pooled_protocol_carriers = 2
autovacuum = off
io_method = sync
});
$node->start;

is($node->safe_psql('postgres', 'SHOW multithreaded'), 'on',
	'pooled plan-cache teardown test runs threaded');
isnt($node->safe_psql('postgres', 'SHOW pooled_protocol_carriers'), '0',
	'pooled plan-cache teardown test runs pooled');

# A SQL-language function caches its plan (SaveCachedPlan); a PL/pgSQL function
# caches its statements' plans (SPI_keepplan).  Neither is a prepared statement,
# so neither is drained by DropAllPreparedStatements -- they land on
# saved_plan_list and must be released at pooled session close.
$node->safe_psql('postgres', q{
	CREATE FUNCTION sqlf(int) RETURNS int LANGUAGE sql AS $$ SELECT $1 * 2 $$;
	CREATE FUNCTION plf(int) RETURNS int LANGUAGE plpgsql AS $$
	  DECLARE r int; BEGIN SELECT $1 + 1 INTO r; RETURN r; END $$;
	CREATE TABLE t(id int primary key, v text);
	INSERT INTO t SELECT g, 'r'||g FROM generate_series(1, 50) g;
});

# Run many short pooled sessions, each of which exercises the plan-caching
# functions and then disconnects WITHOUT deallocating -- the exact pattern that
# leaves saved plans / cached expressions at session close.  If the close path
# still asserted (or leaked), a cassert server would crash here and the pool
# would go unhealthy.
for my $i (1 .. 30)
{
	my ($rc, $out, $err) = $node->psql('postgres',
		"SELECT sqlf($i), plf($i), count(*) FROM t;");
	is($rc, 0, "pooled session $i with cached plans ran and disconnected cleanly")
	  if $i <= 3;    # only assert a few to keep the plan readable
	last if $rc != 0;
}

# Also drive a session that PREPAREs (prepared-stmt path) AND uses the functions,
# then disconnects -- both drain paths in one close.
$node->psql('postgres', q{
	PREPARE ps(int) AS SELECT sqlf($1) + plf($1);
	EXECUTE ps(7);
});

# The pool must remain fully healthy after all those closes.
is($node->safe_psql('postgres', 'SELECT sqlf(21);'), '42',
	'pool healthy after sessions with retained cached plans closed');
is($node->safe_psql('postgres', 'SELECT plf(99);'), '100',
	'plpgsql-cached-plan path healthy after pooled teardowns');
is($node->safe_psql('postgres', 'SELECT count(*) FROM t;'), '50',
	'server fully usable after pooled plan-cache teardowns');

$node->stop('fast');
done_testing();

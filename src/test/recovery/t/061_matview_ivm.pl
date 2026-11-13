
# Copyright (c) 2026, PostgreSQL Global Development Group

# Incrementally maintained materialized views and recovery.  The view's
# rows are written in the same transaction as the base-table change, so a
# crash in the middle of maintaining them loses both, and a standby sees
# both together.
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

my $node_primary = PostgreSQL::Test::Cluster->new('primary');
$node_primary->init(allows_streaming => 1);
$node_primary->append_conf('postgresql.conf', 'restart_after_crash = on');
$node_primary->start;

if (!$node_primary->check_extension('injection_points'))
{
	plan skip_all => 'Extension injection_points not installed';
}

$node_primary->safe_psql(
	'postgres', q{
	CREATE EXTENSION injection_points;
	CREATE TABLE ivm_t (k int, v int);
	INSERT INTO ivm_t SELECT g % 10, g FROM generate_series(1, 1000) g;
	CREATE MATERIALIZED VIEW ivm_a WITH (incremental_view_maintenance) AS
	  SELECT k, count(*) AS n, sum(v) AS s FROM ivm_t GROUP BY k;
	CREATE MATERIALIZED VIEW ivm_b WITH (incremental_view_maintenance) AS
	  SELECT count(*) AS n, max(v) AS hi FROM ivm_t;
	CREATE FUNCTION ivm_ok() RETURNS bool LANGUAGE sql AS $$
	  SELECT NOT EXISTS (
	    (SELECT * FROM ivm_a EXCEPT ALL
	     SELECT k, count(*), sum(v) FROM ivm_t GROUP BY k)
	    UNION ALL
	    (SELECT k, count(*), sum(v) FROM ivm_t GROUP BY k
	     EXCEPT ALL SELECT * FROM ivm_a))
	  AND (SELECT (n, hi) FROM ivm_b) =
	      (SELECT (count(*), max(v)) FROM ivm_t)
	$$;
});

my $backup_name = 'my_backup';
$node_primary->backup($backup_name);
my $node_standby = PostgreSQL::Test::Cluster->new('standby');
$node_standby->init_from_backup($node_primary, $backup_name,
	has_streaming => 1);
$node_standby->start;

# A standby sees each committed change and its view rows together.
$node_primary->safe_psql('postgres',
	"INSERT INTO ivm_t SELECT g % 13, g FROM generate_series(1001, 1500) g");
$node_primary->safe_psql('postgres', "DELETE FROM ivm_t WHERE k = 3");
$node_primary->wait_for_replay_catchup($node_standby);
is($node_standby->safe_psql('postgres', "SELECT ivm_ok()"),
	't', 'standby sees maintained views');

# Crash the primary after it has maintained the first view of a commit and
# before it maintains the second.  Neither the base-table change nor the
# first view's new rows survive.
my $before = $node_primary->safe_psql('postgres',
	"SELECT (SELECT sum(n) FROM ivm_a), (SELECT n FROM ivm_b)");
$node_primary->safe_psql('postgres',
	"SELECT injection_points_attach('matview-ivm-maintained', 'wait')");
my $writer = $node_primary->background_psql('postgres', on_error_stop => 0);
$writer->query_until(
	qr/starting_commit/, q(
	\echo starting_commit
	INSERT INTO ivm_t SELECT 99, g FROM generate_series(1, 100) g;
));
$node_primary->wait_for_event('client backend', 'matview-ivm-maintained');

$node_primary->stop('immediate');
$writer->{run}->kill_kill;
$node_primary->start;

is( $node_primary->safe_psql(
		'postgres',
		"SELECT ivm_ok(), (SELECT sum(n) FROM ivm_a), (SELECT n FROM ivm_b), "
		  . "(SELECT count(*) FROM ivm_t WHERE k = 99)"),
	"t|$before|0",
	'crash during maintenance leaves base table and views unchanged');

# Maintenance works after recovery, and the standby follows.
$node_primary->safe_psql('postgres',
	"INSERT INTO ivm_t SELECT 99, g FROM generate_series(1, 100) g");
is($node_primary->safe_psql('postgres', "SELECT ivm_ok()"),
	't', 'views maintained after crash recovery');
$node_primary->wait_for_replay_catchup($node_standby);
is( $node_standby->safe_psql(
		'postgres', "SELECT ivm_ok(), (SELECT n FROM ivm_a WHERE k = 99)"),
	't|100',
	'standby follows after the primary crash');

$node_standby->stop;
$node_primary->stop;

done_testing();

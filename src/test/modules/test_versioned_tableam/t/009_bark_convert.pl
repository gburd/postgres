
# Copyright (c) 2026, PostgreSQL Global Development Group

# The conversion of a tagged multikey BARK index made unique or exclusion
# after its build ("Conversion" in the BARK README): VACUUM's barrier,
# which a manual VACUUM waits in and autovacuum does not, and the
# completion that clears BARK_META_MKTAGS (T12a and T12e of the V2 design),
# under wal_consistency_checking = bark, with a standby that must see the
# meta page's bits and return the primary's MVCC results throughout.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init(allows_streaming => 1);
$node->append_conf(
	'postgresql.conf', q{
autovacuum_naptime = 1s
wal_consistency_checking = 'bark'
});
$node->start;
$node->backup('b');
my $standby = PostgreSQL::Test::Cluster->new('standby');
$standby->init_from_backup($node, 'b', has_streaming => 1);
$standby->start;

$node->safe_psql(
	'postgres', q{
	CREATE EXTENSION test_versioned_tableam;
	CREATE EXTENSION bark_multikey;
	CREATE EXTENSION amcheck;
	CREATE EXTENSION pageinspect;
	CREATE EXTENSION injection_points;
});

# MKTAGS (4), CONVERTING (8) and SETTLED (16) of the index's meta page, on
# `node`
sub bits
{
	my ($n, $idx) = @_;
	return $n->safe_psql('postgres', "SELECT flags & 28 FROM bark_metap('$idx')");
}

# what an MVCC scan of the index returns, on `node`
my $q = q{SET enable_seqscan = off; SET enable_bitmapscan = off;
	SELECT string_agg(id::text, ',' ORDER BY id) FROM tc WHERE doc && '{10,21,30,4010,10000}'};

sub check_standby
{
	my ($what) = @_;
	# VACUUM commits without flushing its meta page's record: wait for it
	$node->wait_for_catchup($standby, 'replay', $node->lsn('insert'));
	is(bits($standby, 'tc_doc'), bits($node, 'tc_doc'),
		"$what: the standby sees the meta page's bits");
	is($standby->safe_psql('postgres', $q), $node->safe_psql('postgres', $q),
		"$what: the standby returns the primary's rows");
	is($standby->safe_psql('postgres', "SELECT bark_index_check('tc_doc')"),
		'', "$what: bark_index_check on the standby");
}

$node->safe_psql(
	'postgres', q{
	CREATE TABLE tc (id int, doc int4[]) USING heap_versioned;
	INSERT INTO tc SELECT g, ARRAY[10 * g, 10 * g + 1] FROM generate_series(1, 400) g;
	CREATE INDEX tc_doc ON tc USING bark (doc bark_int4_array_ops);
	SELECT test_versioned_add_exclusion('tc_doc', '&&(anyarray,anyarray)');
	UPDATE pg_catalog.pg_index SET indisexclusion = true
	  WHERE indexrelid = 'tc_doc'::regclass;
	-- row 3 has history in doc (heap_versioned never untags such a row's
	-- entries), so its tagged entries hold the conversion open until the
	-- row goes
	SELECT test_versioned_mark_history('tc', ctid, 'doc') FROM tc WHERE id = 3;
});
is(bits($node, 'tc_doc'), '4', 'built with MKTAGS');

# A standby's dirty scan of the index sees the constraint but writes
# nothing: the conversion begins on the primary.
$node->wait_for_replay_catchup($standby);
is( $standby->safe_psql(
		'postgres', q{
		SELECT count(*) FROM test_versioned_dirty_scan('tc_doc', '&&(anyarray,anyarray)', '{10}'::int4[])}),
	'1', 'standby: a dirty scan of the converted index');
is(bits($standby, 'tc_doc'), '4', 'standby: CONVERTING not set in recovery');

# An old writer: a transaction that holds the table's RowExclusiveLock from
# before the conversion began.
my $old = $node->background_psql('postgres');
$old->query_safe(q{BEGIN; INSERT INTO tc VALUES (1000, '{10000}')});

$node->safe_psql('postgres', q{INSERT INTO tc VALUES (401, '{4010}')});
is(bits($node, 'tc_doc'), '12', 'an insert begins the conversion: CONVERTING');
check_standby('converting');

# Autovacuum does not wait: it finds the old writer, gives up, and leaves
# SETTLED unset for the next VACUUM.
$node->safe_psql('postgres', q{
	SELECT injection_points_attach('bark-convert-autovac-busy', 'wait');
	ALTER TABLE tc SET (autovacuum_vacuum_threshold = 0, autovacuum_vacuum_scale_factor = 0);
	DELETE FROM tc WHERE id = 400;
});
$node->wait_for_event('autovacuum worker', 'bark-convert-autovac-busy');
pass('autovacuum: the barrier finds a writer and does not wait');
$node->safe_psql('postgres', q{
	SELECT injection_points_wakeup('bark-convert-autovac-busy');
	SELECT injection_points_detach('bark-convert-autovac-busy');
});
$node->poll_query_until('postgres',
	q{SELECT autovacuum_count > 0 FROM pg_stat_user_tables WHERE relname = 'tc'})
  or die 'autovacuum did not finish';
$node->safe_psql('postgres', q{ALTER TABLE tc SET (autovacuum_enabled = off)});
is(bits($node, 'tc_doc'), '12', 'autovacuum: SETTLED not set');

# A manual VACUUM waits for the old writer, and its pass leaves row 3's
# tagged entries, so the conversion settles without completing.
$node->safe_psql('postgres',
	q{SELECT injection_points_attach('bark-convert-barrier-wait', 'notice')});
my $vac = $node->background_psql('postgres', on_error_stop => 0);
$vac->query_until(qr/start/, q{
	\echo start
	VACUUM tc;
});
$node->poll_query_until('postgres', q{
	SELECT count(*) > 0 FROM pg_locks l JOIN pg_stat_activity a USING (pid)
	 WHERE a.query LIKE 'VACUUM tc%' AND l.locktype = 'virtualxid' AND NOT l.granted})
  or die 'VACUUM does not wait for the old writer';
is(bits($node, 'tc_doc'), '12', 'manual VACUUM: waits in the barrier, SETTLED not yet');
$old->query_safe('COMMIT');
$old->quit;
$vac->query('SELECT 1');
$vac->quit;
is(bits($node, 'tc_doc'), '28', 'manual VACUUM: SETTLED after the writer ended');
like($vac->{stderr}, qr/(notice triggered for injection point bark-convert-barrier-wait.*){2}/s,
	'manual VACUUM: it waited again after setting SETTLED');
$node->safe_psql('postgres',
	q{SELECT injection_points_detach('bark-convert-barrier-wait')});
check_standby('settled');

is( $node->safe_psql(
		'postgres', q{
		SELECT count(i.tag) FROM generate_series(1, pg_relation_size('tc_doc') / 8192 - 1) b,
		       LATERAL bark_page_items('tc_doc', b) i
		 WHERE i.shape IN ('SINGLE', 'OVERSIZED')}),
	'2', 'settled: only row 3\'s two entries are still tagged');

# After SETTLED, entries are stored untagged, though the table AM tags
# them; once row 3 is gone, VACUUM finds no tagged entry and completes the
# conversion.
$node->safe_psql('postgres', q{
	SET test_versioned_tableam.keep_locator = pretend;
	UPDATE tc SET doc = '{20,21,22}' WHERE id = 2;
	RESET test_versioned_tableam.keep_locator;
});
is( $node->safe_psql(
		'postgres', q{
		SELECT count(i.tag) || '/' || count(*)
		  FROM tc t, generate_series(1, pg_relation_size('tc_doc') / 8192 - 1) b,
		       LATERAL bark_page_items('tc_doc', b) i
		 WHERE t.id = 2 AND (i.htid = t.ctid OR t.ctid = ANY (i.tids))}),
	'0/3', 'settled: an update stores untagged entries (some join a LIST)');
$node->safe_psql('postgres', q{VACUUM tc});
is(bits($node, 'tc_doc'), '28', 'settled: VACUUM keeps the conversion open');
$node->safe_psql('postgres', q{DELETE FROM tc WHERE id = 3; VACUUM tc});
is(bits($node, 'tc_doc'), '0', 'completion: MKTAGS, CONVERTING and SETTLED cleared');
is( $node->safe_psql(
		'postgres', q{
		SELECT count(i.tag) FROM generate_series(1, pg_relation_size('tc_doc') / 8192 - 1) b,
		       LATERAL bark_page_items('tc_doc', b) i
		 WHERE i.shape IN ('SINGLE', 'OVERSIZED')}),
	'0', 'completion: no tagged leaf entry left');
is($node->safe_psql('postgres', $q), '1,2,401,1000', 'completion: MVCC results');
my ($ret, $out, $err) =
  $node->psql('postgres', q{INSERT INTO tc VALUES (2000, '{22}')});
like($err, qr/conflicting key value violates exclusion constraint "tc_doc"/,
	'completion: the constraint holds');
is($node->safe_psql('postgres', "SELECT bark_index_parent_check('tc_doc', true)"),
	'', 'completion: bark_index_parent_check');
check_standby('complete');

$node->stop;
$standby->stop;
done_testing();

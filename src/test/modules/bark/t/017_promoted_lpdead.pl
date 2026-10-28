
# Copyright (c) 2026, PostgreSQL Global Development Group

# Simple deletion on a promoted standby must not trust the LP_DEAD marks it
# inherited from the old primary (Peter Geoghegan's nbtree repro, run against
# a BARK index):
#
# The primary marks entries LP_DEAD by its own xmin horizon, and full-page
# images carry the marks to the standby.  A transaction that began during
# recovery does not skip marked entries in its own scans
# (ignore_killed_tuples is false), and it survives promotion.  After
# promotion, simple deletion by a new inserter must still not delete a
# marked entry whose row that transaction's snapshot sees, or the
# transaction's index scans miss a row its sequential scan finds.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $primary = PostgreSQL::Test::Cluster->new('primary');
$primary->init(allows_streaming => 1);
$primary->append_conf('postgresql.conf', 'autovacuum = off');
$primary->start;

# lpdead_victim in database postgres is the main case.  The same table in
# database other, and lpdead_second in postgres, test that the marks are
# not trusted again while a transaction from recovery runs in any database:
# the marks' horizon is not per database.
$primary->safe_psql('postgres', 'CREATE DATABASE other');
sub fixture
{
	my ($tab) = @_;
	return qq(
	CREATE TABLE $tab (id int, k int, pad text);
	CREATE INDEX ${tab}_k ON $tab USING bark (k) WITH (fillfactor = 100);
	INSERT INTO $tab SELECT i, (i % 10) * 1000, 'x' FROM generate_series(1, 100) i;
	VACUUM $tab;
);
}
$primary->safe_psql('postgres',
	'CREATE EXTENSION pageinspect;' . fixture('lpdead_victim') . fixture('lpdead_second'));
$primary->safe_psql('other',
	'CREATE EXTENSION pageinspect;' . fixture('lpdead_victim'));

$primary->backup('backup');
my $standby = PostgreSQL::Test::Cluster->new('standby');
$standby->init_from_backup($primary, 'backup', has_streaming => 1);
$standby->start;
$primary->wait_for_catchup($standby, 'replay', $primary->lsn('insert'));

# A transaction that begins during recovery, and sees all ten rows of key
# 3000.
my $rr = $standby->background_psql('postgres', on_error_stop => 1);
$rr->query_safe('BEGIN ISOLATION LEVEL REPEATABLE READ');
is(count_victim($rr, 'index'), 10,
	'snapshot taken during recovery sees ten rows');
my $rr_other = $standby->background_psql('other', on_error_stop => 1);
$rr_other->query_safe('BEGIN ISOLATION LEVEL REPEATABLE READ');
is(count_victim($rr_other, 'index'), 10,
	'so does one in database other');

# Delete them on the primary, and let index scans mark their entry, a LIST
# (a scan marks an entry once every one of its rows is dead).
foreach my $case (['postgres', 'lpdead_victim'], ['postgres', 'lpdead_second'],
	['other', 'lpdead_victim'])
{
	my ($db, $tab) = @$case;
	$primary->safe_psql($db, "DELETE FROM $tab WHERE k = 3000");
	for (1 .. 3)
	{
		$primary->safe_psql($db,
			"SET enable_seqscan = off; SET enable_bitmapscan = off; "
			  . "SELECT count(*) FROM $tab WHERE k = 3000");
	}
}
is( dead_entries($primary) . '|'
	  . dead_entries($primary, 'postgres', 'lpdead_second') . '|'
	  . dead_entries($primary, 'other'),
	'1|1|1',
	'primary marked the deleted rows\' entry LP_DEAD');

# The first change after a checkpoint ships the leaf, mark included, as a
# full-page image.
$primary->safe_psql('postgres', 'CHECKPOINT');
$primary->safe_psql('postgres',
	q(INSERT INTO lpdead_victim VALUES (1000, 5000, 'y');
	  INSERT INTO lpdead_second VALUES (1000, 5000, 'y')));
$primary->safe_psql('other',
	q(INSERT INTO lpdead_victim VALUES (1000, 5000, 'y')));
$primary->wait_for_catchup($standby, 'replay', $primary->lsn('insert'));
is( dead_entries($standby) . '|'
	  . dead_entries($standby, 'postgres', 'lpdead_second') . '|'
	  . dead_entries($standby, 'other'),
	'1|1|1',
	'the LP_DEAD marks reached the standby');

$standby->promote;

# A new inserter on the promoted server fills the leaf, so simple deletion
# runs over the inherited mark before the leaf splits.  The fill keys sort
# below the victim's, so the mark survives on the leaf that keeps it, until
# the next fill of that leaf.
$standby->safe_psql('postgres',
	q(INSERT INTO lpdead_victim SELECT i, 3000 + i - 1999, 'z' FROM generate_series(2000, 2599) i)
);
cmp_ok($standby->safe_psql('postgres',
		q(SELECT count(*) FROM bark_multi_page_stats('lpdead_victim_k', 1, -1) WHERE type = 'leaf')),
	'>', 1, 'the inserts split the leaf');

is(count_victim($rr, 'seq'), 10, 'snapshot still sees ten rows by seqscan');
is(count_victim($rr, 'index'), 10,
	'snapshot still sees ten rows by index scan');
$rr->query_safe('COMMIT');
$rr->quit;

# No transaction from recovery is left in database postgres, but one is in
# database other.  Simple deletion in postgres may run now (on
# lpdead_second's inherited mark); it must not make the marks trusted in
# every database, or the fill of the same leaf in database other would
# delete the entry that transaction needs.
# The VACUUM brings that session's view of its database's horizon up to
# date first, so that the check there is made with the horizon by which the
# marks would be trusted.
my $fill = q(SELECT i, 3000 + i - 1999, 'z' FROM generate_series(2000, 2599) i);
$standby->safe_psql('postgres',
	"VACUUM lpdead_victim; INSERT INTO lpdead_second $fill");
$standby->safe_psql('other', "INSERT INTO lpdead_victim $fill");
is(count_victim($rr_other, 'seq') . '|' . count_victim($rr_other, 'index'),
	'10|10',
	'snapshot in database other still sees ten rows by both scans');
$rr_other->query_safe('COMMIT');
$rr_other->quit;

# Once that transaction has ended, the marks are trusted again: an entry the
# promoted server marks itself is deleted by the next fill of its leaf
# before the leaf splits.
$standby->safe_psql('postgres', 'DELETE FROM lpdead_victim WHERE k = 7000');
for (1 .. 3)
{
	$standby->safe_psql(
		'postgres', q(
		SET enable_seqscan = off;
		SET enable_bitmapscan = off;
		SELECT count(*) FROM lpdead_victim WHERE k = 7000;
	));
}
my $marked_leaf = $standby->safe_psql('postgres',
	q(SELECT blkno FROM bark_multi_page_stats('lpdead_victim_k', 1, -1)
	  WHERE type IN ('leaf', 'root') AND dead_items > 0 ORDER BY blkno LIMIT 1));
isnt($marked_leaf, '', 'the promoted server marks the entry of its own deleted rows');
my $leaves_before = $standby->safe_psql('postgres',
	q(SELECT count(*) FROM bark_multi_page_stats('lpdead_victim_k', 1, -1) WHERE type = 'leaf'));
# Fill that leaf, with distinct keys just above the marked one (a run of one
# key would coalesce and never fill it), until simple deletion has run on it.
$standby->safe_psql('postgres', qq(
DO \$\$
DECLARE n int := 7000;
BEGIN
  WHILE (SELECT dead_items FROM bark_page_stats('lpdead_victim_k', $marked_leaf)) > 0
        AND n < 7999 LOOP
    n := n + 1;
    INSERT INTO lpdead_victim VALUES (n + 3000, n, 'w');
  END LOOP;
END \$\$;
));
is($standby->safe_psql('postgres',
		"SELECT dead_items FROM bark_page_stats('lpdead_victim_k', $marked_leaf)"),
	'0', 'after the transaction ends, simple deletion removes marked entries');
# A split also drops the marks; this must have been the deletion.
is($standby->safe_psql('postgres',
		q(SELECT count(*) FROM bark_multi_page_stats('lpdead_victim_k', 1, -1) WHERE type = 'leaf')),
	$leaves_before, 'without splitting the leaf');
is($standby->safe_psql('postgres',
		q(SELECT count(*) FROM lpdead_victim WHERE k = 3000)), 0,
	'the deleted rows stay deleted');
is($standby->safe_psql('postgres',
		q(SELECT count(*) FROM lpdead_victim WHERE k = 7000)), 0,
	'and so do the rows deleted after the promotion');

done_testing();

sub scan_settings
{
	my ($how) = @_;
	my ($seqscan, $indexscan) = $how eq 'index' ? ('off', 'on') : ('on', 'off');
	return
	    "SET LOCAL enable_seqscan = $seqscan; SET LOCAL enable_bitmapscan = off; "
	  . "SET LOCAL enable_indexscan = $indexscan; SET LOCAL enable_indexonlyscan = $indexscan;";
}

sub count_victim
{
	my ($session, $how) = @_;
	return $session->query_safe(scan_settings($how)
		  . ' SELECT count(*) FROM lpdead_victim WHERE k = 3000');
}

sub dead_entries
{
	my ($node, $db, $tab) = @_;
	$tab //= 'lpdead_victim';
	return $node->safe_psql($db // 'postgres',
		"SELECT coalesce(sum(dead_items), 0) FROM bark_multi_page_stats('${tab}_k', 1, -1) WHERE type IN ('leaf', 'root')"
	);
}

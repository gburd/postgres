
# Copyright (c) 2026, PostgreSQL Global Development Group

# The BARK page validator (bark_verify_pages, cassert builds only): a fault
# planted in a page right after a change is logged must be reported by the
# statement that made the change, with the validator on, and only by a
# later amcheck with it off.  WAL redo checks the pages it replays and
# PANICs on a bad one.  The faults are planted by the injection points
# bark-verify-fault-count (a LIST entry's count set to 1, a SHAPES check)
# and bark-verify-fault-order (the entry's line pointer swapped with the
# one before it, an ORDER check), after an ADD_TID record, on the primary
# or in the standby's redo of it.  This is the check sqlxtc ran for its
# own port of RethinkDB's validate-after-every-mutation: a planted
# one-byte fault, caught at the call that made it.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

if (!check_pg_config('#define USE_ASSERT_CHECKING 1'))
{
	plan skip_all => 'the page validator exists only in cassert builds';
}

my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init(allows_streaming => 1);
$node->append_conf('postgresql.conf', "autovacuum = off\n");
$node->start;
$node->safe_psql('postgres',
	'CREATE EXTENSION amcheck; CREATE EXTENSION injection_points;');

is($node->safe_psql('postgres', 'SHOW bark_verify_pages'),
	'on', 'the validator is on by default in a cassert build');

# A rightmost root leaf of keys 1..10 (no high key, so key k at offset k)
# whose key 5 is a plain LIST of two TIDs: the next insert of 5 adds a TID
# to it, an ADD_TID record.
sub setup
{
	my ($n, $t) = @_;
	$n->safe_psql(
		'postgres', qq[
		CREATE TABLE $t (a int);
		CREATE INDEX ${t}_idx ON $t USING bark (a);
		INSERT INTO $t SELECT g FROM generate_series(1, 10) g;
		INSERT INTO $t VALUES (5);
	]);
}

# Plant `fault`, insert 5, and return (rc, stderr) of the insert and the
# report of bark_index_check afterwards.
sub fault_run
{
	my ($t, $fault, $validator) = @_;
	setup($node, $t);
	my $set = $validator ? '' : 'SET bark_verify_pages = off;';
	my ($rc, $out, $err) = $node->psql(
		'postgres', qq[
		SELECT injection_points_attach('bark-verify-fault-$fault', 'notice');
		$set
		INSERT INTO $t VALUES (5);
	]);
	$node->safe_psql('postgres',
		"SELECT injection_points_detach('bark-verify-fault-$fault')");
	my ($crc, $cout, $cerr) =
	  $node->psql('postgres', "SELECT bark_index_check('${t}_idx')");
	return ($rc, $err, $cerr);
}

my $blk = 1;	# the index's one leaf, its root
my $lp = qr/has a list entry with 1 locators on page $blk at offset 5/;
my $ord = qr/has out-of-order keys on page $blk at offset 5/;

# SHAPES: the count of the LIST that just took a TID.
my ($rc, $err, $cerr) = fault_run('t_count_on', 'count', 1);
isnt($rc, 0, 'count fault, validator on: the insert fails');
like($err, qr/ERROR:  BARK index "t_count_on_idx" $lp/,
	'count fault, validator on: reported by the insert');
like($err,
	qr/CONTEXT:  BARK page validator, block $blk after bark_add_tid_entry/,
	'count fault, validator on: the context names the change');
like($cerr, qr/$lp/, 'count fault, validator on: amcheck sees the same page');

($rc, $err, $cerr) = fault_run('t_count_off', 'count', 0);
is($rc, 0, 'count fault, validator off: the insert succeeds');
unlike($err, qr/ERROR/, 'count fault, validator off: nothing reported');
like($cerr, qr/BARK index "t_count_off_idx" $lp/,
	'count fault, validator off: only amcheck reports it');

# ORDER: the LIST's line pointer swapped with key 4's, before it.
($rc, $err, $cerr) = fault_run('t_order_on', 'order', 1);
isnt($rc, 0, 'order fault, validator on: the insert fails');
like($err, qr/ERROR:  BARK index "t_order_on_idx" $ord/,
	'order fault, validator on: reported by the insert');
like($cerr, qr/$ord/, 'order fault, validator on: amcheck sees the same page');

($rc, $err, $cerr) = fault_run('t_order_off', 'order', 0);
is($rc, 0, 'order fault, validator off: the insert succeeds');
like($cerr, qr/BARK index "t_order_off_idx" $ord/,
	'order fault, validator off: only amcheck reports it');

# Unfaulted, the same inserts pass the validator and amcheck.
setup($node, 't_clean');
$node->safe_psql('postgres', 'INSERT INTO t_clean VALUES (5), (5), (11)');
is(scalar $node->psql('postgres', "SELECT bark_index_parent_check('t_clean_idx')"),
	0, 'no fault: the validator and amcheck pass');

# Redo: the standby plants the count fault as it replays an ADD_TID record,
# and its redo check PANICs.  The primary's page is untouched.
$node->backup('b');
my $standby = PostgreSQL::Test::Cluster->new('standby');
$standby->init_from_backup($node, 'b', has_streaming => 1);
$standby->start;
setup($node, 't_redo');
$node->wait_for_replay_catchup($standby);
$standby->safe_psql('postgres',
	"SELECT injection_points_attach('bark-verify-fault-count', 'notice')");
my $off = -s $standby->logfile;
$node->safe_psql('postgres', 'INSERT INTO t_redo VALUES (5)');
ok( $standby->wait_for_log(
		qr/PANIC:  BARK index "\d+" has a list entry with 1 locators on page $blk at offset 5/,
		$off),
	'redo: the standby PANICs on the page it replayed');
is(scalar $node->psql('postgres', "SELECT bark_index_parent_check('t_redo_idx')"),
	0, 'redo: the primary page is sound');
$standby->stop('immediate', fail_ok => 1);

$node->stop;
done_testing();

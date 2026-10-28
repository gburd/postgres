
# Copyright (c) 2026, PostgreSQL Global Development Group

# The BARK page validator (bark_verify_pages, cassert builds only): a fault
# planted in a page right after a change is logged must be reported by the
# statement that made the change, with the validator on, and only by a
# later amcheck with it off.  WAL redo checks the pages it replays and
# PANICs on a bad one.  The faults are planted by the injection points
# bark-verify-fault-count (a LIST entry's count set to 1, a SHAPES check)
# and bark-verify-fault-order (the entry's line pointer swapped with the
# one before it, an ORDER check), after an ADD_TID record, on the primary
# or in the standby's redo of it.  Both lie in the entries the ADD_TID
# touched, which the validator checks after every such change.  A third,
# bark-verify-fault-far, swaps two entries far from the change, which only
# a whole-page check sees: one forced by bark-verify-force-full, or the
# validator's periodic one.  This is the check sqlxtc ran for its
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

# A fault away from the change.  An INSERT of 9 adds a TID to the LIST at
# offset 9 while bark-verify-fault-far swaps the entries at offsets 1 and 2.
# The bounded check of an ADD_TID reads offsets 8..10 and the high key
# (this rightmost leaf has none), so it misses the swap and the INSERT
# succeeds: it is the first BARK change of a fresh session, and only every
# 32nd bounded check of a backend reads the whole page.  The point is
# detached at once, since a second swap would undo the fault.  A later
# change's whole-page check then reports the swap at offset 2: forced at
# once with bark-verify-force-full, and without it within
# BARK_VERIFY_FULL_EVERY (32) more changes in the backend.  With no later
# change only amcheck would report it.
sub far_fault
{
	my ($t, $more) = @_;
	setup($node, $t);
	$node->safe_psql('postgres', "INSERT INTO $t VALUES (9)");
	return $node->psql(
		'postgres', qq[
		SELECT injection_points_attach('bark-verify-fault-far', 'notice');
		INSERT INTO $t VALUES (9);
		SELECT injection_points_detach('bark-verify-fault-far');
		SELECT 'planted';
		$more
	]);
}
my $far = qr/ERROR:  BARK index "t_far\w*_idx" has out-of-order keys on page $blk at offset 2/;
my $farctx = qr/CONTEXT:  BARK page validator, block $blk after bark_add_tid_entry/;

($rc, my $out, $err) = far_fault('t_far_forced', q[
		SELECT injection_points_attach('bark-verify-force-full', 'notice');
		INSERT INTO t_far_forced VALUES (9);]);
$node->safe_psql('postgres',
	"SELECT injection_points_detach('bark-verify-force-full')");
like($out, qr/planted/, 'far fault: the INSERT that planted it succeeds');
like($err, $far, 'far fault: a forced whole-page check reports it');
like($err, $farctx, 'far fault: after the next ADD_TID');

($rc, $out, $err) = far_fault('t_far_periodic', q[
		DO $$ BEGIN FOR i IN 1..40 LOOP
			RAISE NOTICE 'insert %', i; INSERT INTO t_far_periodic VALUES (9);
		END LOOP; END $$;]);
my @n = $err =~ /NOTICE:  insert (\d+)/g;
like($out, qr/planted/, 'far fault, periodic: the planting INSERT succeeds');
like($err, $far, 'far fault, periodic: a later full check reports it');
ok(@n && $n[-1] <= 32,
	'far fault, periodic: within 32 changes (' . ($n[-1] // 'none') . ')');

# Faults the validator finds in the other pages a change writes: the meta
# page's version (VACUUM stores the index's key count there,
# bark_set_nkeys) and an overflow page's left link (an OVERSIZED key's
# chain).
($rc, $out, $err) = $node->psql(
	'postgres', q[
	CREATE TABLE t_meta (a int) WITH (autovacuum_enabled = off);
	CREATE INDEX t_meta_idx ON t_meta USING bark (a);
	INSERT INTO t_meta SELECT g FROM generate_series(1, 100) g;
	SELECT injection_points_attach('bark-verify-fault-meta', 'notice');
	VACUUM t_meta;]);
like($err,
	qr/ERROR:  BARK index "t_meta_idx" has a meta page 0 with magic number 5241424B and version 0/,
	'meta fault: reported by the change that wrote the meta page');
like($err, qr/CONTEXT:  BARK page validator, block 0 after bark_set_nkeys/,
	'meta fault: the context names the meta-page writer');
$node->safe_psql('postgres',
	"SELECT injection_points_detach('bark-verify-fault-meta')");
($rc, $out, $err) = $node->psql(
	'postgres', q[
	CREATE TABLE t_ovf (k text);
	ALTER TABLE t_ovf ALTER COLUMN k SET STORAGE EXTERNAL;
	CREATE INDEX t_ovf_idx ON t_ovf USING bark (k);
	SELECT injection_points_attach('bark-verify-fault-overflow', 'notice');
	INSERT INTO t_ovf SELECT string_agg(md5(g::text), '') FROM generate_series(1, 400) g;]);
like($err,
	qr/ERROR:  BARK index "t_ovf_idx" has an overflow page \d+ that does not have the overflow-page layout/,
	'overflow fault: reported by the chain write');
like($err, qr/CONTEXT:  BARK page validator, block \d+ after bark_write_overflow_chain/,
	'overflow fault: the context names the chain write');
$node->safe_psql('postgres',
	"SELECT injection_points_detach('bark-verify-fault-overflow')");

# Unfaulted, the same inserts pass the validator and amcheck.
setup($node, 't_clean');
$node->safe_psql('postgres', 'INSERT INTO t_clean VALUES (5), (5), (11)');
is(scalar $node->psql('postgres', "SELECT bark_index_parent_check('t_clean_idx')"),
	0, 'no fault: the validator and amcheck pass');

# A version tag of 0 in a tagged SINGLE (BARK_HAS_TAG kept): planted by
# bark-verify-fault-tag in the entry an INSERT_LEAF just added, which only a
# table AM that hands out tags makes (test_versioned_tableam's
# heap_versioned, where it is installed).
SKIP:
{
	skip 'test_versioned_tableam is not installed', 4
	  if $node->safe_psql('postgres',
		q{SELECT count(*) FROM pg_available_extensions WHERE name = 'test_versioned_tableam'}) ne '1';
	$node->safe_psql(
		'postgres', q[
		CREATE EXTENSION test_versioned_tableam;
		CREATE TABLE t_tag (id int, k int) USING heap_versioned;
		INSERT INTO t_tag SELECT g, g FROM generate_series(1, 10) g;
		CREATE INDEX t_tag_idx ON t_tag USING bark (k);
		CREATE TABLE t_tag_off (id int, k int) USING heap_versioned;
		INSERT INTO t_tag_off SELECT g, g FROM generate_series(1, 10) g;
		CREATE INDEX t_tag_off_idx ON t_tag_off USING bark (k);
	]);
	my $tagfault = qr/has a tagged entry with tag 0 on page $blk at offset \d+/;
	($rc, $out, $err) = $node->psql(
		'postgres', q[
		SELECT injection_points_attach('bark-verify-fault-tag', 'notice');
		UPDATE t_tag SET k = 100 WHERE id = 3;]);
	$node->safe_psql('postgres',
		"SELECT injection_points_detach('bark-verify-fault-tag')");
	like($err, qr/ERROR:  BARK index "t_tag_idx" $tagfault/,
		'tag fault, validator on: reported by the update');
	like($err, qr/CONTEXT:  BARK page validator, block $blk after bark_insert_entry/,
		'tag fault, validator on: the context names the insert');
	($rc, $out, $err) = $node->psql(
		'postgres', q[
		SELECT injection_points_attach('bark-verify-fault-tag', 'notice');
		SET bark_verify_pages = off;
		UPDATE t_tag_off SET k = 100 WHERE id = 3;]);
	$node->safe_psql('postgres',
		"SELECT injection_points_detach('bark-verify-fault-tag')");
	is($rc, 0, 'tag fault, validator off: the update succeeds');
	($rc, $out, $err) =
	  $node->psql('postgres', "SELECT bark_index_check('t_tag_off_idx')");
	like($err, qr/BARK index "t_tag_off_idx" $tagfault/,
		'tag fault, validator off: only amcheck reports it');
}

# Redo: the standby plants the count fault as it replays an ADD_TID record,
# and its redo check PANICs.  Redo checks one such record in 32, so the
# check is forced with bark-verify-force-full.  The primary's page is
# untouched.
$node->backup('b');
my $standby = PostgreSQL::Test::Cluster->new('standby');
$standby->init_from_backup($node, 'b', has_streaming => 1);
$standby->start;
setup($node, 't_redo');
$node->wait_for_replay_catchup($standby);
$standby->safe_psql('postgres',
	"SELECT injection_points_attach('bark-verify-fault-count', 'notice'); "
	  . "SELECT injection_points_attach('bark-verify-force-full', 'notice')");
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

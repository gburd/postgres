# Copyright (c) 2026, PostgreSQL Global Development Group

# FileOpsCreate() on a path that already exists, then ROLLBACK.  The UNDO
# record of a create unlinks the path, so if the call opened an existing file
# a rollback would remove a committed one.  The committed file must survive
# on the primary, on a standby replaying the abort record, and on the primary
# after crash recovery replays it.
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $primary = PostgreSQL::Test::Cluster->new('primary');
$primary->init(allows_streaming => 1);
$primary->start;
$primary->safe_psql('postgres', 'CREATE EXTENSION test_fileops');
$primary->backup('backup');
my $standby = PostgreSQL::Test::Cluster->new('standby');
$standby->init_from_backup($primary, 'backup', has_streaming => 1);
$standby->start;

$primary->safe_psql('postgres', q{
BEGIN;
SELECT test_fileops_create('existing.dat', 384);
SELECT test_fileops_write('existing.dat', 0, 'committed');
COMMIT;
});

my ($ret, $out, $err) = $primary->psql('postgres', q{
BEGIN;
SELECT test_fileops_create('existing.dat', 384);
ROLLBACK;
});
like($err, qr/could not create file "existing.dat": File exists/,
	'creating an existing path is an ERROR');

$primary->wait_for_catchup($standby);
for my $stage ('online', 'after crash')
{
	if ($stage eq 'after crash')
	{
		$primary->stop('immediate');
		$primary->start;
	}
	my $f = $primary->data_dir . '/existing.dat';
	is(-e $f ? slurp_file($f) : '(missing)',
		'committed', "$stage: primary keeps the committed file");
}
my $sf = $standby->data_dir . '/existing.dat';
is(-e $sf ? slurp_file($sf) : '(missing)',
	'committed', 'standby keeps the committed file');

$standby->stop;
$primary->stop;
done_testing();

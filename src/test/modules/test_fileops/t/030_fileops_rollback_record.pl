# Copyright (c) 2026, PostgreSQL Global Development Group

# The undo phase at the end of crash recovery logs each loser's rollback in
# an XLOG_UNDO_ROLLBACK record, carrying the records that applied.  A standby
# applies them when it replays that record.  So the standby must keep a
# change the primary could not roll back, and a second crash of the primary
# must not apply the loser's records again over a committed reuse of its
# name.
#
# Must not run as root (root ignores directory permissions).
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'running as root: EACCES cannot be produced' if $> == 0;
plan skip_all => 'directory search permission is POSIX-only'
  if $PostgreSQL::Test::Utils::windows_os;

my $primary = PostgreSQL::Test::Cluster->new('primary');
$primary->init(allows_streaming => 1);
$primary->start;
$primary->safe_psql('postgres', 'CREATE EXTENSION test_fileops');
$primary->backup('b');
my $standby = PostgreSQL::Test::Cluster->new('standby');
$standby->init_from_backup($primary, 'b', has_streaming => 1);
$standby->start;
my $pd = $primary->data_dir;
my $sd = $standby->data_dir;

# A loser creates two files, one inside a directory that loses its search
# permission before the restart, so the primary cannot remove that one.
mkdir("$pd/locked") or die "mkdir: $!";
mkdir("$sd/locked") or die "mkdir: $!";
my $a = $primary->background_psql('postgres');
$a->query_safe("BEGIN; SELECT test_fileops_create('loser.dat', 384);"
	  . " SELECT test_fileops_create('locked/stuck.dat', 384);");
$primary->safe_psql('postgres', 'CREATE TABLE flush_me (a int)');
$primary->wait_for_catchup($standby);
ok(-e "$sd/loser.dat", 'standby has the loser file');

$primary->stop('immediate');
eval { $a->quit; };
chmod(0600, "$pd/locked") or die "chmod: $!";
$primary->start;
chmod(0700, "$pd/locked") or die "chmod: $!";
ok(!-e "$pd/loser.dat", 'primary removed the loser file');
ok(-e "$pd/locked/stuck.dat", 'primary could not remove the locked file');

# The standby replays the rollback record and does what the primary did.
$primary->wait_for_catchup($standby);
ok(!-e "$sd/loser.dat", 'standby removed the loser file');
ok(-e "$sd/locked/stuck.dat",
	'standby keeps the file the primary could not remove');

# The name is reused and committed.  A second crash replays the rollback
# record, which must leave the committed file alone.
$primary->safe_psql(
	'postgres', q{
BEGIN; SELECT test_fileops_create('loser.dat', 384);
SELECT test_fileops_write('loser.dat', 0, 'committed'); COMMIT;
});
$primary->stop('immediate');
$primary->start;
is(slurp_file("$pd/loser.dat"), 'committed',
	'a second crash leaves the committed reuse');
$primary->wait_for_catchup($standby);
is(slurp_file("$sd/loser.dat"), 'committed',
	'the standby has the committed reuse');

$standby->stop;
$primary->stop;
done_testing();

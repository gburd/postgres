# Copyright (c) 2026, PostgreSQL Global Development Group

# A transaction is in flight with a FILEOPS create and mkdir when the primary
# crashes, so it writes no abort record.  The primary's crash recovery rolls
# it back and logs the rollback (XLOG_UNDO_ROLLBACK), and a streaming standby
# must end with the primary's files when it replays that record.
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $primary = PostgreSQL::Test::Cluster->new('primary');
$primary->init(allows_streaming => 1);
$primary->append_conf('postgresql.conf', "restart_after_crash = on\n");
$primary->start;
$primary->safe_psql('postgres', 'CREATE EXTENSION test_fileops');
$primary->backup('b');
my $standby = PostgreSQL::Test::Cluster->new('standby');
$standby->init_from_backup($primary, 'b', has_streaming => 1);
$standby->start;

for my $how ('immediate stop', 'backend killed')
{
	(my $f = "loser-$how.dat") =~ s/ /-/g;
	my $a = $primary->background_psql('postgres');
	$a->query_safe("BEGIN; SELECT test_fileops_create('$f', 384);"
		  . " SELECT test_fileops_mkdir('$f.d', 448);");
	# Make it durable and replicated.
	$primary->safe_psql('postgres',
		'CREATE TABLE IF NOT EXISTS flush_me (a int)');
	$primary->wait_for_catchup($standby);
	ok(-e $standby->data_dir . "/$f", "$how: standby has the in-flight file");

	if ($how eq 'immediate stop')
	{
		$primary->stop('immediate');
		eval { $a->quit; };
		$primary->start;
	}
	else
	{
		my $pid = $a->query_safe('SELECT pg_backend_pid()');
		chomp $pid;
		PostgreSQL::Test::Utils::system_or_bail('pg_ctl', 'kill', 'KILL', $pid);
		eval { $a->quit; };
		$primary->poll_query_until('postgres', 'SELECT 1', '1');
	}
	ok(!-e $primary->data_dir . "/$f", "$how: primary reversed the loser");
	$primary->safe_psql('postgres', 'CHECKPOINT');
	$primary->wait_for_catchup($standby);
	ok(!-e $standby->data_dir . "/$f", "$how: standby reversed the loser file");
	ok(!-e $standby->data_dir . "/$f.d", "$how: standby reversed the loser dir");
}

$standby->promote;
for my $how ('immediate-stop', 'backend-killed')
{
	ok(!-e $standby->data_dir . "/loser-$how.dat",
		"promoted standby: loser-$how.dat absent");
}
$standby->stop;
$primary->stop;
done_testing();

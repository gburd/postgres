# Copyright (c) 2026, PostgreSQL Global Development Group

# InsertXactUndoData() holds off a checkpoint's redo-pointer phase between
# inserting an UNDO batch and publishing the chain head that the checkpoint's
# XLOG_UNDO_INFLIGHT record lists.  Stop a backend in that interval, request
# a CHECKPOINT, and crash.  Without the hold-off the checkpoint completes, its
# redo pointer follows the batch, and its record omits the chain, so nothing
# reverses the create.
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'injection points not supported by this build'
  unless $ENV{enable_injection_points} eq 'yes';

my $node = PostgreSQL::Test::Cluster->new('n');
$node->init(allows_streaming => 1);
$node->append_conf('postgresql.conf', "checkpoint_timeout = 1h\n");
$node->start;
$node->safe_psql('postgres',
	'CREATE EXTENSION test_fileops; CREATE EXTENSION injection_points');

# The point is in a critical section, so the waiting backend must attach to
# the injection points' shared state beforehand.
my $a = $node->background_psql('postgres');
$a->query_safe('SELECT injection_points_set_local()');
$a->query_safe(
	"SELECT injection_points_attach('undo-batch-after-wal-insert', 'wait')");
$a->query_until(qr/go/,
	"\\echo go\nBEGIN; SELECT test_fileops_create('holdoff.dat', 384);\n");
$node->wait_for_event('client backend', 'undo-batch-after-wal-insert');

# Make the create and its UNDO batch durable, so that the crash below falls
# outside the documented window between a system call and its WAL record:
# another transaction's commit flushes everything inserted before it.
my $ins = $node->safe_psql('postgres', 'SELECT pg_current_wal_insert_lsn()');
$node->safe_psql('postgres', 'CREATE TABLE flush_me (a int)');
is( $node->safe_psql(
		'postgres', "SELECT pg_current_wal_flush_lsn() >= '$ins'::pg_lsn"),
	't',
	'the create is durable before the checkpoint');

# The checkpoint either waits for the backend or, without the hold-off,
# completes.
my $before = $node->safe_psql('postgres',
	'SELECT checkpoint_lsn FROM pg_control_checkpoint()');
my $c = $node->background_psql('postgres');
$c->query_until(qr/go/, "\\echo go\nCHECKPOINT;\n");
$node->poll_query_until(
	'postgres', qq{
SELECT EXISTS (SELECT 1 FROM pg_stat_activity
               WHERE backend_type = 'checkpointer'
                 AND wait_event = 'CheckpointDelayStart')
    OR (SELECT checkpoint_lsn <> '$before' FROM pg_control_checkpoint())})
  or die 'checkpoint neither waited nor completed';

$node->stop('immediate');
eval { $a->quit; };
eval { $c->quit; };
$node->start;
ok(!-e $node->data_dir . '/holdoff.dat',
	'crash recovery removed the loser file');

$node->stop;
done_testing();

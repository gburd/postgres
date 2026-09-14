# Phase 2d: per-backend UNDO engine lifecycle + crash-recovery smoke.
# The engine is wired and runs (shmem, bgworkers, smgr, WAL rmgr, xact-abort
# dispatch) but is DORMANT until a table AM produces per-backend undo (Phase 8+).
# This test confirms the engine initializes, workers launch, and a crash/recover
# cycle with the workers enabled is clean.
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('pbu_primary');
$node->init;
$node->append_conf('postgresql.conf', "pbu_undo_workers_enabled = on\n");
$node->start;

# 1. server up with pbu workers enabled
my $up = $node->safe_psql('postgres', 'SELECT 1');
is($up, '1', 'server up with per-backend undo workers enabled');

# 2. txn + rollback + commit + checkpoint are correct (heap; engine dormant)
$node->safe_psql('postgres', 'CREATE TABLE t(a int)');
$node->safe_psql('postgres', 'BEGIN; INSERT INTO t VALUES (1),(2); ROLLBACK');
$node->safe_psql('postgres', 'INSERT INTO t VALUES (3)');
$node->safe_psql('postgres', 'CHECKPOINT');
my $cnt = $node->safe_psql('postgres', 'SELECT count(*) FROM t');
is($cnt, '1', 'rollback discarded uncommitted rows, commit survived');

# 3. bgworkers actually present
my $log = slurp_file($node->logfile);
like($log, qr/UNDO worker started/, 'undo worker launched');
like($log, qr/discard worker started/, 'discard worker launched');

# 4. crash + recover cleanly with the engine enabled
$node->stop('immediate');
$node->start;
my $after = $node->safe_psql('postgres', 'SELECT count(*) FROM t');
is($after, '1', 'data intact after crash recovery with per-backend engine enabled');
my $rlog = slurp_file($node->logfile);
unlike($rlog, qr/PANIC|inconsistent|could not|corrupt/i, 'clean crash recovery, no PANIC/corruption');

$node->stop;
done_testing();

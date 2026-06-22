#!/usr/bin/perl

# Copyright (c) 2021-2026, PostgreSQL Global Development Group

# Regression test for a RECNO/UNDO inline-rollback interrupt-holdoff crash.
#
# When a transaction ROLLs BACK, AbortTransaction() runs inside a
# HOLD_INTERRUPTS() section and drives per-relation UNDO apply INLINE via
# AtAbort_XactUndo() -> ApplyPerRelUndo() -> RelUndoApplyChain().  If the
# chain apply hits a before-image whose target slot was concurrently reclaimed
# (a committed DELETE followed by a VACUUM/prune turned the ItemId non-normal),
# RelUndoApplyUpdate/Insert throw ERROR.  That ERROR is caught by the inline
# apply's PG_CATCH so the abort can fall back to the background revert worker.
#
# But errfinish() zeroes InterruptHoldoffCount / QueryCancelHoldoffCount before
# longjmp'ing to any handler (it assumes no handler runs inside a holdoff
# section).  This handler DOES: it runs inside AbortTransaction()'s
# HOLD_INTERRUPTS().  With the counts zeroed, AbortTransaction()'s matching
# RESUME_INTERRUPTS() trips
#     TRAP: failed Assert("InterruptHoldoffCount > 0")  (xact.c)
# and the backend (then the whole cluster, restart_after_crash off) goes down.
#
# The fix saves/restores the two holdoff counts around each inline-UNDO
# PG_TRY/PG_CATCH in xactundo.c.
#
# Reproducing needs REAL concurrency: rolling-back size-changing UPDATEs on a
# varlena column racing committed DELETEs plus VACUUM prune of the same rows,
# so a rollback's UNDO apply lands on a reclaimed slot.  We fork independent
# psql writers (UPDATE .. ROLLBACK, and DELETE .. COMMIT) and a VACUUM loop.
# Without the fix the cluster reliably crashes with the holdoff assert; with
# the fix the run completes and the server stays up.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Utils;
use Test::More;
use PostgreSQL::Test::Cluster;
use Time::HiRes qw(time usleep);
use POSIX qw(_exit);

my $node = PostgreSQL::Test::Cluster->new('recno_undo_rollback');
$node->init;
# Aggressive autovacuum so prune/reclaim of just-deleted rows races the
# rolling-back UNDO apply; small buffers to keep everyone on the hot pages.
# restart_after_crash off so a single backend crash tears the cluster down
# and the "server still up" assertions below fail loudly.
$node->append_conf('postgresql.conf', qq{
autovacuum = on
autovacuum_naptime = 1s
autovacuum_vacuum_threshold = 1
autovacuum_vacuum_insert_threshold = 1
autovacuum_vacuum_cost_delay = 0
shared_buffers = 16MB
restart_after_crash = off
});
$node->start;

my $nrows = 3000;
$node->safe_psql('postgres', qq{
    CREATE TABLE recno_ur (id int, bal numeric, c_data text)
        USING recno WITH (fillfactor = 30);
    INSERT INTO recno_ur
        SELECT g, 1000.0, repeat('x', 40) FROM generate_series(1, $nrows) g;
});

my $nwriters = 12;    # rolling-back size-changing updaters
my $ndeleters = 4;    # committed delete/re-insert churn (reclaims slots)
my $nvacuum  = 2;     # concurrent VACUUM prune
my $run_secs = 20;

my $connstr = $node->connstr('postgres');

my @pids;

# Updater: BEGIN; UPDATE (grow/shrink varlena) ...; ROLLBACK.  The ROLLBACK
# drives inline UNDO apply; if the row was reclaimed under it, apply errors.
for my $w (0 .. $nwriters - 1) {
    my $pid = fork();
    die "fork failed: $!" unless defined $pid;
    if ($pid == 0) {
        my $deadline = time() + $run_secs;
        my $ok = open(my $fh, '|-', 'psql', '-X', '-q', '-d', $connstr,
             '-v', 'ON_ERROR_STOP=0');
        _exit(0) unless $ok;
        my $seed = $w * 7919 + 1;
        while (time() < $deadline) {
            for (1 .. 20) {
                $seed = ($seed * 1103515245 + 12345) & 0x7fffffff;
                my $id  = ($seed % $nrows) + 1;
                my $cls = ($seed >> 8) % 4;
                my $n = $cls == 0 ? 20 + ($seed % 80)
                      : $cls == 1 ? 2000 + ($seed % 5000)   # overflow
                      : $cls == 2 ? 3 + ($seed % 10)         # shrink
                      :             500 + ($seed % 1500);
                # Mostly ROLLBACK so inline UNDO apply runs constantly.
                my $end = (($seed >> 16) % 3 == 0) ? 'COMMIT' : 'ROLLBACK';
                print $fh "BEGIN; UPDATE recno_ur SET c_data = repeat('${w}z', $n), "
                  . "bal = bal - 0.7 WHERE id = $id; $end;\n";
            }
        }
        close($fh);
        _exit(0);
    }
    push @pids, $pid;
}

# Deleter: DELETE some ids and COMMIT, then re-INSERT them, so slots get
# reclaimed by VACUUM out from under concurrent rolling-back updaters.
for my $d (0 .. $ndeleters - 1) {
    my $pid = fork();
    die "fork failed: $!" unless defined $pid;
    if ($pid == 0) {
        my $deadline = time() + $run_secs;
        my $ok = open(my $fh, '|-', 'psql', '-X', '-q', '-d', $connstr,
             '-v', 'ON_ERROR_STOP=0');
        _exit(0) unless $ok;
        my $seed = $d * 104729 + 7;
        while (time() < $deadline) {
            for (1 .. 10) {
                $seed = ($seed * 1103515245 + 12345) & 0x7fffffff;
                my $id = ($seed % $nrows) + 1;
                print $fh "DELETE FROM recno_ur WHERE id = $id;\n";
                print $fh "INSERT INTO recno_ur VALUES "
                  . "($id, 1000.0, repeat('d', 60));\n";
            }
        }
        close($fh);
        _exit(0);
    }
    push @pids, $pid;
}

# VACUUM loop: prunes reclaimed slots (turns their ItemIds non-normal) and
# discards UNDO, racing the rolling-back updaters.
for my $v (0 .. $nvacuum - 1) {
    my $pid = fork();
    die "fork failed: $!" unless defined $pid;
    if ($pid == 0) {
        my $deadline = time() + $run_secs;
        while (time() < $deadline) {
            system('psql', '-X', '-q', '-d', $connstr, '-c', 'VACUUM recno_ur');
            usleep(150_000);
        }
        _exit(0);
    }
    push @pids, $pid;
}

waitpid($_, 0) for @pids;

# The cluster must still be up: the holdoff-assert crash took the whole
# cluster down (restart_after_crash off).
my ($rc, $out, $err) = $node->psql('postgres', 'SELECT count(*) FROM recno_ur');
is($rc, 0, 'server still accepting queries after concurrent rollback/reclaim churn')
    or diag("psql rc=$rc err=$err");

# No crash / holdoff assert in the server log.
my $log = slurp_file($node->logfile);
unlike($log,
    qr/TRAP: failed Assert\("InterruptHoldoffCount > 0"\)/,
    'no InterruptHoldoffCount holdoff assert during inline UNDO rollback');
unlike($log,
    qr/terminated by signal|was terminated by signal|server closed the connection unexpectedly|PANIC/,
    'no backend crash logged during concurrent rollback/reclaim churn');

$node->safe_psql('postgres', 'DROP TABLE recno_ur') if $rc == 0;

done_testing();

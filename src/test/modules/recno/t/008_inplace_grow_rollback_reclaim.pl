#!/usr/bin/perl

# Copyright (c) 2021-2026, PostgreSQL Global Development Group

# Regression test for a RECNO/UNDO reclaim-vs-rollback crash in the inline
# per-relation UNDO apply path (ItemIdHasStorage assertion in bufpage.c).
#
# When a size-GROWING UPDATE is rolled back, the before-image is larger than
# the current on-page (new) tuple, so RelUndoApplyUpdate() takes its "old tuple
# larger" branch: it deletes the current item and re-adds the larger old tuple
# at the same offset.  It previously used the core PageIndexTupleDelete(), whose
# page-compaction loop asserts that EVERY remaining line pointer on the page has
# storage (Assert(ItemIdHasStorage(ii)), bufpage.c:1153).
#
# But a RECNO data page routinely carries storage-less LP_DEAD / LP_UNUSED line
# pointers left by a concurrent committed DELETE followed by opportunistic
# prune / VACUUM of *other* rows on the same page.  When the rolled-back UPDATE
# grows on a page that also holds such a reclaimed sibling, the compaction loop
# trips the assert and the backend (then the whole cluster, restart_after_crash
# off) goes down with:
#     TRAP: failed Assert("ItemIdHasStorage(ii)")  (bufpage.c:1153)
#     client backend terminated by signal 6: Aborted
#
# The rolled-back slot itself is still normal -- the reclamation gate correctly
# reclaimed the genuinely-dead sibling rows -- so this is NOT a reclamation-gate
# bug and must NOT be fixed by pinning (that would reintroduce bloat).  The fix
# routes the grow branch through the AM's LP_DEAD/LP_UNUSED-tolerant page delete
# (RecnoPageIndexTupleDelete, installed via RelUndoPageIndexTupleDelete_hook),
# exactly as RECNO's own delete/defrag sites already do.
#
# Reaching the inline RelUndoApplyChain grow branch (rather than the WAL-replay
# rollback path used for small transactions) requires transactions whose UNDO
# volume exceeds undo_instant_abort_threshold (65536 bytes).  We therefore roll
# back WIDE, growing multi-row range UPDATEs so one transaction's UNDO is large,
# racing committed DELETE + reINSERT churn and VACUUM prune of the same pages.
# Without the fix the cluster reliably crashes with the ItemIdHasStorage assert;
# with the fix the run completes and the server stays up.  A rolled-back UPDATE
# whose own slot was reclaimed still raises "tuple ... is not normal", which is
# caught and deferred to the ATM revert worker -- that is expected and benign.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Utils;
use Test::More;
use PostgreSQL::Test::Cluster;
use Time::HiRes qw(time usleep);
use POSIX qw(_exit);

my $node = PostgreSQL::Test::Cluster->new('recno_undo_grow_reclaim');
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

my $nwriters = 16;    # WIDE rolling-back growing updaters (large UNDO)
my $ndeleters = 6;    # committed delete/re-insert churn (reclaims slots)
my $nvacuum  = 3;     # concurrent VACUUM prune
my $run_secs = 30;

my $connstr = $node->connstr('postgres');

my @pids;

# Updater: BEGIN; UPDATE a WHOLE RANGE of ids, growing each row's varlena;
# ROLLBACK.  Updating ~300 rows in one transaction makes its UNDO exceed
# undo_instant_abort_threshold (65536), so the rollback is applied via the
# inline per-relation UNDO chain (RelUndoApplyChain -> RelUndoApplyUpdate),
# whose grow branch is the site that used the intolerant page-delete.
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
            $seed = ($seed * 1103515245 + 12345) & 0x7fffffff;
            my $lo = ($seed % ($nrows - 400)) + 1;
            my $hi = $lo + 300;                   # ~300-row txn -> big UNDO
            my $n  = 2000 + ($seed % 5000);       # grow (overflow-sized)
            # Mostly ROLLBACK so the inline UNDO grow-branch apply runs often.
            my $end = (($seed >> 16) % 4 == 0) ? 'COMMIT' : 'ROLLBACK';
            print $fh "BEGIN; UPDATE recno_ur SET c_data = repeat('${w}z', $n), "
              . "bal = bal - 0.7 WHERE id BETWEEN $lo AND $hi; $end;\n";
        }
        close($fh);
        _exit(0);
    }
    push @pids, $pid;
}

# Deleter: DELETE some ids and COMMIT, then re-INSERT them, so slots get
# reclaimed (LP_DEAD/LP_UNUSED, storage removed) by prune/VACUUM out from
# under the concurrent rolling-back updaters.
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

# VACUUM loop: prunes reclaimed slots (turns their ItemIds storage-less) and
# discards UNDO, racing the rolling-back updaters.
for my $v (0 .. $nvacuum - 1) {
    my $pid = fork();
    die "fork failed: $!" unless defined $pid;
    if ($pid == 0) {
        my $deadline = time() + $run_secs;
        while (time() < $deadline) {
            system('psql', '-X', '-q', '-d', $connstr, '-c', 'VACUUM recno_ur');
            usleep(120_000);
        }
        _exit(0);
    }
    push @pids, $pid;
}

waitpid($_, 0) for @pids;

# The cluster must still be up: the ItemIdHasStorage assert crash took the
# whole cluster down (restart_after_crash off).
my ($rc, $out, $err) = $node->psql('postgres', 'SELECT count(*) FROM recno_ur');
is($rc, 0, 'server still accepting queries after concurrent grow-rollback/reclaim churn')
    or diag("psql rc=$rc err=$err");

# No ItemIdHasStorage assert / crash in the server log.
my $log = slurp_file($node->logfile);
unlike($log,
    qr/TRAP: failed Assert\("ItemIdHasStorage/,
    'no ItemIdHasStorage assert during inline UNDO grow-branch rollback');
unlike($log,
    qr/terminated by signal|was terminated by signal|server closed the connection unexpectedly|PANIC/,
    'no backend crash logged during concurrent grow-rollback/reclaim churn');

$node->safe_psql('postgres', 'DROP TABLE recno_ur') if $rc == 0;

done_testing();

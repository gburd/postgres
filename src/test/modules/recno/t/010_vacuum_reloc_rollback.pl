#!/usr/bin/perl

# Copyright (c) 2021-2026, PostgreSQL Global Development Group

# Regression test for a RECNO/UNDO crash where a concurrent VACUUM relocates an
# in-flight (or version-carrying) in-place-updated tuple out from under a
# transaction whose rollback then runs inside proc_exit.
#
# ROOT CAUSE (proven by instrumentation + core-file backtrace):
#
#   Two independent defects combine into a cluster crash under TPC-C-shaped
#   load (multi-statement txns, hot-row in-place numeric UPDATEs, ~10-30%
#   rollback incl. clients that disconnect while idle-in-transaction, plus
#   concurrent VACUUM + committed-DELETE churn).
#
#   Defect A (correctness -- recno_operations.c, RecnoVacuumCrossPageDefrag):
#     VACUUM Phase IV-B relocates live tuples off tail pages onto front pages
#     to enable truncation, changing the moved tuple's TID and marking the old
#     line pointer LP_UNUSED.  Its eligibility gate skipped pages with
#     overflow / DELETED / non-self-ctid tuples but NOT tuples that are
#     RECNO_TUPLE_UNCOMMITTED (an in-flight in-place UPDATE/INSERT by a
#     still-running txn) or that carry a version pointer (UNDO-fork history
#     head).  RECNO keeps a tuple's TID across in-place UPDATE
#     (am_inplace_update_keeps_tid) and its UNDO records -- both the pending
#     rollback record of an uncommitted updater and the retained before-image
#     chain of a committed one -- reference that TID.  Relocating such a tuple
#     ORPHANS those records: a later abort's RelUndoApplyUpdate() targets the
#     vacated old offset and finds it non-normal.  (Directly observed:
#     "DIAG XPDefrag would-move rel=district tid=(1,13) flags=0x188 xmin=714
#     verptr_valid=1" -- flags 0x188 = UNCOMMITTED|HAS_VERSION_PTR|UPDATED --
#     immediately followed by "RelUndoApplyUpdate off=13 NOTNORMAL lp_flags=0".)
#
#   Defect B (crash safety -- xactundo.c, ApplyPerRelUndo):
#     Inline per-relation UNDO apply relies on its per-entry PG_CATCH to demote
#     an apply ERROR (e.g. the orphaned-slot "tuple is not normal" from Defect
#     A) to a background-ATM deferral.  But when the abort runs inside proc_exit
#     (a client disconnected while idle-in-transaction, so backend shutdown
#     reached ShutdownPostgres -> AbortOutOfAnyTransaction -> AbortTransaction
#     -> AtAbort_XactUndo -> ApplyPerRelUndo), errfinish() promotes the ERROR to
#     FATAL (proc_exit_inprogress is set; elog.c), longjmp'ing straight back
#     into proc_exit and BYPASSING the PG_CATCH.  The re-entrant proc_exit runs
#     pgstat_shutdown_hook -> pgstat_report_stat, which trips
#         TRAP: failed Assert("!IsTransactionOrTransactionBlock()")  (pgstat.c)
#         client backend terminated by signal 6: Aborted
#     and, with restart_after_crash off, takes the whole cluster down (a bare
#     SIGSEGV on a production -O2 build with no cassert).
#
#   Backtrace (cassert+debug core):
#     pgstat_report_stat <- pgstat_shutdown_hook <- shmem_exit <- proc_exit
#       <- errfinish <- RelUndoApplyUpdate <- RelUndoApplyOneRecord
#       <- RelUndoApplyChain <- ApplyPerRelUndo <- AtAbort_XactUndo
#       <- AbortTransaction <- AbortOutOfAnyTransaction <- ShutdownPostgres
#       <- shmem_exit <- proc_exit <- PostgresMain
#
# THE FIX:
#   A: RecnoVacuumCrossPageDefrag skips relocating any tuple that is
#      RECNO_TUPLE_UNCOMMITTED or carries a valid version pointer, so no tuple
#      whose TID an UNDO record references is ever moved.  (Root cause: stops
#      the orphaning at the source; a later VACUUM pass relocates the tuple
#      once the updater has committed/aborted and the version pointer is
#      discarded.)
#   B: ApplyPerRelUndo defers ALL per-relation UNDO to the background revert
#      worker when proc_exit_inprogress, instead of applying inline where a
#      corruption/reclaimed-slot ERROR would be promoted to FATAL.  (Defence in
#      depth: the ATM worker's idempotent already-applied check makes the
#      deferral safe, and there is no lost-update window to close for a
#      terminating backend.)
#
# NON-VACUITY (measured on this tree, cassert+debug, restart_after_crash off):
#   - Both fixes reverted (original): Part A crashes 5/5, stress crashes 2/2,
#     always with "FATAL: RelUndoApplyUpdate ... is not normal" ->
#     Assert("!IsTransactionOrTransactionBlock()") -> signal 6.
#   - Defect A reverted, Defect B present: server survives but the orphaned
#     UNDO record ERRORs forever in the background revert worker (rollback
#     never completes -- correctness degraded, no crash).  Proves both fixes
#     are load-bearing.
#   - Both fixes present: Part A 5/5 clean + rollback restores the row's value,
#     stress clean.
#
# RELIABILITY NOTE:
#   Part A is a DETERMINISTIC single-shot reproduction (5/5 without the fix,
#   0/5 with it): it builds a multi-page table, deletes the front rows to leave
#   reclaimable front space, updates a tail-page row IN PLACE inside an open
#   transaction, runs VACUUM (which relocates that in-flight tail row -- Defect
#   A -- vacating its old TID), then disconnects the session while
#   idle-in-transaction so its rollback runs inside proc_exit (Defect B).
#   Part B is a best-effort TPC-C-shaped stress that reproduced the crash ~1:1
#   at this scale before the fix.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Utils;
use Test::More;
use PostgreSQL::Test::Cluster;
use Time::HiRes qw(time usleep);
use POSIX qw(_exit);

my $node = PostgreSQL::Test::Cluster->new('recno_vacuum_reloc_rollback');
$node->init;
# autovacuum off so the test drives VACUUM timing precisely in Part A; small
# shared_buffers keeps everyone on the hot pages; restart_after_crash off so a
# single backend crash tears the cluster down and the "server up" checks fail
# loudly.
$node->append_conf('postgresql.conf', qq{
autovacuum = off
shared_buffers = 8MB
restart_after_crash = off
deadlock_timeout = 200ms
log_min_messages = warning
});
$node->start;

my $connstr  = $node->connstr('postgres');
my $psql_bin = 'psql';

# ---------------------------------------------------------------------------
# Part A: deterministic reproduction.
#
# Relocate an in-flight in-place-updated tuple with VACUUM, then roll back the
# updater from inside proc_exit.  Without either fix this crashes the cluster
# on the first shot; with the fixes the tuple is not relocated (Defect A guard)
# and/or the rollback is deferred to the ATM worker (Defect B guard), and the
# in-place UPDATE is correctly rolled back.
# ---------------------------------------------------------------------------
$node->safe_psql('postgres', qq{
    CREATE TABLE reloc_t (id int, v numeric, pad text)
        USING recno WITH (fillfactor = 90);
    INSERT INTO reloc_t
        SELECT g, g::numeric, repeat('p', 200) FROM generate_series(1, 4000) g;
    -- Delete the front rows: after VACUUM their space is reclaimable front
    -- free space, the relocation target for the surviving tail rows.
    DELETE FROM reloc_t WHERE id <= 3950;
});

my $tail_id = $node->safe_psql('postgres',
    'SELECT id FROM reloc_t ORDER BY (ctid::text::point)[0] DESC, id LIMIT 1');
my $v_before = $node->safe_psql('postgres',
    "SELECT v FROM reloc_t WHERE id = $tail_id");

# Session S1: open a transaction, UPDATE the tail-page row IN PLACE, and hold
# the transaction open (do NOT commit/rollback).  We feed it through a pipe so
# we can run a concurrent VACUUM and then close the pipe -- an abrupt disconnect
# while idle-in-transaction, which drives backend cleanup through proc_exit.
my $sql = "BEGIN;\nUPDATE reloc_t SET v = v + 0.5 WHERE id = $tail_id;\n";
my $s1_pid = open(my $s1_fh, '|-', $psql_bin, '-X', '-q', '-d', $connstr,
    '-v', 'ON_ERROR_STOP=0')
  or die "could not spawn S1 psql: $!";
print $s1_fh $sql;
$s1_fh->flush;
usleep(750_000);    # let the in-place UPDATE land and stay uncommitted

# Concurrent VACUUM: relocates the in-flight tail row (Defect A) unless guarded.
$node->safe_psql('postgres', 'VACUUM reloc_t');
usleep(500_000);

# Disconnect S1 while idle-in-transaction -> proc_exit -> AbortOutOfAnyTransaction
# -> inline per-relation UNDO apply (Defect B path).
close($s1_fh);
waitpid($s1_pid, 0);
usleep(750_000);

my $a_alive = $node->safe_psql('postgres', 'SELECT 1');
is($a_alive, '1',
    'Part A: server survives VACUUM-relocation + rollback-in-proc_exit');

# The in-place UPDATE must have been rolled back (value restored), the row must
# be present exactly once, and every numeric must be well-formed.
my $v_after = $node->safe_psql('postgres',
    "SELECT v FROM reloc_t WHERE id = $tail_id");
is($v_after, $v_before,
    "Part A: aborted in-place UPDATE rolled back (v restored to $v_before)");

my $tail_count = $node->safe_psql('postgres',
    "SELECT count(*) FROM reloc_t WHERE id = $tail_id");
is($tail_count, '1', 'Part A: relocated-then-rolled-back row present exactly once');

my ($rc_a, $out_a, $err_a) = $node->psql('postgres',
    'SELECT count(*), count(v) FROM reloc_t');
is($rc_a, 0, 'Part A: full-table numeric read succeeds');
like($out_a, qr/^50\|50$/, 'Part A: all 50 surviving rows present, v well-formed');

# ---------------------------------------------------------------------------
# Part B: TPC-C-shaped stress (best-effort breadth).
#
# Several backends run multi-statement transactions with SELECT FOR UPDATE on a
# hot district-like row (numeric increment), in-place numeric UPDATEs, multi-row
# UPDATEs, size-changing varlena appends, and ~10-30% rollback -- including a
# dedicated pool of "abandon" backends that open a transaction, do an in-place
# UPDATE, then disconnect while idle-in-transaction (the Defect B trigger).  A
# concurrent VACUUM + committed-DELETE churn stream races to relocate/reclaim
# the same pages (the Defect A trigger).
# ---------------------------------------------------------------------------
my $wh = 8;
$node->safe_psql('postgres', qq{
    CREATE TABLE district (w_id int, d_id int, d_ytd numeric, d_next_o_id int)
        USING recno WITH (fillfactor = 60);
    CREATE TABLE customer (w_id int, d_id int, c_id int, c_bal numeric, c_data text)
        USING recno WITH (fillfactor = 50);
    CREATE TABLE stock (w_id int, i_id int, s_qty numeric, s_ytd numeric)
        USING recno WITH (fillfactor = 60);
    CREATE TABLE order_line (w_id int, d_id int, o_id int, ol_amt numeric, ol_deliv text)
        USING recno WITH (fillfactor = 70);
    INSERT INTO district SELECT w, d, 30000.0, 3001
        FROM generate_series(1,$wh) w, generate_series(1,10) d;
    INSERT INTO customer SELECT w, d, c, 0.0, repeat('c',60)
        FROM generate_series(1,$wh) w, generate_series(1,10) d, generate_series(1,30) c;
    INSERT INTO stock SELECT w, i, 100.0, 0.0
        FROM generate_series(1,$wh) w, generate_series(1,200) i;
    INSERT INTO order_line SELECT w, d, o, 10.0, NULL
        FROM generate_series(1,$wh) w, generate_series(1,10) d, generate_series(1,20) o;
});

# Spawn $n independent psql-fed worker processes of kind $kind until $deadline.
# POSIX::_exit so the inherited Cluster END/DESTROY handler does not run in the
# child and shut the shared node down.
sub spawn
{
    my ($n, $kind, $deadline, $tag) = @_;
    my @pids;
    for my $w (0 .. $n - 1)
    {
        my $pid = fork();
        die "fork failed: $!" unless defined $pid;
        if ($pid == 0)
        {
            my $seed = ($w + 1) * 7919 + $tag * 104729 + 1;
            while (time() < $deadline)
            {
                my $ok = open(my $fh, '|-', $psql_bin, '-X', '-q', '-d', $connstr,
                    '-v', 'ON_ERROR_STOP=0');
                _exit(0) unless $ok;
                for (1 .. 25)
                {
                    $seed = ($seed * 1103515245 + 12345) & 0x7fffffff;
                    my $wid = ($seed % $wh) + 1;
                    $seed = ($seed * 1103515245 + 12345) & 0x7fffffff;
                    my $d = ($seed % 10) + 1;
                    $seed = ($seed * 1103515245 + 12345) & 0x7fffffff;
                    my $c = ($seed % 30) + 1;
                    $seed = ($seed * 1103515245 + 12345) & 0x7fffffff;
                    my $i = ($seed % 200) + 1;
                    $seed = ($seed * 1103515245 + 12345) & 0x7fffffff;
                    my $roll = $seed % 100;
                    $seed = ($seed * 1103515245 + 12345) & 0x7fffffff;
                    my $len = ($seed % 400) + 5;

                    if ($kind eq 'txn')
                    {
                        # multi-statement txn: hot-row FOR UPDATE numeric incr,
                        # in-place numeric UPDATEs, multi-row UPDATE, varlena
                        # append; ~15% rollback.
                        print $fh "BEGIN;\n";
                        print $fh "SELECT d_next_o_id, d_ytd - 1 FROM district "
                          . "WHERE w_id=$wid AND d_id=$d FOR UPDATE;\n";
                        print $fh "UPDATE district SET d_ytd = d_ytd + 10.0, "
                          . "d_next_o_id = d_next_o_id + 1 WHERE w_id=$wid AND d_id=$d;\n";
                        print $fh "UPDATE stock SET s_qty = s_qty - 1, s_ytd = s_ytd + 1 "
                          . "WHERE w_id=$wid AND i_id=$i;\n";
                        print $fh "UPDATE customer SET c_bal = c_bal - 10.0, "
                          . "c_data = repeat('x',$len) WHERE w_id=$wid AND d_id=$d AND c_id=$c;\n";
                        print $fh "UPDATE order_line SET ol_amt = ol_amt + 0.5, "
                          . "ol_deliv = 'now' WHERE w_id=$wid AND d_id=$d;\n";
                        print $fh(($roll < 15) ? "ROLLBACK;\n" : "COMMIT;\n");
                    }
                    else    # 'abandon': open txn, in-place UPDATE, disconnect
                            # idle-in-transaction (no COMMIT/ROLLBACK) -> the
                            # proc_exit rollback path (Defect B trigger).
                    {
                        print $fh "BEGIN;\n";
                        print $fh "UPDATE district SET d_ytd = d_ytd + 1.0 "
                          . "WHERE w_id=$wid AND d_id=$d;\n";
                        print $fh "UPDATE customer SET c_bal = c_bal - 1.0 "
                          . "WHERE w_id=$wid AND d_id=$d AND c_id=$c;\n";
                        last;    # close pipe -> disconnect while in transaction
                    }
                }
                close($fh);
            }
            _exit(0);
        }
        push @pids, $pid;
    }
    return @pids;
}

sub churn_kids
{
    my ($n, $deadline) = @_;
    my @pids;
    for my $w (0 .. $n - 1)
    {
        my $pid = fork();
        die "fork failed: $!" unless defined $pid;
        if ($pid == 0)
        {
            while (time() < $deadline)
            {
                PostgreSQL::Test::Utils::system_log($psql_bin, '-X', '-q',
                    '-d', $connstr, '-v', 'ON_ERROR_STOP=0', '-c',
                    'VACUUM (SKIP_LOCKED) district, customer, stock, order_line');
                usleep(150_000);
            }
            _exit(0);
        }
        push @pids, $pid;
    }
    return @pids;
}

my $round_secs = 15;
my $deadline   = time() + $round_secs;
my @pids;
push @pids, spawn(10, 'txn',     $deadline, 1);
push @pids, spawn(6,  'abandon', $deadline, 2);
push @pids, churn_kids(2, $deadline);

# Main process also drives EPQ + reads numerics.
while (time() < $deadline)
{
    $node->psql('postgres',
        'UPDATE district SET d_ytd = d_ytd - 0.1 WHERE d_ytd - 1 > 0');
    $node->psql('postgres', 'SELECT sum(d_ytd - 0.5) FROM district');
    usleep(2000);
}
waitpid($_, 0) for @pids;

my $b_alive = $node->safe_psql('postgres', 'SELECT 1');
is($b_alive, '1', 'Part B: server survives TPC-C-shaped churn + VACUUM relocation');

my ($rc_b, $out_b) = $node->psql('postgres',
    'SELECT count(*), count(d_ytd) FROM district');
is($rc_b, 0, 'Part B: full-table numeric read of district succeeds');
like($out_b, qr/^${\($wh*10)}\|${\($wh*10)}$/,
    'Part B: all district rows present and d_ytd well-formed');

# No crash / promoted-FATAL / torn-numeric signature in the server log.
my $log = slurp_file($node->logfile);
unlike(
    $log,
    qr/TRAP: failed Assert|terminated by signal|was terminated|server closed the connection unexpectedly|is not normal.*FATAL|PANIC/,
    'no backend crash / promoted-FATAL / assert in server log');

$node->safe_psql('postgres',
    'DROP TABLE reloc_t, district, customer, stock, order_line');

done_testing();

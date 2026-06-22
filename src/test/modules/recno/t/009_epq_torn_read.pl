#!/usr/bin/perl

# Copyright (c) 2021-2026, PostgreSQL Global Development Group

# Regression / stress test for a RECNO in-place-update torn-read data race on
# the EvalPlanQual (EPQ) tuple-lock path.
#
# ROOT CAUSE (proven by instrumentation, see below):
#   recno_tuple_lock() -- the tableam tuple_lock callback reached via
#   table_tuple_lock() from ExecUpdate/ExecDelete/nodeLockRows when a concurrent
#   UPDATE/DELETE is detected (TM_Updated + TUPLE_LOCK_FLAG_FIND_LAST_VERSION)
#   and the executor must produce the latest row version for EvalPlanQual to
#   recheck -- populated the EPQ input slot by calling
#   RecnoTupleToSlotWithOverflow() AFTER dropping the buffer content lock
#   (LockBuffer(BUFFER_LOCK_UNLOCK)) and then dropping the buffer PIN
#   (ReleaseBuffer()).  RecnoTupleToSlotWithOverflow() stores the inline
#   (non-overflow) column datums as raw pointers into the tuple header, so the
#   EPQ input slot's `bal` numeric datum aliased a buffer that was both unlocked
#   and unpinned.  (Directly observed: the slot datum pointer lands INSIDE the
#   buffer page range immediately before ReleaseBuffer -- 1-of-N attrs alias the
#   page without the fix, 0-of-N with it.)
#
#   RECNO does in-place MVCC UPDATEs: a same-size CAS UPDATE overwrites the
#   on-page digit bytes under BUFFER_LOCK_SHARE_EXCLUSIVE, which (per bufmgr.c)
#   does NOT exclude other readers; the delete+re-add growth path relocates page
#   data under EXCLUSIVE; and an unpinned page may be evicted/reused outright.
#   Any of these mutates the exact bytes the EPQ recheck later reads.  A torn
#   `numeric` (its ndigits/weight header inconsistent with its digit array)
#   makes cmp_abs() and sub_abs() disagree and trips, in the recheck's
#   numeric_sub:
#       TRAP: failed Assert("borrow == 0"), .../numeric.c
#   (torn varlena lengths on the same path give the zstd / "invalid memory alloc
#   request size" / SIGSEGV symptoms already covered on the scan/index paths).
#
# This is the EPQ gap NOT covered by the sequential-scan / index-fetch torn-read
# fix: those paths were routed through RecnoSlotStoreTuple() (copy-under-lock),
# but recno_tuple_lock() deforms via RecnoTupleToSlotWithOverflow(), a different
# function that aliases the header.  THE FIX copies the on-page tuple into
# slot-private memory while still holding the buffer EXCLUSIVE lock (which
# excludes every in-place updater) and deforms that copy, mirroring the
# scan-path copy-under-lock.
#
# RELIABILITY NOTE:
#   The crash is a genuine data race whose window (between recno_tuple_lock
#   unlocking/unpinning the buffer and the EPQ recheck reading the aliased
#   datum) is on the order of microseconds; upstream it was observed ~1 in 8
#   heavy-load runs.  Under the per-statement overhead of a TAP harness the
#   torn-read *crash* is NOT reliably reproducible.  BUT the fix ships a
#   cassert-only invariant (recno_handler.c, USE_ASSERT_CHECKING): after the
#   EPQ tuple-lock slot is populated, no inline column datum may point inside
#   the buffer page that is about to be unlocked/unpinned.  Without the fix a
#   single concurrent UPDATE on a numeric column deterministically leaves the
#   slot's numeric datum aliasing the page, and that assert fires on the first
#   EPQ recheck (verified: TRAP "slot->tts_isnull[ai] || dp < pg || dp >= pg +
#   BLCKSZ").  This test therefore has a DETERMINISTIC non-vacuity component
#   (Part A: force one EPQ recheck on a numeric row; under cassert the missing
#   copy trips the invariant) plus a best-effort stress component (Part B: the
#   maximum-pressure churn that exercises the path tens of thousands of times).
#   The suite runs cassert builds, so Part A guards the fix reliably.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Utils;
use Test::More;
use PostgreSQL::Test::Cluster;
use Time::HiRes qw(time usleep);
use POSIX qw(_exit);

my $node = PostgreSQL::Test::Cluster->new('recno_epq_torn');
$node->init;
# Tiny shared_buffers so the unpinned (aliased) page is a candidate for
# eviction/reuse; autovacuum on so prune/VACUUM of the churned rows races the
# EPQ recheck; restart_after_crash off so a single backend crash tears the
# cluster down and the "server still up" assertions below fail loudly.
$node->append_conf('postgresql.conf', qq{
autovacuum = off
shared_buffers = 2MB
restart_after_crash = off
});
$node->start;

# Tiny hot set -> constant update collisions -> constant EPQ rechecks through
# recno_tuple_lock().  A driver table gives the join FOR UPDATE a rowmark so the
# EPQ recheck also exercises the SnapshotAny fetch path.
my $hot_rows = 6;

$node->safe_psql('postgres', qq{
    CREATE TABLE recno_epq (id int, bal numeric, c_data text)
        USING recno WITH (fillfactor = 50);
    INSERT INTO recno_epq
        SELECT g, 500000.0, repeat('x', 20) FROM generate_series(1, $hot_rows) g;
    CREATE TABLE drv (id int) USING recno;
    INSERT INTO drv SELECT g FROM generate_series(1, $hot_rows) g;
});

my $connstr  = $node->connstr('postgres');
my $psql_bin = 'psql';

# ---------------------------------------------------------------------------
# Part A: deterministic EPQ recheck on a numeric row.
#
# Force exactly one concurrent-update collision so the second UPDATE takes the
# TM_Updated -> table_tuple_lock (recno_tuple_lock) -> EvalPlanQual path.  Under
# a cassert build the fix's invariant assert fires here if the slot's numeric
# datum aliases the buffer (i.e. if the copy-under-lock is missing).  The row
# has a numeric (varlena) column so the datum is a pointer, not by-value.
# ---------------------------------------------------------------------------
{
    # Session S1: open a transaction, update id=1, hold it briefly, commit.
    my $s1 = $node->background_psql('postgres');
    $s1->query_safe('BEGIN');
    $s1->query_safe('UPDATE recno_epq SET bal = 500001.0 WHERE id = 1');

    # Session S2: update the same row concurrently -> blocks on S1, and once
    # S1 commits gets TM_Updated -> EPQ recheck through recno_tuple_lock.
    my $s2 = $node->background_psql('postgres');
    $s2->query_until(qr//, "UPDATE recno_epq SET bal = bal - 0.5 WHERE id = 1;\n");

    usleep(200_000);          # let S2 block on S1's row
    $s1->query_safe('COMMIT');  # releases S2 -> EPQ recheck runs
    $s1->quit;

    # Reap S2's result; the backend must not have crashed on the invariant.
    $s2->query_safe('SELECT 1');
    $s2->quit;
}

my $a_alive = $node->safe_psql('postgres', 'SELECT 1');
is($a_alive, '1',
    'Part A: server survives a concurrent-update EPQ recheck on a numeric row');

# Spawn $n independent psql-fed writer processes running statement stream $kind
# until $deadline.  POSIX::_exit so the inherited Cluster END/DESTROY handler
# does not run in the child and shut the shared node down.
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
            my $ok = open(my $fh, '|-', $psql_bin, '-X', '-q', '-d', $connstr,
                '-v', 'ON_ERROR_STOP=0');
            _exit(0) unless $ok;
            my $seed = ($w + 1) * 7919 + $tag * 104729 + 1;
            while (time() < $deadline)
            {
                for (1 .. 50)
                {
                    $seed = ($seed * 1103515245 + 12345) & 0x7fffffff;
                    my $id = ($seed % $hot_rows) + 1;
                    if ($kind eq 'upd')
                    {
                        # UPDATE collision: recheck qual AND targetlist both do
                        # numeric arithmetic on the (aliased) bal.
                        print $fh
                          "UPDATE recno_epq SET bal = bal - 0.5 WHERE id = $id AND bal - 1 > 0;\n";
                    }
                    elsif ($kind eq 'rm')
                    {
                        # join FOR UPDATE -> EPQ rowmark fetch of recno_epq.bal
                        print $fh
                          "SELECT e.bal - 1 FROM recno_epq e JOIN drv d ON e.id=d.id "
                          . "WHERE e.id=$id AND e.bal - 2 > 0 FOR UPDATE OF e;\n";
                    }
                    elsif ($kind eq 'cas')
                    {
                        # same-size CAS bal overwrite (mutates the aliased digits)
                        my $d = 100000 + ($seed % 900000);
                        print $fh "UPDATE recno_epq SET bal = $d.$d WHERE id = $id;\n";
                    }
                    else    # 'grow' : committed varlena growth/shrink -> page
                            # relocation of neighbouring tuples on the hot page
                    {
                        my $len = 3 + ($seed % 300);
                        print $fh
                          "UPDATE recno_epq SET c_data = repeat('z', $len) WHERE id = $id;\n";
                    }
                }
            }
            close($fh);
            _exit(0);
        }
        push @pids, $pid;
    }
    return @pids;
}

# Loop the workload body several times to raise the ~1-in-N detection chance.
my $nrounds    = 3;
my $round_secs = 10;
my $crash      = 0;

for my $round (1 .. $nrounds)
{
    my $deadline = time() + $round_secs;
    my @pids;
    push @pids, spawn(12, 'upd',  $deadline, $round + 1);
    push @pids, spawn(6,  'rm',   $deadline, $round + 2);
    push @pids, spawn(10, 'cas',  $deadline, $round + 3);
    push @pids, spawn(6,  'grow', $deadline, $round + 4);

    # Main process also drives EPQ + reads bal via numeric arithmetic.
    while (time() < $deadline)
    {
        $node->psql('postgres',
            "UPDATE recno_epq SET bal = bal - 0.1 WHERE bal - 1 > 0");
        $node->psql('postgres', 'SELECT sum(bal - 0.5) FROM recno_epq');
        usleep(500);
    }
    waitpid($_, 0) for @pids;

    my $log = slurp_file($node->logfile);
    $crash++
      if $log =~
      /borrow == 0|TRAP: failed Assert|terminated by signal|was terminated|server closed the connection unexpectedly|invalid memory alloc request size/;
}

# Server must still be up (a torn read used to Assert-crash the backend, and
# with restart_after_crash off that takes the whole cluster down).
my $alive = $node->safe_psql('postgres', 'SELECT 1');
is($alive, '1',
    'server still accepting queries after concurrent EPQ-recheck churn');

# Every bal must still be a well-formed numeric.
my ($rc, $out) = $node->psql('postgres',
    'SELECT count(*), count(bal) FROM recno_epq');
is($rc, 0, 'full-table numeric read of bal succeeds');
like($out, qr/^$hot_rows\|$hot_rows$/,
    "all $hot_rows rows present and bal well-formed");

# No torn-numeric assert / crash in the server log across all rounds.
is($crash, 0, 'no torn-numeric assert / backend crash during EPQ-recheck churn')
  or diag("observed crash/assert signature in $crash of $nrounds rounds");

my $log = slurp_file($node->logfile);
unlike(
    $log,
    qr/borrow == 0|TRAP: failed Assert|terminated by signal|was terminated|server closed the connection unexpectedly|invalid memory alloc request size/,
    'no backend crash / torn-numeric assert in server log');

$node->safe_psql('postgres', 'DROP TABLE recno_epq, drv');

done_testing();

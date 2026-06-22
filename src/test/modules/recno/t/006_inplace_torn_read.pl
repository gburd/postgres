#!/usr/bin/perl

# Copyright (c) 2021-2026, PostgreSQL Global Development Group

# Regression test for a RECNO in-place-update torn-read data race.
#
# RECNO performs in-place MVCC UPDATEs: a committed updater overwrites the
# on-page bytes of a live tuple.  The page-mode sequential scan and the
# index-fetch paths formerly parked a pointer into the (pinned but NOT
# content-locked) buffer in the scan slot and deformed/decompressed directly
# from it -- copying heap's page-mode pattern, which is only safe because heap
# tuples are immutable.  Under RECNO an updater running concurrently with a
# reader mutates the exact bytes being deformed, so a reader that scanned a
# compressible/overflowing varlena column while writers grew and shrank it saw:
#   * zstd "unknown frame descriptor" / "destination buffer is too small"
#     (compressed header paired with a different version's payload),
#   * "invalid memory alloc request size N" (torn overflow-pointer length),
#   * failed Assert("(data - start) == data_size") in heap_fill_tuple / SIGSEGV
#     as a torn varlena length propagated into tuple re-forming.
#
# This test needs REAL concurrency (a reader mid-deform while a writer
# overwrites), so it launches independent OS processes -- looping psql writers
# that grow/shrink a varlena column (some values large enough to spill to the
# overflow area, mimicking TPC-C c_data) -- and then runs full-table scans that
# deform that column from the main process.  Without the fix the scans reliably
# error out or the backend crashes; with the fix the run completes clean.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Utils;
use Test::More;
use PostgreSQL::Test::Cluster;
use Time::HiRes qw(time usleep);
use POSIX qw(_exit);

my $node = PostgreSQL::Test::Cluster->new('recno_torn_read');
$node->init;
# Small buffers so readers and writers contend on the same cached pages; no
# autovacuum so nothing widens or narrows the race window behind our backs.
$node->append_conf('postgresql.conf', qq{
autovacuum = off
shared_buffers = 32MB
});
$node->start;

$node->safe_psql('postgres', q{
    CREATE TABLE recno_tr (id int, bal numeric, c_data text)
        USING recno WITH (fillfactor = 30);
    INSERT INTO recno_tr
        SELECT g, 1000.0, repeat('x', 40) FROM generate_series(1, 2000) g;
});

my $nrows    = 2000;
my $nwriters = 8;
my $run_secs = 12;

my $connstr = $node->connstr('postgres');
my $psql_bin = 'psql';

# Each writer is an independent psql process fed a stream of size-changing
# UPDATE transactions for $run_secs seconds.  Running as detached processes
# gives true concurrency with the scanning main process.
my @writer_pids;
for my $w (0 .. $nwriters - 1) {
    my $pid = fork();
    die "fork failed: $!" unless defined $pid;
    if ($pid == 0) {
        # Child: loop UPDATEs until the deadline via a single psql fed on stdin.
        # Use POSIX::_exit so the inherited PostgreSQL::Test::Cluster END/DESTROY
        # handler does NOT run in the child and shut down the shared node.
        my $deadline = time() + $run_secs;
        my $ok = open(my $fh, '|-', $psql_bin, '-X', '-q', '-d', $connstr,
             '-v', 'ON_ERROR_STOP=0');
        if (!$ok) { _exit(0); }
        my $seed = $w * 7919 + 1;
        while (time() < $deadline) {
            $seed = ($seed * 1103515245 + 12345) & 0x7fffffff;
            my $id  = ($seed % $nrows) + 1;
            my $cls = ($seed >> 8) % 4;
            my $n = $cls == 0 ? 20 + ($seed % 80)
                  : $cls == 1 ? 200 + ($seed % 400)
                  : $cls == 2 ? 2000 + ($seed % 5000)   # forces overflow
                  :             3 + ($seed % 10);        # shrink
            my $end = (($seed >> 16) % 5 == 0) ? 'ROLLBACK' : 'COMMIT';
            print $fh
                "BEGIN; UPDATE recno_tr SET c_data = repeat('${w}z', $n), "
              . "bal = bal - 0.7 WHERE id = $id; $end;\n";
        }
        close($fh);
        _exit(0);
    }
    push @writer_pids, $pid;
}

# Main process: hammer full-table deforms of the churned column while the
# writers run.  A torn read surfaces as a query error or a lost connection
# (backend crash).  We tolerate serialization/other transient errors but a
# crash or torn-read corruption fails the test.
my $errors      = 0;
my $crash       = 0;
my $deadline    = time() + $run_secs;
my $iterations  = 0;
while (time() < $deadline) {
    my ($rc, $out, $err) = $node->psql('postgres',
        'SELECT count(*), sum(length(c_data)) FROM recno_tr');
    $iterations++;
    if ($rc != 0) {
        # A clean, expected error would be rare here; a torn read shows up as a
        # data-corruption error or a lost connection.
        if ($err =~ /zstd|invalid memory alloc request size|unexpected chunk|could not|server closed|terminating connection because of crash/) {
            $errors++;
        }
    }
    # md5 forces a full detoast/decompress of the varlena on every row.
    ($rc, $out, $err) = $node->psql('postgres',
        'SELECT count(md5(c_data)) FROM recno_tr');
    if ($rc != 0
        && $err =~ /zstd|invalid memory alloc request size|server closed|terminating connection because of crash/) {
        $errors++;
    }
    usleep(1000);
}

waitpid($_, 0) for @writer_pids;

# Server must still be up (a torn read used to SIGSEGV the backend/postmaster).
my $alive = $node->safe_psql('postgres', 'SELECT 1');
is($alive, '1', 'server still accepting queries after concurrent size-changing churn');

# Every row must still deform cleanly (no torn varlena / compressed payload).
my ($rc, $out) = $node->psql('postgres',
    'SELECT count(*), count(md5(c_data)) FROM recno_tr');
is($rc, 0, 'full-table detoast/decompress of every row succeeds');
like($out, qr/^2000\|2000$/, 'all 2000 rows present and well-formed');

# No torn-read corruption error observed by the scanning process.
is($errors, 0, 'no torn-read decompression / alloc / crash errors during the run')
    or diag("observed $errors corruption/crash errors across $iterations scan iterations");

# No crash/assert lines in the server log.
my $log = slurp_file($node->logfile);
unlike($log,
    qr/terminated by signal|TRAP: failed Assert|invalid memory alloc request size|server closed the connection unexpectedly|was terminated by signal/,
    'no backend crash or torn-read corruption logged');

$node->safe_psql('postgres', 'DROP TABLE recno_tr');

done_testing();

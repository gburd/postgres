#!/usr/bin/perl

# Copyright (c) 2021-2026, PostgreSQL Global Development Group

# Deterministic regression test for a RECNO same-size CAS in-place-update
# torn-read that walks a reader's deform loop off the page (SIGSEGV).
#
# RECNO's fast-path UPDATE (recno_tuple_update, the "CAS" path) overwrites a
# live tuple in place with a NON-atomic memcpy while holding the buffer content
# lock plus a per-tuple t_writer CAS lock.  This is a district-shaped hot-row
# update: a fixed-length numeric (d_ytd) whose value changes but whose on-disk
# length is constant stays SAME-SIZE, so it takes the CAS fast path -- exactly
# the TPC-C district update.
#
# The read paths (seqscan, index-fetch) copy the on-page tuple into slot-private
# memory before deforming.  They copy under BUFFER_LOCK_SHARE.  The bug: the CAS
# writer held BUFFER_LOCK_SHARE_EXCLUSIVE for the overwrite, and bufmgr treats
# SHARE and SHARE_EXCLUSIVE as COMPATIBLE (one writer + many readers).  So a
# reader could copy a tuple mid-overwrite and capture a TORN image: a
# half-updated varlena length word.  The deform loop then reads a garbage
# VARSIZE and advances its data pointer off the tuple -- and off the page -- so
# the next VARSIZE_ANY faults (the field crash: SIGSEGV in VARSIZE_ANY under
# "select max(d_id) from district").
#
# The fix takes BUFFER_LOCK_EXCLUSIVE (not SHARE_EXCLUSIVE) for the CAS in-place
# overwrite: EXCLUSIVE conflicts with the readers' SHARE lock, so a reader waits
# for the (short) overwrite to finish and always copies a complete tuple, the
# same discipline heap_update uses.  The update stays on the same-size CAS fast
# path (only the lock mode changed, not eligibility).
#
# DETERMINISM.  The natural race is far too tight to reproduce by luck (two
# ~200-byte memcpys must overlap on the same tuple).  A blind pg_usleep window
# proves nothing.  Instead this test uses a WAIT injection point,
# "recno-cas-torn-wait", wired into the CAS overwrite site: when attached, the
# writer scribbles a garbage 0xEE tail into the LIVE on-page tuple, then BLOCKS
# on the injection point (holding the buffer content lock) until the test wakes
# it, then restores the clean bytes.  While the writer is parked mid-tear we run
# a full deform of that exact row in a separate reader session:
#   * WITHOUT the fix (SHARE_EXCLUSIVE): the reader's SHARE copy is not excluded,
#     it copies the torn image, and the deform-bounds oracle Assert in
#     tts_recno_deform / RecnoTupleToSlotWithOverflow trips (or, without asserts,
#     VARSIZE_ANY SIGSEGVs) -- the backend goes down.
#   * WITH the fix (EXCLUSIVE): the reader's SHARE lock conflicts with the
#     writer's EXCLUSIVE lock, so the reader BLOCKS until we wake the writer and
#     it restores + commits a clean tuple; the reader then copies a clean image.
# The wait point guarantees the reader meets the torn image (no fix) or is
# blocked out of it (fix) -- no reliance on timing or overlap luck.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Utils;
use PostgreSQL::Test::Cluster;
use Test::More;
use Time::HiRes qw(usleep);

if (($ENV{enable_injection_points} // '') ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

my $node = PostgreSQL::Test::Cluster->new('recno_cas_torn');
$node->init;
$node->append_conf('postgresql.conf', qq{
autovacuum = off
shared_buffers = 32MB
});
$node->start;

plan skip_all => 'Extension injection_points not installed'
  unless $node->check_extension('injection_points');

$node->safe_psql('postgres', 'CREATE EXTENSION injection_points;');

# district-shaped RECNO table: int key + numerics + varchars + fixed char, 11
# attrs.  Rows start with a constant-width 6-significant-digit numeric so every
# UPDATE below keeps d_ytd the same on-disk length -> stays on the CAS fast
# path.  A trailing wide varchar (d_name) is what the reader deforms across, so
# a torn length word there walks the deform pointer off the tuple.
$node->safe_psql('postgres', q{
    CREATE TABLE district (
      d_id int, d_w_id int, d_name varchar(10),
      d_street_1 varchar(20), d_street_2 varchar(20), d_city varchar(20),
      d_state char(2), d_zip char(9), d_tax numeric, d_ytd numeric, d_next_o_id int
    ) USING recno WITH (fillfactor = 30);
    INSERT INTO district
      SELECT g, 1, 'r'||g||'n00000', 's1', 's2', 'city', 'CA', '90210',
             0.1, 100000.99, 3001
      FROM generate_series(1, 10) g;
});

# The row we tear.  A SAME-SIZE update: 6-significant-digit numeric stays the
# same on-disk width, d_name stays a 9-char varchar -> CAS fast path -> the
# torn-wait injection point fires.
my $target_id  = 5;
my $update_sql = q{UPDATE district SET d_ytd = 222222.99, }
  . q{d_name = 'r5n99999' WHERE d_id = } . $target_id . q{;};

# A reader query that forces a full deform of every row (md5 over the varlena
# d_name), i.e. it walks the deform loop across the torn varchar of the target
# row.
my $read_sql =
  q{SELECT count(md5(d_name)), sum(d_ytd), max(d_id) FROM district;};

# ---------------------------------------------------------------------------
# Run one deterministic torn-read attempt.  Returns the reader session's
# accumulated stderr ('' on clean).  Attaches the wait point, parks a writer
# mid-tear, deforms the torn row in a reader session, then wakes the writer.
# ---------------------------------------------------------------------------
sub torn_read_attempt
{
	my ($node) = @_;

	my ($reader_err, $writer_err) = ('', '');

	# The no-fix path crashes a backend, which PANICs the whole cluster; any
	# safe_psql issued against the restarting cluster then errors.  Wrap the
	# whole attempt so a crash is reported as a fatal reader error rather than
	# aborting the test with a hard die -- we want to count every attempt.
	eval {
		# Global attach: only the CAS *write* path reaches this point, so only
		# the writer session blocks.  The reader never executes an UPDATE and so
		# never hits the point.
		$node->safe_psql('postgres',
			q{SELECT injection_points_attach('recno-cas-torn-wait', 'wait');});

		my $writer = $node->background_psql('postgres', on_error_stop => 0);
		my $reader = $node->background_psql('postgres', on_error_stop => 0);

		# Writer: begin the CAS UPDATE.  It scribbles the garbage tail then parks
		# on the wait point holding the buffer content lock.
		$writer->query_until(
			qr/starting_writer/, qq[
\\echo starting_writer
$update_sql
]);

		# Wait until the writer is provably parked inside the torn window.
		my $parked = wait_for_injection_point($node, 'recno-cas-torn-wait');
		ok($parked, 'writer parked mid-tear on recno-cas-torn-wait');

		# Reader: deform the torn row.  Without the fix this crashes / asserts on
		# the torn image; with the fix it BLOCKS on the writer's EXCLUSIVE buffer
		# lock and only completes after we wake the writer below.
		$reader->query_until(
			qr/starting_reader/, qq[
\\echo starting_reader
$read_sql
]);

		# Give the reader a beat to reach the buffer copy / deform.  With the fix
		# it is now blocked on the buffer lock; without the fix it has already
		# deformed the torn tuple (and, if it crashed, taken the backend down).
		usleep(200_000);

		# Wake the writer: it restores the clean bytes, finishes the crit
		# section, commits, and releases the buffer lock.  A fixed reader then
		# unblocks and reads the clean tuple.
		$node->safe_psql('postgres', qq[
SELECT injection_points_wakeup('recno-cas-torn-wait');
SELECT injection_points_detach('recno-cas-torn-wait');
]);

		# Drain both sessions, collecting any error text (a crashed backend shows
		# as a lost-connection error on the reader session).
		$reader_err = safe_quit($reader);
		$writer_err = safe_quit($writer);
		1;
	} or do {
		$reader_err .= "\n" . ($@ // 'attempt aborted') . " (torn-read crash)";
	};

	return ($reader_err, $writer_err);
}

# Run the deterministic attempt a few times to show it is not lucky.
my $attempts     = 5;
my $reader_fatal = 0;
for my $i (1 .. $attempts)
{
	my ($reader_err, $writer_err) = torn_read_attempt($node);

	if ($reader_err =~
		/server closed|terminating connection because of crash|failed Assert|lost synchronization|connection to server was lost|ended prematurely/
	  )
	{
		$reader_fatal++;
		diag("attempt $i: reader saw fatal torn-read error: $reader_err");
	}
}

is($reader_fatal, 0,
	"no torn-read crash / deform-bounds Assert across $attempts deterministic attempts");

my $log = slurp_file($node->logfile);
my $crash_logged = $log =~
  qr/terminated by signal|TRAP: failed Assert|server closed the connection unexpectedly|was terminated by signal/;

unlike($log,
	qr/terminated by signal|TRAP: failed Assert|server closed the connection unexpectedly|was terminated by signal/,
	'no backend crash or deform-bounds Assert logged');

SKIP:
{
	# A no-fix run crashes the cluster; the recovery-dependent checks below
	# would hard-die against a downed postmaster.  The failure is already
	# recorded by the two assertions above; skip the liveness checks.
	skip 'backend crashed (torn read reproduced) -- skipping liveness checks',
	  3
	  if $reader_fatal || $crash_logged;

	# WITH the fix: server must still be up and every row deforms cleanly.
	my $alive = $node->safe_psql('postgres', 'SELECT 1');
	is($alive, '1', 'server still up after deterministic torn-read attempts');

	my ($rc, $out) = $node->psql('postgres',
		'SELECT count(*), count(md5(d_name)) FROM district');
	is($rc, 0, 'full-table deform of every row succeeds');
	like($out, qr/^10\|10$/, 'all 10 rows present and well-formed');

	$node->safe_psql('postgres', 'DROP TABLE district');
}

done_testing();

# ---------------------------------------------------------------------------
# Helpers (mirrors src/test/modules/test_misc/t/010_index_concurrently_upsert.pl)
# ---------------------------------------------------------------------------

# Poll pg_stat_activity until some backend is waiting on the named injection
# point.  Returns 1 when parked, 0 on timeout.
sub wait_for_injection_point
{
	my ($node, $point_name, $timeout) = @_;
	$timeout //= $PostgreSQL::Test::Utils::timeout_default / 2;

	for (my $elapsed = 0; $elapsed < $timeout * 10; $elapsed++)
	{
		my $pid = $node->safe_psql(
			'postgres', qq[
			SELECT pid FROM pg_stat_activity
			WHERE wait_event_type = 'InjectionPoint'
			  AND wait_event = '$point_name'
			LIMIT 1;
		]);
		return 1 if $pid ne '';
		usleep(100_000);
	}

	my $activity = $node->safe_psql(
		'postgres', q[
		SELECT format('pid=%s, state=%s, wait_event_type=%s, wait_event=%s, query=%s',
			pid, state, wait_event_type, wait_event, left(query, 100))
		FROM pg_stat_activity
		ORDER BY pid;
	]);
	diag(   "wait_for_injection_point timeout waiting for: $point_name\n"
		  . "Current queries in pg_stat_activity:\n$activity");
	return 0;
}

# Complete any pending query on a background session, capture its stderr, and
# close it.  Returns the captured stderr (excluding the internal marker).  If
# the backend died mid-query (a torn-read crash takes it down with signal 6),
# the marker never echoes: catch the pump death and report it as a fatal error
# string instead of die-ing, so the caller counts it as a failed attempt.
sub safe_quit
{
	my ($session) = @_;

	my $banner       = "safe_quit_marker";
	my $banner_match = qr/(^|\n)$banner\r?\n/;

	$session->{stdin} .= "\\echo $banner\n\\warn $banner\n";

	my $died = 0;
	eval {
		pump_until($session->{run}, $session->{timeout},
			\$session->{stdout}, $banner_match);
		pump_until($session->{run}, $session->{timeout},
			\$session->{stderr}, $banner_match);
		1;
	} or do {
		$died = 1;
	};

	my $stderr = $session->{stderr} // '';
	$stderr =~ s/$banner_match//;
	$stderr .= "\nbackend process ended prematurely (torn-read crash)"
	  if $died;

	eval { $session->quit; };

	return $stderr;
}

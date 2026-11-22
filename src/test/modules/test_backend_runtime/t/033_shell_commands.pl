# Copyright (c) 2026, PostgreSQL Global Development Group

# Shell commands under multithreaded=on.  archive_command and restore_command
# run through pg_system(), which parks the fiber on the child's pidfd instead
# of blocking the carrier loop in system(), and the postmaster reaps only its
# own children so it never takes that child's exit status.
#
# Each carrier thread keeps every process signal blocked; the postmaster
# thread handles them.  A transaction abort used to unblock them on whatever
# thread the backend fiber ran on, after which a child's SIGCHLD could land
# there.  The first loop below aborts a transaction on every loop, then makes
# children exit.
use strict;
use warnings FATAL => 'all';

use File::Copy;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $primary = PostgreSQL::Test::Cluster->new('primary');
$primary->init(has_archiving => 1, allows_streaming => 1);
$primary->append_conf('postgresql.conf', qq{
multithreaded = on
carrier_loops = 2
});
$primary->start;
is($primary->safe_psql('postgres', 'SHOW multithreaded'), 'on',
	'primary runs threaded');

# An aborted transaction on each loop must leave the thread's signals blocked.
for (1 .. 4)
{
	my ($rc, $out, $err) = $primary->psql('postgres', 'SELECT 1/0');
	isnt($rc, 0, "aborting transaction $_ fails");
}
my $pm = (split /\n/, slurp_file($primary->data_dir . '/postmaster.pid'))[0];
SKIP:
{
	skip 'no /proc', 1 unless -d "/proc/$pm/task";
	my $unblocked = 0;
	opendir(my $dh, "/proc/$pm/task") or die "opendir: $!";
	for my $tid (grep { /^\d+$/ } readdir($dh))
	{
		next if $tid == $pm;
		my $status = slurp_file("/proc/$pm/task/$tid/status");
		next unless $status =~ /^Name:\s*postgres\s*$/m;
		# SIGCHLD is signal 17 on Linux: bit 16 of SigBlk.
		my ($blk) = $status =~ /^SigBlk:\s*([0-9a-f]+)/m;
		$unblocked++ unless hex(substr($blk, -8)) & (1 << 16);
	}
	closedir($dh);
	is($unblocked, 0, 'no carrier thread has SIGCHLD unblocked');
}

# archive_command runs on the archiver fiber.
$primary->safe_psql('postgres', q{
	CREATE TABLE t AS SELECT generate_series(1, 100) AS a;
	SELECT pg_switch_wal();
});
my $segment = $primary->safe_psql('postgres',
	q{SELECT pg_walfile_name(pg_current_wal_lsn())});
ok( $primary->poll_query_until(
		'postgres',
		q{SELECT archived_count > 0 AND failed_count = 0 FROM pg_stat_archiver}),
	'archive_command archived a segment');

# A failing archive_command is reported and retried, not lost.
$primary->safe_psql('postgres', q{
	ALTER SYSTEM SET archive_command = 'false';
	SELECT pg_reload_conf();
});
$primary->poll_query_until('postgres',
	q{SELECT current_setting('archive_command') = 'false'});
$primary->safe_psql('postgres',
	q{INSERT INTO t VALUES (0); SELECT pg_switch_wal();});
ok( $primary->poll_query_until(
		'postgres', q{SELECT failed_count > 0 FROM pg_stat_archiver}),
	'a failing archive_command is counted as failed');
$primary->safe_psql('postgres', q{
	ALTER SYSTEM RESET archive_command;
	SELECT pg_reload_conf();
});
$primary->poll_query_until('postgres',
	q{SELECT current_setting('archive_command') <> 'false'});

# restore_command runs on the startup fiber of a threaded standby.
$primary->backup('backup');
$primary->safe_psql('postgres', q{
	INSERT INTO t SELECT generate_series(101, 200);
	SELECT pg_switch_wal();
});
my $last = $primary->safe_psql('postgres',
	q{SELECT pg_walfile_name(pg_current_wal_lsn() - 1)});
ok( $primary->poll_query_until(
		'postgres',
		qq{SELECT last_archived_wal >= '$last' FROM pg_stat_archiver}),
	'segments with the new rows are archived');

my $standby = PostgreSQL::Test::Cluster->new('standby');
$standby->init_from_backup($primary, 'backup', has_restoring => 1);
$standby->start;
ok( $standby->poll_query_until(
		'postgres', q{SELECT count(*) = 201 FROM t}),
	'standby replays archived WAL through restore_command');
like(slurp_file($standby->logfile), qr/restored log file ".*" from archive/,
	'restore_command restored a segment');

for my $node ($primary, $standby)
{
	unlike(slurp_file($node->logfile),
		qr/TRAP:|PANIC:|GENUINE-CRASH|terminating threaded server runtime/,
		$node->name . ': no crash signatures');
}

$standby->stop;
$primary->stop;
done_testing();

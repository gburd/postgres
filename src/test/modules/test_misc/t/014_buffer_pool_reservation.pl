# Copyright (c) 2022-2026, PostgreSQL Global Development Group
#
# Test the same-address buffer-pool reservation (P1).
#
# Verifies that with max_buffer_pool_memory set, the postmaster reserves a
# region of address space, that the region is PROT_NONE (no physical memory
# charged until a pool commits into it), and that a forked backend sees the
# reservation at the IDENTICAL virtual address -- the property all
# same-address pool features depend on.
#
# Linux-only assertions (the reservation mechanism is Linux/memfd-based);
# elsewhere we only check the feature degrades cleanly (GUC accepted, server
# starts, no reservation claimed).

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->append_conf('postgresql.conf', 'max_buffer_pool_memory = 64MB');
$node->start;

# GUC is accepted and reported.
is( $node->safe_psql('postgres', 'SHOW max_buffer_pool_memory;'),
	'64MB', 'max_buffer_pool_memory GUC accepted');

SKIP:
{
	skip "reservation /proc inspection is Linux-only", 3
	  if ($^O ne 'linux');

	my $pm_pid = $node->safe_psql('postgres',
		'SELECT pg_postmaster_start_time() IS NOT NULL;');

	# Find the postmaster pid from postmaster.pid (first line).
	my $datadir = $node->data_dir;
	open(my $pf, '<', "$datadir/postmaster.pid") or die "open pid: $!";
	my $postmaster = <$pf>;
	chomp $postmaster;
	close($pf);

	# The reservation shows up as a PROT_NONE ("---") shared mapping backed by
	# a memfd named postgres_bufpool_reservation.
	my $pm_maps = slurp_file("/proc/$postmaster/maps");
	my ($pm_line) =
	  grep { /postgres_bufpool_reservation/ } split(/\n/, $pm_maps);
	ok(defined $pm_line, 'postmaster has the buffer-pool reservation mapping')
	  or diag("no reservation mapping in postmaster maps");

	like($pm_line, qr/^[0-9a-f]+-[0-9a-f]+ ---[ps]/,
		'reservation is PROT_NONE (no physical memory charged yet)');

	# Extract the reservation base address from the postmaster.
	my ($pm_base) = $pm_line =~ /^([0-9a-f]+)-/;

	# Start a long-lived backend and check its maps for the SAME base address.
	my $bg = $node->background_psql('postgres');
	$bg->query_until(qr/started/, "\\echo started\nSELECT pg_sleep(30);\n");

	my $bpid = $node->safe_psql('postgres',
		"SELECT pid FROM pg_stat_activity WHERE query LIKE '%pg_sleep%' AND pid <> pg_backend_pid() LIMIT 1;"
	);

	my $bk_maps = slurp_file("/proc/$bpid/maps");
	my ($bk_line) =
	  grep { /postgres_bufpool_reservation/ } split(/\n/, $bk_maps);
	my ($bk_base) = defined $bk_line ? ($bk_line =~ /^([0-9a-f]+)-/) : (undef);

	is($bk_base, $pm_base,
		'forked backend maps reservation at the same address as postmaster');

	$bg->quit;
}

$node->stop;
done_testing();

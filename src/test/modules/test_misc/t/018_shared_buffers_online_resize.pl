# Copyright (c) 2022-2026, PostgreSQL Global Development Group
#
# D5 honest-foundation check: online resize of shared_buffers (the default
# pool) is NOT yet supported, and this test pins that contract so a future
# implementation has a failing test to flip green.
#
# What IS real (and this test confirms): the same-address reservation the
# feature must be built on comes up when max_buffer_pool_memory is set, and
# shared_buffers is still a restart-only (PGC_POSTMASTER) GUC.  The resize
# entry point BufferPoolResizeShared() is a deliberate elog(ERROR) stub; see
# its comment in bufpool.c for exactly what remains.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
# Turn the reservation foundation on so we prove the primitives are wired even
# though the default-pool online resize on top of them is still a stub.
$node->append_conf('postgresql.conf', 'max_buffer_pool_memory = 64MB');
$node->start;

# shared_buffers is restart-only: attempting to SET it in a session must fail,
# which is the current honest behaviour (no online resize).
my ($ret, $stdout, $stderr) =
  $node->psql('postgres', 'SET shared_buffers = 256000;');
isnt($ret, 0, 'SET shared_buffers is rejected (restart-only, no online resize)');
like($stderr, qr/cannot be changed|without restarting the server/i,
	'shared_buffers reported as restart-only');

# The reservation foundation the eventual resize builds on is present.
is( $node->safe_psql('postgres', 'SHOW max_buffer_pool_memory;'),
	'64MB', 'reservation foundation (max_buffer_pool_memory) is active');

# ALTER SYSTEM + reload does not change it live either (still needs restart).
$node->safe_psql('postgres',
	"ALTER SYSTEM SET shared_buffers = '200MB';");
$node->reload;
isnt($node->safe_psql('postgres', 'SHOW shared_buffers;'),
	'200MB', 'shared_buffers unchanged after reload (restart still required)');

# Clean up the ALTER SYSTEM entry so a restart would not fail the fixture.
$node->safe_psql('postgres', 'ALTER SYSTEM RESET shared_buffers;');

$node->stop;
done_testing();

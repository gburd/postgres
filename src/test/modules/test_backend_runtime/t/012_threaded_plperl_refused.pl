# Copyright (c) 2026, PostgreSQL Global Development Group
#
# PL/Perl is a process-only module: Perl keeps its current interpreter in
# OS-thread-local storage, which a session fiber that moves between threads
# cannot keep consistent.  Under multithreaded=on a fiber session must refuse
# to load it with the "process-only" error.  A session forced onto the
# fork+exec'd process fallback runs the process runtime and loads it normally.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Start a threaded node with the given extra settings placed before
# multithreaded=on, which the fallback settings require.
sub start_threaded_node
{
	my ($name, $extra) = @_;
	my $node = PostgreSQL::Test::Cluster->new($name);
	$node->init;
	$node->append_conf('postgresql.conf', $extra . "multithreaded = on\n");
	$node->start;
	return $node;
}

my $node = start_threaded_node('threaded_plperl_refused', '');
is($node->safe_psql('postgres', 'SHOW multithreaded'),
	'on', 'threaded runtime active');

# CREATE EXTENSION loads the handler library, so a build with plperl fails here
# with the refusal.
my $err;
my $rc = $node->psql('postgres', 'CREATE EXTENSION plperl;', stderr => \$err);
my $have_plperl = $err !~ /is not available|could not open extension control file/i;
$node->stop;

SKIP:
{
	skip "plperl not available: $err", 3 unless $have_plperl;

	isnt($rc, 0, 'CREATE EXTENSION plperl fails on a fiber session');
	like(
		$err,
		qr/library ".*plperl.*" is not supported in the threaded backend runtime/,
		'plperl is refused at load');
	like($err, qr/the library is process-only/, 'refusal names it process-only');
}

# The process fallback needs SysV shared memory to re-attach after exec.
SKIP:
{
	skip 'plperl not available', 2 unless $have_plperl;
	skip 'process fallback requires Unix and SysV shared memory', 2
	  if $windows_os;

	my $fb = start_threaded_node('threaded_plperl_fallback',
		"xtc_force_process_fallback = on\nshared_memory_type = sysv\n");
	skip 'process-fallback route not engaged', 2
	  if $fb->safe_psql('postgres', 'SHOW xtc_force_process_fallback') ne
	  'on';

	$fb->safe_psql('postgres', 'CREATE EXTENSION plperl;');
	pass('CREATE EXTENSION plperl succeeds in a fallback process');
	$fb->safe_psql('postgres',
		q{CREATE FUNCTION perl_add(int, int) RETURNS int LANGUAGE plperl AS $$ return $_[0] + $_[1]; $$;}
	);
	is($fb->safe_psql('postgres', 'SELECT perl_add(2, 3)'),
		'5', 'plperl code runs in a fallback process');
	$fb->stop;
}

done_testing();

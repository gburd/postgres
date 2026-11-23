# Copyright (c) 2026, PostgreSQL Global Development Group

# Databases with different LC_CTYPE under multithreaded=on.  Each threaded
# session installs its database's locale on its own thread with uselocale(),
# and reinstalls it whenever its fiber resumes.  With one carrier loop every
# session shares a thread, so a session that parks and resumes after another
# database's session ran must still see its own LC_CTYPE.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $utf8_locale;
for my $candidate ('C.UTF-8', 'C.utf8', 'en_US.UTF-8', 'en_US.utf8')
{
	my $out = `locale -a 2>/dev/null` // '';
	(my $plain = lc $candidate) =~ s/-//g;
	if (grep { (my $l = lc $_) =~ s/-//g; $l eq $plain } split /\n/, $out)
	{
		$utf8_locale = $candidate;
		last;
	}
}
plan skip_all => 'no UTF-8 locale installed' unless defined $utf8_locale;

my $node = PostgreSQL::Test::Cluster->new('database_locale');
$node->init(extra => [ '--locale=C', '--encoding=SQL_ASCII' ]);
$node->append_conf('postgresql.conf', qq{
multithreaded = on
carrier_loops = 1
});
$node->start;
is($node->safe_psql('postgres', 'SHOW multithreaded'), 'on',
	'server runs threaded');

$node->safe_psql('postgres',
	"CREATE DATABASE utf8db TEMPLATE template0 ENCODING 'UTF8' "
	  . "LOCALE_PROVIDER libc LOCALE '$utf8_locale'");
$node->safe_psql($_, 'CREATE EXTENSION test_backend_runtime_threaded')
  for ('postgres', 'utf8db');

is( $node->safe_psql('utf8db', 'SELECT test_backend_runtime_ctype_probe(0)'),
	'mb_cur_max=6', 'UTF-8 database session sees its own LC_CTYPE');
is( $node->safe_psql('postgres', 'SELECT test_backend_runtime_ctype_probe(0)'),
	'mb_cur_max=1', 'C database session sees its own LC_CTYPE');
is($node->safe_psql('utf8db', q{SELECT upper('é')}),
	'É', 'UTF-8 database uppercases non-ASCII');

# Interleave the two on the single carrier thread: each parks in pg_usleep()
# while the other runs, then reads its LC_CTYPE again.
my $utf8 = $node->background_psql('utf8db');
my $c = $node->background_psql('postgres');
for (1 .. 3)
{
	$utf8->query_until(qr/start/,
		"\\echo start\nSELECT test_backend_runtime_ctype_probe(300);\n");
	my $c_out =
	  $c->query_safe('SELECT test_backend_runtime_ctype_probe(0)');
	is($c_out, 'mb_cur_max=1', "round $_: C session runs while UTF-8 parked");
	like($utf8->query_safe('SELECT 1'), qr/mb_cur_max=6/,
		"round $_: UTF-8 session resumed with its own LC_CTYPE");
}
$utf8->quit;
$c->quit;

$node->stop;
done_testing();

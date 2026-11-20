# Copyright (c) 2026, PostgreSQL Global Development Group

# A checkpoint must not move the redo pointer past an abort record whose
# FILEOPS inverses are not yet durable.  Pause the aborting backend after the
# abort record is flushed and before it applies the inverses, request a
# CHECKPOINT, then crash.  Recovery must replay the abort record and remove
# the file, for ROLLBACK and for ROLLBACK PREPARED.  Without the hold-off the
# checkpoint completes during the pause, redo starts after the abort record,
# and the file survives.
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

plan skip_all => 'injection points not supported by this build'
  unless $ENV{enable_injection_points} eq 'yes';

my $node = PostgreSQL::Test::Cluster->new('n');
$node->init(allows_streaming => 1);
$node->append_conf('postgresql.conf',
	"max_prepared_transactions = 5\ncheckpoint_timeout = 1h\n");
$node->start;
$node->safe_psql('postgres',
	'CREATE EXTENSION test_fileops; CREATE EXTENSION injection_points');

for my $case ('rollback', 'rollback prepared')
{
	(my $f = "ck-$case.dat") =~ s/ /-/g;
	$node->safe_psql('postgres',
		"SELECT injection_points_attach('fileops-abort-after-flush', 'wait')");
	my $before = $node->safe_psql('postgres',
		'SELECT checkpoint_lsn FROM pg_control_checkpoint()');

	my $a = $node->background_psql('postgres');
	if ($case eq 'rollback')
	{
		$a->query_until(qr/go/, qq{\\echo go
BEGIN; SELECT test_fileops_create('$f', 384); ROLLBACK;
});
	}
	else
	{
		$node->safe_psql('postgres',
			"BEGIN; SELECT test_fileops_create('$f', 384); PREPARE TRANSACTION 'p';");
		$a->query_until(qr/go/, qq{\\echo go
ROLLBACK PREPARED 'p';
});
	}
	$node->wait_for_event('client backend', 'fileops-abort-after-flush');
	ok(-e $node->data_dir . "/$f", "$case: file still there at the pause");

	my $c = $node->background_psql('postgres');
	$c->query_until(qr/go/, "\\echo go\nCHECKPOINT;\n");

	# Give the checkpoint time to finish if nothing holds it back.
	my $after = $before;
	for (1 .. 50)
	{
		$after = $node->safe_psql('postgres',
			'SELECT checkpoint_lsn FROM pg_control_checkpoint()');
		last if $after ne $before;
		usleep(100_000);
	}
	note "$case: checkpoint completed during the pause: "
	  . ($after ne $before ? 'yes' : 'no');

	$node->stop('immediate');
	eval { $a->quit; };
	eval { $c->quit; };
	$node->start;
	ok(!-e $node->data_dir . "/$f", "$case: crash recovery removed the file");
}

$node->stop;
done_testing();

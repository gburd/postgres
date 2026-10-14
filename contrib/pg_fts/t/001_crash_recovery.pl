# Copyright (c) 2024-2026, PostgreSQL Global Development Group

# Crash-recovery / WAL-replay correctness for the pg_fts bm25 index.
#
# Every page mutation in pg_fts goes through GenericXLog, so an immediate
# (crash) shutdown followed by recovery must reproduce the exact index state.
# This test builds an index, records query answers, crashes the server, and
# after automatic recovery verifies the answers are identical -- covering the
# build, incremental-insert (pending list), and post-merge paths.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init;
# make crashes deterministic: full-page writes on, no fsync needed for the test
$node->append_conf('postgresql.conf', "fsync = off\n");
$node->start;

$node->safe_psql('postgres', 'CREATE EXTENSION pg_fts');
$node->safe_psql(
	'postgres', q{
	CREATE TABLE docs (id int primary key, d ftsdoc);
	INSERT INTO docs SELECT g, to_ftsdoc('alpha doc ' || g)
		FROM generate_series(1, 2000) g;
	CREATE INDEX docs_bm25 ON docs USING bm25 (d);
});

# Add rows AFTER the build so the pending-write-buffer path is on disk too,
# and delete some so a tombstone is present.
$node->safe_psql(
	'postgres', q{
	INSERT INTO docs SELECT g, to_ftsdoc('beta doc ' || g)
		FROM generate_series(3000, 3200) g;
	DELETE FROM docs WHERE id <= 50;
	VACUUM docs;
});

# Record the pre-crash answers via the index.
my $q = "SET enable_seqscan=off;";
my $alpha_before =
  $node->safe_psql('postgres',
	"$q SELECT count(*) FROM docs WHERE d \@\@\@ 'alpha'::ftsquery");
my $beta_before =
  $node->safe_psql('postgres',
	"$q SELECT count(*) FROM docs WHERE d \@\@\@ 'beta'::ftsquery");
my $fc_before =
  $node->safe_psql('postgres',
	"SELECT fts_count('docs_bm25', 'alpha'::ftsquery)");
my $rank_before = $node->safe_psql('postgres',
	"$q SELECT string_agg(id::text, ',') FROM (SELECT id FROM docs WHERE d \@\@\@ 'alpha'::ftsquery ORDER BY d <=> 'alpha'::ftsquery LIMIT 10) x"
);

# Crash: immediate stop discards shared buffers; recovery must rebuild state
# from WAL (GenericXLog records) alone.
$node->stop('immediate');
$node->start;

my $alpha_after =
  $node->safe_psql('postgres',
	"$q SELECT count(*) FROM docs WHERE d \@\@\@ 'alpha'::ftsquery");
my $beta_after =
  $node->safe_psql('postgres',
	"$q SELECT count(*) FROM docs WHERE d \@\@\@ 'beta'::ftsquery");
my $fc_after =
  $node->safe_psql('postgres',
	"SELECT fts_count('docs_bm25', 'alpha'::ftsquery)");
my $rank_after = $node->safe_psql('postgres',
	"$q SELECT string_agg(id::text, ',') FROM (SELECT id FROM docs WHERE d \@\@\@ 'alpha'::ftsquery ORDER BY d <=> 'alpha'::ftsquery LIMIT 10) x"
);

is($alpha_after, $alpha_before, 'alpha count survives crash recovery');
is($beta_after,  $beta_before,  'pending-list beta count survives crash recovery');
is($fc_after,    $fc_before,    'fts_count survives crash recovery');
is($rank_after,  $rank_before,  'ranked top-10 identical after crash recovery');

# The index must still be usable for new writes after recovery.
$node->safe_psql('postgres',
	"INSERT INTO docs VALUES (99999, to_ftsdoc('alpha extra'))");
my $alpha_grown =
  $node->safe_psql('postgres',
	"$q SELECT count(*) FROM docs WHERE d \@\@\@ 'alpha'::ftsquery");
is($alpha_grown, $alpha_before + 1, 'index writable after recovery');

$node->stop;
done_testing();

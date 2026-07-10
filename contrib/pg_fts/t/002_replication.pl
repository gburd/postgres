# Copyright (c) 2024-2026, PostgreSQL Global Development Group

# Physical (streaming) replication of the pg_fts bm25 index.
#
# The bm25 index is fully WAL-logged via GenericXLog, so a streaming standby
# must reconstruct it from the primary's WAL and answer @@@ / <=> / fts_count
# identically.  This test builds and mutates the index on the primary, waits
# for the standby to catch up, and compares query answers on both nodes --
# including the incremental-insert (pending) and delete/VACUUM (tombstone)
# paths, which must all replicate.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Primary
my $primary = PostgreSQL::Test::Cluster->new('primary');
$primary->init(allows_streaming => 1);
$primary->start;

$primary->safe_psql('postgres', 'CREATE EXTENSION pg_fts');
$primary->safe_psql(
	'postgres', q{
	CREATE TABLE docs (id int primary key, d ftsdoc);
	INSERT INTO docs SELECT g, to_ftsdoc('alpha doc ' || g)
		FROM generate_series(1, 1500) g;
	CREATE INDEX docs_bm25 ON docs USING bm25 (d);
});

# Standby from a base backup
my $backup = 'bkp';
$primary->backup($backup);
my $standby = PostgreSQL::Test::Cluster->new('standby');
$standby->init_from_backup($primary, $backup, has_streaming => 1);
$standby->start;

# Mutate the primary AFTER the standby is streaming: pending-list inserts,
# a delete, and a VACUUM (flush + merge + tombstone) -- all must replicate.
$primary->safe_psql(
	'postgres', q{
	INSERT INTO docs SELECT g, to_ftsdoc('beta doc ' || g)
		FROM generate_series(5000, 5200) g;
	DELETE FROM docs WHERE id <= 100;
	VACUUM docs;
});

# Wait for the standby to replay up to the primary's current WAL position.
$primary->wait_for_catchup($standby);

my $q = "SET enable_seqscan=off;";
for my $case (
	[ 'alpha count', "SELECT count(*) FROM docs WHERE d \@\@\@ 'alpha'::ftsquery" ],
	[ 'beta count',  "SELECT count(*) FROM docs WHERE d \@\@\@ 'beta'::ftsquery" ],
	[
		'ranked top-10',
		"SELECT string_agg(id::text, ',') FROM (SELECT id FROM docs WHERE d \@\@\@ 'alpha'::ftsquery ORDER BY d <=> 'alpha'::ftsquery LIMIT 10) x"
	],
	[
		'fts_count',
		"SELECT fts_count('docs_bm25', 'alpha'::ftsquery)"
	])
{
	my ($label, $sql) = @$case;
	my $on_primary = $primary->safe_psql('postgres', "$q $sql");
	my $on_standby = $standby->safe_psql('postgres', "$q $sql");
	is($on_standby, $on_primary, "standby matches primary: $label");
}

# The replicated index must also be MVCC/tombstone-correct on the standby:
# the 100 deleted alpha docs must not appear.
my $alpha_standby = $standby->safe_psql('postgres',
	"$q SELECT count(*) FROM docs WHERE d \@\@\@ 'alpha'::ftsquery");
is($alpha_standby, 1400, 'standby reflects deletes (1500 - 100 tombstoned)');

$standby->stop;
$primary->stop;
done_testing();

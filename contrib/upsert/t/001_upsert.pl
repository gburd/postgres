# Copyright (c) 2026, PostgreSQL Global Development Group
#
# upsert: verify that the UPSERT statement (a grammar extension that
# lowers to INSERT ... ON CONFLICT (...) DO UPDATE) produces results
# identical to the equivalent hand-written INSERT ... ON CONFLICT.
#
# The extension loads via shared_preload_libraries; _PG_init registers
# the UPSERT keyword and the upsert_stmt production with the in-process
# composed grammar.  No subprocess, no C compiler, no .so cache.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('upsert_main');
$node->init;
$node->append_conf('postgresql.conf',
	"shared_preload_libraries = 'upsert'\n"
	. "log_min_messages = debug1\n"
	. "client_min_messages = notice\n");
$node->start;

$node->safe_psql('postgres', 'CREATE EXTENSION upsert');

# Two structurally-identical tables: one driven by UPSERT, one by the
# equivalent INSERT ... ON CONFLICT.  Both must end up identical.
$node->safe_psql('postgres', q{
	CREATE TABLE inv_upsert (sku text PRIMARY KEY, qty int, note text);
	CREATE TABLE inv_sql    (sku text PRIMARY KEY, qty int, note text);
});

# ------------------------------------------------------------------
# Test 1: insert path -- key does not yet exist, row is inserted.
# ------------------------------------------------------------------
$node->safe_psql('postgres',
	q{UPSERT INTO inv_upsert (sku, qty, note) VALUES ('A-1', 5, 'first') ON (sku)});
$node->safe_psql('postgres',
	q{INSERT INTO inv_sql (sku, qty, note) VALUES ('A-1', 5, 'first')
	  ON CONFLICT (sku) DO UPDATE SET qty = excluded.qty, note = excluded.note});

is(
	$node->safe_psql('postgres', 'SELECT sku, qty, note FROM inv_upsert ORDER BY sku'),
	$node->safe_psql('postgres', 'SELECT sku, qty, note FROM inv_sql ORDER BY sku'),
	'UPSERT insert path matches INSERT ... ON CONFLICT');

# ------------------------------------------------------------------
# Test 2: update path -- same key, conflicting insert updates the row.
# ------------------------------------------------------------------
$node->safe_psql('postgres',
	q{UPSERT INTO inv_upsert (sku, qty, note) VALUES ('A-1', 12, 'restock') ON (sku)});
$node->safe_psql('postgres',
	q{INSERT INTO inv_sql (sku, qty, note) VALUES ('A-1', 12, 'restock')
	  ON CONFLICT (sku) DO UPDATE SET qty = excluded.qty, note = excluded.note});

is(
	$node->safe_psql('postgres', 'SELECT sku, qty, note FROM inv_upsert ORDER BY sku'),
	$node->safe_psql('postgres', 'SELECT sku, qty, note FROM inv_sql ORDER BY sku'),
	'UPSERT update path matches INSERT ... ON CONFLICT');

is($node->safe_psql('postgres', "SELECT qty FROM inv_upsert WHERE sku = 'A-1'"),
	'12', 'UPSERT updated qty to the new value');

# ------------------------------------------------------------------
# Test 3: multi-row sequence, interleaved with plain SQL.  The
# composed grammar must keep base SQL and UPSERT both working.
# ------------------------------------------------------------------
$node->safe_psql('postgres', q{
	UPSERT INTO inv_upsert (sku, qty, note) VALUES ('B-2', 3, 'new') ON (sku);
	INSERT INTO inv_upsert (sku, qty, note) VALUES ('C-3', 7, 'plain');
	UPSERT INTO inv_upsert (sku, qty, note) VALUES ('B-2', 9, 'bumped') ON (sku);
});
is(
	$node->safe_psql('postgres',
		q{SELECT sku, qty FROM inv_upsert WHERE sku IN ('B-2','C-3') ORDER BY sku}),
	"B-2|9\nC-3|7",
	'UPSERT and plain INSERT interleave correctly');

# ------------------------------------------------------------------
# Test 4: all-columns-are-conflict-columns -> DO NOTHING degenerate
# form.  A second UPSERT of an existing key with no non-key columns
# to update must not error and must not change the row.
# ------------------------------------------------------------------
$node->safe_psql('postgres', q{
	CREATE TABLE inv_keyonly (sku text PRIMARY KEY);
	UPSERT INTO inv_keyonly (sku) VALUES ('K-1') ON (sku);
	UPSERT INTO inv_keyonly (sku) VALUES ('K-1') ON (sku);
});
is($node->safe_psql('postgres', 'SELECT count(*) FROM inv_keyonly'),
	'1', 'UPSERT with only conflict columns degrades to DO NOTHING');

# ------------------------------------------------------------------
# Test 5: base SQL is invariant with the extension loaded.
# ------------------------------------------------------------------
is($node->safe_psql('postgres', 'SELECT 1 + 1'), '2',
	'base SQL arithmetic invariant');
is(
	$node->safe_psql('postgres',
		'SELECT count(*) FROM generate_series(1, 10)'),
	'10', 'base SQL function invariant');

# ------------------------------------------------------------------
# Test 6: the compose ran in-process (no subprocess / no .so cache).
# ------------------------------------------------------------------
{
	my $logfile = $node->logfile;
	open(my $fh, '<', $logfile) or die "cannot read $logfile: $!";
	local $/;
	my $logs = <$fh>;
	close $fh;

	like($logs, qr/upsert: registered/, 'upsert registered at preload');
	like($logs, qr/composing grammar in-process for \d+ extension\(s\)/,
		'grammar composed in-process');
	unlike($logs, qr{/pg_parser_cache/[0-9a-f]{64}\.so},
		'no .so cache path (in-process compose)');
}

$node->stop;
done_testing();

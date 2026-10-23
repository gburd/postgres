
# Copyright (c) 2026, PostgreSQL Global Development Group

# The chunk directory of an OVERSIZED entry's overflow chain.
#
# 1. Keys of 30 to 60 kB (4 to 8 pages) get a directory on their chain's
#    first page that lists the chain's other pages, and an index scan
#    returns each key whole.  So does a key whose chain is longer than the
#    directory (2.5 MB, 257 directory entries' worth and more), read on
#    by its links past the directory's last block.
# 2. Chains written without a directory (the injection point
#    bark-overflow-no-dir), as BARK wrote them before, still read, next
#    to new ones, and amcheck passes both.
# 3. The chains survive a crash: redo rebuilds each page, directory and
#    all.
# 4. A standby replays them under wal_consistency_checking, which compares
#    every page redo rebuilds with the primary's, and returns the same
#    keys; VACUUM frees the chains, by their links.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init(allows_streaming => 1);
$node->append_conf(
	'postgresql.conf', q[
autovacuum = off
wal_consistency_checking = 'Bark'
wal_keep_size = 1GB
# bark_write_overflow_chain pins every page of a chain at once: the 2.5 MB
# key's 300-odd need more than the default 128 buffers.
shared_buffers = 32MB
]);
$node->start;

$node->safe_psql(
	'postgres', q[
CREATE EXTENSION amcheck;
CREATE EXTENSION pageinspect;
CREATE EXTENSION injection_points;
CREATE FUNCTION big(s int, n int) RETURNS text LANGUAGE sql IMMUTABLE AS
$$ SELECT substr(string_agg(md5(s::text || ':' || g::text), ''), 1, n)
   FROM generate_series(1, (n + 31) / 32) g $$;
CREATE TABLE t (id int, k text) WITH (autovacuum_enabled = off);
ALTER TABLE t ALTER COLUMN k SET STORAGE EXTERNAL;
CREATE INDEX t_k ON t USING bark (k);
-- The little-endian uint32 at offset off of a page.
CREATE FUNCTION get_u32(p bytea, off int) RETURNS int8 LANGUAGE sql AS
$$ SELECT get_byte(p, off) + (get_byte(p, off + 1) << 8) +
          (get_byte(p, off + 2) << 16) + (get_byte(p, off + 3)::int8 << 24) $$;
-- Each row's chain: its pages, the first page's flags, the length of the
-- directory it lists, and whether the directory lists the pages the links
-- reach.
CREATE FUNCTION chains() RETURNS TABLE (id int, pages int, dir bool, ndir int, dirok bool)
LANGUAGE plpgsql AS $$
DECLARE r record; b int8; n int; d int8[]; raw bytea; nd int;
BEGIN
  FOR r IN SELECT i.htid, i.overflow_blkno AS first FROM bark_multi_page_stats('t_k', 1, -1) s,
             bark_page_items('t_k', s.blkno) i
           WHERE s.type IN ('leaf', 'root') AND i.shape = 'OVERSIZED' LOOP
    SELECT t.id INTO id FROM t WHERE t.ctid = r.htid;
    b := r.first; n := 0; d := '{}';
    WHILE b <> 0 LOOP
      n := n + 1;
      IF n > 1 THEN d := d || b; END IF;
      SELECT bark_next INTO b FROM bark_page_stats('t_k', b);
    END LOOP;
    pages := n;
    SELECT 'overflow_dir' = ANY (flags) INTO dir FROM bark_page_stats('t_k', r.first);
    ndir := NULL; dirok := NULL;
    IF dir THEN
      raw := get_raw_page('t_k', r.first::int);
      nd := get_u32(raw, 24);
      ndir := nd;
      dirok := true;
      FOR j IN 0 .. nd - 1 LOOP
        IF get_u32(raw, 28 + 4 * j) <> d[j + 1] THEN
          dirok := false;
        END IF;
      END LOOP;
    END IF;
    RETURN NEXT;
  END LOOP;
END $$;
-- Does an index scan return every key whole?
CREATE FUNCTION scan_ok() RETURNS bool LANGUAGE plpgsql AS $$
DECLARE ok bool;
BEGIN
  SET LOCAL enable_seqscan = off;
  SET LOCAL enable_bitmapscan = off;
  SELECT bool_and(i.k = big(i.id, length(i.k))) AND count(*) = (SELECT count(*) FROM t)
    INTO ok FROM (SELECT id, k FROM t WHERE k > '' ORDER BY k) i;
  RETURN ok;
END $$;
]);

## 1: keys of 30 to 60 kB, and one longer than the directory
$node->safe_psql(
	'postgres', q[
INSERT INTO t SELECT g, big(g, 30000 + 30000 * (g % 7) / 6) FROM generate_series(1, 14) g;
INSERT INTO t VALUES (100, big(100, 2500000));
]);
is( $node->safe_psql(
		'postgres', q[
SELECT min(pages) || '-' || max(pages) FROM chains() WHERE id < 100]),
	'4-8', 'keys of 30 to 60 kB take chains of 4 to 8 pages');
is( $node->safe_psql(
		'postgres', q[
SELECT bool_and(dir AND ndir = pages - 1 AND dirok) FROM chains() WHERE id < 100]),
	't', 'each has a directory that lists its other pages, as the links do');
my ($pages, $ndir, $dirok) = split /\|/, $node->safe_psql(
	'postgres', 'SELECT pages, ndir, dirok FROM chains() WHERE id = 100');
cmp_ok($pages, '>', 257, "the 2.5 MB key's chain of $pages pages is longer than the directory");
is($ndir, '256', 'whose directory lists 256 pages');
is($dirok, 't', 'the pages its links reach');
is($node->safe_psql('postgres', 'SELECT scan_ok()'), 't',
	'an index scan returns every key whole');

## 2: chains written without a directory
$node->safe_psql(
	'postgres', q[
SELECT injection_points_set_local();
SELECT injection_points_attach('bark-overflow-no-dir', 'notice');
INSERT INTO t SELECT g, big(g, 45000) FROM generate_series(200, 203) g;
]);
is( $node->safe_psql(
		'postgres', q[
SELECT count(*) FILTER (WHERE NOT dir) || '/' || count(*) FROM chains() WHERE id >= 200]),
	'4/4', 'with bark-overflow-no-dir, chains have no directory');
is($node->safe_psql('postgres', 'SELECT scan_ok()'), 't',
	'an index scan returns both kinds whole');
is($node->safe_psql('postgres', "SELECT bark_index_parent_check('t_k', true)"),
	'', 'amcheck passes an index with both kinds');

## 3: a crash
$node->safe_psql('postgres', 'CHECKPOINT');
$node->safe_psql('postgres',
	'INSERT INTO t SELECT g, big(g, 50000) FROM generate_series(300, 305) g');
$node->stop('immediate');
$node->start;
ok($node->log_contains('database system was interrupted'),
	'the server went through crash recovery');
is( $node->safe_psql(
		'postgres', q[
SELECT bool_and(dir AND ndir = pages - 1 AND dirok) FROM chains() WHERE id >= 300]),
	't', 'the replayed chains have their directories');
is($node->safe_psql('postgres', 'SELECT scan_ok()'), 't',
	'an index scan returns every key whole after recovery');
is($node->safe_psql('postgres', "SELECT bark_index_parent_check('t_k', true)"),
	'', 'amcheck passes the recovered index');

## 4: a standby, and VACUUM
$node->backup('backup');
my $standby = PostgreSQL::Test::Cluster->new('standby');
$standby->init_from_backup($node, 'backup', has_streaming => 1);
$standby->start;
$node->safe_psql(
	'postgres', q[
INSERT INTO t SELECT g, big(g, 30000 + g) FROM generate_series(400, 409) g;
DELETE FROM t WHERE id BETWEEN 1 AND 7 OR id = 100;
VACUUM t;
]);
$node->wait_for_replay_catchup($standby);
my $want = $node->safe_psql('postgres', 'SELECT string_agg(id::text, \',\' ORDER BY k) FROM t');
is( $standby->safe_psql(
		'postgres', q[
SET enable_seqscan = off; SET enable_bitmapscan = off;
SELECT string_agg(id::text, ',' ORDER BY k) FROM t WHERE k > '']),
	$want, 'an index scan on the standby returns the primary\'s keys');
is($standby->safe_psql('postgres', 'SELECT scan_ok()'), 't',
	'and every key whole');
is( $node->safe_psql(
		'postgres', q[
SELECT count(*) FROM bark_multi_page_stats('t_k', 1, -1) WHERE type = 'deleted']) > 300 ? 't' : 'f',
	't', 'VACUUM freed the deleted keys\' chains, the long one too');
is($node->safe_psql('postgres', "SELECT bark_index_parent_check('t_k', true)"),
	'', 'amcheck passes the index after VACUUM');
$standby->stop;
ok(!$standby->log_contains('inconsistent page found'),
	'wal_consistency_checking found every replayed page consistent');

$node->stop;
done_testing();

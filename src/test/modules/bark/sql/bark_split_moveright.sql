--
-- Test that a write descent finishes an incomplete split it moves right past.
--
-- When a split is interrupted after the right half was linked in but before
-- its downlink reached the parent, the parent still routes the right half's
-- keys to the left half, and the descent reaches the right half only by
-- moving right.  If later inserts only ever go to the right half, the left
-- half is never their target, so the descent itself must finish the split
-- when it moves right: otherwise the right half, which has no downlink, fails
-- to find its parent when it splits in turn.  This is tested at the leaf
-- level and, through an interrupted split of the root, at an internal level.
--
set client_min_messages TO 'warning';
create extension if not exists injection_points;
create extension if not exists amcheck;
reset client_min_messages;

-- Keep injection points local to this backend.
SELECT injection_points_set_local();

-- Notice whenever an incomplete split is finished.
SELECT injection_points_attach('bark-finish-incomplete-split', 'notice');

-- Leaf level: a two-level index with many leaves.
create table bark_mr_leaf (i int4) with (autovacuum_enabled = off);
create index bark_mr_leaf_idx on bark_mr_leaf using bark (i);
insert into bark_mr_leaf select g * 10 from generate_series(1, 3000) g;

-- Interrupt the next leaf split, one in the middle of the key range.
SELECT injection_points_attach('bark-leave-leaf-split-incomplete', 'error');
do $$
begin
  for n in 0..2000 loop
    begin
      insert into bark_mr_leaf values (15001 + n);
    exception when others then
      return;
    end;
  end loop;
  raise exception 'test did not produce an incomplete split';
end $$;
SELECT injection_points_detach('bark-leave-leaf-split-incomplete');

-- Insert only keys above every key on the interrupted page, so that each
-- insert moves right onto its right half; that page fills and splits.  The
-- first insert finishes the interrupted split (one notice).
insert into bark_mr_leaf select 15500 + g from generate_series(1, 2000) g;
select bark_index_check('bark_mr_leaf_idx');

-- The index must agree with the heap.  (The row count depends on where the
-- interrupted split fell, so compare rather than print it.)
set enable_seqscan = off;
set enable_bitmapscan = off;
select count(*) as idx_count from bark_mr_leaf where i between 14000 and 18000
\gset
reset enable_seqscan;
reset enable_bitmapscan;
select count(*) as seq_count from bark_mr_leaf where i between 14000 and 18000
\gset
select :idx_count = :seq_count as counts_match, :seq_count >= 2401 as has_rows;

-- Internal level: wide keys, so a few hundred rows make the root split.
create table bark_mr_root (t text collate "C") with (autovacuum_enabled = off);
create index bark_mr_root_idx on bark_mr_root using bark (t);

-- Interrupt the first split of the root once it is an internal page.  Its
-- right half is then a second page at the top level with no parent at all.
SELECT injection_points_attach('bark-leave-internal-split-incomplete', 'error');
do $$
begin
  for n in 0..10000 loop
    begin
      insert into bark_mr_root values ('a' || lpad(n::text, 399, '0'));
    exception when others then
      return;
    end;
  end loop;
  raise exception 'test did not produce an incomplete split';
end $$;
SELECT injection_points_detach('bark-leave-internal-split-incomplete');

-- Insert only keys above every existing key.  The first insert's descent
-- finishes the root split (one notice) before moving right; the right half
-- then fills and splits under the new root.
insert into bark_mr_root select 'b' || lpad(g::text, 399, '0') from generate_series(1, 1000) g;
select bark_index_check('bark_mr_root_idx');

set enable_seqscan = off;
set enable_bitmapscan = off;
select count(*) as idx_count from bark_mr_root where t > ''
\gset
reset enable_seqscan;
reset enable_bitmapscan;
select count(*) as seq_count from bark_mr_root
\gset
select :idx_count = :seq_count as counts_match, :seq_count > 1000 as has_rows;

SELECT injection_points_detach('bark-finish-incomplete-split');
drop table bark_mr_leaf;
drop table bark_mr_root;

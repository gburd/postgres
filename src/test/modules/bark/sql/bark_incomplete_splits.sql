--
-- Test incomplete-split recovery in BARK indexes.
--
-- A BARK page split publishes the new right sibling (right-linked, high key
-- set) in one WAL record, then writes the sibling's downlink into the parent
-- in a second record.  A crash in between leaves the left page flagged
-- BARK_INCOMPLETE_SPLIT: reachable by reads (via the right link) but missing
-- its parent downlink.  The next writer to descend onto that leaf finishes the
-- split (bark_finish_split) before proceeding.
--
-- We use injection points to force the error that leaves a split incomplete,
-- then verify that (a) the finish-split path runs on the following insert and
-- (b) the index still answers scans in agreement with a sequential scan.
--
set client_min_messages TO 'warning';
create extension if not exists injection_points;
reset client_min_messages;

-- Keep injection points local to this backend.
SELECT injection_points_set_local();

-- Notice whenever an incomplete split is finished.
SELECT injection_points_attach('bark-finish-incomplete-split', 'notice');

create table bark_incsplit(i int4) with (autovacuum_enabled = off);
create index bark_incsplit_idx on bark_incsplit using bark (i);

set enable_seqscan = off;

-- Force the next leaf split to error out just before it publishes its
-- downlink, leaving an incomplete split on disk.
SELECT injection_points_attach('bark-leave-leaf-split-incomplete', 'error');

-- Insert in small batches until one batch fails: that failing batch is the one
-- whose leaf split was interrupted.
do $$
declare
  n int := 0;
  failed bool := false;
begin
  loop
    begin
      insert into bark_incsplit select g from generate_series(n, n + 9) g;
      n := n + 10;
    exception when others then
      failed := true;
      exit;
    end;
    if n > 50000 then
      exit;			-- safety valve: should split well before this
    end if;
  end loop;
  if not failed then
    raise exception 'test did not produce an incomplete split';
  end if;
end $$;

SELECT injection_points_detach('bark-leave-leaf-split-incomplete');

-- The next insert descends onto the interrupted leaf and finishes its split
-- (emitting the notice above), then inserts normally.
insert into bark_incsplit select g from generate_series(0, 3000) g;

-- The index must now agree with a sequential scan over a range that spans the
-- repaired boundary.
set enable_indexscan = on;
set enable_bitmapscan = off;
select count(*) as idx_range from bark_incsplit where i between 50 and 350;

set enable_seqscan = on;
set enable_indexscan = off;
select count(*) as seq_range from bark_incsplit where i between 50 and 350;

SELECT injection_points_detach('bark-finish-incomplete-split');
drop table bark_incsplit;

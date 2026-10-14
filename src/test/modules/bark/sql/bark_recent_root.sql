--
-- The root buffer hint (BarkAmCache.rootbuf): every descent re-pins the
-- buffer that last held the root with ReadRecentBuffer.  The hint may name a
-- buffer that no longer holds the root block (evicted, or reused for another
-- block), or a root that is no longer the root (after a root split); each
-- case must fall back to a plain read and keep giving the same answers.
--
set client_min_messages TO 'warning';
create extension if not exists injection_points;
create extension if not exists pg_buffercache;
create extension if not exists amcheck;
create extension if not exists pageinspect;
reset client_min_messages;

-- A NOTICE whenever a descent finds the hint unusable and reads the root
-- again; a descent that re-pins the hinted buffer is silent.
SELECT injection_points_set_local();
SELECT injection_points_attach('bark-root-hint-miss', 'notice');
-- And whenever the cached root fails its page check (it split) and the meta
-- page is read for the new one.
SELECT injection_points_attach('bark-root-cache-stale', 'notice');

-- The load's inserts set the hint; the one stale-cache notice is the load
-- splitting the root its own descents had cached.
create table bark_rr (i int4) with (autovacuum_enabled = off);
create index bark_rr_idx on bark_rr using bark (i);
insert into bark_rr select g from generate_series(1, 20000) g;
set enable_seqscan = off;
set enable_bitmapscan = off;

-- The block the meta page names as root, and the shared buffer holding it.
create function bark_rr_root_buffers() returns setof int language sql as $$
  select b.bufferid from pg_buffercache b
  where b.relfilenode = pg_relation_filenode('bark_rr_idx')
    and b.relblocknumber = (select (bark_metap('bark_rr_idx')).root)
$$;

-- Lookups re-pin the hinted buffer, silently.
select count(*) from bark_rr where i = 4242;
select count(*) from bark_rr where i between 100 and 199;
select count(*) from bark_rr where i = 77;

-- Evict the root's buffer: the hint now names a buffer without the root's
-- block, ReadRecentBuffer declines, and the block is read again.
select bool_and((pg_buffercache_evict(b)).buffer_evicted) as evicted
from bark_rr_root_buffers() b;
select count(*) from bark_rr where i = 4242;
select count(*) from bark_rr where i between 100 and 199;

-- Evict every page of the index, so the hint's buffer is likely reused for
-- another block of it; the next descents read it back.
select count(*) > 0 as evicted_some
from pg_buffercache b, pg_buffercache_evict(b.bufferid) e
where b.relfilenode = pg_relation_filenode('bark_rr_idx') and e.buffer_evicted;
select count(*) from bark_rr where i = 17;
select count(*) from bark_rr where i between 19000 and 19999;

-- Grow the tree by a level, so that the cached root (and its hint) is no
-- longer the root: the page check refuses it, the meta page is read, and the
-- new root's buffer becomes the hint.  The insert's own descents notice that
-- once, at the root split; the notices are not counted, so they are kept off.
select (bark_metap('bark_rr_idx')).level as level_before;
SELECT injection_points_detach('bark-root-cache-stale');
insert into bark_rr select g from generate_series(20001, 400000) g;
SELECT injection_points_attach('bark-root-cache-stale', 'notice');
select (bark_metap('bark_rr_idx')).level as level_after;
select count(*) from bark_rr where i = 4242;
select count(*) from bark_rr where i = 399999;
select count(*) from bark_rr where i between 250000 and 250999;

-- A temporary index uses local buffers; its hint is a local buffer.
create temp table bark_rr_t (i int4);
create index bark_rr_t_idx on bark_rr_t using bark (i);
insert into bark_rr_t select g from generate_series(1, 20000) g;
select count(*) from bark_rr_t where i = 4242;
select count(*) from bark_rr_t where i between 100 and 199;

SELECT injection_points_detach('bark-root-hint-miss');
SELECT injection_points_detach('bark-root-cache-stale');
select bark_index_check('bark_rr_idx', true);
reset enable_seqscan;
reset enable_bitmapscan;
drop table bark_rr, bark_rr_t;
drop function bark_rr_root_buffers();

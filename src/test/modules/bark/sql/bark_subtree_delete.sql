--
-- VACUUM deletes an empty leaf together with the chain of internal pages
-- above it that have it as their only descendant (bark_delete_subtree): a
-- "skinny subtree", in one UNLINK_PAGE record.  A contiguous delete empties
-- whole subtrees of a four-level tree, whose chains reach up to two
-- internal levels.  Every scan mode then answers as a btree index on a copy
-- of the table does, and amcheck's parent check passes.  A queue keeps the
-- internal levels bounded where they used to grow, and OVERSIZED separators
-- have their overflow chains freed with the downlink that named them.
--
set client_min_messages TO 'warning';
create extension if not exists amcheck;
create extension if not exists pageinspect;
create extension if not exists pg_walinspect;
reset client_min_messages;
-- Keep the WAL the tests read back.
select 1 from pg_create_physical_replication_slot('bark_subtree_delete', true);

-- 1000-byte keys stored inline make six entries a leaf and about seven
-- downlinks an internal page, so 3000 rows make a tree of height 4.
create function sdk(int) returns text language sql immutable as
  $$ select lpad($1::text, 8, '0') || repeat('s', 1000) $$;
create table sd (k text collate "C", n int)
  with (autovacuum_enabled = off, parallel_workers = 2);
alter table sd alter column k set storage plain;
insert into sd select sdk(g), g from generate_series(1, 3000) g;
create index sd_idx on sd using bark (k);
-- The oracle: the same rows under a btree index.
create table sd_t (k text collate "C", n int) with (autovacuum_enabled = off);
alter table sd_t alter column k set storage plain;
insert into sd_t select * from sd;
create index sd_t_idx on sd_t using btree (k);

-- Live pages per level, and the deleted ones (level -1).
create function sd_shape(idx regclass)
  returns table (lvl int8, pages int8) language sql as $$
  select case when type = 'deleted' then -1 else bark_level end, count(*)
    from bark_multi_page_stats(idx::text, 1, -1)
   where type in ('leaf', 'internal', 'root', 'deleted')
   group by 1 order by 1 $$;
create function bt_shape(idx regclass)
  returns table (lvl int8, pages int8) language sql as $$
  select btpo_level, count(*) from bt_multi_page_stats(idx::text, 1, -1)
   where type in ('l', 'i', 'r') group by 1 order by 1 $$;
select level from bark_metap('sd_idx');
select * from sd_shape('sd_idx');

-- Empty every leaf of keys 400 .. 2600: most level-1 and some level-2
-- pages lose every child.
select pg_current_wal_lsn() as lsn \gset
delete from sd where n between 400 and 2600;
delete from sd_t where n between 400 and 2600;
vacuum sd;
vacuum sd_t;
-- BARK keeps one more page at levels 2 and 3 than nbtree, which also
-- deletes the leftmost page of a level and a chain that is its parent's
-- last child, and so whole subtrees in one pass.
select * from sd_shape('sd_idx');
select * from bt_shape('sd_t_idx');
select bark_index_parent_check('sd_idx', true);
-- The deletions, by the internal levels each took with its leaf.  (VACUUM
-- assigns no XID, so its WAL is flushed by the checkpoint.)
checkpoint;
select coalesce(substring(description from 'internal levels: (\d+)'), '0') as levels,
       count(*) > 0 as some
  from pg_get_wal_records_info(:'lsn', pg_current_wal_lsn())
 where resource_manager = 'Bark' and record_type = 'UNLINK_PAGE'
 group by 1 order by 1;
-- A deleted page has no items and level 0, whatever it was.
select count(*) as deleted_pages, bool_and(live_items = 0 and bark_level = 0) as empty
  from bark_multi_page_stats('sd_idx', 1, -1) where type = 'deleted';
-- Every internal page whose left neighbour's subtree went has a first
-- downlink with no key: minus infinity, as at the left edge of a level.
select count(*) > 0 as minus_infinity_first_downlinks
  from bark_multi_page_stats('sd_idx', 1, -1) s, bark_page_items('sd_idx', s.blkno) i
 where s.type = 'internal' and s.bark_prev <> 0
   and i.itemoffset = case when s.bark_next = 0 then 1 else 2 end
   and i.data = '';

-- Every scan mode answers as the btree copy does.  sd_same compares the
-- rows a query returns from sd, with the given scan types on, with the same
-- query on sd_t.
create function sd_same(q text, mode text) returns bool language plpgsql as $$
declare a text; b text;
begin
  perform set_config('enable_seqscan', 'off', true);
  perform set_config('enable_indexscan', (mode = 'index')::text, true);
  perform set_config('enable_indexonlyscan', (mode = 'ios')::text, true);
  perform set_config('enable_bitmapscan', (mode = 'bitmap')::text, true);
  execute format('select coalesce(md5(string_agg(r::text, '','')), '''') from (%s) r', q) into a;
  execute format('select coalesce(md5(string_agg(r::text, '','')), '''') from (%s) r',
                 replace(q, 'from sd ', 'from sd_t ')) into b;
  return a = b;
end $$;
select q, bool_and(sd_same(q, m)) as same
  from (values ('select n from sd where k > '''' order by k'),
               ('select n from sd where k < ''00002700'' order by k desc'),
               ('select n from sd where k between ''00000300'' and ''00002700'' order by k'),
               ('select n from sd where k >= ''00001000'' order by k limit 3'),
               ('select n from sd where k <= ''00002600'' order by k desc limit 3'),
               ('select substr(k, 1, 8) from sd where k > ''00000390'' order by k limit 20'),
               ('select count(*) from sd where k > ''00000300'' and k < ''00002700''')) v(q),
       (values ('index'), ('ios'), ('bitmap')) w(m)
 where not (m = 'bitmap' and q ~ 'order by')
 group by q order by q;

-- A scroll cursor walks across the deleted subtrees both ways.
begin;
set local enable_seqscan = off; set local enable_bitmapscan = off;
declare c scroll cursor for select n from sd where k > '00000390' order by k;
fetch 12 from c;
fetch backward 4 from c;
fetch last from c;
fetch absolute 9 from c;
commit;

-- A parallel index scan, too.
analyze sd;
begin;
set local parallel_setup_cost = 0;
set local parallel_tuple_cost = 0;
set local min_parallel_index_scan_size = 0;
set local min_parallel_table_scan_size = 0;
set local enable_seqscan = off;
set local enable_bitmapscan = off;
set local max_parallel_workers_per_gather = 2;
-- (a table this small is cheap enough to read serially otherwise)
set local cpu_tuple_cost = 1;
set local cpu_index_tuple_cost = 1;
set local cpu_operator_cost = 1;
explain (costs off) select count(*), sum(n) from sd where k > '00000100';
select count(*), sum(n) from sd where k > '00000100';
explain (costs off) select count(*) from sd where k > '00000100';
select count(*) from sd where k > '00000100';
commit;
select count(*), sum(n) from sd_t where k > '00000100';

-- Inserts into the ranges the deleted subtrees covered go to the pages
-- that took the ranges over, and the deleted pages are reused once no
-- snapshot can still see them.
insert into sd select sdk(g), g from generate_series(400, 2600, 3) g;
insert into sd_t select sdk(g), g from generate_series(400, 2600, 3) g;
select bark_index_parent_check('sd_idx', true);
select bool_and(sd_same('select n from sd where k > '''' order by k', m)) as same
  from (values ('index'), ('ios')) w(m);
drop table sd, sd_t;

-- A queue: insert the next block of keys, delete the oldest, VACUUM.
-- Without subtree deletion every cycle leaves its emptied level-1 pages
-- behind, each with one empty leaf, and the internal levels grow without
-- bound.  With it, they stay the size the live keys need, as nbtree's do;
-- the same queue on a btree index stays as tall.
create table sq (k text collate "C", n int) with (autovacuum_enabled = off);
alter table sq alter column k set storage plain;
create index sq_idx on sq using bark (k);
create table sq_t (k text collate "C", n int) with (autovacuum_enabled = off);
alter table sq_t alter column k set storage plain;
create index sq_t_idx on sq_t using btree (k);
insert into sq select sdk(g), g from generate_series(1, 600) g;
insert into sq_t select * from sq;
create table sq_internal (cycle int, internal int8);
select format('insert into %s select sdk(g), g from generate_series(%s, %s) g', t, c * 300 + 301, c * 300 + 600),
       format('delete from %s where n <= %s', t, c * 300),
       format('vacuum %s', t),
       case when t = 'sq' then
         format($$insert into sq_internal select %s, count(*) from bark_multi_page_stats('sq_idx', 1, -1)
                   where type in ('internal', 'root') and bark_level > 0$$, c)
       else 'select' end
  from generate_series(1, 20) c, (values ('sq'), ('sq_t')) v(t) order by c, t \gexec
-- The last ten cycles need no more internal pages than the first five.
select max(internal) filter (where cycle > 10) <= max(internal) filter (where cycle <= 5)
         as internal_bounded,
       (select level from bark_metap('sq_idx')) <= (select level from bt_metap('sq_t_idx'))
         as height_bounded
  from sq_internal;
select bark_index_parent_check('sq_idx', true);
begin;
set local enable_seqscan = off; set local enable_bitmapscan = off;
select (select md5(string_agg(n::text, ',')) from (select n from sq order by k) s) =
       (select md5(string_agg(n::text, ',')) from (select n from sq_t order by k) s) as same;
commit;
drop table sq, sq_t, sq_internal;

-- An unlogged index deletes its subtrees without WAL, the same way.
create unlogged table su (k text collate "C", n int) with (autovacuum_enabled = off);
alter table su alter column k set storage plain;
insert into su select sdk(g), g from generate_series(1, 3000) g;
create index su_idx on su using bark (k);
delete from su where n between 400 and 2600;
vacuum su;
select count(*) filter (where type = 'deleted') > 400 as deleted,
       count(*) filter (where type = 'internal' and bark_level = 1) < 30 as level1_left
  from bark_multi_page_stats('su_idx', 1, -1);
select bark_index_parent_check('su_idx', true);
begin;
set local enable_seqscan = off; set local enable_bitmapscan = off;
select count(*), sum(n) from su where k > '';
commit;
drop table su;

-- An empty leaf with no downlink yet, the right half of a split that
-- stopped before its parent got the downlink, is not deleted: its parent
-- does not name it.  The split's next insert finishes it.
set client_min_messages to 'warning';
create extension if not exists injection_points;
reset client_min_messages;
select injection_points_set_local();
create table sr (k text collate "C", n int) with (autovacuum_enabled = off);
alter table sr alter column k set storage plain;
insert into sr select sdk(g * 10), g * 10 from generate_series(1, 60) g;
create index sr_idx on sr using bark (k);
select injection_points_attach('bark-leave-leaf-split-incomplete', 'error');
insert into sr values (sdk(205), 205);
select injection_points_detach('bark-leave-leaf-split-incomplete');
-- empty the split's right half
select s.blkno as half
  from bark_multi_page_stats('sr_idx', 1, -1) s
 where 'incomplete_split' = any (s.flags) \gset
delete from sr where ctid in
  (select i.htid from bark_page_stats('sr_idx', :half) s,
                      bark_page_items('sr_idx', s.bark_next) i where i.htid is not null);
select bark_next as half_right from bark_page_stats('sr_idx', :half) \gset
vacuum sr;
select type, live_items = 1 as only_high_key from bark_page_stats('sr_idx', :half_right);
-- an insert through the left half finishes the split; then VACUUM deletes it
insert into sr values (sdk(200) || 'b', 0);
vacuum sr;
select type from bark_page_stats('sr_idx', :half_right);
select bark_index_parent_check('sr_idx', true);
drop table sr;

-- OVERSIZED separators: 3 kB keys stored uncompressed, so every high key and
-- downlink is OVERSIZED, with its key on an overflow chain shared by the
-- copies one split made.  Deleting a subtree removes every item that named
-- the removed downlink's chain (the deleted pages' high keys, the downlink
-- itself and the first items of their right siblings, which become minus
-- infinity), and the chain is freed: amcheck finds no orphaned chain.
create function sdo(int) returns text language sql immutable as
  $$ select lpad($1::text, 8, '0') || repeat('o', 3000) $$;
create table so (k text collate "C", n int) with (autovacuum_enabled = off);
alter table so alter column k set storage plain;
insert into so select sdo(g), g from generate_series(1, 3000) g;
create index so_idx on so using bark (k) with (fillfactor = 10);
select level >= 2 as tall from bark_metap('so_idx');
create function so_overflow() returns int8 language sql as $$
  select count(*) from bark_multi_page_stats('so_idx', 1, -1) where type = 'overflow' $$;
select so_overflow() as overflow_before \gset
delete from so where n between 50 and 2850;
select pg_current_wal_lsn() as lsn \gset
vacuum so;
checkpoint;
select count(*) > 0 as subtrees_deleted
  from pg_get_wal_records_info(:'lsn', pg_current_wal_lsn())
 where resource_manager = 'Bark' and record_type = 'UNLINK_PAGE'
   and description ~ 'internal levels';
select so_overflow() < :overflow_before as chains_freed;
select bark_index_parent_check('so_idx', true);
set enable_seqscan = off; set enable_bitmapscan = off;
select count(*), sum(n) from so where k > '';
select count(*) from so where k = sdo(49) or k = sdo(2851);
reset enable_seqscan; reset enable_bitmapscan;
drop table so;
select pg_drop_replication_slot('bark_subtree_delete');
drop function sdk(int), sdo(int), sd_shape(regclass), bt_shape(regclass),
  sd_same(text, text), so_overflow();

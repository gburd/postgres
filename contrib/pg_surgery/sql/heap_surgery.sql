create extension pg_surgery;

-- create a normal heap table and insert some rows.
-- use a temp table so that vacuum behavior doesn't depend on global xmin
create temp table htab (a int);
insert into htab values (100), (200), (300), (400), (500);

-- test empty TID array
select heap_force_freeze('htab'::regclass, ARRAY[]::tid[]);

-- nothing should be frozen yet
select * from htab where xmin = 2;

-- freeze forcibly
select heap_force_freeze('htab'::regclass, ARRAY['(0, 4)']::tid[]);

-- now we should have one frozen tuple
select ctid, xmax from htab where xmin = 2;

-- kill forcibly
select heap_force_kill('htab'::regclass, ARRAY['(0, 4)']::tid[]);

-- should be gone now
select * from htab where ctid = '(0, 4)';

-- should now be skipped because it's already dead
select heap_force_kill('htab'::regclass, ARRAY['(0, 4)']::tid[]);
select heap_force_freeze('htab'::regclass, ARRAY['(0, 4)']::tid[]);

-- freeze two TIDs at once while skipping an out-of-range block number
select heap_force_freeze('htab'::regclass,
						 ARRAY['(0, 1)', '(0, 3)', '(1, 1)']::tid[]);

-- we should now have two frozen tuples
select ctid, xmax from htab where xmin = 2;

-- out-of-range TIDs should be skipped
select heap_force_freeze('htab'::regclass, ARRAY['(0, 0)', '(0, 6)']::tid[]);

-- set up a new table with a redirected line pointer
-- use a temp table so that vacuum behavior doesn't depend on global xmin
create temp table htab2(a int);
insert into htab2 values (100);
update htab2 set a = 200;
vacuum htab2;

-- redirected TIDs should be skipped
select heap_force_kill('htab2'::regclass, ARRAY['(0, 1)']::tid[]);

-- now create an unused line pointer
select ctid from htab2;
update htab2 set a = 300;
select ctid from htab2;
vacuum freeze htab2;

-- unused TIDs should be skipped
select heap_force_kill('htab2'::regclass, ARRAY['(0, 2)']::tid[]);

-- multidimensional TID array should be rejected
select heap_force_kill('htab2'::regclass, ARRAY[['(0, 2)']]::tid[]);

-- TID array with nulls should be rejected
select heap_force_kill('htab2'::regclass, ARRAY[NULL]::tid[]);

-- but we should be able to kill the one tuple we have
select heap_force_kill('htab2'::regclass, ARRAY['(0, 3)']::tid[]);

-- materialized view.
-- note that we don't commit the transaction, so autovacuum can't interfere.
begin;
create materialized view mvw as select a from generate_series(1, 3) a;

select * from mvw where xmin = 2;
select heap_force_freeze('mvw'::regclass, ARRAY['(0, 3)']::tid[]);
select * from mvw where xmin = 2;

select heap_force_kill('mvw'::regclass, ARRAY['(0, 3)']::tid[]);
select * from mvw where ctid = '(0, 3)';
rollback;

-- check that it fails on an unsupported relkind
create view vw as select 1;
select heap_force_kill('vw'::regclass, ARRAY['(0, 1)']::tid[]);
select heap_force_freeze('vw'::regclass, ARRAY['(0, 1)']::tid[]);

-- HOT-indexed tombstones are LP_NORMAL items that are not real tuples; forcing
-- them would corrupt the heap (freeze would surface a phantom natts==0 row,
-- kill would drop a chain hop), so both functions must skip them.
create extension pageinspect;
create table htomb (id int primary key, a int, b int) with (fillfactor = 50);
create index htomb_a on htomb(a);
insert into htomb values (1, 10, 20);
-- changes one of two indexed attrs (<= threshold) -> HOT-indexed -> tombstone
update htomb set a = 11 where id = 1;
select n_tombstones > 0 as have_tombstone
  from pg_relation_hot_indexed_stats('htomb');
-- locate the tombstone on block 0: LP_NORMAL item with HEAP_INDEXED_UPDATED
-- (infomask2 & 0x0800) set and natts (infomask2 & 0x07FF) zero.
select lp as tomb_off
  from heap_page_items(get_raw_page('htomb', 0))
 where lp_flags = 1 and (t_infomask2 & 2048) <> 0 and (t_infomask2 & 2047) = 0 \gset
-- both surgery ops must skip it (NOTICE), leaving the heap unchanged
select heap_force_freeze('htomb'::regclass, ARRAY[('(0,' || :tomb_off || ')')::tid]);
select heap_force_kill('htomb'::regclass, ARRAY[('(0,' || :tomb_off || ')')::tid]);
-- live row intact, no phantom, tombstone still present
select id, a, b from htomb;
select n_tombstones > 0 as still_have_tombstone
  from pg_relation_hot_indexed_stats('htomb');
drop table htomb;
drop extension pageinspect;

-- cleanup.
drop view vw;
drop extension pg_surgery;

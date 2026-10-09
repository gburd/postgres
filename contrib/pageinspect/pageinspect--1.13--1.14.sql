/* contrib/pageinspect/pageinspect--1.13--1.14.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION pageinspect UPDATE TO '1.14'" to load this file. \quit

--
-- bark_metap()
--
CREATE FUNCTION bark_metap(IN relname text,
    OUT magic int4,
    OUT version int4,
    OUT root int8,
    OUT level int8,
    OUT allequalimage boolean,
    OUT flags int8,
    OUT nkeys int8)
AS 'MODULE_PATHNAME', 'bark_metap'
LANGUAGE C STRICT PARALLEL SAFE;

--
-- bark_page_stats()
--
CREATE FUNCTION bark_page_stats(IN relname text, IN blkno int8,
    OUT blkno int8,
    OUT type text,
    OUT live_items int4,
    OUT dead_items int4,
    OUT avg_item_size int4,
    OUT page_size int4,
    OUT free_size int4,
    OUT bark_prev int8,
    OUT bark_next int8,
    OUT bark_level int8,
    OUT bark_cycleid int4,
    OUT flags text[])
AS 'MODULE_PATHNAME', 'bark_page_stats'
LANGUAGE C STRICT PARALLEL SAFE;

--
-- bark_multi_page_stats()
--
CREATE FUNCTION bark_multi_page_stats(IN relname text, IN blkno int8,
    IN blk_count int8,
    OUT blkno int8,
    OUT type text,
    OUT live_items int4,
    OUT dead_items int4,
    OUT avg_item_size int4,
    OUT page_size int4,
    OUT free_size int4,
    OUT bark_prev int8,
    OUT bark_next int8,
    OUT bark_level int8,
    OUT bark_cycleid int4,
    OUT flags text[])
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'bark_multi_page_stats'
LANGUAGE C STRICT PARALLEL RESTRICTED;

--
-- bark_page_items()
--
CREATE FUNCTION bark_page_items(IN relname text, IN blkno int8,
    OUT itemoffset smallint,
    OUT ctid tid,
    OUT itemlen smallint,
    OUT shape text,
    OUT ntids int4,
    OUT htid tid,
    OUT tids tid[],
    OUT downlink int8,
    OUT overflow_blkno int8,
    OUT data text)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'bark_page_items'
LANGUAGE C STRICT PARALLEL SAFE;

CREATE FUNCTION bark_page_items(IN page bytea,
    OUT itemoffset smallint,
    OUT ctid tid,
    OUT itemlen smallint,
    OUT shape text,
    OUT ntids int4,
    OUT htid tid,
    OUT tids tid[],
    OUT downlink int8,
    OUT overflow_blkno int8,
    OUT data text)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'bark_page_items_bytea'
LANGUAGE C STRICT PARALLEL SAFE;

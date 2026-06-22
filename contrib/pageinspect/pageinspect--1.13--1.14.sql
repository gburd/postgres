/* contrib/pageinspect/pageinspect--1.13--1.14.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION pageinspect UPDATE TO '1.14'" to load this file. \quit

--
-- recno_page_items()
--
CREATE FUNCTION recno_page_items(IN page bytea,
    OUT lp smallint,
    OUT lp_off smallint,
    OUT lp_flags smallint,
    OUT lp_len smallint,
    OUT t_len integer,
    OUT t_natts smallint,
    OUT t_flags smallint,
    OUT t_commit_ts bigint,
    OUT t_infomask smallint,
    OUT t_data bytea)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'recno_page_items'
LANGUAGE C STRICT PARALLEL SAFE;

--
-- recno_page_stats()
--
CREATE FUNCTION recno_page_stats(IN page bytea,
    OUT lsn pg_lsn,
    OUT tli smallint,
    OUT flags smallint,
    OUT lower smallint,
    OUT upper smallint,
    OUT special smallint,
    OUT pagesize smallint,
    OUT version smallint,
    OUT free_size smallint,
    OUT pd_commit_ts bigint,
    OUT pd_free_space smallint,
    OUT pd_flags integer,
    OUT max_off integer)
RETURNS record
AS 'MODULE_PATHNAME', 'recno_page_stats'
LANGUAGE C STRICT PARALLEL SAFE;

--
-- recno_tuple_infomask_flags()
--
CREATE FUNCTION recno_tuple_infomask_flags(
    IN t_infomask integer,
    OUT raw_flags text[],
    OUT combined_flags text[])
RETURNS record
AS 'MODULE_PATHNAME', 'recno_tuple_infomask_flags'
LANGUAGE C STRICT PARALLEL SAFE;

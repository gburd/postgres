/* src/test/modules/test_undo_tam/test_undo_tam--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_undo_tam" to load this file. \quit

-- Handler function for the table access method
CREATE FUNCTION test_undo_tam_handler(internal)
RETURNS table_am_handler
AS 'MODULE_PATHNAME'
LANGUAGE C;

-- Create the table access method
CREATE ACCESS METHOD test_undo_tam TYPE TABLE HANDLER test_undo_tam_handler;
COMMENT ON ACCESS METHOD test_undo_tam IS 'test table AM using per-relation UNDO for MVCC';

-- Introspection function to dump the UNDO chain for a relation
CREATE FUNCTION test_undo_tam_dump_chain(regclass)
RETURNS TABLE (
    undo_ptr bigint,
    rec_type text,
    xid xid,
    prev_undo_ptr bigint,
    payload_size integer,
    first_tid tid,
    end_tid tid
)
AS 'MODULE_PATHNAME', 'test_undo_tam_dump_chain'
LANGUAGE C STRICT;

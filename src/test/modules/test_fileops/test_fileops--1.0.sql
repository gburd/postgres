/* src/test/modules/test_fileops/test_fileops--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_fileops" to load this file. \quit

-- Create a temporary file and return its path
CREATE FUNCTION test_fileops_create_tempfile(filename text)
   RETURNS text
   AS 'MODULE_PATHNAME' LANGUAGE C STRICT;

-- Truncate a file to the given length (uses FileOpsTruncate)
CREATE FUNCTION test_fileops_truncate(filepath text, length bigint)
   RETURNS void
   AS 'MODULE_PATHNAME' LANGUAGE C STRICT;

-- Get file size
CREATE FUNCTION test_fileops_file_size(filepath text)
   RETURNS bigint
   AS 'MODULE_PATHNAME' LANGUAGE C STRICT;

-- Chmod a file (uses FileOpsChmod)
CREATE FUNCTION test_fileops_chmod(filepath text, mode int)
   RETURNS void
   AS 'MODULE_PATHNAME' LANGUAGE C STRICT;

-- Get file permissions (mode)
CREATE FUNCTION test_fileops_get_mode(filepath text)
   RETURNS int
   AS 'MODULE_PATHNAME' LANGUAGE C STRICT;

-- Create a hard link (uses FileOpsLink)
CREATE FUNCTION test_fileops_link(oldpath text, newpath text)
   RETURNS void
   AS 'MODULE_PATHNAME' LANGUAGE C STRICT;

-- Check if a file exists
CREATE FUNCTION test_fileops_file_exists(filepath text)
   RETURNS boolean
   AS 'MODULE_PATHNAME' LANGUAGE C STRICT;

-- Set extended attribute (uses FileOpsSetXattr)
CREATE FUNCTION test_fileops_setxattr(filepath text, attrname text, attrvalue text)
   RETURNS boolean
   AS 'MODULE_PATHNAME' LANGUAGE C STRICT;

-- Get extended attribute value
CREATE FUNCTION test_fileops_getxattr(filepath text, attrname text)
   RETURNS text
   AS 'MODULE_PATHNAME' LANGUAGE C STRICT;

-- Remove extended attribute (uses FileOpsRemoveXattr)
CREATE FUNCTION test_fileops_removexattr(filepath text, attrname text)
   RETURNS boolean
   AS 'MODULE_PATHNAME' LANGUAGE C STRICT;

-- Get the data directory path (for constructing absolute paths)
CREATE FUNCTION test_fileops_data_dir()
   RETURNS text
   AS 'MODULE_PATHNAME' LANGUAGE C STRICT;

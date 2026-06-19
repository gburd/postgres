CREATE FUNCTION test_ext_backend_model_get()
RETURNS text
AS 'test_ext_backend_model', 'test_ext_backend_model_get'
LANGUAGE C;

CREATE FUNCTION test_ext_backend_model_set(text)
RETURNS text
AS 'test_ext_backend_model', 'test_ext_backend_model_set'
LANGUAGE C STRICT;

CREATE FUNCTION test_ext_backend_model_expect_load_error(text, text)
RETURNS text
AS 'test_ext_backend_model', 'test_ext_backend_model_expect_load_error'
LANGUAGE C STRICT;

CREATE FUNCTION test_ext_backend_model_expect_lookup_error(text, text, text, text)
RETURNS text
AS 'test_ext_backend_model', 'test_ext_backend_model_expect_lookup_error'
LANGUAGE C STRICT;

CREATE FUNCTION test_ext_backend_model_expect_set_error(text, text)
RETURNS text
AS 'test_ext_backend_model', 'test_ext_backend_model_expect_set_error'
LANGUAGE C STRICT;

SELECT test_ext_backend_model_get();
SELECT test_ext_backend_model_expect_load_error('test_ext_bad_backend_model',
											   'invalid backend model');
SELECT test_ext_backend_model_expect_load_error('test_ext_short_magic',
											   'magic block mismatch');
SELECT test_ext_backend_model_set('thread-per-session');
LOAD 'test_ext_threaded';
LOAD 'test_ext_backend_model';
SELECT test_ext_backend_model_expect_load_error('test_ext',
											   'backend model mismatch');
SELECT test_ext_backend_model_expect_load_error('test_ext_bad_backend_model',
											   'invalid backend model');
SELECT test_ext_backend_model_expect_load_error('test_ext_short_magic',
											   'magic block mismatch');
LOAD 'plpgsql';
SELECT test_ext_backend_model_expect_lookup_error('test_ext_backend_model',
												 'test_ext_backend_model_get',
												 'pooled-protocol-affine',
												 'backend model mismatch');
SELECT test_ext_backend_model_expect_set_error('pooled-protocol-affine',
											  'backend model mismatch');

SELECT test_ext_backend_model_set('process');
LOAD 'test_ext';
LOAD 'test_ext_threaded';
LOAD 'plpgsql';
SELECT test_ext_backend_model_expect_lookup_error('test_ext', 'test_ext',
												 'thread-per-session',
												 'backend model mismatch');
SELECT test_ext_backend_model_get();
SELECT test_ext_backend_model_expect_set_error('thread-per-session',
											  'backend model mismatch');
SELECT test_ext_backend_model_get();

SELECT test_ext_backend_model_set('not-a-model');

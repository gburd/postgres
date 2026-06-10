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

SELECT test_ext_backend_model_get();

LOAD 'test_ext';

SELECT test_ext_backend_model_set('thread-per-session');
LOAD 'test_ext_threaded';
LOAD 'test_ext_backend_model';
SELECT test_ext_backend_model_expect_load_error('test_ext',
											   'backend model mismatch');
SELECT test_ext_backend_model_expect_load_error('plpgsql',
											   'backend model mismatch');

SELECT test_ext_backend_model_set('pooled-scheduler');
LOAD 'test_ext_backend_model';
SELECT test_ext_backend_model_expect_load_error('test_ext_threaded',
											   'backend model mismatch');

SELECT test_ext_backend_model_set('process');
LOAD 'test_ext';
LOAD 'test_ext_threaded';
LOAD 'plpgsql';
SELECT test_ext_backend_model_get();

SELECT test_ext_backend_model_set('not-a-model');

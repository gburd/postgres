CREATE OR REPLACE FUNCTION test_ext_backend_model_set(text)
RETURNS text
AS 'test_ext_backend_model', 'test_ext_backend_model_set'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION test_ext_backend_model_expect_load_error(text, text)
RETURNS text
AS 'test_ext_backend_model', 'test_ext_backend_model_expect_load_error'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION test_ext_backend_model_expect_set_error(text, text)
RETURNS text
AS 'test_ext_backend_model', 'test_ext_backend_model_expect_set_error'
LANGUAGE C STRICT;

SELECT test_ext_backend_model_set('pooled-scheduler');
LOAD 'test_ext_backend_model';
SELECT test_ext_backend_model_expect_set_error('pooled-protocol-affine',
											  'backend model mismatch');
SELECT test_ext_backend_model_expect_load_error('test_ext_threaded',
											   'backend model mismatch');
SELECT test_ext_backend_model_expect_load_error('test_ext',
											   'not supported in the threaded backend runtime');
SELECT test_ext_backend_model_expect_load_error('test_ext_bad_backend_model',
											   'invalid backend model');
SELECT test_ext_backend_model_expect_load_error('test_ext_short_magic',
											   'magic block mismatch');
-- plpgsql is pooled-protocol-affine, which satisfies the pooled-scheduler
-- requirement set above, so it loads cleanly (it is not an incompatible module).
LOAD 'plpgsql';
SELECT test_ext_backend_model_set('not-a-model');

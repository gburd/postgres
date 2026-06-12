CREATE EXTENSION test_backend_runtime;

SELECT test_backend_exit_runtime_continuation();
SELECT test_backend_dsm_shutdown_is_backend_local();
SELECT test_backend_interrupt_wakes_target_latch();

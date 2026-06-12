CREATE EXTENSION test_backend_runtime;

SELECT test_backend_exit_runtime_continuation();
SELECT test_backend_dsm_shutdown_is_backend_local();
SELECT test_backend_interrupt_wakes_target_latch();
SELECT test_backend_thread_create_join();
SELECT test_backend_thread_exit_join();
SELECT test_backend_thread_runtime_state();
SELECT test_backend_pgproc_has_logical_id();
SELECT test_backend_thread_ids_are_logical();
SELECT test_backend_interrupt_holdoffs_are_backend_local();

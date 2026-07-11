SELECT name, type, size > 0 AS size_ok
FROM pg_dsm_registry_allocations
WHERE name like 'test_dsm_registry%' ORDER BY name;
CREATE EXTENSION test_dsm_registry;
SELECT set_val_in_shmem(1236);
SELECT set_val_in_hash('test', '1414');
\c
SELECT get_val_in_shmem();
SELECT get_val_in_hash('test');
\c
SELECT reset_dsm_detach_count();
SELECT register_dsm_detach_for_backend_exit();
\c
SELECT get_dsm_detach_count();
\c
SELECT reset_exit_callback_order();
SELECT register_exit_callback_order();
\c
SELECT get_exit_callback_order();
\c
SELECT reset_backend_exit_temp_file_path();
SELECT create_temp_file_for_backend_exit();
\c
SELECT backend_exit_temp_file_removed();
SELECT name, type, size > 0 AS size_ok
FROM pg_dsm_registry_allocations
WHERE name like 'test_dsm_registry%' ORDER BY name;

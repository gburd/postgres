# Copyright (c) 2024-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long
"""Port of src/test/modules/test_shmem/t/001_late_shmem_alloc.pl.

A shared-memory segment requested after startup (via CREATE EXTENSION) runs its
attach callback in every backend, so a per-backend attach counter rises with
each new connection. When the module is instead loaded via
shared_preload_libraries, the segment is allocated once in the postmaster and
inherited by fork, so without EXEC_BACKEND the attach callback never runs.
"""


def test_001_late_shmem_alloc(create_pg):
    """Late shmem attach callback fires per backend, but not when preloaded."""
    node = create_pg("main")
    node.safe_psql("CREATE EXTENSION test_shmem;")
    attach_count1 = node.safe_psql("SELECT get_test_shmem_attach_count();")
    attach_count2 = node.safe_psql("SELECT get_test_shmem_attach_count();")
    assert int(attach_count2) > int(
        attach_count1
    ), "attach callback is called in each backend"
    node.stop()
    node.append_conf("shared_preload_libraries = 'test_shmem'")
    node.start()
    exec_backend = node.safe_psql("SHOW debug_exec_backend;") == "on"
    attach_count1 = node.safe_psql("SELECT get_test_shmem_attach_count();")
    attach_count2 = node.safe_psql("SELECT get_test_shmem_attach_count();")
    if exec_backend:
        assert int(attach_count2) > int(
            attach_count1
        ), "attach callback is called in each backend when loaded via shared_preload_libraries"
    else:
        assert (
            int(attach_count1) == 0 and int(attach_count2) == 0
        ), "attach callback is not called when loaded via shared_preload_libraries"
    node.stop()

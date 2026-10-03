# Copyright (c) 2024-2026, PostgreSQL Global Development Group

"""Python port of TestAio (src/test/modules/test_aio/t/TestAio.pm).

Helpers for writing AIO-related pytest tests: enumerating the supported
io_method GUC values and applying the shared cluster configuration the test
suite expects.
"""

import os
import re
import subprocess

import pypg


def have_io_uring():
    """Return True if this build supports io_method=io_uring.

    Mirrors TestAio::have_io_uring. To detect whether io_uring is supported, we
    look at the error message for assigning an invalid value to the io_method
    enum GUC, which lists all the valid options. We use ``postgres -C`` so the
    superuser check is omitted (matters when running as administrator on
    Windows).

    As a fast path we first consult pg_config.h for ``#define USE_LIBURING 1``;
    when that header marker is present the runtime probe is skipped.
    """
    if pypg.check_pg_config(r"#define USE_LIBURING 1"):
        return True

    postgres = os.environ.get("PG_CONFIG")
    if postgres:
        bindir = subprocess.run(
            [postgres, "--bindir"],
            stdout=subprocess.PIPE,
            encoding="utf-8",
            check=True,
        ).stdout.strip()
        postgres = os.path.join(bindir, "postgres")
    else:
        postgres = "postgres"

    proc = subprocess.run(
        [postgres, "-C", "invalid", "-c", "io_method=invalid"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    match = re.search(r"Available values: ([^.]+)\.", proc.stderr)
    if match is None:
        raise RuntimeError("can't determine supported io_method values")
    return "io_uring" in match.group(1)


def supported_io_methods():
    """Return the list of supported values for the io_method GUC.

    Mirrors TestAio::supported_io_methods: ``worker`` first, ``io_uring`` if the
    build supports it, and ``sync`` last (it least commonly fails).
    """
    io_methods = ["worker"]
    if have_io_uring():
        io_methods.append("io_uring")
    # Return sync last, as it will least commonly fail.
    io_methods.append("sync")
    return io_methods


def configure(node):
    """Prepare a cluster for AIO tests (mirrors TestAio::configure)."""
    node.append_conf(
        "\n"
        "shared_preload_libraries=test_aio\n"
        "log_min_messages = 'DEBUG3'\n"
        "log_statement=all\n"
        "log_error_verbosity=default\n"
        "restart_after_crash=false\n"
        "temp_buffers=100\n"
    )

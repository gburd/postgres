/*--------------------------------------------------------------------------
 *
 * test_backend_runtime.h
 *		Shared declarations for backend runtime scaffolding tests.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_backend_runtime/test_backend_runtime.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef TEST_BACKEND_RUNTIME_H
#define TEST_BACKEND_RUNTIME_H

#include "postgres.h"

#include <errno.h>

#include "access/gin.h"
#include "access/parallel.h"
#include "access/session.h"
#include "access/tableam.h"
#include "access/toast_compression.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/binary_upgrade.h"
#include "catalog/storage.h"
#include "common/pg_prng.h"
#include "commands/async.h"
#include "commands/event_trigger.h"
#include "commands/explain_state.h"
#include "commands/extension.h"
#include "commands/repack.h"
#include "commands/tablespace.h"
#include "commands/trigger.h"
#include "commands/user.h"
#include "commands/vacuum.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "jit/jit.h"
#include "libpq/libpq-be.h"
#include "libpq/libpq.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/queryjumble.h"
#include "optimizer/cost.h"
#include "optimizer/extendplan.h"
#include "optimizer/geqo.h"
#include "optimizer/optimizer.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "parser/parser.h"
#include "parser/parse_expr.h"
#include "postmaster/bgworker.h"
#include "postmaster/postmaster.h"
#include "port/atomics.h"
#include "port/pg_thread.h"
#include "replication/reorderbuffer.h"
#include "replication/slot.h"
#include "replication/syncrep.h"
#include "replication/walreceiver.h"
#include "replication/walsender.h"
#include "storage/aio_internal.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/copydir.h"
#include "storage/dsm.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/large_object.h"
#include "storage/lock.h"
#include "storage/proc.h"
#include "storage/sinval.h"
#include "tcop/backend_startup.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "tsearch/ts_cache.h"
#include "utils/backend_runtime.h"
#include "postmaster/datachecksum_state.h"
#include "utils/backend_status.h"
#include "utils/builtins.h"
#include "utils/bytea.h"
#include "utils/float.h"
#include "utils/fmgroids.h"
#include "utils/fmgrprotos.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/pg_locale.h"
#include "utils/pgstat_internal.h"
#include "utils/plancache.h"
#include "utils/ps_status.h"
#include "utils/resowner.h"
#include "utils/rls.h"
#include "utils/wait_event.h"
#include "utils/xml.h"

extern void test_copy_current_user_identity(PgSession *session);

#endif							/* TEST_BACKEND_RUNTIME_H */

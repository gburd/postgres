/*-------------------------------------------------------------------------
 *
 * bgwriter.c
 *
 * The background writer (bgwriter) used to be a dedicated auxiliary process
 * that trickled dirty shared buffers out to disk so that regular backends
 * would less often have to write a dirty buffer themselves when evicting one.
 *
 * That role has been retired: dirty-buffer writeback is now handled per
 * buffer pool by trickle writers (see storage/buffer/bufpool.c), including
 * one for the default pool that starts during recovery.  Backends still write
 * their own dirty victims inline in GetVictimBuffer() when no clean buffer is
 * available, and the checkpointer performs checkpoints as before.  No
 * dedicated background writer process is launched anymore.
 *
 * What remains here is only the bgwriter_delay GUC variable, kept for
 * backward compatibility with existing configuration files (it is still a
 * valid, if now inert, parameter).  The periodic xl_running_xacts logging the
 * bgwriter once performed during recovery/standby was moved to the walwriter.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/bgwriter.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "postmaster/bgwriter.h"

/*
 * GUC parameter.  Retained for configuration-file compatibility; no process
 * consumes it now that the dedicated background writer has been retired.
 */
int			BgWriterDelay = 200;

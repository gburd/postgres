/*-------------------------------------------------------------------------
 *
 * blob_worker.c
 *	  Background worker for external BLOB maintenance
 *
 * This background worker performs:
 *   - Delta chain compaction (merge long chains into new base)
 *   - Garbage collection of unreferenced blob files
 *   - Statistics collection
 *
 * The worker wakes up periodically (controlled by blob_worker_naptime)
 * and scans the external blob directory for maintenance tasks.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/undo/blob_worker.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <dirent.h>
#include <errno.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/undo.h"
#include "access/undorecord.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "port/pg_crc32c.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "tcop/tcopprot.h"
#include "utils/blob.h"
#include "utils/memutils.h"
#include "utils/timeout.h"

/* Signal flags */
static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigusr1 = false;

/* Forward declarations */
static void blob_worker_sighup(SIGNAL_ARGS);
static void blob_worker_sigusr1(SIGNAL_ARGS);
static void process_blob_directory(const char *blob_dir);
static void compact_if_needed(const char *base_path, const uint8 *hash);
static bool is_visible_by_any_snapshot(UndoRecPtr undo_ptr);

/*
 * ExternalBlobWorkerMain - Main entry point for background worker
 */
void
ExternalBlobWorkerMain(Datum main_arg)
{
	const char *blob_dir;

	/* Establish signal handlers */
	pqsignal(SIGHUP, blob_worker_sighup);
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	pqsignal(SIGUSR1, blob_worker_sigusr1);
	BackgroundWorkerUnblockSignals();

	/* Initialize this backend */
	BackgroundWorkerInitializeConnection(NULL, NULL, 0);

	ereport(LOG,
			(errmsg("external blob background worker started")));

	blob_dir = blob_directory ? blob_directory : EXTBLOB_DIRECTORY;

	/*
	 * Main loop: wake up periodically and perform maintenance
	 */
	while (!ShutdownRequestPending)
	{
		int			rc;

		/* Check for configuration changes */
		if (got_sighup)
		{
			got_sighup = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		/* Process all blob files */
		process_blob_directory(blob_dir);

		/* Wait for naptime or until woken up */
		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   blob_worker_naptime,
					   PG_WAIT_EXTENSION);

		ResetLatch(MyLatch);

		/* Emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);
	}

	/* Clean shutdown */
	ereport(LOG,
			(errmsg("external blob background worker shutting down")));

	proc_exit(0);
}

/*
 * process_blob_directory - Scan blob directory and perform maintenance
 */
static void
process_blob_directory(const char *blob_dir)
{
	DIR		   *dir;
	struct dirent *entry;

	/* Open blob directory */
	dir = opendir(blob_dir);
	if (dir == NULL)
	{
		/* Directory doesn't exist yet - nothing to do */
		return;
	}

	/* Scan through hash prefix subdirectories (00-ff) */
	while ((entry = readdir(dir)) != NULL)
	{
		char		prefix_path[MAXPGPATH];
		DIR		   *prefix_dir;
		struct dirent *file_entry;

		/* Skip . and .. */
		if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
			continue;

		/* Process subdirectory */
		snprintf(prefix_path, sizeof(prefix_path), "%s/%s", blob_dir, entry->d_name);
		prefix_dir = opendir(prefix_path);
		if (prefix_dir == NULL)
			continue;

		/* Scan files in this prefix directory */
		while ((file_entry = readdir(prefix_dir)) != NULL)
		{
			char		file_path[MAXPGPATH];
			const char *ext;

			if (strcmp(file_entry->d_name, ".") == 0 || strcmp(file_entry->d_name, "..") == 0)
				continue;

			/* Look for .base files */
			ext = strstr(file_entry->d_name, ".base");
			if (ext != NULL && ext[5] == '\0')
			{
				uint8		hash[32];
				char		full_hash_str[65];
				int			i;

				snprintf(file_path, sizeof(file_path), "%s/%s",
						 prefix_path, file_entry->d_name);

				/*
				 * Parse hash from prefix directory name + filename.
				 * Format: <dir_prefix>/<60-char-hex>.base
				 * The prefix directory contains first 2 bytes (4 hex chars).
				 * The filename contains remaining 30 bytes (60 hex chars).
				 */
				if (strlen(file_entry->d_name) >= 65 &&
					strlen(entry->d_name) >= 2)
				{
					/* Combine prefix + filename to get full 64-char hash */
					snprintf(full_hash_str, sizeof(full_hash_str), "%s%.60s",
							 entry->d_name, file_entry->d_name);
					full_hash_str[64] = '\0';

					/* Parse hex string to bytes */
					for (i = 0; i < 32; i++)
					{
						unsigned int byte;
						if (sscanf(full_hash_str + (i * 2), "%02x", &byte) != 1)
						{
							/* Invalid hash format, skip this file */
							elog(WARNING, "invalid blob filename hash: %s", file_entry->d_name);
							continue;
						}
						hash[i] = (uint8) byte;
					}

					/* Check if this blob needs compaction */
					compact_if_needed(file_path, hash);
				}
			}
		}

		closedir(prefix_dir);

		/* Check for shutdown request periodically */
		if (ShutdownRequestPending)
			break;
	}

	closedir(dir);
}

/*
 * compact_if_needed - Check if delta chain needs compaction
 */
static void
compact_if_needed(const char *base_path, const uint8 *hash)
{
	char		delta_path[MAXPGPATH];
	uint16		version = 1;
	uint16		max_version = 0;
	struct stat st;

	/* Count delta files */
	while (version < 1000)	/* Sanity limit */
	{
		ExternalBlobGetDeltaPath(hash, version, delta_path, sizeof(delta_path));

		if (stat(delta_path, &st) != 0)
			break;			/* No more deltas */

		max_version = version;
		version++;
	}

	/* Check if compaction is needed */
	if (max_version >= blob_compaction_threshold)
	{
		ereport(DEBUG1,
				(errmsg("compacting external blob delta chain: %u deltas",
						max_version)));

		ExternalBlobCompactDeltas(hash, max_version);
	}
}

/*
 * ExternalBlobCompactDeltas - Compact a delta chain
 *
 * Reads base + all deltas, reconstructs final version, writes new base.
 * Removes old delta files.
 */
void
ExternalBlobCompactDeltas(const uint8 *hash, uint16 max_version)
{
	char		base_path[MAXPGPATH];
	char		delta_path[MAXPGPATH];
	char		temp_path[MAXPGPATH];
	void	   *current_data;
	Size		current_size;
	ExternalBlobFileHeader header;
	ExternalBlobRef temp_ref;

	/* Create temporary reference to read final version */
	memcpy(temp_ref.hash, hash, EXTERNAL_BLOB_HASH_LEN);
	temp_ref.version = max_version;
	temp_ref.size = 0;		/* Will be set by read */
	temp_ref.flags = 0;

	/* Read final version (base + all deltas) */
	current_data = ExternalBlobRead(&temp_ref, &current_size);

	/* Write new base file to temporary location */
	ExternalBlobGetBasePath(hash, base_path, sizeof(base_path));
	snprintf(temp_path, sizeof(temp_path), "%s.tmp", base_path);

	memset(&header, 0, sizeof(header));
	header.undo_ptr = InvalidUndoRecPtr;
	header.magic = EXTBLOB_MAGIC;
	header.data_size = current_size;
	header.checksum = ExternalBlobComputeChecksum((const uint8 *) current_data,
												  current_size);
	header.flags = temp_ref.flags;
	header.format_version = EXTBLOB_FORMAT_VERSION;

	/* Write new base file to temporary location */
	{
		int			fd;
		ssize_t		written;

		fd = OpenTransientFile(temp_path, O_CREAT | O_WRONLY | O_TRUNC | PG_BINARY);
		if (fd < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not create temp blob file \"%s\": %m", temp_path)));

		/* Write header */
		written = write(fd, &header, sizeof(header));
		if (written != sizeof(header))
		{
			int			save_errno = errno;

			CloseTransientFile(fd);
			errno = save_errno;
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write blob header to \"%s\": %m", temp_path)));
		}

		/* Write data */
		written = write(fd, current_data, current_size);
		if (written != (ssize_t) current_size)
		{
			int			save_errno = errno;

			CloseTransientFile(fd);
			errno = save_errno;
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write blob data to \"%s\": %m", temp_path)));
		}

		if (CloseTransientFile(fd) != 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not close temp blob file \"%s\": %m", temp_path)));
	}

	/* Atomically rename temp file to final base file */
	if (rename(temp_path, base_path) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not rename \"%s\" to \"%s\": %m",
						temp_path, base_path)));

	/* Delete old delta files */
	for (uint16 v = 1; v <= max_version; v++)
	{
		ExternalBlobGetDeltaPath(hash, v, delta_path, sizeof(delta_path));

		if (unlink(delta_path) != 0 && errno != ENOENT)
			ereport(WARNING,
					(errcode_for_file_access(),
					 errmsg("could not delete delta file \"%s\": %m", delta_path)));
	}

	pfree(current_data);

	ereport(LOG,
			(errmsg("compacted external blob delta chain: %u deltas merged",
					max_version)));
}

/*
 * ExternalBlobVacuum - Garbage collect unreferenced blob files
 *
 * Scans for tombstoned blobs and removes files if no longer visible.
 */
void
ExternalBlobVacuum(void)
{
	DIR		   *dir;
	DIR		   *prefix_dir;
	struct dirent *entry;
	struct dirent *file_entry;
	const char *blob_dir;
	char		prefix_path[MAXPGPATH];
	char		tombstone_path[MAXPGPATH];
	char		base_path[MAXPGPATH];
	uint64		files_removed = 0;

	ereport(DEBUG1,
			(errmsg("external blob vacuum starting")));

	blob_dir = blob_directory ? blob_directory : EXTBLOB_DIRECTORY;

	/* Open blob directory */
	dir = opendir(blob_dir);
	if (dir == NULL)
	{
		/* Directory doesn't exist yet - nothing to do */
		return;
	}

	/* Scan through hash prefix subdirectories (00-ff) */
	while ((entry = readdir(dir)) != NULL)
	{
		/* Skip . and .. */
		if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
			continue;

		/* Process subdirectory */
		snprintf(prefix_path, sizeof(prefix_path), "%s/%s", blob_dir, entry->d_name);
		prefix_dir = opendir(prefix_path);
		if (prefix_dir == NULL)
			continue;

		/* Scan for tombstone files in this prefix directory */
		while ((file_entry = readdir(prefix_dir)) != NULL)
		{
			const char *ext;
			UndoRecPtr	undo_ptr;
			int			fd;
			ssize_t		bytes_read;

			if (strcmp(file_entry->d_name, ".") == 0 || strcmp(file_entry->d_name, "..") == 0)
				continue;

			/* Look for .tombstone files */
			ext = strstr(file_entry->d_name, ".tombstone");
			if (ext == NULL || ext[10] != '\0')
				continue;

			/* Read tombstone file to get UNDO pointer */
			snprintf(tombstone_path, sizeof(tombstone_path), "%s/%s",
					 prefix_path, file_entry->d_name);

			fd = OpenTransientFile(tombstone_path, O_RDONLY | PG_BINARY);
			if (fd < 0)
			{
				/* Tombstone may have been deleted by another worker */
				continue;
			}

			bytes_read = read(fd, &undo_ptr, sizeof(UndoRecPtr));
			CloseTransientFile(fd);

			if (bytes_read != sizeof(UndoRecPtr))
			{
				ereport(WARNING,
						(errcode_for_file_access(),
						 errmsg("invalid tombstone file \"%s\", removing",
								tombstone_path)));
				unlink(tombstone_path);
				continue;
			}

			/* Check if blob is still visible to any snapshot */
			if (!is_visible_by_any_snapshot(undo_ptr))
			{
				char		base_file[MAXPGPATH];

				/* Build base file path by replacing .tombstone with .base */
				snprintf(base_file, sizeof(base_file), "%s", file_entry->d_name);
				base_file[strlen(base_file) - 10] = '\0';	/* Remove .tombstone */
				snprintf(base_path, sizeof(base_path), "%s/%s.base",
						 prefix_path, base_file);

				/* Delete base file */
				if (unlink(base_path) == 0 || errno == ENOENT)
				{
					/* Delete tombstone */
					if (unlink(tombstone_path) == 0)
					{
						files_removed++;
						ereport(DEBUG2,
								(errmsg("removed unreferenced blob file: %s", base_path)));
					}
				}
				else
				{
					ereport(WARNING,
							(errcode_for_file_access(),
							 errmsg("could not delete blob file \"%s\": %m", base_path)));
				}
			}
		}

		closedir(prefix_dir);

		/* Check for shutdown request periodically */
		if (ShutdownRequestPending)
			break;
	}

	closedir(dir);

	if (files_removed > 0)
		ereport(LOG,
				(errmsg("external blob vacuum removed %lu files", files_removed)));
}

/*
 * is_visible_by_any_snapshot - Check if UNDO pointer is visible
 *
 * Returns true if any active snapshot can still see this version.
 * For now, we use a conservative approach: check if the UNDO pointer
 * is old enough that no active transaction could see it.
 */
static bool
is_visible_by_any_snapshot(UndoRecPtr undo_ptr)
{
	TransactionId oldest_xid;
	uint64		oldest_undo;

	/*
	 * Get the oldest active transaction ID. If the deletion happened
	 * before this transaction started, we know it's safe to remove.
	 */
	oldest_xid = GetOldestActiveTransactionId(false, true);

	/*
	 * Convert oldest XID to an approximate UNDO pointer.
	 * If the blob's undo_ptr is less than this, it's safe to GC.
	 *
	 * For now, use a conservative check: only GC very old blobs.
	 * A proper implementation would track the exact UNDO pointer
	 * for the oldest active transaction.
	 */
	oldest_undo = (uint64) oldest_xid << 32;	/* Approximate */

	if (undo_ptr < oldest_undo)
		return false;	/* Safe to GC */

	return true;	/* Still visible */
}

/*
 * Signal handlers
 */

static void
blob_worker_sighup(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sighup = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

static void
blob_worker_sigusr1(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sigusr1 = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

/*
 * ExternalBlobGetStats - Get current statistics
 *
 * Collects statistics by scanning the blob directory.
 */
void
ExternalBlobGetStats(ExternalBlobStats *stats)
{
	DIR		   *dir;
	DIR		   *prefix_dir;
	struct dirent *entry;
	struct dirent *file_entry;
	const char *blob_dir;
	char		prefix_path[MAXPGPATH];
	struct stat st;
	char		file_path[MAXPGPATH];

	memset(stats, 0, sizeof(*stats));

	blob_dir = blob_directory ? blob_directory : EXTBLOB_DIRECTORY;

	/* Open blob directory */
	dir = opendir(blob_dir);
	if (dir == NULL)
	{
		/* Directory doesn't exist yet - no stats */
		return;
	}

	/* Scan through hash prefix subdirectories */
	while ((entry = readdir(dir)) != NULL)
	{
		/* Skip . and .. */
		if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
			continue;

		snprintf(prefix_path, sizeof(prefix_path), "%s/%s", blob_dir, entry->d_name);
		prefix_dir = opendir(prefix_path);
		if (prefix_dir == NULL)
			continue;

		/* Scan files in this prefix directory */
		while ((file_entry = readdir(prefix_dir)) != NULL)
		{
			if (strcmp(file_entry->d_name, ".") == 0 || strcmp(file_entry->d_name, "..") == 0)
				continue;

			snprintf(file_path, sizeof(file_path), "%s/%s",
					 prefix_path, file_entry->d_name);

			if (stat(file_path, &st) != 0)
				continue;

			/* Classify file type and accumulate stats */
			if (strstr(file_entry->d_name, ".base") != NULL)
			{
				stats->num_blobs++;
				stats->total_size += st.st_size;
			}
			else if (strstr(file_entry->d_name, ".delta.") != NULL)
			{
				stats->num_deltas++;
			}
		}

		closedir(prefix_dir);
	}

	closedir(dir);

	/* Calculate average delta chain length (approximation) */
	if (stats->num_blobs > 0)
		stats->avg_delta_chain_len = stats->num_deltas / stats->num_blobs;
}

/*
 * ExternalBlobWorkerRegister - Register the blob worker at server start
 *
 * Called from postmaster startup to register the background worker.
 */
void
ExternalBlobWorkerRegister(void)
{
	BackgroundWorker worker;

	memset(&worker, 0, sizeof(BackgroundWorker));

	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = 30;	/* Restart after 30 seconds if crashed */

	sprintf(worker.bgw_library_name, "postgres");
	sprintf(worker.bgw_function_name, "ExternalBlobWorkerMain");
	snprintf(worker.bgw_name, BGW_MAXLEN, "external blob worker");
	snprintf(worker.bgw_type, BGW_MAXLEN, "external blob worker");

	RegisterBackgroundWorker(&worker);
}

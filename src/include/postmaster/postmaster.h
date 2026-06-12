/*-------------------------------------------------------------------------
 *
 * postmaster.h
 *	  Exports from postmaster/postmaster.c.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/postmaster/postmaster.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _POSTMASTER_H
#define _POSTMASTER_H

#include "lib/ilist.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "port/pg_thread.h"
#include "utils/global_lifetime.h"

struct Latch;

/*
 * A struct representing an active postmaster child.  This is used mainly to
 * keep track of how many children we have and signal process-backed children
 * when necessary.  All postmaster children are assigned a PMChild entry.  That
 * includes "normal" client sessions, but also autovacuum workers, walsenders,
 * background workers, and aux processes.  (Note that at the time of launch,
 * walsenders are labeled B_BACKEND; we relabel them to B_WAL_SENDER upon
 * noticing they've changed their PMChildFlags entry.  Hence that check must be
 * done before any operation that needs to distinguish walsenders from normal
 * backends.)
 *
 * "dead-end" children are also allocated a PMChild entry: these are children
 * launched just for the purpose of sending a friendly rejection message to a
 * would-be client.  We must track them because they are attached to shared
 * memory, but we know they will never become live backends.
 *
 * child_slot is an identifier that is unique across all running child
 * processes.  It is used as an index into the PMChildFlags array.  dead-end
 * children are not assigned a child_slot and have child_slot == 0 (valid
 * child_slot ids start from 1).
 */
typedef enum PMChildCarrierKind
{
	PM_CHILD_CARRIER_PROCESS,
	PM_CHILD_CARRIER_THREAD
} PMChildCarrierKind;

typedef struct
{
	PMChildCarrierKind carrier_kind;	/* process, thread, or future carrier */
	pid_t		pid;			/* process id, if process-backed */
	PgThread	thread;			/* native thread handle, if thread-backed */
	int			thread_exitstatus;	/* waitpid-style status for threads */
	pg_atomic_uint32 thread_exited;	/* set when a thread carrier exits */
	int			child_slot;		/* PMChildSlot for this backend, if any */
	BackendType bkend_type;		/* child process flavor, see above */
	struct RegisteredBgWorker *rw;	/* bgworker info, if this is a bgworker */
	bool		bgworker_notify;	/* gets bgworker start/stop notifications */
	dlist_node	elem;			/* list link in ActiveChildList */
} PMChild;

#ifdef EXEC_BACKEND
extern PGDLLIMPORT PG_GLOBAL_RUNTIME int num_pmchild_slots;
#endif

/* GUC options */
extern PGDLLIMPORT PG_GLOBAL_RUNTIME bool EnableSSL;
extern PGDLLIMPORT PG_GLOBAL_RUNTIME int SuperuserReservedConnections;
extern PGDLLIMPORT PG_GLOBAL_RUNTIME int ReservedConnections;
extern PGDLLIMPORT PG_GLOBAL_RUNTIME int PostPortNumber;
extern PGDLLIMPORT PG_GLOBAL_RUNTIME int Unix_socket_permissions;
extern PGDLLIMPORT PG_GLOBAL_RUNTIME char *Unix_socket_group;
extern PGDLLIMPORT PG_GLOBAL_RUNTIME char *Unix_socket_directories;
extern PGDLLIMPORT PG_GLOBAL_RUNTIME char *ListenAddresses;
extern PGDLLIMPORT PG_THREAD_LOCAL PG_GLOBAL_CONNECTION bool ClientAuthInProgress;
extern PGDLLIMPORT PG_GLOBAL_RUNTIME int PreAuthDelay;
extern PGDLLIMPORT PG_GLOBAL_RUNTIME int AuthenticationTimeout;
extern PGDLLIMPORT PG_GLOBAL_RUNTIME bool log_hostname;
extern PGDLLIMPORT PG_GLOBAL_RUNTIME bool enable_bonjour;
extern PGDLLIMPORT PG_GLOBAL_RUNTIME char *bonjour_name;
extern PGDLLIMPORT PG_GLOBAL_RUNTIME bool restart_after_crash;
extern PGDLLIMPORT PG_GLOBAL_RUNTIME bool remove_temp_files_after_crash;
extern PGDLLIMPORT PG_GLOBAL_RUNTIME bool send_abort_for_crash;
extern PGDLLIMPORT PG_GLOBAL_RUNTIME bool send_abort_for_kill;

#ifdef WIN32
extern PGDLLIMPORT PG_GLOBAL_RUNTIME HANDLE PostmasterHandle;
#else
extern PGDLLIMPORT PG_GLOBAL_RUNTIME int postmaster_alive_fds[2];

/*
 * Constants that represent which of postmaster_alive_fds is held by
 * postmaster, and which is used in children to check for postmaster death.
 */
#define POSTMASTER_FD_WATCH		0	/* used in children to check for
									 * postmaster death */
#define POSTMASTER_FD_OWN		1	/* kept open by postmaster only */
#endif

extern PGDLLIMPORT PG_GLOBAL_RUNTIME const char *progname;

extern PGDLLIMPORT PG_GLOBAL_RUNTIME bool redirection_done;
extern PGDLLIMPORT PG_GLOBAL_RUNTIME bool LoadedSSL;

pg_noreturn extern void PostmasterMain(int argc, char *argv[]);
extern void ClosePostmasterPorts(bool am_syslogger);
extern void InitProcessGlobals(void);

extern int	MaxLivePostmasterChildren(void);

extern bool PostmasterMarkPIDForWorkerNotify(int);

#ifdef WIN32
extern void pgwin32_register_deadchild_callback(HANDLE procHandle, DWORD procId);
#endif

/* defined in globals.c */
extern PGDLLIMPORT PG_THREAD_LOCAL PG_GLOBAL_CONNECTION struct ClientSocket *MyClientSocket;

/* prototypes for functions in launch_backend.c */
extern bool postmaster_child_launch_carrier(PMChild *pmchild,
											BackendType child_type,
											int child_slot,
											void *startup_data,
											size_t startup_data_len,
											const struct ClientSocket *client_sock);
extern pid_t postmaster_child_launch(BackendType child_type,
									 int child_slot,
									 void *startup_data,
									 size_t startup_data_len,
									 const struct ClientSocket *client_sock);
const char *PostmasterChildName(BackendType child_type);
#ifdef EXEC_BACKEND
pg_noreturn extern void SubPostmasterMain(int argc, char *argv[]);
#endif

/* defined in pmchild.c */
extern PGDLLIMPORT PG_GLOBAL_RUNTIME dlist_head ActiveChildList;

extern void InitPostmasterChildSlots(void);
extern PMChild *AssignPostmasterChildSlot(BackendType btype);
extern PMChild *AllocDeadEndChild(void);
extern bool PostmasterChildIsProcess(const PMChild *pmchild);
extern bool PostmasterChildIsThread(const PMChild *pmchild);
extern void PostmasterChildSetProcess(PMChild *pmchild, pid_t pid);
extern void PostmasterChildSetThread(PMChild *pmchild, const PgThread *thread);
extern void PostmasterChildMarkThreadExited(PMChild *pmchild, int exitstatus,
											struct Latch *postmaster_latch);
extern bool PostmasterChildHasExitedThread(PMChild *pmchild, int *exitstatus);
extern bool ReleasePostmasterChildSlot(PMChild *pmchild);
extern PMChild *FindPostmasterChildByPid(int pid);

/*
 * These values correspond to the special must-be-first options for dispatching
 * to various subprograms.  parse_dispatch_option() can be used to convert an
 * option name to one of these values.
 */
typedef enum DispatchOption
{
	DISPATCH_CHECK,
	DISPATCH_BOOT,
	DISPATCH_FORKCHILD,
	DISPATCH_DESCRIBE_CONFIG,
	DISPATCH_SINGLE,
	DISPATCH_POSTMASTER,		/* must be last */
} DispatchOption;

extern DispatchOption parse_dispatch_option(const char *name);

#endif							/* _POSTMASTER_H */

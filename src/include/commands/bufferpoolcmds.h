/*-------------------------------------------------------------------------
 *
 * bufferpoolcmds.h
 *	  prototypes for bufferpoolcmds.c.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/bufferpoolcmds.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BUFFERPOOLCMDS_H
#define BUFFERPOOLCMDS_H

#include "catalog/objectaddress.h"
#include "nodes/parsenodes.h"

extern ObjectAddress CreateBufferPool(CreateBufferPoolStmt *stmt);
extern ObjectAddress AlterBufferPool(AlterBufferPoolStmt *stmt);
extern ObjectAddress RenameBufferPool(const char *oldname, const char *newname);
extern void DropBufferPoolById(Oid bpoid);
extern Oid	get_bufferpool_oid(const char *bpname, bool missing_ok);

#endif							/* BUFFERPOOLCMDS_H */

/*-------------------------------------------------------------------------
 *
 * copydir.h
 *	  Copy a directory.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/copydir.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef COPYDIR_H
#define COPYDIR_H

typedef enum FileCopyMethod
{
	FILE_COPY_METHOD_COPY,
	FILE_COPY_METHOD_CLONE,
}			FileCopyMethod;

/* GUC parameters */
#ifndef PgCurrentFileCopyMethodRef
extern int *PgCurrentFileCopyMethodRef(void);
#endif

#define file_copy_method (*PgCurrentFileCopyMethodRef())

extern void copydir(const char *fromdir, const char *todir, bool recurse);
extern void copy_file(const char *fromfile, const char *tofile);

#endif							/* COPYDIR_H */

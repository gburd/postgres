/*-------------------------------------------------------------------------
 *
 * ts_cache.h
 *	  Tsearch related object caches.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/tsearch/ts_cache.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TS_CACHE_H
#define TS_CACHE_H

#include "fmgr.h"
#include "utils/global_lifetime.h"
#include "utils/hsearch.h"


/*
 * All TS*CacheEntry structs must share this common header
 * (see InvalidateTSCacheCallBack)
 */
typedef struct TSAnyCacheEntry
{
	Oid			objId;
	bool		isvalid;
} TSAnyCacheEntry;


typedef struct TSParserCacheEntry
{
	/* prsId is the hash lookup key and MUST BE FIRST */
	Oid			prsId;			/* OID of the parser */
	bool		isvalid;

	Oid			startOid;
	Oid			tokenOid;
	Oid			endOid;
	Oid			headlineOid;
	Oid			lextypeOid;

	/*
	 * Pre-set-up fmgr call of most needed parser's methods
	 */
	FmgrInfo	prsstart;
	FmgrInfo	prstoken;
	FmgrInfo	prsend;
	FmgrInfo	prsheadline;
} TSParserCacheEntry;

typedef struct TSDictionaryCacheEntry
{
	/* dictId is the hash lookup key and MUST BE FIRST */
	Oid			dictId;
	bool		isvalid;

	/* most frequent fmgr call */
	Oid			lexizeOid;
	FmgrInfo	lexize;

	MemoryContext dictCtx;		/* memory context to store private data */
	void	   *dictData;
} TSDictionaryCacheEntry;

typedef struct
{
	int			len;
	Oid		   *dictIds;
} ListDictionary;

typedef struct TSConfigCacheEntry
{
	/* cfgId is the hash lookup key and MUST BE FIRST */
	Oid			cfgId;
	bool		isvalid;

	Oid			prsId;

	int			lenmap;
	ListDictionary *map;
} TSConfigCacheEntry;


/*
 * GUC variable for current configuration
 */
#ifndef PgCurrentTSCurrentConfigRef
extern char **PgCurrentTSCurrentConfigRef(void);
#endif
#ifndef PgCurrentTSCurrentConfigCacheRef
extern Oid *PgCurrentTSCurrentConfigCacheRef(void);
#endif
#ifndef PgCurrentTSParserCacheHashRef
extern HTAB **PgCurrentTSParserCacheHashRef(void);
#endif
#ifndef PgCurrentTSLastUsedParserRef
extern TSParserCacheEntry **PgCurrentTSLastUsedParserRef(void);
#endif
#ifndef PgCurrentTSDictionaryCacheHashRef
extern HTAB **PgCurrentTSDictionaryCacheHashRef(void);
#endif
#ifndef PgCurrentTSLastUsedDictionaryRef
extern TSDictionaryCacheEntry **PgCurrentTSLastUsedDictionaryRef(void);
#endif
#ifndef PgCurrentTSConfigCacheHashRef
extern HTAB **PgCurrentTSConfigCacheHashRef(void);
#endif
#ifndef PgCurrentTSLastUsedConfigRef
extern TSConfigCacheEntry **PgCurrentTSLastUsedConfigRef(void);
#endif
#define TSCurrentConfig (*PgCurrentTSCurrentConfigRef())


extern TSParserCacheEntry *lookup_ts_parser_cache(Oid prsId);
extern TSDictionaryCacheEntry *lookup_ts_dictionary_cache(Oid dictId);
extern TSConfigCacheEntry *lookup_ts_config_cache(Oid cfgId);

extern Oid	getTSCurrentConfig(bool emitError);

#endif							/* TS_CACHE_H */

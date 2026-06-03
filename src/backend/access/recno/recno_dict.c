/*-------------------------------------------------------------------------
 *
 * recno_dict.c
 *	  Persistent storage for RECNO trained compression dictionaries
 *
 * Trained dictionary blobs are stored append-only in the relation's
 * RECNO_DICT_FORKNUM fork.  Block 0 is a directory metapage; subsequent
 * blocks hold serialized blobs as chains of data pages.  Dictionaries are
 * never removed, so any compressed datum remains decompressable by the
 * dictionary id embedded in its compression header even after a newer
 * default dictionary is selected for new writes.
 *
 * WAL: appends mark buffers dirty inside critical sections.  The
 * XLOG_RECNO_WRITE_DICT redo record that makes these writes crash-safe is
 * emitted by the callers wired up in the WAL layer; see recno_xlog.c.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/recno/recno_dict.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/recno_dict.h"
#include "common/relpath.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/smgr.h"
#include "utils/memutils.h"

static Buffer recno_dict_get_metapage(Relation rel, int mode);
static void recno_dict_init_metapage(Page page);

/*
 * recno_dict_init_metapage -- initialize an empty directory metapage in place.
 */
static void
recno_dict_init_metapage(Page page)
{
	RecnoDictMeta *meta;

	PageInit(page, BLCKSZ, 0);
	meta = (RecnoDictMeta *) PageGetContents(page);
	meta->magic = RECNO_DICT_METAPAGE_MAGIC;
	meta->version = RECNO_DICT_METAPAGE_VERSION;
	meta->count = 0;
	meta->next_dictid = 1;		/* id 0 is reserved for "no dictionary" */
}

/*
 * recno_dict_get_metapage -- read and lock the directory metapage,
 * creating and initializing it on first use.
 *
 * Returns a pinned buffer locked in the requested mode.  Creation requires
 * BUFFER_LOCK_EXCLUSIVE; a SHARE caller on an empty fork gets an error.
 */
static Buffer
recno_dict_get_metapage(Relation rel, int mode)
{
	Buffer		buf;
	Page		page;
	RecnoDictMeta *meta;

	if (smgrnblocks(RelationGetSmgr(rel), RECNO_DICT_FORKNUM) == 0)
	{
		if (mode != BUFFER_LOCK_EXCLUSIVE)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("RECNO dictionary fork for relation \"%s\" has no blocks",
							RelationGetRelationName(rel))));

		buf = ExtendBufferedRel(BMR_REL(rel), RECNO_DICT_FORKNUM, NULL,
								EB_LOCK_FIRST);
		Assert(BufferGetBlockNumber(buf) == 0);

		START_CRIT_SECTION();
		page = BufferGetPage(buf);
		recno_dict_init_metapage(page);
		MarkBufferDirty(buf);
		END_CRIT_SECTION();

		return buf;
	}

	buf = ReadBufferExtended(rel, RECNO_DICT_FORKNUM, 0, RBM_NORMAL, NULL);
	LockBuffer(buf, mode);
	page = BufferGetPage(buf);
	meta = (RecnoDictMeta *) PageGetContents(page);

	if (meta->magic != RECNO_DICT_METAPAGE_MAGIC)
	{
		if (mode != BUFFER_LOCK_EXCLUSIVE)
		{
			LockBuffer(buf, BUFFER_LOCK_UNLOCK);
			LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
			page = BufferGetPage(buf);
			meta = (RecnoDictMeta *) PageGetContents(page);
			if (meta->magic == RECNO_DICT_METAPAGE_MAGIC)
			{
				LockBuffer(buf, BUFFER_LOCK_UNLOCK);
				LockBuffer(buf, mode);
				return buf;
			}
		}

		START_CRIT_SECTION();
		recno_dict_init_metapage(page);
		MarkBufferDirty(buf);
		END_CRIT_SECTION();

		if (mode != BUFFER_LOCK_EXCLUSIVE)
		{
			LockBuffer(buf, BUFFER_LOCK_UNLOCK);
			LockBuffer(buf, mode);
		}
	}

	return buf;
}

/*
 * recno_dict_count -- number of dictionaries stored for the relation.
 */
uint32
recno_dict_count(Relation rel)
{
	Buffer		buf;
	RecnoDictMeta *meta;
	uint32		count;

	if (smgrexists(RelationGetSmgr(rel), RECNO_DICT_FORKNUM) == false ||
		smgrnblocks(RelationGetSmgr(rel), RECNO_DICT_FORKNUM) == 0)
		return 0;

	buf = recno_dict_get_metapage(rel, BUFFER_LOCK_SHARE);
	meta = (RecnoDictMeta *) PageGetContents(BufferGetPage(buf));
	count = meta->count;
	UnlockReleaseBuffer(buf);

	return count;
}

/*
 * recno_dict_append -- store a trained dictionary blob, returning its id.
 *
 * The blob is written across a chain of newly extended data pages, then a
 * directory entry is added to the metapage.  Returns the assigned dictid.
 */
uint32
recno_dict_append(Relation rel, uint8 codec, const char *blob, uint32 length,
				  uint32 orig_sample_size, uint64 trained_ts)
{
	Buffer		metabuf;
	RecnoDictMeta *meta;
	RecnoDictDirEntry *entry;
	uint32		dictid;
	uint32		remaining = length;
	const char *src = blob;
	BlockNumber start_blkno = InvalidBlockNumber;
	Buffer		prevbuf = InvalidBuffer;

	if (length == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("cannot store empty RECNO dictionary")));

	metabuf = recno_dict_get_metapage(rel, BUFFER_LOCK_EXCLUSIVE);
	meta = (RecnoDictMeta *) PageGetContents(BufferGetPage(metabuf));

	if (meta->count >= RECNO_DICT_MAX_DIRECTORY)
	{
		UnlockReleaseBuffer(metabuf);
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("RECNO dictionary directory full for relation \"%s\"",
						RelationGetRelationName(rel))));
	}

	dictid = meta->next_dictid;

	/*
	 * Write the blob into a chain of data pages.  Each page is freshly
	 * extended, so no other backend can see it until we commit the metapage
	 * directory entry below.
	 */
	do
	{
		Buffer		buf;
		Page		page;
		RecnoDictPageHeader *ph;
		uint32		chunk;
		char	   *payload;

		buf = ExtendBufferedRel(BMR_REL(rel), RECNO_DICT_FORKNUM, NULL,
								EB_LOCK_FIRST);
		page = BufferGetPage(buf);

		chunk = Min(remaining, (uint32) RecnoDictPagePayload);

		START_CRIT_SECTION();
		PageInit(page, BLCKSZ, 0);
		ph = (RecnoDictPageHeader *) PageGetContents(page);
		ph->next_blkno = InvalidBlockNumber;
		ph->bytes_on_page = chunk;
		payload = (char *) ph + MAXALIGN(sizeof(RecnoDictPageHeader));
		memcpy(payload, src, chunk);
		((PageHeader) page)->pd_lower =
			((char *) payload + chunk) - (char *) page;
		MarkBufferDirty(buf);
		END_CRIT_SECTION();

		if (start_blkno == InvalidBlockNumber)
			start_blkno = BufferGetBlockNumber(buf);

		if (prevbuf != InvalidBuffer)
		{
			Page		prevpage = BufferGetPage(prevbuf);
			RecnoDictPageHeader *prevph =
				(RecnoDictPageHeader *) PageGetContents(prevpage);

			START_CRIT_SECTION();
			prevph->next_blkno = BufferGetBlockNumber(buf);
			MarkBufferDirty(prevbuf);
			END_CRIT_SECTION();
			UnlockReleaseBuffer(prevbuf);
		}

		prevbuf = buf;
		src += chunk;
		remaining -= chunk;
	} while (remaining > 0);

	if (prevbuf != InvalidBuffer)
		UnlockReleaseBuffer(prevbuf);

	/* Commit the directory entry, publishing the dictionary. */
	START_CRIT_SECTION();
	entry = &meta->entries[meta->count];
	entry->dictid = dictid;
	entry->codec = codec;
	entry->_pad[0] = entry->_pad[1] = entry->_pad[2] = 0;
	entry->start_blkno = start_blkno;
	entry->length = length;
	entry->orig_sample_size = orig_sample_size;
	entry->trained_ts = trained_ts;
	meta->count++;
	meta->next_dictid++;
	MarkBufferDirty(metabuf);
	END_CRIT_SECTION();

	UnlockReleaseBuffer(metabuf);

	return dictid;
}

/*
 * recno_dict_read -- reassemble the serialized blob for a dictionary id.
 *
 * Returns a palloc'd buffer in the current memory context, sets *codec and
 * *length.  Errors if the id is unknown.
 */
char *
recno_dict_read(Relation rel, uint32 dictid, uint8 *codec, uint32 *length)
{
	Buffer		metabuf;
	RecnoDictMeta *meta;
	RecnoDictDirEntry entry;
	bool		found = false;
	char	   *result;
	uint32		copied = 0;
	BlockNumber blkno;
	uint32		i;

	metabuf = recno_dict_get_metapage(rel, BUFFER_LOCK_SHARE);
	meta = (RecnoDictMeta *) PageGetContents(BufferGetPage(metabuf));

	for (i = 0; i < meta->count; i++)
	{
		if (meta->entries[i].dictid == dictid)
		{
			entry = meta->entries[i];
			found = true;
			break;
		}
	}
	UnlockReleaseBuffer(metabuf);

	if (!found)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("RECNO dictionary id %u not found for relation \"%s\"",
						dictid, RelationGetRelationName(rel))));

	result = (char *) palloc(entry.length);
	blkno = entry.start_blkno;

	while (blkno != InvalidBlockNumber && copied < entry.length)
	{
		Buffer		buf = ReadBufferExtended(rel, RECNO_DICT_FORKNUM, blkno,
											 RBM_NORMAL, NULL);
		Page		page;
		RecnoDictPageHeader *ph;
		char	   *payload;

		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);
		ph = (RecnoDictPageHeader *) PageGetContents(page);
		payload = (char *) ph + MAXALIGN(sizeof(RecnoDictPageHeader));

		if (copied + ph->bytes_on_page > entry.length)
		{
			UnlockReleaseBuffer(buf);
			pfree(result);
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("RECNO dictionary id %u blob overruns recorded length",
							dictid)));
		}

		memcpy(result + copied, payload, ph->bytes_on_page);
		copied += ph->bytes_on_page;
		blkno = ph->next_blkno;
		UnlockReleaseBuffer(buf);
	}

	if (copied != entry.length)
	{
		pfree(result);
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("RECNO dictionary id %u blob truncated (%u of %u bytes)",
						dictid, copied, entry.length)));
	}

	*codec = entry.codec;
	*length = entry.length;
	return result;
}

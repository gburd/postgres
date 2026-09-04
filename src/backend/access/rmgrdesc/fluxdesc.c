/*-------------------------------------------------------------------------
 *
 * fluxdesc.c
 *	  Resource manager descriptor for FLUX - frontend version
 *
 * This provides minimal desc/identify functions for frontend tools like pg_waldump.
 * The full implementations are in flux_xlog.c for backend use.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/rmgrdesc/fluxdesc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"

/* Function prototypes */
extern void flux_desc(StringInfo buf, XLogReaderState *record);
extern const char *flux_identify(uint8 info);

/* FLUX WAL record types - keep in sync with flux_xlog.h */
#define XLOG_FLUX_INSERT			0x00
#define XLOG_FLUX_UPDATE			0x10
#define XLOG_FLUX_DELETE			0x20
#define XLOG_FLUX_VACUUM			0x30
#define XLOG_FLUX_OVERFLOW_WRITE	0x40
#define XLOG_FLUX_COMPRESS			0x50
#define XLOG_FLUX_INIT_PAGE		0x60
#define XLOG_FLUX_CROSS_PAGE_DEFRAG 0x70
#define XLOG_FLUX_VM_SET			0x80
#define XLOG_FLUX_VM_CLEAR			0x90
#define XLOG_FLUX_LOCK				0xA0
#define XLOG_FLUX_CAS_UPDATE		0xB0
#define XLOG_FLUX_WRITE_DICT		0xC0
#define XLOG_FLUX_MULTI_INSERT		0xD0
#define XLOG_FLUX_CAS_UPDATE_UNDO	0xE0
#define XLOG_FLUX_OPMASK			0xF0

/* WAL record flags - keep in sync with flux_xlog.h */
#define FLUX_WAL_CROSS_PAGE		0x0002

/*
 * Frontend-safe copies of WAL record structures from flux_xlog.h.
 * Duplicated here because fluxdesc.c is compiled with FRONTEND defined and
 * we need to parse these records in pg_waldump without pulling in backend
 * headers.
 */
typedef struct xl_flux_insert_fe
{
	uint16		offnum;
	uint16		flags;
	uint64		commit_ts;
	uint64		xact_ts;
} xl_flux_insert_fe;

typedef struct xl_flux_delete_fe
{
	uint16		offnum;
	uint16		flags;
	uint64		commit_ts;
	uint64		xact_ts;
} xl_flux_delete_fe;

typedef struct xl_flux_update_fe
{
	uint16		offnum;
	uint16		flags;
	uint64		old_commit_ts;
	uint64		new_commit_ts;
	uint64		xact_ts;
} xl_flux_update_fe;

/* Mirrors backend xl_flux_cas_update_undo (flux_xlog.h) */
typedef struct xl_flux_cas_update_undo_fe
{
	uint16		offnum;
	uint16		flags;
	uint16		data_offset;
	uint16		data_len;
	uint64		new_commit_ts;
	uint8		urec_type;
	uint8		is_new_page;
	uint16		urec_len;
	uint16		page_offset;
	uint16		new_pd_lower;
	uint32		max_xid;
} xl_flux_cas_update_undo_fe;

typedef struct xl_flux_vacuum_fe
{
	uint32		ntuples;
} xl_flux_vacuum_fe;

typedef struct xl_flux_compress_fe
{
	uint16		offnum;
	uint16		attr_num;
	uint8		comp_type;
	uint8		comp_level;
	uint32		orig_size;
	uint32		comp_size;
	uint64		commit_ts;
} xl_flux_compress_fe;

typedef struct xl_flux_overflow_write_fe
{
	uint16		offnum;
	uint16		flags;
	uint32		data_len;
	uint64		commit_ts;
} xl_flux_overflow_write_fe;

typedef struct xl_flux_init_page_fe
{
	uint32		flags;
	uint64		commit_ts;
} xl_flux_init_page_fe;

typedef struct xl_flux_cross_page_defrag_fe
{
	uint16		src_offnum;
	uint16		dst_offnum;
	uint32		tuple_len;
} xl_flux_cross_page_defrag_fe;

typedef struct xl_flux_vm_set_fe
{
	uint32		heapBlk;
	uint8		flags;
} xl_flux_vm_set_fe;

typedef struct xl_flux_vm_clear_fe
{
	uint32		heapBlk;
	uint8		flags;
} xl_flux_vm_clear_fe;

typedef struct xl_flux_lock_fe
{
	uint16		offnum;
	uint16		flags;
	uint32		xmax;
	uint16		infomask;
	uint16		infomask2;
	uint8		lock_mode;
} xl_flux_lock_fe;

typedef struct xl_flux_multi_insert_fe
{
	uint16		ntuples;
	uint16		flags;
	uint64		commit_ts;
} xl_flux_multi_insert_fe;

/*
 * Human-readable compression type names.
 */
static const char *
flux_comp_type_name(uint8 comp_type)
{
	switch (comp_type)
	{
		case 0:
			return "NONE";
		case 1:
			return "LZ4";
		case 2:
			return "ZSTD";
		case 3:
			return "DELTA";
		case 4:
			return "DICTIONARY";
		default:
			return "UNKNOWN";
	}
}

void
flux_desc(StringInfo buf, XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
	char	   *data = XLogRecGetData(record);
	Size		datalen = XLogRecGetDataLen(record);
	uint16		flags = 0;

	switch (info & XLOG_FLUX_OPMASK)
	{
		case XLOG_FLUX_INSERT:
			{
				if (datalen >= sizeof(xl_flux_insert_fe))
				{
					xl_flux_insert_fe xlrec;

					memcpy(&xlrec, data, sizeof(xl_flux_insert_fe));
					flags = xlrec.flags;
					appendStringInfo(buf, "off: %u, flags: 0x%04X, "
									 "commit_ts: " UINT64_FORMAT ", "
									 "xact_ts: " UINT64_FORMAT,
									 xlrec.offnum, xlrec.flags,
									 xlrec.commit_ts, xlrec.xact_ts);
				}
				else
					appendStringInfoString(buf, "insert (truncated)");
			}
			break;
		case XLOG_FLUX_DELETE:
			{
				if (datalen >= sizeof(xl_flux_delete_fe))
				{
					xl_flux_delete_fe xlrec;

					memcpy(&xlrec, data, sizeof(xl_flux_delete_fe));
					flags = xlrec.flags;
					appendStringInfo(buf, "off: %u, flags: 0x%04X, "
									 "commit_ts: " UINT64_FORMAT ", "
									 "xact_ts: " UINT64_FORMAT,
									 xlrec.offnum, xlrec.flags,
									 xlrec.commit_ts, xlrec.xact_ts);
				}
				else
					appendStringInfoString(buf, "delete (truncated)");
			}
			break;
		case XLOG_FLUX_UPDATE:
			{
				if (datalen >= sizeof(xl_flux_update_fe))
				{
					xl_flux_update_fe xlrec;

					memcpy(&xlrec, data, sizeof(xl_flux_update_fe));
					flags = xlrec.flags;
					appendStringInfo(buf, "off: %u, flags: 0x%04X, "
									 "old_commit_ts: " UINT64_FORMAT ", "
									 "new_commit_ts: " UINT64_FORMAT ", "
									 "xact_ts: " UINT64_FORMAT,
									 xlrec.offnum, xlrec.flags,
									 xlrec.old_commit_ts,
									 xlrec.new_commit_ts,
									 xlrec.xact_ts);
					if (flags & FLUX_WAL_CROSS_PAGE)
						appendStringInfoString(buf, ", cross_page: true");
				}
				else
					appendStringInfoString(buf, "update (truncated)");
			}
			break;
		case XLOG_FLUX_VACUUM:
			{
				if (datalen >= sizeof(xl_flux_vacuum_fe))
				{
					xl_flux_vacuum_fe xlrec;

					memcpy(&xlrec, data, sizeof(xl_flux_vacuum_fe));
					appendStringInfo(buf, "ntuples: %u", xlrec.ntuples);
				}
				else
					appendStringInfoString(buf, "vacuum (truncated)");
			}
			break;
		case XLOG_FLUX_COMPRESS:
			{
				if (datalen >= sizeof(xl_flux_compress_fe))
				{
					xl_flux_compress_fe xlrec;

					memcpy(&xlrec, data, sizeof(xl_flux_compress_fe));
					appendStringInfo(buf, "off: %u, attr: %u, "
									 "comp_type: %s, comp_level: %u, "
									 "orig_size: %u, comp_size: %u, "
									 "commit_ts: " UINT64_FORMAT,
									 xlrec.offnum, xlrec.attr_num,
									 flux_comp_type_name(xlrec.comp_type),
									 xlrec.comp_level,
									 xlrec.orig_size, xlrec.comp_size,
									 xlrec.commit_ts);
				}
				else
					appendStringInfoString(buf, "compress (truncated)");
			}
			break;
		case XLOG_FLUX_OVERFLOW_WRITE:
			{
				if (datalen >= sizeof(xl_flux_overflow_write_fe))
				{
					xl_flux_overflow_write_fe xlrec;

					memcpy(&xlrec, data, sizeof(xl_flux_overflow_write_fe));
					appendStringInfo(buf, "off: %u, flags: 0x%04X, "
									 "data_len: %u, "
									 "commit_ts: " UINT64_FORMAT,
									 xlrec.offnum, xlrec.flags,
									 xlrec.data_len, xlrec.commit_ts);
				}
				else
					appendStringInfoString(buf, "overflow_write (truncated)");
			}
			break;
		case XLOG_FLUX_INIT_PAGE:
			{
				if (datalen >= sizeof(xl_flux_init_page_fe))
				{
					xl_flux_init_page_fe xlrec;

					memcpy(&xlrec, data, sizeof(xl_flux_init_page_fe));
					appendStringInfo(buf, "flags: 0x%08X, "
									 "commit_ts: " UINT64_FORMAT,
									 xlrec.flags, xlrec.commit_ts);
				}
				else
					appendStringInfoString(buf, "init_page (truncated)");
			}
			break;
		case XLOG_FLUX_CROSS_PAGE_DEFRAG:
			{
				if (datalen >= sizeof(xl_flux_cross_page_defrag_fe))
				{
					xl_flux_cross_page_defrag_fe xlrec;

					memcpy(&xlrec, data, sizeof(xl_flux_cross_page_defrag_fe));
					appendStringInfo(buf, "src_off: %u, dst_off: %u, "
									 "tuple_len: %u",
									 xlrec.src_offnum, xlrec.dst_offnum,
									 xlrec.tuple_len);
				}
				else
					appendStringInfoString(buf, "cross_page_defrag (truncated)");
			}
			break;
		case XLOG_FLUX_VM_SET:
			{
				if (datalen >= sizeof(xl_flux_vm_set_fe))
				{
					xl_flux_vm_set_fe xlrec;

					memcpy(&xlrec, data, sizeof(xl_flux_vm_set_fe));
					appendStringInfo(buf, "heapBlk: %u, flags: 0x%02X",
									 xlrec.heapBlk, xlrec.flags);
				}
				else
					appendStringInfoString(buf, "vm_set (truncated)");
			}
			break;
		case XLOG_FLUX_VM_CLEAR:
			{
				if (datalen >= sizeof(xl_flux_vm_clear_fe))
				{
					xl_flux_vm_clear_fe xlrec;

					memcpy(&xlrec, data, sizeof(xl_flux_vm_clear_fe));
					appendStringInfo(buf, "heapBlk: %u, flags: 0x%02X",
									 xlrec.heapBlk, xlrec.flags);
				}
				else
					appendStringInfoString(buf, "vm_clear (truncated)");
			}
			break;
		case XLOG_FLUX_LOCK:
			{
				if (datalen >= sizeof(xl_flux_lock_fe))
				{
					xl_flux_lock_fe xlrec;

					memcpy(&xlrec, data, sizeof(xl_flux_lock_fe));
					appendStringInfo(buf, "off: %u, xmax: %u, "
									 "infomask: 0x%04X, infomask2: 0x%04X, "
									 "lock_mode: %u",
									 xlrec.offnum, xlrec.xmax,
									 xlrec.infomask, xlrec.infomask2,
									 xlrec.lock_mode);
				}
				else
					appendStringInfoString(buf, "lock (truncated)");
			}
			break;
		case XLOG_FLUX_CAS_UPDATE:
			{
				if (datalen >= 14)	/* minimum:
									 * offnum(2)+flags(2)+offset(2)+len(2)+ts(8)
									 * - 2 padding */
				{
					uint16		offnum;
					uint16		d_offset;
					uint16		d_len;

					memcpy(&offnum, data, sizeof(uint16));
					memcpy(&d_offset, data + 4, sizeof(uint16));
					memcpy(&d_len, data + 6, sizeof(uint16));
					appendStringInfo(buf, "off: %u, data_offset: %u, data_len: %u",
									 offnum, d_offset, d_len);
				}
				else
					appendStringInfoString(buf, "cas_update (truncated)");
			}
			break;
		case XLOG_FLUX_CAS_UPDATE_UNDO:
			{
				if (datalen >= (int) sizeof(xl_flux_cas_update_undo_fe))
				{
					xl_flux_cas_update_undo_fe xlrec;

					memcpy(&xlrec, data, sizeof(xl_flux_cas_update_undo_fe));
					appendStringInfo(buf,
									 "off: %u, data_offset: %u, data_len: %u, "
									 "urec_type: %u, urec_len: %u, undo_off: %u, new_page: %u",
									 xlrec.offnum, xlrec.data_offset, xlrec.data_len,
									 xlrec.urec_type, xlrec.urec_len, xlrec.page_offset,
									 xlrec.is_new_page);
				}
				else
					appendStringInfoString(buf, "cas_update_undo (truncated)");
			}
			break;
		case XLOG_FLUX_WRITE_DICT:
			{
				if (datalen >= sizeof(uint32))
				{
					uint32		blkno;

					memcpy(&blkno, data, sizeof(uint32));
					appendStringInfo(buf, "blkno: %u (full-page image)", blkno);
				}
				else
					appendStringInfoString(buf, "write_dict (truncated)");
			}
			break;
		case XLOG_FLUX_MULTI_INSERT:
			{
				if (datalen >= sizeof(xl_flux_multi_insert_fe))
				{
					xl_flux_multi_insert_fe xlrec;

					memcpy(&xlrec, data, sizeof(xl_flux_multi_insert_fe));
					appendStringInfo(buf, "ntuples: %u, flags: 0x%04X, "
									 "commit_ts: " UINT64_FORMAT,
									 xlrec.ntuples, xlrec.flags,
									 xlrec.commit_ts);
				}
				else
					appendStringInfoString(buf, "multi_insert (truncated)");
			}
			break;
		default:
			appendStringInfoString(buf, "UNKNOWN");
			break;
	}
}

const char *
flux_identify(uint8 info)
{
	const char *id = NULL;

	switch (info & XLOG_FLUX_OPMASK)
	{
		case XLOG_FLUX_INSERT:
			id = "INSERT";
			break;
		case XLOG_FLUX_DELETE:
			id = "DELETE";
			break;
		case XLOG_FLUX_UPDATE:
			id = "UPDATE";
			break;
		case XLOG_FLUX_VACUUM:
			id = "VACUUM";
			break;
		case XLOG_FLUX_COMPRESS:
			id = "COMPRESS";
			break;
		case XLOG_FLUX_OVERFLOW_WRITE:
			id = "OVERFLOW_WRITE";
			break;
		case XLOG_FLUX_INIT_PAGE:
			id = "INIT_PAGE";
			break;
		case XLOG_FLUX_CROSS_PAGE_DEFRAG:
			id = "CROSS_PAGE_DEFRAG";
			break;
		case XLOG_FLUX_VM_SET:
			id = "VM_SET";
			break;
		case XLOG_FLUX_VM_CLEAR:
			id = "VM_CLEAR";
			break;
		case XLOG_FLUX_LOCK:
			id = "LOCK";
			break;
		case XLOG_FLUX_CAS_UPDATE:
			id = "CAS_UPDATE";
			break;
		case XLOG_FLUX_CAS_UPDATE_UNDO:
			id = "CAS_UPDATE_UNDO";
			break;
		case XLOG_FLUX_WRITE_DICT:
			id = "WRITE_DICT";
			break;
		case XLOG_FLUX_MULTI_INSERT:
			id = "MULTI_INSERT";
			break;
		default:
			id = NULL;
			break;
	}

	return id;
}

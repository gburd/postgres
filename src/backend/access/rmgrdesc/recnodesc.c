/*-------------------------------------------------------------------------
 *
 * recnodesc.c
 *	  Resource manager descriptor for RECNO - frontend version
 *
 * This provides minimal desc/identify functions for frontend tools like pg_waldump.
 * The full implementations are in recno_xlog.c for backend use.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/rmgrdesc/recnodesc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"

/* Function prototypes */
extern void recno_desc(StringInfo buf, XLogReaderState *record);
extern const char *recno_identify(uint8 info);

/* RECNO WAL record types - keep in sync with recno_xlog.h */
#define XLOG_RECNO_INSERT			0x00
#define XLOG_RECNO_UPDATE			0x10
#define XLOG_RECNO_DELETE			0x20
#define XLOG_RECNO_VACUUM			0x30
#define XLOG_RECNO_OVERFLOW_WRITE	0x40
#define XLOG_RECNO_COMPRESS			0x50
#define XLOG_RECNO_INIT_PAGE		0x60
#define XLOG_RECNO_CROSS_PAGE_DEFRAG 0x70
#define XLOG_RECNO_VM_SET			0x80
#define XLOG_RECNO_VM_CLEAR			0x90
#define XLOG_RECNO_LOCK				0xA0
#define XLOG_RECNO_CAS_UPDATE		0xB0
#define XLOG_RECNO_OPMASK			0xF0

/* WAL record flags - keep in sync with recno_xlog.h */
#define RECNO_WAL_HAS_HLC			0x0001
#define RECNO_WAL_CROSS_PAGE		0x0002

/*
 * Frontend-safe copies of WAL record structures from recno_xlog.h.
 * Duplicated here because recnodesc.c is compiled with FRONTEND defined and
 * we need to parse these records in pg_waldump without pulling in backend
 * headers.
 */
typedef struct xl_recno_hlc_info_fe
{
	uint64		commit_hlc;
	uint64		commit_dvv;
	uint64		uncertainty_lower;
	uint64		uncertainty_upper;
}			xl_recno_hlc_info_fe;

#define SizeOfXlRecnoHlcInfoFE	sizeof(xl_recno_hlc_info_fe)

typedef struct xl_recno_insert_fe
{
	uint16		offnum;
	uint16		flags;
	uint64		commit_ts;
	uint64		xact_ts;
}			xl_recno_insert_fe;

typedef struct xl_recno_delete_fe
{
	uint16		offnum;
	uint16		flags;
	uint64		commit_ts;
	uint64		xact_ts;
}			xl_recno_delete_fe;

typedef struct xl_recno_update_fe
{
	uint16		offnum;
	uint16		flags;
	uint64		old_commit_ts;
	uint64		new_commit_ts;
	uint64		xact_ts;
}			xl_recno_update_fe;

typedef struct xl_recno_vacuum_fe
{
	uint32		ntuples;
}			xl_recno_vacuum_fe;

typedef struct xl_recno_compress_fe
{
	uint16		offnum;
	uint16		attr_num;
	uint8		comp_type;
	uint8		comp_level;
	uint32		orig_size;
	uint32		comp_size;
	uint64		commit_ts;
}			xl_recno_compress_fe;

typedef struct xl_recno_overflow_write_fe
{
	uint16		offnum;
	uint16		flags;
	uint32		data_len;
	uint64		commit_ts;
}			xl_recno_overflow_write_fe;

typedef struct xl_recno_init_page_fe
{
	uint32		flags;
	uint64		commit_ts;
}			xl_recno_init_page_fe;

typedef struct xl_recno_cross_page_defrag_fe
{
	uint16		src_offnum;
	uint16		dst_offnum;
	uint32		tuple_len;
}			xl_recno_cross_page_defrag_fe;

typedef struct xl_recno_vm_set_fe
{
	uint32		heapBlk;
	uint8		flags;
}			xl_recno_vm_set_fe;

typedef struct xl_recno_vm_clear_fe
{
	uint32		heapBlk;
	uint8		flags;
}			xl_recno_vm_clear_fe;

typedef struct xl_recno_lock_fe
{
	uint16		offnum;
	uint16		flags;
	uint32		xmax;
	uint16		infomask;
	uint16		infomask2;
	uint8		lock_mode;
}			xl_recno_lock_fe;

/*
 * Human-readable compression type names.
 */
static const char *
recno_comp_type_name(uint8 comp_type)
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

/*
 * recno_desc_hlc -- append HLC uncertainty details to a WAL record desc.
 *
 * If the record's flags indicate RECNO_WAL_HAS_HLC, this extracts the
 * trailing xl_recno_hlc_info and prints it for pg_waldump.
 */
static void
recno_desc_hlc(StringInfo buf, XLogReaderState *record, uint16 flags)
{
	Size		total_len;
	char	   *data;
	const		xl_recno_hlc_info_fe *hlc;

	if (!(flags & RECNO_WAL_HAS_HLC))
		return;

	data = XLogRecGetData(record);
	total_len = XLogRecGetDataLen(record);

	if (total_len < SizeOfXlRecnoHlcInfoFE)
		return;

	hlc = (const xl_recno_hlc_info_fe *)
		(data + total_len - SizeOfXlRecnoHlcInfoFE);

	appendStringInfo(buf, ", hlc " UINT64_FORMAT
					 " dvv " UINT64_FORMAT
					 " uncertainty [" UINT64_FORMAT ", " UINT64_FORMAT "]",
					 hlc->commit_hlc,
					 hlc->commit_dvv,
					 hlc->uncertainty_lower,
					 hlc->uncertainty_upper);
}

void
recno_desc(StringInfo buf, XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
	char	   *data = XLogRecGetData(record);
	Size		datalen = XLogRecGetDataLen(record);
	uint16		flags = 0;

	switch (info & XLOG_RECNO_OPMASK)
	{
		case XLOG_RECNO_INSERT:
			{
				if (datalen >= sizeof(xl_recno_insert_fe))
				{
					xl_recno_insert_fe xlrec;

					memcpy(&xlrec, data, sizeof(xl_recno_insert_fe));
					flags = xlrec.flags;
					appendStringInfo(buf, "off: %u, flags: 0x%04X, "
									 "commit_ts: " UINT64_FORMAT ", "
									 "xact_ts: " UINT64_FORMAT,
									 xlrec.offnum, xlrec.flags,
									 xlrec.commit_ts, xlrec.xact_ts);
				}
				else
					appendStringInfoString(buf, "insert (truncated)");
				recno_desc_hlc(buf, record, flags);
			}
			break;
		case XLOG_RECNO_DELETE:
			{
				if (datalen >= sizeof(xl_recno_delete_fe))
				{
					xl_recno_delete_fe xlrec;

					memcpy(&xlrec, data, sizeof(xl_recno_delete_fe));
					flags = xlrec.flags;
					appendStringInfo(buf, "off: %u, flags: 0x%04X, "
									 "commit_ts: " UINT64_FORMAT ", "
									 "xact_ts: " UINT64_FORMAT,
									 xlrec.offnum, xlrec.flags,
									 xlrec.commit_ts, xlrec.xact_ts);
				}
				else
					appendStringInfoString(buf, "delete (truncated)");
				recno_desc_hlc(buf, record, flags);
			}
			break;
		case XLOG_RECNO_UPDATE:
			{
				if (datalen >= sizeof(xl_recno_update_fe))
				{
					xl_recno_update_fe xlrec;

					memcpy(&xlrec, data, sizeof(xl_recno_update_fe));
					flags = xlrec.flags;
					appendStringInfo(buf, "off: %u, flags: 0x%04X, "
									 "old_commit_ts: " UINT64_FORMAT ", "
									 "new_commit_ts: " UINT64_FORMAT ", "
									 "xact_ts: " UINT64_FORMAT,
									 xlrec.offnum, xlrec.flags,
									 xlrec.old_commit_ts,
									 xlrec.new_commit_ts,
									 xlrec.xact_ts);
					if (flags & RECNO_WAL_CROSS_PAGE)
						appendStringInfoString(buf, ", cross_page: true");
				}
				else
					appendStringInfoString(buf, "update (truncated)");
				recno_desc_hlc(buf, record, flags);
			}
			break;
		case XLOG_RECNO_VACUUM:
			{
				if (datalen >= sizeof(xl_recno_vacuum_fe))
				{
					xl_recno_vacuum_fe xlrec;

					memcpy(&xlrec, data, sizeof(xl_recno_vacuum_fe));
					appendStringInfo(buf, "ntuples: %u", xlrec.ntuples);
				}
				else
					appendStringInfoString(buf, "vacuum (truncated)");
			}
			break;
		case XLOG_RECNO_COMPRESS:
			{
				if (datalen >= sizeof(xl_recno_compress_fe))
				{
					xl_recno_compress_fe xlrec;

					memcpy(&xlrec, data, sizeof(xl_recno_compress_fe));
					appendStringInfo(buf, "off: %u, attr: %u, "
									 "comp_type: %s, comp_level: %u, "
									 "orig_size: %u, comp_size: %u, "
									 "commit_ts: " UINT64_FORMAT,
									 xlrec.offnum, xlrec.attr_num,
									 recno_comp_type_name(xlrec.comp_type),
									 xlrec.comp_level,
									 xlrec.orig_size, xlrec.comp_size,
									 xlrec.commit_ts);
				}
				else
					appendStringInfoString(buf, "compress (truncated)");
			}
			break;
		case XLOG_RECNO_OVERFLOW_WRITE:
			{
				if (datalen >= sizeof(xl_recno_overflow_write_fe))
				{
					xl_recno_overflow_write_fe xlrec;

					memcpy(&xlrec, data, sizeof(xl_recno_overflow_write_fe));
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
		case XLOG_RECNO_INIT_PAGE:
			{
				if (datalen >= sizeof(xl_recno_init_page_fe))
				{
					xl_recno_init_page_fe xlrec;

					memcpy(&xlrec, data, sizeof(xl_recno_init_page_fe));
					appendStringInfo(buf, "flags: 0x%08X, "
									 "commit_ts: " UINT64_FORMAT,
									 xlrec.flags, xlrec.commit_ts);
				}
				else
					appendStringInfoString(buf, "init_page (truncated)");
			}
			break;
		case XLOG_RECNO_CROSS_PAGE_DEFRAG:
			{
				if (datalen >= sizeof(xl_recno_cross_page_defrag_fe))
				{
					xl_recno_cross_page_defrag_fe xlrec;

					memcpy(&xlrec, data, sizeof(xl_recno_cross_page_defrag_fe));
					appendStringInfo(buf, "src_off: %u, dst_off: %u, "
									 "tuple_len: %u",
									 xlrec.src_offnum, xlrec.dst_offnum,
									 xlrec.tuple_len);
				}
				else
					appendStringInfoString(buf, "cross_page_defrag (truncated)");
			}
			break;
		case XLOG_RECNO_VM_SET:
			{
				if (datalen >= sizeof(xl_recno_vm_set_fe))
				{
					xl_recno_vm_set_fe xlrec;

					memcpy(&xlrec, data, sizeof(xl_recno_vm_set_fe));
					appendStringInfo(buf, "heapBlk: %u, flags: 0x%02X",
									 xlrec.heapBlk, xlrec.flags);
				}
				else
					appendStringInfoString(buf, "vm_set (truncated)");
			}
			break;
		case XLOG_RECNO_VM_CLEAR:
			{
				if (datalen >= sizeof(xl_recno_vm_clear_fe))
				{
					xl_recno_vm_clear_fe xlrec;

					memcpy(&xlrec, data, sizeof(xl_recno_vm_clear_fe));
					appendStringInfo(buf, "heapBlk: %u, flags: 0x%02X",
									 xlrec.heapBlk, xlrec.flags);
				}
				else
					appendStringInfoString(buf, "vm_clear (truncated)");
			}
			break;
		case XLOG_RECNO_LOCK:
			{
				if (datalen >= sizeof(xl_recno_lock_fe))
				{
					xl_recno_lock_fe xlrec;

					memcpy(&xlrec, data, sizeof(xl_recno_lock_fe));
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
		case XLOG_RECNO_CAS_UPDATE:
			{
				if (datalen >= 14)	/* minimum: offnum(2)+flags(2)+offset(2)+len(2)+ts(8) - 2 padding */
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
		default:
			appendStringInfoString(buf, "UNKNOWN");
			break;
	}
}

const char *
recno_identify(uint8 info)
{
	const char *id = NULL;

	switch (info & XLOG_RECNO_OPMASK)
	{
		case XLOG_RECNO_INSERT:
			id = "INSERT";
			break;
		case XLOG_RECNO_DELETE:
			id = "DELETE";
			break;
		case XLOG_RECNO_UPDATE:
			id = "UPDATE";
			break;
		case XLOG_RECNO_VACUUM:
			id = "VACUUM";
			break;
		case XLOG_RECNO_COMPRESS:
			id = "COMPRESS";
			break;
		case XLOG_RECNO_OVERFLOW_WRITE:
			id = "OVERFLOW_WRITE";
			break;
		case XLOG_RECNO_INIT_PAGE:
			id = "INIT_PAGE";
			break;
		case XLOG_RECNO_CROSS_PAGE_DEFRAG:
			id = "CROSS_PAGE_DEFRAG";
			break;
		case XLOG_RECNO_VM_SET:
			id = "VM_SET";
			break;
		case XLOG_RECNO_VM_CLEAR:
			id = "VM_CLEAR";
			break;
		case XLOG_RECNO_LOCK:
			id = "LOCK";
			break;
		case XLOG_RECNO_CAS_UPDATE:
			id = "CAS_UPDATE";
			break;
		default:
			id = NULL;
			break;
	}

	return id;
}

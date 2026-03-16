/*-------------------------------------------------------------------------
 *
 * pg_undorecover.c
 *	  Point-in-time recovery tool for UNDO log data
 *
 * This tool reads UNDO log files from $PGDATA/base/undo/ and outputs
 * recovered tuple data. It can be used to recover data from:
 *   - Pruned tuples (UNDO_PRUNE records)
 *   - Deleted tuples (UNDO_DELETE records)
 *   - Updated tuples (UNDO_UPDATE records, old versions)
 *
 * Output formats: text (default), csv, json
 *
 * Usage:
 *   pg_undorecover [OPTIONS] PGDATA
 *
 *   -r RELOID    Filter by relation OID
 *   -x XID       Filter by transaction ID
 *   -t TYPE      Filter by record type (insert/delete/update/prune/inplace)
 *   -f FORMAT    Output format: text (default), csv, json
 *   -v           Verbose output
 *   -s           Show statistics only
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/bin/pg_undorecover/pg_undorecover.c
 *
 *-------------------------------------------------------------------------
 */

#define FRONTEND 1
#include "postgres.h"

#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>

#include <stdlib.h>

#include "common/file_perm.h"
#include "common/logging.h"
#include "getopt_long.h"

/*
 * Frontend-safe type definitions for types normally provided by backend
 * headers. These must match the backend definitions exactly.
 */
#ifndef InvalidTransactionId
#define InvalidTransactionId ((TransactionId) 0)
#endif

/*
 * UNDO record type codes - must match undorecord.h
 */
#define UNDO_INSERT		0x0001
#define UNDO_DELETE		0x0002
#define UNDO_UPDATE		0x0003
#define UNDO_PRUNE		0x0004
#define UNDO_INPLACE	0x0005

/*
 * UNDO record info flags - must match undorecord.h
 */
#define UNDO_INFO_HAS_TUPLE		0x01
#define UNDO_INFO_HAS_DELTA		0x02
#define UNDO_INFO_HAS_TOAST		0x04
#define UNDO_INFO_XID_VALID		0x08

/*
 * UndoRecordHeader - must match undorecord.h layout exactly
 */
typedef struct UndoRecordHeader
{
	uint16		urec_type;
	uint16		urec_info;
	uint32		urec_len;

	TransactionId urec_xid;
	uint64		urec_prev;		/* UndoRecPtr */

	Oid			urec_reloid;
	uint32		urec_blkno;		/* BlockNumber */
	uint16		urec_offset;	/* OffsetNumber */

	uint16		urec_payload_len;
} UndoRecordHeader;

#define SizeOfUndoRecordHeader	(offsetof(UndoRecordHeader, urec_payload_len) + sizeof(uint16))

/* UndoRecPtr manipulation (must match undolog.h) */
typedef uint64 UndoRecPtr;
#define InvalidUndoRecPtr ((UndoRecPtr) 0)
#define UndoRecPtrIsValid(ptr) ((ptr) != InvalidUndoRecPtr)
#define UndoRecPtrGetLogNo(ptr) ((uint32) (((uint64) (ptr)) >> 40))
#define UndoRecPtrGetOffset(ptr) (((uint64) (ptr)) & 0xFFFFFFFFFFULL)

/* Output format */
typedef enum
{
	FORMAT_TEXT,
	FORMAT_CSV,
	FORMAT_JSON
} OutputFormat;

/* Command-line options */
static char *pgdata = NULL;
static Oid	filter_reloid = InvalidOid;
static TransactionId filter_xid = InvalidTransactionId;
static int	filter_type = 0;	/* 0 = all types */
static OutputFormat output_format = FORMAT_TEXT;
static bool verbose = false;
static bool stats_only = false;

/* Statistics */
static int	total_records = 0;
static int	insert_records = 0;
static int	delete_records = 0;
static int	update_records = 0;
static int	prune_records = 0;
static int	inplace_records = 0;
static int	matched_records = 0;
static uint64 total_bytes = 0;

static const char *
type_name(uint16 type)
{
	switch (type)
	{
		case UNDO_INSERT:
			return "INSERT";
		case UNDO_DELETE:
			return "DELETE";
		case UNDO_UPDATE:
			return "UPDATE";
		case UNDO_PRUNE:
			return "PRUNE";
		case UNDO_INPLACE:
			return "INPLACE";
		default:
			return "UNKNOWN";
	}
}

static int
parse_type(const char *name)
{
	if (pg_strcasecmp(name, "insert") == 0)
		return UNDO_INSERT;
	if (pg_strcasecmp(name, "delete") == 0)
		return UNDO_DELETE;
	if (pg_strcasecmp(name, "update") == 0)
		return UNDO_UPDATE;
	if (pg_strcasecmp(name, "prune") == 0)
		return UNDO_PRUNE;
	if (pg_strcasecmp(name, "inplace") == 0)
		return UNDO_INPLACE;
	return -1;
}

/*
 * Check if a record matches the filter criteria
 */
static bool
record_matches_filter(UndoRecordHeader *header)
{
	if (filter_reloid != InvalidOid && header->urec_reloid != filter_reloid)
		return false;
	if (filter_xid != InvalidTransactionId && header->urec_xid != filter_xid)
		return false;
	if (filter_type != 0 && header->urec_type != filter_type)
		return false;
	return true;
}

/*
 * Output a hex dump of payload data
 */
static void
output_hex(const unsigned char *data, int len, FILE *out)
{
	int			i;

	for (i = 0; i < len; i++)
	{
		fprintf(out, "%02x", data[i]);
		if (i < len - 1 && (i + 1) % 32 == 0)
			fprintf(out, "\n    ");
		else if (i < len - 1)
			fprintf(out, " ");
	}
}

/*
 * Output a record in text format
 */
static void
output_text(UndoRecordHeader *header, const char *payload, uint32 log_number,
			uint64 offset, FILE *out)
{
	fprintf(out, "--- UNDO Record ---\n");
	fprintf(out, "  Log: %u  Offset: %llu\n", log_number,
			(unsigned long long) offset);
	fprintf(out, "  Type: %s (%u)\n", type_name(header->urec_type),
			header->urec_type);
	fprintf(out, "  XID: %u\n", header->urec_xid);
	fprintf(out, "  Relation: %u\n", header->urec_reloid);
	fprintf(out, "  Block: %u  Offset: %u\n", header->urec_blkno,
			header->urec_offset);
	fprintf(out, "  Record length: %u\n", header->urec_len);
	fprintf(out, "  Payload length: %u\n", header->urec_payload_len);
	fprintf(out, "  Prev: %llu\n", (unsigned long long) header->urec_prev);
	fprintf(out, "  Info flags: 0x%04x", header->urec_info);
	if (header->urec_info & UNDO_INFO_HAS_TUPLE)
		fprintf(out, " HAS_TUPLE");
	if (header->urec_info & UNDO_INFO_HAS_DELTA)
		fprintf(out, " HAS_DELTA");
	if (header->urec_info & UNDO_INFO_HAS_TOAST)
		fprintf(out, " HAS_TOAST");
	if (header->urec_info & UNDO_INFO_XID_VALID)
		fprintf(out, " XID_VALID");
	fprintf(out, "\n");

	if (payload && header->urec_payload_len > 0)
	{
		fprintf(out, "  Payload (hex):\n    ");
		output_hex((const unsigned char *) payload, header->urec_payload_len, out);
		fprintf(out, "\n");
	}
	fprintf(out, "\n");
}

/*
 * Output a record in CSV format
 */
static void
output_csv(UndoRecordHeader *header, const char *payload, uint32 log_number,
		   uint64 offset, FILE *out)
{
	fprintf(out, "%u,%llu,%s,%u,%u,%u,%u,%u,%llu,%u",
			log_number, (unsigned long long) offset,
			type_name(header->urec_type),
			header->urec_xid,
			header->urec_reloid,
			header->urec_blkno,
			header->urec_offset,
			header->urec_len,
			(unsigned long long) header->urec_prev,
			header->urec_payload_len);

	if (payload && header->urec_payload_len > 0)
	{
		fprintf(out, ",");
		output_hex((const unsigned char *) payload, header->urec_payload_len, out);
	}
	else
		fprintf(out, ",");

	fprintf(out, "\n");
}

/*
 * Output a record in JSON format
 */
static void
output_json(UndoRecordHeader *header, const char *payload, uint32 log_number,
			uint64 offset, FILE *out, bool first)
{
	if (!first)
		fprintf(out, ",\n");

	fprintf(out, "  {\n");
	fprintf(out, "    \"log\": %u,\n", log_number);
	fprintf(out, "    \"offset\": %llu,\n", (unsigned long long) offset);
	fprintf(out, "    \"type\": \"%s\",\n", type_name(header->urec_type));
	fprintf(out, "    \"xid\": %u,\n", header->urec_xid);
	fprintf(out, "    \"reloid\": %u,\n", header->urec_reloid);
	fprintf(out, "    \"block\": %u,\n", header->urec_blkno);
	fprintf(out, "    \"offset_in_page\": %u,\n", header->urec_offset);
	fprintf(out, "    \"record_len\": %u,\n", header->urec_len);
	fprintf(out, "    \"prev\": %llu,\n", (unsigned long long) header->urec_prev);
	fprintf(out, "    \"payload_len\": %u", header->urec_payload_len);

	if (payload && header->urec_payload_len > 0)
	{
		fprintf(out, ",\n    \"payload_hex\": \"");
		output_hex((const unsigned char *) payload, header->urec_payload_len, out);
		fprintf(out, "\"");
	}

	fprintf(out, "\n  }");
}

/*
 * Process a single UNDO log file
 */
static void
process_undo_log(const char *filepath, uint32 log_number, FILE *out,
				 bool *first_json)
{
	FILE	   *fp;
	char	   *buffer;
	size_t		file_size;
	uint64		pos;
	struct stat statbuf;

	fp = fopen(filepath, "rb");
	if (!fp)
	{
		pg_log_warning("could not open UNDO log file \"%s\": %m", filepath);
		return;
	}

	if (fstat(fileno(fp), &statbuf) != 0)
	{
		pg_log_warning("could not stat UNDO log file \"%s\": %m", filepath);
		fclose(fp);
		return;
	}

	file_size = statbuf.st_size;
	if (file_size == 0)
	{
		if (verbose)
			pg_log_info("skipping empty UNDO log file: %s", filepath);
		fclose(fp);
		return;
	}

	/* Read entire file into memory */
	buffer = malloc(file_size);
	if (fread(buffer, 1, file_size, fp) != file_size)
	{
		pg_log_warning("could not read UNDO log file \"%s\": %m", filepath);
		free(buffer);
		fclose(fp);
		return;
	}
	fclose(fp);

	if (verbose)
		pg_log_info("processing UNDO log %u (%zu bytes)", log_number, file_size);

	total_bytes += file_size;

	/* Walk through records in the file */
	pos = 0;
	while (pos + SizeOfUndoRecordHeader <= file_size)
	{
		UndoRecordHeader header;
		const char *payload = NULL;

		memcpy(&header, buffer + pos, SizeOfUndoRecordHeader);

		/* Sanity check: valid record size */
		if (header.urec_len < SizeOfUndoRecordHeader ||
			header.urec_len > file_size - pos)
		{
			/* Reached end of valid data or corrupt record */
			if (verbose && header.urec_len != 0)
				pg_log_info("  stopping at offset %llu: invalid record length %u",
							(unsigned long long) pos, header.urec_len);
			break;
		}

		/* Check if this looks like a valid record */
		if (header.urec_type == 0 && header.urec_len == 0)
		{
			/* Zero-filled area, end of data */
			break;
		}

		if (header.urec_type < UNDO_INSERT || header.urec_type > UNDO_INPLACE)
		{
			if (verbose)
				pg_log_info("  stopping at offset %llu: unknown type %u",
							(unsigned long long) pos, header.urec_type);
			break;
		}

		/* Count by type */
		total_records++;
		switch (header.urec_type)
		{
			case UNDO_INSERT:
				insert_records++;
				break;
			case UNDO_DELETE:
				delete_records++;
				break;
			case UNDO_UPDATE:
				update_records++;
				break;
			case UNDO_PRUNE:
				prune_records++;
				break;
			case UNDO_INPLACE:
				inplace_records++;
				break;
		}

		/* Get payload if present */
		if (header.urec_payload_len > 0 &&
			pos + SizeOfUndoRecordHeader + header.urec_payload_len <= file_size)
		{
			payload = buffer + pos + SizeOfUndoRecordHeader;
		}

		/* Apply filters and output */
		if (record_matches_filter(&header))
		{
			matched_records++;

			if (!stats_only)
			{
				switch (output_format)
				{
					case FORMAT_TEXT:
						output_text(&header, payload, log_number, pos, out);
						break;
					case FORMAT_CSV:
						output_csv(&header, payload, log_number, pos, out);
						break;
					case FORMAT_JSON:
						output_json(&header, payload, log_number, pos, out,
									*first_json);
						*first_json = false;
						break;
				}
			}
		}

		/* Advance to next record */
		pos += header.urec_len;
	}

	free(buffer);
}

/*
 * Scan the UNDO directory and process all log files
 */
static void
scan_undo_directory(FILE *out)
{
	char		undo_dir[MAXPGPATH];
	DIR		   *dir;
	struct dirent *de;
	bool		first_json = true;

	snprintf(undo_dir, MAXPGPATH, "%s/base/undo", pgdata);

	dir = opendir(undo_dir);
	if (!dir)
	{
		pg_log_error("could not open UNDO directory \"%s\": %m", undo_dir);
		pg_log_error_hint("Is this a valid PostgreSQL data directory with UNDO enabled?");
		exit(1);
	}

	/* CSV header */
	if (output_format == FORMAT_CSV && !stats_only)
		fprintf(out, "log,offset,type,xid,reloid,block,page_offset,record_len,prev,payload_len,payload_hex\n");

	/* JSON array start */
	if (output_format == FORMAT_JSON && !stats_only)
		fprintf(out, "[\n");

	while ((de = readdir(dir)) != NULL)
	{
		char		filepath[MAXPGPATH];
		uint32		log_number;
		char	   *endptr;

		/* Skip . and .. */
		if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
			continue;

		/* UNDO log files are 12-digit zero-padded numbers */
		if (strlen(de->d_name) != 12)
			continue;

		log_number = strtoul(de->d_name, &endptr, 10);
		if (*endptr != '\0')
			continue;

		snprintf(filepath, MAXPGPATH, "%s/%s", undo_dir, de->d_name);
		process_undo_log(filepath, log_number, out, &first_json);
	}

	closedir(dir);

	/* JSON array end */
	if (output_format == FORMAT_JSON && !stats_only)
		fprintf(out, "\n]\n");
}

static void
print_statistics(FILE *out)
{
	fprintf(out, "\n--- UNDO Recovery Statistics ---\n");
	fprintf(out, "Total bytes scanned:  %llu\n", (unsigned long long) total_bytes);
	fprintf(out, "Total records found:  %d\n", total_records);
	fprintf(out, "  INSERT records:     %d\n", insert_records);
	fprintf(out, "  DELETE records:     %d\n", delete_records);
	fprintf(out, "  UPDATE records:     %d\n", update_records);
	fprintf(out, "  PRUNE records:      %d\n", prune_records);
	fprintf(out, "  INPLACE records:    %d\n", inplace_records);
	fprintf(out, "Records matching filter: %d\n", matched_records);
}

static void
usage(const char *progname)
{
	printf("%s reads UNDO log files and outputs recovered tuple data.\n\n",
		   progname);
	printf("Usage:\n  %s [OPTION]... PGDATA\n\n", progname);
	printf("Options:\n");
	printf("  -r RELOID    filter by relation OID\n");
	printf("  -x XID       filter by transaction ID\n");
	printf("  -t TYPE      filter by record type (insert/delete/update/prune/inplace)\n");
	printf("  -f FORMAT    output format: text (default), csv, json\n");
	printf("  -s           show statistics only (no record output)\n");
	printf("  -v           verbose mode\n");
	printf("  -V, --version  output version information, then exit\n");
	printf("  -?, --help   show this help, then exit\n");
}

int
main(int argc, char *argv[])
{
	static struct option long_options[] = {
		{"help", no_argument, NULL, '?'},
		{"version", no_argument, NULL, 'V'},
		{NULL, 0, NULL, 0}
	};

	int			c;
	const char *progname;

	pg_logging_init(argv[0]);
	progname = get_progname(argv[0]);

	if (argc > 1)
	{
		if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0)
		{
			usage(progname);
			exit(0);
		}
		if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0)
		{
			puts("pg_undorecover (PostgreSQL) " PG_VERSION);
			exit(0);
		}
	}

	while ((c = getopt_long(argc, argv, "r:x:t:f:svV", long_options, NULL)) != -1)
	{
		switch (c)
		{
			case 'r':
				filter_reloid = (Oid) strtoul(optarg, NULL, 10);
				break;
			case 'x':
				filter_xid = (TransactionId) strtoul(optarg, NULL, 10);
				break;
			case 't':
				filter_type = parse_type(optarg);
				if (filter_type < 0)
				{
					pg_log_error("invalid record type: %s", optarg);
					pg_log_error_hint("Valid types: insert, delete, update, prune, inplace");
					exit(1);
				}
				break;
			case 'f':
				if (strcmp(optarg, "text") == 0)
					output_format = FORMAT_TEXT;
				else if (strcmp(optarg, "csv") == 0)
					output_format = FORMAT_CSV;
				else if (strcmp(optarg, "json") == 0)
					output_format = FORMAT_JSON;
				else
				{
					pg_log_error("invalid output format: %s", optarg);
					pg_log_error_hint("Valid formats: text, csv, json");
					exit(1);
				}
				break;
			case 's':
				stats_only = true;
				break;
			case 'v':
				verbose = true;
				break;
			case 'V':
				puts("pg_undorecover (PostgreSQL) " PG_VERSION);
				exit(0);
			default:
				/* getopt_long already emitted a complaint */
				pg_log_error_hint("Try \"%s --help\" for more information.", progname);
				exit(1);
		}
	}

	if (optind >= argc)
	{
		pg_log_error("no data directory specified");
		pg_log_error_hint("Try \"%s --help\" for more information.", progname);
		exit(1);
	}

	pgdata = argv[optind];

	/* Verify the data directory exists */
	{
		struct stat statbuf;

		if (stat(pgdata, &statbuf) != 0 || !S_ISDIR(statbuf.st_mode))
		{
			pg_log_error("\"%s\" is not a valid directory", pgdata);
			exit(1);
		}
	}

	if (verbose)
	{
		pg_log_info("scanning UNDO logs in %s/base/undo/", pgdata);
		if (filter_reloid != InvalidOid)
			pg_log_info("  filtering by relation OID: %u", filter_reloid);
		if (filter_xid != InvalidTransactionId)
			pg_log_info("  filtering by transaction ID: %u", filter_xid);
		if (filter_type != 0)
			pg_log_info("  filtering by type: %s", type_name(filter_type));
	}

	scan_undo_directory(stdout);

	if (stats_only || verbose)
		print_statistics(stderr);

	return 0;
}

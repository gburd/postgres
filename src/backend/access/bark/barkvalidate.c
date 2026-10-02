/*-------------------------------------------------------------------------
 *
 * barkvalidate.c
 *	  Opclass validator for the BARK index access method.
 *
 * BARK's operator families are btree operator families (see
 * BARK-Design.mediawiki and the ambtreeopfamilies capability): they use the
 * btree strategy numbers 1..5 (<, <=, =, >=, >) and a single required support
 * function, number 1, the three-way ordering comparator.  This validator
 * checks that an opclass offers exactly that, with the right signatures.  It
 * is intentionally narrower than btvalidate -- BARK does not yet define the
 * optional btree support functions (sortsupport, in_range, equalimage,
 * options, skipsupport); those arrive with the features that need them.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/bark/barkvalidate.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/amvalidate.h"
#include "access/bark.h"
#include "access/htup_details.h"
#include "catalog/pg_amop.h"
#include "catalog/pg_amproc.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_type.h"
#include "utils/lsyscache.h"
#include "utils/regproc.h"
#include "utils/syscache.h"

/*
 * Validator for a BARK opclass.
 *
 * Checks that the support functions and operators of the opclass's operator
 * family have BARK-legal numbers and sensible signatures, and that the
 * opclass itself provides the required ordering comparator.
 */
bool
barkvalidate(Oid opclassoid)
{
	bool		result = true;
	HeapTuple	classtup;
	Form_pg_opclass classform;
	Oid			opfamilyoid;
	Oid			opcintype;
	char	   *opclassname;
	char	   *opfamilyname;
	CatCList   *proclist,
			   *oprlist;
	bool		seen_order_proc = false;
	int			i;

	classtup = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclassoid));
	if (!HeapTupleIsValid(classtup))
		elog(ERROR, "cache lookup failed for operator class %u", opclassoid);
	classform = (Form_pg_opclass) GETSTRUCT(classtup);

	opfamilyoid = classform->opcfamily;
	opcintype = classform->opcintype;
	opclassname = NameStr(classform->opcname);
	opfamilyname = get_opfamily_name(opfamilyoid, false);

	oprlist = SearchSysCacheList1(AMOPSTRATEGY, ObjectIdGetDatum(opfamilyoid));
	proclist = SearchSysCacheList1(AMPROCNUM, ObjectIdGetDatum(opfamilyoid));

	/*
	 * Check support functions.  BARK opclasses live in btree operator
	 * families, so the family may legitimately carry any of btree's support
	 * functions (the comparator plus the optional sortsupport, in_range,
	 * equalimage, options, and skipsupport procs).  Validate each against the
	 * same signature btree requires; BARK requires only the comparator
	 * (support function 1) and simply does not call the others.
	 */
	for (i = 0; i < proclist->n_members; i++)
	{
		HeapTuple	proctup = &proclist->members[i]->tuple;
		Form_pg_amproc procform = (Form_pg_amproc) GETSTRUCT(proctup);
		bool		ok;

		switch (procform->amprocnum)
		{
			case BARK_ORDER_PROC:	/* 1: three-way comparator */
				ok = check_amproc_signature(procform->amproc, INT4OID, true,
											2, 2, procform->amproclefttype,
											procform->amprocrighttype);
				if (procform->amproclefttype == opcintype &&
					procform->amprocrighttype == opcintype)
					seen_order_proc = true;
				break;
			case 2:					/* sortsupport */
			case 6:					/* skipsupport */
				ok = check_amproc_signature(procform->amproc, VOIDOID, true,
											1, 1, INTERNALOID);
				break;
			case 3:					/* in_range */
				ok = check_amproc_signature(procform->amproc, BOOLOID, true,
											5, 5,
											procform->amproclefttype,
											procform->amproclefttype,
											procform->amprocrighttype,
											BOOLOID, BOOLOID);
				break;
			case 4:					/* equalimage */
				ok = check_amproc_signature(procform->amproc, BOOLOID, true,
											1, 1, OIDOID);
				break;
			case 5:					/* options */
				ok = check_amoptsproc_signature(procform->amproc);
				break;
			default:
				ereport(INFO,
						(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
						 errmsg("operator family \"%s\" of access method %s contains function %s with invalid support number %d",
								opfamilyname, "bark",
								format_procedure(procform->amproc),
								procform->amprocnum)));
				result = false;
				continue;
		}
		if (!ok)
		{
			ereport(INFO,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("operator family \"%s\" of access method %s contains function %s with wrong signature for support number %d",
							opfamilyname, "bark",
							format_procedure(procform->amproc),
							procform->amprocnum)));
			result = false;
		}
	}

	/* Check operators: strategies must be in the btree 1..5 range. */
	for (i = 0; i < oprlist->n_members; i++)
	{
		HeapTuple	oprtup = &oprlist->members[i]->tuple;
		Form_pg_amop oprform = (Form_pg_amop) GETSTRUCT(oprtup);

		if (oprform->amopstrategy < 1 ||
			oprform->amopstrategy > BARK_NSTRATEGIES)
		{
			ereport(INFO,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("operator family \"%s\" of access method %s contains operator %s with invalid strategy number %d",
							opfamilyname, "bark",
							format_operator(oprform->amopopr),
							oprform->amopstrategy)));
			result = false;
		}

		/* BARK only supports plain comparison (search) operators. */
		if (oprform->amoppurpose != AMOP_SEARCH)
		{
			ereport(INFO,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("operator family \"%s\" of access method %s contains invalid ORDER BY specification for operator %s",
							opfamilyname, "bark",
							format_operator(oprform->amopopr))));
			result = false;
			continue;
		}

		/* Comparison operators must return boolean. */
		if (!check_amop_signature(oprform->amopopr, BOOLOID,
								  oprform->amoplefttype,
								  oprform->amoprighttype))
		{
			ereport(INFO,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("operator family \"%s\" of access method %s contains operator %s with wrong signature",
							opfamilyname, "bark",
							format_operator(oprform->amopopr))));
			result = false;
		}
	}

	/* The opclass must supply its own-type ordering comparator. */
	if (!seen_order_proc)
	{
		ereport(INFO,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("operator class \"%s\" of access method %s is missing support function %d",
						opclassname, "bark", BARK_ORDER_PROC)));
		result = false;
	}

	ReleaseCatCacheList(proclist);
	ReleaseCatCacheList(oprlist);
	ReleaseSysCache(classtup);

	return result;
}

/*-------------------------------------------------------------------------
 *
 * barkvalidate.c
 *	  Opclass validator for the BARK index access method.
 *
 * A BARK operator class lives in one of two kinds of operator family.  A
 * scalar class lives in the btree operator family of its type (the
 * ambtreeopfamilies capability) and is held to btree's rules: strategies
 * 1..5 (<, <=, =, >=, >), the KNN ordering strategy 6, and btree's support
 * functions 1..6, of which BARK requires only the comparator.  A multikey
 * class lives in an operator family of BARK's own and extracts several keys
 * from one value (see "M1: multikey keys" in BARK-Design.mediawiki); its
 * operators may have any strategy number, and its support functions are
 * btree's over the class's storage type plus procedures 7..14.
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
#include "access/xact.h"
#include "catalog/pg_am_d.h"
#include "catalog/pg_amop.h"
#include "catalog/pg_amproc.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/regproc.h"
#include "utils/syscache.h"

static bool bark_validate_multikey(Form_pg_opclass classform);

/*
 * Validator for a BARK opclass.
 *
 * Checks that the support functions and operators of the opclass's operator
 * family have BARK-legal numbers and sensible signatures, and that the
 * opclass itself provides the required ordering comparator.  A class in an
 * operator family of BARK's own is a multikey class, checked by
 * bark_validate_multikey.
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

	if (get_opfamily_method(classform->opcfamily) == BARK_AM_OID)
	{
		result = bark_validate_multikey(classform);
		ReleaseSysCache(classtup);
		return result;
	}

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

		/*
		 * An ORDER BY operator is BARK's KNN distance operator (`<~>`): it has
		 * amoppurpose 'o', the KNN strategy number, a sort family, and returns
		 * the distance type (float8).  Validate it separately from the search
		 * operators, which use the btree 1..5 strategies and return boolean.
		 */
		if (oprform->amoppurpose == AMOP_ORDER)
		{
			if (oprform->amopstrategy != BARK_KNN_STRATEGY)
			{
				ereport(INFO,
						(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
						 errmsg("operator family \"%s\" of access method %s contains operator %s with invalid strategy number %d",
								opfamilyname, "bark",
								format_operator(oprform->amopopr),
								oprform->amopstrategy)));
				result = false;
			}
			if (!OidIsValid(oprform->amopsortfamily))
			{
				ereport(INFO,
						(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
						 errmsg("operator family \"%s\" of access method %s contains invalid ORDER BY specification for operator %s",
								opfamilyname, "bark",
								format_operator(oprform->amopopr))));
				result = false;
			}
			continue;
		}

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

/*
 * Validate a multikey operator class: one in an operator family of BARK's
 * own.  Its support functions are registered under the class's input type,
 * as GIN's are, and those of procedures 1, 2, 4, 5 and 6 have btree's
 * meanings over the storage (key) type.  Procedures 1 and 7 are required, 9
 * is required with 8, and in_range (3) is refused: an extracted column's
 * keys have an order, but its values do not.  Operators may have any
 * strategy number, since only procedures 8 and 9 read it.
 */
static bool
bark_validate_multikey(Form_pg_opclass classform)
{
	bool		result = true;
	Oid			opfamilyoid = classform->opcfamily;
	Oid			opcintype = classform->opcintype;
	Oid			opckeytype = classform->opckeytype;
	char	   *opclassname = NameStr(classform->opcname);
	char	   *opfamilyname = get_opfamily_name(opfamilyoid, false);
	CatCList   *proclist,
			   *oprlist;
	uint64		procs = 0;
	bool		keytype_order_proc = false;

	if (!OidIsValid(opckeytype))
		opckeytype = opcintype;

	oprlist = SearchSysCacheList1(AMOPSTRATEGY, ObjectIdGetDatum(opfamilyoid));
	proclist = SearchSysCacheList1(AMPROCNUM, ObjectIdGetDatum(opfamilyoid));

	for (int i = 0; i < proclist->n_members; i++)
	{
		HeapTuple	proctup = &proclist->members[i]->tuple;
		Form_pg_amproc procform = (Form_pg_amproc) GETSTRUCT(proctup);
		bool		ok;

		if (procform->amproclefttype != procform->amprocrighttype)
		{
			ereport(INFO,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("operator family \"%s\" of access method %s contains support function %s with different left and right input types",
							opfamilyname, "bark",
							format_procedure(procform->amproc))));
			result = false;
		}

		/*
		 * A comparator registered under the storage type, as a btree class
		 * would register it, is never found: BARK looks its support functions
		 * up under the input type.  Remember it for the hint.
		 */
		if (procform->amprocnum == BARK_ORDER_PROC &&
			opckeytype != opcintype &&
			procform->amproclefttype == opckeytype &&
			procform->amprocrighttype == opckeytype)
			keytype_order_proc = true;

		/* Signatures depend on the storage type, known only for this class. */
		if (procform->amproclefttype != opcintype ||
			procform->amprocrighttype != opcintype)
			continue;

		switch (procform->amprocnum)
		{
			case BARK_ORDER_PROC:
				ok = check_amproc_signature(procform->amproc, INT4OID, false,
											2, 2, opckeytype, opckeytype);
				break;
			case BARK_SORTSUPPORT_PROC:
			case BARK_SKIPSUPPORT_PROC:
				ok = check_amproc_signature(procform->amproc, VOIDOID, true,
											1, 1, INTERNALOID);
				break;
			case BARK_EQUALIMAGE_PROC:
				ok = check_amproc_signature(procform->amproc, BOOLOID, true,
											1, 1, OIDOID);
				break;
			case BARK_OPTIONS_PROC:
				ok = check_amoptsproc_signature(procform->amproc);
				break;
			case BARK_EXTRACTVALUE_PROC:
				/* pg_extended_btree's has two arguments, GIN's three */
				ok = check_amproc_signature(procform->amproc, INTERNALOID, false,
											2, 5, opcintype, INTERNALOID,
											INTERNALOID, INTERNALOID,
											INTERNALOID);
				break;
			case BARK_EXTRACTQUERY_PROC:
				/* the seventh argument, BarkQueryFlags, is optional */
				ok = check_amproc_signature(procform->amproc, VOIDOID, false,
											6, 7, opcintype, INT2OID,
											INTERNALOID, INTERNALOID,
											INTERNALOID, INTERNALOID,
											INTERNALOID);
				break;
			case BARK_INDEXRECHECK_PROC:
				ok = check_amproc_signature(procform->amproc, INT2OID, false,
											3, 3, opckeytype, INT2OID,
											INTERNALOID);
				break;
			case BARK_GIN_EXTRACTQUERY_PROC:
				ok = check_amproc_signature(procform->amproc, INTERNALOID, false,
											5, 7, opcintype, INTERNALOID,
											INT2OID, INTERNALOID, INTERNALOID,
											INTERNALOID, INTERNALOID);
				break;
			case BARK_GIN_CONSISTENT_PROC:
				ok = check_amproc_signature(procform->amproc, BOOLOID, false,
											6, 8, INTERNALOID, INT2OID,
											opcintype, INT4OID,
											INTERNALOID, INTERNALOID,
											INTERNALOID, INTERNALOID);
				break;
			case BARK_GIN_COMPAREPARTIAL_PROC:
				ok = check_amproc_signature(procform->amproc, INT4OID, false,
											4, 4, opckeytype, opckeytype,
											INT2OID, INTERNALOID);
				break;
			case BARK_GIN_TRICONSISTENT_PROC:
				ok = check_amproc_signature(procform->amproc, CHAROID, false,
											7, 7, INTERNALOID, INT2OID,
											opcintype, INT4OID,
											INTERNALOID, INTERNALOID,
											INTERNALOID);
				break;
			case BARK_FETCH_PROC:
				ok = check_amproc_signature(procform->amproc, opcintype, false,
											1, 1, opckeytype);
				break;
			default:
				/* in_range (3) included: see the function's comment */
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
		procs |= ((uint64) 1) << procform->amprocnum;
	}

	for (int i = 0; i < oprlist->n_members; i++)
	{
		HeapTuple	oprtup = &oprlist->members[i]->tuple;
		Form_pg_amop oprform = (Form_pg_amop) GETSTRUCT(oprtup);

		/* Ordering operators return their sort family's type, not bool. */
		if (oprform->amoppurpose == AMOP_ORDER)
		{
			if (!OidIsValid(oprform->amopsortfamily))
			{
				ereport(INFO,
						(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
						 errmsg("operator family \"%s\" of access method %s contains invalid ORDER BY specification for operator %s",
								opfamilyname, "bark",
								format_operator(oprform->amopopr))));
				result = false;
			}
			continue;
		}

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

	if ((procs & (((uint64) 1) << BARK_ORDER_PROC)) == 0)
	{
		if (keytype_order_proc)
			ereport(INFO,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("operator class \"%s\" of access method %s is missing support function %d",
							opclassname, "bark", BARK_ORDER_PROC),
					 errdetail("Support function %d is registered for type %s, the storage type.",
							   BARK_ORDER_PROC, format_type_be(opckeytype)),
					 errhint("Register it as FUNCTION %d (%s, %s), under the input type.",
							 BARK_ORDER_PROC, format_type_be(opcintype),
							 format_type_be(opcintype))));
		else
			ereport(INFO,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("operator class \"%s\" of access method %s is missing support function %d",
							opclassname, "bark", BARK_ORDER_PROC),
					 errhint("Register it as FUNCTION %d (%s, %s), under the input type.",
							 BARK_ORDER_PROC, format_type_be(opcintype),
							 format_type_be(opcintype))));
		result = false;
	}
	if ((procs & (((uint64) 1) << BARK_EXTRACTVALUE_PROC)) == 0)
	{
		ereport(INFO,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("operator class \"%s\" of access method %s is missing support function %d",
						opclassname, "bark", BARK_EXTRACTVALUE_PROC)));
		result = false;
	}
	if ((procs & (((uint64) 1) << BARK_EXTRACTQUERY_PROC)) != 0 &&
		(procs & (((uint64) 1) << BARK_INDEXRECHECK_PROC)) == 0)
	{
		ereport(INFO,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("operator class \"%s\" of access method %s has support function %d but not %d",
						opclassname, "bark", BARK_EXTRACTQUERY_PROC,
						BARK_INDEXRECHECK_PROC)));
		result = false;
	}

	ReleaseCatCacheList(proclist);
	ReleaseCatCacheList(oprlist);

	return result;
}

/*
 * Check new members of an operator family as CREATE OPERATOR CLASS adds
 * them.  For its own families BARK's amstrategies is 0, its amsupport 14
 * and its amstorage true, so the core code accepts any strategy number,
 * support functions up to 14 and a STORAGE clause.  A BARK class in a btree
 * family must not get any of them: such a member would make btvalidate
 * report btree's own classes in that family invalid, and the class stores
 * its input type, as btree does.  So a btree family keeps the limits it had
 * before BARK had multikey classes: strategies 1 to 5, support functions 1
 * to 6, no storage type.
 */
void
barkadjustmembers(Oid opfamilyoid, Oid opclassoid, List *operators,
				  List *functions)
{
	ListCell   *lc;

	/*
	 * During CREATE OPERATOR CLASS, need CCI to see the pg_opclass row and
	 * the operator family, which the command may have just made.
	 */
	if (OidIsValid(opclassoid))
		CommandCounterIncrement();
	if (get_opfamily_method(opfamilyoid) != BTREE_AM_OID)
		return;

	foreach(lc, operators)
	{
		OpFamilyMember *op = (OpFamilyMember *) lfirst(lc);

		if (op->number < 1 || op->number > BARK_NSTRATEGIES)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("invalid operator number %d, must be between 1 and %d",
							op->number, BARK_NSTRATEGIES)));
	}
	foreach(lc, functions)
	{
		OpFamilyMember *proc = (OpFamilyMember *) lfirst(lc);

		if (proc->number < 1 || proc->number > BARK_NPROCS)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("invalid function number %d, must be between 1 and %d",
							proc->number, BARK_NPROCS)));
	}
	if (OidIsValid(opclassoid))
	{
		HeapTuple	classtup = SearchSysCache1(CLAOID,
											   ObjectIdGetDatum(opclassoid));

		if (!HeapTupleIsValid(classtup))
			elog(ERROR, "cache lookup failed for operator class %u", opclassoid);
		if (OidIsValid(((Form_pg_opclass) GETSTRUCT(classtup))->opckeytype))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("storage type cannot be different from data type for access method \"%s\" in a btree operator family",
							"bark")));
		ReleaseSysCache(classtup);
	}
}

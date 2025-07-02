// In src/backend/access/heap/heap_equality.c
typedef struct TypeEqualityInfo {
    Oid typoid;
    Oid eq_operator;
    Oid eq_proc_oid;
    bool needs_operator;
    bool is_cached;
    Oid access_method;
} TypeEqualityInfo;

static HTAB *type_equality_cache = NULL;

static void
init_type_equality_cache(void)
{
    HASHCTL hashctl;

    if (type_equality_cache)
        return;

    MemSet(&hashctl, 0, sizeof(hashctl));
    hashctl.keysize = sizeof(Oid) * 2; /* typoid + access_method */
    hashctl.entrysize = sizeof(TypeEqualityInfo);
    hashctl.hcxt = CacheMemoryContext;

    type_equality_cache = hash_create("Type Equality Cache",
                                     512,
                                     &hashctl,
                                     HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

static bool
type_needs_operator_equality(Oid typoid)
{
    HeapTuple typeTuple;
    Form_pg_type typeForm;
    bool needs_operator = false;

    /* Fast path for known built-in types */
    switch (typoid) {
        case JSONBOID:
        case NUMERICOID:
        case TIMESTAMPTZOID:
        case INTERVALOID:
            return true;
        default:
            break;
    }

    /* Check arrays and composite types */
    if (type_is_array(typoid) || type_is_rowtype(typoid))
        return true;

    typeTuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typoid));
    if (!HeapTupleIsValid(typeTuple))
        return false;

    typeForm = (Form_pg_type) GETSTRUCT(typeTuple);

    if (typeForm->typtype == TYPTYPE_BASE) {
        /*
         * Check if type has custom equality operators in ANY access method.
         * Look across all major access methods, not just B-tree.
         */
        needs_operator = type_has_custom_equality_operators(typoid);
    } else {
        /* Domain, composite, enum types generally need operator equality */
        needs_operator = true;
    }

    ReleaseSysCache(typeTuple);
    return needs_operator;
}

static bool
type_has_custom_equality_operators(Oid typoid)
{
    Oid access_methods[] = {BTREE_AM_OID, HASH_AM_OID, GIN_AM_OID, GIST_AM_OID};
    int num_ams = sizeof(access_methods) / sizeof(Oid);

    for (int i = 0; i < num_ams; i++) {
        Oid opfamily = get_default_opfamily_for_am(typoid, access_methods[i]);
        if (OidIsValid(opfamily)) {
            StrategyNumber eq_strategy = get_equality_strategy_for_am(access_methods[i]);
            Oid eq_op = get_opfamily_member(opfamily, typoid, typoid, eq_strategy);

            if (OidIsValid(eq_op)) {
                Oid eq_proc = get_opcode(eq_op);
                if (!is_builtin_equality_proc(eq_proc)) {
                    return true; /* Found custom equality operator */
                }
            }
        }
    }

    return false; /* No custom equality operators found */
}

static bool
relation_attr_needs_operator_equality(Relation relation, AttrNumber attnum)
{
    TupleDesc tupdesc = RelationGetDescr(relation);
    Form_pg_attribute att = TupleDescAttr(tupdesc, attnum - 1);
    List *indexlist = RelationGetIndexList(relation);
    ListCell *lc;
    bool needs_operator = false;

    /* Check each index that includes this attribute */
    foreach(lc, indexlist) {
        Oid indexoid = lfirst_oid(lc);
        Relation indexrel = index_open(indexoid, AccessShareLock);

        /* Check if this attribute is in this index */
        for (int i = 0; i < indexrel->rd_index->indnatts; i++) {
            if (indexrel->rd_index->indkey.values[i] == attnum) {
                /* Check if this AM+type combination needs operator equality */
                TypeEqualityInfo *info = get_type_equality_info_for_am(
                    att->atttypid, indexrel->rd_rel->relam);

                if (info->needs_operator) {
                    needs_operator = true;
                    break;
                }
            }
        }

        index_close(indexrel, AccessShareLock);

        if (needs_operator)
            break;
    }

    list_free(indexlist);
    return needs_operator;
}

static bool
is_builtin_equality_proc(Oid proc_oid)
{
    switch (proc_oid) {
        case F_INT4EQ:
        case F_INT8EQ:
        case F_FLOAT4EQ:
        case F_FLOAT8EQ:
        case F_BOOLEQ:
        case F_CHAREQ:
        case F_TEXTEQ:
            return true;
        default:
            return false;
    }
}

static StrategyNumber
get_equality_strategy_for_am(Oid access_method)
{
    IndexAmRoutine *amroutine;
    StrategyNumber strategy;

    /* Get the access method routine */
    amroutine = GetIndexAmRoutine(GetAccessMethodHandler(access_method));

    /* Check if AM provides custom equality strategy function */
    if (amroutine->amequalitystrategy != NULL) {
        strategy = amroutine->amequalitystrategy();
    } else {
        /* Fall back to hardcoded defaults */
        switch (access_method) {
            case BTREE_AM_OID:
                strategy = BTEqualStrategyNumber;
                break;
            case HASH_AM_OID:
                strategy = HTEqualStrategyNumber;
                break;
            case GIN_AM_OID:
                strategy = GinEqualStrategyNumber;
                break;
            case GIST_AM_OID:
                strategy = GISTEqualStrategyNumber;
                break;
            default:
                /* For unknown AMs, assume strategy 1 is equality */
                strategy = 1;
                break;
        }
    }

    return strategy;
}

static Oid
get_equality_operator_for_am(Oid typoid, Oid access_method)
{
    Oid opfamily;
    Oid eq_operator = InvalidOid;
    StrategyNumber eq_strategy;

    /* Get the default operator family for this access method and type */
    opfamily = get_default_opfamily_for_am(typoid, access_method);

    if (OidIsValid(opfamily)) {
        eq_strategy = get_equality_strategy_for_am(access_method);
        eq_operator = get_opfamily_member(opfamily,
                                        typoid, typoid,
                                        eq_strategy);
    }

    return eq_operator;
}

static TypeEqualityInfo *
get_type_equality_info_for_am(Oid typoid, Oid access_method)
{
    TypeEqualityInfo *info;
    bool found;
    Oid cache_key[2];

    init_type_equality_cache();

    /* Create composite key: typoid + access_method */
    cache_key[0] = typoid;
    cache_key[1] = access_method;

    info = (TypeEqualityInfo *) hash_search(type_equality_cache,
                                           cache_key,
                                           HASH_ENTER,
                                           &found);

    if (!found) {
        info->typoid = typoid;
        info->access_method = access_method;
        info->needs_operator = type_needs_operator_equality(typoid);
        info->is_cached = false;

        if (info->needs_operator) {
            info->eq_operator = get_equality_operator_for_am(typoid, access_method);

            if (OidIsValid(info->eq_operator)) {
                info->eq_proc_oid = get_opcode(info->eq_operator);
                info->is_cached = true;
            }
        }
    }

    return info;
}

static bool
call_cached_equality_proc(Oid eq_proc, Datum val1, Datum val2,
                         bool isnull1, bool isnull2)
{
    /* Handle nulls first */
    if (isnull1 || isnull2)
        return (isnull1 && isnull2);

    if (!OidIsValid(eq_proc)) {
        /* No cached procedure, fall back to binary comparison */
        return datumIsEqual(val1, val2, true, -1); /* Conservative */
    }

    /* Call the cached equality procedure */
    return DatumGetBool(OidFunctionCall2(eq_proc, val1, val2));
}

static Bitmapset *
precompute_complex_equality(Relation relation, HeapTuple oldtup,
                           HeapTuple newtup, Bitmapset *interesting_cols)
{
    Bitmapset *definitely_modified = NULL;
    TupleDesc tupdesc = RelationGetDescr(relation);

    /* Quick check - any attributes need operator equality? */
    Bitmapset *opequal_interesting =
        bms_intersect(relation->rd_opequal_attrs, interesting_cols);

    if (bms_is_empty(opequal_interesting)) {
        bms_free(opequal_interesting);
        return NULL; /* No complex types to check */
    }

    int attnum = -1;
    while ((attnum = bms_next_member(opequal_interesting, attnum)) >= 0) {
        bool isnull1, isnull2;
        Datum val1 = heap_getattr(oldtup, attnum, tupdesc, &isnull1);
        Datum val2 = heap_getattr(newtup, attnum, tupdesc, &isnull2);

        /* Use cached equality procedure */
        Oid eq_proc = relation->rd_attr_eqprocs[attnum - 1];

        if (!call_cached_equality_proc(eq_proc, val1, val2, isnull1, isnull2)) {
            definitely_modified = bms_add_member(definitely_modified, attnum);
        }
    }

    bms_free(opequal_interesting);
    return definitely_modified;
}

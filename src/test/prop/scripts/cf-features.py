#!/usr/bin/env python3
"""cf-features.py -- extract the SQL-surface features a commitfest patch touches,
so the query generator can bias toward exercising them (PG_PROP_GEN_BIAS).

Input: one or more unified-diff / git-format-patch files (the CF patch).
Output: a comma-separated list of feature tags on stdout, or nothing.

Design, after testing v1 on 12 real CF entries:

* A tag needs a *distinctive* signal, not an incidental C token.  v1 tagged a
  pure build-config patch (port/atomics.h) as "numeric,cast" because its C
  code contains `int8` and `::`.  So the hunk patterns are split:
    - SYM rules match a distinctive backend identifier (e.g. PlaceHolderVar,
      ExecConstraints) anywhere in a changed C line.  These are specific
      enough to trust.
    - SQL rules match a SQL keyword (JOIN, UNION, PARTITION, ...) but only on
      lines that are actually SQL: a hunk in a .sql/.out file, or a quoted
      string / ereport message inside C.  A bare `int`/`::`/`UNION` in C code
      no longer counts.
* Rank tags by their share of the signal and emit only the top few, so a
  focused fix yields a focused bias.
* Refuse to bias a patch too large to focus (a new subsystem, not a bug
  fix): it prints nothing and the generator runs unbiased.

Path signal (the file a hunk touches) is the strongest and is weighted
highest; it is immune to the C-token noise above.

Limits (seen on the 12 CF entries this was calibrated against): a fix in
backend-internal code with no SQL surface (e.g. an executor tuple-slot fix
touching tts_tid / ExecForceStoreHeapTuple) yields a weak or empty bias --
correctly, since there is little SQL surface to aim a query generator at.
That is the intended behaviour: a weak/empty bias is better than a
confident wrong one, and such a patch is a poor biased-hunt target anyway.

Tags (kept in sync with the bias table in query_gen.c):
  joins setops aggregation window distinct subquery nulls collation
  numeric partition toast index constraint cast
"""
import re
import sys

# PATH rules: (regex over the touched file path) -> tag.  Weight 4 (strong).
PATH_RULES = [
    (r'optimizer/(path|plan)/(join|analyzejoins|initsplan)|paths?/joinpath|joinrels', 'joins'),
    (r'optimizer/prep/prepunion|path/.*setop|executor/nodeSetOp|executor/nodeAppend', 'setops'),
    (r'executor/nodeAgg|optimizer/.*/.*agg|parser/parse_agg|nodeGroup', 'aggregation'),
    (r'executor/nodeWindowAgg|optimizer/.*window|nodeWindowFunc', 'window'),
    (r'optimizer/prep/prepjointree|plan/subselect|nodeSubplan|nodeSubqueryscan', 'subquery'),
    (r'utils/adt/(varlena|pg_locale|like_|formatting)|backend/utils/mb|regc_|collationcmds', 'collation'),
    (r'utils/adt/(numeric|float|cash)\.c|utils/adt/int8\.c', 'numeric'),
    (r'partitioning/|partdesc|partprune|partbounds|optimizer/.*partition|execPartition', 'partition'),
    (r'access/(heap|common)/.*toast|access/table/toast|heaptoast|tuptoaster|rewriteheap|detoast', 'toast'),
    (r'access/(nbtree|gist|gin|spgist|hash|brin)/|optimizer/path/indxpath|executor/nodeIndex', 'index'),
    (r'commands/(trigger|tablecmds)\.c|executor/execConstr|catalog/pg_constraint|utils/adt/ri_triggers|commands/typecmds', 'constraint'),
    (r'parser/parse_coerce|utils/adt/.*(_in|_out|_recv|_send)\b', 'cast'),
]

# SYM rules: distinctive backend identifier in any changed C line.  Weight 2.
SYM_RULES = [
    (r'\b(PlaceHolderVar|nullingrel\w*|join_is_removable|remove_useless_join|JoinExpr|make_outerjoininfo|deconstruct_jointree)\b', 'joins'),
    (r'\b(SetOperationStmt|generate_(union|nonunion)_paths|plan_set_operations|make_setop)\b', 'setops'),
    (r'\b(Aggref|GroupingFunc|make_agg|AggState|create_agg_path|grouping_sets?|create_groupingsets_path|eager.?aggregat\w*)\b', 'aggregation'),
    (r'\b(WindowFunc|WindowAggState|create_windowagg_path|frameOptions|WindowClause)\b', 'window'),
    (r'\b(SubLink|SubPlan|pull_up_subqueries|convert_ANY_sublink|convert_EXISTS_sublink|hashed.?subplan)\b', 'subquery'),
    (r'\b(DistinctExpr|create_distinct_paths|distinctClause|rel_is_distinct_for|query_is_distinct_for)\b', 'distinct'),
    (r'\b(contain_nonstrict\w*|clause_is_strict\w*|not?_?null\w*frac|nulltesttype|BoolExprType|ExecEvalCase\w*|expr_is_nonnullable)\b', 'nulls'),
    (r'\b(varstr_cmp|pg_ascii_tolower|towlower|iswupper|lc_collate\w*|get_collation_isdeterministic|pg_newlocale\w*|OidIsValid\(.*collid)\b', 'collation'),
    (r'\b(numeric_\w+|float[48]_\w+|trim_scale|make_numeric|NUMERIC_\w+|cash_\w+)\b', 'numeric'),
    (r'\b(RelationGetPartition\w*|get_partition_\w+|apply_scanjoin_target|partition_bounds\w*|PartitionPruneState|ExecFindPartition)\b', 'partition'),
    (r'\b(toast_\w+|detoast\w*|heap_(fetch|tuple)_?toast|TOAST_\w+|varatt\w*|reltoastrelid|init_toast_snapshot)\b', 'toast'),
    (r'\b(amcanreturn|IndexOnlyScan\w*|index_(getnext|rescan|beginscan)|ScanKeyEntry|BTScanOpaque|_bt_\w+|gistget\w*|skip.?scan\w*)\b', 'index'),
    (r'\b(ExecConstraints|RI_\w+|ri_\w+|ATExecAddConstraint|check_constraint|DomainConstraint\w*|CheckConstraintFetch)\b', 'constraint'),
    (r'\b(coerce_type|CoerceViaIO|coerce_to_target_type|RelabelType|CoerceToDomain|find_coercion_pathway)\b', 'cast'),
]

# SQL rules: a SQL keyword, counted only on a SQL-ish line (see sql_context).
# Weight 1.
SQL_RULES = [
    (r'\b(FULL|RIGHT|LEFT)\s+(OUTER\s+)?JOIN\b', 'joins'),
    (r'\b(UNION|INTERSECT|EXCEPT)\b', 'setops'),
    (r'\bGROUP\s+BY\b|\bHAVING\b|\bGROUPING\s+SETS\b|\bROLLUP\b|\bCUBE\b', 'aggregation'),
    (r'\bWINDOW\b|\bOVER\s*\(|\b(ROWS|RANGE|GROUPS)\s+BETWEEN\b', 'window'),
    (r'\bDISTINCT\b', 'distinct'),
    (r'\bLATERAL\b|\bEXISTS\s*\(|\bIN\s*\(\s*SELECT\b', 'subquery'),
    (r'\bIS\s+(NOT\s+)?NULL\b|\bIS\s+(NOT\s+)?DISTINCT\s+FROM\b', 'nulls'),
    (r'\bCOLLATE\b|deterministic\s*=\s*false', 'collation'),
    (r"\bnumeric\s*\(|\bNaN\b|'Infinity'|::numeric\b|::float[48]\b|\bscale\s*\(", 'numeric'),
    (r'\bPARTITION\s+(BY|OF)\b|\b(ATTACH|DETACH)\s+PARTITION\b', 'partition'),
    (r'\bSET\s+STORAGE\b|\b(EXTERNAL|EXTENDED|MAIN)\b|missing\s+chunk', 'toast'),
    (r'\bUSING\s+(gist|gin|spgist|hash|brin|btree)\b|\btext_pattern_ops\b|\bINDEX\s+ONLY\b', 'index'),
    (r'\bCHECK\s*\(|\bNOT\s+NULL\b|\bFOREIGN\s+KEY\b|\bREFERENCES\b|\bUNIQUE\b|\bEXCLUDE\s+USING\b', 'constraint'),
    (r'\bCAST\s*\(|::\w+|\bWITHOUT\s+OVERLAPS\b', 'cast'),
]

PATH_W, SYM_W, SQL_W = 4, 2, 1

# ---------------------------------------------------------------------------
# Vettability classification.  The differential/property method only has an
# oracle for patches with a SQL-observable wrong answer.  Map a patch to the
# oracle class that applies so "validate them all" becomes a defensible scoped
# list -- and so a cost-only or display-only or tooling patch is not mistaken
# for something a result differential can judge (it cannot, by construction).
#
# Classes, most-specific first:
#   deparse      a pg_get_*def / pg_get_*_ddl / ruleutils change -> round-trip
#                oracle (deparse, recreate, require same definition + rows).
#   isolation    a concurrency/SSI change -> needs an isolation spec, not the
#                single-session generator.
#   estimate     a selectivity/stats change -> estimate-vs-actual, not a
#                row-set differential (the rows do not change, the estimate
#                does).
#   differential a planner/executor/AM/type change that can change the result
#                set -> the row-multiset differential (plan A == plan B).
#   not-vettable docs, tooling (bin/, psql, pg_dump, pg_rewind, pgbench),
#                refactors, pure cost tweaks, EXPLAIN-text-only, replication/WAL
#                operational -> human review, no differential oracle.
CLASS_RULES = [
    # (regex over a touched file path, class, why)
    ('deparse', r'ruleutils\.c|utils/adt/ddlutils\.c|pg_get_\w+def|pg_get_\w+_ddl', 'deparse round-trip'),
    ('isolation', r'storage/lmgr/predicate\.c|lmgr/.*ssi|/isolation/specs/|README-SSI', 'isolation spec'),
    ('estimate', r'utils/adt/selfuncs\.c|optimizer/path/clausesel\.c|statistics/(extended_stats|mcv|dependencies)|optimizer/util/plancat\.c', 'estimate vs actual'),
    ('differential', r'executor/node|optimizer/(path|plan|prep)/|access/(nbtree|gist|gin|spgist|hash|brin|heap|common|table)/|utils/adt/\w+\.c|parser/parse_|commands/\w+\.c|rewrite/|catalog/pg_proc\.dat|contrib/\w+/\w+\.c', 'row-set differential'),
]
# paths that force not-vettable regardless of the above (tooling, docs, tests).
NOT_VETTABLE = re.compile(
    r'(^|/)(doc/|src/bin/|contrib/\w+/t/|/t/\d|psql/|pg_dump/|pg_rewind/|pg_basebackup/|'
    r'pg_combinebackup/|pg_resetwal/|pg_surgery/|pg_walinspect/|pgbench/|'
    r'\.sgml|\.po$|/po/|regress/(sql|expected)/)')

# A patch larger than this many changed lines is a new subsystem, not a bug
# fix a focused bias helps -> emit nothing.  (Tag count alone is a poor
# gate: a focused fix can touch several surfaces loosely; ranking handles
# that.)
MAX_DIFF_LINES = 50000

# Keep at most this many top tags, and drop any whose score is below this
# fraction of the leader's (a negligible tail tag only dilutes the bias).
MAX_KEEP = 3
TAIL_FRACTION = 0.10


def is_sql_file(path):
    return bool(re.search(r'\.(sql|out)(\s|$)', path))


def sql_context(line, in_sql_file):
    """True if a SQL keyword on this C/SQL line should count as SQL surface:
    the whole file is SQL, or the token sits inside a quoted string / an
    ereport/errmsg in C (where SQL keywords denote real surface, not C code)."""
    if in_sql_file:
        return True
    # a quoted string on the line, or an ereport/errmsg/appendStringInfo call
    return ('"' in line and re.search(r'"[^"]*\b[A-Z]{2,}\b', line) is not None) \
        or re.search(r'errmsg|ereport|appendStringInfo|elog\s*\(', line) is not None


def extract(difftext):
    tags = {}
    changed = 0
    cur_path = ''
    in_sql_file = False
    for line in difftext.splitlines():
        if line.startswith('+++ ') or line.startswith('--- '):
            cur_path = line[4:]
            in_sql_file = is_sql_file(cur_path)
            if line.startswith('+++ '):          # count the file once
                for pat, tag in PATH_RULES:
                    if re.search(pat, cur_path, re.I):
                        tags[tag] = tags.get(tag, 0) + PATH_W
            continue
        if not (line.startswith('+') or line.startswith('-')):
            continue
        body = line[1:]
        changed += 1
        for pat, tag in SYM_RULES:
            if re.search(pat, body):
                tags[tag] = tags.get(tag, 0) + SYM_W
        if in_sql_file or sql_context(body, in_sql_file):
            for pat, tag in SQL_RULES:
                if re.search(pat, body):
                    tags[tag] = tags.get(tag, 0) + SQL_W
    return tags, changed


def touched_paths(difftext):
    """The set of non-/dev/null file paths the diff touches (from +++ lines)."""
    paths = set()
    for line in difftext.splitlines():
        if line.startswith('+++ '):
            p = line[4:].strip()
            if p and p != '/dev/null':
                paths.add(re.sub(r'^[ab]/', '', p))
    return paths


def classify(difftext):
    """Return (class, why): the oracle class that can vet this patch.

    A patch that touches ONLY not-vettable paths (docs, tooling, tests) is
    not-vettable.  Otherwise the most-specific matching CLASS_RULES entry
    over any backend/contrib source path wins."""
    paths = touched_paths(difftext)
    src = [p for p in paths if not NOT_VETTABLE.search(p)]
    if not src:
        return 'not-vettable', 'only docs/tooling/tests touched'
    for cls, pat, why in CLASS_RULES:
        rx = re.compile(pat, re.I)
        if any(rx.search(p) for p in src):
            return cls, why
    return 'not-vettable', 'no backend change with a differential oracle'


def main(argv):
    if len(argv) < 2:
        sys.stderr.write("usage: cf-features.py [--classify] PATCH [PATCH ...]\n")
        return 2
    do_classify = False
    args = argv[1:]
    if args and args[0] == '--classify':
        do_classify = True
        args = args[1:]
    if not args:
        sys.stderr.write("usage: cf-features.py [--classify] PATCH [PATCH ...]\n")
        return 2
    text = ''
    for fn in args:
        with open(fn, encoding='utf-8', errors='replace') as f:
            text += f.read()
    if do_classify:
        cls, why = classify(text)
        print('%s\t%s' % (cls, why))
        return 0
    tags, changed = extract(text)
    # a tag needs at least a path hit, a symbol hit, or two SQL-context hits
    strong = {t: n for t, n in tags.items() if n >= SYM_W}
    if not strong:
        return 0
    # a new subsystem (huge diff) is not something a focused bias helps
    if changed > MAX_DIFF_LINES:
        sys.stderr.write("cf-features: patch too large to bias (%d changed lines); "
                         "run unbiased\n" % changed)
        return 0
    ranked = sorted(strong, key=lambda t: -strong[t])
    lead = strong[ranked[0]]
    # keep the top MAX_KEEP, dropping any negligible tail relative to the leader
    keep = [t for t in ranked[:MAX_KEEP] if strong[t] >= lead * TAIL_FRACTION]
    print(','.join(keep))
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))

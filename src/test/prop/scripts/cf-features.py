#!/usr/bin/env python3
"""cf-features.py -- extract the SQL-surface features a commitfest patch touches,
so the query generator can bias toward exercising them (task #3).

Input: one or more unified-diff / git-format-patch files (the CF patch).
Output: a PG_PROP_GEN_BIAS value -- a comma-separated list of feature tags the
generator understands -- printed to stdout.

The mapping is deliberately conservative: a tag is emitted only when the diff
touches a file or identifier strongly associated with that SQL surface, so a
biased run still generates everything, just with extra weight on the area the
patch changed.  Over-biasing would narrow the search and miss neighbouring
bugs; under-biasing costs only a little focus.  When nothing matches, print
nothing (the generator runs unbiased).

Tags (kept in sync with the bias table in query_gen.c):
  joins        FULL/RIGHT/outer-join planning, join removal, PlaceHolderVar
  setops       UNION/INTERSECT/EXCEPT, append, setop planning
  aggregation  GROUP BY / aggregates / grouping sets / eager aggregate
  window       window functions / frames
  distinct     DISTINCT / DISTINCT ON / unique-ification
  subquery     subquery pull-up, LATERAL, correlated, sublink
  nulls        NULL / three-valued logic / strictness
  collation    collation, especially nondeterministic
  numeric      numeric scale / float / cast
  partition    partitioning, pruning, partitionwise
  toast        TOAST / detoast / external storage / rewrite
  index        index scan / index-only / amcanreturn / opclass
  constraint   CHECK / NOT NULL / FK / domain / unique
  cast         coercion / cast / type input-output
"""
import re
import sys

# (compiled regex over added/removed diff lines and touched paths) -> tag
# Each rule is (where, pattern, tag): where is 'path' (the +++/--- file) or
# 'hunk' (a +/- code line).
RULES = [
    ('path', r'optimizer/(path|plan)/(join|analyzejoins|initsplan)', 'joins'),
    ('hunk', r'\b(FULL|RIGHT|LEFT)\s+(OUTER\s+)?JOIN\b|PlaceHolderVar|nullingrel|outerjoin|join_is_removable|JoinExpr', 'joins'),
    ('path', r'optimizer/(prep/prepunion|path/.*setop)|executor/nodeSetOp|nodeAppend', 'setops'),
    ('hunk', r'\b(UNION|INTERSECT|EXCEPT)\b|SetOp|generate_union|append_rel', 'setops'),
    ('path', r'executor/nodeAgg|optimizer/.*agg|parser/parse_agg', 'aggregation'),
    ('hunk', r'\bGROUP\s+BY\b|\bHAVING\b|Aggref|grouping\s*set|GroupingFunc|eager.?aggregat|aggsplit|partial.?aggregat', 'aggregation'),
    ('path', r'executor/nodeWindowAgg|optimizer/.*window|parser/parse_(clause|func).*window', 'window'),
    ('hunk', r'\bWINDOW\b|WindowFunc|window\s*frame|\bOVER\b\s*\(|ROWS\s+BETWEEN|RANGE\s+BETWEEN|GROUPS\s+BETWEEN', 'window'),
    ('hunk', r'\bDISTINCT\b|DISTINCT\s+ON|unique.?if|rel_is_distinct|distinctClause', 'distinct'),
    ('path', r'optimizer/(prep/prepjointree|plan/subselect)|parser/parse_(clause|relation)', 'subquery'),
    ('hunk', r'\bLATERAL\b|pull_up_subqueries|SubLink|SubPlan|flatten|convert_ANY|convert_EXISTS|correlat', 'subquery'),
    ('hunk', r'\bIS\s+(NOT\s+)?NULL\b|\bIS\s+(NOT\s+)?DISTINCT\b|nonstrict|strictness|contain_nonstrict|null.?frac|three.?valued|ExecEvalCase', 'nulls'),
    ('path', r'utils/adt/(varlena|pg_locale|like)|backend/utils/mb', 'collation'),
    ('hunk', r'\bCOLLATE\b|collation|deterministic\s*=\s*false|nondeterministic|lc_collate|towlower|iswupper|pg_ascii_tolower|varstr_cmp', 'collation'),
    ('path', r'utils/adt/(numeric|float|cash|int8?)\b', 'numeric'),
    ('hunk', r'\bnumeric\b|\bfloat[48]\b|trim_scale|\bNaN\b|Infinity|scale\s*\(|rint|pg_ascii', 'numeric'),
    ('path', r'(partitioning|partdesc|partprune|partbounds)|executor/nodeAppend|optimizer/.*partition', 'partition'),
    ('hunk', r'\bPARTITION\b|partition.?wise|partition.?prun|RelationGetPartition|ATTACH\s+PARTITION|DETACH\s+PARTITION', 'partition'),
    ('path', r'access/(heap|common)/.*toast|access/table/toast|detoast|heaptoast|tuptoaster|rewriteheap', 'toast'),
    ('hunk', r'\bTOAST\b|detoast|toast_fetch|SET\s+STORAGE|EXTERNAL|EXTENDED|varatt|missing\s+chunk|reltoastrelid', 'toast'),
    ('path', r'access/(nbtree|gist|gin|spgist|hash|brin)|optimizer/path/indxpath|executor/nodeIndex', 'index'),
    ('hunk', r'index.?only|amcanreturn|IndexOnlyScan|opclass|operator\s+class|index_(get|rescan)|\bamsearch|skip\s*scan', 'index'),
    ('path', r'commands/(trigger|tablecmds)|executor/execConstr|catalog/pg_constraint|utils/adt/ri_triggers', 'constraint'),
    ('hunk', r'\bCHECK\b|\bNOT\s+NULL\b|\bFOREIGN\s+KEY\b|\bUNIQUE\b|ExecConstraints|\bdomain\b|RI_\w+|constraint', 'constraint'),
    ('path', r'parser/parse_coerce|utils/adt/.*(in|out|recv|send)\b', 'cast'),
    ('hunk', r'\bCAST\b|coerc|::\s*\w+|input\s+function|CoerceViaIO|typmod|RelabelType', 'cast'),
]


def extract(difftext):
    tags = {}  # tag -> hit count, for ranking
    cur_path = ''
    for line in difftext.splitlines():
        if line.startswith('+++ ') or line.startswith('--- '):
            cur_path = line[4:]
            for where, pat, tag in RULES:
                if where == 'path' and re.search(pat, cur_path, re.I):
                    tags[tag] = tags.get(tag, 0) + 3  # path match is a strong signal
        elif line.startswith('+') or line.startswith('-'):
            if line.startswith('+++') or line.startswith('---'):
                continue
            body = line[1:]
            for where, pat, tag in RULES:
                if where == 'hunk' and re.search(pat, body):
                    tags[tag] = tags.get(tag, 0) + 1
    return tags


def main(argv):
    if len(argv) < 2:
        sys.stderr.write("usage: cf-features.py PATCH [PATCH ...]\n")
        return 2
    text = ''
    for fn in argv[1:]:
        with open(fn, encoding='utf-8', errors='replace') as f:
            text += f.read()
    tags = extract(text)
    # keep tags with a meaningful signal (>= 2: a path hit, or two hunk hits)
    ranked = sorted((t for t, n in tags.items() if n >= 2),
                    key=lambda t: -tags[t])
    if ranked:
        print(','.join(ranked))
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))

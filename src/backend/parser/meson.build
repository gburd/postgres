# Copyright (c) 2022-2026, PostgreSQL Global Development Group

backend_sources += files(
  'analyze.c',
  'parse_agg.c',
  'parse_clause.c',
  'parse_coerce.c',
  'parse_collate.c',
  'parse_cte.c',
  'parse_enr.c',
  'parse_expr.c',
  'parse_func.c',
  'parse_graphtable.c',
  'parse_jsontable.c',
  'parse_merge.c',
  'parse_node.c',
  'parse_oper.c',
  'parse_param.c',
  'parse_relation.c',
  'parse_target.c',
  'parse_type.c',
  'parse_utilcmd.c',
  'scansup.c',
)

# Build a small utility static lib for the parser. The generation of the
# parser is slow, and building this separately avoids other parts of the
# backend having to wait till gram.h is generated.
#
# scan_lex.c is the Lime-generated scanner (replacing scan.c's hand-rolled
# state machine; scan.c is now a parser-driver shim around
# CoreLexFeedBytes).  Like upstream's flex output it is compiled directly
# into the parser lib -- Lime v1.5.1 emits warning-clean scanner code.
parser_sources = files('parser.c', 'parser_extension.c', 'scan.c')

# When AOT is enabled, lime -j produces gram_aot.c alongside gram.c.
# gram.c gets compiled with -DYYAOT so its yy_find_shift_action()
# forwards to yy_find_shift_action_aot() (defined in gram_aot.c).
# gram_aot.c is a switch-of-switches that the C compiler optimises
# into jump tables -- the static-codegen equivalent of Lime's
# runtime LLVM JIT, with zero runtime cost.
#
# We also emit gram_snapshot.c (lime -n): base_yyBuildSnapshot()
# returns a ParserSnapshot built from the generator tables with the
# grammar source embedded.  The runtime grammar-extension framework
# (parser_extension.c) uses it to compose extension grammars
# in-process (lime_compile_grammar_in_process), with no subprocess.
if lime_aot_enabled
  backend_parser = custom_target('gram_aot',
    input: 'gram.lime',
    kwargs: lime_aot_snapshot_kw,
  )
else
  backend_parser = custom_target('gram',
    input: 'gram.lime',
    kwargs: lime_snapshot_kw,
  )
endif
generated_sources += backend_parser.to_list()

# The generated grammar snapshot (gram_snapshot.c) provides
# base_yyBuildSnapshot().  It #include's Lime's runtime headers with
# bare names ("snapshot.h", "snapshot_build.h") whose basenames collide
# with PostgreSQL's own utils/snapshot.h.  Compile it in an isolated
# static_library that puts Lime's include directory on the search path,
# so its bare includes resolve to Lime's headers without leaking that
# directory onto any backend translation unit (which must keep seeing
# PostgreSQL's snapshot.h).  The index of the _snapshot.c output is 3
# under AOT (c, h, aot.c, snapshot.c) and 2 otherwise (c, h, snapshot.c).
backend_gram_snapshot_idx = lime_aot_enabled ? 3 : 2
if lime_runtime_dep.found()
  lime_runtime_incdir = include_directories(
    lime_runtime_dep.get_variable(pkgconfig: 'includedir') / 'lime',
    is_system: true,
  )
  gram_snapshot_lib = static_library('gram_snapshot',
    backend_parser[backend_gram_snapshot_idx],
    dependencies: [backend_code, lime_runtime_dep],
    include_directories: [include_directories('.'), lime_runtime_incdir],
    c_args: ['-Wno-missing-prototypes'],
    kwargs: internal_lib_args,
  )
  backend_link_with += gram_snapshot_lib
endif

# Lime-generated scanner.  Compiled into the parser lib below alongside
# the generated grammar -- both are warning-clean as of Lime v1.5.x, so
# (like upstream's flex scan.c + bison gram.c) they need no isolation.
scan_lex_gen = custom_target('scan_lex',
  input: 'scan.lex',
  output: ['scan_lex.c', 'scan_lex.h'],
  command: lime_lex_cmd,
)
generated_sources += scan_lex_gen

# The `parser` lib compiles the hand-written sources, the generated
# scanner, and the generated grammar -- but NOT gram_snapshot.c (built
# in its own lib above, since it needs Lime's include path).  Pass the
# grammar's .c (index 0), .h (index 1), and -- under AOT -- _aot.c
# (index 2) explicitly, omitting the _snapshot.c output.
backend_parser_objs = lime_aot_enabled \
  ? [backend_parser[0], backend_parser[1], backend_parser[2]] \
  : [backend_parser[0], backend_parser[1]]
parser = static_library('parser',
  parser_sources,
  scan_lex_gen,
  backend_parser_objs,
  dependencies: [backend_code],
  include_directories: include_directories('.'),
  c_args: lime_aot_enabled ? ['-DYYAOT'] : [],
  kwargs: internal_lib_args,
)
backend_link_with += parser

# Install gram.lime to $PGSHAREDIR/parser/gram.lime so the Phase 4
# subprocess pipeline (parser_extension.c) can locate it at runtime
# to rebuild the parser when grammar extensions register.
install_data('gram.lime',
  install_dir: dir_data / 'parser',
)

# Install gramparse.h (a header private to the in-tree build) to
# $PGSHAREDIR/include/server/parser so the runtime-rebuilt parser .so
# (parser_extension.c) can compile.  Lime's emitted gram.c does
# #include "gramparse.h" verbatim from the prologue; we install the
# header alongside the other server-internal headers and the runtime
# adds -I<server_includes>/parser to its cc invocation.  The local
# `#include "gram.h"` inside gramparse.h resolves to the cache-dir
# shim the runtime writes alongside the regenerated <hex>.{c,h}.
install_headers('gramparse.h',
  install_dir: dir_include_server / 'parser',
)

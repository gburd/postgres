#!/usr/bin/env python3
"""
lime_convert_gram.py - Mechanical Bison (.y) -> Lime (.lime) converter.

This is the Phase 2k.2 scaffolding called out in AGENTS.md.  It performs
the mechanical translation from Bison grammar files to Lime grammar
files; subtle hand work (fixing precedence ambiguities, integrating
with PostgreSQL's hand-rolled scanner) is expected to follow.

Usage:
    lime_convert_gram.py <input.y> <output.lime> [--prefix NAME]

Supported directives (translated):
    %pure-parser       -> dropped (Lime parsers are always reentrant)
    %locations         -> dropped (caller-managed in Lime)
    %expect N          -> %expect N.
    %name-prefix=foo   -> %name foo
    %name-prefix "foo" -> %name foo
    %parse-param {...} -> %extra_argument {...}  (single allowed; multi-param
                          grammars are folded by the converter into a struct
                          if it can detect them, otherwise it errors out.)
    %lex-param {...}   -> dropped (Lime is push-parser; caller drives lexer)
    %left X Y          -> %left X Y.
    %right X Y         -> %right X Y.
    %nonassoc X Y      -> %nonassoc X Y.
    %prec TOKEN  (rule-trailing) -> [TOKEN]  (square brackets)
    %token NAME        -> %token NAME.
    %token <m> N1 N2   -> %token N1.\n%token N2.   (member resolved via %union)
    %type <m> N1 N2    -> %type N1 {Ctype}\n%type N2 {Ctype}
    %union { ... }     -> stripped from Lime body and re-emitted as a typedef
                          inside %include {...} so generated parser sees it.
    %start NT          -> %start_symbol NT
    %code { ... }      -> %include { ... }
    %glr-parser        -> hard error (Lime is LALR only)

Action rewriting:
    $$  -> A           (LHS letter label)
    $N  -> B,C,D,...   (RHS letter labels, indexed only over actual symbols;
                        char literals like '(' that we map to LPAREN still
                        count as one symbol position.)
    @N  -> @<label>    (Lime convention; project glue can `#define LOC(L) ...`)
    Mid-rule actions   -> extracted to per-rule helper non-terminals.

Validation:
    - Warns if a %token is never referenced on a rule's RHS.
    - Warns if a %type-decl'd non-terminal never appears as an LHS.
    - Warns if a non-terminal appearing on an RHS lacks a %type declaration
      (Bison default-promoted to <type>=int).
"""

from __future__ import annotations

import argparse
import re
import sys
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple


# ---------------------------------------------------------------------------
# Char-literal -> symbolic-token-name table.
#
# PostgreSQL's scanner currently returns raw ASCII for these tokens; the Lime
# port will need a glue layer that maps the integer code to the symbolic name
# the Lime-generated parser expects.  Until that glue lands the char names
# below are placeholders that match the lime/examples/pg/ convention.
# ---------------------------------------------------------------------------
CHAR_TOKEN_MAP: Dict[str, str] = {
    "'('": "LPAREN",
    "')'": "RPAREN",
    "'['": "LBRACKET",
    "']'": "RBRACKET",
    "','": "COMMA",
    "';'": "SEMI",
    "':'": "COLON",
    "'.'": "DOT",
    "'+'": "PLUS",
    "'-'": "MINUS",
    "'*'": "STAR",
    "'/'": "SLASH",
    "'%'": "PERCENT",
    "'^'": "CARET",
    "'|'": "PIPE",
    "'<'": "LT",
    "'>'": "GT",
    "'='": "EQ",
    "'!'": "BANG",
    "'?'": "QMARK",
    "'@'": "AT_SIGN",
    "'~'": "TILDE",
    "'#'": "HASH",
    "'&'": "AMP",
    "'{'": "LBRACE",
    "'}'": "RBRACE",
}

# Bison token names whose ALL-CAPS Lime form differs.  We deliberately keep
# the Bison spellings we know; everything not in this map is passed through
# unchanged.
TOKEN_RENAME: Dict[str, str] = {
    # PostgreSQL gram.y has one camelCase token: Op (the catch-all
    # operator).  Lime conventionally uses ALL CAPS for tokens.  Rename
    # mechanically; the scanner-side glue will follow.
    "Op": "OP",
}


# ---------------------------------------------------------------------------
# Push-parser driver template for the backend SQL grammar (%name base_yy).
#
# Lime emits a reentrant push parser; PG's callers (parser.c) still call
# base_yyparse() and expect it to drive base_yylex() to completion.  This
# block is appended verbatim to gram.lime so the generated .c contains the
# shim.  The ASCII-to-Lime mapper handles single-character tokens that
# scan.c still returns as raw bytes (Bison auto-declared them; Lime needs
# explicit symbolic names).
# ---------------------------------------------------------------------------
_BACKEND_PARSER_DRIVER = """\
/* Driver. */
%include {
#include "utils/palloc.h"

extern void *base_yyAlloc(void *(*mallocProc)(size_t));
extern void base_yyLoc(void *yyp, int yymajor, YYSTYPE yyminor,
					   YYLTYPE yyloc, core_yyscan_t yyscanner);
extern void base_yyFree(void *p, void (*freeProc)(void *));

/*
 * Translate raw-ASCII single-char tokens scan.c emits (',', ';', '(', etc.)
 * to their Lime symbolic ids.  With %first_token 258 in effect the keyword
 * tokens are 258+ and ASCII bytes 0..127 are guaranteed to mean the literal
 * character; this translation is now unambiguous.  See Lime upstream commit
 * 4255b05 (P0-NEW-4).
 */
static inline int
ascii_to_lime_token(int t)
{
	switch (t)
	{
		case '(':	return LPAREN;
		case ')':	return RPAREN;
		case '[':	return LBRACKET;
		case ']':	return RBRACKET;
		case ',':	return COMMA;
		case ';':	return SEMI;
		case ':':	return COLON;
		case '.':	return DOT;
		case '+':	return PLUS;
		case '-':	return MINUS;
		case '*':	return STAR;
		case '/':	return SLASH;
		case '%':	return PERCENT;
		case '^':	return CARET;
		case '|':	return PIPE;
		case '<':	return LT;
		case '>':	return GT;
		case '=':	return EQ;
		default:	return t;
	}
}

int
base_yyparse(core_yyscan_t yyscanner)
{
	void	   *parser;
	YYSTYPE		lval;
	YYLTYPE		lloc = 0;
	int			token;

	parser = base_yyAlloc(palloc);
	while ((token = base_yylex(&lval, &lloc, yyscanner)) != 0)
	{
		base_yyLoc(parser, ascii_to_lime_token(token), lval, lloc, yyscanner);
	}
	base_yyLoc(parser, 0, lval, lloc, yyscanner);
	base_yyFree(parser, pfree);
	return 0;
}
}
"""

_PLPGSQL_PARSER_DRIVER = """\
/* Driver. */
%include {
#include "utils/palloc.h"

extern void *plpgsql_yyAlloc(void *(*mallocProc)(size_t));
extern void plpgsql_yyLoc(void *yyp, int yymajor, YYSTYPE yyminor,
						  YYLTYPE yyloc, struct GramParseExtra *extra);
extern void plpgsql_yyFree(void *p, void (*freeProc)(void *));

static inline int
plpgsql_ascii_to_lime_token(int t)
{
	switch (t)
	{
		case '(':	return LPAREN;
		case ')':	return RPAREN;
		case '[':	return LBRACKET;
		case ']':	return RBRACKET;
		case ',':	return COMMA;
		case ';':	return SEMI;
		case ':':	return COLON;
		case '.':	return DOT;
		case '+':	return PLUS;
		case '-':	return MINUS;
		case '*':	return STAR;
		case '/':	return SLASH;
		case '%':	return PERCENT;
		case '^':	return CARET;
		case '|':	return PIPE;
		case '<':	return LT;
		case '>':	return GT;
		case '=':	return EQ;
		default:	return t;
	}
}

int
plpgsql_yyparse(PLpgSQL_stmt_block **plpgsql_parse_result_p, yyscan_t yyscanner)
{
	struct GramParseExtra	extra = {
		.plpgsql_parse_result_p = plpgsql_parse_result_p,
		.yyscanner = yyscanner,
	};
	void	   *parser;
	YYSTYPE		lval;
	YYLTYPE		lloc = 0;
	int			token;

	parser = plpgsql_yyAlloc(palloc);
	while ((token = plpgsql_yylex(&lval, &lloc, yyscanner)) != 0)
	{
		plpgsql_yyLoc(parser, plpgsql_ascii_to_lime_token(token),
					  lval, lloc, &extra);
	}
	plpgsql_yyLoc(parser, 0, lval, lloc, &extra);
	plpgsql_yyFree(parser, pfree);
	return 0;
}
}
"""


def _to_nonterm(name: str) -> str:
    """Lime infers symbol class from the first character: uppercase = terminal,
    lowercase = non-terminal (the OPPOSITE of yacc).  PostgreSQL's gram.y
    has many PascalCase non-terminals (CallStmt, OptRoleList, Typename...);
    we lowercase the first letter mechanically.  Pure underscored or already
    lowercased names are returned unchanged.
    """
    if not name:
        return name
    if name[0].isupper():
        return name[0].lower() + name[1:]
    return name


def build_nt_rename(
    type_decls: "OrderedDict[str, str]",
    rule_lhs: List[str],
) -> Dict[str, str]:
    """Build a rename map for non-terminals so that, after lowercasing the
    first letter, no two distinct Bison names collide.  Where a PascalCase
    name `Foo` would collide with an already-lowercase `foo`, the PascalCase
    one gets a `_nt` suffix.
    """
    all_names: List[str] = []
    seen: Set[str] = set()
    for name in list(type_decls.keys()) + list(rule_lhs):
        if name in seen:
            continue
        seen.add(name)
        all_names.append(name)

    # Bucket by lowercased form.
    buckets: "OrderedDict[str, List[str]]" = OrderedDict()
    for name in all_names:
        lo = _to_nonterm(name)
        buckets.setdefault(lo, []).append(name)

    rename: Dict[str, str] = {}
    for lo, names in buckets.items():
        if len(names) == 1:
            rename[names[0]] = lo
            continue
        # Collision.  Keep the originally-lowercase variant as-is; for the
        # PascalCase variants append `_nt` (and disambiguate further if
        # needed).
        chosen: Set[str] = set()
        for name in names:
            if name == lo:
                rename[name] = lo
                chosen.add(lo)
        for name in names:
            if name in rename:
                continue
            candidate = lo + "_nt"
            i = 2
            while candidate in chosen:
                candidate = f"{lo}_nt{i}"
                i += 1
            rename[name] = candidate
            chosen.add(candidate)
    return rename


# ---------------------------------------------------------------------------
# Comment/string-aware utilities.
# ---------------------------------------------------------------------------

def strip_block_comments(text: str) -> str:
    """Remove /* ... */ comments while preserving newlines (so line numbers
    stay stable).  Single-quoted char literals and double-quoted strings are
    respected so we don't confuse e.g. 'a/*b*/' inside a literal.
    """
    out: List[str] = []
    i = 0
    n = len(text)
    in_dq = False
    in_sq = False
    while i < n:
        c = text[i]
        if not in_dq and not in_sq and c == "/" and i + 1 < n and text[i + 1] == "*":
            # block comment
            j = text.find("*/", i + 2)
            if j == -1:
                # unterminated; keep rest as-is
                out.append(text[i:])
                break
            out.append("\n" * text.count("\n", i, j + 2))
            i = j + 2
            continue
        if not in_dq and not in_sq and c == "/" and i + 1 < n and text[i + 1] == "/":
            j = text.find("\n", i)
            if j == -1:
                break
            i = j  # keep the newline
            continue
        if c == "\\" and i + 1 < n and (in_dq or in_sq):
            out.append(c)
            out.append(text[i + 1])
            i += 2
            continue
        if c == '"' and not in_sq:
            in_dq = not in_dq
        elif c == "'" and not in_dq:
            in_sq = not in_sq
        out.append(c)
        i += 1
    return "".join(out)


def find_balanced_brace(text: str, start: int) -> int:
    """Given text[start] == '{', return index just past the matching '}'.
    Tracks string and char literals so we don't misread embedded braces.
    """
    assert text[start] == "{"
    depth = 1
    i = start + 1
    n = len(text)
    in_dq = False
    in_sq = False
    in_block_comment = False
    in_line_comment = False
    while i < n:
        c = text[i]
        if in_block_comment:
            if c == "*" and i + 1 < n and text[i + 1] == "/":
                in_block_comment = False
                i += 2
                continue
            i += 1
            continue
        if in_line_comment:
            if c == "\n":
                in_line_comment = False
            i += 1
            continue
        if in_dq:
            if c == "\\" and i + 1 < n:
                i += 2
                continue
            if c == '"':
                in_dq = False
            i += 1
            continue
        if in_sq:
            if c == "\\" and i + 1 < n:
                i += 2
                continue
            if c == "'":
                in_sq = False
            i += 1
            continue
        if c == "/" and i + 1 < n and text[i + 1] == "*":
            in_block_comment = True
            i += 2
            continue
        if c == "/" and i + 1 < n and text[i + 1] == "/":
            in_line_comment = True
            i += 2
            continue
        if c == '"':
            in_dq = True
        elif c == "'":
            in_sq = True
        elif c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                return i + 1
        i += 1
    raise ValueError(f"Unterminated brace block starting at offset {start}")


# ---------------------------------------------------------------------------
# Section split.
# ---------------------------------------------------------------------------

@dataclass
class GrammarFile:
    prologue_c: str            # body inside the leading %{ ... %}
    declarations: str          # text between %} and first %% (directives)
    rules: str                 # text between the two %%
    epilogue: str              # text after second %%


def split_sections(text: str) -> GrammarFile:
    """Carve the .y file into its four customary regions."""
    # 1. The leading %{ ... %} prologue.  Bison allows multiple prologues; we
    #    concatenate them.
    prologue_chunks: List[str] = []
    work = text
    while True:
        m = re.search(r"%\{", work)
        if not m:
            break
        end = work.find("%}", m.end())
        if end == -1:
            raise SystemExit("ERROR: unterminated %{ ... %} prologue")
        prologue_chunks.append(work[m.end() : end])
        # remove this prologue (preserve newlines so line numbers stay sane
        # for diagnostics)
        nl = "\n" * work.count("\n", m.start(), end + 2)
        work = work[: m.start()] + nl + work[end + 2 :]
        # only chunks before the first %% are prologues
        first_double_pct = re.search(r"^%%\s*$", work, re.MULTILINE)
        if first_double_pct and m.start() > first_double_pct.start():
            break

    # 2. The two %% markers split declarations / rules / epilogue.
    pct = [m.start() for m in re.finditer(r"^%%\s*$", work, re.MULTILINE)]
    if len(pct) < 2:
        raise SystemExit(
            "ERROR: could not find both %% markers; "
            f"found {len(pct)} (need 2)."
        )
    declarations = work[: pct[0]]
    rules = work[pct[0] + 2 : pct[1]]
    epilogue = work[pct[1] + 2 :]

    return GrammarFile(
        prologue_c="".join(prologue_chunks),
        declarations=declarations,
        rules=rules,
        epilogue=epilogue,
    )


# ---------------------------------------------------------------------------
# %union body parser.
# ---------------------------------------------------------------------------

@dataclass
class UnionField:
    member: str       # e.g. "ival"
    ctype: str        # e.g. "int" or "Node *"


def parse_union(body: str) -> "OrderedDict[str, UnionField]":
    """Parse a union body and return a member-name -> UnionField map.

    Handles nested struct/union/anonymous types; brace-aware split on ';'.
    """
    fields: "OrderedDict[str, UnionField]" = OrderedDict()
    body = strip_block_comments(body)
    # Split on top-level (depth 0) ';' only.
    pieces: List[str] = []
    depth = 0
    start = 0
    for i, c in enumerate(body):
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
        elif c == ";" and depth == 0:
            pieces.append(body[start:i])
            start = i + 1
    if body[start:].strip():
        pieces.append(body[start:])

    for raw in pieces:
        decl = raw.strip()
        if not decl:
            continue
        # Last identifier (possibly preceded by '*'s) is the member name.
        m = re.match(r"^(.*?)([A-Za-z_][A-Za-z0-9_]*)\s*$", decl, re.DOTALL)
        if not m:
            continue
        type_part = m.group(1).strip()
        name = m.group(2)
        ctype = re.sub(r"\s+", " ", type_part).strip()
        fields[name] = UnionField(member=name, ctype=ctype)
    return fields


# ---------------------------------------------------------------------------
# Declaration-section parser.
# ---------------------------------------------------------------------------

@dataclass
class Declarations:
    name_prefix: str = "yy"
    expect: Optional[int] = None
    extra_args: List[str] = field(default_factory=list)   # %parse-param contents
    # When multiple %parse-param entries are folded into a single struct,
    # this carries the trailing identifiers so action bodies can be
    # rewritten to access them via extra->ident.
    parse_params_idents: List[str] = field(default_factory=list)
    union_body: str = ""
    union_fields: "OrderedDict[str, UnionField]" = field(default_factory=OrderedDict)
    type_decls: "OrderedDict[str, str]" = field(default_factory=OrderedDict)  # name -> ctype
    token_decls: "OrderedDict[str, Optional[str]]" = field(default_factory=OrderedDict)  # name -> ctype-or-None
    token_members: "OrderedDict[str, Optional[str]]" = field(default_factory=OrderedDict)  # name -> %union member or None
    type_members: "OrderedDict[str, Optional[str]]" = field(default_factory=OrderedDict)  # non-terminal name -> %union member or None
    precedence: List[Tuple[str, List[str]]] = field(default_factory=list)
    start_symbol: Optional[str] = None
    code_blocks: List[str] = field(default_factory=list)
    # %locations was set in the original .y
    has_locations: bool = False
    # Members of %type<member> across all symbols, for cross-checking.
    type_members_used: Set[str] = field(default_factory=set)
    # Bison-name -> Lime-name rename map for non-terminals (built late, after
    # rules are parsed, so collisions can be detected).
    nt_rename: Dict[str, str] = field(default_factory=dict)


def parse_declarations(text: str, decl: Declarations) -> None:
    """Walk the declaration section and fill `decl` in place."""
    text = strip_block_comments(text)

    i = 0
    n = len(text)

    def skip_ws(idx: int) -> int:
        while idx < n and text[idx] in " \t\r\n":
            idx += 1
        return idx

    def read_ident(idx: int) -> Tuple[str, int]:
        m = re.match(r"[A-Za-z_][A-Za-z0-9_]*", text[idx:])
        if not m:
            return ("", idx)
        return (m.group(0), idx + m.end())

    def read_brace_block(idx: int) -> Tuple[str, int]:
        """Read a {...} block starting at idx.  Returns (body_no_braces, end_idx)."""
        end = find_balanced_brace(text, idx)
        return (text[idx + 1 : end - 1], end)

    while i < n:
        i = skip_ws(i)
        if i >= n:
            break
        if text[i] != "%":
            # Free-floating C code is not allowed in Bison declarations
            # except inside %{...%}, which we already stripped.  Skip the
            # offending char so we make progress.
            i += 1
            continue

        # Read the directive name.
        j = i + 1
        while j < n and (text[j].isalnum() or text[j] in "-_"):
            j += 1
        directive = text[i:j]
        i = j

        if directive == "%pure-parser":
            continue
        if directive == "%locations":
            decl.has_locations = True
            continue
        if directive == "%glr-parser":
            raise SystemExit(
                "ERROR: %glr-parser is unsupported by Lime (LALR only)"
            )
        if directive == "%name-prefix":
            i = skip_ws(i)
            # both `%name-prefix=foo` and `%name-prefix "foo"` and
            # `%name-prefix foo` are accepted by Bison
            if i < n and text[i] == "=":
                i += 1
                i = skip_ws(i)
            if i < n and text[i] == '"':
                end = text.find('"', i + 1)
                if end == -1:
                    raise SystemExit("ERROR: unterminated %name-prefix string")
                decl.name_prefix = text[i + 1 : end]
                i = end + 1
            else:
                ident, i = read_ident(i)
                decl.name_prefix = ident
            continue
        if directive == "%expect":
            i = skip_ws(i)
            m = re.match(r"\d+", text[i:])
            if not m:
                raise SystemExit("ERROR: %expect missing integer count")
            decl.expect = int(m.group(0))
            i += m.end()
            continue
        if directive == "%parse-param":
            i = skip_ws(i)
            if i >= n or text[i] != "{":
                raise SystemExit("ERROR: %parse-param missing { ... }")
            body, i = read_brace_block(i)
            decl.extra_args.append(body.strip())
            continue
        if directive == "%lex-param":
            i = skip_ws(i)
            if i >= n or text[i] != "{":
                raise SystemExit("ERROR: %lex-param missing { ... }")
            _, i = read_brace_block(i)
            continue
        if directive == "%union":
            i = skip_ws(i)
            if i >= n or text[i] != "{":
                raise SystemExit("ERROR: %union missing { ... }")
            body, i = read_brace_block(i)
            decl.union_body = body
            decl.union_fields = parse_union(body)
            continue
        if directive == "%code":
            # %code { ... }   or   %code <qualifier> { ... }
            i = skip_ws(i)
            # optional qualifier
            if i < n and text[i] != "{":
                _, i = read_ident(i)
                i = skip_ws(i)
            if i >= n or text[i] != "{":
                raise SystemExit("ERROR: %code missing { ... }")
            body, i = read_brace_block(i)
            decl.code_blocks.append(body)
            continue
        if directive == "%start":
            i = skip_ws(i)
            ident, i = read_ident(i)
            decl.start_symbol = ident
            continue
        if directive in ("%left", "%right", "%nonassoc"):
            tokens, i = _read_directive_tokens(text, i)
            decl.precedence.append((directive[1:], tokens))
            # Symbols mentioned in precedence declarations are terminals,
            # even if never declared via %token (e.g. UMINUS, RIGHT_ARROW).
            for tok in tokens:
                if tok.startswith("'") and tok.endswith("'"):
                    name = CHAR_TOKEN_MAP.get(tok)
                    if name and name not in decl.token_decls:
                        decl.token_decls[name] = None
                        decl.token_members[name] = None
                else:
                    name = TOKEN_RENAME.get(tok, tok)
                    if name not in decl.token_decls:
                        decl.token_decls[name] = None
                        decl.token_members[name] = None
            continue
        if directive == "%token":
            i = skip_ws(i)
            member: Optional[str] = None
            if i < n and text[i] == "<":
                end = text.find(">", i + 1)
                if end == -1:
                    raise SystemExit("ERROR: unterminated <member> in %token")
                member = text[i + 1 : end]
                decl.type_members_used.add(member)
                i = end + 1
            tokens, i = _read_directive_tokens(text, i)
            ctype = (
                decl.union_fields[member].ctype
                if member and member in decl.union_fields
                else None
            )
            for tok in tokens:
                # Char literals are valid in %token too (rare but legal).
                if tok.startswith("'") and tok.endswith("'"):
                    name = CHAR_TOKEN_MAP.get(tok)
                    if name is None:
                        raise SystemExit(
                            f"ERROR: unmapped char literal {tok!r} in %token"
                        )
                    decl.token_decls.setdefault(name, ctype)
                    decl.token_members.setdefault(name, member)
                else:
                    name = TOKEN_RENAME.get(tok, tok)
                    decl.token_decls.setdefault(name, ctype)
                    decl.token_members.setdefault(name, member)
            continue
        if directive == "%type":
            i = skip_ws(i)
            member = None
            if i < n and text[i] == "<":
                end = text.find(">", i + 1)
                if end == -1:
                    raise SystemExit("ERROR: unterminated <member> in %type")
                member = text[i + 1 : end]
                decl.type_members_used.add(member)
                i = end + 1
            else:
                raise SystemExit(
                    "ERROR: %type without <member> is not supported by the converter"
                )
            tokens, i = _read_directive_tokens(text, i)
            if member not in decl.union_fields:
                raise SystemExit(
                    f"ERROR: %type <{member}> references unknown union member"
                )
            ctype = decl.union_fields[member].ctype
            for sym in tokens:
                # char literals never appear here
                decl.type_decls[sym] = ctype
                decl.type_members[sym] = member
            continue

        # Unknown directive: skip the rest of the line so we don't get stuck.
        nl = text.find("\n", i)
        i = nl + 1 if nl != -1 else n


def _read_directive_tokens(text: str, idx: int) -> Tuple[List[str], int]:
    """Read whitespace-separated tokens that follow a directive like
    %left/%right/%nonassoc/%token/%type up until the next %directive or EOF.

    Names continue across blank lines (Bison style); we stop at the next
    `%`-directive or the next `;` (which would be wrong here, so we keep it).
    Char literals 'x' and identifiers are both accepted.
    """
    n = len(text)
    out: List[str] = []
    while idx < n:
        # skip spaces, tabs, commas
        while idx < n and text[idx] in " \t\r\n,":
            idx += 1
        if idx >= n:
            break
        c = text[idx]
        if c == "%":
            break
        if c == "'":
            # char literal
            if idx + 2 >= n:
                break
            # Either 'x' or '\x' or '\\' etc.
            if text[idx + 1] == "\\" and idx + 3 < n:
                lit = text[idx : idx + 4]
                idx += 4
            else:
                lit = text[idx : idx + 3]
                idx += 3
            out.append(lit)
            continue
        if c.isalpha() or c == "_":
            m = re.match(r"[A-Za-z_][A-Za-z0-9_]*", text[idx:])
            assert m is not None
            out.append(m.group(0))
            idx += m.end()
            continue
        # Anything else (e.g. '<' beginning the next directive's <member>)
        # signals end of this list.
        break
    return out, idx


# ---------------------------------------------------------------------------
# Rule-section tokenizer.
# ---------------------------------------------------------------------------

@dataclass
class RuleToken:
    kind: str           # 'SYM' | 'CHAR' | 'COLON' | 'PIPE' | 'SEMI' | 'ACTION' | 'PREC'
    text: str           # symbol/char-literal/action body/prec ident
    line: int           # 1-based for diagnostics


def tokenize_rules(text: str) -> List[RuleToken]:
    out: List[RuleToken] = []
    i = 0
    n = len(text)
    line = 1
    while i < n:
        c = text[i]
        if c == "\n":
            line += 1
            i += 1
            continue
        if c in " \t\r":
            i += 1
            continue
        if c == "/" and i + 1 < n and text[i + 1] == "*":
            j = text.find("*/", i + 2)
            if j == -1:
                break
            line += text.count("\n", i, j + 2)
            i = j + 2
            continue
        if c == "/" and i + 1 < n and text[i + 1] == "/":
            j = text.find("\n", i)
            i = j if j != -1 else n
            continue
        if c == "'":
            # char literal: '\?.' or '.'
            if i + 1 < n and text[i + 1] == "\\" and i + 3 < n:
                lit = text[i : i + 4]
                i += 4
            elif i + 2 < n:
                lit = text[i : i + 3]
                i += 3
            else:
                break
            out.append(RuleToken("CHAR", lit, line))
            continue
        if c == "{":
            end = find_balanced_brace(text, i)
            body = text[i + 1 : end - 1]
            out.append(RuleToken("ACTION", body, line))
            line += text.count("\n", i, end)
            i = end
            continue
        if c == ":":
            out.append(RuleToken("COLON", ":", line))
            i += 1
            continue
        if c == "|":
            out.append(RuleToken("PIPE", "|", line))
            i += 1
            continue
        if c == ";":
            out.append(RuleToken("SEMI", ";", line))
            i += 1
            continue
        if text.startswith("%prec", i):
            j = i + 5
            while j < n and text[j] in " \t":
                j += 1
            mm = re.match(r"[A-Za-z_][A-Za-z0-9_]*", text[j:])
            if not mm:
                # could still be a char literal as the prec marker
                if j < n and text[j] == "'" and j + 2 < n:
                    lit = text[j : j + 3]
                    out.append(RuleToken("PREC", lit, line))
                    i = j + 3
                    continue
                raise SystemExit(
                    f"ERROR: %prec without identifier at line {line}"
                )
            out.append(RuleToken("PREC", mm.group(0), line))
            i = j + mm.end()
            continue
        if text.startswith("%empty", i):
            # treat as a no-op marker
            i += 6
            continue
        if c.isalpha() or c == "_":
            m = re.match(r"[A-Za-z_][A-Za-z0-9_]*", text[i:])
            assert m is not None
            out.append(RuleToken("SYM", m.group(0), line))
            i += m.end()
            continue
        # Skip stray punctuation
        i += 1
    return out


# ---------------------------------------------------------------------------
# Rule parser.
# ---------------------------------------------------------------------------

@dataclass
class RhsElement:
    """One position on the RHS.  Either a symbol reference or an inline
    action that the converter has lifted into a helper non-terminal.
    """
    kind: str              # 'SYM' | 'CHAR' | 'MIDACT'
    text: str              # symbol name / char literal / helper-NT name


@dataclass
class Alternative:
    rhs: List[RhsElement] = field(default_factory=list)
    action: str = ""
    action_line: int = 0
    prec: Optional[str] = None
    # When mid-rule actions were lifted, we may need the reverse map for
    # action $N rewriting.  Specifically: if the original Bison RHS had
    # symbols [a, {act}, b, {act}, c], we created two helper NTs and the
    # final RHS becomes [a, _midNNN, b, _midMMM, c].  $1=a, $2=helper,
    # $3=b, $4=helper, $5=c.  So Bison's $N indexes the original positions
    # one-to-one with rhs[].
    line: int = 0


@dataclass
class Rule:
    lhs: str
    alternatives: List[Alternative] = field(default_factory=list)
    line: int = 0


@dataclass
class HelperRule:
    name: str
    action: str
    action_line: int
    # Set by restructure_midrules() when this helper references parent
    # prefix positions and needs to capture them.  prefix_rhs is the
    # parent rule's RHS slice [0..midact_idx-1] that becomes the
    # helper's own RHS.  prefix_refs is a set of (kind, n, member-or-None)
    # tuples describing what the helper's action body and the parent's
    # final action need to access from the prefix.
    prefix_rhs: "Optional[List[RhsElement]]" = None
    prefix_refs: "Optional[set]" = None


def _rewrite_prefix_refs_in_action(action: str, prefix_size: int,
                                  helper_name: str) -> str:
    """Rewrite $N/@N references in `action` where N <= prefix_size to
    use the per-helper scratchpad globals.  String literals are
    skipped.  Returns the rewritten action."""
    out = []
    i = 0
    n_text = len(action)
    while i < n_text:
        c = action[i]
        if c == '"' or c == "'":
            quote = c
            start = i
            i += 1
            while i < n_text:
                if action[i] == "\\":
                    i += 2
                    continue
                if action[i] == quote:
                    i += 1
                    break
                i += 1
            out.append(action[start:i])
            continue
        if c == "$" and i + 1 < n_text and action[i + 1].isdigit():
            m = re.match(r"\$(\d+)(?:\.([A-Za-z_][A-Za-z0-9_]*))?", action[i:])
            if m:
                n = int(m.group(1))
                if 1 <= n <= prefix_size:
                    member = m.group(2)
                    if member:
                        out.append(f"_midshim_{helper_name}_arg{n}_val.{member}")
                    else:
                        out.append(f"_midshim_{helper_name}_arg{n}_val")
                    i += m.end()
                    continue
        if c == "@" and i + 1 < n_text and action[i + 1].isdigit():
            m = re.match(r"@(\d+)", action[i:])
            if m:
                n = int(m.group(1))
                if 1 <= n <= prefix_size:
                    out.append(f"_midshim_{helper_name}_arg{n}_loc")
                    i += m.end()
                    continue
        out.append(c)
        i += 1
    return "".join(out)


def _shift_position_refs(action: str, prefix_size: int) -> str:
    """After replacing prefix+midact (prefix_size+1 slots) with a single
    helper at position 1, $N references for N > prefix_size shift down
    by prefix_size.  String literals are skipped."""
    out = []
    i = 0
    n_text = len(action)
    while i < n_text:
        c = action[i]
        if c == '"' or c == "'":
            quote = c
            start = i
            i += 1
            while i < n_text:
                if action[i] == "\\":
                    i += 2
                    continue
                if action[i] == quote:
                    i += 1
                    break
                i += 1
            out.append(action[start:i])
            continue
        if c == "$" and i + 1 < n_text and action[i + 1].isdigit():
            m = re.match(r"\$(\d+)(\.[A-Za-z_][A-Za-z0-9_]*)?", action[i:])
            if m:
                n = int(m.group(1))
                tail = m.group(2) or ""
                if n > prefix_size:
                    out.append(f"${n - prefix_size}{tail}")
                    i += m.end()
                    continue
        if c == "@" and i + 1 < n_text and action[i + 1].isdigit():
            m = re.match(r"@(\d+)", action[i:])
            if m:
                n = int(m.group(1))
                if n > prefix_size:
                    out.append(f"@{n - prefix_size}")
                    i += m.end()
                    continue
        out.append(c)
        i += 1
    return "".join(out)


def _resolve_symbol_type(elem,
                        decl: "Declarations") -> str:
    """Look up the C type of an RHS symbol.  Returns the %type ctype
    for non-terminals, the %union member's ctype for terminals (where
    declared), or 'YYSTYPE' as a catch-all.  Used for sizing
    scratchpad globals in the restructured-helper emission path."""
    if elem is None:
        return "YYSTYPE"
    if elem.kind == "SYM":
        renamed = TOKEN_RENAME.get(elem.text, elem.text)
        if renamed in decl.type_decls:
            ct = decl.type_decls[renamed]
            if ct:
                return ct
        if elem.text in decl.type_decls:
            ct = decl.type_decls[elem.text]
            if ct:
                return ct
        if renamed in decl.token_decls:
            ct = decl.token_decls.get(renamed)
            if ct:
                return ct
            member = decl.token_members.get(renamed)
            if member and member in decl.union_fields:
                return decl.union_fields[member].ctype
        if elem.text in decl.token_decls:
            ct = decl.token_decls.get(elem.text)
            if ct:
                return ct
            member = decl.token_members.get(elem.text)
            if member and member in decl.union_fields:
                return decl.union_fields[member].ctype
    elif elem.kind == "CHAR":
        mapped = CHAR_TOKEN_MAP.get(elem.text)
        if mapped:
            return _resolve_symbol_type(
                RhsElement(kind="SYM", text=mapped), decl
            )
    return "YYSTYPE"


def collect_prefix_refs(action: str, prefix_size: int) -> set:
    """Return the set of (kind, n, member) tuples for $N/@N references
    in `action` where 1 <= n <= prefix_size.  kind is 'val' for $N,
    'loc' for @N.  member is None for @N or for plain $N; for $N.field
    references it's the field name.  String literals are skipped
    (mirrors the rewrite_action _rewrite_outside_strings logic).
    """
    refs = set()
    i = 0
    n_text = len(action)
    while i < n_text:
        c = action[i]
        if c == '"' or c == "'":
            quote = c
            i += 1
            while i < n_text:
                if action[i] == "\\":
                    i += 2
                    continue
                if action[i] == quote:
                    i += 1
                    break
                i += 1
            continue
        if c == "$" and i + 1 < n_text and action[i + 1].isdigit():
            m = re.match(r"\$(\d+)(?:\.([A-Za-z_][A-Za-z0-9_]*))?", action[i:])
            if m:
                n = int(m.group(1))
                if 1 <= n <= prefix_size:
                    refs.add(("val", n, m.group(2)))
                i += m.end()
                continue
        if c == "@" and i + 1 < n_text and action[i + 1].isdigit():
            m = re.match(r"@(\d+)", action[i:])
            if m:
                n = int(m.group(1))
                if 1 <= n <= prefix_size:
                    refs.add(("loc", n, None))
                i += m.end()
                continue
        i += 1
    return refs


def restructure_midrules(rules: "List[Rule]", helpers: "List[HelperRule]",
                        decl: "Declarations") -> None:
    """Detect mid-rule actions that reference parent prefix positions
    and restructure them so the values are accessible.

    Strategy: the helper takes the prefix as its own RHS.  Both the
    helper's action body and the parent's final action body get
    rewritten to read prefix values from per-helper scratchpad globals
    (one var per (kind, n) tuple referenced).  This avoids per-helper
    struct typedefs at the cost of declaring a few file-static vars --
    acceptable for ecpg's single-threaded parser.

    Helpers whose action body and parent's final action don't reference
    prefix positions are left as empty hoists (the original strategy).

    Restricted to the FIRST mid-rule action per alternative -- supports
    ecpg's pattern; multi-midact-with-refs would need a recursive pass.
    """
    helper_by_name = {h.name: h for h in helpers}
    for rule in rules:
        for alt in rule.alternatives:
            for midact_idx, elem in enumerate(alt.rhs):
                if elem.kind != "MIDACT":
                    continue
                helper = helper_by_name.get(elem.text)
                if helper is None:
                    continue
                prefix_size = midact_idx
                if prefix_size == 0:
                    continue  # nothing to capture
                refs = collect_prefix_refs(helper.action, prefix_size)
                refs |= collect_prefix_refs(alt.action, prefix_size)
                if not refs:
                    continue
                # Mark helper for restructured emission.  Capture the
                # prefix RHS so the emitter knows what RHS to give the
                # helper, plus the union of refs from both action
                # bodies so it knows which scratchpad globals to set.
                helper.prefix_rhs = list(alt.rhs[:midact_idx])
                helper.prefix_refs = refs
                helper.action = _rewrite_prefix_refs_in_action(
                    helper.action, prefix_size, helper.name)
                # Rewrite alt.action: prefix refs to scratchpad,
                # remaining position refs shifted down by prefix_size.
                alt.action = _rewrite_prefix_refs_in_action(
                    alt.action, prefix_size, helper.name)
                alt.action = _shift_position_refs(alt.action, prefix_size)
                # Modify alt.rhs: replace prefix + midact with the helper.
                helper_elem = RhsElement(kind="SYM", text=helper.name)
                alt.rhs = [helper_elem] + alt.rhs[midact_idx + 1:]
                # Done with this alt's first restructure-needed midact.
                # Subsequent midacts (if any) are at later positions in
                # the new rhs; the loop above won't re-process them
                # because we break here.  Multi-midact-with-refs grammars
                # would need this generalized.
                break


def parse_rules(tokens: List[RuleToken]) -> Tuple[List[Rule], List[HelperRule]]:
    """Parse tokenized rule section.  Returns (rules, helpers)."""
    rules: List[Rule] = []
    helpers: List[HelperRule] = []
    helper_counter = 0

    i = 0
    while i < len(tokens):
        # Find LHS pattern: SYM COLON
        if (
            i + 1 < len(tokens)
            and tokens[i].kind == "SYM"
            and tokens[i + 1].kind == "COLON"
        ):
            lhs = tokens[i].text
            rule = Rule(lhs=lhs, line=tokens[i].line)
            i += 2

            # Track whether we just consumed a `|` so we can detect a
            # trailing empty alternative (`foo: A |;`).  The outer-loop
            # exit-on-SEMI condition would otherwise miss it.
            just_consumed_pipe = False
            while i < len(tokens) and (
                tokens[i].kind != "SEMI" or just_consumed_pipe
            ):
                # Bison allows a missing ';' between rules; detect the start
                # of a new rule (SYM COLON) and stop.
                if (
                    tokens[i].kind == "SYM"
                    and i + 1 < len(tokens)
                    and tokens[i + 1].kind == "COLON"
                ):
                    break
                just_consumed_pipe = False
                alt = Alternative()
                alt.line = tokens[i].line
                # Read symbols/actions/prec until PIPE/SEMI.
                # Track pending actions; if a non-final action is seen,
                # lift it into a helper rule.
                while i < len(tokens) and tokens[i].kind not in ("PIPE", "SEMI"):
                    # Stop on a new rule's start so we don't gobble it.
                    if (
                        tokens[i].kind == "SYM"
                        and i + 1 < len(tokens)
                        and tokens[i + 1].kind == "COLON"
                    ):
                        break
                    tok = tokens[i]
                    if tok.kind in ("SYM", "CHAR"):
                        alt.rhs.append(RhsElement(kind=tok.kind, text=tok.text))
                        i += 1
                        continue
                    if tok.kind == "PREC":
                        alt.prec = tok.text
                        i += 1
                        continue
                    if tok.kind == "ACTION":
                        # Look ahead: if next non-space is PIPE/SEMI, this is
                        # the final action.  Otherwise it's a mid-rule action.
                        # Also treat the start of a new rule (SYM COLON) and
                        # end-of-input as final.
                        nxt_kind = tokens[i + 1].kind if i + 1 < len(tokens) else "EOF"
                        nxt2_kind = tokens[i + 2].kind if i + 2 < len(tokens) else "EOF"
                        nxt_starts_rule = (
                            nxt_kind == "SYM"
                            and i + 2 < len(tokens)
                            and tokens[i + 2].kind == "COLON"
                        )
                        is_final = (
                            nxt_kind in ("PIPE", "SEMI", "EOF")
                            or nxt_starts_rule
                            or (
                                nxt_kind == "PREC"
                                and nxt2_kind in ("PIPE", "SEMI", "EOF")
                            )
                        )
                        if is_final:
                            alt.action = tok.text
                            alt.action_line = tok.line
                            i += 1
                        else:
                            helper_counter += 1
                            helper_name = f"midact{helper_counter}"
                            helpers.append(
                                HelperRule(
                                    name=helper_name,
                                    action=tok.text,
                                    action_line=tok.line,
                                )
                            )
                            alt.rhs.append(
                                RhsElement(kind="MIDACT", text=helper_name)
                            )
                            i += 1
                        continue
                    # Unknown token in rule body
                    i += 1

                rule.alternatives.append(alt)
                if i < len(tokens) and tokens[i].kind == "PIPE":
                    i += 1
                    just_consumed_pipe = True
                    continue
                # otherwise SEMI -> break

            # consume SEMI
            if i < len(tokens) and tokens[i].kind == "SEMI":
                i += 1
            rules.append(rule)
        else:
            i += 1
    return rules, helpers


# ---------------------------------------------------------------------------
# Action rewriter.
# ---------------------------------------------------------------------------

def _label_for_index(idx: int) -> str:
    """RHS index (1-based) -> letter label.  $1->B, $2->C, ...
    A is reserved for $$ (LHS).  After Z we wrap to two-letter labels.
    """
    if idx <= 0:
        raise ValueError("$0 is not allowed")
    # Use 'B' (1) ... 'Z' (25), then 'BA','BB',... only if needed.
    # 25 single letters cover 99% of rules.
    letters = "BCDEFGHIJKLMNOPQRSTUVWXYZ"
    if idx <= len(letters):
        return letters[idx - 1]
    # fallback: P{idx}
    return f"P{idx}"


# Matches one leading C declaration line inside an action body, e.g.
#   "PLpgSQL_stmt_if *new;"           -> bare declaration
#   "int tok = plpgsql_yylex(...);"   -> declaration with initialiser
# Captures: (indent)(type+declarator)(= initialiser)?(;)
_LEADING_DECL_RE = re.compile(
    r"^([ \t]*)"                                  # indent
    r"((?:[A-Za-z_][A-Za-z0-9_]*\b[ \t\*]*)+?"     # type words + stars
    r"\*?[A-Za-z_][A-Za-z0-9_]*"                   # declarator name
    r"(?:\[[^\]]*\])?)"                            # optional array bound
    r"[ \t]*(=[ \t]*[^;]+?)?[ \t]*;[ \t]*$"        # optional initialiser, ;
)


def _hoist_leading_decls(action: str) -> Tuple[str, List[str]]:
    """Split the leading C declarations out of an action body.

    The converter injects a setup prologue (parse-param locals, (void)
    casts, yylval/yylloc init, drain_lookahead) ahead of the user action
    body.  PostgreSQL's grammar action bodies declare their locals first
    (declarations-before-statements / -Wdeclaration-after-statement), so
    those declarations must be emitted alongside the prologue's own
    declarations -- otherwise they land after the injected statements and
    trip the warning.

    Returns (rewritten_body, hoisted_decls):
      * hoisted_decls is a list of bare declaration strings
        ("PLpgSQL_var *new", "int tok") to emit with the prologue decls.
      * A declaration with an initialiser is split: the bare declaration
        is hoisted, and an assignment ("name = init;") is left at the top
        of the body so any dependency on the prologue (e.g. yylval set by
        drain_lookahead) is still honoured.
      * Scanning stops at the first non-declaration, non-blank line, so
        only the contiguous leading declaration block is touched.
    """
    lines = action.split("\n")
    hoisted: List[str] = []
    body_lines: List[str] = []
    i = 0
    consuming = True
    while i < len(lines):
        line = lines[i]
        if consuming and line.strip() == "":
            i += 1
            continue
        m = _LEADING_DECL_RE.match(line) if consuming else None
        if m:
            indent, declarator, initialiser = m.group(1), m.group(2), m.group(3)
            # Normalise internal whitespace in the declarator for the
            # hoisted form but keep it readable.
            hoisted.append(declarator.strip())
            if initialiser:
                # Leave "name = init;" in the body.  The declarator's
                # last identifier token is the variable name.
                name = re.split(r"[ \t\*]+", declarator.strip())[-1]
                name = name.split("[", 1)[0]
                body_lines.append(f"{indent}{name} {initialiser};")
            i += 1
            continue
        consuming = False
        body_lines.append(line)
        i += 1
    return ("\n".join(body_lines), hoisted)


def rewrite_action(
    action: str,
    rhs_len: int,
    rhs: Optional[List["RhsElement"]] = None,
    decl: Optional["Declarations"] = None,
    lhs_member: Optional[str] = None,
    lhs_is_token: bool = False,
) -> str:
    """Rewrite Bison action body to Lime equivalent.

    - $$  -> A   (or A.<member> if LHS is a token, which is rare)
    - $N  -> letter label for position N
             (or letter.<member> if RHS[N-1] is a token whose %union
             member resolves to a primitive type; non-terminals stay
             bare because their %type declaration sets the C type)
    - @$  -> @A   (Lime reduce-action @<label> macro; project glue may
                   `#define` LOC(L) to expand to (L)-th location)
    - @N  -> @<label>   (locations are always int -- no member access)
    - $<member>$ / $<member>N -> A / label.<member>  (typed refs are
                                                      preserved with the
                                                      explicit member)
    """

    def _member_for_position(n: int) -> Optional[str]:
        """Return the %union member name for RHS position n (1-based)
        if it is a token with a typed value, else None."""
        if rhs is None or decl is None:
            return None
        if n < 1 or n > len(rhs):
            return None
        elem = rhs[n - 1]
        if elem.kind == "CHAR":
            # Char literal -> token name via CHAR_TOKEN_MAP.
            mapped = CHAR_TOKEN_MAP.get(elem.text)
            return decl.token_members.get(mapped) if mapped else None
        if elem.kind == "MIDACT":
            return None
        # SYM: apply TOKEN_RENAME and look up.  Bison may have used
        # camelCase token names that we mapped to UPPER (e.g. Op->OP).
        renamed = TOKEN_RENAME.get(elem.text, elem.text)
        if renamed in decl.token_decls:
            return decl.token_members.get(renamed)
        if elem.text in decl.token_decls:
            return decl.token_members.get(elem.text)
        return None

    # Typed forms first: $<m>$ and $<m>N -- preserve the explicit member.
    def _typed(m: re.Match) -> str:
        member = m.group(1)
        rest = m.group(2)
        if rest == "$":
            # $<m>$: LHS access with explicit member.
            return f"A.{member}" if lhs_is_token else "A"
        n = int(rest)
        label = _label_for_index(n)
        if rhs is None or decl is None:
            return label
        # Only attach .member when the position is a token; for typed
        # non-terminals the type IS the member's C type already.
        if n >= 1 and n <= len(rhs):
            elem = rhs[n - 1]
            if elem.kind == "SYM" and elem.text in decl.token_decls:
                return f"{label}.{member}"
        return label

    action = re.sub(r"\$<([A-Za-z_][A-Za-z0-9_]*)>(\$|\d+)", _typed, action)

    # @N and @$ are now passed through verbatim.  With %locations and
    # %location_type {YYLTYPE} declared at the top of the grammar, Lime
    # expands @<rhs-alias> to yymsp[i].yyloc and @$ / @<lhs-alias> to
    # the LHS slot's yyloc (set by the post-reduce YYLLOC_DEFAULT step).
    # See Lime upstream commit 0384bfe (P0-NEW-2).
    def _at(m: re.Match) -> str:
        n = int(m.group(1))
        if n == 0:
            return "@A"
        return f"@{_label_for_index(n)}"

    # String-aware $N / @N substitution.  Action bodies sometimes
    # contain `$0` or `@N` inside C string literals (notably ecpg
    # uses cursor markers like "$0" in output text); we mustn't
    # rewrite those.  Walk the string token-by-token, only rewriting
    # outside of `"..."` and `'...'` literals.
    def _rewrite_outside_strings(s: str, pattern: str, repl) -> str:
        out = []
        i = 0
        n = len(s)
        while i < n:
            c = s[i]
            if c == '"' or c == "'":
                # copy string literal verbatim
                quote = c
                start = i
                i += 1
                while i < n:
                    if s[i] == "\\":
                        i += 2
                        continue
                    if s[i] == quote:
                        i += 1
                        break
                    i += 1
                out.append(s[start:i])
            else:
                # scan ahead until next quote, regex-substitute that chunk
                start = i
                while i < n and s[i] != '"' and s[i] != "'":
                    i += 1
                out.append(re.sub(pattern, repl, s[start:i]))
        return "".join(out)

    action = _rewrite_outside_strings(action, r"@(\d+)", _at)
    action = action.replace("@$", "@A")

    def _dollar(m: re.Match) -> str:
        n = int(m.group(1))
        label = _label_for_index(n)
        member = _member_for_position(n)
        if member is None:
            return label
        return f"{label}.{member}"

    action = _rewrite_outside_strings(action, r"\$(\d+)", _dollar)
    if lhs_is_token and lhs_member:
        action = re.sub(r"\$\$", f"A.{lhs_member}", action)
    else:
        action = re.sub(r"\$\$", "A", action)

    # Strip Bison-internal idioms that are meaningless under Lime.
    # `(void) yynerrs;` was used in gram.y to suppress unused-variable
    # warnings on Bison's parser-state error counter; Lime's generated
    # parser has no such variable.
    action = re.sub(
        r"^\s*\(void\)\s*yynerrs\s*;[^\n]*\n?", "", action,
        flags=re.MULTILINE,
    )

    # When the converter folded multiple %parse-param entries into a
    # single struct (extra->...), the plpgsql action-body bison-ism
    # cleanup below also handles the parse-param ident aliasing in the
    # injected local-decl block; for grammars without that bison-ism
    # cleanup (i.e. the backend grammar), action bodies don't reference
    # the parse-param idents directly so no rewrite is needed.
    pass

    # plpgsql-specific: rewrite bison-isms that the original gram.y
    # action bodies use.  Bison provides yylval/yylloc as parser-state
    # references inside actions; Lime push-parser doesn't.  The
    # plpgsql idiom is to call helpers like read_sql_expression(&yylval,
    # &yylloc, yyscanner) which use those as scratch -- a local
    # YYSTYPE/YYLTYPE shadow at action-block scope works fine.
    #
    # Other bison-internals get translated:
    #   yyerror(...)   -> plpgsql_yyerror(...)
    #   yylex(...)     -> plpgsql_yylex(...)
    #   yychar         -> plpgsql_yy_get_lookahead(yypParser, &yylval, &yylloc)
    #                     (Lime upstream a9706ad shipped P0-NEW-5.  The
    #                     parser exposes its in-flight lookahead via the
    #                     prefix-renamed Parse_get_lookahead helper.)
    #   yyclearin;     -> plpgsql_yy_clear_lookahead(yypParser);
    #                     (matches bison's yyclearin semantics: tell the
    #                     parser the action consumed the lookahead so the
    #                     dispatch loop skips the trailing shift.)
    #   YYEMPTY        -> stays; the prologue will #define it to -2.
    #
    # Also alias the original %parse-param idents (yyscanner,
    # plpgsql_parse_result_p) as locals at the start of each action
    # that uses them, so existing actions and prologue macros (which
    # reference these idents directly, not via extra->...) keep
    # compiling unchanged.
    if decl is not None and decl.name_prefix == "plpgsql_yy":
        used_yylval = re.search(r"(?<![A-Za-z0-9_])yylval(?![A-Za-z0-9_])", action) is not None
        used_yylloc = re.search(r"(?<![A-Za-z0-9_])yylloc(?![A-Za-z0-9_])", action) is not None
        used_yychar = re.search(r"(?<![A-Za-z0-9_])yychar(?![A-Za-z0-9_])", action) is not None
        # Detect actions that consume tokens via plpgsql's lex helpers.
        # These need Lime's pending lookahead pushed back onto the
        # lexer pushback stack first, otherwise they'll skip past it
        # (Lime's push-model means the lookahead reached Parse() before
        # the action body; bison's pull-model would have left it for
        # the action's first yylex call).  Check the ORIGINAL action
        # body before yylex->plpgsql_yylex rewrite below.
        uses_lex_helpers = bool(re.search(
            r"(?<![A-Za-z0-9_])(read_sql_construct|read_sql_stmt|"
            r"read_sql_expression(?:2)?|read_datatype|read_raise_options|"
            r"read_into_target|read_fetch_direction|read_using_list|"
            r"make_execsql_stmt|make_return_(?:next_)?(?:query_)?stmt|"
            r"make_case|make_scalar_assign_stmt|yylex|plpgsql_yylex|"
            r"plpgsql_peek)"
            r"(?![A-Za-z0-9_])",
            action,
        ))
        action = re.sub(r"(?<![A-Za-z0-9_])yyerror(?=\s*\()", "plpgsql_yyerror", action)
        action = re.sub(r"(?<![A-Za-z0-9_])yylex(?=\s*\()", "plpgsql_yylex", action)
        # yychar -> plpgsql_yy_get_lookahead(...).  The action body has
        # &yylval/&yylloc local already (we inject locals below); the
        # Lime parser handle is yypParser, exposed by the template.
        action = re.sub(
            r"(?<![A-Za-z0-9_])yychar(?![A-Za-z0-9_])",
            "plpgsql_yy_get_lookahead(yypParser, &yylval, &yylloc)",
            action,
        )
        action = re.sub(
            r"(?<![A-Za-z0-9_])yyclearin\s*;",
            "plpgsql_yy_clear_lookahead(yypParser);",
            action,
        )
        # If the action used yychar, force injection of yylval/yylloc
        # locals -- the rewritten get_lookahead call needs them.
        if used_yychar:
            used_yylval = True
            used_yylloc = True
        # Compute which parse-param idents are referenced; emit locals
        # for those (and yylval/yylloc).  Always inject if extra_args
        # exists, so prologue macros that reference these idents (e.g.
        # parser_errposition uses yyscanner) expand correctly inside
        # any action.
        decls = []
        param_idents = []
        if getattr(decl, "parse_params_idents", None):
            for ident in decl.parse_params_idents:
                # Look up the original full declarator from extra_args.
                for arg in decl.extra_args:
                    a = arg.strip()
                    if a.split()[-1].lstrip("*[]") == ident or a.endswith(ident):
                        decls.append(f"{a} = extra->{ident}")
                        param_idents.append(ident)
                        break
        if used_yylval:
            decls.append("YYSTYPE\tyylval")
        if used_yylloc:
            decls.append("YYLTYPE\tyylloc")
        if decls:
            # Suppress unused-variable warnings on the parse-param
            # locals (the action body may not actually reference them;
            # they're injected to make prologue macros expand correctly).
            voids = "; ".join(f"(void){i}" for i in param_idents)
            tail = (";\n\t" + voids + ";") if voids else ""
            # Initialise yylval/yylloc to mirror Bison's runtime
            # semantics:
            #   - empty rule: lookahead is in-flight (Bison can't
            #     default-reduce empty rules without one).  yylval =
            #     lookahead's value -- that's what plpgsql_yy_get_lookahead
            #     returns.
            #   - non-empty rule: Bison's yylval was the most-recent
            #     yylex output, which (when the parser default-reduces
            #     without pulling a lookahead) is the LAST shifted RHS
            #     symbol's value, i.e. $rhs_len.  Lime's push-model
            #     can't replicate this exactly because by the time the
            #     action runs Lime has already consumed the next token
            #     as a lookahead.  We approximate: init yylloc from
            #     @B (the FIRST RHS symbol's location, which is what
            #     plpgsql_push_back_token cares about for source-text
            #     slicing); yylval stays uninit (action helpers either
            #     overwrite it via yylex out-params or don't read it).
            init = ""
            if rhs_len == 0:
                if used_yylval and used_yylloc:
                    init = ("\n\t(void) plpgsql_yy_get_lookahead(yypParser, "
                            "&yylval, &yylloc);")
                elif used_yylval:
                    init = ("\n\t(void) plpgsql_yy_get_lookahead(yypParser, "
                            "&yylval, NULL);")
                elif used_yylloc:
                    init = ("\n\t(void) plpgsql_yy_get_lookahead(yypParser, "
                            "NULL, &yylloc);")
            else:
                if used_yylloc:
                    init = "\n\tyylloc = @B;"
                if used_yylval:
                    init += "\n\tmemset(&yylval, 0, sizeof(yylval));"
            if uses_lex_helpers and not used_yychar:
                # Drain Lime's pending lookahead into the lexer's
                # pushback stack so the action's first plpgsql_yylex
                # call returns it.  Skip when the action also uses
                # yychar -- yychar uses the lookahead directly via
                # plpgsql_yy_get_lookahead and the Lime lookahead
                # is consumed by an explicit clear_lookahead call.
                init += ("\n\t(void) plpgsql_yy_drain_lookahead("
                        "yypParser, extra->yyscanner);")
            body, hoisted = _hoist_leading_decls(action.rstrip())
            all_decls = decls + hoisted
            action = (
                "{ " + "; ".join(all_decls) + ";" + tail + init + "\n"
                + body
                + "\n}"
            )

    return action


def _inject_parse_param_locals(action: str, decl: "Declarations") -> str:
    """For grammars with multiple %parse-param decls folded into a
    struct GramParseExtra, inject locals at the top of each action
    body so original action code that references the parse-param
    names (e.g. `result`, `escontext`) compiles unchanged.

    Used by non-plpgsql grammars (cube, etc.).  plpgsql_yy has its
    own pathway above that also handles yychar/yyerror rewrites and
    drain_lookahead injection.
    """
    if (decl is None
        or decl.name_prefix == "plpgsql_yy"
        or not getattr(decl, "parse_params_idents", None)
        or len(decl.parse_params_idents) < 2):
        return action
    decls = []
    param_idents = []
    for ident in decl.parse_params_idents:
        if not re.search(rf"(?<![A-Za-z0-9_]){re.escape(ident)}(?![A-Za-z0-9_])", action):
            continue
        for arg in decl.extra_args:
            a = arg.strip()
            if a.split()[-1].lstrip("*[]") == ident or a.endswith(ident):
                decls.append(f"{a} = extra->{ident}")
                param_idents.append(ident)
                break
    if not decls:
        return action
    voids = "; ".join(f"(void){i}" for i in param_idents)
    tail = (";\n\t" + voids + ";") if voids else ""
    body, hoisted = _hoist_leading_decls(action.rstrip())
    all_decls = decls + hoisted
    return (
        "{ " + "; ".join(all_decls) + ";" + tail + "\n"
        + body
        + "\n}"
    )


# ---------------------------------------------------------------------------
# Lime emission.
# ---------------------------------------------------------------------------

def map_token_name(name: str) -> str:
    if name.startswith("'") and name.endswith("'"):
        mapped = CHAR_TOKEN_MAP.get(name)
        if mapped is None:
            raise SystemExit(f"ERROR: unmapped char literal {name!r}")
        return mapped
    return TOKEN_RENAME.get(name, name)


def is_terminal(name: str, decl: Declarations) -> bool:
    """Heuristic: a name is a terminal if it was declared via %token."""
    return name in decl.token_decls


def map_nt_or_token(name: str, decl: Declarations) -> str:
    """Rewrite a Bison symbol name into its Lime spelling, picking
    the right side of the "first letter" convention based on whether the
    symbol was declared as a token in %token or %left/%right/%nonassoc.
    """
    renamed = TOKEN_RENAME.get(name, name)
    if renamed in decl.token_decls:
        return renamed
    # Otherwise it's a non-terminal (or an unknown symbol; lowercase it
    # to make it a non-terminal in Lime's eyes).
    if name in decl.nt_rename:
        return decl.nt_rename[name]
    return _to_nonterm(renamed)


def format_rhs(elem: RhsElement, idx: int, decl: Declarations) -> Tuple[str, str]:
    """Format one Lime RHS atom; return (formatted, label_letter).
    The label is always assigned (positionally) so that action rewriting
    aligns even when the symbol has no semantic value.
    """
    label = _label_for_index(idx)
    if elem.kind == "CHAR":
        sym = map_token_name(elem.text)
        return (f"{sym}({label})", label)
    if elem.kind == "MIDACT":
        return (f"{elem.text}({label})", label)
    sym = map_nt_or_token(elem.text, decl)
    return (f"{sym}({label})", label)


def emit_lime(decl: Declarations, rules: List[Rule], helpers: List[HelperRule],
              prologue_c: str, source_path: str,
              epilogue_c: str = "", driver_block: Optional[str] = None,
              yyerror_fn: Optional[str] = None,
              empty_loc: Optional[str] = None) -> str:
    out: List[str] = []
    add = out.append

    # Header
    add("/*-------------------------------------------------------------------------")
    add(" *")
    add(" * gram.lime")
    add(" *\t  Lime grammar for the PostgreSQL backend SQL parser.")
    add(" *")
    add(" * Mechanically converted from " + source_path + " by")
    add(" * src/tools/lime_convert_gram.py.  Hand edits are expected to follow")
    add(" * for precedence/conflict tuning and scanner glue.")
    add(" *")
    add(" * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group")
    add(" * Portions Copyright (c) 1994, Regents of the University of California")
    add(" *")
    add(" *-------------------------------------------------------------------------")
    add(" */")
    add("")

    # Reverse-converter aid: emit the non-terminal rename map (Bison
    # name -> Lime name).  Lime infers symbol class from the first
    # character (uppercase = terminal, lowercase = non-terminal), the
    # opposite of yacc; the converter flipped Bison's PascalCase NT
    # names like CallStmt -> callStmt.  Without preserving this map,
    # a reverse converter (lime_to_bison_gram.py) can't know that
    # `typename` was originally `Typename`.  Emitted as a magic
    # comment block; ignored by lime, parsed by lime_to_bison_gram.py.
    if decl.nt_rename:
        add("/* lime_to_bison_gram nt_rename map -- DO NOT EDIT BY HAND.")
        add(" * Each line: <bison-name> -> <lime-name>.")
        # Sort by bison name for deterministic output.
        for bison_name in sorted(decl.nt_rename.keys()):
            lime_name = decl.nt_rename[bison_name]
            if bison_name != lime_name:
                add(f" * {bison_name} -> {lime_name}")
        add(" */")
        add("")

    # %name
    add(f"%name {decl.name_prefix}")

    # %expect (Lime 0.1.0 accepts no terminating period for this directive in
    # this tree's pinned build; the docs say `%expect N.` but landed Lime
    # grammars use the no-period form, so we follow them.)
    if decl.expect is not None:
        add(f"%expect {decl.expect}")

    # Token type:  point at YYSTYPE (which we re-emit as a typedef union below).
    add("%token_type {YYSTYPE}")

    # %first_token shifts terminal numbering to 258+ (Bison parity).  Required
    # so scan.c's raw-ASCII punctuation returns (e.g. ';' = 59) cannot collide
    # with keyword tokens (formerly ATOMIC=42 collided with '*'=42).  See
    # Lime upstream P0-NEW-4 / commit 4255b05.
    #
    # Lime emits constant value = i + first_token where i is the 1-based
    # internal index, so first_token=257 yields IDENT=258 (matching Bison's
    # convention that IDENT is the first token after the 256-wide ASCII
    # reservation -- index 257 is reserved by Bison for the synthetic EOF
    # symbol, leaving 258 as the first user-declared token).
    add("%first_token 257")

    # %locations + %location_type unlock Bison-compatible @N expansion.  See
    # Lime upstream P0-NEW-2 / commit 0384bfe.  PG's YYLTYPE is `int` (byte
    # offset into scanbuf); we override Lime's default LimeLocation struct.
    if decl.has_locations:
        add("%locations")
        add("%location_type {YYLTYPE}")

    # Extra arg(s)
    parse_params_struct = None  # name of synthesized struct, if any
    parse_params_fields = []    # list of (decl_text) for the synthesized struct
    if decl.extra_args:
        if len(decl.extra_args) == 1:
            add("%extra_argument {" + decl.extra_args[0] + "}")
        else:
            # Lime allows only one extra_argument.  Fold the original
            # %parse-param entries into a synthetic struct, and emit
            # macro shadows so existing actions that reference the
            # individual parameters by name keep compiling.
            #
            # Each extra_args entry has the shape "<C type> <ident>",
            # e.g. "PLpgSQL_stmt_block **plpgsql_parse_result_p" or
            # "yyscan_t yyscanner".  We extract the trailing identifier
            # (the last whitespace-separated word, stripped of any
            # `[]`/`*`) as the field name; the type is everything
            # before it.
            parse_params_struct = "GramParseExtra"
            for arg in decl.extra_args:
                parse_params_fields.append(arg.strip())
                ident = re.split(r"[\s\*\[\]]+", arg.strip())[-1]
                if ident:
                    decl.parse_params_idents.append(ident)
            sys.stderr.write(
                f"WARN: multiple %parse-param entries ({len(decl.extra_args)});"
                f" folding into struct {parse_params_struct}\n"
            )
            add("%extra_argument {struct " + parse_params_struct + " *extra}")

    add("")

    # Prologue, wrapped in %include so the generated parser sees it.
    # Note: Lime emits %include text verbatim at the top of the .c.  We
    # do NOT re-emit the %union body as a typedef here -- the caller is
    # expected to provide YYSTYPE via a header included from the prologue
    # (jsonpath_internal.h for jsonpath; gramparse.h for the backend SQL
    # parser).  Re-emitting it would conflict with that header.
    add("%include {")
    if decl.name_prefix == "plpgsql_yy":
        # plpgsql's YYSTYPE union body lives in pl_gram_types.h.  Pull it
        # in FIRST -- it's self-contained (pulls postgres.h, plpgsql.h,
        # etc.) so it's safe to include before the original gram.y
        # prologue.  This makes YYSTYPE visible to the prologue's
        # function declarations (current_token_is_not_variable, etc.).
        add('#include "pl_gram_types.h"')
        add("")
        # YYEMPTY is bison's no-lookahead sentinel; some action bodies
        # use it (rewritten from yychar by rewrite_action()).  The Lime
        # runtime doesn't define it, so supply our own.
        add("#ifndef YYEMPTY")
        add("#define YYEMPTY (-2)")
        add("#endif")
        add("")
    if prologue_c.strip():
        add("/* ---- BEGIN gram.y prologue ---- */")
        add(prologue_c.rstrip())
        add("/* ---- END gram.y prologue ---- */")
    for code in decl.code_blocks:
        add(code.rstrip())
    if parse_params_struct is not None:
        # Synthesize the folded struct + name-shadow macros so existing
        # action bodies that reference the original %parse-param idents
        # keep compiling unchanged.
        add("")
        add(f"/* Synthesized to fold the original %parse-param entries")
        add(f" * into a single Lime %extra_argument.  Action bodies that")
        add(f" * referenced the original idents by name keep compiling")
        add(f" * unchanged via the macro shadows below. */")
        add(f"struct {parse_params_struct}")
        add("{")
        for f in parse_params_fields:
            add(f"\t{f};")
        add("};")
        # No macro shadows: action bodies are rewritten by
        # rewrite_action() to use `extra->ident` directly, so the
        # function declarations in the prologue (which use the same
        # idents as parameter names) keep compiling.
    add("}")
    add("")

    # %syntax_error template (PG canonical).  plpgsql uses its own
    # plpgsql_yyerror(YYLTYPE *, PLpgSQL_stmt_block **, yyscan_t, const char *)
    # signature; backend uses parser_yyerror() macro from gram.y's prologue;
    # ecpg uses base_yyerror(const char *).  CLI override available.
    add("%syntax_error {")
    if decl.name_prefix == "plpgsql_yy":
        add("\tplpgsql_yyerror(&yyloc, extra->plpgsql_parse_result_p, "
            "extra->yyscanner, \"syntax error\");")
    else:
        fn = yyerror_fn or "parser_yyerror"
        add(f"\t{fn}(\"syntax error\");")
    add("}")
    add("%parse_failure {")
    if decl.name_prefix == "plpgsql_yy":
        # %parse_failure has no yyloc parameter; read it directly from
        # the parser's stashed lookahead location instead.
        add("\tplpgsql_yyerror(&yypParser->yyLookaheadLoc, "
            "extra->plpgsql_parse_result_p, "
            "extra->yyscanner, \"parse failure\");")
    else:
        fn = yyerror_fn or "parser_yyerror"
        add(f"\t{fn}(\"parse failure\");")
    add("}")
    add("")

    # Start symbol
    start_sym = decl.start_symbol
    if start_sym is None and rules:
        start_sym = rules[0].lhs
    if start_sym:
        add(f"%start_symbol {decl.nt_rename.get(start_sym, _to_nonterm(start_sym))}")
    add("")

    # Auto-declare any char-literal tokens that appear in rule RHS but
    # were not explicitly declared.  Bison auto-declares single-char
    # tokens implicitly; Lime requires explicit %token declarations.
    for rule in rules:
        for alt in rule.alternatives:
            for elem in alt.rhs:
                if elem.kind == "CHAR":
                    name = CHAR_TOKEN_MAP.get(elem.text)
                    if name and name not in decl.token_decls:
                        decl.token_decls[name] = None

    # Tokens
    add("/* ======================================================================")
    add(" * TOKENS")
    add(" * ====================================================================== */")
    for name in decl.token_decls:
        add(f"%token {name}.")
    add("")

    # Precedence
    add("/* ======================================================================")
    add(" * PRECEDENCE")
    add(" * ====================================================================== */")
    for direction, syms in decl.precedence:
        mapped = [map_token_name(s) for s in syms]
        add(f"%{direction} {' '.join(mapped)}.")
    add("")

    # Type declarations for non-terminals
    add("/* ======================================================================")
    add(" * NON-TERMINAL TYPES")
    add(" * ====================================================================== */")
    for name, ctype in decl.type_decls.items():
        add(f"%type {decl.nt_rename.get(name, _to_nonterm(name))} {{{ctype}}}")
    add("")

    # Rules
    add("/* ======================================================================")
    add(" * GRAMMAR RULES")
    add(" * ====================================================================== */")
    add("")

    # Helper non-terminals from mid-rule actions
    if helpers:
        add("/* ---- helper non-terminals lifted from mid-rule actions ---- */")
        # First, emit scratchpad globals for restructured helpers (those
        # whose action body or parent's final action references parent
        # prefix positions).  Globals are declared in a single %include
        # block at file scope.
        scratchpad_globals = []
        for h in helpers:
            if not h.prefix_refs:
                continue
            for kind, n, _member in sorted(
                h.prefix_refs, key=lambda r: (r[1], r[0], r[2] or "")
            ):
                if kind == "loc":
                    var = f"_midshim_{h.name}_arg{n}_loc"
                    decl_str = f"static YYLTYPE\t{var};"
                elif kind == "val":
                    sym = h.prefix_rhs[n - 1] if h.prefix_rhs else None
                    sym_type = _resolve_symbol_type(sym, decl) if sym else "YYSTYPE"
                    var = f"_midshim_{h.name}_arg{n}_val"
                    decl_str = f"static {sym_type}\t{var};"
                if (var, decl_str) not in scratchpad_globals:
                    scratchpad_globals.append((var, decl_str))
        if scratchpad_globals:
            add("%include {")
            add("\t/* Scratchpad globals for mid-rule actions that reference")
            add("\t * parent prefix positions.  See restructure_midrules() in")
            add("\t * src/tools/lime_convert_gram.py.  Single-threaded parser")
            add("\t * (ecpg's preproc) so file-static is fine. */")
            seen_decls = set()
            for var, decl_str in scratchpad_globals:
                if decl_str in seen_decls:
                    continue
                seen_decls.add(decl_str)
                add("\t" + decl_str)
            add("}")
            add("")
        for h in helpers:
            # Detect typed mid-rule action: `$<member>$ = ...` sets the
            # helper's return type from the union member.  Otherwise
            # default to int (semantically opaque).
            ctype = "int"
            m = re.search(r"\$<([A-Za-z_][A-Za-z0-9_]*)>\$", h.action)
            if m and m.group(1) in decl.union_fields:
                ctype = decl.union_fields[m.group(1)].ctype
            h.ctype = ctype
            add(f"%type {h.name} {{{ctype}}}    /* mid-rule action helper */")
        for h in helpers:
            if h.prefix_rhs is not None:
                # Restructured helper: takes the prefix as its own RHS.
                # Helper's action body: capture prefix values into
                # scratchpad globals, then run the original mid-action
                # body (with prefix refs already rewritten to globals).
                rhs_strs = []
                labels_used = set()
                for idx, elem in enumerate(h.prefix_rhs, start=1):
                    label = _label_for_index(idx)
                    if elem.kind == "CHAR":
                        sym = map_token_name(elem.text)
                    elif elem.kind == "MIDACT":
                        sym = elem.text
                    else:
                        sym = map_nt_or_token(elem.text, decl)
                    rhs_strs.append(f"{sym}({label})")
                    labels_used.add(label)
                rhs_text = " ".join(rhs_strs)
                add(f"{h.name}(A) ::= {rhs_text}. {{")
                if h.ctype in ("int", "bool"):
                    add("\tA = 0;")
                # Emit scratchpad-population statements -- dedupe by
                # (kind, n), since multiple member references on the
                # same position only need one capture.
                seen_assigns = set()
                for kind, n, _member in sorted(
                    h.prefix_refs, key=lambda r: (r[1], r[0])
                ):
                    key = (kind, n)
                    if key in seen_assigns:
                        continue
                    seen_assigns.add(key)
                    label = _label_for_index(n)
                    if kind == "loc":
                        add(f"\t_midshim_{h.name}_arg{n}_loc = @{label};")
                    else:
                        add(f"\t_midshim_{h.name}_arg{n}_val = {label};")
                # Emit the (already rewritten) action body.
                body = rewrite_action(h.action, len(h.prefix_rhs),
                                       rhs=h.prefix_rhs, decl=decl)
                for ln in body.splitlines():
                    add("\t" + ln)
                add("}")
            else:
                # Empty hoist: no parent prefix references.
                body = rewrite_action(h.action, 0, [], decl)
                add(f"{h.name}(A) ::= . {{")
                if getattr(h, "ctype", "int") in ("int", "bool"):
                    add("\tA = 0;")
                for ln in body.splitlines():
                    add("\t" + ln)
                add("}")
        add("")

    for rule in rules:
        lhs_name = decl.nt_rename.get(rule.lhs, _to_nonterm(rule.lhs))
        add(f"/* ----- {lhs_name} ----- */")
        for alt in rule.alternatives:
            add(_emit_alternative(rule.lhs, lhs_name, alt, decl,
                                   empty_loc=empty_loc))
        add("")

    # Epilogue from gram.y (post-%% C code) -- verbatim inside %include {}.
    if epilogue_c.strip():
        epilogue_text = epilogue_c.rstrip()
        if decl.name_prefix == "plpgsql_yy":
            # The epilogue's helper functions use the same bison-isms as
            # action bodies (yylex, yyerror).  Translate them once at
            # emission time so the helpers compile against Lime's runtime.
            epilogue_text = re.sub(
                r"(?<![A-Za-z0-9_])yyerror(?=\s*\()",
                "plpgsql_yyerror",
                epilogue_text,
            )
            epilogue_text = re.sub(
                r"(?<![A-Za-z0-9_])yylex(?=\s*\()",
                "plpgsql_yylex",
                epilogue_text,
            )
        add("/* Epilogue from gram.y. */")
        add("%include {")
        add(epilogue_text)
        add("}")
        add("")

    # Driver block (parser glue: ASCII-to-Lime token mapper + push-parser
    # loop).  Lime parsers are reentrant push parsers; PG's existing
    # callers expect a base_yyparse() shim that drives base_yylex().
    if driver_block is not None:
        add(driver_block.rstrip())
        add("")

    return "\n".join(out) + "\n"


def _labels_referenced(action: str) -> Set[str]:
    """Return the set of letter labels (A, B, C, ...) referenced by a
    rewritten Lime action body.  We only emit `(LABEL)` decorations on
    the rule LHS / RHS when the action actually references the label,
    because Lime treats unused labels as errors (rule.c: `Label X for
    "sym(X)" is never used` -> errorcnt++).
    """
    refs: Set[str] = set()
    # Tokens we count as label references: a single uppercase letter
    # used as an identifier (matches the rewriter's letter labels) and
    # the location form @LETTER.
    for m in re.finditer(r"(?<![A-Za-z0-9_])([A-Z])(?![A-Za-z0-9_])", action):
        refs.add(m.group(1))
    for m in re.finditer(r"@([A-Z])", action):
        refs.add(m.group(1))
    return refs


def _emit_alternative(lhs_orig: str, lhs: str, alt: Alternative,
                      decl: Declarations,
                      empty_loc: Optional[str] = None) -> str:
    # Resolve LHS member info: gram.y's LHS is always a non-terminal,
    # but be defensive and check token_decls anyway.
    lhs_is_token = lhs_orig in decl.token_decls
    lhs_member = (
        decl.token_members.get(lhs_orig) if lhs_is_token
        else decl.type_members.get(lhs_orig)
    )
    # First compute the action body so we know which labels matter.
    if not alt.action:
        if len(alt.rhs) == 1:
            # Default Bison action: $$ = $1.  Account for the union
            # member shift between LHS (typed via %type or untyped) and
            # RHS[0] (typed via %token or %type).
            rhs0 = alt.rhs[0]
            rhs0_member: Optional[str] = None
            rhs0_is_token = False
            if rhs0.kind == "SYM":
                renamed = TOKEN_RENAME.get(rhs0.text, rhs0.text)
                if renamed in decl.token_decls:
                    rhs0_is_token = True
                    rhs0_member = decl.token_members.get(renamed)
                elif rhs0.text in decl.token_decls:
                    rhs0_is_token = True
                    rhs0_member = decl.token_members.get(rhs0.text)
                elif rhs0.text in decl.type_decls:
                    rhs0_member = decl.type_members.get(rhs0.text)
            elif rhs0.kind == "CHAR":
                mapped = CHAR_TOKEN_MAP.get(rhs0.text)
                if mapped and mapped in decl.token_decls:
                    rhs0_is_token = True
                    rhs0_member = decl.token_members.get(mapped)
            # If LHS has no declared type, it inherits YYSTYPE; assign
            # full union from B regardless of RHS member info.  Only
            # access .member when LHS has a specific C type.
            lhs_typed = (lhs_orig in decl.type_decls and
                         decl.type_decls.get(lhs_orig) is not None)
            if lhs_typed and rhs0_is_token and rhs0_member:
                action = f"A = B.{rhs0_member};"
            elif lhs_is_token and lhs_member and rhs0_is_token and rhs0_member:
                action = f"A.{lhs_member} = B.{rhs0_member};"
            elif lhs_is_token and lhs_member:
                action = f"A.{lhs_member} = B;"
            else:
                action = "A = B;"
        else:
            action = ""
    else:
        action = rewrite_action(
            alt.action,
            len(alt.rhs),
            rhs=alt.rhs,
            decl=decl,
            lhs_member=lhs_member,
            lhs_is_token=lhs_is_token,
        ).strip()
        # For non-plpgsql grammars with multiple %parse-params folded
        # into struct GramParseExtra, inject parse-param locals at
        # the top of the action body so original code that references
        # the bare param names (cube uses `result`, `escontext`,
        # `scanbuflen`, `yyscanner`) compiles unchanged.
        action = _inject_parse_param_locals(action, decl)

    # Empty alternatives without an explicit user action: optionally
    # set @A to a caller-specified default expression.  ecpg uses
    # YYLTYPE = const char * and expects empty alternatives to
    # default to ""; Lime's runtime sets the LHS yyloc to
    # yyLookaheadLoc which is wrong for ecpg.  Inject `@A = <expr>;`
    # to override.  No-op for grammars that don't pass --empty-loc.
    if (empty_loc is not None and not alt.rhs and not action):
        action = f"@A = {empty_loc};"

    refs = _labels_referenced(action)

    rhs_strs: List[str] = []
    for idx, elem in enumerate(alt.rhs, start=1):
        label = _label_for_index(idx)
        if elem.kind == "CHAR":
            sym = map_token_name(elem.text)
        elif elem.kind == "MIDACT":
            sym = elem.text
        else:
            sym = map_nt_or_token(elem.text, decl)
        if label in refs:
            rhs_strs.append(f"{sym}({label})")
        else:
            rhs_strs.append(sym)

    prec_text = f" [{map_token_name(alt.prec)}]" if alt.prec else ""

    # LHS label is needed iff action references it.
    lhs_decoration = "(A)" if "A" in refs else ""
    head = f"{lhs}{lhs_decoration} ::= "
    if rhs_strs:
        body = " ".join(rhs_strs) + f".{prec_text}"
    else:
        body = f".{prec_text}  /* empty */"

    if action:
        out_lines = [head + body + " {"]
        for ln in action.splitlines():
            out_lines.append("\t" + ln)
        out_lines.append("}")
        return "\n".join(out_lines)
    return head + body


# ---------------------------------------------------------------------------
# Validation.
# ---------------------------------------------------------------------------

def validate(decl: Declarations, rules: List[Rule], no_driver: bool = False) -> int:
    """Print warnings to stderr.  Return non-fatal warning count.

    When ``no_driver`` is set the consumer supplies its own driver and value
    model (this is the ecpg preproc case): its grammar deliberately reuses
    backend non-terminals WITHOUT %type declarations -- ecpg's parse.pl only
    emits %type for its own handful of symbols and lets the reused backend
    non-terminals carry the default (string-concatenation) semantics.  In
    that mode the "non-terminal on RHS has no %type" check is a false
    positive (Lime accepts the default there), so it is suppressed.  The
    other two checks -- declared-but-unused %type, and unused %token --
    remain valid signals in every mode.
    """
    warnings = 0
    lhs_set: Set[str] = {r.lhs for r in rules}
    rhs_syms: Set[str] = set()
    for r in rules:
        for alt in r.alternatives:
            for elem in alt.rhs:
                if elem.kind == "SYM":
                    rhs_syms.add(elem.text)
                elif elem.kind == "CHAR":
                    rhs_syms.add(map_token_name(elem.text))

    for sym in decl.type_decls:
        if sym not in lhs_set:
            sys.stderr.write(
                f"WARN: %type {sym} declared but never appears as LHS\n"
            )
            warnings += 1

    for tok in decl.token_decls:
        if tok not in rhs_syms:
            sys.stderr.write(f"WARN: %token {tok} never used in any rule\n")
            warnings += 1

    if not no_driver:
        for sym in rhs_syms:
            if sym in lhs_set and sym not in decl.type_decls:
                # Non-terminal with no %type -- Bison defaults to int, Lime
                # would refuse.  Only a real bug when we own the value model
                # (i.e. NOT the --no-driver / ecpg case; see the docstring).
                sys.stderr.write(
                    f"WARN: non-terminal {sym} appears on RHS but has no "
                    f"%type declaration\n"
                )
                warnings += 1
    return warnings


# ---------------------------------------------------------------------------
# Main.
# ---------------------------------------------------------------------------

def _make_plpgsql_driver(decl: "Declarations") -> str:
    """Build the plpgsql driver block, emitting only ASCII->token cases
    for tokens actually declared in this grammar.
    """
    char_to_name = [
        ("'('", "LPAREN"),
        ("')'", "RPAREN"),
        ("'['", "LBRACKET"),
        ("']'", "RBRACKET"),
        ("','", "COMMA"),
        ("';'", "SEMI"),
        ("':'", "COLON"),
        ("'.'", "DOT"),
        ("'+'", "PLUS"),
        ("'-'", "MINUS"),
        ("'*'", "STAR"),
        ("'/'", "SLASH"),
        ("'%'", "PERCENT"),
        ("'^'", "CARET"),
        ("'|'", "PIPE"),
        ("'<'", "LT"),
        ("'>'", "GT"),
        ("'='", "EQ"),
        ("'#'", "HASH"),
        ("'~'", "TILDE"),
        ("'?'", "QMARK"),
        ("'@'", "AT_SIGN"),
        ("'!'", "BANG"),
        ("'&'", "AMP"),
        ("'{'", "LBRACE"),
        ("'}'", "RBRACE"),
    ]
    # plpgsql passes raw ASCII for any single-char token through the
    # driver, including chars only referenced in C action bodies
    # (e.g. `tok == '['`).  Bison auto-declared these implicitly;
    # Lime needs explicit %token decls plus an ASCII->Lime mapping
    # in the driver, otherwise the driver pushes a raw ASCII code
    # below %first_token and Lime fires a syntax error.  Declare
    # them all unconditionally for plpgsql.
    for ch, name in char_to_name:
        decl.token_decls.setdefault(name, None)
    cases = []
    rev_cases = []
    for ch, name in char_to_name:
        cases.append(f"\t\tcase {ch}:\treturn {name};")
        rev_cases.append(f"\t\tcase {name}:\treturn {ch};")
    cases_block = "\n".join(cases)
    rev_cases_block = "\n".join(rev_cases)
    return (
        "/* Driver. */\n"
        "%include {\n"
        "#include \"utils/palloc.h\"\n"
        "\n"
        "extern void *plpgsql_yyAlloc(void *(*mallocProc)(size_t));\n"
        "extern void plpgsql_yyLoc(void *yyp, int yymajor, YYSTYPE yyminor,\n"
        "\t\t\t\t\t\t  YYLTYPE yyloc, struct GramParseExtra *extra);\n"
        "extern void plpgsql_yyFree(void *p, void (*freeProc)(void *));\n"
        "\n"
        "static inline int\n"
        "plpgsql_ascii_to_lime_token(int t)\n"
        "{\n"
        "\tswitch (t)\n"
        "\t{\n"
        f"{cases_block}\n"
        "\t\tdefault:\treturn t;\n"
        "\t}\n"
        "}\n"
        "\n"
        "/* Reverse mapping: convert Lime symbolic codes back to the\n"
        " * raw ASCII the plpgsql lexer originally returned.  Used by\n"
        " * plpgsql_yy_drain_lookahead so a Lime lookahead pushed back\n"
        " * onto the lexer pushback stack pops out as the same numeric\n"
        " * token plpgsql_yylex would have returned, matching the\n"
        " * `tok == '['`-style comparisons in pl_gram.y action bodies.\n"
        " */\n"
        "int\n"
        "plpgsql_lime_to_ascii_token(int t)\n"
        "{\n"
        "\tswitch (t)\n"
        "\t{\n"
        f"{rev_cases_block}\n"
        "\t\tdefault:\treturn t;\n"
        "\t}\n"
        "}\n"
        "\n"
        "int\n"
        "plpgsql_yyparse(PLpgSQL_stmt_block **plpgsql_parse_result_p, yyscan_t yyscanner)\n"
        "{\n"
        "\tstruct GramParseExtra\textra = {\n"
        "\t\t.plpgsql_parse_result_p = plpgsql_parse_result_p,\n"
        "\t\t.yyscanner = yyscanner,\n"
        "\t};\n"
        "\tvoid\t   *parser;\n"
        "\tYYSTYPE\t\tlval;\n"
        "\tYYLTYPE\t\tlloc = 0;\n"
        "\tint\t\t\ttoken;\n"
        "\n"
        "\tparser = plpgsql_yyAlloc(palloc);\n"
        "\twhile ((token = plpgsql_yylex(&lval, &lloc, yyscanner)) != 0)\n"
        "\t{\n"
        "\t\t/* Mirror the state-mutating actions for tokens whose\n"
        "\t\t * grammar reduce-action sets scanner state.  In Bison's\n"
        "\t\t * pull model the action runs before the next yylex; in\n"
        "\t\t * Lime's push model the next token is already in flight\n"
        "\t\t * by the time the reduce fires, so the scanner sees the\n"
        "\t\t * old state.  Driver-level mirroring keeps state changes\n"
        "\t\t * in lockstep with the scanner. */\n"
        "\t\tswitch (token)\n"
        "\t\t{\n"
        "\t\t\tcase K_DECLARE:\n"
        "\t\t\t\t/* decl_start: K_DECLARE { plpgsql_IdentifierLookup =\n"
        "\t\t\t\t * IDENTIFIER_LOOKUP_DECLARE; } */\n"
        "\t\t\t\tplpgsql_IdentifierLookup = IDENTIFIER_LOOKUP_DECLARE;\n"
        "\t\t\t\tbreak;\n"
        "\t\t\tcase K_BEGIN:\n"
        "\t\t\t\t/* decl_sect's reduce action runs after BEGIN is\n"
        "\t\t\t\t * shifted: plpgsql_IdentifierLookup =\n"
        "\t\t\t\t * IDENTIFIER_LOOKUP_NORMAL */\n"
        "\t\t\t\tplpgsql_IdentifierLookup = IDENTIFIER_LOOKUP_NORMAL;\n"
        "\t\t\t\tbreak;\n"
        "\t\t\tdefault:\n"
        "\t\t\t\tbreak;\n"
        "\t\t}\n"
        "\t\tplpgsql_yyLoc(parser, plpgsql_ascii_to_lime_token(token),\n"
        "\t\t\t\t\t  lval, lloc, &extra);\n"
        "\t}\n"
        "\tplpgsql_yyLoc(parser, 0, lval, lloc, &extra);\n"
        "\tplpgsql_yyFree(parser, pfree);\n"
        "\treturn 0;\n"
        "}\n"
        "}\n"
    )


def main() -> int:
    ap = argparse.ArgumentParser(description="Mechanical Bison .y -> Lime .lime converter.")
    ap.add_argument("input", help="Bison .y input file")
    ap.add_argument("output", help="Lime .lime output file")
    ap.add_argument(
        "--prefix",
        help="override name-prefix (else taken from %%name-prefix in source)",
    )
    ap.add_argument(
        "--no-driver",
        action="store_true",
        help="omit the converter's built-in driver block.  Use this when"
             " the consumer (e.g. ecpg's preproc) provides its own driver"
             " via parser.c.",
    )
    ap.add_argument(
        "--yyerror-fn",
        default=None,
        help="override the function name used inside %%syntax_error /"
             " %%parse_failure blocks.  Defaults to parser_yyerror for"
             " the backend (base_yy prefix) and plpgsql_yyerror for"
             " plpgsql.  Use base_yyerror for ecpg.",
    )
    ap.add_argument(
        "--empty-loc",
        default=None,
        help="C expression to set @LHS to for empty alternatives (no"
             " RHS, no user action).  ecpg uses YYLTYPE = const char *"
             " and expects empty alternatives to default to \"\"."
             " Lime's default sets yyloc to the lookahead location"
             " which is wrong for ecpg.",
    )
    args = ap.parse_args()

    with open(args.input) as f:
        text = f.read()

    sections = split_sections(text)
    decl = Declarations()
    parse_declarations(sections.declarations, decl)
    if args.prefix:
        decl.name_prefix = args.prefix

    rule_text = strip_block_comments(sections.rules)
    tokens = tokenize_rules(rule_text)
    rules, helpers = parse_rules(tokens)

    # Detect mid-rule actions that reference parent prefix positions
    # and mark their helpers for restructured emission (capture the
    # prefix into the helper's RHS, expose values via static globals).
    restructure_midrules(rules, helpers, decl)

    decl.nt_rename = build_nt_rename(
        decl.type_decls, [r.lhs for r in rules]
    )

    sys.stderr.write(
        f"converted {len(rules)} non-terminals, "
        f"{sum(len(r.alternatives) for r in rules)} alternatives, "
        f"{len(helpers)} mid-rule helpers, "
        f"{len(decl.token_decls)} tokens, "
        f"{len(decl.type_decls)} %type decls.\n"
    )

    # The driver block depends on the FULL set of declared tokens
    # (including the char-literal auto-declares emit_lime ran), so
    # construct it FIRST -- its setdefault calls populate decl.token_decls
    # with chars referenced in action bodies (e.g. `[`/`]` in plpgsql).
    # emit_lime then emits %token decls for them and the driver block
    # appears at the end of the .lime output.
    driver_block = None
    if not args.no_driver:
        driver_block = (
            _BACKEND_PARSER_DRIVER if decl.name_prefix == "base_yy"
            else _make_plpgsql_driver(decl) if decl.name_prefix == "plpgsql_yy"
            else None
        )
    output = emit_lime(
        decl, rules, helpers, sections.prologue_c, args.input,
        epilogue_c=sections.epilogue,
        driver_block=driver_block,
        yyerror_fn=args.yyerror_fn,
        empty_loc=args.empty_loc,
    )
    with open(args.output, "w") as f:
        f.write(output)
    sys.stderr.write(f"wrote {args.output} ({len(output.splitlines())} lines)\n")

    nwarn = validate(decl, rules, no_driver=args.no_driver)
    if nwarn:
        sys.stderr.write(f"validation: {nwarn} warning(s)\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

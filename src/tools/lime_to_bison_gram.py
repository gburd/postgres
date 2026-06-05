#!/usr/bin/env python3
"""
lime_to_bison_gram.py -- emit a parse.pl-compatible Bison .y from a
Lime .lime grammar.

ecpg's parse.pl (src/interfaces/ecpg/preproc/parse.pl) reads the
backend grammar to produce ecpg's preproc.y.  Historically it read
gram.y directly; with Phase 2k.2 the authoritative grammar moved to
gram.lime.  Rather than rewrite parse.pl to understand Lime syntax,
we emit a Bison-format scaffold from gram.lime that parse.pl can
consume unchanged.

What we emit:

  - %token directives copied through (Lime `%token NAME.` becomes
    Bison `%token NAME`).
  - %left/%right/%nonassoc directives copied through (period stripped).
  - %type declarations: Lime is per-symbol (`%type sym {Ctype}`);
    Bison uses %union members.  parse.pl looks at the BARE token /
    nonterminal NAMES and the `<member>` tags from gram.y's %type
    decls.  We don't have member info in gram.lime (Lime
    deliberately collapsed it via per-symbol %type).  But parse.pl
    only USES the type info for `replace_types`-driven overrides
    (PrepareStmt, ExecuteStmt, opt_array_bounds) and for "ignore" --
    it doesn't care about the original member names.  So we can omit
    %type decls entirely; parse.pl's `replace_types` hash provides
    the few it actually needs.
  - Rules: Lime's `lhs(X) ::= rhs.` becomes Bison's `lhs : rhs ;`.
    Action bodies are stripped (parse.pl ignores actions; ecpg
    provides its own).  Labels (the parenthesized letters) are
    stripped from RHS symbols.  When multiple Lime rules share an
    LHS, they get merged into Bison alternatives separated by `|`.
  - Char-token aliases reverse: Lime grammar uses LPAREN, COMMA,
    SEMI, etc.; Bison gram.y used '(', ',', ';'.  We map back so
    parse.pl sees the original Bison spellings.

What we don't emit:

  - %include {...} blocks (C code; parse.pl doesn't need it; ecpg
    provides its own header/trailer/addons).
  - %first_token, %locations, %location_type, %name, %expect,
    %token_type, %extra_argument -- all Lime-specific.
  - Mid-rule action helpers (midactN) -- parse.pl doesn't need
    them; the original gram.y had inline mid-rule actions and
    ecpg.addons handles ecpg-specific overrides.

Usage:
  lime_to_bison_gram.py <input.lime> <output.y>
"""

from __future__ import annotations

import re
import sys
from collections import OrderedDict


# Reverse of the lime_convert_gram.py CHAR_TOKEN_MAP.  Keys are
# Lime symbolic token names; values are the original Bison char-literal
# spellings (with single quotes).
CHAR_TOKEN_REVERSE: dict[str, str] = {
    "LPAREN": "'('",
    "RPAREN": "')'",
    "LBRACKET": "'['",
    "RBRACKET": "']'",
    "COMMA": "','",
    "SEMI": "';'",
    "COLON": "':'",
    "DOT": "'.'",
    "PLUS": "'+'",
    "MINUS": "'-'",
    "STAR": "'*'",
    "SLASH": "'/'",
    "PERCENT": "'%'",
    "CARET": "'^'",
    "PIPE": "'|'",
    "LT": "'<'",
    "GT": "'>'",
    "EQ": "'='",
    "BANG": "'!'",
    "QMARK": "'?'",
    "AT_SIGN": "'@'",
    "TILDE": "'~'",
    "HASH": "'#'",
    "AMP": "'&'",
    "LBRACE": "'{'",
    "RBRACE": "'}'",
}

# Symbol renames the converter applied (Lime -> Bison).
SYMBOL_REVERSE: dict[str, str] = {
    "OP": "Op",
}


def reverse_symbol(name: str) -> str:
    """Map a Lime symbol back to its Bison spelling.  Char-literal
    aliases (LPAREN, etc.) become single-quoted chars; renamed tokens
    (OP -> Op) become their original.  Otherwise pass through."""
    if name in CHAR_TOKEN_REVERSE:
        return CHAR_TOKEN_REVERSE[name]
    if name in SYMBOL_REVERSE:
        return SYMBOL_REVERSE[name]
    return name


def first_letter_unflip(name: str, nt_rename: dict[str, str] | None = None) -> str:
    """Map a Lime non-terminal name back to its Bison spelling.
    First consults the nt_rename map (extracted from gram.lime's
    magic comment).  Falls back to a heuristic: if the name has
    lowercase first + any later uppercase, it was Pascal-case ->
    capitalize the first letter.  Otherwise leave alone.

    Strips the converter's `_nt` suffix (collision-rename marker).
    """
    if nt_rename and name in nt_rename:
        return nt_rename[name]

    # Strip _nt suffix.
    if name.endswith("_nt"):
        stem = name[:-3]
        if nt_rename and stem in nt_rename:
            return nt_rename[stem]
        name = stem

    # Pascal-case detection: lowercase first + any later uppercase.
    if name and name[0].islower() and any(c.isupper() for c in name[1:]):
        return name[0].upper() + name[1:]
    return name


def parse_nt_rename(text: str) -> dict[str, str]:
    """Extract the nt_rename map from a magic comment block.
    Returns a dict mapping Lime name -> Bison name (note: reversed
    from how the forward converter stores it -- we want lookups
    keyed by what's in gram.lime)."""
    rev: dict[str, str] = {}
    in_block = False
    for line in text.splitlines():
        s = line.strip()
        if "lime_to_bison_gram nt_rename map" in s:
            in_block = True
            continue
        if in_block:
            if s == "*/":
                break
            m = re.match(r"\*\s*([A-Za-z_][A-Za-z0-9_]*)\s*->\s*([A-Za-z_][A-Za-z0-9_]*)", s)
            if m:
                bison_name, lime_name = m.group(1), m.group(2)
                rev[lime_name] = bison_name
    return rev


def strip_block_comments(text: str) -> str:
    """Remove /* ... */ comments.  Idempotent."""
    return re.sub(r"/\*.*?\*/", " ", text, flags=re.DOTALL)


def find_balanced_brace(text: str, start: int) -> int:
    """Return index of matching `}` for the `{` at text[start].
    Tracks nested braces; ignores braces inside string literals and
    line comments."""
    assert text[start] == "{"
    depth = 1
    i = start + 1
    n = len(text)
    while i < n:
        c = text[i]
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                return i
        elif c == "/" and i + 1 < n and text[i + 1] == "/":
            # line comment
            j = text.find("\n", i)
            i = j if j >= 0 else n
            continue
        elif c == "/" and i + 1 < n and text[i + 1] == "*":
            # block comment
            j = text.find("*/", i + 2)
            i = j + 2 if j >= 0 else n
            continue
        elif c in '"\'':
            # string literal
            quote = c
            j = i + 1
            while j < n:
                if text[j] == "\\":
                    j += 2
                    continue
                if text[j] == quote:
                    j += 1
                    break
                j += 1
            i = j
            continue
        i += 1
    raise SystemExit("unbalanced brace in lime input")


def parse_lime(text: str) -> tuple[list[str], list[tuple[str, list[str]]],
                                   "OrderedDict[str, list[tuple[list[str], str | None]]]"]:
    """Parse Lime input.  Returns (tokens, precedence, rules).

    tokens: list of token names declared via %token.
    precedence: list of (assoc_directive, [symbols]) in source order.
    rules: OrderedDict mapping LHS -> list of (RHS, prec_token-or-None);
           each RHS is a list of symbol names; empty list = epsilon.
    """
    # Strip block comments first; line comments handled by line walk.
    text = strip_block_comments(text)

    tokens: list[str] = []
    precedence: list[tuple[str, list[str]]] = []
    rules: "OrderedDict[str, list[list[str]]]" = OrderedDict()

    i = 0
    n = len(text)

    while i < n:
        # Skip whitespace.
        while i < n and text[i] in " \t\r\n":
            i += 1
        if i >= n:
            break

        # Skip line comments.
        if text[i:i + 2] == "//":
            j = text.find("\n", i)
            i = j if j >= 0 else n
            continue

        # %include { ... }: skip whole block.
        if re.match(r"%include\b", text[i:]):
            i += 8
            j = text.find("{", i)
            if j < 0:
                raise SystemExit("malformed %include: no '{'")
            end = find_balanced_brace(text, j)
            i = end + 1
            continue

        # %token NAME.
        if re.match(r"%token\b", text[i:]) and not re.match(r"%token_type\b", text[i:]):
            i += 6
            # Read names until the period.
            while i < n and text[i] != ".":
                # Skip whitespace and angle-bracket type tags.
                while i < n and text[i] in " \t\r\n":
                    i += 1
                if i < n and text[i] == "<":
                    # %token <member> NAME -- Lime doesn't use this
                    # form, but be defensive.
                    end = text.find(">", i)
                    if end < 0:
                        raise SystemExit("malformed <member>")
                    i = end + 1
                    continue
                if i >= n or text[i] == ".":
                    break
                m = re.match(r"[A-Za-z_][A-Za-z0-9_]*", text[i:])
                if m:
                    tokens.append(m.group(0))
                    i += m.end()
                else:
                    i += 1
            if i < n and text[i] == ".":
                i += 1
            continue

        # %left, %right, %nonassoc TOK1 TOK2 ... .
        m = re.match(r"%(left|right|nonassoc)\b", text[i:])
        if m:
            assoc = m.group(1)
            i += m.end()
            syms: list[str] = []
            while i < n and text[i] != ".":
                while i < n and text[i] in " \t\r\n":
                    i += 1
                if i >= n or text[i] == ".":
                    break
                tm = re.match(r"[A-Za-z_][A-Za-z0-9_]*", text[i:])
                if tm:
                    syms.append(tm.group(0))
                    i += tm.end()
                else:
                    i += 1
            if i < n and text[i] == ".":
                i += 1
            precedence.append((assoc, syms))
            continue

        # %type sym {Ctype} -- skip, we don't translate types.
        if re.match(r"%type\b", text[i:]):
            i += 5
            # Skip identifier(s) until we find the {Ctype} brace.
            while i < n and text[i] != "{" and text[i] != "\n":
                i += 1
            if i < n and text[i] == "{":
                i = find_balanced_brace(text, i) + 1
            continue

        # %name, %name_prefix, %expect, %first_token, %locations,
        # %location_type, %extra_argument, %token_type, %start_symbol,
        # %start, %syntax_error, %parse_failure, %destructor, %fallback,
        # ...  Skip until period or closing brace if a {} block follows.
        if text[i] == "%":
            # Find end of directive: either a period followed by
            # newline, or a {brace block}, or end-of-line.
            m = re.match(r"%[A-Za-z_][A-Za-z0-9_]*", text[i:])
            if not m:
                i += 1
                continue
            i += m.end()
            # Look ahead for '{' (means action block) or '.' or end-of-line.
            while i < n and text[i] in " \t\r":
                i += 1
            if i < n and text[i] == "{":
                i = find_balanced_brace(text, i) + 1
                continue
            # Skip to the period or newline.
            while i < n and text[i] not in ".\n":
                i += 1
            if i < n and text[i] == ".":
                i += 1
            continue

        # Otherwise this is a rule.  Lime rule shape:
        #   lhs(X) ::= sym1(A) sym2(B) ... . [PRECEDENCE] { action }
        # or
        #   lhs ::= . [PRECEDENCE]                          (epsilon)
        m = re.match(r"([A-Za-z_][A-Za-z0-9_]*)\s*(?:\([A-Za-z]\))?\s*::=",
                     text[i:])
        if not m:
            # Unrecognized; skip to next line.
            j = text.find("\n", i)
            i = j + 1 if j >= 0 else n
            continue
        lhs = m.group(1)
        i += m.end()

        # Read RHS until '.'
        rhs: list[str] = []
        while i < n:
            while i < n and text[i] in " \t\r\n":
                i += 1
            if i >= n:
                break
            if text[i] == ".":
                i += 1
                break
            # Symbol name optionally followed by (label).
            sm = re.match(r"([A-Za-z_][A-Za-z0-9_]*)\s*(?:\([A-Za-z]\))?",
                          text[i:])
            if sm:
                rhs.append(sm.group(1))
                i += sm.end()
            else:
                i += 1

        # Optional precedence marker [TOKEN] -- capture the symbol
        # so we can emit %prec in the Bison output.
        prec: str | None = None
        while i < n and text[i] in " \t\r":
            i += 1
        if i < n and text[i] == "[":
            j = text.find("]", i)
            if j > 0:
                pm = re.match(r"\[\s*([A-Za-z_][A-Za-z0-9_]*)\s*\]", text[i:])
                if pm:
                    prec = pm.group(1)
            i = j + 1 if j >= 0 else i + 1
        # Optional action {}.
        while i < n and text[i] in " \t\r":
            i += 1
        if i < n and text[i] == "{":
            i = find_balanced_brace(text, i) + 1

        rules.setdefault(lhs, []).append((rhs, prec))

    return tokens, precedence, rules


def emit_bison(tokens: list[str],
               precedence: list[tuple[str, list[str]]],
               rules: "OrderedDict[str, list[tuple[list[str], str | None]]]",
               source_path: str,
               nt_rename: dict[str, str] | None = None) -> str:
    """Emit a parse.pl-compatible Bison .y."""
    out: list[str] = []
    add = out.append

    add("%{")
    add(f"/* Generated from {source_path} by lime_to_bison_gram.py.")
    add(" * Consumed by ecpg's parse.pl ONLY -- not a complete bison")
    add(" * grammar.  Action bodies are stripped; types are omitted")
    add(" * (parse.pl provides its own via the replace_types hash).")
    add(" * Do not hand-edit. */")
    add("%}")
    add("")

    # Tokens.  Filter out the converter's char-literal aliases -- those
    # become single-quoted chars in rule bodies, not %token decls.
    real_tokens = [t for t in tokens
                   if t not in CHAR_TOKEN_REVERSE]
    # De-duplicate while preserving order.
    seen: set[str] = set()
    for t in real_tokens:
        if t not in seen:
            seen.add(t)
            mapped = SYMBOL_REVERSE.get(t, t)
            add(f"%token {mapped}")
    add("")

    # Precedence directives.
    for assoc, syms in precedence:
        mapped = [reverse_symbol(s) for s in syms]
        add(f"%{assoc} {' '.join(mapped)}")
    add("")

    add("%%")
    add("")

    # Rules.  Skip mid-rule action helpers (midactN) -- parse.pl
    # doesn't need them.
    for lhs, alternatives in rules.items():
        if lhs.startswith("midact"):
            continue
        bison_lhs = first_letter_unflip(lhs, nt_rename)
        add(f"{bison_lhs}:")
        for idx, alt in enumerate(alternatives):
            rhs, prec = alt
            mapped_rhs = []
            for sym in rhs:
                if sym.startswith("midact"):
                    continue  # skip helper references
                mapped_rhs.append(reverse_symbol(first_letter_unflip(sym, nt_rename)))
            sep = "|" if idx > 0 else " "
            body = " ".join(mapped_rhs) if mapped_rhs else "/* empty */"
            if prec:
                body += f" %prec {reverse_symbol(prec)}"
            add(f"\t{sep} {body}")
        add("\t;")
        add("")

    add("%%")
    add("")
    add("/* Epilogue stripped; parse.pl ignores it. */")
    return "\n".join(out) + "\n"


def main() -> int:
    if len(sys.argv) != 3:
        sys.stderr.write("usage: lime_to_bison_gram.py <input.lime> <output.y>\n")
        return 2
    with open(sys.argv[1]) as f:
        text = f.read()
    nt_rename = parse_nt_rename(text)
    tokens, precedence, rules = parse_lime(text)
    output = emit_bison(tokens, precedence, rules, sys.argv[1], nt_rename)
    with open(sys.argv[2], "w") as f:
        f.write(output)
    sys.stderr.write(
        f"converted {len(rules)} non-terminals, "
        f"{sum(len(v) for v in rules.values())} alternatives, "
        f"{len(tokens)} tokens.\n"
        f"wrote {sys.argv[2]} ({len(output.splitlines())} lines)\n"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

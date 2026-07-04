#!/usr/bin/env python3
"""
pg-history: tie a PR's changes to PostgreSQL git + pgsql-hackers email history.

OCR (the code reviewer) cannot call MCP servers, so this is a separate agent:
it runs a Bedrock (Claude Opus) tool-use loop wired to the Agora MCP server at
https://pg.ddx.io/mcp, lets the model search the mailing-list archives / commit
history / commitfest data, and emits a Markdown summary linking the changes to
the relevant threads (https://pg.ddx.io/m/pgsql-hackers/<message-id>).

Env:
  PG_HISTORY_MCP_URL   MCP endpoint (default https://pg.ddx.io/mcp)
  PG_HISTORY_MODEL     Bedrock model id (e.g. us.anthropic.claude-opus-4-8)
  AWS_REGION           region (creds come from the OIDC step's env)
  BASE_REF, HEAD_SHA   PR base ref and head sha (for the git diff context)
  GH_PR_TITLE          PR title (optional, adds context)
  PG_HISTORY_OUT       output markdown path (default /tmp/pg-history.md)
Writes the markdown to PG_HISTORY_OUT; exits 0 even on soft failures (writes a note).
"""
import json, os, subprocess, sys, urllib.request

MCP_URL = os.environ.get("PG_HISTORY_MCP_URL", "https://pg.ddx.io/mcp")
MODEL = os.environ.get("PG_HISTORY_MODEL", "us.anthropic.claude-opus-4-8").replace("bedrock/converse/", "").replace("bedrock/", "")
REGION = os.environ.get("AWS_REGION", "us-east-1")
BASE_REF = os.environ.get("BASE_REF", "")
HEAD_SHA = os.environ.get("HEAD_SHA", "")
PR_TITLE = os.environ.get("GH_PR_TITLE", "")
OUT = os.environ.get("PG_HISTORY_OUT", "/tmp/pg-history.md")
UA = "pg-history/0.1 (+github-actions)"

# Curated subset of the 108 Agora tools — the ones useful for connecting a
# change to its discussion/commit history. Intersected with what the server
# actually exposes, so unknown names are harmless.
TOOL_WHITELIST = {
    "find_related_discussions", "find_similar_messages", "get_thread",
    "discussion_links", "get_author_messages", "browse_by_date",
    "blame_symbol", "check_upstream_status", "find_related",
    "find_entries_for_thread", "find_entries_for_author", "get_commit",
    "search", "hybrid_search", "get_callers", "get_callees", "find_pattern",
}
MAX_ROUNDS = 14
TOOL_RESULT_CAP = 8000  # chars per tool result fed back to the model


def _mcp_post(body, sid=None):
    headers = {"Content-Type": "application/json",
               "Accept": "application/json, text/event-stream", "User-Agent": UA}
    if sid:
        headers["Mcp-Session-Id"] = sid
    req = urllib.request.Request(MCP_URL, data=json.dumps(body).encode(), headers=headers, method="POST")
    resp = urllib.request.urlopen(req, timeout=60)
    sid_out = resp.headers.get("Mcp-Session-Id")
    result = None
    for line in resp.read().decode().splitlines():
        line = line.strip()
        if line.startswith("data:"):
            line = line[5:].strip()
        if not line or line.startswith("event:"):
            continue
        try:
            obj = json.loads(line)
        except Exception:
            continue
        if isinstance(obj, dict) and ("result" in obj or "error" in obj):
            result = obj
    return result, sid_out


class MCP:
    def __init__(self):
        init, self.sid = _mcp_post({"jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": "2025-06-18", "capabilities": {},
                       "clientInfo": {"name": "pg-history", "version": "0.1"}}})
        if not init or "result" not in init:
            raise RuntimeError(f"MCP initialize failed: {init}")
        try:
            _mcp_post({"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}}, self.sid)
        except Exception:
            pass
        self._id = 1

    def list_tools(self):
        self._id += 1
        res, _ = _mcp_post({"jsonrpc": "2.0", "id": self._id, "method": "tools/list", "params": {}}, self.sid)
        return (res or {}).get("result", {}).get("tools", [])

    def call(self, name, args):
        self._id += 1
        res, _ = _mcp_post({"jsonrpc": "2.0", "id": self._id, "method": "tools/call",
                            "params": {"name": name, "arguments": args or {}}}, self.sid)
        if not res:
            return "(no response)"
        if "error" in res:
            return f"ERROR: {json.dumps(res['error'])[:500]}"
        parts = []
        for c in res.get("result", {}).get("content", []):
            if c.get("type") == "text":
                parts.append(c["text"])
        return ("\n".join(parts) or "(empty)")[:TOOL_RESULT_CAP]


def git(*args):
    try:
        return subprocess.check_output(["git", *args], text=True, stderr=subprocess.DEVNULL).strip()
    except Exception:
        return ""


def pr_context():
    base = f"origin/{BASE_REF}" if BASE_REF else ""
    rng = f"{base}..{HEAD_SHA}" if base and HEAD_SHA else HEAD_SHA
    commits = git("log", "--no-merges", "--format=%h %s", f"{rng}") if rng else ""
    stat = git("diff", "--stat", rng) if rng else ""
    files = git("diff", "--name-only", rng) if rng else ""
    return commits[:4000], stat[:3000], files[:2000]


SYSTEM = """You are a PostgreSQL community research assistant. Given a pull request's
commits and changed files, use the available tools (backed by the Agora index of
pgsql-hackers mail, commit history, and commitfest data) to connect the change to
its history. Your goal:

- Find the mailing-list thread(s) and prior discussion behind this change.
- Identify related/superseded prior commits and any commitfest entry.
- Note relevant prior art, rejected approaches, or design rationale.

Rules (voice & rigor):
- Be precise and blunt. No praise, no filler, no hedging, no disclaimers. Accuracy is
  the only success metric — not the author's approval. Lead with the most important finding.
- NEVER hallucinate. Verify every Message-ID, thread subject, commit hash, author name,
  and date against an actual tool result before citing it. If a search returns nothing,
  say so plainly — do not guess or fabricate a plausible-looking link.
- Assess the change on its merits, independent of how the PR frames it.
- Tag any inferred (not tool-confirmed) linkage with an explicit confidence level:
  high / moderate / low.
- Be decisive and efficient: a handful of targeted tool calls, not exhaustive search.
- Cite every mailing-list message as a Markdown link: [subject](https://pg.ddx.io/m/pgsql-hackers/MESSAGE_ID).
- If you find nothing relevant, say so in one line — do not pad.

When done, output ONLY Markdown (no preamble) with these sections, omitting any that are empty:
## 🧵 Related discussion
## 🔗 Related commits / prior art
## 📋 Commitfest
## 🧭 Context for reviewers
Keep it tight (use bullets; link generously)."""


def to_toolspec(t):
    schema = t.get("inputSchema") or {"type": "object", "properties": {}}
    return {"toolSpec": {"name": t["name"],
                         "description": (t.get("description") or "")[:600],
                         "inputSchema": {"json": schema}}}


def main():
    commits, stat, files = pr_context()
    if not commits and not files:
        open(OUT, "w").write("")  # nothing to do
        print("No PR diff context; skipping.")
        return
    user = (f"PR title: {PR_TITLE}\n\n" if PR_TITLE else "") + \
        f"Commits:\n{commits or '(none)'}\n\nChanged files:\n{files or '(none)'}\n\nDiffstat:\n{stat or '(none)'}\n"

    try:
        mcp = MCP()
        tools = [to_toolspec(t) for t in mcp.list_tools() if t.get("name") in TOOL_WHITELIST]
    except Exception as e:
        open(OUT, "w").write(f"_pg-history: could not reach the Agora MCP server ({MCP_URL}): {e}_\n")
        print(f"MCP unavailable: {e}")
        return
    if not tools:
        open(OUT, "w").write("_pg-history: no usable MCP tools available._\n")
        return

    import boto3
    from botocore.config import Config

    # botocore's default read timeout (60s) is too short for a multi-round
    # (MAX_ROUNDS) tool-use loop against a large PR diff on a reasoning model;
    # each converse() call alone can take several minutes.  Bump it well past
    # what a single round needs; connect_timeout stays short since a stuck
    # TCP handshake is a different (and much cheaper to detect) failure mode.
    brt = boto3.client("bedrock-runtime", region_name=REGION,
                       config=Config(read_timeout=900, connect_timeout=10))
    messages = [{"role": "user", "content": [{"text": user}]}]
    final_text = ""
    try:
        for _ in range(MAX_ROUNDS):
            resp = brt.converse(
                modelId=MODEL,
                system=[{"text": SYSTEM}],
                messages=messages,
                toolConfig={"tools": tools},
                inferenceConfig={"maxTokens": 4096},
            )
            out = resp["output"]["message"]
            messages.append(out)
            if resp.get("stopReason") == "tool_use":
                results = []
                for blk in out["content"]:
                    tu = blk.get("toolUse")
                    if not tu:
                        continue
                    res_text = mcp.call(tu["name"], tu.get("input") or {})
                    results.append({"toolResult": {"toolUseId": tu["toolUseId"],
                                                    "content": [{"text": res_text}]}})
                messages.append({"role": "user", "content": results})
                continue
            final_text = "".join(b.get("text", "") for b in out["content"]).strip()
            break
    except Exception as e:
        open(OUT, "w").write(f"_pg-history: Bedrock call failed: {e}_\n")
        print(f"Bedrock error: {e}")
        return

    if not final_text:
        final_text = "_pg-history: no related history found._"
    body = "## 📜 Change history & discussion (Agora / pg.ddx.io)\n\n" + final_text + \
           "\n\n<sub>Generated by pg-history via the Agora MCP server (pg.ddx.io).</sub>\n"
    open(OUT, "w").write(body)
    print(body)


if __name__ == "__main__":
    main()

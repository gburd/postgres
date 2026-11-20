# Dev Setup Commit Fix - Summary

**Date:** 2026-03-10
**Issue:** Sync workflow was failing because "dev setup" commits were detected as pristine master violations

## Problem

The sync workflow was rejecting the "dev setup v19" commit (e5aa2da496c) because it modifies files outside `.github/`. The original logic only allowed `.github/`-only commits, but didn't account for personal development environment commits.

## Solution

Updated sync workflows to recognize commits with messages starting with "dev setup" (case-insensitive) as allowed on master, in addition to `.github/`-only commits.

## Changes Made

### 1. Updated Sync Workflows

**Files modified:**
- `.github/workflows/sync-upstream.yml` (automatic hourly sync)
- `.github/workflows/sync-upstream-manual.yml` (manual sync)

**New logic:**
```bash
# Check for "dev setup" commits
DEV_SETUP_COMMITS=$(git log --format=%s upstream/master..origin/master | grep -i "^dev setup" | wc -l)

# Allow merge if:
# - Only .github/ changes, OR
# - Has "dev setup" commits
if [ "$COMMITS_AHEAD" -gt 0 ] && [ "$NON_GITHUB_CHANGES" -gt 0 ]; then
  if [ "$DEV_SETUP_COMMITS" -eq 0 ]; then
    # FAIL: Code changes outside .github/ that aren't dev setup
    exit 1
  else
    # OK: Dev setup commits are allowed
    continue merge
  fi
fi
```

### 2. Created Policy Documentation

**New file:** `.github/docs/pristine-master-policy.md`

Documents the "mostly pristine" master policy:
- ✅ `.github/` commits allowed (CI/CD configuration)
- ✅ "dev setup ..." commits allowed (personal development environment)
- ❌ Code changes not allowed (must use feature branches)

## Current Commit Order

```
master:
1. 9a2b895daa0 - Complete Phase 3: Windows builds + fix sync (newest)
2. 1e6379300f8 - Add CI/CD automation: hourly sync, Bedrock AI review
3. e5aa2da496c - dev setup v19
4. 03facc1211b - upstream commits... (oldest)
```

**All three local commits will now be preserved during sync:**
- Commit 1: Modifies `.github/` ✅
- Commit 2: Modifies `.github/` ✅
- Commit 3: Named "dev setup v19" ✅

## Testing

After committing these changes, the next hourly sync should:
1. Detect 3 commits ahead of upstream (including the fix commit)
2. Recognize that they're all allowed (`.github/` or "dev setup")
3. Successfully merge upstream changes
4. Create merge commit preserving all local commits

**Verify manually:**
```bash
# Trigger manual sync
# Actions → "Sync from Upstream (Manual)" → Run workflow

# Check logs for:
# "✓ Found 1 'dev setup' commit(s) - will merge"
# "✓ Successfully merged upstream with local configuration"
```

## Future Updates

When updating your development environment:

```bash
# Make changes
git add .clangd flake.nix .vscode/ .idea/

# IMPORTANT: Start commit message with "dev setup"
git commit -m "dev setup v20: Update IDE and LSP configuration"

git push origin master
```

The sync will recognize this and preserve it during merges.

**Naming patterns recognized:**
- `dev setup v20` ✅
- `Dev setup: Update tools` ✅
- `DEV SETUP - New config` ✅
- `development environment changes` ❌ (doesn't start with "dev setup")

## Benefits

1. **No manual sync resolution needed** for dev environment updates
2. **Simpler workflow** - dev setup stays on master where it's convenient
3. **Clear policy** - documented what's allowed vs what requires feature branches
4. **Automatic detection** - sync workflow handles it all automatically

## What to Commit

```bash
git add .github/workflows/sync-upstream.yml
git add .github/workflows/sync-upstream-manual.yml
git add .github/docs/pristine-master-policy.md
git add .github/DEV_SETUP_FIX.md

git commit -m "Fix sync to allow 'dev setup' commits on master

The sync workflow was failing because the 'dev setup v19' commit
modifies files outside .github/. Updated workflows to recognize
commits with messages starting with 'dev setup' as allowed on master.

Changes:
- Detect 'dev setup' commits by message pattern
- Allow merge if commits are .github/ OR dev setup
- Update merge messages to reflect preserved changes
- Document pristine master policy

This allows personal development environment commits (IDE configs,
debugging tools, shell aliases, etc.) on master without violating
the pristine mirror policy.

See .github/docs/pristine-master-policy.md for details"

git push origin master
```

## Next Sync Expected Behavior

```
Before:
  Upstream: A---B---C---D (latest upstream)
  Master:   A---B---C---X---Y---Z (X=CI/CD, Y=CI/CD, Z=dev setup)

  Status: 3 commits ahead, 1 commit behind

After:
  Master:   A---B---C---X---Y---Z---M
                        \         /
                         D-------/

  Where M = Merge commit preserving all local changes
```

All three local commits (CI/CD + dev setup) preserved! ✅

---

**Status:** Ready to commit and test
**Documentation:** See `.github/docs/pristine-master-policy.md`

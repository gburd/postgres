# Pristine Master Policy

## Overview

The `master` branch in this mirror repository follows a "mostly pristine" policy, meaning it should closely mirror the upstream `postgres/postgres` repository with only specific exceptions allowed.

## Allowed Commits on Master

Master is considered "pristine" and the sync workflow will successfully merge upstream changes if local commits fall into these categories:

### 1. ✅ CI/CD Configuration (`.github/` directory only)

Commits that only modify files within the `.github/` directory are allowed.

**Examples:**
- Adding GitHub Actions workflows
- Updating AI review configuration
- Modifying sync schedules
- Adding documentation in `.github/docs/`

**Rationale:** CI/CD configuration is repository-specific and doesn't affect the PostgreSQL codebase itself.

### 2. ✅ Development Environment Setup (commits named "dev setup ...")

Commits with messages starting with "dev setup" (case-insensitive) are allowed, even if they modify files outside `.github/`.

**Examples:**
- `dev setup v19`
- `Dev Setup: Add debugging configuration`
- `DEV SETUP - IDE and tooling`

**Typical files in dev setup commits:**
- `.clang-format`, `.clangd` - Code formatting and LSP config
- `.envrc` - Directory environment variables (direnv)
- `.gdbinit` - Debugger configuration
- `.idea/`, `.vscode/` - IDE settings
- `flake.nix`, `shell.nix` - Nix development environment
- `pg-aliases.sh` - Personal shell aliases
- Other personal development tools

**Rationale:** Development environment configuration is personal and doesn't affect the code or CI/CD. It's frequently updated as developers refine their workflow.

### 3. ❌ Code Changes (NOT allowed)

Any commits that:
- Modify PostgreSQL source code (`src/`, `contrib/`, etc.)
- Modify tests outside `.github/`
- Modify build system outside `.github/`
- Are not `.github/`-only AND don't start with "dev setup"

**These will cause sync failures** and require manual resolution.

## Branch Strategy

### Master Branch
- **Purpose:** Mirror of upstream `postgres/postgres` + local CI/CD + dev environment
- **Updates:** Automatic hourly sync from upstream
- **Direct commits:** Only `.github/` changes or "dev setup" commits
- **All other work:** Use feature branches

### Feature Branches
- **Purpose:** All PostgreSQL development work
- **Pattern:** `feature/*`, `dev/*`, `experiment/*`
- **Workflow:**
  ```bash
  git checkout master
  git pull origin master
  git checkout -b feature/my-feature
  # Make changes...
  git push origin feature/my-feature
  # Create PR: feature/my-feature → master
  ```

## Sync Workflow Behavior

### Scenario 1: No Local Commits
```
Upstream: A---B---C
Master:   A---B---C
```
**Result:** ✅ Already up to date (no action needed)

### Scenario 2: Only .github/ Commits
```
Upstream: A---B---C---D
Master:   A---B---C---X (X modifies .github/ only)
```
**Result:** ✅ Merge commit created
```
Master:   A---B---C---X---M
                    \     /
                     D---/
```

### Scenario 3: Only "dev setup" Commits
```
Upstream: A---B---C---D
Master:   A---B---C---Y (Y is "dev setup v19")
```
**Result:** ✅ Merge commit created
```
Master:   A---B---C---Y---M
                    \     /
                     D---/
```

### Scenario 4: Mix of Allowed Commits
```
Upstream: A---B---C---D
Master:   A---B---C---X---Y (X=.github/, Y=dev setup)
```
**Result:** ✅ Merge commit created

### Scenario 5: Code Changes (Violation)
```
Upstream: A---B---C---D
Master:   A---B---C---Z (Z modifies src/backend/)
```
**Result:** ❌ Sync fails, issue created

**Recovery:**
1. Create feature branch from Z
2. Reset master to match upstream
3. Rebase feature branch
4. Create PR

## Updating Dev Setup

When you update your development environment:

```bash
# Make changes to .clangd, flake.nix, etc.
git add .clangd flake.nix .vscode/

# Important: Start message with "dev setup"
git commit -m "dev setup v20: Update clangd config and add new aliases"

git push origin master
```

The sync workflow will recognize this as a dev setup commit and preserve it during merges.

**Naming convention:**
- ✅ `dev setup v20`
- ✅ `Dev setup: Update IDE config`
- ✅ `DEV SETUP - Add debugging tools`
- ❌ `Update development environment` (doesn't start with "dev setup")
- ❌ `dev environment changes` (doesn't start with "dev setup")

## Sync Failure Recovery

If sync fails because of non-allowed commits:

### Check What's Wrong
```bash
git fetch origin
git fetch upstream https://github.com/postgres/postgres.git master

# See which commits are problematic
git log upstream/master..origin/master --oneline

# See which files were changed
git diff --name-only upstream/master...origin/master
```

### Option 1: Make Commit Acceptable

If the commit should have been a "dev setup" commit:

```bash
# Amend the commit message
git commit --amend -m "dev setup v21: Previous changes"
git push origin master --force-with-lease
```

### Option 2: Move to Feature Branch

If the commit contains code changes:

```bash
# Create feature branch
git checkout -b feature/recovery origin/master

# Reset master to upstream
git checkout master
git reset --hard upstream/master
git push origin master --force

# Your changes are safe in feature/recovery
git checkout feature/recovery
# Create PR when ready
```

## FAQ

**Q: Why allow dev setup commits on master?**
A: Development environment configuration is personal, frequently updated, and doesn't affect the codebase or CI/CD. It's more convenient to keep it on master than manage separate branches.

**Q: What if I forget to name it "dev setup"?**
A: Sync will fail. You can amend the commit message (see recovery above) or move the commit to a feature branch.

**Q: Can I have both .github/ and dev setup changes in one commit?**
A: Yes! The sync workflow allows commits that modify .github/, or are named "dev setup", or both.

**Q: What if upstream modifies the same files as my dev setup commit?**
A: The sync will attempt to merge automatically. If there are conflicts, you'll need to resolve them manually (rare, since upstream shouldn't touch personal dev files).

**Q: Can I reorder commits on master?**
A: It's not recommended due to complexity. The sync workflow handles commits in any order as long as they follow the policy.

## Monitoring

**Check sync status:**
- Actions → "Sync from Upstream (Automatic)"
- Look for green ✅ on recent runs

**Check for policy violations:**
- Open issues with label `sync-failure`
- These indicate commits that violated the pristine master policy

## Related Documentation

- [Sync Setup Guide](sync-setup.md) - Detailed sync workflow documentation
- [QUICKSTART](../QUICKSTART.md) - Quick setup guide
- [README](../README.md) - System overview

# Automated Upstream Sync Documentation

## Overview

This repository maintains a mirror of the official PostgreSQL repository at `postgres/postgres`. The sync system automatically keeps the `master` branch synchronized with upstream changes.

## System Components

### 1. Automatic Daily Sync
**File:** `.github/workflows/sync-upstream.yml`

- **Trigger:** Daily at 00:00 UTC (cron schedule)
- **Purpose:** Automatically sync master branch without manual intervention
- **Process:**
  1. Fetches latest commits from `postgres/postgres`
  2. Fast-forward merges to local master (conflict-free)
  3. Pushes to `origin/master`
  4. Creates GitHub issue if conflicts detected
  5. Closes existing sync-failure issues on success

### 2. Manual Sync Workflow
**File:** `.github/workflows/sync-upstream-manual.yml`

- **Trigger:** Manual via Actions tab → "Sync from Upstream (Manual)" → Run workflow
- **Purpose:** Testing and on-demand syncs
- **Options:**
  - `force_push`: Use `--force-with-lease` when pushing (default: true)

## Branch Strategy

### Critical Rule: Master is Pristine

- **master branch:** Mirror only - pristine copy of `postgres/postgres`
- **All development:** Feature branches (e.g., `feature/hot-updates`, `experiment/zheap`)
- **Never commit directly to master** - this will cause sync failures

### Feature Branch Workflow

```bash
# Start new feature from latest master
git checkout master
git pull origin master
git checkout -b feature/my-feature

# Work on feature
git commit -m "Add feature"

# Keep feature updated with upstream
git checkout master
git pull origin master
git checkout feature/my-feature
git rebase master

# Push feature branch
git push origin feature/my-feature

# Create PR: feature/my-feature → master
```

## Sync Failure Recovery

### Diagnosis

If sync fails, you'll receive a GitHub issue with label `sync-failure`. Check what commits are on master but not upstream:

```bash
# Clone or update your local repository
git fetch origin
git fetch upstream https://github.com/postgres/postgres.git master

# View conflicting commits
git log upstream/master..origin/master --oneline

# See detailed changes
git diff upstream/master...origin/master
```

### Recovery Option 1: Preserve Commits (Recommended)

If the commits on master should be kept:

```bash
# Create backup branch from current master
git checkout origin/master
git checkout -b recovery/master-backup-$(date +%Y%m%d)
git push origin recovery/master-backup-$(date +%Y%m%d)

# Reset master to upstream
git checkout master
git reset --hard upstream/master
git push origin master --force

# Create feature branch from backup
git checkout -b feature/recovered-work recovery/master-backup-$(date +%Y%m%d)

# Optional: rebase onto new master
git rebase master

# Push feature branch
git push origin feature/recovered-work

# Create PR: feature/recovered-work → master
```

### Recovery Option 2: Discard Commits

If the commits on master were mistakes or already merged upstream:

```bash
git checkout master
git reset --hard upstream/master
git push origin master --force
```

### Verification

After recovery, verify sync status:

```bash
# Check that master matches upstream
git log origin/master --oneline -10
git log upstream/master --oneline -10

# These should be identical

# Or run manual sync workflow
# GitHub → Actions → "Sync from Upstream (Manual)" → Run workflow
```

The automatic sync will resume on next scheduled run (00:00 UTC daily).

## Monitoring

### Success Indicators

- ✓ GitHub Actions badge shows passing
- ✓ No open issues with label `sync-failure`
- ✓ `master` branch commit history matches `postgres/postgres`

### Check Sync Status

**Via GitHub UI:**
1. Go to: Actions → "Sync from Upstream (Automatic)"
2. Check latest run status

**Via Git:**
```bash
git fetch origin
git fetch upstream https://github.com/postgres/postgres.git master
git log origin/master..upstream/master --oneline

# No output = fully synced
# Commits listed = behind upstream (sync pending or failed)
```

**Via API:**
```bash
# Check latest workflow run
gh run list --workflow=sync-upstream.yml --limit 1

# View run details
gh run view <run-id>
```

### Sync Lag

Expected lag: <1 hour from upstream commit to mirror

- Upstream commits at 12:30 UTC → Synced at next daily run (00:00 UTC next day) = ~11.5 hours max
- For faster sync: Manually trigger workflow after major upstream merges

## Configuration

### GitHub Actions Permissions

Required settings (already configured):

1. **Settings → Actions → General → Workflow permissions:**
   - ✓ "Read and write permissions"
   - ✓ "Allow GitHub Actions to create and approve pull requests"

2. **Repository Settings → Branches:**
   - Consider: Branch protection rule on `master` to prevent direct pushes
   - Exception: Allow `github-actions[bot]` to push

### Adjusting Sync Schedule

Edit `.github/workflows/sync-upstream.yml`:

```yaml
on:
  schedule:
    # Current: Daily at 00:00 UTC
    - cron: '0 0 * * *'

    # Examples:
    # Every 6 hours: '0 */6 * * *'
    # Twice daily: '0 0,12 * * *'
    # Weekdays only: '0 0 * * 1-5'
```

**Recommendation:** Keep daily schedule to balance freshness with API usage.

## Troubleshooting

### Issue: Workflow not running

**Check:**
1. Actions tab → Check if workflow is disabled
2. Settings → Actions → Ensure workflows are enabled for repository

**Fix:**
- Enable workflow: Actions → Select workflow → "Enable workflow"

### Issue: Permission denied on push

**Check:**
- Settings → Actions → General → Workflow permissions

**Fix:**
- Set to "Read and write permissions"
- Enable "Allow GitHub Actions to create and approve pull requests"

### Issue: Merge conflicts every sync

**Root cause:** Commits being made directly to master

**Fix:**
1. Review `.git/hooks/` for pre-commit hooks that might auto-commit
2. Check if any automation is committing to master
3. Enforce branch protection rules
4. Educate team members on feature branch workflow

### Issue: Sync successful but CI fails

**This is expected** if upstream introduced breaking changes or test failures.

**Handling:**
- Upstream tests failures are upstream's responsibility
- Focus: Ensure mirror stays in sync
- Separate: Your feature branches should pass CI

## Cost and Usage

### GitHub Actions Minutes

- **Sync workflow:** ~2-3 minutes per run
- **Frequency:** Daily = 60-90 minutes/month
- **Free tier:** 2,000 minutes/month (public repos: unlimited)
- **Cost:** $0 (well within limits)

### Network Usage

- Fetches only new commits (incremental)
- Typical: <10 MB per sync
- Total: <300 MB/month

## Security Considerations

### Secrets

- Uses `GITHUB_TOKEN` (automatically provided, scoped to repository)
- No additional secrets required
- Token permissions: Minimum necessary (contents:write, issues:write)

### Audit Trail

All syncs are logged:
- GitHub Actions run history (90 days retention)
- Git reflog on server
- Issue creation/closure for failures

## Integration with Other Workflows

### Cirrus CI

Cirrus CI tests trigger on pushes to master:
- Sync pushes → Cirrus CI runs tests on synced commits
- This validates upstream changes against your test matrix

### AI Code Review

AI review workflows trigger on PRs, not master pushes:
- Sync to master does NOT trigger AI reviews
- Feature branch PRs → master do trigger AI reviews

### Windows Builds

Windows dependency builds trigger on master pushes:
- Sync pushes → Windows builds run
- Ensures dependencies stay compatible with latest upstream

## Support

### Reporting Issues

If sync consistently fails:

1. Check open issues with label `sync-failure`
2. Review workflow logs: Actions → Failed run → View logs
3. Create issue with:
   - Workflow run URL
   - Error messages from logs
   - Output of `git log upstream/master..origin/master`

### Disabling Automatic Sync

If needed (e.g., during major refactoring):

```bash
# Disable via GitHub UI
# Actions → "Sync from Upstream (Automatic)" → "..." → Disable workflow

# Or delete/rename the workflow file
git mv .github/workflows/sync-upstream.yml .github/workflows/sync-upstream.yml.disabled
git commit -m "Temporarily disable automatic sync"
git push
```

**Remember to re-enable** once work is complete.

## References

- Upstream repository: https://github.com/postgres/postgres
- GitHub Actions docs: https://docs.github.com/en/actions
- Git branching strategies: https://git-scm.com/book/en/v2/Git-Branching-Branching-Workflows

# PostgreSQL Mirror CI/CD System

This directory contains the CI/CD infrastructure for the PostgreSQL personal mirror repository.

## System Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    PostgreSQL Mirror CI/CD                   │
└─────────────────────────────────────────────────────────────┘
                               │
        ┌──────────────────────┼──────────────────────┐
        │                      │                      │
   [1] Sync              [2] AI Review         [3] Windows
   Daily @ 00:00         On PR Events          On Master Push
        │                      │                      │
        ▼                      ▼                      ▼
  postgres/postgres     Claude API            Dependency Builds
        │                      │                      │
        ▼                      ▼                      ▼
  github.com/gburd      PR Comments           Build Artifacts
     /postgres/         + Labels              (90-day retention)
      master
```

## Components

### 1. Automated Upstream Sync
**Status:** ✓ Implemented
**Files:** `workflows/sync-upstream*.yml`

Automatically syncs the `master` branch with upstream `postgres/postgres` daily.

- **Frequency:** Daily at 00:00 UTC
- **Trigger:** Cron schedule + manual
- **Features:**
  - Fast-forward merge (conflict-free)
  - Automatic issue creation on conflicts
  - Issue auto-closure on resolution
- **Cost:** Free (~150 min/month, well within free tier)

**Documentation:** [docs/sync-setup.md](docs/sync-setup.md)

### 2. AI-Powered Code Review
**Status:** ✓ Implemented
**Files:** `workflows/ai-code-review.yml`, `scripts/ai-review/`

Uses Claude API to provide PostgreSQL-aware code review on pull requests.

- **Trigger:** PR opened/updated, ready for review
- **Features:**
  - PostgreSQL-specific C code review
  - SQL, documentation, build system review
  - Inline comments on issues
  - Automatic labeling (security, performance, etc.)
  - Cost tracking and limits
  - **Provider Options:** Anthropic API or AWS Bedrock
- **Cost:** $35-50/month (estimated)
- **Model:** Claude 3.5 Sonnet

**Documentation:** [docs/ai-review-guide.md](docs/ai-review-guide.md)

### 3. Windows Build Integration
**Status:** ✅ Implemented
**Files:** `workflows/windows-dependencies.yml`, `windows/`, `scripts/windows/`

Builds PostgreSQL Windows dependencies for x64 Windows.

- **Trigger:** Manual, manifest changes, weekly refresh
- **Features:**
  - Core dependencies: OpenSSL, zlib, libxml2
  - Smart caching by version hash
  - Dependency bundling
  - Artifact publishing (90-day retention)
  - PowerShell download helper
  - **Cost optimization:** Skips builds for pristine commits (dev setup, .github/ only)
- **Cost:** ~$5-8/month (with caching and optimization)

**Documentation:** [docs/windows-builds.md](docs/windows-builds.md) | [Usage](docs/windows-builds-usage.md)

## Quick Start

### Prerequisites

1. **GitHub Actions enabled:**
   - Settings → Actions → General → Allow all actions

2. **Workflow permissions:**
   - Settings → Actions → General → Workflow permissions
   - Select: "Read and write permissions"
   - Enable: "Allow GitHub Actions to create and approve pull requests"

3. **Secrets configured:**
   - **Option A - Anthropic API:**
     - Settings → Secrets and variables → Actions
     - Add: `ANTHROPIC_API_KEY` (get from https://console.anthropic.com/)
   - **Option B - AWS Bedrock:**
     - Add: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`
     - See: [docs/bedrock-setup.md](docs/bedrock-setup.md)

### Using the Sync System

**Manual sync:**
```bash
# Via GitHub UI:
# Actions → "Sync from Upstream (Manual)" → Run workflow

# Via GitHub CLI:
gh workflow run sync-upstream-manual.yml
```

**Check sync status:**
```bash
# Latest sync run
gh run list --workflow=sync-upstream.yml --limit 1

# View details
gh run view <run-id>
```

### Using AI Code Review

AI reviews run automatically on PRs. To test manually:

```bash
# Via GitHub UI:
# Actions → "AI Code Review" → Run workflow → Enter PR number

# Via GitHub CLI:
gh workflow run ai-code-review.yml -f pr_number=123
```

**Reviewing AI feedback:**
1. AI posts inline comments on specific lines
2. AI posts summary comment with overview
3. AI adds labels (security-concern, needs-tests, etc.)
4. Review and address feedback like human reviewer comments

### Cost Monitoring

**View AI review costs:**
```bash
# Download cost logs
gh run download <run-id> -n ai-review-cost-log-<pr-number>
```

**Expected monthly costs (with optimizations):**
- Sync: $0 (free tier)
- AI Review: $30-45 (only on PRs, skips drafts)
- Windows Builds: $5-8 (caching + pristine commit skipping)
- **Total: $35-53/month**

**Cost optimizations:**
- Windows builds skip "dev setup" and .github/-only commits
- AI review only runs on non-draft PRs
- Aggressive caching reduces build times by 80-90%
- See [Cost Optimization Guide](docs/cost-optimization.md) for details

## Workflow Files

### Sync Workflows
- `workflows/sync-upstream.yml` - Automatic daily sync
- `workflows/sync-upstream-manual.yml` - Manual testing sync

### AI Review Workflows
- `workflows/ai-code-review.yml` - Automatic PR review

### Windows Build Workflows
- `workflows/windows-dependencies.yml` - Dependency builds (TBD)

## Configuration Files

### AI Review Configuration
- `scripts/ai-review/config.json` - Cost limits, file patterns, labels
- `scripts/ai-review/prompts/*.md` - Review prompts by file type
- `scripts/ai-review/package.json` - Node.js dependencies

### Windows Build Configuration
- `windows/manifest.json` - Dependency versions (TBD)

## Branch Strategy

### Master Branch: Mirror Only
- **Purpose:** Pristine copy of `postgres/postgres`
- **Rule:** Never commit directly to master
- **Sync:** Automatic via GitHub Actions
- **Protection:** Consider branch protection rules

### Feature Branches: Development
- **Pattern:** `feature/*`, `dev/*`, `experiment/*`
- **Workflow:**
  ```bash
  git checkout master
  git pull origin master
  git checkout -b feature/my-feature
  # ... make changes ...
  git push origin feature/my-feature
  # Create PR: feature/my-feature → master
  ```

### Special Branches
- `recovery/*` - Temporary branches for sync conflict resolution
- Development remotes: commitfest, heikki, orioledb, zheap

## Integration with Cirrus CI

GitHub Actions and Cirrus CI run independently:

- **Cirrus CI:** Comprehensive testing (Linux, FreeBSD, macOS, Windows)
- **GitHub Actions:** Sync, AI review, Windows dependency builds
- **No conflicts:** Both can run on same commits

## Troubleshooting

### Sync Issues

**Problem:** Sync workflow failing
**Check:** Actions → "Sync from Upstream (Automatic)" → Latest run
**Fix:** See [docs/sync-setup.md](docs/sync-setup.md#sync-failure-recovery)

### AI Review Issues

**Problem:** AI review not running
**Check:** Is PR a draft? Draft PRs are skipped
**Fix:** Mark PR as ready for review

**Problem:** AI review too expensive
**Check:** Cost logs in workflow artifacts
**Fix:** Adjust limits in `scripts/ai-review/config.json`

### Workflow Permission Issues

**Problem:** "Resource not accessible by integration"
**Check:** Settings → Actions → General → Workflow permissions
**Fix:** Enable "Read and write permissions"

## Security

### Secrets Management
- `ANTHROPIC_API_KEY`: Claude API key (required for AI review)
- `GITHUB_TOKEN`: Auto-generated, scoped to repository
- Never commit secrets to repository
- Rotate API keys quarterly

### Permissions
- Workflows use minimum necessary permissions
- `contents: read` for code access
- `pull-requests: write` for comments
- `issues: write` for sync failure issues

### Audit Trail
- All workflow runs logged (90-day retention)
- Cost tracking for AI reviews
- GitHub Actions audit log available

## Support and Documentation

### Detailed Documentation
- [Sync Setup Guide](docs/sync-setup.md) - Upstream sync system
- [AI Review Guide](docs/ai-review-guide.md) - AI code review system
- [Windows Builds Guide](docs/windows-builds.md) - Windows dependencies
- [Cost Optimization Guide](docs/cost-optimization.md) - Reducing CI/CD costs
- [Pristine Master Policy](docs/pristine-master-policy.md) - Master branch management

### Reporting Issues

Issues with CI/CD system:
1. Check workflow logs: Actions → Failed run → View logs
2. Search existing issues: label:automation
3. Create issue with workflow run URL and error messages

### Modifying Workflows

**Disabling a workflow:**
```bash
# Via GitHub UI:
# Actions → Select workflow → "..." → Disable workflow

# Via git:
git mv .github/workflows/workflow-name.yml .github/workflows/workflow-name.yml.disabled
git commit -m "Disable workflow"
```

**Testing workflow changes:**
1. Create feature branch
2. Modify workflow file
3. Use `workflow_dispatch` trigger to test
4. Verify in Actions tab
5. Merge to master when working

## Cost Summary

| Component | Monthly Cost | Usage | Notes |
|-----------|-------------|-------|-------|
| Sync | $0 | ~150 min | Free tier: 2,000 min |
| AI Review | $30-45 | Variable | Claude API usage-based |
| Windows Builds | $5-8 | ~2,500 min | With caching + optimization |
| **Total** | **$35-53** | | After cost optimizations |

**Comparison:** CodeRabbit (turnkey solution) = $99-499/month

**Cost savings:** ~40-47% reduction through optimizations (see [Cost Optimization Guide](docs/cost-optimization.md))

## References

- PostgreSQL: https://github.com/postgres/postgres
- GitHub Actions: https://docs.github.com/en/actions
- Claude API: https://docs.anthropic.com/
- Cirrus CI: https://cirrus-ci.org/
- winpgbuild: https://github.com/dpage/winpgbuild

---

**Last Updated:** 2026-03-10
**Maintained by:** PostgreSQL Mirror Automation

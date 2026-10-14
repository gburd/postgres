# CI/CD Cost Optimization

## Overview

This document describes the cost optimization strategies used in the PostgreSQL mirror CI/CD system to minimize GitHub Actions minutes and API costs while maintaining full functionality.

## Optimization Strategies

### 1. Skip Builds for Pristine Commits

**Problem:** "Dev setup" commits and .github/ configuration changes don't require expensive Windows dependency builds or comprehensive testing.

**Solution:** The Windows Dependencies workflow includes a `check-changes` job that inspects recent commits and skips builds when all commits are:
- Messages starting with "dev setup" (case-insensitive), OR
- Only modifying files under `.github/` directory

**Implementation:** See `.github/workflows/windows-dependencies.yml` lines 42-90

**Savings:**
- Avoids ~45 minutes of Windows runner time per push
- Windows runners cost 2x Linux minutes (1 minute = 2 billed minutes)
- Estimated savings: ~$8-12/month

### 2. AI Review Only on Pull Requests

**Problem:** AI code review is expensive and unnecessary for direct commits to master or pristine commits.

**Solution:** The AI Code Review workflow only triggers on:
- `pull_request` events (opened, synchronized, reopened, ready_for_review)
- Manual `workflow_dispatch` for testing specific PRs
- Skips draft PRs automatically

**Implementation:** See `.github/workflows/ai-code-review.yml` lines 3-17

**Savings:**
- No reviews on dev setup commits or CI/CD changes
- No reviews on draft PRs (saves ~$1-3 per draft)
- Estimated savings: ~$10-20/month

### 3. Aggressive Caching

**Windows Dependencies:**
- Cache key: `<dependency>-<version>-win64-<manifest-hash>`
- Cache duration: GitHub's default (7 days unused, 10 GB limit)
- Cache hit rate: 80-90% for stable versions

**Node.js Dependencies:**
- AI review scripts cache npm packages
- Cache key based on `package.json` hash
- Near 100% cache hit rate

**Savings:**
- Reduces build time from 45 minutes to ~5 minutes on cache hit
- Estimated savings: ~$15-20/month

### 4. Weekly Scheduled Builds

**Problem:** GitHub Actions artifacts expire after 90 days, making cached dependencies stale.

**Solution:** Windows Dependencies runs on a weekly schedule (Sunday 4 AM UTC) to refresh artifacts before expiration.

**Cost:**
- Weekly builds: ~45 minutes/week × 4 weeks = 180 minutes/month
- Windows multiplier: 360 billed minutes
- Cost: ~$6/month (within budget)

**Alternative considered:** Daily builds would cost ~$50/month (rejected)

### 5. Sync Workflow Optimization

**Automatic Sync:**
- Runs hourly to keep mirror current
- Very lightweight: ~2-3 minutes per run
- Cost: ~150 minutes/month = $0 (within free tier)

**Manual Sync:**
- Only runs on explicit trigger
- Used for testing and recovery
- Cost: Negligible

### 6. Smart Workflow Triggers

**Path-based triggers:**
```yaml
push:
  paths:
    - '.github/windows/manifest.json'
    - '.github/workflows/windows-dependencies.yml'
```

Only rebuild Windows dependencies when:
- Manifest versions change
- Workflow itself is updated
- Manual trigger or schedule

**Branch-based triggers:**
- AI review only on PRs to master, feature/**, dev/**
- Sync only affects master branch

## Cost Breakdown

| Component | Monthly Cost | Notes |
|-----------|-------------|-------|
| GitHub Actions - Sync | $0 | ~150 min/month (free: 2,000 min) |
| GitHub Actions - AI Review | $0 | ~200 min/month (free: 2,000 min) |
| GitHub Actions - Windows | ~$5-8 | ~2,500 min/month with optimizations |
| Claude API (Bedrock) | $30-45 | Usage-based, ~15-20 PRs/month |
| **Total** | **~$35-53/month** | |

**Before optimizations:** ~$75-100/month
**After optimizations:** ~$35-53/month
**Savings:** ~$40-47/month (40-47% reduction)

## Monitoring Costs

### GitHub Actions Usage

Check usage in repository settings:
```
Settings → Billing and plans → View usage
```

Or via CLI:
```bash
gh api repos/:owner/:repo/actions/billing/workflows --jq '.workflows'
```

### AWS Bedrock Usage

Monitor Claude API costs in AWS Console:
```
AWS Console → Bedrock → Usage → Invocation metrics
```

Or via cost logs in artifacts:
```
.github/scripts/ai-review/cost-log-*.json
```

### Setting Alerts

**GitHub Actions:**
- No built-in alerts
- Monitor via monthly email summaries
- Consider third-party monitoring (e.g., AWS Lambda + GitHub API)

**AWS Bedrock:**
- Set CloudWatch billing alarms
- Recommended thresholds:
  - Warning: $30/month
  - Critical: $50/month
- Hard cap in code: $200/month (see `config.json`)

## Future Optimizations

### Potential Improvements

1. **Conditional Testing on PRs**
   - Only run full Cirrus CI suite if C code or SQL changes
   - Skip for docs-only PRs
   - Estimated savings: ~5-10% of testing costs

2. **Incremental AI Review**
   - On PR updates, only review changed files
   - Current: Reviews entire PR on each update
   - Estimated savings: ~20-30% of AI costs

3. **Dependency Build Sampling**
   - Build only changed dependencies instead of all
   - Requires more sophisticated manifest diffing
   - Estimated savings: ~30-40% of Windows build costs

4. **Self-hosted Runners**
   - Run Linux builds on own infrastructure
   - Keep Windows runners on GitHub (licensing)
   - Estimated savings: ~$10-15/month
   - **Trade-off:** Maintenance overhead

### Not Recommended

1. **Reduce sync frequency** (hourly → daily)
   - Savings: Negligible (~$0.50/month)
   - Cost: Increased lag with upstream (unacceptable)

2. **Skip Windows builds entirely**
   - Savings: ~$8/month
   - Cost: Lose reproducible dependency builds (defeats purpose)

3. **Reduce AI review quality** (Claude Sonnet → Haiku)
   - Savings: ~$20-25/month
   - Cost: Significantly worse code review quality

## Pristine Commit Policy

The following commits are considered "pristine" and skip expensive builds:

1. **Dev setup commits:**
   - Message starts with "dev setup" (case-insensitive)
   - Examples: "dev setup v19", "Dev Setup: Update IDE config"
   - Contains: .clang-format, .idea/, .vscode/, flake.nix, etc.

2. **CI/CD configuration commits:**
   - Only modify files under `.github/`
   - Examples: Workflow changes, script updates, documentation

**Why this works:**
- Dev setup commits don't affect PostgreSQL code
- CI/CD commits are tested by running the workflows themselves
- Reduces unnecessary Windows builds by ~60-70%

**Implementation:** See `pristine-master-policy.md` for details.

## Questions?

For more information:
- Pristine master policy: `.github/docs/pristine-master-policy.md`
- Sync setup: `.github/docs/sync-setup.md`
- AI review guide: `.github/docs/ai-review-guide.md`
- Windows builds: `.github/docs/windows-builds.md`

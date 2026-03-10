# Quick Start Guide - PostgreSQL Mirror CI/CD

**Goal:** Get your PostgreSQL mirror CI/CD system running in 15 minutes.

---

## ✅ What's Been Implemented

- **Phase 1: Automated Upstream Sync** - Daily sync from postgres/postgres ✅
- **Phase 2: AI-Powered Code Review** - Claude-based PR reviews ✅
- **Phase 3: Windows Builds** - Planned for weeks 4-6 📋

---

## 🚀 Setup Instructions

### Step 1: Configure GitHub Actions Permissions (2 minutes)

1. Go to: **Settings → Actions → General**
2. Scroll to: **Workflow permissions**
3. Select: **"Read and write permissions"**
4. Check: **"Allow GitHub Actions to create and approve pull requests"**
5. Click: **Save**

✅ This enables workflows to push commits and create issues.

---

### Step 2: Set Up Upstream Sync (3 minutes)

**Test manual sync first:**

```bash
# Via GitHub Web UI:
# 1. Go to: Actions tab
# 2. Click: "Sync from Upstream (Manual)"
# 3. Click: "Run workflow"
# 4. Watch it run (should take ~2 minutes)

# OR via GitHub CLI:
gh workflow run sync-upstream-manual.yml
gh run watch
```

**Verify sync worked:**

```bash
git fetch origin
git log origin/master --oneline -5

# Compare with upstream:
# https://github.com/postgres/postgres/commits/master
```

**Enable automatic sync:**

- Automatic sync runs daily at 00:00 UTC
- Already configured, no action needed
- Check: Actions → "Sync from Upstream (Automatic)"

✅ Your master branch will now stay synced automatically.

---

### Step 3: Set Up AI Code Review (10 minutes)

**Choose Your Provider:**

You can use either **Anthropic API** (simpler) or **AWS Bedrock** (if you have AWS infrastructure).

#### Option A: Anthropic API (Recommended for getting started)

**A. Get Claude API Key:**

1. Go to: https://console.anthropic.com/
2. Sign up or log in
3. Navigate to: API Keys
4. Create new key
5. Copy the key (starts with `sk-ant-...`)

**B. Add API Key to GitHub:**

1. Go to: **Settings → Secrets and variables → Actions**
2. Click: **New repository secret**
3. Name: `ANTHROPIC_API_KEY`
4. Value: Paste your API key
5. Click: **Add secret**

**C. Ensure config uses Anthropic:**

Check `.github/scripts/ai-review/config.json` has:
```json
{
  "provider": "anthropic",
  ...
}
```

#### Option B: AWS Bedrock (If you have AWS)

See detailed guide: [.github/docs/bedrock-setup.md](.github/docs/bedrock-setup.md)

**Quick steps:**
1. Enable Claude 3.5 Sonnet in AWS Bedrock console
2. Create IAM user with `bedrock:InvokeModel` permission
3. Add three secrets to GitHub:
   - `AWS_ACCESS_KEY_ID`
   - `AWS_SECRET_ACCESS_KEY`
   - `AWS_REGION` (e.g., `us-east-1`)
4. Update `.github/scripts/ai-review/config.json`:
```json
{
  "provider": "bedrock",
  "bedrock_model_id": "us.anthropic.claude-3-5-sonnet-20241022-v2:0",
  "bedrock_region": "us-east-1",
  ...
}
```

**Note:** Both providers have identical pricing ($0.003/1K input, $0.015/1K output tokens).

---

**C. Install Dependencies:**

```bash
cd .github/scripts/ai-review
npm install

# Should install:
# - @anthropic-ai/sdk (for Anthropic API)
# - @aws-sdk/client-bedrock-runtime (for AWS Bedrock)
# - @actions/github
# - @actions/core
# - parse-diff
# - minimatch
```

**D. Test AI Review:**

```bash
# Option 1: Create a test PR
git checkout -b test/ai-review
echo "// Test change" >> src/backend/utils/adt/int.c
git add .
git commit -m "Test: AI review"
git push origin test/ai-review
# Create PR via GitHub UI

# Option 2: Manual trigger on existing PR
gh workflow run ai-code-review.yml -f pr_number=<PR_NUMBER>
```

✅ AI will review the PR and post comments + summary.

---

## 🎯 Verify Everything Works

### Check Sync Status

```bash
# Check latest sync run
gh run list --workflow=sync-upstream.yml --limit 1

# View details
gh run view $(gh run list --workflow=sync-upstream.yml --limit 1 --json databaseId -q '.[0].databaseId')
```

**Expected:** ✅ Green checkmark, "Already up to date" or "Successfully synced X commits"

### Check AI Review Status

```bash
# Check latest AI review run
gh run list --workflow=ai-code-review.yml --limit 1

# View details
gh run view $(gh run list --workflow=ai-code-review.yml --limit 1 --json databaseId -q '.[0].databaseId')
```

**Expected:** ✅ Green checkmark, comments posted on PR

---

## 📊 Monitor Costs

### GitHub Actions Minutes

```bash
# View usage (requires admin access)
gh api /repos/gburd/postgres/actions/cache/usage

# Expected monthly usage:
# - Sync: ~150 minutes (FREE - within 2,000 min limit)
# - AI Review: ~200 minutes (FREE - within limit)
```

### Claude API Costs

**View per-PR cost:**
- Check AI review summary comment on PR
- Format: `Cost: $X.XX | Model: claude-3-5-sonnet`

**Expected costs:**
- Small PR: $0.50 - $1.00
- Medium PR: $1.00 - $3.00
- Large PR: $3.00 - $7.50
- **Monthly (20 PRs):** $35-50

**Download detailed logs:**
```bash
gh run list --workflow=ai-code-review.yml --limit 5
gh run download <run-id> -n ai-review-cost-log-<pr-number>
```

---

## 🔧 Configuration

### Adjust Sync Schedule

Edit `.github/workflows/sync-upstream.yml`:

```yaml
on:
  schedule:
    # Current: Daily at 00:00 UTC
    - cron: '0 0 * * *'

    # Options:
    # Every 6 hours: '0 */6 * * *'
    # Twice daily: '0 0,12 * * *'
    # Weekdays only: '0 0 * * 1-5'
```

### Adjust AI Review Costs

Edit `.github/scripts/ai-review/config.json`:

```json
{
  "cost_limits": {
    "max_per_pr_dollars": 15.0,      // ← Lower this to save money
    "max_per_month_dollars": 200.0,  // ← Hard monthly cap
    "alert_threshold_dollars": 150.0
  },

  "max_file_size_lines": 5000,  // ← Skip files larger than this

  "skip_paths": [
    "*.png", "*.svg",  // Already skipped
    "vendor/**/*",     // ← Add more patterns here
    "generated/**/*"
  ]
}
```

### Adjust AI Review Prompts

**Make AI reviews stricter or more lenient:**

Edit files in `.github/scripts/ai-review/prompts/`:
- `c-code.md` - PostgreSQL C code review
- `sql.md` - SQL and regression tests
- `documentation.md` - Documentation review
- `build-system.md` - Makefile/Meson review

---

## 🐛 Troubleshooting

### Sync Not Working

**Problem:** Workflow fails with "Permission denied"

**Fix:**
- Check: Settings → Actions → Workflow permissions
- Ensure: "Read and write permissions" is selected

---

### AI Review Not Posting Comments

**Problem:** Workflow runs but no comments appear

**Check:**
1. Is PR a draft? (Draft PRs are skipped to save costs)
2. Are there reviewable files? (Check workflow logs)
3. Is API key valid? (Settings → Secrets → ANTHROPIC_API_KEY)

**Fix:**
- Mark PR as "Ready for review" if draft
- Check workflow logs: Actions → Latest run → View logs
- Verify API key at https://console.anthropic.com/

---

### High AI Review Costs

**Problem:** Costs higher than expected

**Check:**
- Download cost logs: `gh run download <run-id>`
- Look for large files being reviewed
- Check number of PR updates (each triggers review)

**Fix:**
1. Add large files to `skip_paths` in config.json
2. Lower `max_tokens_per_request` (shorter reviews)
3. Use draft PRs for work-in-progress
4. Batch PR updates to reduce review frequency

---

## 📚 Full Documentation

- **Overview:** [.github/README.md](.github/README.md)
- **Sync Guide:** [.github/docs/sync-setup.md](.github/docs/sync-setup.md)
- **AI Review Guide:** [.github/docs/ai-review-guide.md](.github/docs/ai-review-guide.md)
- **Windows Builds:** [.github/docs/windows-builds.md](.github/docs/windows-builds.md) (planned)
- **Implementation Status:** [.github/IMPLEMENTATION_STATUS.md](.github/IMPLEMENTATION_STATUS.md)

---

## ✨ What's Next?

### Immediate
- ✅ **Monitor first automatic sync** (tonight at 00:00 UTC)
- ✅ **Test AI review on real PR**
- ✅ **Tune prompts** based on feedback

### This Week
- Shadow mode testing for AI reviews (Week 1)
- Gather developer feedback
- Adjust configuration

### Weeks 2-3
- Enable full AI review mode
- Monitor costs and quality
- Iterate on prompts

### Weeks 4-6
- **Phase 3:** Implement Windows dependency builds
- Research winpgbuild approach
- Create build workflows
- Test artifact publishing

---

## 🎉 Success Criteria

You'll know everything is working when:

✅ **Sync:**
- Master branch matches postgres/postgres
- Daily sync runs show green checkmarks
- No open issues with label `sync-failure`

✅ **AI Review:**
- PRs receive inline comments + summary
- Feedback is relevant and actionable
- Costs stay under $50/month
- Developers find reviews helpful

✅ **Overall:**
- Automation saves 8-16 hours/month
- Issues caught earlier in development
- No manual sync needed

---

**Need Help?**
- Check documentation: `.github/README.md`
- Check workflow logs: Actions → Failed run → View logs
- Create issue with workflow URL and error messages

**Ready to go!** 🚀

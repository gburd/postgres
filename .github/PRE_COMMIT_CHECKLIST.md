# Pre-Commit Checklist - CI/CD Setup Verification

**Date:** 2026-03-10
**Repository:** github.com/gburd/postgres

Run through this checklist before committing and pushing the CI/CD configuration.

---

## ✅ Requirement 1: Multi-Platform CI Testing

**Status:** ✅ **ALREADY CONFIGURED** (via Cirrus CI)

Your repository already has Cirrus CI configured via `.cirrus.yml`:
- ✅ Linux (multiple distributions)
- ✅ FreeBSD
- ✅ macOS
- ✅ Windows
- ✅ Other PostgreSQL-supported platforms

**GitHub Actions we added are for:**
- Upstream sync (not CI testing)
- AI code review (not CI testing)

**No action needed** - Cirrus CI handles all platform testing.

**Verify Cirrus CI is active:**
```bash
# Check if you have recent Cirrus CI builds
# Visit: https://cirrus-ci.com/github/gburd/postgres
```

---

## ✅ Requirement 2: Bedrock Claude 4.5 for PR Reviews

### Configuration Status

**File:** `.github/scripts/ai-review/config.json`
```json
{
  "provider": "bedrock",
  "bedrock_model_id": "us.anthropic.claude-sonnet-4-5-20250929-v1:0",
  "bedrock_region": "us-east-1"
}
```

✅ Provider set to Bedrock
✅ Model ID configured for Claude Sonnet 4.5

### Required GitHub Secrets

Before pushing, verify these secrets exist:

**Settings → Secrets and variables → Actions**

1. **AWS_ACCESS_KEY_ID**
   - [ ] Secret exists
   - Value: Your AWS access key ID

2. **AWS_SECRET_ACCESS_KEY**
   - [ ] Secret exists
   - Value: Your AWS secret access key

3. **AWS_REGION**
   - [ ] Secret exists
   - Value: `us-east-1` (or your preferred region)

4. **GITHUB_TOKEN**
   - [ ] Automatically provided by GitHub Actions
   - No action needed

### AWS Bedrock Requirements

Before pushing, verify in AWS:

1. **Model Access Enabled:**
   ```bash
   # Check if Claude Sonnet 4.5 is enabled
   aws bedrock list-foundation-models \
     --region us-east-1 \
     --by-provider anthropic \
     --query 'modelSummaries[?contains(modelId, `claude-sonnet-4-5`)]'
   ```
   - [ ] Model is available in your region
   - [ ] Model access is granted in Bedrock console

2. **IAM Permissions:**
   - [ ] IAM user/role has `bedrock:InvokeModel` permission
   - [ ] Policy allows access to Claude models

**Test Bedrock access locally:**
```bash
aws bedrock-runtime invoke-model \
  --region us-east-1 \
  --model-id us.anthropic.claude-sonnet-4-5-20250929-v1:0 \
  --body '{"anthropic_version":"bedrock-2023-05-31","max_tokens":100,"messages":[{"role":"user","content":"Hello"}]}' \
  /tmp/bedrock-test.json

cat /tmp/bedrock-test.json
```
- [ ] Test succeeds (no errors)

### Dependencies Installed

- [ ] Run: `cd .github/scripts/ai-review && npm install`
- [ ] No errors during npm install
- [ ] Packages installed:
  - `@anthropic-ai/sdk`
  - `@aws-sdk/client-bedrock-runtime`
  - `@actions/github`
  - `@actions/core`
  - `parse-diff`
  - `minimatch`

---

## ✅ Requirement 3: Hourly Upstream Sync

### Configuration Status

**File:** `.github/workflows/sync-upstream.yml`
```yaml
on:
  schedule:
    # Run hourly every day
    - cron: '0 * * * *'
```

✅ **UPDATED** - Now runs hourly (every hour on the hour)
✅ Runs every day of the week

**Schedule details:**
- Runs: Every hour at :00 minutes past the hour
- Frequency: 24 times per day
- Days: All 7 days of the week
- Time zone: UTC

**Examples:**
- 00:00 UTC, 01:00 UTC, 02:00 UTC, ... 23:00 UTC
- Converts to your local time automatically

### GitHub Actions Permissions

**Settings → Actions → General → Workflow permissions**

- [ ] **"Read and write permissions"** is selected
- [ ] **"Allow GitHub Actions to create and approve pull requests"** is checked

**Without these, sync will fail with permission errors.**

---

## 📋 Pre-Push Verification Checklist

Run these commands before `git push`:

### 1. Verify File Changes
```bash
cd /home/gburd/ws/postgres/master

# Check what will be committed
git status .github/

# Review the changes
git diff .github/
```

**Expected new/modified files:**
- `.github/workflows/sync-upstream.yml` (modified - hourly sync)
- `.github/workflows/sync-upstream-manual.yml`
- `.github/workflows/ai-code-review.yml`
- `.github/workflows/windows-dependencies.yml` (placeholder)
- `.github/scripts/ai-review/*` (all AI review files)
- `.github/docs/*` (documentation)
- `.github/windows/manifest.json`
- `.github/README.md`
- `.github/QUICKSTART.md`
- `.github/IMPLEMENTATION_STATUS.md`
- `.github/PRE_COMMIT_CHECKLIST.md` (this file)

### 2. Verify Syntax
```bash
# Check YAML syntax (requires yamllint)
yamllint .github/workflows/*.yml 2>/dev/null || echo "yamllint not installed (optional)"

# Check JSON syntax
for f in .github/**/*.json; do
  echo "Checking $f"
  python3 -m json.tool "$f" >/dev/null && echo "  ✓ Valid JSON" || echo "  ✗ Invalid JSON"
done

# Check JavaScript syntax (requires Node.js)
node --check .github/scripts/ai-review/review-pr.js && echo "✓ review-pr.js syntax OK"
```

### 3. Verify Dependencies
```bash
cd .github/scripts/ai-review

# Install dependencies
npm install

# Check for vulnerabilities (optional but recommended)
npm audit
```

### 4. Test Workflows Locally (Optional)

**Install act (GitHub Actions local runner):**
```bash
# See: https://github.com/nektos/act
# Then test workflows:
act -l  # List all workflows
```

### 5. Verify No Secrets in Code
```bash
cd /home/gburd/ws/postgres/master

# Search for potential secrets
grep -r "sk-ant-" .github/ && echo "⚠️  Found potential Anthropic API key!" || echo "✓ No API keys found"
grep -r "AKIA" .github/ && echo "⚠️  Found potential AWS access key!" || echo "✓ No AWS keys found"
grep -r "aws_secret_access_key" .github/ && echo "⚠️  Found potential AWS secret!" || echo "✓ No secrets found"
```

**Result should be:** ✓ No keys/secrets found

---

## 🚀 Commit and Push Commands

Once all checks pass:

```bash
cd /home/gburd/ws/postgres/master

# Stage all CI/CD files
git add .github/

# Commit
git commit -m "Add CI/CD automation: hourly sync, Bedrock AI review, multi-platform CI

- Hourly upstream sync from postgres/postgres
- AI-powered PR reviews using AWS Bedrock Claude Sonnet 4.5
- Multi-platform CI via existing Cirrus CI configuration
- Documentation and setup guides included

See .github/README.md for overview"

# Push to origin
git push origin master
```

---

## 🧪 Post-Push Testing

After pushing, verify everything works:

### Test 1: Manual Sync (2 minutes)

1. Go to: **Actions** tab
2. Click: **"Sync from Upstream (Manual)"**
3. Click: **"Run workflow"**
4. Wait ~2 minutes
5. Verify: ✅ Green checkmark

**Check logs for:**
- "Fetching from upstream postgres/postgres..."
- "Successfully synced" or "Already up to date"

### Test 2: First Automatic Sync (within 1 hour)

Wait for the next hour (e.g., if it's 10:30, wait until 11:00):

1. Go to: **Actions** → **"Sync from Upstream (Automatic)"**
2. Check latest run at the top of the hour
3. Verify: ✅ Green checkmark

### Test 3: AI Review on Test PR (5 minutes)

```bash
# Create test PR
git checkout -b test/ci-verification
echo "// Test CI/CD setup" >> test-file.c
git add test-file.c
git commit -m "Test: Verify CI/CD automation"
git push origin test/ci-verification
```

Then:
1. Create PR via GitHub UI
2. Wait 2-3 minutes
3. Check PR for AI review comments
4. Check **Actions** tab for workflow run
5. Verify workflow logs show: "Using AWS Bedrock as provider"

### Test 4: Cirrus CI Runs (verify existing)

1. Go to: https://cirrus-ci.com/github/gburd/postgres
2. Verify: Recent builds on multiple platforms
3. Check: Linux, FreeBSD, macOS, Windows tests

---

## 📊 Expected Costs

### GitHub Actions Minutes
- Hourly sync: 24 runs/day × 3 min = 72 min/day = ~2,200 min/month
- **Status:** ✅ Within free tier (2,000 min/month for public repos, unlimited for public repos actually)
- AI review: ~200 min/month
- **Total:** ~2,400 min/month (FREE for public repositories)

### AWS Bedrock
- Claude Sonnet 4.5: $0.003/1K input, $0.015/1K output
- Small PR: $0.50-$1.00
- Medium PR: $1.00-$3.00
- Large PR: $3.00-$7.50
- **Expected:** $35-50/month (20 PRs)

### Cirrus CI
- Already configured (existing cost/free tier)

---

## ⚠️ Important Notes

1. **First hourly sync:** Will run at the next hour (e.g., 11:00, 12:00, etc.)

2. **Branch protection:** Consider adding branch protection to master:
   - Settings → Branches → Add rule
   - Branch name: `master`
   - ✅ Require pull request before merging
   - Exception: Allow GitHub Actions bot to push

3. **Cost monitoring:** Set up AWS Budget alerts:
   - AWS Console → Billing → Budgets
   - Create alert at $40/month

4. **Bedrock quotas:** Default quota is usually sufficient, but check:
   ```bash
   aws service-quotas get-service-quota \
     --service-code bedrock \
     --quota-code L-...(varies by region)
   ```

5. **Rate limiting:** If you get many PRs, review rate limits:
   - Bedrock: 200 requests/minute (adjustable)
   - GitHub API: 5,000 requests/hour

---

## 🐛 Troubleshooting

### Sync fails with "Permission denied"
- Check: GitHub Actions permissions (Step "GitHub Actions Permissions" above)

### AI Review fails with "Access denied to model"
- Check: Bedrock model access enabled
- Check: IAM permissions include `bedrock:InvokeModel`

### AI Review fails with "InvalidSignatureException"
- Check: AWS secrets correct in GitHub
- Verify: No extra spaces in secret values

### Hourly sync not running
- Check: Actions are enabled (Settings → Actions)
- Wait: First run is at the next hour boundary

---

## ✅ Final Checklist Before Push

- [ ] All GitHub secrets configured (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION)
- [ ] Bedrock model access enabled for Claude Sonnet 4.5
- [ ] IAM permissions configured
- [ ] npm install completed successfully in .github/scripts/ai-review
- [ ] GitHub Actions permissions set (read+write, create PRs)
- [ ] No secrets committed to code (verified with grep)
- [ ] YAML/JSON syntax validated
- [ ] Reviewed git diff to confirm changes
- [ ] Cirrus CI still active (existing CI not disrupted)

**All items checked?** ✅ **Ready to commit and push!**

---

**Questions or issues?** Check:
- `.github/README.md` - System overview
- `.github/QUICKSTART.md` - Setup guide
- `.github/docs/bedrock-setup.md` - Bedrock details
- `.github/IMPLEMENTATION_STATUS.md` - Implementation status

# AI-Powered Code Review Guide

## Overview

This system uses Claude AI (Anthropic) to provide PostgreSQL-aware code reviews on pull requests. Reviews are similar in style to feedback from the PostgreSQL Hackers mailing list.

## How It Works

```
PR Event (opened/updated)
    ↓
GitHub Actions Workflow Starts
    ↓
Fetch PR diff + metadata
    ↓
Filter reviewable files (.c, .h, .sql, docs, Makefiles)
    ↓
Route each file to appropriate review prompt
    ↓
Send to Claude API with PostgreSQL context
    ↓
Parse response for issues
    ↓
Post inline comments + summary to PR
    ↓
Add labels (security-concern, performance, etc.)
```

## Features

### PostgreSQL-Specific Reviews

**C Code Review:**
- Memory management (palloc/pfree, memory contexts)
- Concurrency (lock ordering, race conditions)
- Error handling (elog/ereport patterns)
- Performance (algorithm complexity, cache efficiency)
- Security (buffer overflows, SQL injection vectors)
- PostgreSQL conventions (naming, comments, style)

**SQL Review:**
- PostgreSQL SQL dialect correctness
- Regression test patterns
- Performance (index usage, join strategy)
- Deterministic output for tests
- Edge case coverage

**Documentation Review:**
- Technical accuracy
- SGML/DocBook format
- PostgreSQL style guide compliance
- Examples and cross-references

**Build System Review:**
- Makefile correctness (GNU Make, PGXS)
- Meson build consistency
- Cross-platform portability
- VPATH build support

### Automatic Labeling

Reviews automatically add labels based on findings:

- `security-concern` - Security issues, vulnerabilities
- `performance-concern` - Performance problems
- `needs-tests` - Missing test coverage
- `needs-docs` - Missing documentation
- `memory-management` - Memory leaks, context issues
- `concurrency-issue` - Deadlocks, race conditions

### Cost Management

- **Per-PR limit:** $15 (configurable)
- **Monthly limit:** $200 (configurable)
- **Alert threshold:** $150
- **Skip draft PRs** to save costs
- **Skip large files** (>5000 lines)
- **Skip binary/generated files**

## Setup

### 1. Install Dependencies

```bash
cd .github/scripts/ai-review
npm install
```

### 2. Configure API Key

Get API key from: https://console.anthropic.com/

Add to repository secrets:
1. Settings → Secrets and variables → Actions
2. New repository secret
3. Name: `ANTHROPIC_API_KEY`
4. Value: Your API key
5. Add secret

### 3. Enable Workflow

The workflow is triggered automatically on PR events:
- PR opened
- PR synchronized (updated)
- PR reopened
- PR marked ready for review (draft → ready)

**Draft PRs are skipped** to save costs.

## Configuration

### Main Configuration: `config.json`

```json
{
  "model": "claude-3-5-sonnet-20241022",
  "max_tokens_per_request": 4096,
  "max_file_size_lines": 5000,

  "cost_limits": {
    "max_per_pr_dollars": 15.0,
    "max_per_month_dollars": 200.0,
    "alert_threshold_dollars": 150.0
  },

  "skip_paths": [
    "*.png", "*.jpg", "*.svg",
    "src/test/regress/expected/*",
    "*.po", "*.pot"
  ],

  "auto_labels": {
    "security-concern": ["security issue", "vulnerability"],
    "performance-concern": ["inefficient", "O(n²)"],
    "needs-tests": ["missing test", "no test coverage"]
  }
}
```

**Tunable parameters:**
- `max_tokens_per_request`: Response length (4096 = ~3000 words)
- `max_file_size_lines`: Skip files larger than this
- `cost_limits`: Adjust budget caps
- `skip_paths`: Add more patterns to skip
- `auto_labels`: Customize label keywords

### Review Prompts

Located in `.github/scripts/ai-review/prompts/`:

- `c-code.md` - PostgreSQL C code review
- `sql.md` - SQL and regression test review
- `documentation.md` - Documentation review
- `build-system.md` - Makefile/Meson review

**Customization:** Edit prompts to adjust review focus and style.

## Usage

### Automatic Reviews

Reviews run automatically on PRs to `master` and `feature/**` branches.

**Typical workflow:**
1. Create feature branch
2. Make changes
3. Push branch: `git push origin feature/my-feature`
4. Create PR
5. AI review runs automatically
6. Review AI feedback
7. Make updates if needed
8. Push updates → AI re-reviews

### Manual Reviews

Trigger manually via GitHub Actions:

**Via UI:**
1. Actions → "AI Code Review"
2. Run workflow
3. Enter PR number
4. Run workflow

**Via CLI:**
```bash
gh workflow run ai-code-review.yml -f pr_number=123
```

### Interpreting Reviews

**Inline comments:**
- Posted on specific lines of code
- Format: `**[Category]**` followed by description
- Categories: Memory, Security, Performance, etc.

**Summary comment:**
- Posted at PR level
- Overview of files reviewed
- Issue count by category
- Cost information

**Labels:**
- Automatically added based on findings
- Filter PRs by label to prioritize
- Remove label manually if false positive

### Best Practices

**Trust but verify:**
- AI reviews are helpful but not infallible
- False positives happen (~5% rate)
- Use judgment - AI doesn't have full context
- Especially verify: security and correctness issues

**Iterative improvement:**
- AI learns from the prompts, not from feedback
- If AI consistently misses something, update prompts
- Share false positives/negatives to improve system

**Cost consciousness:**
- Keep PRs focused (fewer files = lower cost)
- Use draft PRs for work-in-progress (AI skips drafts)
- Mark PR ready when you want AI review

## Cost Tracking

### View Costs

**Per-PR cost:**
- Shown in AI review summary comment
- Format: `Cost: $X.XX | Model: claude-3-5-sonnet`

**Monthly cost:**
- Download cost logs from workflow artifacts
- Aggregate to calculate monthly total

**Download cost logs:**
```bash
# List recent runs
gh run list --workflow=ai-code-review.yml --limit 10

# Download artifact
gh run download <run-id> -n ai-review-cost-log-<pr-number>
```

### Cost Estimation

**Token costs (Claude 3.5 Sonnet):**
- Input: $0.003 per 1K tokens
- Output: $0.015 per 1K tokens

**Typical costs:**
- Small PR (<500 lines, 5 files): $0.50-$1.00
- Medium PR (500-2000 lines, 15 files): $1.00-$3.00
- Large PR (2000-5000 lines, 30 files): $3.00-$7.50

**Expected monthly (20 PRs/month mixed sizes):** $35-50

### Budget Controls

**Automatic limits:**
- Per-PR limit: Stops reviewing after $15
- Monthly limit: Stops at $200 (requires manual override)
- Alert: Warning at $150

**Manual controls:**
- Disable workflow: Actions → AI Code Review → Disable
- Reduce `max_tokens_per_request` in config
- Add more patterns to `skip_paths`
- Increase `max_file_size_lines` threshold

## Troubleshooting

### Issue: No review posted

**Possible causes:**
1. PR is draft (intentionally skipped)
2. No reviewable files (all binary or skipped patterns)
3. API key missing or invalid
4. Cost limit reached

**Check:**
- Actions → "AI Code Review" → Latest run → View logs
- Look for: "Skipping draft PR" or "No reviewable files"
- Verify: `ANTHROPIC_API_KEY` secret exists

### Issue: Review incomplete

**Possible causes:**
1. PR cost limit reached ($15 default)
2. File too large (>5000 lines)
3. API rate limit hit

**Check:**
- Review summary comment for "Reached PR cost limit"
- Workflow logs for "Skipping X - too large"

**Fix:**
- Increase `max_per_pr_dollars` in config
- Increase `max_file_size_lines` (trade-off: higher cost)
- Split large PR into smaller PRs

### Issue: False positives

**Example:** AI flags correct code as problematic

**Handling:**
1. Ignore the comment (human judgment overrides)
2. Reply to comment explaining why it's correct
3. If systematic: Update prompt to clarify

**Note:** Some false positives are acceptable (5-10% rate)

### Issue: Claude API errors

**Error types:**
- `401 Unauthorized`: Invalid API key
- `429 Too Many Requests`: Rate limit
- `500 Internal Server Error`: Claude service issue

**Check:**
- Workflow logs for error messages
- Claude status: https://status.anthropic.com/

**Fix:**
- Rotate API key if 401
- Wait and retry if 429 or 500
- Contact Anthropic support if persistent

### Issue: High costs

**Unexpected high costs:**
1. Check cost logs for large PRs
2. Review `skip_paths` - are large files being reviewed?
3. Check for repeated reviews (PR updated many times)

**Optimization:**
- Add more skip patterns for generated files
- Lower `max_tokens_per_request` (shorter reviews)
- Increase `max_file_size_lines` to skip more files
- Batch PR updates to reduce review runs

## Disabling AI Review

### Temporarily disable

**For one PR:**
- Convert to draft
- Or add `[skip ai]` to PR title (requires workflow modification)

**For all PRs:**
```bash
# Via GitHub UI:
# Actions → "AI Code Review" → "..." → Disable workflow

# Via git:
git mv .github/workflows/ai-code-review.yml \
       .github/workflows/ai-code-review.yml.disabled
git commit -m "Disable AI code review"
git push
```

### Permanently remove

```bash
# Remove workflow
rm .github/workflows/ai-code-review.yml

# Remove scripts
rm -rf .github/scripts/ai-review

# Commit
git commit -am "Remove AI code review system"
git push
```

## Testing and Iteration

### Shadow Mode (Week 1)

Run reviews but don't post comments:

1. Modify `review-pr.js`:
   ```javascript
   // Comment out posting functions
   // await postInlineComments(...)
   // await postSummaryComment(...)
   ```

2. Reviews saved to workflow artifacts
3. Review quality offline
4. Tune prompts based on results

### Comment Mode (Week 2)

Post comments with `[AI Review]` prefix:

1. Add prefix to comment body:
   ```javascript
   const body = `**[AI Review] [${issue.category}]**\n\n${issue.description}`;
   ```

2. Gather feedback from developers
3. Adjust prompts and configuration

### Full Mode (Week 3+)

Remove prefix, enable all features:

1. Remove `[AI Review]` prefix
2. Enable auto-labeling
3. Monitor quality and costs
4. Iterate on prompts as needed

## Advanced Customization

### Custom Review Prompts

Add a new prompt for a file type:

1. Create `.github/scripts/ai-review/prompts/my-type.md`
2. Write review guidelines (see existing prompts)
3. Update `config.json`:
   ```json
   "file_type_patterns": {
     "my_type": ["*.ext", "special/*.files"]
   }
   ```
4. Test with manual workflow trigger

### Conditional Reviews

Skip AI review for certain PRs:

Modify `.github/workflows/ai-code-review.yml`:
```yaml
jobs:
  ai-review:
    if: |
      github.event.pull_request.draft == false &&
      !contains(github.event.pull_request.title, '[skip ai]') &&
      !contains(github.event.pull_request.labels.*.name, 'no-ai-review')
```

### Cost Alerts

Add cost alert notifications:

1. Create workflow in `.github/workflows/cost-alert.yml`
2. Trigger: On schedule (weekly)
3. Aggregate cost logs
4. Post issue if over threshold

## Security and Privacy

### API Key Security

- Store only in GitHub Secrets (encrypted at rest)
- Never commit to repository
- Never log in workflow output
- Rotate quarterly

### Code Privacy

- Code sent to Claude API (Anthropic)
- Anthropic does not train on API data
- API requests are not retained long-term
- See: https://www.anthropic.com/legal/privacy

### Sensitive Code

If reviewing sensitive/proprietary code:

1. Review Anthropic's terms of service
2. Consider: Self-hosted alternative (future)
3. Or: Skip AI review for sensitive PRs (add label)

## Support

### Questions

- Check this guide first
- Search GitHub issues: label:ai-review
- Check Claude API docs: https://docs.anthropic.com/

### Reporting Issues

Create issue with:
- PR number
- Workflow run URL
- Error messages from logs
- Expected vs actual behavior

### Improving Prompts

Contributions welcome:
1. Identify systematic issue (false positive/negative)
2. Propose prompt modification
3. Test on sample PRs
4. Submit PR with updated prompt

## References

- Claude API: https://docs.anthropic.com/
- Claude Models: https://www.anthropic.com/product
- PostgreSQL Hacker's Guide: https://wiki.postgresql.org/wiki/Developer_FAQ
- GitHub Actions: https://docs.github.com/en/actions

---

**Version:** 1.0
**Last Updated:** 2026-03-10

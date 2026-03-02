# Troubleshooting: PR conflicts + GitGuardian failure

## 1) Resolve merge conflicts for this branch
If GitHub shows conflicts in:
- `.env.example`
- `.github/workflows/bot-hybrid.yml`
- `bot/config.py`
- `bot/db.py`
- `bot/main.py`
- `bot/worker_scan.py`
- `requirements.txt`

Use command line (recommended):

```bash
git checkout work
git fetch origin
git rebase origin/main
```

When rebase stops with conflicts:

```bash
git status

# keep your branch versions for files that intentionally changed
git checkout --theirs .env.example .github/workflows/bot-hybrid.yml bot/config.py bot/db.py bot/main.py bot/worker_scan.py requirements.txt

git add .env.example .github/workflows/bot-hybrid.yml bot/config.py bot/db.py bot/main.py bot/worker_scan.py requirements.txt
git rebase --continue
```

Repeat until rebase completes, then push:

```bash
git push --force-with-lease
```

---

## 2) Fix GitGuardian "1 secret uncovered"
Important: GitGuardian scans **commit history in the branch**, not only current file contents.

### 2.1 Find the exact commit and file flagged by GitGuardian
1. Open failed check details in GitHub Actions / checks page.
2. Copy secret type + commit SHA + file path reported.

### 2.2 Remove secret from branch history
If secret was introduced in the latest commit:

```bash
# edit file and remove secret value
git add <file>
git commit --amend --no-edit
git push --force-with-lease
```

If secret was introduced in older commit(s):

```bash
git rebase -i origin/main
# mark bad commit(s) as edit
# remove secret from file(s), then:
git add <file>
git commit --amend --no-edit
git rebase --continue
git push --force-with-lease
```

### 2.3 Rotate compromised secrets
Even if removed from git history, rotate leaked credentials:
- Telegram `BOT_TOKEN` in @BotFather
- Cloudflare `D1_API_TOKEN`
- Any API key found in check details

---

## 3) Prevent recurrence
- Keep `.env.example` with empty placeholders only.
- Put real values only in GitHub/Render secrets.
- Never commit `.env`.
- Before push, run quick local grep:

```bash
rg -n "(BOT_TOKEN=|D1_API_TOKEN=|CF_ACCOUNT_ID=|ghp_|xoxb-|AKIA|AIza)" .
```

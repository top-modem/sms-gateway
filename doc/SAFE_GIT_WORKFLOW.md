# Safe Git Workflow (No Surprise Changes)

Use this workflow to avoid accidental edits being committed.

## 1) Check working tree first

```powershell
git status --short
```

Review all modified, deleted, and untracked files before staging anything.

## 2) Use scoped commit script

Use the script to stage exact files only, run checks, and commit.

```powershell
powershell -ExecutionPolicy Bypass -File scripts/scoped_commit.ps1 \
  -Message "fix: your message" \
  -Files src/api/mod.rs,frontend/src/pages/SimDashboard.svelte
```

What it protects:
- Blocks if there are pre-staged files.
- Stages only the file list you pass.
- Verifies staged files exactly match your list.
- Runs `cargo check -q` when Rust files are included.
- Runs `pnpm --dir frontend build` when frontend files are included.

## 3) Push only after commit summary is correct

```powershell
git show --name-only --stat --oneline -1
git push origin main
```

## 4) Generated folders ignored by git

These local/generated folders are ignored now:
- `/debug/`
- `/release/`
- `/frontend/logs/`
- `/frontend/data/`

## 5) Important note about already tracked files

If a file is already tracked by git, `.gitignore` does not remove it from history.
Use explicit review with `git status --short` before each commit.

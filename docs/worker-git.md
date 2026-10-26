# Worker git and PR flow

How an agent worker publishes work on `m7jay/pigeon` without a human in the loop.
Every card that changes code follows this file. If a card and this file disagree,
this file is wrong only after it is changed in a PR.

## 1. What is provisioned, and where

| Thing | Location | Notes |
|---|---|---|
| `GH_TOKEN` | each worker profile's `.env` (`pigeon-dev`, `pigeon-rev`, `pigeon-ops`, `pigeon-arch`), mode `600` | Profiles live outside the repo, so the token cannot be committed by accident |
| git credential helper | `<profile>/home/.gitconfig` → `helper = !/opt/data/toolchain/gh auth git-credential` | No token in this file, in a remote URL, or in any repo config |
| `gh` CLI | `/opt/data/toolchain/gh` (v2.103.2) | On the worker `PATH` via `PATH=/opt/data/toolchain:${PATH}` in the same `.env` |
| commit identity | `GIT_AUTHOR_*` / `GIT_COMMITTER_*` in the same `.env` | See §6 — the shared checkout pins `user.name` locally |

The token is a fine-grained PAT owned by `m7jay`. It can push, open PRs, and read
Actions runs. It **cannot** read check runs:

```
$ gh api repos/m7jay/pigeon/commits/<sha>/check-runs
gh: Resource not accessible by personal access token (HTTP 403)   # no checks:read
```

Because of that, kanban's built-in completion-contract gate cannot verify CI, so
**cards stay `local-only`** and a worker verifies CI by hand (§5). Do not switch a
card to a PR-URL completion contract — the gate will block forever on a 403.

Verify credentials are wired before you start:

```bash
gh auth status                 # "Logged in to github.com account m7jay (GH_TOKEN)"
git config --get credential.https://github.com.helper
```

Neither command prints the token. Never `echo $GH_TOKEN`, never put it in a URL,
never write it into a file under the repo.

## 2. Pick a workspace

Never work in `/opt/data/workspace/projects/pigeon` itself — it is one checkout
shared by every worker, and switching its branch under another worker's feet loses
their work. Use a worktree per card:

```bash
cd /opt/data/workspace/projects/pigeon
git fetch origin
git worktree add "$HERMES_KANBAN_WORKSPACE/wt" -b wt/<card-id>-<slug> origin/<default-branch>
cd "$HERMES_KANBAN_WORKSPACE/wt"
```

`HERMES_KANBAN_WORKSPACE/wt` is the worktree to use; the scratch workspace root is
yours and is discarded, so nothing important may live outside `wt/`.

## 3. Branch naming

```
wt/<card-id>-<slug>
```

- `<card-id>` is the kanban task id, e.g. `t_8b5d4ad0`.
- `<slug>` is 2-4 hyphenated words naming the change, e.g. `worker-git-docs`.

Examples: `wt/t_8b5d4ad0-worker-git-docs`, `wt/t_f78a96c3-s0-postgres-evidence`.

Rules: one branch per card, branched from the repo's current default branch
(`gh repo view m7jay/pigeon --json defaultBranchRef` — today `feat/m0-skeleton`).
Never push to `master` or the default branch directly. Never force-push a branch
that has an open PR.

## 4. Commits

One commit per logical block — a slice of work that stands alone and would pass
review by itself (schema + test, then handler + test; not "wip" and not one commit
for a whole card).

Conventional Commits, citing the requirement ids the change serves:

```
feat(api): issue producer API keys with one-time reveal (FR-201, AC-07)

Keys are stored as SHA-256 hashes; the plaintext is returned once at creation
and never persisted. RLS policy limits reads to the owning producer.
```

- `type(scope): summary` — `feat`, `fix`, `test`, `refactor`, `docs`, `ci`, `chore`.
- Cite requirement ids in the subject or body: `FR-nnn`, `AC-nn`, `D-n` from
  `REQUIREMENTS.md`, and the slice from `PLAN.md` when the commit closes one.
- Signing is not required. Do not amend or rebase a branch that already has an
  open PR unless the card asks for it; add commits instead.

## 5. Push, PR and CI

```bash
git push -u origin wt/<card-id>-<slug>      # credential helper supplies the token
gh pr create --base <default-branch> \
  --title "feat(api): issue producer API keys (FR-201)" \
  --body-file /tmp/pr-body.md
```

One PR per card. The PR body states what changed and any item that needs a human
decision, plus how the change was verified.

CI: check the run for your head commit, and read the log — a skipped test is not a
pass.

```bash
gh run list --branch wt/<card-id>-<slug> -L 5
gh run view <run-id> --log | grep -E '^(ok|FAIL|---)'   # e.g. "ok  github.com/m7jay/pigeon/internal/store"
```

Put the run id and its conclusion in `kanban_complete(summary=..., metadata=...)`,
together with `metadata["published_pr"] = "<PR URL>"`.

## 6. Identity: commits are authored by the worker, not by Jay

The shared checkout pins `user.name = Jayachandra` in `.git/config`, which beats
`~/.gitconfig`, so a worker must not rely on the profile config for authorship.
Each profile sets its own identity in `.env`:

```
GIT_AUTHOR_NAME=pigeon-dev (agent)
GIT_AUTHOR_EMAIL=pigeon-dev.agent@users.noreply.github.com
GIT_COMMITTER_NAME=pigeon-dev (agent)
GIT_COMMITTER_EMAIL=pigeon-dev.agent@users.noreply.github.com
```

Do not override these to impersonate a human, and do not set a different identity
per commit.

## 7. Never merge

The worker opens the PR and stops. Merging, squashing, rebasing and closing PRs
belong to Jay. If CI is red, fix it on the same branch — do not merge around it,
and do not disable a check.

After a PR is merged, the branch is deleted by Jay (or the next worker); a worker
may delete its own branch only after it has confirmed the merge:

```bash
gh pr view <n> --json state,mergedAt
git push origin --delete wt/<card-id>-<slug>
git worktree remove "$HERMES_KANBAN_WORKSPACE/wt"
```

## 8. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `gh: command not found` | profile `.env` missing the `PATH` line | add `PATH=/opt/data/toolchain:${PATH}` to the profile `.env`, or call `/opt/data/toolchain/gh` |
| `Authentication failed` on push | `GH_TOKEN` absent/empty in the worker's env | check `gh auth status`; the token comes from `<profile>/.env`, mode 600 |
| helper prints nothing | `gh` not resolvable inside the git helper (PATH differs under git) | the helper uses the absolute path on purpose — keep it absolute |
| commits show `Jayachandra` | repo-local `user.name` wins over `~/.gitconfig` | the `GIT_AUTHOR_*`/`GIT_COMMITTER_*` lines in `.env` are missing |
| `403` on `check-runs` | token has no `checks:read` | expected — verify CI by hand with `gh run list` / `gh run view --log` |

## 9. Rollback and revocation

- The whole publishing setup is three files per profile: `.env` (token + PATH +
  identity), `home/.gitconfig` (credential helper). Delete those lines and the
  profile can no longer push; nothing else in the environment changes.
- To revoke access, rotate the PAT at GitHub (Settings → Developer settings →
  Fine-grained tokens) and rewrite `GH_TOKEN` in the four `.env` files. The old
  token dies everywhere at once, including any shell that still holds it.
- Nothing in this setup is stored in the repository, so a leaked-token incident is
  never a git-history rewrite.

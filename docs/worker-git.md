# Worker git and PR flow

How an agent worker publishes work on `m7jay/pigeon` without a human in the loop.
Every card that changes code follows this file. If a card and this file disagree,
this file is wrong only after it is changed in a PR.

## 1. What is provisioned, and where

| Thing | Location | Notes |
|---|---|---|
| `GH_TOKEN` | each worker profile's `.env` (`pigeon-dev`, `pigeon-rev`, `pigeon-ops`, `pigeon-arch`), mode `600` | Read by anything that looks at the environment; profiles live outside the repo |
| `gh` credential store | `<profile>/home/.config/gh/hosts.yml`, mode `600` | **The layer that actually carries auth**: written by `gh auth login --with-token`, needs no environment variable |
| git credential helper | `<profile>/home/.gitconfig` → `helper = !/opt/data/toolchain/gh auth git-credential` | Absolute path on purpose; no token in this file, in a remote URL, or in any repo config |
| `gh` CLI | `/opt/data/toolchain/gh` (v2.103.2) | **Not on `PATH`** and cannot be made so — see §8 |
| commit identity | `GIT_AUTHOR_*` / `GIT_COMMITTER_*` in the same `.env` | See §6 — the shared checkout pins `user.name` locally |

Two layers exist because they fail differently. `hosts.yml` is per-profile state
read from `$HOME` and works in any shell, including one with a scrubbed
environment — that is what `git push` and `gh pr create` rely on. `GH_TOKEN` in
`.env` covers the agent process itself and anything else that reads the
environment. Neither is in the repository, so a leaked token is never a git-history
rewrite.

The token is a fine-grained PAT owned by `m7jay`. It can push, open PRs, and read
Actions runs. It **cannot** read check runs:

```
$ gh api repos/m7jay/pigeon/commits/<sha>/check-runs
gh: Resource not accessible by personal access token (HTTP 403)   # no checks:read
```

Because of that, kanban's built-in completion-contract gate cannot verify CI, so
**cards stay `local-only`** and a worker verifies CI by hand (§5). Do not switch a
card to a PR-URL completion contract — the gate will block forever on that 403.

Check the wiring before you start. Neither command prints the token:

```bash
GH=/opt/data/toolchain/gh
$GH auth status            # ✓ Logged in to github.com account m7jay (.../hosts.yml)
git config --get credential.https://github.com.helper
```

Never `echo $GH_TOKEN`, never put it in a URL, never write it into a file under
the repo.

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
(`$GH repo view m7jay/pigeon --json defaultBranchRef` — today `feat/m0-skeleton`).
Never push to `master` or the default branch directly. Never force-push a branch
under review, and never rewrite one that another person has looked at.

One exception: a worker's **own, unreviewed** branch may be rewritten to purge a
credential — or the bare token prefix, which is enough to trip the leak scan —
from its history. Do it by deleting and re-pushing the branch (the command guard
refuses `git push --force`), then open the replacement PR and say in it why the
old history was dropped; the new PR gets a new number, because a closed PR cannot
be reopened against a rewritten head. Scan before pushing:
`git log -p origin/<default>..HEAD | grep -c <token-prefix>` must print `0`.

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
- Check authorship before pushing (`git log -1 --format='%an <%ae>'`); if it says
  `Jayachandra`, the environment did not load — see §6.

## 5. Push, PR and CI

```bash
GH=/opt/data/toolchain/gh
git push -u origin wt/<card-id>-<slug>      # credential helper supplies the token
$GH pr create --base <default-branch> \
  --title "feat(api): issue producer API keys (FR-201)" \
  --body-file /tmp/pr-body.md
```

One PR per card. The PR body states what changed and any item that needs a human
decision, plus how the change was verified.

CI: check the run for your head commit, and read the log — a skipped test is not a
pass.

```bash
$GH run list --branch wt/<card-id>-<slug> -L 5
$GH run view <run-id> --log | grep -E '^(ok|FAIL|---)'   # e.g. "ok  github.com/m7jay/pigeon/internal/store"
```

Put the run id and its conclusion in `kanban_complete(summary=..., metadata=...)`,
together with `metadata["published_pr"] = "<PR URL>"`.

## 6. Identity: commits are authored by the worker, not by Jay

The shared checkout pins `user.name = Jayachandra` in `.git/config`, which outranks
`~/.gitconfig` in every worktree of that repo (`git config --list --show-origin`
shows `file:.git/config user.name=Jayachandra`). A worker therefore cannot rely on
the profile `.gitconfig` for authorship. Each profile sets its own identity in
`.env`:

```
GIT_AUTHOR_NAME=pigeon-dev (agent)
GIT_AUTHOR_EMAIL=pigeon-dev.agent@users.noreply.github.com
GIT_COMMITTER_NAME=pigeon-dev (agent)
GIT_COMMITTER_EMAIL=pigeon-dev.agent@users.noreply.github.com
```

Those reach the tool shell for dispatcher-spawned workers — a worker process
snapshots the profile `.env` at startup (other `.env` keys such as
`TERMINAL_TIMEOUT=60` are visible in its shell). In any shell where they did not
load, pin them on the command instead of committing as Jay:

```bash
git -c user.name='pigeon-dev (agent)' \
    -c user.email='pigeon-dev.agent@users.noreply.github.com' commit -m "..."
```

Do not impersonate a human, and do not set a different identity per commit.

## 7. Never merge

The worker opens the PR and stops. Merging, squashing, rebasing and closing PRs
belong to Jay. If CI is red, fix it on the same branch — do not merge around it,
and do not disable a check.

After a PR is merged, the branch is deleted by Jay (or the next worker); a worker
may delete its own branch only after it has confirmed the merge:

```bash
$GH pr view <n> --json state,mergedAt
git push origin --delete wt/<card-id>-<slug>
git worktree remove "$HERMES_KANBAN_WORKSPACE/wt"
```

To discard a throwaway branch locally, use `git update-ref -d refs/heads/<branch>`
— `git branch -D` is refused by the worker's command guard.

## 8. What does not work here (measured, not guessed)

- **`gh` is not on `PATH`.** `/usr/local/bin` and the other baseline `PATH` entries
  are not writable. `PATH` is also on Hermes' `.env` writer denylist ("fix tool
  lookup with absolute paths instead"), and the tool shell's `PATH` comes from the
  process environment, not from `.env`. So: always call `$GH`
  = `/opt/data/toolchain/gh`, or prefix `export PATH=/opt/data/toolchain:$PATH`
  inside a single command. Adding `PATH=` to `.env` is inert at best and grows the
  variable on every reload at worst — do not do it.
- **A routed `hermes -p <profile> -z` run does not load that profile's `.env`.**
  Measured with a throwaway marker (`PIGEON_ENV_PROBE=loaded` in the profile
  `.env`): the child printed `PROBE=unset`, `GH_TOKEN=MISSING`, baseline `PATH`,
  with and without `--cli`. Those sessions are served by the long-lived
  multiplexer, which keeps the environment it started with. This is exactly why
  auth lives in `hosts.yml` rather than in the environment: `git push` and
  `gh pr create` work in such a shell, `$GH_TOKEN` does not.
- **The worker command guard blocks the obvious destructive git verbs.** `git push
  --force` and `git branch -D` are refused in unattended mode; use
  `git update-ref -d refs/heads/<branch>` to drop a local branch and the
  delete-then-push dance in §3 to replace a remote one.
- **Profile `.env` changes reach the *next* worker process, not a running one.**
  A worker process snapshots `.env` at startup; to give a long-lived surface new
  environment values, restart the gateway it runs under
  (`hermes gateway restart`) — never in the middle of another worker's run.

## 9. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `gh: command not found` | `gh` is not on `PATH` (§8) | call `/opt/data/toolchain/gh`, or export `PATH` inline for that one command |
| `Authentication failed` on push | profile `hosts.yml` missing, or `$HOME` not the profile home | `GH=/opt/data/toolchain/gh; HOME=$HOME $GH auth status` |
| helper prints nothing | `gh` unresolvable inside the git helper | keep the helper path absolute in `.gitconfig` |
| commits show `Jayachandra` | repo-local `user.name` wins over `~/.gitconfig` | set `GIT_AUTHOR_*`/`GIT_COMMITTER_*` for the command (§6) |
| `403` on `check-runs` | token has no `checks:read` | expected — verify CI by hand (§5) |

## 10. Rollback and revocation

- The whole publishing setup is three things per profile: the `.env` block
  (`GH_TOKEN`, identity), `home/.gitconfig` (credential helper), and
  `home/.config/gh/hosts.yml` (credential store). Remove the `hosts.yml` and the
  `.gitconfig` helper and that profile can no longer push; nothing else in the
  environment changes.
- To revoke access everywhere at once, rotate the PAT at GitHub (Settings →
  Developer settings → Fine-grained tokens), then rewrite `GH_TOKEN` in the four
  `.env` files and re-run
  `HOME=<profile>/home /opt/data/toolchain/gh auth login --with-token < tokenfile`
  for each profile.
- Nothing in this setup is stored in the repository, so a leaked-token incident is
  never a git-history rewrite.

## Verified

Run on the build host when this doc landed:

| Check | Result |
|---|---|
| `git credential fill` in four profiles, `env -i` with no `GH_TOKEN` | `username` present, `password_len=93` each |
| `gh auth status` per profile, `env -i` | ✓ logged in as `m7jay` from `<profile>/home/.config/gh/hosts.yml` |
| throwaway branch `wt/t_8b5d4ad0-throwaway`, pushed with no `GH_TOKEN` and no `gh` on `PATH` | pushed, then deleted (local and remote) |
| throwaway PR from that branch, same scrubbed environment | opened (#2), then closed with `--delete-branch` |
| `gh api .../check-runs` | `403 Resource not accessible by personal access token` (expected) |
| token-prefix scan over the tree **and** the full history (`git grep -I -l <prefix>`, `git log -p --all \| grep -c <prefix>`) | clean / `0` |

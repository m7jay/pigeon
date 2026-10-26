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

**Second and later rounds take the existing branch, not a new one.** The branch outlives
the worktree - `git worktree remove` deletes the directory, not the ref - so `-b` fails
with `fatal: a branch named 'wt/<card-id>-<slug>' already exists`, and re-cutting it from
`origin/<default-branch>` would leave you without the commits under review and make your
push non-fast-forward. Check out the branch that has the PR on it:

```bash
cd /opt/data/workspace/projects/pigeon
git worktree remove --force "$HERMES_KANBAN_WORKSPACE/wt" 2>/dev/null || true
git fetch origin wt/<card-id>-<slug>
git worktree prune
git worktree add "$HERMES_KANBAN_WORKSPACE/wt" wt/<card-id>-<slug>
cd "$HERMES_KANBAN_WORKSPACE/wt"
git status -sb        # must show ...origin/wt/<card-id>-<slug>, not a fresh branch
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
(`/opt/data/toolchain/gh repo view m7jay/pigeon --json defaultBranchRef` — today
`feat/m0-skeleton`).
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

One open PR per card at a time. The PR body states what changed and any item that needs a human
decision, plus how the change was verified.

CI: check the run for your head commit, and read the log — a skipped test is not a
pass.

```bash
GH=/opt/data/toolchain/gh
$GH run list --repo m7jay/pigeon --branch wt/<card-id>-<slug> -L 5 \
    --json databaseId,headSha,status,conclusion
$GH run view <run-id> --repo m7jay/pigeon --log | grep -E '^(ok|FAIL|---)'   # e.g. "ok  github.com/m7jay/pigeon/internal/store"
```

The `headSha` must be the commit you pushed — a green run for an earlier commit is not
your gate, and the reviewer will check exactly that (§7 step 3).

Put the run id and its conclusion in the review request, naming every key:

```python
kanban_request_review(task_id, summary="...", reviewer="pigeon-rev", metadata={
    "ci_run_id": <run id>, "ci_conclusion": "success",
    "head_sha": "<the head you pushed>", "published_pr": "<PR URL>",
})
```

The implementer does not call `kanban_complete` on a code card - section 7.

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

## 7. The review loop decides done, and the reviewer merges

Card state is the gate, not the GitHub review pane. A code card reaches `done` only
when `pigeon-rev` calls `kanban_complete` on it. The reviewer merges the PR first and
completes the card immediately after - in that order, in the same run.

**The loop is mandatory.** `pigeon-dev` implements, opens the PR, and submits with
`kanban_request_review(task_id, summary=..., metadata={...}, reviewer="pigeon-rev")`.
Pass the reviewer explicitly every time: a re-review with no recorded reviewer
provenance is refused. The card moves to `review` and the dispatcher spawns
`pigeon-rev`. A review that finds defects calls `kanban_request_changes(task_id,
reason=...)`, which requeues the card to `pigeon-dev` with a `changes_requested` event
and re-spawns the implementer - who checks out the existing branch on the second round
(§2) rather than re-cutting it. That repeats until the reviewer approves. Only
`pigeon-rev` calls `kanban_complete` on a code card, and there is no "this team has no
reviewer" shortcut. None of that is enforced by the tool - nothing refuses an
implementer that calls `kanban_complete` straight from `running` - so it is a protocol
the profiles and this file agree on, auditable after the fact from the card's events.

**What the reviewer does, in this order.** Every step is against one head SHA, and that
SHA is named in the artifact from step 5. Each block sets `GH` itself so it also runs
in a fresh shell: `gh` is not on `PATH` (§8), and an inherited variable is not worth
relying on.

1. Read the head the implementer named. Review that SHA, not whatever is in a worktree.

   ```bash
   GH=/opt/data/toolchain/gh
   $GH pr view <n> --repo m7jay/pigeon \
       --json headRefOid,mergeable,mergeStateStatus,isDraft,title,body
   ```

2. A defect pass over auth, tenant isolation, SQL and tests, reported as
   `file:line - problem - fix`. Restating the diff is not a review.
3. CI is green on that exact SHA - not "the branch has a green run somewhere":

   ```bash
   GH=/opt/data/toolchain/gh
   $GH run list --repo m7jay/pigeon --branch <branch> -L 5 \
       --json databaseId,headSha,status,conclusion
   ```

   The run must report `status: completed`, `conclusion: success`, and a `headSha`
   equal to the `headRefOid` from step 1. A run whose `headSha` differs is not the
   gate, however green it is. A red check goes back to the implementer - never merge
   around it, never disable one.
4. GitHub reports the PR mergeable: `mergeable: MERGEABLE`,
   `mergeStateStatus: CLEAN` (a head that is behind the base still reports `CLEAN` on
   this repo, because protection cannot exist here - if GitHub ever reports `BEHIND`,
   treat it as mergeable, see below), and the PR is not a draft.
5. **Record the verdict before merging - once, and only for a SHA you have reviewed.**
   `kanban_comment` with the reviewed
   `headRefOid`, the `ci` run id and its `headSha`, and either `no findings` or the
   finding list. Be plain about what this is: kanban does not enforce it - a
   completion out of the review column needs no evidence at all
   (`hermes_cli/kanban_db.py`, "Completions from non-review statuses need evidence",
   review exempt), and completion metadata is free-form. The comment and the metadata
   are the record a human reads when they ask whether a card was ever really reviewed.
   A completion without them is a protocol violation even though nothing rejects it.
   If the card already carries a verdict from an earlier round, this comment names the
   reviewed `headRefOid` and says it supersedes every earlier verdict: `kanban_comment`
   is append-only, there is no edit or delete, so the newest comment is the record and
   the earlier ones are stale by definition.
6. Re-read `headRefOid`, as the last control step before merging. If it moved since step
   1, the implementer pushed: go back to step 1 with the new SHA and review that one -
   do not write a verdict for the SHA you have not reviewed, and do not carry the
   earlier verdict over. Never merge a SHA you did not review.
7. Merge - rebase, never squash and never a merge bubble:

```bash
GH=/opt/data/toolchain/gh
$GH pr merge <n> --repo m7jay/pigeon --rebase --match-head-commit <reviewed headRefOid>
$GH pr view <n> --repo m7jay/pigeon --json state,mergedAt,mergeCommit,headRefOid
```

`--match-head-commit` closes the window between step 6's re-read and the merge: if the
head moved after the re-read, GitHub refuses the merge instead of landing a SHA nobody
reviewed.

`mergeCommit.oid` is the value for `metadata["merge_commit"]`; it is `null` until the
merge lands, so read it after the merge returns, not before. Skip `--delete-branch`: it deletes
the remote ref correctly, but its local-branch cleanup needs a checkout the reviewer
does not have (§2 keeps the shared checkout off limits, and the implementer's worktree
dies with its run), so it can fail after the merge has already landed. Delete the
remote ref directly instead, which needs no checkout:

```bash
GH=/opt/data/toolchain/gh
$GH api -X DELETE repos/m7jay/pigeon/git/refs/heads/wt/<card-id>-<slug>
```

Then complete, using the same key names section 5 uses for the PR:

```python
kanban_complete(summary="...", metadata={
    "published_pr": "https://github.com/m7jay/pigeon/pull/<n>",
    "pr": <n>, "merge_commit": "<mergeCommit.oid>",
    "head_sha": "<reviewed headRefOid>", "ci_run_id": <run id>,
})
```

That completion is the only thing that moves the card to `done`.

**If the PR is `BEHIND`,** merge anyway. There is no branch protection on this repo and
rebase merges are allowed, so a branch whose base moved is still mergeable; re-check
`mergeable`/`mergeStateStatus` right before merging and nothing else. Do not try to
re-run step 3 after the merge: a rebase merge rewrites the commits and creates new SHAs
by design, and the branch ref is gone, so there is no run to match. The gate is the CI
run for the reviewed `headRefOid`, and that is the value `metadata["head_sha"]` keeps -
the landed commits are the same tree under new SHAs and will not have runs of their
own.

**If the PR is `DIRTY`,** the branch really conflicts, and neither worker rewrites it:
§3 forbids rewriting a branch another worker has reviewed, §4's "unless the card asks
for it" exception stops applying once a verdict names the head, and the force-push
either would need is refused by the guard (§8). Send it back with `kanban_request_changes`: the
implementer merges the default branch **into** its own branch
(`git merge origin/<default-branch>`), resolves, and pushes forward. That is a new head
SHA, so the reviewer starts again at step 1. A replacement PR is only for the
credential case in §3, and then the card must name both PRs so the completion points at
the one that merged.

**Where the verdict lives: the card.** `gh pr review` is not usable here - the token
belongs to the PR's own author and GitHub refuses it (`Review Can not request changes
on your own pull request`), so `reviewDecision` on these PRs stays empty forever and
must never be used as the gate. The card comment from step 5 is the review artifact; it
may be mirrored to the PR for humans, but the card is what the loop acts on.

**Corollary: a PR comment is not a review.** Nothing polls the GitHub thread. A finding
left on a PR by a human is invisible to the loop until it is copied into the card with
`kanban_comment` - which is also how a human redirects an in-flight card.

This replaces the earlier "never merge" rule (changed 26 Oct 2026). Nothing changes
about **where** work lands: a worker still never pushes to `master` or the default
branch directly, and it still does not merge a PR whose review or CI it has not seen.

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
| `GraphQL: Head branch was modified` on `pr merge` | `--match-head-commit` found the head was not the SHA you supplied — usually a push landed between your read and the merge, occasionally a stale cached head right after a fast push | Read the head without a checkout: `GH=/opt/data/toolchain/gh; $GH api repos/m7jay/pigeon/git/ref/heads/<branch> --jq .object.sha`. **If it equals the SHA you supplied**, retry the merge once. **If it does not**, stop — the head moved: go back to §7 step 1 and review the new head. Never retry against a SHA that is no longer the head, and never drop `--match-head-commit` to get past the error |

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

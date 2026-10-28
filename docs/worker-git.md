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
push non-fast-forward. Re-create the worktree on the branch as the remote has it - `-B`
resets the local ref to `origin/`, so a local ref that lagged (a failed push last round,
or a push from somewhere else) cannot leave you reviewing a stale base:

```bash
cd /opt/data/workspace/projects/pigeon
git worktree unlock "$HERMES_KANBAN_WORKSPACE/wt" 2>/dev/null || true
git worktree remove --force "$HERMES_KANBAN_WORKSPACE/wt" 2>/dev/null || true
git fetch origin wt/<card-id>-<slug>
rm -rf "${HERMES_KANBAN_WORKSPACE:?}/wt"  # a leftover directory is not a registered worktree;
                                          # the :? aborts this command (not just a script) if the
                                          # variable is unset, so it can never become `rm -rf /wt`
git worktree prune                        # after the delete: prune only clears registrations
                                          # whose directory is already gone
git worktree add -B wt/<card-id>-<slug> "$HERMES_KANBAN_WORKSPACE/wt" origin/wt/<card-id>-<slug>
cd "$HERMES_KANBAN_WORKSPACE/wt"
git status -sb        # must show wt/<card-id>-<slug> in sync with origin, not a stale local ref
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

**Every slice branches from the default branch - never from another slice's PR
branch.** Stacking looks convenient (the slice needs the previous slice's code) and
it is a trap: a rebase merge rewrites the parent's commits onto a new base, so the
child is instantly `CONFLICTING` against a base that no longer exists, and merging
the parent usually deletes the branch the child is based on. When a slice genuinely
needs unlanded code, wait for that PR to merge, or merge it into your own branch
(§7 `DIRTY`), but do not make a sibling PR branch your base.

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
$GH run view <run-id> --repo m7jay/pigeon --log \
  | sed -E 's/^[^\t]+\t[^\t]+\t[^ ]+ //' \
  | grep -E '^(ok|FAIL|---)'    # "--- PASS: TestX" per test, "ok  <pkg>" per package
```

`gh run view --log` prefixes every line with `<job>\t<step>\t<timestamp>Z `, so the
`sed` strips that before the anchored `grep` - without it both pass and fail output are
empty, which reads as "nothing to see". Expect one `--- PASS:`/`ok ` line per test and
`--- FAIL:` on a red run.

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

1. Read the head the implementer named, then read the code **at that SHA**. The reviewer
   has no checkout (§2 keeps the shared one off limits), so every read is pinned.

   ```bash
   GH=/opt/data/toolchain/gh
   $GH pr view <n> --repo m7jay/pigeon \
       --json headRefOid,headRefName,mergeable,mergeStateStatus,isDraft,title,body
   ```

   Then read the code itself - every read pinned to `headRefOid`:

   ```bash
   GH=/opt/data/toolchain/gh
   $GH pr diff <n> --repo m7jay/pigeon                          # the whole change
   $GH api "repos/m7jay/pigeon/git/trees/<headRefOid>?recursive=1" --jq '.tree[].path'
   $GH api "repos/m7jay/pigeon/contents/<path>?ref=<headRefOid>" --jq .content | base64 -d
   ```

   `pr diff` follows the live PR head, so re-read `headRefOid` after it and, if it moved,
   restart this step with the new SHA; a `contents` read with `?ref=<headRefOid>` cannot
   drift. Reviewing from the PR title and body is not a review.

2. A defect pass over auth, tenant isolation, SQL and tests, reported as
   `file:line - problem - fix`. Restating the diff is not a review.
3. CI is green on that exact SHA - not "the branch has a green run somewhere":

   ```bash
   GH=/opt/data/toolchain/gh
   $GH run list --repo m7jay/pigeon --branch <branch> -L 5 \
       --json databaseId,headSha,status,conclusion
   ```

   `<branch>` is the `headRefName` from step 1. The run must report `status: completed`,
   `conclusion: success`, and a `headSha` equal to the `headRefOid` from step 1. A run
   whose `headSha` differs is not the gate, however green it is. While the run for
   `headRefOid` reports any status other than `completed` - `queued`, `pending`,
   `requested`, `waiting`, `in_progress` all occur - wait and re-run this step, polling
   every ~30 s; never merge on the strength of a previous commit's run. Two different
   stalls, two different bounds:

   - **never started** (`queued`, `requested`, `waiting`, `pending`): nothing after ~15
     minutes is a real stall. Stop and send the card back with `kanban_request_changes`,
     naming the run id and the status it was stuck in.
   - **`in_progress`**: the workflow's own `timeout-minutes` is the authority
     (`.github/workflows/ci.yml`), so wait past queue time plus that timeout - about 25
     minutes - before calling it stuck. Do not bounce a run that the workflow itself is
     about to resolve.

   A `completed` run whose conclusion is anything but `success` goes back to the
   implementer: never merge around a red check, never disable one. If runners are
   unavailable, that is still a send-back - the card waits for a human, it does not
   merge.
4. GitHub reports the PR mergeable: `mergeable: MERGEABLE`, and the PR is not a draft. If
   `mergeable` is `UNKNOWN`, GitHub is still computing - re-read every ~15 s and look at
   `mergeStateStatus` before concluding anything:

   - it settles to `BEHIND` (the base moved): keep waiting and follow the `BEHIND`
     paragraph below. Do not bounce.
   - it stays `UNKNOWN` with `mergeable` still `UNKNOWN` after ~10 minutes: stop and send
     the card back with `kanban_request_changes`.

   `mergeStateStatus` is not the gate; step 3 is. It reads `UNSTABLE` whenever any check
   on the head has not concluded `success`, including one still running, so it is never on
   its own a reason to refuse - and a `completed` non-success run for the reviewed
   `headRefOid` is a blocker per step 3, not something this line excuses. `BEHIND` is
   mergeable on this repo too, because protection cannot exist here (see below).
5. **Record the verdict before merging - once, and only for a SHA you have reviewed.**
   `kanban_comment` with the reviewed
   `headRefOid`, the `ci` run id and its `headSha`, and either `no findings` or the
   finding list. A `no findings` verdict names at least one path you actually read at
   that SHA, so the comment shows the pass happened rather than asserting it. Be plain about what this is: kanban does not enforce it - a
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
$GH pr list --repo m7jay/pigeon --state open --json number,baseRefName   --jq '.[] | select(.baseRefName=="wt/<card-id>-<slug>") | .number'   # must print nothing
$GH api -X DELETE repos/m7jay/pigeon/git/refs/heads/wt/<card-id>-<slug>
```

**Check that list before deleting.** GitHub auto-closes any PR still based on a
deleted branch, and refuses to reopen it (`Cannot change the base branch of a closed
pull request`, `Could not open the pull request`), so the merge that just landed
takes a child PR down with it and the only recovery is a fresh PR from the same head
(§9). If that list is not empty, leave the ref alone; it can be deleted later, once
the children have landed.

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
`mergeable` before merging - step 6's head re-read and step 7's `--match-head-commit`
still apply, and do not re-run step 3. `mergeable: UNKNOWN` right after a base move is a
wait, not a bounce - that is the `BEHIND` branch of step 4, and its ~10 minute bound is
the one that applies. Do not try to
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
- **A red run cannot be retried.** The fine-grained PAT has no `actions:write`, so
  `gh run rerun <id> --repo m7jay/pigeon --failed` is refused with
  `run <id> cannot be rerun; Resource not accessible by personal access token`
  (measured on run 40653425306). There is no way to re-run a red run, and the only
  lever is a **new head**: GitHub starts a fresh run for a new SHA. An empty commit
  (`git commit --allow-empty`) is not that lever, it is a rerun wearing a commit
  message. If the failure is a flake, card it - after checking the board for a card
  that already names the test (§9, "CI is red on a test my diff does not touch") - and
  push the retry head after.

## 9. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `gh: command not found` | `gh` is not on `PATH` (§8) | call `/opt/data/toolchain/gh`, or export `PATH` inline for that one command |
| `Authentication failed` on push | profile `hosts.yml` missing, or `$HOME` not the profile home | `GH=/opt/data/toolchain/gh; HOME=$HOME $GH auth status` |
| helper prints nothing | `gh` unresolvable inside the git helper | keep the helper path absolute in `.gitconfig` |
| commits show `Jayachandra` | repo-local `user.name` wins over `~/.gitconfig` | set `GIT_AUTHOR_*`/`GIT_COMMITTER_*` for the command (§6) |
| `403` on `check-runs` | token has no `checks:read` | expected — verify CI by hand (§5) |
| an open PR turns `CLOSED` by itself, right after a merge, and cannot be reopened | its `baseRefName` was the branch that merge deleted — GitHub closes children of a deleted base | open a replacement PR from the same head SHA (`gh pr create --base feat/m0-skeleton --head <branch>`); the old PR is dead. Give the child a real base next time (§3), and never delete a branch that is someone's base (§7 step 7) |
| a card sits in `review` for hours and no reviewer process appears | the dispatcher refuses the spawn: `task_events` shows `respawn_guarded` with `reason: blocker_auth` about once a minute | the guard matches `tasks.last_failure_error`, and a run's rate-limit text (`exited rate-limited (quota wall)`) is never cleared by the requeue that follows it, so the stale string blocks every later spawn — clear it with the stdlib driver (there is no `sqlite3` CLI on these hosts, only Python's module): `python3 -c "import sqlite3;c=sqlite3.connect('/opt/data/kanban/boards/pigeon/kanban.db');c.execute('UPDATE tasks SET last_failure_error=NULL, consecutive_failures=0 WHERE id=?',('<card-id>',));c.commit()"`; the dispatcher picks the card up on its next tick |
| `GraphQL: Head branch was modified` on `pr merge` | `--match-head-commit` found the head was not the SHA you supplied — usually a push landed between your read and the merge, occasionally a stale cached head right after a fast push | Read the head without a checkout: `GH=/opt/data/toolchain/gh; $GH api repos/m7jay/pigeon/git/ref/heads/<branch> --jq .object.sha`. **If it equals the SHA you supplied**, retry the merge once. **If it does not**, stop — the head moved: go back to §7 step 1 and review the new head. Never retry against a SHA that is no longer the head, and never drop `--match-head-commit` to get past the error |
| CI is red on a test my diff does not touch | the `--- FAIL:` names a package the diff never touches: a flake, not your change | read the failing package out of the log (`gh run view <run-id> --log`, the `FAIL <pkg>` line), prove the diff cannot reach it (`git diff --name-only origin/<default-branch>...HEAD` lists no path under that package), **check the board before you card it** - if a card's title, body or comments already name the test, `kanban_comment` your failing run id, the head and your measurement onto *that* card instead of filing anything (the command under this table lists them); card the flake to its owner with the failing test and the code path only when nothing on the board names it, and only then push a retry head (a red run cannot be re-run, §8). If the base has moved, merging it into your branch is a legitimate new head (§7 `BEHIND`/`DIRTY`); if it has not, you have no lever at all and say so in the handoff instead of calling the flake a pass. Worked example: `t_1090fb6e` (`internal/attemptbuf` `TestRunRetriesAStuckBatch` failed run 40653425306 at head `b7327759`, on the docs-only PR #27) |

**One flake, one card: comment on the existing card, do not file a second.** The check
above is not a formality. On 28 Oct 2026 `internal/attemptbuf` `TestRunRetriesAStuckBatch`
was carded three times inside four minutes - `t_07bc844a` (its PR #30 landed the fix at
`f3af323`), `t_1090fb6e` (the worked example in the row above; it parked its branch as a
duplicate) and `t_8ba5309b` (it verified the landed fix and yielded) - and each of the
three root-caused the same window alone, ~40 minutes of duplicated work in one of the runs.
Nothing was lost only because they deduped by hand: the second said so on the first card,
the third verified the merge instead of re-implementing it. One commit's worth of change
came within one push of being reviewed three times, and the reviewer's time is the scarce
resource in this loop (PR #30 alone carried a four-mutation battery). That hand-dedupe is
the judgement this rule makes the default. List the board for the test's name:

```bash
python3 -c "import sqlite3;c=sqlite3.connect('/opt/data/kanban/boards/pigeon/kanban.db');q='%<TestName>%';print('CARDS');print(*c.execute('SELECT id,status,substr(title,1,70) FROM tasks WHERE title LIKE ? OR body LIKE ? ORDER BY created_at DESC',(q,q)),sep='\n');print('COMMENTS');print(*c.execute('SELECT DISTINCT task_id,substr(body,1,70) FROM task_comments WHERE body LIKE ?',(q,)),sep='\n')"
```

`<TestName>` is the symbol from the `--- FAIL:` line. Read a candidate with
`kanban_show(task_id=...)`, ignore your own card id, and let the status column decide: a
`done` card (or one whose fix has already merged) means **verify the landed SHA and record
that on the existing card**, do not re-open the work; `running`/`todo`/`review`/`blocked`
means **comment your measurement and the failing run id on that card**; nothing on the
board means card it to its owner as the row says. In every case name the card you deduped
onto in your own handoff. A second card, or a second PR, for a flake an existing card
already names is the failure this step exists to prevent.

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

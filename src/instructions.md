Tools for jj (Jujutsu), a version control system layered on git. Repositories
are colocated: `.jj` and `.git` side by side, git as storage and transport.

Every tool except `workspace_identity` takes `repo`: the absolute path of the
repository or any directory inside it. Pass it whenever the tool accepts it.

How jj differs from git:

- There is no staging area. The working copy is a commit, `@`, and every
  file that is not ignored is part of it as soon as it is written. Check
  `status` before describing or closing a commit; a stray file leaves with
  `file_untrack` after adding it to `.gitignore`.
- A commit has a change id (stable across rewrites) and a commit id (changes
  on every rewrite). Refer to commits by change id.
- Making a commit: edit files, `describe` with the message, then `new` to
  start the next one. `commit` does both, optionally for some paths only.
- Rewriting is normal and cheap: `squash`, `split` (by paths, or inside a
  file with `contents`), `rebase`,
  `describe` on any mutable revision. Descendants are rebased automatically.
  Commits marked `immutable` in `log` cannot be rewritten.
- Bookmarks are git branches. They do not move when you commit; move them
  with `bookmark_set` before pushing.
- Conflicts do not stop operations. They are recorded in commits (`conflict`
  in `log`, listed by `status`) and resolved by editing the files.
- Every operation is recorded. `undo` reverts the last one; `op_log` shows
  the history.

Publishing: point a bookmark at the finished commit (`bookmark_set` with
`revision: "@-"` after `commit`), then `git_push` with that bookmark. Every
pushed commit is signed and each signature may ask the user to touch a
security key: run `git_push` with `dry_run: true` first and tell the user
how many commits will be signed (`commits to sign: N`).

For a jj command no tool covers (`bookmark delete`, `bookmark track`,
`duplicate`, `file track`, ...), use `run` with the arguments as a list,
for example `["bookmark", "delete", "old"]`. Interactive commands fail there.

Revisions use jj's revset language: `@` (working copy), `@-` (its parent),
`main`, `feat@origin`, `root()..@`, `trunk()`. Paths in `paths` are taken
literally, relative to `repo`.


Workspace ownership (when enabled by the integration): read and plan before
locking. Immediately before the first edit call `workspace_lock` with repo,
task and expected_seconds. Use the returned workspace path and revalidate the
base/live files after acquisition. Keep the lease through every edit, required
check and saved result. A busy/overdue lease remains reserved; wait briefly or
request parallel=true. Never steal it based on an estimate. Caller identity and
generations are supplied by the integration; do not invent IDs or manage tokens.
Read tools in this mode show saved snapshots and never capture live edits. Use
an explicit full revision for stable content. Local writes protect other
outstanding workspace leases, including against indirect rewrites. Released
results can be merged, rebased, squashed or edited by any executor holding a
workspace lease; bookmark_set can move main. Adopted changes remain protected
until that executor unlocks. Undo and network mutations require all other leases
in the repository to be released; run requires all other leases coordinated by
the backend to be released because aliases/overrides can target other repositories.
External editing and build/test tools need the integration's begin/end hooks.
After all processes stop and checks pass, workspace_unlock with repo, a final
message and checks_complete=true saves and verifies the result, then releases.
Unlock releases protection of the result; it does not integrate or publish
automatically. If a transition was interrupted, inspect retained work before
explicitly resuming it; other mutations and new acquisitions wait for recovery. Use
workspace_identity to diagnose identity delivery, workspace_lock_status for
occupancy, and workspace_cleanup only for explicit idle-copy cleanup. Cleanup
requires saved work to be integrated into local main (or trunk()), or an explicit
integrated_into destination. It retains unintegrated or locally changed copies
and reports retained_reasons. Unlock alone does not prove integration.

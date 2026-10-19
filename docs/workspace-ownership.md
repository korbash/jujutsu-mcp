# Workspace ownership

```sh
jujutsu-mcp --ownership-state /absolute/external/state --idle-limit 1
```

Cooperating backends must share this directory on a local filesystem with
advisory locks and atomic rename. Keep it outside source trees. Without the flag,
workspace ownership is disabled; `setup` registers the server without ownership.

## Task workflow

1. Read and plan without locking. Reads use saved snapshots
   (`--ignore-working-copy --at-operation=@`); live file reads may see edits.
   Use a full revision ID when a consistent view is needed.
2. Call `workspace_lock` before editing, then revalidate the base and live files.
   Use its returned path. Retry returns the same assignment; a busy caller can
   wait up to 30 seconds or request `parallel: true`.
3. Keep ownership through all edits and checks, including external hooks below.
   Duration is an estimate: overdue or disconnected owners retain their locks.
4. After processes stop and checks pass, call `workspace_unlock` with a final
   `message` and `checks_complete: true`. It saves the result and prepares a clean
   child for reuse, releasing protection of the task's changes. It does not
   automatically integrate, move bookmarks or publish.

An executor has at most one active workspace per shared repository. An outstanding
lease protects its changes until explicit unlock; released results have no
continuing task ownership. Any executor holding a workspace lease can integrate
released results with `new`, `rebase`, `squash`, `edit` and `bookmark_set`, including
moving main. Changes it rewrites, checks out or targets with a bookmark move are
reserved until its own unlock. Other outstanding work remains protected, including
against indirect descendant rewrites.

`undo`, `git_fetch` and `git_push` can affect the whole repository and wait for
every other lease there to be released. Generic `run` waits for every other lease
coordinated by the backend, since its aliases and overrides may target other
repositories. These operations then use the caller's ordinary lease; no separate
operator mode is required. Interrupted transitions retain their reservations and
must be resumed before other mutations or new acquisitions in that repository.
See [tools.md](tools.md#workspace-ownership-opt-in) for parameters and results.

## Caller identity

Integrations attach metadata to `tools/call`, outside tool arguments:

```json
{
  "_meta": {
    "jujutsu-mcp/identity": {
      "provider": "claude",
      "session_id": "session",
      "executor_id": "child-or-explicit-main",
      "source": "acp",
      "session_name": "Fix authentication"
    },
    "jujutsu-mcp/generation": 17
  }
}
```

`source` is `acp`, `claude` or `adapter`. Provider/session/executor/source identify
the owner; children need distinct executor IDs. Missing or conflicting identity
fails. ACP context must be forwarded explicitly; the HTTP `Mcp-Session-Id` is not
caller identity. Without forwarded context, native Codex `threadId` identifies
the executor; `sessionId` alone is insufficient and is only a diagnostic alias.

Optional `session_name` is a display label, independent of ownership and aliases.
Native clients can supply `_meta["jujutsu-mcp/session-name"]`. Names are trimmed,
nonempty, at most 512 bytes and contain no control characters. Omitted names
preserve the stored label; renames never transfer a lease.

Adapters retain the returned generation and attach it to subsequent calls. Native
direct clients may omit it, but requests delayed before backend arrival then
cannot be distinguished from fresh requests after reacquisition. The model never
supplies identity or generations.

For one dedicated executor, the integration can supply an identity JSON file:

```sh
python3 adapters/stdio_identity.py --identity-file /run/agent/identity.json -- \
  jujutsu-mcp --ownership-state /absolute/external/state
```

The file contains the identity fields above. Do not use one adapter instance for
pooled callers. Verify `workspace_identity` through each actual editor/agent path,
including children; isolated tests do not establish Zed/Claude metadata delivery.

## External edits and processes

MCP checks cannot intercept Edit/Write/apply_patch or shell writes. Integrations
must block these on a failed hook and bracket edits/builds/tests with:

```sh
jujutsu-mcp workspace-hook --ownership-state /absolute/external/state
```

Pass JSON on stdin, using the same metadata and adapter-managed generation:

```json
{
  "repo": "/absolute/assigned/workspace",
  "metadata": {"threadId": "codex-thread", "jujutsu-mcp/generation": 17},
  "activity_id": "unique-tool-invocation",
  "phase": "begin"
}
```

Send matching `phase: "end"` only after the operation and any background processes
exit. Active activities prevent unlock across restarts. Cover every workspace a
command writes to. Claude/ACP adapters must explicitly distinguish main and child
executors and handle completion, failure and cancellation. These are coordination
hooks for trusted integrations, not authentication or a filesystem sandbox.

## Recovery and cleanup

Ownership survives restart. After an interrupted transition, inspect retained
work and stop old processes, then acquire as the same owner with `resume: true`.
Finish missing activity end hooks only after verifying processes stopped.
Missing/partially created workspaces and interrupted cleanup require operator
repair. Never reset corrupt state, replace `backend.lock` while servers run, or
infer abandonment from age. Back up state together with its managed workspaces.

`workspace_cleanup` keeps `--idle-limit` extra copies (default 1).
Direct reacquisition copies managed status and the workspace name from the latest
lease, keeping server-managed copies eligible. A path inside the pool alone never
proves server ownership. Removal ends the old allocation, so a manually recreated
workspace is not adopted from stale history.

For copies beyond the idle limit, every saved task head from every use must be
integrated into local `main` (falling back to `trunk()`) or explicit
`integrated_into` destinations.
Destinations must be conflict-free named bookmarks/tags. Ancestry or identical
saved trees prove integration; uncertain squash/rebase matches retain the copy.
Tracked live bytes/modes must match the saved result. Modified, ignored/untracked,
missing or unsupported files and unknown metadata retain the copy, with reasons
in the result. Primary checkouts are never removed.

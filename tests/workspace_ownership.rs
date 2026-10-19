//! Isolated MCP clients exercise the wire metadata contract and persistent state.
mod common;

use std::path::Path;

use common::{Client, TestRepo, text_of};
use jujutsu_mcp::ownership::{Ownership, caller};
use jujutsu_mcp::server::JjServer;
use rmcp::ServiceExt;
use rmcp::model::{CallToolRequestParams, RequestMetaObject, RequestParamsMeta};
use serde_json::{Value, json};

async fn client(repo: &TestRepo, state: &Path) -> Client {
    client_with_idle_limit(repo, state, 1).await
}

async fn client_with_idle_limit(repo: &TestRepo, state: &Path, idle_limit: usize) -> Client {
    let (server_io, client_io) = tokio::io::duplex(1 << 20);
    let server = JjServer::new(repo.runner())
        .with_ownership(Ownership::new(state.to_path_buf(), idle_limit).unwrap());
    tokio::spawn(async move {
        let service = server.serve(server_io).await.unwrap();
        let _ = service.waiting().await;
    });
    ().serve(client_io).await.unwrap()
}

fn metadata(session: &str) -> RequestMetaObject {
    json!({"threadId": session, "sessionId": session})
        .as_object()
        .unwrap()
        .clone()
        .into()
}

async fn request(
    client: &Client,
    tool: &str,
    arguments: Value,
    meta: RequestMetaObject,
) -> rmcp::model::CallToolResult {
    let mut params = CallToolRequestParams::new(tool.to_owned())
        .with_arguments(arguments.as_object().unwrap().clone());
    params.set_meta(meta);
    tokio::time::timeout(common::CALL_TIMEOUT, client.call_tool(params))
        .await
        .unwrap()
        .unwrap()
}

async fn lock(client: &Client, repo: &Value, session: &str, parallel: bool) -> Value {
    let result = request(
        client,
        "workspace_lock",
        json!({"repo": repo, "task": session, "expected_seconds": 60, "parallel": parallel}),
        metadata(session),
    )
    .await;
    assert_ne!(result.is_error, Some(true), "{result:?}");
    result.structured_content.unwrap()
}

async fn unlock(client: &Client, repo: &Value, session: &str) -> Value {
    let result = request(
        client,
        "workspace_unlock",
        json!({"repo": repo, "message": "feat: saved task", "checks_complete": true}),
        metadata(session),
    )
    .await;
    assert_ne!(result.is_error, Some(true), "{result:?}");
    result.structured_content.unwrap()
}

#[test]
fn identity_is_namespaced_and_requires_executor_context() {
    let codex = caller(&metadata("same")).unwrap();
    let explicit: RequestMetaObject = json!({"jujutsu-mcp/identity": {"provider": "claude", "source": "acp", "session_id": "same", "executor_id": "main"}}).as_object().unwrap().clone().into();
    let claude = caller(&explicit).unwrap();
    assert!(!codex.same_executor(&claude));
    assert!(caller(&RequestMetaObject::new()).is_err());
    let ambiguous: RequestMetaObject = json!({"sessionId": "parent"})
        .as_object()
        .unwrap()
        .clone()
        .into();
    assert!(caller(&ambiguous).is_err());
    let conflict: RequestMetaObject = json!({"threadId": "one", "jujutsu-mcp/identity": {"provider": "claude", "source": "acp", "session_id": "one", "executor_id": "main"}}).as_object().unwrap().clone().into();
    assert!(caller(&conflict).is_err());
}

#[tokio::test]
async fn independent_servers_race_retry_restart_and_reuse() {
    let repo = TestRepo::new();
    let state = tempfile::tempdir().unwrap();
    let a = client(&repo, state.path()).await;
    let b = client(&repo, state.path()).await;
    let arg = repo.repo_arg();
    let (first, second) = tokio::join!(lock(&a, &arg, "a", false), lock(&b, &arg, "b", false));
    assert_ne!(first["acquired"], second["acquired"]);
    let (winner, loser, owner, other, assignment) = if first["acquired"] == true {
        (&a, &b, "a", "b", first)
    } else {
        (&b, &a, "b", "a", second)
    };
    let retry = lock(winner, &arg, owner, false).await;
    assert_eq!(
        assignment["lease"]["generation"],
        retry["lease"]["generation"]
    );
    let reconnect = client(&repo, state.path()).await;
    assert_eq!(
        retry["lease"]["generation"],
        lock(&reconnect, &arg, owner, false).await["lease"]["generation"]
    );
    let denied = request(
        loser,
        "describe",
        json!({"repo": arg, "message": "steal"}),
        metadata(other),
    )
    .await;
    assert_eq!(denied.is_error, Some(true), "{denied:?}");
    let denied = request(
        loser,
        "workspace_unlock",
        json!({"repo": arg, "message": "steal", "checks_complete": true}),
        metadata(other),
    )
    .await;
    assert_eq!(denied.is_error, Some(true));
    repo.write("saved.txt", "keep me\n");
    let saved = unlock(winner, &arg, owner).await;
    assert_eq!(saved["state"], "released");
    let next = lock(loser, &arg, other, false).await;
    assert_eq!(next["lease"]["workspace"], assignment["lease"]["workspace"]);
    assert_ne!(next["lease"]["change_id"], assignment["lease"]["change_id"]);
    assert_ne!(
        next["lease"]["generation"],
        assignment["lease"]["generation"]
    );
    let mut stale = metadata(other);
    stale.insert(
        "jujutsu-mcp/generation".into(),
        assignment["lease"]["generation"].clone(),
    );
    let result = request(
        loser,
        "describe",
        json!({"repo": arg, "message": "stale"}),
        stale,
    )
    .await;
    assert_eq!(result.is_error, Some(true));
    assert!(
        repo.jj(&[
            "file",
            "show",
            "-r",
            saved["saved_commit"].as_str().unwrap(),
            "saved.txt"
        ])
        .contains("keep me")
    );
}

#[tokio::test]
async fn reads_do_not_snapshot_and_identity_reaches_backend() {
    let repo = TestRepo::new();
    let state = tempfile::tempdir().unwrap();
    let client = client(&repo, state.path()).await;
    let identity = request(&client, "workspace_identity", json!({}), metadata("native")).await;
    assert_eq!(
        identity.structured_content.unwrap()["source"],
        "codex-metadata"
    );
    let before = repo.jj(&[
        "--ignore-working-copy",
        "op",
        "log",
        "--no-graph",
        "--limit=1",
        "-T",
        "id",
    ]);
    repo.write("unsnapshotted", "live\n");
    for (tool, arguments) in [
        ("status", json!({"repo": repo.repo_arg()})),
        ("log", json!({"repo": repo.repo_arg()})),
        ("show", json!({"repo": repo.repo_arg()})),
        ("diff", json!({"repo": repo.repo_arg()})),
        ("bookmark_list", json!({"repo": repo.repo_arg()})),
        ("op_log", json!({"repo": repo.repo_arg()})),
    ] {
        let result = request(&client, tool, arguments, RequestMetaObject::new()).await;
        assert_ne!(result.is_error, Some(true), "{tool}: {result:?}");
    }
    assert_eq!(
        before,
        repo.jj(&[
            "--ignore-working-copy",
            "op",
            "log",
            "--no-graph",
            "--limit=1",
            "-T",
            "id"
        ])
    );
    let result = request(
        &client,
        "describe",
        json!({"repo": repo.repo_arg(), "message": "missing"}),
        RequestMetaObject::new(),
    )
    .await;
    assert_eq!(result.is_error, Some(true));
    assert!(text_of(&result).contains("identity"));
}

#[tokio::test]
async fn parallel_workspaces_are_isolated_and_foreign_revisions_protected() {
    let repo = TestRepo::new();
    let state = tempfile::tempdir().unwrap();
    let a = client(&repo, state.path()).await;
    let b = client(&repo, state.path()).await;
    let primary = lock(&a, &repo.repo_arg(), "a", false).await;
    let parallel = lock(&b, &repo.repo_arg(), "b", true).await;
    assert_ne!(
        primary["lease"]["workspace"],
        parallel["lease"]["workspace"]
    );
    let path = parallel["lease"]["workspace"].clone();
    assert_eq!(
        parallel["lease"]["generation"],
        lock(&b, &repo.repo_arg(), "b", true).await["lease"]["generation"]
    );
    let own = request(
        &b,
        "describe",
        json!({"repo": path, "message": "own"}),
        metadata("b"),
    )
    .await;
    assert_ne!(own.is_error, Some(true), "{own:?}");
    for (tool, extra) in [
        (
            "describe",
            json!({"message": "foreign", "revision": primary["lease"]["change_id"]}),
        ),
        ("edit", json!({"revision": primary["lease"]["change_id"]})),
        (
            "run",
            json!({"args": ["--repository", repo.path, "describe", "-m", "escape"]}),
        ),
        ("undo", json!({})),
        (
            "bookmark_set",
            json!({"name": "foreign", "revision": primary["lease"]["change_id"]}),
        ),
        ("rebase", json!({"onto": primary["lease"]["change_id"]})),
    ] {
        let mut arguments = extra.as_object().unwrap().clone();
        arguments.insert("repo".into(), path.clone());
        let denied = request(&b, tool, Value::Object(arguments), metadata("b")).await;
        assert_eq!(denied.is_error, Some(true), "{tool}: {denied:?}");
    }
    unlock(&b, &path, "b").await;
    let reused = lock(&b, &repo.repo_arg(), "b", true).await;
    assert_eq!(reused["lease"]["workspace"], path);
}

#[tokio::test]
async fn released_results_can_be_merged_and_main_moved_by_another_executor() {
    let repo = cleanup_repo();
    let state = tempfile::tempdir().unwrap();
    let client = client(&repo, state.path()).await;
    lock(&client, &repo.repo_arg(), "a", false).await;
    let b = lock(&client, &repo.repo_arg(), "b", true).await;
    repo.write("a.txt", "a\n");
    std::fs::write(
        Path::new(b["lease"]["workspace"].as_str().unwrap()).join("b.txt"),
        "b\n",
    )
    .unwrap();
    let saved_a = unlock(&client, &repo.repo_arg(), "a").await;
    let saved_b = unlock(&client, &b["lease"]["workspace"], "b").await;

    // A new executor uses the same ownership-enabled server for the whole integration.
    lock(&client, &repo.repo_arg(), "integrator", false).await;
    for (tool, arguments) in [
        (
            "new",
            json!({"parents": ["main", saved_a["saved_commit"], saved_b["saved_commit"]], "message": "merge results"}),
        ),
        ("bookmark_set", json!({"name": "main", "revision": "@"})),
    ] {
        let mut arguments = arguments.as_object().unwrap().clone();
        arguments.insert("repo".into(), repo.repo_arg());
        let result = request(
            &client,
            tool,
            Value::Object(arguments),
            metadata("integrator"),
        )
        .await;
        assert_ne!(result.is_error, Some(true), "{tool}: {result:?}");
    }
    assert_eq!(repo.jj(&["file", "show", "-r", "main", "a.txt"]), "a\n");
    assert_eq!(repo.jj(&["file", "show", "-r", "main", "b.txt"]), "b\n");
    let saved = unlock(&client, &repo.repo_arg(), "integrator").await;
    assert_eq!(
        repo.jj(&["log", "--no-graph", "-r", "main", "-T", "commit_id"]),
        saved["saved_commit"].as_str().unwrap()
    );
}

#[tokio::test]
async fn released_result_is_rewritable_while_an_unrelated_workspace_remains_active() {
    let repo = cleanup_repo();
    let state = tempfile::tempdir().unwrap();
    let client = client(&repo, state.path()).await;
    lock(&client, &repo.repo_arg(), "a", false).await;
    let b = lock(&client, &repo.repo_arg(), "b", true).await;
    repo.write("a.txt", "a\n");
    let saved = unlock(&client, &repo.repo_arg(), "a").await;
    let change = saved["change_id"].clone();
    let adopted = request(
        &client,
        "describe",
        json!({"repo": b["lease"]["workspace"], "revision": change, "message": "adopted"}),
        metadata("b"),
    )
    .await;
    assert_ne!(adopted.is_error, Some(true), "{adopted:?}");

    lock(&client, &repo.repo_arg(), "c", false).await;
    let denied = request(
        &client,
        "describe",
        json!({"repo": repo.repo_arg(), "revision": change, "message": "must wait"}),
        metadata("c"),
    )
    .await;
    assert_eq!(denied.is_error, Some(true), "{denied:?}");
    unlock(&client, &b["lease"]["workspace"], "b").await;
    for (tool, arguments) in [
        ("rebase", json!({"revisions": change, "onto": "main"})),
        ("edit", json!({"revision": change})),
        ("describe", json!({"message": "now available"})),
        ("bookmark_set", json!({"name": "main", "revision": "@"})),
    ] {
        let mut arguments = arguments.as_object().unwrap().clone();
        arguments.insert("repo".into(), repo.repo_arg());
        let result = request(&client, tool, Value::Object(arguments), metadata("c")).await;
        assert_ne!(result.is_error, Some(true), "{tool}: {result:?}");
    }
    assert_eq!(repo.description("main"), "now available\n");
}

#[tokio::test]
async fn checking_out_released_work_reserves_it_until_unlock() {
    let repo = cleanup_repo();
    let state = tempfile::tempdir().unwrap();
    let client = client(&repo, state.path()).await;
    lock(&client, &repo.repo_arg(), "a", false).await;
    let b = lock(&client, &repo.repo_arg(), "b", true).await;
    let c = lock(&client, &repo.repo_arg(), "c", true).await;
    repo.write("saved.txt", "saved\n");
    let saved = unlock(&client, &repo.repo_arg(), "a").await;
    let edited = request(
        &client,
        "edit",
        json!({"repo": b["lease"]["workspace"], "revision": saved["change_id"]}),
        metadata("b"),
    )
    .await;
    assert_ne!(edited.is_error, Some(true), "{edited:?}");
    let denied = request(
        &client,
        "edit",
        json!({"repo": c["lease"]["workspace"], "revision": saved["change_id"]}),
        metadata("c"),
    )
    .await;
    assert_eq!(denied.is_error, Some(true), "{denied:?}");
    unlock(&client, &b["lease"]["workspace"], "b").await;
    let edited = request(
        &client,
        "edit",
        json!({"repo": c["lease"]["workspace"], "revision": saved["change_id"]}),
        metadata("c"),
    )
    .await;
    assert_ne!(edited.is_error, Some(true), "{edited:?}");
}

#[tokio::test]
async fn rewriting_an_ancestor_cannot_indirectly_change_active_work() {
    let repo = cleanup_repo();
    let state = tempfile::tempdir().unwrap();
    let client = client(&repo, state.path()).await;
    lock(&client, &repo.repo_arg(), "a", false).await;
    let b = lock(&client, &repo.repo_arg(), "b", true).await;
    let denied = request(
        &client,
        "describe",
        json!({"repo": b["lease"]["workspace"], "revision": "main", "message": "would rebase a"}),
        metadata("b"),
    )
    .await;
    assert_eq!(denied.is_error, Some(true), "{denied:?}");
    assert_eq!(repo.description("main"), "base\n");
    unlock(&client, &repo.repo_arg(), "a").await;
    let allowed = request(
        &client,
        "describe",
        json!({"repo": b["lease"]["workspace"], "revision": "main", "message": "available"}),
        metadata("b"),
    )
    .await;
    assert_ne!(allowed.is_error, Some(true), "{allowed:?}");
}

#[tokio::test]
async fn broad_operations_wait_for_active_leases_instead_of_requiring_a_separate_mode() {
    let repo = cleanup_repo();
    let state = tempfile::tempdir().unwrap();
    let client = client(&repo, state.path()).await;
    lock(&client, &repo.repo_arg(), "a", false).await;
    let b = lock(&client, &repo.repo_arg(), "b", true).await;
    for (tool, arguments) in [
        (
            "run",
            json!({"args": ["bookmark", "create", "integrated", "-r", "main"]}),
        ),
        ("undo", json!({})),
        ("git_fetch", json!({})),
        ("git_push", json!({"bookmarks": ["main"], "dry_run": true})),
    ] {
        let mut arguments = arguments.as_object().unwrap().clone();
        arguments.insert("repo".into(), repo.repo_arg());
        let denied = request(&client, tool, Value::Object(arguments), metadata("a")).await;
        assert_eq!(denied.is_error, Some(true), "{tool}: {denied:?}");
        assert!(
            text_of(&denied).contains("active leases"),
            "{tool}: {denied:?}"
        );
    }
    unlock(&client, &b["lease"]["workspace"], "b").await;
    let result = request(&client, "run", json!({"repo": repo.repo_arg(), "args": ["bookmark", "create", "integrated", "-r", "main"]}), metadata("a")).await;
    assert_ne!(result.is_error, Some(true), "{result:?}");
    assert_eq!(repo.change_id("integrated"), repo.change_id("main"));
    let result = request(
        &client,
        "undo",
        json!({"repo": repo.repo_arg()}),
        metadata("a"),
    )
    .await;
    assert_ne!(result.is_error, Some(true), "{result:?}");
    assert!(repo.jj(&["bookmark", "list", "integrated"]).is_empty());
}

#[tokio::test]
async fn ownership_mode_can_fetch_and_push_after_other_workspaces_are_released() {
    let remote = common::SigningRemote::new();
    let repo = remote.repo();
    repo.write("base.txt", "base\n");
    repo.jj(&["commit", "-m", "base"]);
    repo.jj(&["bookmark", "create", "main", "-r", "@-"]);
    let state = tempfile::tempdir().unwrap();
    let client = client(&repo, state.path()).await;
    lock(&client, &repo.repo_arg(), "a", false).await;
    let b = lock(&client, &repo.repo_arg(), "b", true).await;
    unlock(&client, &b["lease"]["workspace"], "b").await;
    for tool in ["git_fetch", "git_push"] {
        let result = request(
            &client,
            tool,
            json!({"repo": repo.repo_arg(), "bookmarks": ["main"]}),
            metadata("a"),
        )
        .await;
        assert_ne!(result.is_error, Some(true), "{tool}: {result:?}");
    }
    assert_eq!(
        remote.branch("main").unwrap(),
        repo.jj(&["log", "--no-graph", "-r", "main", "-T", "commit_id"])
    );
}

#[tokio::test]
async fn arbitrary_run_waits_for_other_repositories_but_dedicated_tools_do_not() {
    let repo = TestRepo::new();
    let other_repo = TestRepo::new();
    let state = tempfile::tempdir().unwrap();
    let a = client(&repo, state.path()).await;
    let b = client(&other_repo, state.path()).await;
    lock(&a, &repo.repo_arg(), "a", false).await;
    lock(&b, &other_repo.repo_arg(), "b", false).await;
    let described = request(
        &a,
        "describe",
        json!({"repo": repo.repo_arg(), "message": "independent"}),
        metadata("a"),
    )
    .await;
    assert_ne!(described.is_error, Some(true), "{described:?}");
    let args = json!({"repo": repo.repo_arg(), "args": ["bookmark", "create", "ready", "-r", "@"]});
    let denied = request(&a, "run", args.clone(), metadata("a")).await;
    assert_eq!(denied.is_error, Some(true), "{denied:?}");
    unlock(&b, &other_repo.repo_arg(), "b").await;
    let result = request(&a, "run", args, metadata("a")).await;
    assert_ne!(result.is_error, Some(true), "{result:?}");
}

#[tokio::test]
async fn bookmark_target_is_reserved_only_until_its_current_owner_unlocks() {
    let repo = cleanup_repo();
    repo.write("result.txt", "ready\n");
    repo.jj(&["describe", "-m", "free result"]);
    let result_change = repo.change_id("@");
    repo.jj(&["new", "main"]);
    let state = tempfile::tempdir().unwrap();
    let client = client(&repo, state.path()).await;
    lock(&client, &repo.repo_arg(), "a", false).await;
    let b = lock(&client, &repo.repo_arg(), "b", true).await;
    let args = json!({"repo": repo.repo_arg(), "name": "main", "revision": result_change});
    let result = request(&client, "bookmark_set", args, metadata("a")).await;
    assert_ne!(result.is_error, Some(true), "{result:?}");
    let args = json!({"repo": b["lease"]["workspace"], "name": "main", "revision": "@", "allow_backwards": true});
    let denied = request(&client, "bookmark_set", args.clone(), metadata("b")).await;
    assert_eq!(denied.is_error, Some(true), "{denied:?}");
    unlock(&client, &repo.repo_arg(), "a").await;
    let result = request(&client, "bookmark_set", args, metadata("b")).await;
    assert_ne!(result.is_error, Some(true), "{result:?}");
}

#[tokio::test]
async fn interrupted_rewrite_recovers_its_reservation_on_preexisting_work() {
    let repo = cleanup_repo();
    repo.write("old.txt", "old\n");
    repo.jj(&["describe", "-m", "free result"]);
    let old = repo.change_id("@");
    repo.jj(&["new", "main"]);
    let state = tempfile::tempdir().unwrap();
    let client = client(&repo, state.path()).await;
    lock(&client, &repo.repo_arg(), "a", false).await;
    let b = lock(&client, &repo.repo_arg(), "b", true).await;
    let inventory = repo.jj(&[
        "--ignore-working-copy",
        "log",
        "--no-graph",
        "-r",
        "all()",
        "-T",
        "commit_id ++ \" \" ++ change_id ++ \"\\n\"",
    ]);
    let ownership = Ownership::new(state.path().into(), 1).unwrap();
    {
        let mut tx = ownership.try_transaction().unwrap().unwrap();
        tx.state.leases[0].mutation_snapshot = Some(
            inventory
                .lines()
                .map(|line| {
                    let (commit, change) = line.split_once(' ').unwrap();
                    (commit.to_owned(), change.to_owned())
                })
                .collect(),
        );
        tx.state.leases[0].state = "mutating".into();
        tx.save().unwrap();
    }
    // Simulate a jj operation committed before the server could save its inventory.
    repo.jj(&["describe", "-r", &old, "-m", "interrupted adoption"]);
    let denied = request(
        &client,
        "workspace_lock",
        json!({"repo": repo.repo_arg(), "task": "c", "expected_seconds": 60, "parallel": true}),
        metadata("c"),
    )
    .await;
    assert_eq!(denied.is_error, Some(true), "{denied:?}");
    let denied = request(
        &client,
        "describe",
        json!({"repo": b["lease"]["workspace"], "message": "must wait for recovery"}),
        metadata("b"),
    )
    .await;
    assert_eq!(denied.is_error, Some(true), "{denied:?}");
    let resumed = request(
        &client,
        "workspace_lock",
        json!({"repo": repo.repo_arg(), "task": "a", "expected_seconds": 60, "resume": true}),
        metadata("a"),
    )
    .await;
    assert_ne!(resumed.is_error, Some(true), "{resumed:?}");
    let args = json!({"repo": b["lease"]["workspace"], "revision": old, "message": "available"});
    let denied = request(&client, "describe", args.clone(), metadata("b")).await;
    assert_eq!(denied.is_error, Some(true), "{denied:?}");
    unlock(&client, &repo.repo_arg(), "a").await;
    let result = request(&client, "describe", args, metadata("b")).await;
    assert_ne!(result.is_error, Some(true), "{result:?}");
}

#[tokio::test]
async fn canonical_subdirectories_cannot_bypass_ownership() {
    let repo = TestRepo::new();
    std::fs::create_dir(repo.path.join("sub")).unwrap();
    let state = tempfile::tempdir().unwrap();
    let client = client(&repo, state.path()).await;
    lock(&client, &repo.repo_arg(), "owner", false).await;
    let alias = json!(repo.path.join("sub/.."));
    assert_eq!(
        lock(&client, &alias, "intruder", false).await["acquired"],
        false
    );
    #[cfg(unix)]
    {
        let link = repo.temp_root().join("alias");
        std::os::unix::fs::symlink(&repo.path, &link).unwrap();
        assert_eq!(
            lock(&client, &json!(link), "intruder", false).await["acquired"],
            false
        );
    }
}

#[tokio::test]
async fn parallel_allocation_does_not_snapshot_the_source_and_materializes_base() {
    let repo = TestRepo::new();
    repo.write("base.txt", "base\n");
    repo.jj(&["commit", "-m", "base"]);
    let state = tempfile::tempdir().unwrap();
    let a = client(&repo, state.path()).await;
    let b = client(&repo, state.path()).await;
    lock(&a, &repo.repo_arg(), "a", false).await;
    let before = repo.jj(&[
        "--ignore-working-copy",
        "log",
        "--no-graph",
        "-r",
        "@",
        "-T",
        "commit_id",
    ]);
    repo.write("base.txt", "live unfinished work\n");
    let parallel = lock(&b, &repo.repo_arg(), "b", true).await;
    let assigned = Path::new(parallel["lease"]["workspace"].as_str().unwrap());
    assert_eq!(
        std::fs::read_to_string(assigned.join("base.txt")).unwrap(),
        "base\n"
    );
    assert_eq!(
        before,
        repo.jj(&[
            "--ignore-working-copy",
            "log",
            "--no-graph",
            "-r",
            "@",
            "-T",
            "commit_id"
        ])
    );
    assert_eq!(
        std::fs::read_to_string(repo.path.join("base.txt")).unwrap(),
        "live unfinished work\n"
    );
}

#[tokio::test]
async fn external_activity_prevents_unlock_and_survives_reconnect() {
    use jujutsu_mcp::ownership::{HookInput, hook};
    let repo = TestRepo::new();
    let state = tempfile::tempdir().unwrap();
    let client = client(&repo, state.path()).await;
    let assignment = lock(&client, &repo.repo_arg(), "owner", false).await;
    let ownership = Ownership::new(state.path().into(), 1).unwrap();
    let mut meta = metadata("owner");
    meta.insert(
        "jujutsu-mcp/generation".into(),
        assignment["lease"]["generation"].clone(),
    );
    let input = |phase: &str| HookInput {
        repo: repo.path.to_string_lossy().into_owned(),
        metadata: meta.clone(),
        activity_id: "build-1".into(),
        phase: phase.into(),
    };
    hook(&ownership, input("begin")).unwrap();
    let denied = request(
        &client,
        "workspace_unlock",
        json!({"repo": repo.repo_arg(), "message": "in flight", "checks_complete": true}),
        metadata("owner"),
    )
    .await;
    assert_eq!(denied.is_error, Some(true));
    assert!(text_of(&denied).contains("in flight"));
    hook(&ownership, input("end")).unwrap();
    unlock(&client, &repo.repo_arg(), "owner").await;
    lock(&client, &repo.repo_arg(), "owner", false).await;
    assert!(hook(&ownership, input("begin")).is_err());
}

#[tokio::test]
async fn recovery_overdue_and_bounded_busy_wait_never_steal() {
    let repo = TestRepo::new();
    let state = tempfile::tempdir().unwrap();
    let client = client(&repo, state.path()).await;
    let original = lock(&client, &repo.repo_arg(), "owner", false).await;
    let ownership = Ownership::new(state.path().into(), 1).unwrap();
    {
        let mut tx = ownership.try_transaction().unwrap().unwrap();
        tx.state.leases[0].state = "releasing".into();
        tx.state.leases[0].expected_completion = 1;
        tx.save().unwrap();
    }
    let status = request(
        &client,
        "workspace_lock_status",
        json!({"repo": repo.repo_arg()}),
        metadata("other"),
    )
    .await;
    assert_eq!(status.structured_content.unwrap()["overdue"], true);
    let started = std::time::Instant::now();
    let busy = request(
        &client,
        "workspace_lock",
        json!({"repo": repo.repo_arg(), "task": "other", "expected_seconds": 1, "wait_seconds": 1}),
        metadata("other"),
    )
    .await;
    assert_eq!(busy.structured_content.unwrap()["acquired"], false);
    assert!(started.elapsed() >= std::time::Duration::from_secs(1));
    assert!(started.elapsed() < std::time::Duration::from_secs(3));
    let resumed = request(
        &client,
        "workspace_lock",
        json!({"repo": repo.repo_arg(), "task": "owner", "expected_seconds": 60, "resume": true}),
        metadata("owner"),
    )
    .await;
    assert_eq!(
        resumed.structured_content.unwrap()["lease"]["generation"],
        original["lease"]["generation"]
    );
    unlock(&client, &repo.repo_arg(), "owner").await;
}

fn cleanup_repo() -> TestRepo {
    let repo = TestRepo::new();
    repo.write(".gitignore", "cache.ignored\n");
    repo.write("base.txt", "base\n");
    repo.jj(&["describe", "-m", "base"]);
    repo.jj(&["bookmark", "create", "main", "-r", "@"]);
    repo.jj(&["new"]);
    repo
}

fn integrate(repo: &TestRepo, heads: &[String], bookmark: &str) -> String {
    let message = format!("cleanup integration {} {}", bookmark, heads.join(" "));
    let mut args = vec![
        "--ignore-working-copy".to_owned(),
        "new".into(),
        "--no-edit".into(),
        format!("--message={message}"),
        "--".into(),
        "main".into(),
    ];
    args.extend_from_slice(heads);
    repo.jj(&args.iter().map(String::as_str).collect::<Vec<_>>());
    let revision = format!(
        "description(exact:{})",
        serde_json::to_string(&format!("{message}\n")).unwrap()
    );
    let commit = repo.jj(&[
        "--ignore-working-copy",
        "log",
        "--no-graph",
        "-r",
        &revision,
        "-T",
        "commit_id",
    ]);
    repo.jj(&[
        "--ignore-working-copy",
        "bookmark",
        "set",
        bookmark,
        "--allow-backwards",
        "-r",
        &commit,
    ]);
    commit
}

async fn cleanup(client: &Client, repo: &Value, target: Option<&str>) -> Value {
    let mut args = json!({"repo": repo});
    if let Some(target) = target {
        args["integrated_into"] = json!(target);
    }
    let result = request(client, "workspace_cleanup", args, metadata("operator")).await;
    assert_ne!(result.is_error, Some(true), "{result:?}");
    result.structured_content.unwrap()
}

#[tokio::test]
async fn direct_path_reacquisition_keeps_managed_copy_eligible_for_cleanup() {
    let repo = cleanup_repo();
    let state = tempfile::tempdir().unwrap();
    let client = client_with_idle_limit(&repo, state.path(), 0).await;
    lock(&client, &repo.repo_arg(), "a", false).await;
    let first = lock(&client, &repo.repo_arg(), "b", true).await;
    let path_arg = first["lease"]["workspace"].clone();
    let path = Path::new(path_arg.as_str().unwrap());
    std::fs::write(path.join("first.txt"), "first\n").unwrap();
    let first_saved = unlock(&client, &path_arg, "b").await;

    let second = lock(&client, &path_arg, "c", false).await;
    assert_eq!(second["lease"]["managed"], true);
    assert_eq!(
        second["lease"]["workspace_name"],
        first["lease"]["workspace_name"]
    );
    std::fs::write(path.join("second.txt"), "second\n").unwrap();
    let second_saved = unlock(&client, &path_arg, "c").await;
    let retained = cleanup(&client, &repo.repo_arg(), None).await;
    assert_eq!(retained["retained"], json!([path]));
    assert!(
        retained["retained_reasons"][path.to_str().unwrap()]
            .as_str()
            .unwrap()
            .contains("not integrated")
    );
    integrate(
        &repo,
        &[
            first_saved["saved_commit"].as_str().unwrap().into(),
            second_saved["saved_commit"].as_str().unwrap().into(),
        ],
        "main",
    );
    let removed = cleanup(&client, &repo.repo_arg(), None).await;
    assert_eq!(removed["removed"], json!([path]));
    assert!(!path.exists());
    assert!(repo.path.is_dir());
}

#[tokio::test]
async fn direct_path_cleanup_checks_the_latest_release_and_local_files() {
    let repo = cleanup_repo();
    let state = tempfile::tempdir().unwrap();
    let client = client_with_idle_limit(&repo, state.path(), 0).await;
    lock(&client, &repo.repo_arg(), "a", false).await;
    let first = lock(&client, &repo.repo_arg(), "b", true).await;
    let path_arg = first["lease"]["workspace"].clone();
    let path = Path::new(path_arg.as_str().unwrap());
    std::fs::write(path.join("first.txt"), "first\n").unwrap();
    let first_saved = unlock(&client, &path_arg, "b").await;
    lock(&client, &path_arg, "c", false).await;
    std::fs::write(path.join("second.txt"), "second\n").unwrap();
    let second_saved = unlock(&client, &path_arg, "c").await;
    integrate(
        &repo,
        &[first_saved["saved_commit"].as_str().unwrap().into()],
        "main",
    );
    let retained = cleanup(&client, &repo.repo_arg(), None).await;
    assert_eq!(retained["retained"], json!([path]));
    assert!(
        retained["retained_reasons"][path.to_str().unwrap()]
            .as_str()
            .unwrap()
            .contains("not integrated")
    );
    integrate(
        &repo,
        &[
            first_saved["saved_commit"].as_str().unwrap().into(),
            second_saved["saved_commit"].as_str().unwrap().into(),
        ],
        "main",
    );
    std::fs::write(path.join("second.txt"), "local edits\n").unwrap();
    let dirty = cleanup(&client, &repo.repo_arg(), None).await;
    assert_eq!(dirty["retained"], json!([path]));
    assert!(
        dirty["retained_reasons"][path.to_str().unwrap()]
            .as_str()
            .unwrap()
            .contains("contents changed")
    );
    std::fs::write(path.join("second.txt"), "second\n").unwrap();
    let removed = cleanup(&client, &repo.repo_arg(), None).await;
    assert_eq!(removed["removed"], json!([path]));
    assert!(!path.exists());
}

#[tokio::test]
async fn direct_path_reacquisition_preserves_active_ownership_and_external_activity() {
    let repo = cleanup_repo();
    let state = tempfile::tempdir().unwrap();
    let client = client_with_idle_limit(&repo, state.path(), 0).await;
    lock(&client, &repo.repo_arg(), "a", false).await;
    let first = lock(&client, &repo.repo_arg(), "b", true).await;
    let path = first["lease"]["workspace"].clone();
    unlock(&client, &path, "b").await;
    let ownership = Ownership::new(state.path().into(), 0).unwrap();
    let active = lock(&client, &path, "c", false).await;
    assert_eq!(active["lease"]["managed"], true);
    assert_eq!(
        active["lease"]["workspace_name"],
        first["lease"]["workspace_name"]
    );
    {
        let mut tx = ownership.try_transaction().unwrap().unwrap();
        let lease = tx.state.leases.last_mut().unwrap();
        lease.external_activity.insert("still-running".into(), 1);
        tx.save().unwrap();
    }
    let before = serde_json::to_value(ownership.snapshot().unwrap()).unwrap();
    let result = cleanup(&client, &repo.repo_arg(), None).await;
    assert!(result["removed"].as_array().unwrap().is_empty());
    assert_eq!(
        serde_json::to_value(ownership.snapshot().unwrap()).unwrap(),
        before
    );
    assert!(Path::new(path.as_str().unwrap()).is_dir());
    let busy = lock(&client, &path, "intruder", false).await;
    assert_eq!(busy["acquired"], false);
    assert_eq!(busy["lease"]["managed"], true);
}

#[tokio::test]
async fn pool_location_alone_does_not_make_a_workspace_managed() {
    let repo = cleanup_repo();
    let state = tempfile::tempdir().unwrap();
    let pool = state.path().join("pool");
    std::fs::create_dir(&pool).unwrap();
    let path = pool.join("manual");
    repo.jj(&[
        "workspace",
        "add",
        "--name=manual",
        "--revision=main",
        path.to_str().unwrap(),
    ]);
    let client = client_with_idle_limit(&repo, state.path(), 0).await;
    let assignment = lock(&client, &json!(path), "owner", false).await;
    assert_eq!(assignment["lease"]["managed"], false);
    assert!(assignment["lease"]["workspace_name"].is_null());
    unlock(&client, &json!(path), "owner").await;
    let result = cleanup(&client, &repo.repo_arg(), None).await;
    assert!(result["removed"].as_array().unwrap().is_empty());
    assert!(result["retained"].as_array().unwrap().is_empty());
    assert!(path.is_dir());
}

#[tokio::test]
async fn allocation_history_does_not_adopt_a_path_reused_by_another_repository() {
    let repo = cleanup_repo();
    let other_repo = cleanup_repo();
    let state = tempfile::tempdir().unwrap();
    let a = client_with_idle_limit(&repo, state.path(), 0).await;
    lock(&a, &repo.repo_arg(), "a", false).await;
    let allocated = lock(&a, &repo.repo_arg(), "b", true).await;
    let path_arg = allocated["lease"]["workspace"].clone();
    let path = Path::new(path_arg.as_str().unwrap());
    unlock(&a, &path_arg, "b").await;
    // A path replacement is not a continuation of the old allocation.
    std::fs::remove_dir_all(path).unwrap();
    other_repo.jj(&[
        "workspace",
        "add",
        "--name=manual",
        "--revision=main",
        path.to_str().unwrap(),
    ]);
    let b = client_with_idle_limit(&other_repo, state.path(), 0).await;
    let manual = lock(&b, &path_arg, "manual", false).await;
    assert_eq!(manual["lease"]["managed"], false);
    assert!(manual["lease"]["workspace_name"].is_null());
    unlock(&b, &path_arg, "manual").await;
    for (client, root) in [(&a, repo.repo_arg()), (&b, other_repo.repo_arg())] {
        let result = cleanup(client, &root, None).await;
        assert!(result["removed"].as_array().unwrap().is_empty());
        assert!(result["retained"].as_array().unwrap().is_empty());
    }
    assert!(path.is_dir());
}

#[tokio::test]
async fn removed_allocation_cannot_promote_or_reuse_a_recreated_unmanaged_workspace() {
    let repo = cleanup_repo();
    let state = tempfile::tempdir().unwrap();
    let client = client_with_idle_limit(&repo, state.path(), 0).await;
    lock(&client, &repo.repo_arg(), "a", false).await;
    let first = lock(&client, &repo.repo_arg(), "b", true).await;
    let path_arg = first["lease"]["workspace"].clone();
    let path = Path::new(path_arg.as_str().unwrap());
    let first_saved = unlock(&client, &path_arg, "b").await;
    lock(&client, &path_arg, "c", false).await;
    let second_saved = unlock(&client, &path_arg, "c").await;
    integrate(
        &repo,
        &[
            first_saved["saved_commit"].as_str().unwrap().into(),
            second_saved["saved_commit"].as_str().unwrap().into(),
        ],
        "main",
    );
    assert_eq!(
        cleanup(&client, &repo.repo_arg(), None).await["removed"],
        json!([path])
    );
    repo.jj(&[
        "workspace",
        "add",
        "--name=manual",
        "--revision=main",
        path.to_str().unwrap(),
    ]);
    let manual = lock(&client, &path_arg, "manual", false).await;
    assert_eq!(manual["lease"]["managed"], false);
    assert!(manual["lease"]["workspace_name"].is_null());
    unlock(&client, &path_arg, "manual").await;
    let parallel = lock(&client, &repo.repo_arg(), "d", true).await;
    assert_ne!(parallel["lease"]["workspace"], path_arg);
    let result = cleanup(&client, &repo.repo_arg(), None).await;
    assert!(result["removed"].as_array().unwrap().is_empty());
    assert!(!result["retained"].as_array().unwrap().contains(&path_arg));
    assert!(path.is_dir());
}

#[tokio::test]
async fn cleanup_requires_integration_and_deletes_verified_files_but_keeps_ignored_data() {
    let repo = cleanup_repo();
    let state = tempfile::tempdir().unwrap();
    let client = client_with_idle_limit(&repo, state.path(), 0).await;
    lock(&client, &repo.repo_arg(), "a", false).await;
    let b = lock(&client, &repo.repo_arg(), "b", true).await;
    let c = lock(&client, &repo.repo_arg(), "c", true).await;
    let b_path = Path::new(b["lease"]["workspace"].as_str().unwrap());
    let c_path = Path::new(c["lease"]["workspace"].as_str().unwrap());
    std::fs::write(b_path.join("b.txt"), "task b\n").unwrap();
    std::fs::write(b_path.join("cache.ignored"), "keep cache").unwrap();
    std::fs::create_dir(c_path.join("nested")).unwrap();
    std::fs::write(c_path.join("nested/binary.dat"), b"\x00\xffsaved\n").unwrap();
    let saved_b = unlock(&client, &b["lease"]["workspace"], "b").await;
    let saved_c = unlock(&client, &c["lease"]["workspace"], "c").await;
    let before = cleanup(&client, &repo.repo_arg(), None).await;
    assert!(before["removed"].as_array().unwrap().is_empty());
    assert!(
        before["retained_reasons"]
            .as_object()
            .unwrap()
            .values()
            .all(|reason| reason.as_str().unwrap().contains("not integrated"))
    );
    integrate(
        &repo,
        &[
            saved_b["saved_commit"].as_str().unwrap().into(),
            saved_c["saved_commit"].as_str().unwrap().into(),
        ],
        "main",
    );
    let result = cleanup(&client, &repo.repo_arg(), None).await;
    assert_eq!(result["removed"], json!([c_path]));
    assert!(!c_path.exists());
    assert!(b_path.join("cache.ignored").is_file());
    assert!(repo.path.is_dir());
    assert!(
        result["retained_reasons"][b_path.to_str().unwrap()]
            .as_str()
            .unwrap()
            .contains("ignored")
    );
    assert_eq!(
        repo.jj(&["file", "show", "-r", "main", "b.txt"]),
        "task b\n"
    );
}

#[tokio::test]
async fn cleanup_keeps_dirty_binary_files_even_after_integration_without_snapshotting() {
    let repo = cleanup_repo();
    let state = tempfile::tempdir().unwrap();
    let client = client_with_idle_limit(&repo, state.path(), 0).await;
    lock(&client, &repo.repo_arg(), "a", false).await;
    let b = lock(&client, &repo.repo_arg(), "b", true).await;
    let path = Path::new(b["lease"]["workspace"].as_str().unwrap());
    std::fs::write(path.join("binary"), [0xff]).unwrap();
    let saved = unlock(&client, &b["lease"]["workspace"], "b").await;
    integrate(
        &repo,
        &[saved["saved_commit"].as_str().unwrap().into()],
        "main",
    );
    std::fs::write(path.join("binary"), [0xfe]).unwrap();
    let operation = repo.jj(&[
        "--ignore-working-copy",
        "op",
        "log",
        "--no-graph",
        "--limit=1",
        "-T",
        "id",
    ]);
    let result = cleanup(&client, &repo.repo_arg(), None).await;
    assert!(result["removed"].as_array().unwrap().is_empty());
    assert!(
        result["retained_reasons"][path.to_str().unwrap()]
            .as_str()
            .unwrap()
            .contains("contents changed")
    );
    assert_eq!(std::fs::read(path.join("binary")).unwrap(), [0xfe]);
    assert_eq!(
        operation,
        repo.jj(&[
            "--ignore-working-copy",
            "op",
            "log",
            "--no-graph",
            "--limit=1",
            "-T",
            "id"
        ])
    );
}

#[tokio::test]
async fn cleanup_checks_every_task_saved_in_a_reused_copy_and_accepts_an_explicit_destination() {
    let repo = cleanup_repo();
    let state = tempfile::tempdir().unwrap();
    let client = client_with_idle_limit(&repo, state.path(), 0).await;
    lock(&client, &repo.repo_arg(), "a", false).await;
    let first = lock(&client, &repo.repo_arg(), "b", true).await;
    let path = Path::new(first["lease"]["workspace"].as_str().unwrap());
    std::fs::write(path.join("first.txt"), "first\n").unwrap();
    let saved_first = unlock(&client, &first["lease"]["workspace"], "b").await;
    let second = lock(&client, &repo.repo_arg(), "b", true).await;
    assert_eq!(first["lease"]["workspace"], second["lease"]["workspace"]);
    std::fs::write(path.join("second.txt"), "second\n").unwrap();
    let saved_second = unlock(&client, &second["lease"]["workspace"], "b").await;
    integrate(
        &repo,
        &[saved_second["saved_commit"].as_str().unwrap().into()],
        "release",
    );
    assert!(
        cleanup(&client, &repo.repo_arg(), None).await["removed"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(
        cleanup(&client, &repo.repo_arg(), Some("release")).await["removed"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    integrate(
        &repo,
        &[
            saved_first["saved_commit"].as_str().unwrap().into(),
            saved_second["saved_commit"].as_str().unwrap().into(),
        ],
        "release",
    );
    assert_eq!(
        cleanup(&client, &repo.repo_arg(), Some("release")).await["removed"],
        json!([path])
    );
    assert!(!path.exists());
}

#[tokio::test]
async fn cleanup_keeps_unintegrated_parallel_task_heads() {
    let repo = cleanup_repo();
    let state = tempfile::tempdir().unwrap();
    let client = client_with_idle_limit(&repo, state.path(), 0).await;
    lock(&client, &repo.repo_arg(), "a", false).await;
    let b = lock(&client, &repo.repo_arg(), "b", true).await;
    let path = Path::new(b["lease"]["workspace"].as_str().unwrap());
    std::fs::write(path.join("one"), "one\n").unwrap();
    std::fs::write(path.join("two"), "two\n").unwrap();
    let split = request(
        &client,
        "split",
        json!({"repo": path, "paths": ["one"], "message": "one", "parallel": true}),
        metadata("b"),
    )
    .await;
    assert_ne!(split.is_error, Some(true), "{split:?}");
    let saved = unlock(&client, &b["lease"]["workspace"], "b").await;
    let heads: Vec<String> = saved["saved_heads"]
        .as_array()
        .unwrap()
        .iter()
        .map(|head| head.as_str().unwrap().into())
        .collect();
    assert_eq!(heads.len(), 2);
    integrate(
        &repo,
        &[saved["saved_commit"].as_str().unwrap().into()],
        "main",
    );
    assert!(
        cleanup(&client, &repo.repo_arg(), None).await["removed"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    integrate(&repo, &heads, "main");
    assert_eq!(
        cleanup(&client, &repo.repo_arg(), None).await["removed"],
        json!([path])
    );
}

async fn process_client(
    repo: &TestRepo,
    state: &Path,
    adapter: Option<&Path>,
) -> (Client, tokio::process::Child) {
    use std::process::Stdio;
    let binary = env!("CARGO_BIN_EXE_jujutsu-mcp");
    let mut command = if let Some(identity) = adapter {
        let mut command = tokio::process::Command::new("python3");
        command
            .arg(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/adapters/stdio_identity.py"
            ))
            .arg("--identity-file")
            .arg(identity)
            .arg("--")
            .arg(binary);
        command
    } else {
        tokio::process::Command::new(binary)
    };
    let mut child = command
        .arg("--ownership-state")
        .arg(state)
        .env("JJ_CONFIG", &repo.config)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let input = child.stdin.take().unwrap();
    let output = child.stdout.take().unwrap();
    let client = tokio::time::timeout(common::CALL_TIMEOUT, ().serve((output, input)))
        .await
        .unwrap()
        .unwrap();
    (client, child)
}

#[tokio::test]
async fn separate_processes_preserve_assignment_after_restart() {
    let repo = TestRepo::new();
    let state = tempfile::tempdir().unwrap();
    let (a, mut process_a) = process_client(&repo, state.path(), None).await;
    let (b, mut process_b) = process_client(&repo, state.path(), None).await;
    let arg = repo.repo_arg();
    let (one, two) = tokio::join!(lock(&a, &arg, "a", false), lock(&b, &arg, "b", false));
    assert_ne!(one["acquired"], two["acquired"]);
    let owner = if one["acquired"] == true { "a" } else { "b" };
    let generation =
        if one["acquired"] == true { &one } else { &two }["lease"]["generation"].clone();
    process_a.kill().await.unwrap();
    process_b.kill().await.unwrap();
    let (resumed, _process) = process_client(&repo, state.path(), None).await;
    assert_eq!(
        lock(&resumed, &arg, owner, false).await["lease"]["generation"],
        generation
    );
    unlock(&resumed, &arg, owner).await;
}

#[cfg(unix)]
#[tokio::test]
async fn stdio_adapter_forwards_acp_identity_without_model_tokens() {
    let repo = TestRepo::new();
    let state = tempfile::tempdir().unwrap();
    let identity_file = repo.temp_root().join("identity.json");
    std::fs::write(&identity_file, json!({"provider": "claude", "session_id": "acp-session", "executor_id": "child-7", "source": "acp", "session_name": "Починка авторизации"}).to_string()).unwrap();
    let (client, _process) = process_client(&repo, state.path(), Some(&identity_file)).await;
    let diagnostic = request(
        &client,
        "workspace_identity",
        json!({}),
        RequestMetaObject::new(),
    )
    .await;
    let diagnostic = diagnostic.structured_content.unwrap();
    assert_eq!(diagnostic["provider"], "claude");
    assert_eq!(diagnostic["executor_id"], "child-7");
    assert_eq!(diagnostic["source"], "acp");
    assert_eq!(diagnostic["session_name"], "Починка авторизации");
    let renamed_meta: RequestMetaObject = json!({"jujutsu-mcp/identity": {"provider": "claude", "session_id": "acp-session", "executor_id": "child-7", "source": "acp", "session_name": "Новое название"}}).as_object().unwrap().clone().into();
    let renamed = request(&client, "workspace_identity", json!({}), renamed_meta).await;
    assert_ne!(renamed.is_error, Some(true), "{renamed:?}");
    assert_eq!(
        renamed.structured_content.unwrap()["session_name"],
        "Новое название"
    );
    let acquire = request(
        &client,
        "workspace_lock",
        json!({"repo": repo.repo_arg(), "task": "adapter", "expected_seconds": 60}),
        RequestMetaObject::new(),
    )
    .await;
    assert_ne!(acquire.is_error, Some(true), "{acquire:?}");
    let write = request(
        &client,
        "describe",
        json!({"repo": repo.repo_arg(), "message": "adapter writes"}),
        RequestMetaObject::new(),
    )
    .await;
    assert_ne!(write.is_error, Some(true), "{write:?}");
    let finish = request(
        &client,
        "workspace_unlock",
        json!({"repo": repo.repo_arg(), "message": "adapter saved", "checks_complete": true}),
        RequestMetaObject::new(),
    )
    .await;
    assert_ne!(finish.is_error, Some(true), "{finish:?}");
}

#[tokio::test]
async fn cancelled_wait_does_not_create_assignment() {
    use rmcp::model::{ClientRequest, Request};
    use rmcp::service::PeerRequestOptions;
    let repo = TestRepo::new();
    let state = tempfile::tempdir().unwrap();
    let client = client(&repo, state.path()).await;
    lock(&client, &repo.repo_arg(), "owner", false).await;
    let mut params = CallToolRequestParams::new("workspace_lock").with_arguments(json!({"repo": repo.repo_arg(), "task": "waiter", "expected_seconds": 30, "wait_seconds": 30}).as_object().unwrap().clone());
    params.set_meta(metadata("waiter"));
    let handle = client
        .send_cancellable_request(
            ClientRequest::CallToolRequest(Request::new(params)),
            PeerRequestOptions::no_options(),
        )
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    handle.cancel(None).await.unwrap();
    // The reservation remains with the owner; completion of cancellation must
    // not allocate a new workspace or turn expected duration into expiration.
    let status = request(
        &client,
        "workspace_lock_status",
        json!({"repo": repo.repo_arg()}),
        metadata("owner"),
    )
    .await;
    assert_eq!(
        status.structured_content.unwrap()["lease"]["owner"]["session_id"],
        "owner"
    );
    assert_eq!(
        Ownership::new(state.path().into(), 1)
            .unwrap()
            .snapshot()
            .unwrap()
            .leases
            .len(),
        1
    );
}

#[tokio::test]
async fn generation_is_captured_before_waiting_for_backend_writes() {
    let repo = TestRepo::new();
    let state = tempfile::tempdir().unwrap();
    let client = client(&repo, state.path()).await;
    lock(&client, &repo.repo_arg(), "owner", false).await;
    let ownership = Ownership::new(state.path().into(), 1).unwrap();
    let mut transaction = ownership.try_transaction().unwrap().unwrap();
    let pending = request(
        &client,
        "describe",
        json!({"repo": repo.repo_arg(), "message": "must not run"}),
        metadata("owner"),
    );
    let replace_generation = async {
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        // A release and reacquisition by the same executor while the operation
        // waited: the absence of adapter metadata must not permit this request.
        transaction.state.leases[0].generation += 1;
        transaction.state.generation += 1;
        transaction.save().unwrap();
        drop(transaction);
    };
    let (result, ()) = tokio::join!(pending, replace_generation);
    assert_eq!(result.is_error, Some(true), "{result:?}");
    assert!(text_of(&result).contains("stale"));
    assert!(!repo.description("@").contains("must not run"));
}

#[tokio::test]
async fn concurrent_completion_releases_exactly_once() {
    let repo = TestRepo::new();
    let state = tempfile::tempdir().unwrap();
    let a = client(&repo, state.path()).await;
    let b = client(&repo, state.path()).await;
    lock(&a, &repo.repo_arg(), "owner", false).await;
    repo.write("final", "saved\n");
    let args = json!({"repo": repo.repo_arg(), "message": "final", "checks_complete": true});
    let (one, two) = tokio::join!(
        request(&a, "workspace_unlock", args.clone(), metadata("owner")),
        request(&b, "workspace_unlock", args, metadata("owner"))
    );
    assert_ne!(
        one.is_error == Some(true),
        two.is_error == Some(true),
        "{one:?} {two:?}"
    );
    assert_eq!(
        Ownership::new(state.path().into(), 1)
            .unwrap()
            .snapshot()
            .unwrap()
            .leases[0]
            .state,
        "released"
    );
    assert_eq!(repo.jj(&["file", "show", "-r", "@-", "final"]), "saved\n");
}

#[tokio::test]
async fn interrupted_mutation_requires_resume_and_recovers_current_change() {
    let repo = TestRepo::new();
    let state = tempfile::tempdir().unwrap();
    let client = client(&repo, state.path()).await;
    lock(&client, &repo.repo_arg(), "owner", false).await;
    repo.write("before-crash", "retained\n");
    repo.jj(&["new", "-m", "interrupted child"]);
    let ownership = Ownership::new(state.path().into(), 1).unwrap();
    {
        let mut tx = ownership.try_transaction().unwrap().unwrap();
        tx.state.leases[0].state = "mutating".into();
        tx.save().unwrap();
    }
    let denied = request(
        &client,
        "describe",
        json!({"repo": repo.repo_arg(), "message": "resume required"}),
        metadata("owner"),
    )
    .await;
    assert_eq!(denied.is_error, Some(true));
    let resumed = request(
        &client,
        "workspace_lock",
        json!({"repo": repo.repo_arg(), "task": "owner", "expected_seconds": 60, "resume": true}),
        metadata("owner"),
    )
    .await;
    assert_ne!(resumed.is_error, Some(true), "{resumed:?}");
    unlock(&client, &repo.repo_arg(), "owner").await;
    assert_eq!(
        repo.jj(&["file", "show", "-r", "@-", "before-crash"]),
        "retained\n"
    );
}

#[test]
fn session_name_is_optional_and_does_not_change_the_owner_key() {
    let original = caller(&metadata("session")).unwrap();
    assert_eq!(original.session_name, None);
    let mut named = metadata("session");
    named.insert(
        "jujutsu-mcp/session-name".into(),
        json!("  Починка авторизации  "),
    );
    let named = caller(&named).unwrap();
    assert_eq!(named.session_name.as_deref(), Some("Починка авторизации"));
    assert!(original.same_executor(&named));
    assert_eq!(original.aliases, named.aliases);

    for invalid in [
        json!(" "),
        json!("line\nbreak"),
        json!("x".repeat(513)),
        json!(42),
    ] {
        let mut meta = metadata("session");
        meta.insert("jujutsu-mcp/session-name".into(), invalid);
        assert!(caller(&meta).is_err());
    }
}

#[tokio::test]
async fn session_name_is_persisted_and_can_change_without_reacquiring() {
    let repo = TestRepo::new();
    let state = tempfile::tempdir().unwrap();
    let a = client(&repo, state.path()).await;
    let named = |title: &str| {
        let mut meta = metadata("owner");
        meta.insert("jujutsu-mcp/session-name".into(), json!(title));
        meta
    };
    let arguments = json!({"repo": repo.repo_arg(), "task": "task", "expected_seconds": 60});
    let acquired = request(
        &a,
        "workspace_lock",
        arguments.clone(),
        named("Original title"),
    )
    .await;
    assert_ne!(acquired.is_error, Some(true), "{acquired:?}");
    let acquired = acquired.structured_content.unwrap();
    let renamed = request(&a, "workspace_lock", arguments, named("Renamed session")).await;
    assert_ne!(renamed.is_error, Some(true), "{renamed:?}");
    let renamed = renamed.structured_content.unwrap();
    assert_eq!(
        acquired["lease"]["generation"],
        renamed["lease"]["generation"]
    );
    assert_eq!(renamed["lease"]["owner"]["session_name"], "Renamed session");

    let write = request(
        &a,
        "describe",
        json!({"repo": repo.repo_arg(), "message": "same owner"}),
        named("Latest session title"),
    )
    .await;
    assert_ne!(write.is_error, Some(true), "{write:?}");
    let b = client(&repo, state.path()).await;
    let status = request(
        &b,
        "workspace_lock_status",
        json!({"repo": repo.repo_arg()}),
        metadata("observer"),
    )
    .await;
    assert_eq!(
        status.structured_content.unwrap()["lease"]["owner"]["session_name"],
        "Latest session title"
    );
    let released = unlock(&a, &repo.repo_arg(), "owner").await;
    assert_eq!(released["owner"]["session_name"], "Latest session title");
}

#[tokio::test]
async fn cleanup_accepts_a_squashed_result_with_an_identical_tree() {
    let repo = cleanup_repo();
    let state = tempfile::tempdir().unwrap();
    let client = client_with_idle_limit(&repo, state.path(), 0).await;
    lock(&client, &repo.repo_arg(), "a", false).await;
    let b = lock(&client, &repo.repo_arg(), "b", true).await;
    let path = Path::new(b["lease"]["workspace"].as_str().unwrap());
    std::fs::write(path.join("task.txt"), "squashed work\n").unwrap();
    let saved = unlock(&client, &b["lease"]["workspace"], "b").await;
    repo.jj(&[
        "--ignore-working-copy",
        "squash",
        &format!("--from={}", saved["saved_commit"].as_str().unwrap()),
        "--into=main",
        "--message=integrated squash",
    ]);
    assert_eq!(
        cleanup(&client, &repo.repo_arg(), None).await["removed"],
        json!([path])
    );
    assert_eq!(
        repo.jj(&[
            "--ignore-working-copy",
            "file",
            "show",
            "-r",
            "main",
            "task.txt"
        ]),
        "squashed work\n"
    );
}

#[tokio::test]
async fn cleanup_honors_idle_limit_after_integration() {
    let repo = cleanup_repo();
    let state = tempfile::tempdir().unwrap();
    let client = client(&repo, state.path()).await;
    lock(&client, &repo.repo_arg(), "a", false).await;
    let b = lock(&client, &repo.repo_arg(), "b", true).await;
    let c = lock(&client, &repo.repo_arg(), "c", true).await;
    let saved_b = unlock(&client, &b["lease"]["workspace"], "b").await;
    let saved_c = unlock(&client, &c["lease"]["workspace"], "c").await;
    integrate(
        &repo,
        &[
            saved_b["saved_commit"].as_str().unwrap().into(),
            saved_c["saved_commit"].as_str().unwrap().into(),
        ],
        "main",
    );
    let result = cleanup(&client, &repo.repo_arg(), None).await;
    assert_eq!(result["removed"].as_array().unwrap().len(), 1);
    assert_eq!(result["retained"].as_array().unwrap().len(), 1);
    assert!(repo.path.is_dir());
}

#[tokio::test]
async fn cleanup_requires_a_named_destination_and_complete_saved_heads() {
    let repo = cleanup_repo();
    let state = tempfile::tempdir().unwrap();
    let client = client_with_idle_limit(&repo, state.path(), 0).await;
    lock(&client, &repo.repo_arg(), "a", false).await;
    let b = lock(&client, &repo.repo_arg(), "b", true).await;
    let path = Path::new(b["lease"]["workspace"].as_str().unwrap());
    std::fs::write(path.join("task"), "retained\n").unwrap();
    let saved = unlock(&client, &b["lease"]["workspace"], "b").await;
    let result = cleanup(&client, &repo.repo_arg(), saved["saved_commit"].as_str()).await;
    assert!(result["removed"].as_array().unwrap().is_empty());
    assert!(result["integration_targets"].as_array().unwrap().is_empty());
    integrate(
        &repo,
        &[saved["saved_commit"].as_str().unwrap().into()],
        "main",
    );
    {
        let ownership = Ownership::new(state.path().into(), 0).unwrap();
        let mut tx = ownership.try_transaction().unwrap().unwrap();
        tx.state.leases.last_mut().unwrap().saved_heads = None;
        tx.save().unwrap();
    }
    let result = cleanup(&client, &repo.repo_arg(), None).await;
    assert!(result["removed"].as_array().unwrap().is_empty());
    assert!(
        result["retained_reasons"][path.to_str().unwrap()]
            .as_str()
            .unwrap()
            .contains("incomplete")
    );
    assert!(path.join("task").is_file());
}

#[tokio::test]
async fn cleanup_preserves_empty_unknown_directories_and_local_jj_metadata() {
    let repo = cleanup_repo();
    let state = tempfile::tempdir().unwrap();
    let client = client_with_idle_limit(&repo, state.path(), 0).await;
    lock(&client, &repo.repo_arg(), "a", false).await;
    let b = lock(&client, &repo.repo_arg(), "b", true).await;
    let path = Path::new(b["lease"]["workspace"].as_str().unwrap());
    std::fs::write(path.join("task"), "integrated\n").unwrap();
    let saved = unlock(&client, &b["lease"]["workspace"], "b").await;
    integrate(
        &repo,
        &[saved["saved_commit"].as_str().unwrap().into()],
        "main",
    );
    std::fs::create_dir(path.join("build-cache")).unwrap();
    assert!(
        cleanup(&client, &repo.repo_arg(), None).await["removed"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(path.join("build-cache").is_dir());
    std::fs::remove_dir(path.join("build-cache")).unwrap();
    std::fs::write(path.join(".jj/local-notes"), "keep notes").unwrap();
    let result = cleanup(&client, &repo.repo_arg(), None).await;
    assert!(result["removed"].as_array().unwrap().is_empty());
    assert!(
        result["retained_reasons"][path.to_str().unwrap()]
            .as_str()
            .unwrap()
            .contains("metadata")
    );
    assert!(path.join("task").is_file());
    assert!(path.join(".jj/local-notes").is_file());
}

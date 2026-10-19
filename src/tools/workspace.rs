//! Ownership tools and the additional guard held across each server mutation.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use rmcp::handler::server::wrapper::{Json, Parameters};
use rmcp::model::RequestMetaObject;
use rmcp::service::RequestContext;
use rmcp::{RoleServer, tool, tool_router};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::jj::{JjOutput, WriteGuard};
use crate::ownership::{Caller, Lease, Ownership, Transaction, caller, now, refused, repository};
use crate::repo::RepoPath;
use crate::server::JjServer;
use crate::tools::remote::Progress;
use crate::tools::{ToolError, non_empty};

#[derive(Debug, Deserialize, JsonSchema)]
pub struct WorkspaceLockParams {
    /// Absolute path inside the requested workspace or shared repository.
    pub repo: String,
    /// Human-readable task label.
    pub task: String,
    /// Estimated duration in seconds (1..86400). Never an eviction timeout.
    pub expected_seconds: u64,
    /// Wait at most this many seconds for the requested workspace (0..30).
    #[serde(default)]
    pub wait_seconds: u64,
    /// Reuse an idle managed copy, or allocate one if the requested workspace is occupied.
    #[serde(default)]
    pub parallel: bool,
    /// Explicitly resume a preparing/releasing lease after inspecting retained work.
    #[serde(default)]
    pub resume: bool,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct WorkspaceParams {
    /// Absolute path inside the specific workspace; required even when the caller owns several.
    pub repo: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct WorkspaceUnlockParams {
    /// Absolute path inside the assigned workspace.
    pub repo: String,
    /// Confirm all external edits, builds and tests have stopped and required checks passed.
    pub checks_complete: bool,
    /// Final description saved with the task's working-copy result.
    pub message: String,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct LockOutput {
    pub acquired: bool,
    pub lease: Option<Lease>,
    /// Re-read/revalidate this saved base and live files before editing after acquisition.
    pub revalidate: bool,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct StatusOutput {
    pub lease: Option<Lease>,
    pub elapsed_seconds: u64,
    pub overdue: bool,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct CleanupParams {
    /// Absolute path inside the shared repository whose extra idle copies may be removed.
    pub repo: String,
    /// Revset containing the integration destination(s), e.g. main or release.
    /// Defaults to local main, then trunk(). Must resolve to named bookmarks/tags,
    /// not workspace-only heads.
    #[serde(default)]
    pub integrated_into: Option<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct CleanupOutput {
    pub removed: Vec<PathBuf>,
    pub retained: Vec<PathBuf>,
    pub retained_reasons: std::collections::BTreeMap<PathBuf, String>,
    /// Resolved, independently named integration commits used for verification.
    pub integration_targets: Vec<String>,
}

#[derive(Debug)]
pub(crate) struct MutationGuard {
    _queue: WriteGuard,
    pub transaction: Option<Transaction>,
    pub index: Option<usize>,
}

impl MutationGuard {
    fn other_leases(&self) -> impl Iterator<Item = &Lease> {
        self.transaction.iter().flat_map(|transaction| {
            transaction
                .state
                .leases
                .iter()
                .enumerate()
                .filter(|(index, lease)| Some(*index) != self.index && lease.is_reserved())
                .map(|(_, lease)| lease)
        })
    }

    fn foreign_leases(&self) -> impl Iterator<Item = &Lease> {
        self.other_leases().filter(|other| {
            self.transaction
                .as_ref()
                .zip(self.index)
                .is_some_and(|(transaction, index)| {
                    other.repository == transaction.state.leases[index].repository
                })
        })
    }

    pub(crate) fn require_exclusive(
        &self,
        operation: &str,
        entire_backend: bool,
    ) -> Result<(), ToolError> {
        let busy = if entire_backend {
            self.other_leases().next().is_some()
        } else {
            self.foreign_leases().next().is_some()
        };
        if busy {
            return Err(refused(format!(
                "{operation} can affect other workspaces; wait for their active leases to be released"
            )).into());
        }
        Ok(())
    }

    fn protected_changes(&self) -> String {
        let changes = self
            .foreign_leases()
            .flat_map(|lease| {
                lease
                    .owned_changes
                    .iter()
                    .chain(std::iter::once(&lease.change_id))
            })
            .filter(|id| !id.is_empty())
            .map(|id| format!("present({id})"))
            .collect::<Vec<_>>();
        if changes.is_empty() {
            "none()".into()
        } else {
            changes.join(" | ")
        }
    }

    pub fn scoped_args(&self, args: &[String]) -> Result<Vec<String>, ToolError> {
        if self.index.is_none() {
            return Ok(args.to_vec());
        }
        if self.foreign_leases().any(Lease::has_pending_transition) {
            return Err(refused("another workspace has an interrupted transition; its owner must recover and release it before repository mutations").into());
        }
        if args.first().is_some_and(|command| command == "undo") {
            self.require_exclusive("undo", false)?;
            return Ok(args.to_vec());
        }
        // Ancestors are immutable too: rewriting one could rebase active work.
        // Released leases confer no protection on their former task changes.
        let expression = format!("builtin_immutable_heads() | ({})", self.protected_changes());
        let mut scoped = vec![format!(
            "--config=revset-aliases.\"immutable_heads()\"={}",
            serde_json::to_string(&expression)?
        )];
        scoped.extend_from_slice(args);
        Ok(scoped)
    }
}

impl JjServer {
    fn ownership_required(&self) -> Result<&Ownership, ToolError> {
        self.ownership.as_ref().ok_or_else(|| refused("ownership mode is disabled; start this backend with --ownership-state ABSOLUTE_DIRECTORY").into())
    }

    pub(crate) async fn read_output(
        &self,
        repo: &RepoPath,
        args: &[String],
    ) -> Result<JjOutput, ToolError> {
        let mut read = Vec::new();
        if self.ownership.is_some() {
            repository(repo)?;
            read.push("--ignore-working-copy".into());
            read.push("--at-operation=@".into());
        }
        read.extend_from_slice(args);
        Ok(self.runner.run(repo, &read).await?)
    }

    pub(crate) async fn snapshot_value(
        &self,
        repo: &RepoPath,
        template: &str,
    ) -> Result<String, ToolError> {
        let output = self
            .read_output(
                repo,
                &[
                    "log".into(),
                    "--no-graph".into(),
                    "--revisions=@".into(),
                    format!("--template={template}"),
                ],
            )
            .await?;
        Ok(output.stdout.trim().to_owned())
    }

    /// Capture the generation before waiting; validate it again while holding
    /// the shared backend lock. Unlock cannot pass an in-flight mutation.
    pub(crate) async fn mutation_guard(
        &self,
        repo: &RepoPath,
        context: &RequestContext<RoleServer>,
    ) -> Result<MutationGuard, ToolError> {
        let initial = if let Some(ownership) = &self.ownership {
            let owner = caller(&context.meta)?;
            let (_, workspace) = repository(repo)?;
            let state = ownership.snapshot()?;
            let generation = metadata_generation(&context.meta)?;
            let index = state.require(&workspace, &owner, generation)?;
            let lease = &state.leases[index];
            if lease.state != "active" && lease.state != "mutating" {
                return Err(refused("lease needs recovery: inspect the workspace and call workspace_lock with resume=true").into());
            }
            Some((owner, workspace, lease.generation))
        } else {
            None
        };
        let queue = tokio::select! {
            biased;
            () = context.ct.cancelled() => return Err(ToolError::Cancelled),
            queue = self.write_queue.lock(repo) => queue,
        };
        let (transaction, index) = if let (Some(ownership), Some((owner, workspace, generation))) =
            (&self.ownership, initial)
        {
            let mut transaction = ownership.transaction(context).await?;
            let index = transaction.require(&workspace, &owner, Some(generation))?;
            if transaction.state.leases[index].state != "active" {
                return Err(refused("lease is being released or requires recovery").into());
            }
            if owner.session_name.is_some() {
                transaction.state.leases[index].owner.session_name = owner.session_name;
            }
            transaction.state.leases[index].last_activity = now();
            transaction.save()?;
            (Some(transaction), Some(index))
        } else {
            (None, None)
        };
        Ok(MutationGuard {
            _queue: queue,
            transaction,
            index,
        })
    }

    async fn commit_changes(&self, repo: &RepoPath) -> Result<BTreeMap<String, String>, ToolError> {
        let output = self
            .read_output(
                repo,
                &[
                    "log".into(),
                    "--no-graph".into(),
                    "--revisions=all()".into(),
                    "--template=commit_id ++ \" \" ++ change_id ++ \"\\n\"".into(),
                ],
            )
            .await?;
        output
            .stdout
            .lines()
            .map(|line| {
                line.split_once(' ')
                    .map(|(commit, change)| (commit.to_owned(), change.to_owned()))
                    .ok_or_else(|| refused("invalid commit inventory returned by jj").into())
            })
            .collect()
    }

    /// Mutations reserve all created, rewritten and checked-out changes until unlock.
    pub(crate) async fn run_owned(
        &self,
        repo: &RepoPath,
        args: &[String],
        context: &RequestContext<RoleServer>,
        guard: &mut MutationGuard,
    ) -> Result<JjOutput, ToolError> {
        self.run_owned_watched(repo, args, context, guard, None)
            .await
    }

    pub(crate) async fn run_owned_watched(
        &self,
        repo: &RepoPath,
        args: &[String],
        context: &RequestContext<RoleServer>,
        guard: &mut MutationGuard,
        progress: Option<Progress<'_>>,
    ) -> Result<JjOutput, ToolError> {
        let scoped = guard.scoped_args(args)?;
        let before = if guard.index.is_some() {
            self.commit_changes(repo).await?
        } else {
            BTreeMap::new()
        };
        let mut bookmark_changes = Vec::new();
        if guard.index.is_some() {
            let protected = guard.protected_changes();
            // Checkout and bookmark changes can target immutable commits without
            // rewriting them, so immutability alone does not constrain these.
            let mut targets = Vec::new();
            if matches!(args.first().map(String::as_str), Some("edit" | "new")) {
                targets = args.iter().position(|arg| arg == "--").map_or_else(
                    || vec!["@".to_owned()],
                    |offset| args[offset + 1..].to_vec(),
                );
            } else if args.starts_with(&["bookmark".into(), "set".into()]) {
                targets.push(
                    args.iter()
                        .find_map(|arg| arg.strip_prefix("--revision="))
                        .unwrap_or("@")
                        .to_owned(),
                );
                if let Some(name) = args.last() {
                    targets.push(format!("bookmarks(exact:{})", serde_json::to_string(name)?));
                }
            }
            for target in targets {
                let output = self
                    .read_output(
                        repo,
                        &[
                            "log".into(),
                            "--no-graph".into(),
                            format!("--revisions=({target}) & ({protected})"),
                            "--template=change_id".into(),
                        ],
                    )
                    .await?;
                if !output.stdout.is_empty() {
                    return Err(refused("checkout/bookmark target belongs to another workspace's active lease; wait for its owner to unlock").into());
                }
            }
            if args.starts_with(&["bookmark".into(), "set".into()]) {
                let target = args
                    .iter()
                    .find_map(|arg| arg.strip_prefix("--revision="))
                    .unwrap_or("@");
                bookmark_changes = self
                    .read_output(
                        repo,
                        &[
                            "log".into(),
                            "--no-graph".into(),
                            format!("--revisions={target}"),
                            "--template=change_id ++ \"\\n\"".into(),
                        ],
                    )
                    .await?
                    .stdout
                    .lines()
                    .map(str::to_owned)
                    .collect();
            }
            if args.first().is_some_and(|command| command == "rebase") {
                for destination in args.iter().filter_map(|arg| {
                    ["--onto=", "--insert-after=", "--insert-before="]
                        .iter()
                        .find_map(|flag| arg.strip_prefix(flag))
                }) {
                    let output = self
                        .read_output(
                            repo,
                            &[
                                "log".into(),
                                "--no-graph".into(),
                                format!("--revisions=({destination}) & ({protected})"),
                                "--template=change_id".into(),
                            ],
                        )
                        .await?;
                    if !output.stdout.is_empty() {
                        return Err(refused(
                            "cannot make this task depend on another executor's active changes",
                        )
                        .into());
                    }
                }
            }
        }
        let mut previous_state = None;
        if let (Some(transaction), Some(index)) = (&mut guard.transaction, guard.index) {
            previous_state = Some(transaction.state.leases[index].state.clone());
            for change in bookmark_changes {
                if !transaction.state.leases[index]
                    .owned_changes
                    .contains(&change)
                {
                    transaction.state.leases[index].owned_changes.push(change);
                }
            }
            transaction.state.leases[index].state = "mutating".into();
            transaction.state.leases[index].mutation_snapshot = Some(before.clone());
            transaction.save()?;
        }
        let result = self.run_watched(repo, &scoped, context, progress).await;
        // A failed/cancelled jj can have committed an operation. Retain any new
        // changes before returning its error; a crash leaves the active lease.
        if let (Some(transaction), Some(index)) = (&mut guard.transaction, guard.index) {
            let after = self.commit_changes(repo).await?;
            let lease = &mut transaction.state.leases[index];
            lease.record_mutation(&before, &after);
            lease.change_id = self.snapshot_value(repo, "change_id").await?;
            if !lease.owned_changes.contains(&lease.change_id) {
                lease.owned_changes.push(lease.change_id.clone());
            }
            lease.mutation_snapshot = None;
            lease.last_activity = now();
            if let Some(previous_state) = previous_state {
                lease.state = previous_state;
            }
            transaction.save()?;
        }
        result
    }

    async fn cleanup_integration(
        &self,
        repo: &RepoPath,
        leases: &[Lease],
        workspace: &std::path::Path,
        targets: &[String],
    ) -> Result<Option<String>, ToolError> {
        for lease in leases
            .iter()
            .filter(|lease| lease.workspace == workspace && lease.state == "released")
        {
            let Some(heads) = lease
                .saved_heads
                .as_ref()
                .filter(|heads| !heads.is_empty() && lease.saved_commit.is_some())
            else {
                return Ok(Some("saved task result is incomplete".into()));
            };
            for head in heads {
                let missing = self
                    .read_output(
                        repo,
                        &[
                            "log".into(),
                            "--no-graph".into(),
                            format!("--revisions={head} ~ ::({})", targets.join(" | ")),
                            "--template=commit_id".into(),
                        ],
                    )
                    .await;
                let missing = match missing {
                    Ok(output) => output,
                    Err(_) => {
                        return Ok(Some(
                            "saved result is unavailable; integration cannot be verified".into(),
                        ));
                    }
                };
                if missing.stdout.is_empty() {
                    continue;
                }
                // Exact tree equality also proves all saved content is present
                // after a squash, without guessing from commit descriptions/IDs.
                let mut equivalent = false;
                for target in targets {
                    let diff = self
                        .read_output(
                            repo,
                            &[
                                "diff".into(),
                                format!("--from={head}"),
                                format!("--to={target}"),
                                "--summary".into(),
                            ],
                        )
                        .await?;
                    if diff.stdout.is_empty() {
                        equivalent = true;
                        break;
                    }
                }
                if !equivalent {
                    return Ok(Some(format!(
                        "task {:?} has a saved head not integrated into the selected destination",
                        lease.task
                    )));
                }
            }
        }
        Ok(None)
    }

    async fn acquire(
        &self,
        params: &WorkspaceLockParams,
        context: &RequestContext<RoleServer>,
    ) -> Result<LockOutput, ToolError> {
        let ownership = self.ownership_required()?;
        let owner = caller(&context.meta)?;
        let repo = RepoPath::new(&params.repo)?;
        let (shared, requested) = repository(&repo)?;
        // A busy result needs only the last durable reservation, not the write
        // lock held by a potentially long-running command of the current owner.
        if !params.parallel {
            let observed = ownership.snapshot()?;
            if let Some(index) = observed.active(&requested) {
                let lease = &observed.leases[index];
                if !lease.owner.same_executor(&owner) {
                    return Ok(LockOutput {
                        acquired: false,
                        lease: Some(lease.clone()),
                        revalidate: false,
                    });
                }
            }
        }
        let mut transaction = ownership.transaction(context).await?;
        let pool = ownership.directory.join("pool");
        // Retrying via the original repository path returns the parallel assignment.
        if let Some(index) = transaction.state.leases.iter().rposition(|lease| {
            lease.repository == shared
                && lease.owner.same_executor(&owner)
                && lease.state != "released"
                && lease.state != "removed"
        }) {
            let lease = &transaction.state.leases[index];
            if lease.workspace != requested && !params.parallel {
                return Err(refused(format!(
                    "caller already owns {}; use that workspace selector",
                    lease.workspace.display()
                ))
                .into());
            }
            if lease.state != "active" {
                if !params.resume {
                    return Err(refused("interrupted acquisition/release retained its reservation; inspect work and retry workspace_lock with resume=true").into());
                }
                let assigned = RepoPath::new(&lease.workspace)?;
                // Never repeat a possibly completed jj new after a crash.
                let change = self.snapshot_value(&assigned, "change_id").await?;
                let after = self.commit_changes(&assigned).await?;
                let lease = &mut transaction.state.leases[index];
                if let Some(before) = lease.mutation_snapshot.take() {
                    lease.record_mutation(&before, &after);
                }
                if !lease.owned_changes.contains(&change) {
                    lease.owned_changes.push(change.clone());
                }
                lease.change_id = change;
                lease.state = "active".into();
                transaction.save()?;
            }
            if owner.session_name.is_some()
                && owner.session_name != transaction.state.leases[index].owner.session_name
            {
                transaction.state.leases[index].owner.session_name = owner.session_name.clone();
                transaction.save()?;
            }
            return Ok(LockOutput {
                acquired: true,
                lease: Some(transaction.state.leases[index].clone()),
                revalidate: true,
            });
        }
        if transaction
            .state
            .leases
            .iter()
            .any(|lease| lease.repository == shared && lease.has_pending_transition())
        {
            return Err(refused("another workspace has an interrupted transition; its owner must resume it before acquiring new work").into());
        }
        let occupied = transaction.active(&requested);
        let mut workspace = requested.clone();
        let previous = transaction
            .state
            .leases
            .iter()
            .rev()
            .find(|lease| lease.workspace == requested);
        let mut workspace_name = previous
            .filter(|lease| {
                lease.repository == shared
                    && lease.managed
                    && lease.state != "removed"
                    && requested.parent() == Some(pool.as_path())
            })
            .and_then(|lease| lease.workspace_name.clone());
        let mut managed = workspace_name.is_some();
        let base;
        if let Some(index) = occupied {
            if !params.parallel {
                return Ok(LockOutput {
                    acquired: false,
                    lease: Some(transaction.state.leases[index].clone()),
                    revalidate: false,
                });
            }
            base = transaction.state.leases[index].base.clone();
            let mut seen = std::collections::HashSet::new();
            if let Some(idle) = transaction.state.leases.iter().rev().find(|lease| {
                seen.insert(lease.workspace.clone())
                    && lease.repository == shared
                    && lease.managed
                    && lease.state == "released"
                    && lease.workspace.is_dir()
                    && transaction.active(&lease.workspace).is_none()
            }) {
                workspace = idle.workspace.clone();
                workspace_name = idle.workspace_name.clone();
                managed = true;
            } else {
                std::fs::create_dir_all(&pool).map_err(crate::ownership::OwnershipError::from)?;
                workspace = tempfile::Builder::new()
                    .prefix("workspace-")
                    .tempdir_in(pool)
                    .map_err(crate::ownership::OwnershipError::from)?
                    .keep();
                managed = true;
                workspace_name = Some(format!(
                    "mcp-{}",
                    workspace.file_name().unwrap_or_default().to_string_lossy()
                ));
            }
        } else {
            base = self.snapshot_value(&repo, "commit_id").await?;
        }
        transaction.state.generation = transaction
            .state
            .generation
            .checked_add(1)
            .ok_or_else(|| refused("ownership generation exhausted"))?;
        let generation = transaction.state.generation;
        let index = transaction.state.leases.len();
        transaction.state.leases.push(Lease {
            external_activity: Default::default(),
            owner,
            repository: shared,
            workspace: workspace.clone(),
            generation,
            task: params.task.clone(),
            acquired_at: now(),
            last_activity: now(),
            expected_completion: now().saturating_add(params.expected_seconds),
            state: "preparing".into(),
            change_id: String::new(),
            owned_changes: Vec::new(),
            mutation_snapshot: None,
            saved_commit: None,
            saved_heads: None,
            saved_operation: None,
            managed,
            workspace_name: workspace_name.clone(),
            base: base.clone(),
        });
        transaction.save()?;
        if !workspace.join(".jj").exists() {
            let name = workspace_name.ok_or_else(|| refused("managed workspace name missing"))?;
            let args = vec![
                "--ignore-working-copy".into(),
                "--config=git.colocate=false".into(),
                "workspace".into(),
                "add".into(),
                "--sparse-patterns=full".into(),
                format!("--name={name}"),
                format!("--revision={base}"),
                "--".into(),
                workspace.to_string_lossy().into_owned(),
            ];
            self.run_watched(&repo, &args, context, None).await?;
            // add with --ignore-working-copy registers the new workspace without
            // touching the source. Materialize only the reserved destination.
            let assigned = RepoPath::new(&workspace)?;
            self.run_watched(
                &assigned,
                &["workspace".into(), "update-stale".into()],
                context,
                None,
            )
            .await?;
        } else {
            let assigned = RepoPath::new(&workspace)?;
            // Snapshot pre-existing work only after persisting the reservation.
            if managed {
                self.run_watched(&assigned, &["status".into()], context, None)
                    .await?;
                if self.snapshot_value(&assigned, "empty").await? != "true" {
                    return Err(refused(
                        "idle copy contains new work; reservation retained for explicit recovery",
                    )
                    .into());
                }
            }
            let mut args = vec!["new".into(), format!("--message={}", params.task)];
            if managed {
                args.push("--".into());
                args.push(base.clone());
            }
            self.run_watched(&assigned, &args, context, None).await?;
        }
        let assigned = RepoPath::new(&workspace)?;
        let change_id = self.snapshot_value(&assigned, "change_id").await?;
        let prepared_base = self
            .snapshot_value(&assigned, "parents.map(|p| p.commit_id()).join(\"\")")
            .await?;
        let lease = &mut transaction.state.leases[index];
        lease.base = prepared_base;
        lease.change_id = change_id.clone();
        lease.owned_changes.push(change_id);
        lease.state = "active".into();
        transaction.save()?;
        Ok(LockOutput {
            acquired: true,
            lease: Some(transaction.state.leases[index].clone()),
            revalidate: true,
        })
    }
}

fn metadata_generation(meta: &RequestMetaObject) -> Result<Option<u64>, ToolError> {
    meta.get("jujutsu-mcp/generation")
        .map(|value| {
            value
                .as_u64()
                .ok_or_else(|| refused("adapter generation must be an unsigned integer").into())
        })
        .transpose()
}

#[tool_router(router = workspace_router, vis = "pub(crate)")]
impl JjServer {
    /// Report normalized identity, optional session name and source/aliases for adapter verification.
    #[tool(annotations(read_only_hint = true, open_world_hint = false))]
    pub async fn workspace_identity(
        &self,
        context: RequestContext<RoleServer>,
    ) -> Result<Json<Caller>, ToolError> {
        Ok(Json(caller(&context.meta)?))
    }

    /// Acquire immediately before the first edit; keep ownership through all checks.
    /// Revalidate the inspected base after acquiring. No model-managed lease token.
    #[tool(annotations(
        read_only_hint = false,
        destructive_hint = false,
        open_world_hint = false
    ))]
    pub async fn workspace_lock(
        &self,
        Parameters(params): Parameters<WorkspaceLockParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<Json<LockOutput>, ToolError> {
        non_empty("task", &params.task)?;
        if !(1..=86400).contains(&params.expected_seconds) || params.wait_seconds > 30 {
            return Err(ToolError::InvalidParams(
                "expected_seconds must be 1..86400 and wait_seconds 0..30".into(),
            ));
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(params.wait_seconds);
        loop {
            let result = self.acquire(&params, &context).await?;
            if result.acquired || tokio::time::Instant::now() >= deadline {
                return Ok(Json(result));
            }
            tokio::select! {
                () = context.ct.cancelled() => return Err(ToolError::Cancelled),
                () = tokio::time::sleep_until(deadline.min(tokio::time::Instant::now() + Duration::from_millis(50))) => {},
            }
        }
    }

    /// Report occupancy and estimate; an overdue lease is never stolen or expired.
    #[tool(annotations(read_only_hint = true, open_world_hint = false))]
    pub async fn workspace_lock_status(
        &self,
        Parameters(params): Parameters<WorkspaceParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<Json<StatusOutput>, ToolError> {
        caller(&context.meta)?;
        let repo = RepoPath::new(&params.repo)?;
        let (_, workspace) = repository(&repo)?;
        let state = self.ownership_required()?.snapshot()?;
        let lease = state
            .active(&workspace)
            .map(|index| state.leases[index].clone());
        Ok(Json(StatusOutput {
            elapsed_seconds: lease
                .as_ref()
                .map_or(0, |lease| now().saturating_sub(lease.acquired_at)),
            overdue: lease
                .as_ref()
                .is_some_and(|lease| now() > lease.expected_completion),
            lease,
        }))
    }

    /// Save and verify the final result, then release. Requires all external writes,
    /// builds and checks to have stopped. Does not integrate or publish anything.
    #[tool(annotations(
        read_only_hint = false,
        destructive_hint = false,
        open_world_hint = false
    ))]
    pub async fn workspace_unlock(
        &self,
        Parameters(params): Parameters<WorkspaceUnlockParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<Json<Lease>, ToolError> {
        non_empty("message", &params.message)?;
        if !params.checks_complete {
            return Err(refused(
                "finish checks and stop all external edits/builds before releasing",
            )
            .into());
        }
        let repo = RepoPath::new(&params.repo)?;
        let mut guard = self.mutation_guard(&repo, &context).await?;
        let index = guard
            .index
            .ok_or_else(|| refused("ownership mode is disabled"))?;
        if let Some(transaction) = &mut guard.transaction {
            if !transaction.state.leases[index].external_activity.is_empty() {
                return Err(refused("external edits/builds/tests are still in flight; finish their matching end hooks before releasing").into());
            }
            transaction.state.leases[index].state = "releasing".into();
            transaction.save()?;
        }
        let args = vec!["describe".into(), format!("--message={}", params.message)];
        self.run_owned(&repo, &args, &context, &mut guard).await?;
        let commit = self.snapshot_value(&repo, "commit_id").await?;
        let change = self.snapshot_value(&repo, "change_id").await?;
        let conflict = self.snapshot_value(&repo, "conflict").await?;
        if conflict != "false" {
            return Err(refused("result has unresolved conflicts; reservation retained; resume, resolve and check before releasing").into());
        }
        // The exact commit is verified, and kept as a parent of the idle @.
        let operation = self
            .read_output(
                &repo,
                &[
                    "op".into(),
                    "log".into(),
                    "--no-graph".into(),
                    "--limit=1".into(),
                    "--template=id".into(),
                ],
            )
            .await?
            .stdout
            .trim()
            .to_owned();
        let owned = guard
            .transaction
            .as_ref()
            .ok_or_else(|| refused("missing ownership transaction"))?
            .state
            .leases[index]
            .owned_changes
            .iter()
            .map(|id| format!("present({id})"))
            .collect::<Vec<_>>()
            .join(" | ");
        let heads = self
            .read_output(
                &repo,
                &[
                    "log".into(),
                    "--no-graph".into(),
                    format!("--revisions=heads(all() & ({owned}))"),
                    "--template=commit_id ++ \"\\n\"".into(),
                ],
            )
            .await?
            .stdout
            .lines()
            .map(str::to_owned)
            .collect();
        if let Some(transaction) = &mut guard.transaction {
            let lease = &mut transaction.state.leases[index];
            lease.saved_heads = Some(heads);
            lease.saved_commit = Some(commit.clone());
            lease.saved_operation = Some(operation);
            lease.change_id = change;
            transaction.save()?;
        }
        self.run_watched(
            &repo,
            &["new".into(), "--".into(), commit.clone()],
            &context,
            None,
        )
        .await?;
        if self.snapshot_value(&repo, "empty").await? != "true"
            || self.snapshot_value(&repo, "conflict").await? != "false"
        {
            return Err(
                refused("could not prepare a clean idle change; reservation retained").into(),
            );
        }
        let transaction = guard
            .transaction
            .as_mut()
            .ok_or_else(|| refused("missing ownership transaction"))?;
        transaction.state.leases[index].state = "released".into();
        transaction.state.leases[index].last_activity = now();
        transaction.save()?;
        Ok(Json(transaction.state.leases[index].clone()))
    }

    /// Remove extra released copies only after every saved task head is verified
    /// in an integration destination. Dirty, ignored/untracked or unknown files retain the copy.
    #[tool(annotations(
        read_only_hint = false,
        destructive_hint = true,
        open_world_hint = false
    ))]
    pub async fn workspace_cleanup(
        &self,
        Parameters(params): Parameters<CleanupParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<Json<CleanupOutput>, ToolError> {
        caller(&context.meta)?;
        let ownership = self.ownership_required()?;
        let repo = RepoPath::new(&params.repo)?;
        let (shared, _) = repository(&repo)?;
        let mut transaction = ownership.transaction(&context).await?;
        let mut candidates: Vec<usize> = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for (index, lease) in transaction.state.leases.iter().enumerate().rev() {
            if !seen.insert(lease.workspace.clone()) || lease.repository != shared {
                continue;
            }
            if lease.managed
                && lease.state == "released"
                && transaction.active(&lease.workspace).is_none()
            {
                candidates.push(index);
            }
        }
        let local_main = self
            .read_output(
                &repo,
                &[
                    "log".into(),
                    "--no-graph".into(),
                    "--revisions=bookmarks(exact:\"main\")".into(),
                    "--template=commit_id".into(),
                ],
            )
            .await?;
        let default_target = if local_main.stdout.is_empty() {
            "trunk()"
        } else {
            "bookmarks(exact:\"main\")"
        };
        let target = params.integrated_into.as_deref().unwrap_or(default_target);
        non_empty("integrated_into", target)?;
        // A named destination is independent of a workspace's automatic @ or a
        // loose saved commit. Resolving arbitrary all()/@ cannot prove integration.
        let resolved = self
            .read_output(
                &repo,
                &[
                    "log".into(),
                    "--no-graph".into(),
                    format!("--revisions=(({target}) & (bookmarks() | remote_bookmarks() | tags())) ~ conflicts()"),
                    "--template=commit_id ++ \"\\n\"".into(),
                ],
            )
            .await?;
        let mut output = CleanupOutput {
            removed: Vec::new(),
            retained: Vec::new(),
            retained_reasons: Default::default(),
            integration_targets: resolved.stdout.lines().map(str::to_owned).collect(),
        };
        for (offset, index) in candidates.into_iter().enumerate() {
            let lease = &transaction.state.leases[index];
            let workspace = lease.workspace.clone();
            let reason = if offset < ownership.idle_limit {
                Some("kept within the configured idle limit".into())
            } else if !workspace.starts_with(ownership.directory.join("pool"))
                || workspace.is_symlink()
                || !workspace.join(".jj").is_dir()
            {
                Some("not an intact managed workspace".into())
            } else if output.integration_targets.is_empty() {
                Some("integration destination has no conflict-free named commits; set integrated_into to the destination bookmark".into())
            } else {
                self.cleanup_integration(
                    &repo,
                    &transaction.state.leases,
                    &workspace,
                    &output.integration_targets,
                )
                .await?
            };
            if let Some(reason) = reason {
                output.retained_reasons.insert(workspace.clone(), reason);
                output.retained.push(workspace);
                continue;
            }
            let assigned = RepoPath::new(&workspace)?;
            let saved = lease
                .saved_commit
                .as_deref()
                .ok_or_else(|| refused("released workspace has no saved result"))?;
            let changed = self
                .read_output(
                    &assigned,
                    &[
                        "diff".into(),
                        format!("--from={saved}"),
                        "--to=@".into(),
                        "--summary".into(),
                    ],
                )
                .await?;
            if repository(&assigned)?.0 != shared
                || self.snapshot_value(&assigned, "empty").await? != "true"
                || !self
                    .snapshot_value(&assigned, "description")
                    .await?
                    .is_empty()
                || !changed.stdout.is_empty()
            {
                output.retained_reasons.insert(
                    workspace.clone(),
                    "workspace snapshot changed since release".into(),
                );
                output.retained.push(workspace);
                continue;
            }
            let verified =
                match crate::cleanup::verify(&self.runner, &assigned, saved, &context).await? {
                    Ok(verified) => verified,
                    Err(reason) => {
                        output.retained_reasons.insert(workspace.clone(), reason);
                        output.retained.push(workspace);
                        continue;
                    }
                };
            let name = lease
                .workspace_name
                .clone()
                .ok_or_else(|| refused("managed workspace name missing"))?;
            transaction.state.leases[index].state = "cleaning".into();
            transaction.save()?;
            // Source files are individually verified and removed. Unknown files
            // or directories are never recursively deleted.
            verified.remove_files()?;
            self.run_watched(
                &repo,
                &[
                    "--ignore-working-copy".into(),
                    "workspace".into(),
                    "forget".into(),
                    "--".into(),
                    name,
                ],
                &context,
                None,
            )
            .await?;
            verified.remove_metadata()?;
            transaction.state.leases[index].state = "removed".into();
            transaction.save()?;
            output.removed.push(workspace);
        }
        Ok(Json(output))
    }
}

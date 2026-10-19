//! Durable, transport-neutral workspace ownership. Caller IDs coordinate trusted clients;
//! they are not authentication. The advisory file lock serializes backend processes.

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rmcp::RoleServer;
use rmcp::model::RequestMetaObject;
use rmcp::service::RequestContext;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::jj::workspace_root;
use crate::repo::RepoPath;
use crate::tools::ToolError;

#[derive(Debug, thiserror::Error)]
pub enum OwnershipError {
    #[error("ownership storage: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid ownership state (refusing to reset it): {0}")]
    State(#[from] serde_json::Error),
    #[error("{0}")]
    Refused(String),
}

pub(crate) fn refused(message: impl Into<String>) -> OwnershipError {
    OwnershipError::Refused(message.into())
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct Caller {
    pub provider: String,
    pub session_id: String,
    /// Human-readable integration-supplied label; never part of the ownership key.
    pub session_name: Option<String>,
    pub executor_id: String,
    pub source: String,
    pub aliases: BTreeMap<String, String>,
}

impl Caller {
    pub fn same_executor(&self, other: &Self) -> bool {
        self.provider == other.provider
            && self.session_id == other.session_id
            && self.executor_id == other.executor_id
            && self.source == other.source
    }
}

/// Only these explicitly forwarded fields are examined. No environment, transport
/// session, initialization product name or transcript is an identity fallback.
pub fn caller(meta: &RequestMetaObject) -> Result<Caller, OwnershipError> {
    let native_name = meta
        .get("jujutsu-mcp/session-name")
        .map(|value| {
            value
                .as_str()
                .ok_or_else(|| refused("session_name must be a string"))
        })
        .transpose()?;
    let native_name = session_name(native_name)?;
    let mut aliases = BTreeMap::new();
    for key in ["threadId", "sessionId"] {
        if let Some(value) = meta.get(key) {
            let id = value
                .as_str()
                .filter(|s| {
                    !s.trim().is_empty() && s.len() <= 512 && !s.chars().any(char::is_control)
                })
                .ok_or_else(|| refused(format!("_meta.{key} must be a non-empty string")))?;
            aliases.insert(format!("codex.{key}"), id.to_owned());
        }
    }
    if let Some(value) = meta.get("jujutsu-mcp/identity") {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Forwarded {
            provider: String,
            session_id: String,
            #[serde(default)]
            session_name: Option<String>,
            executor_id: String,
            source: String,
        }
        let identity: Forwarded = serde_json::from_value(value.clone()).map_err(|_| refused("forwarded identity requires provider, session_id, executor_id and source; only optional session_name is also accepted"))?;
        for (field, value) in [
            ("provider", &identity.provider),
            ("session_id", &identity.session_id),
            ("executor_id", &identity.executor_id),
            ("source", &identity.source),
        ] {
            if value.trim().is_empty() || value.len() > 512 || value.chars().any(char::is_control) {
                return Err(refused(format!(
                    "identity.{field} must be non-empty, at most 512 bytes, without control characters"
                )));
            }
        }
        if !["acp", "claude", "adapter"].contains(&identity.source.as_str()) {
            return Err(refused("identity.source must be acp, claude or adapter"));
        }
        if !aliases.is_empty()
            && (identity.provider != "codex"
                || !aliases.values().any(|id| id == &identity.session_id))
        {
            return Err(refused(
                "ambiguous caller: forwarded identity conflicts with Codex metadata; fix the adapter",
            ));
        }
        return Ok(Caller {
            provider: identity.provider,
            session_id: identity.session_id,
            session_name: session_name(identity.session_name.as_deref())?.or(native_name),
            executor_id: identity.executor_id,
            source: identity.source,
            aliases,
        });
    }
    if let Some(thread) = aliases.get("codex.threadId") {
        return Ok(Caller {
            provider: "codex".into(),
            session_id: thread.clone(),
            session_name: native_name,
            executor_id: "thread".into(),
            source: "codex-metadata".into(),
            aliases,
        });
    }
    Err(refused(
        "missing unambiguous caller identity: forward _meta['jujutsu-mcp/identity'] with provider, session_id, executor_id and source (acp/claude/adapter), or Codex threadId; sessionId alone cannot distinguish children",
    ))
}

fn session_name(name: Option<&str>) -> Result<Option<String>, OwnershipError> {
    name.map(|name| {
        if name.trim().is_empty() || name.len() > 512 || name.chars().any(char::is_control) {
            return Err(refused(
                "session_name must be non-empty, at most 512 bytes, without control characters",
            ));
        }
        Ok(name.trim().to_owned())
    })
    .transpose()
}

pub fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Lease {
    pub external_activity: BTreeMap<String, u64>,
    pub owner: Caller,
    pub repository: PathBuf,
    pub workspace: PathBuf,
    pub generation: u64,
    pub task: String,
    pub acquired_at: u64,
    pub last_activity: u64,
    pub expected_completion: u64,
    /// preparing/mutating/releasing require explicit resume; never inferred idle after a crash.
    pub state: String,
    pub change_id: String,
    /// Changes reserved only until this lease is released, including adopted work.
    pub owned_changes: Vec<String>,
    /// Commit-to-change inventory retained across an interrupted mutation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mutation_snapshot: Option<BTreeMap<String, String>>,
    pub saved_commit: Option<String>,
    /// Final task heads saved on release, including parallel branches.
    pub saved_heads: Option<Vec<String>>,
    pub saved_operation: Option<String>,
    pub managed: bool,
    pub workspace_name: Option<String>,
    pub base: String,
}

impl Lease {
    pub fn is_reserved(&self) -> bool {
        self.state != "released" && self.state != "removed"
    }

    pub(crate) fn has_pending_transition(&self) -> bool {
        matches!(self.state.as_str(), "preparing" | "mutating" | "releasing")
    }

    pub(crate) fn record_mutation(
        &mut self,
        before: &BTreeMap<String, String>,
        after: &BTreeMap<String, String>,
    ) {
        for change in before
            .iter()
            .filter(|(commit, _)| !after.contains_key(*commit))
            .chain(
                after
                    .iter()
                    .filter(|(commit, _)| !before.contains_key(*commit)),
            )
            .map(|(_, change)| change)
        {
            if !self.owned_changes.contains(change) {
                self.owned_changes.push(change.clone());
            }
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct State {
    pub generation: u64,
    pub leases: Vec<Lease>,
}

#[derive(Debug, Clone)]
pub struct Ownership {
    pub directory: PathBuf,
    pub idle_limit: usize,
}

impl Ownership {
    pub fn new(directory: PathBuf, idle_limit: usize) -> Result<Self, OwnershipError> {
        if !directory.is_absolute() {
            return Err(refused("ownership state directory must be absolute"));
        }
        std::fs::create_dir_all(&directory)?;
        let directory = directory.canonicalize()?;
        if directory
            .ancestors()
            .any(|dir| dir.join(".jj").exists() || dir.join(".git").exists())
        {
            return Err(refused(
                "ownership state directory must be outside source trees",
            ));
        }
        Ok(Self {
            directory,
            idle_limit,
        })
    }

    /// Atomic rename makes a lock-free observation safe. Capture the request's
    /// generation before waiting for either the queue or a running backend write.
    pub fn snapshot(&self) -> Result<State, OwnershipError> {
        match std::fs::read(self.directory.join("state.json")) {
            Ok(bytes) => Ok(serde_json::from_slice(&bytes)?),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(State::default()),
            Err(err) => Err(err.into()),
        }
    }

    /// Try once; used by both MCP and external write hooks with the same OS lock.
    pub fn try_transaction(&self) -> Result<Option<Transaction>, OwnershipError> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(self.directory.join("backend.lock"))?;
        match file.try_lock() {
            Ok(()) => {
                let state = match File::open(self.directory.join("state.json")) {
                    Ok(mut input) => {
                        let mut bytes = Vec::new();
                        input.read_to_end(&mut bytes)?;
                        serde_json::from_slice(&bytes)?
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => State::default(),
                    Err(err) => return Err(err.into()),
                };
                Ok(Some(Transaction {
                    _lock: file,
                    directory: self.directory.clone(),
                    state,
                }))
            }
            Err(std::fs::TryLockError::WouldBlock) => Ok(None),
            Err(std::fs::TryLockError::Error(err)) => Err(err.into()),
        }
    }

    /// Nonblocking OS lock acquisition keeps waits cancellable, including across
    /// independently started stdio servers. Lock files are never renamed/deleted.
    pub async fn transaction(
        &self,
        context: &RequestContext<RoleServer>,
    ) -> Result<Transaction, ToolError> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            if context.ct.is_cancelled() {
                return Err(ToolError::Cancelled);
            }
            if let Some(transaction) = self.try_transaction()? {
                return Ok(transaction);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(refused(
                    "ownership backend busy; retry after the running operation completes",
                )
                .into());
            }
            tokio::select! {
                () = context.ct.cancelled() => return Err(ToolError::Cancelled),
                () = tokio::time::sleep(Duration::from_millis(20)) => {},
            }
        }
    }
}

#[derive(Debug)]
pub struct Transaction {
    _lock: File,
    directory: PathBuf,
    pub state: State,
}

impl Transaction {
    pub fn save(&self) -> Result<(), OwnershipError> {
        let mut temporary = tempfile::NamedTempFile::new_in(&self.directory)?;
        serde_json::to_writer(&mut temporary, &self.state)?;
        temporary.flush()?;
        temporary.as_file().sync_all()?;
        temporary
            .persist(self.directory.join("state.json"))
            .map_err(|err| err.error)?;
        #[cfg(unix)]
        File::open(&self.directory)?.sync_all()?;
        Ok(())
    }

    pub fn active(&self, workspace: &Path) -> Option<usize> {
        self.state.active(workspace)
    }
    pub fn require(
        &self,
        workspace: &Path,
        owner: &Caller,
        generation: Option<u64>,
    ) -> Result<usize, OwnershipError> {
        self.state.require(workspace, owner, generation)
    }
}

impl State {
    pub fn active(&self, workspace: &Path) -> Option<usize> {
        self.leases.iter().rposition(|lease| {
            lease.workspace == workspace && lease.state != "released" && lease.state != "removed"
        })
    }

    pub fn require(
        &self,
        workspace: &Path,
        owner: &Caller,
        generation: Option<u64>,
    ) -> Result<usize, OwnershipError> {
        let index = self.active(workspace).ok_or_else(|| {
            refused("workspace is unlocked; call workspace_lock immediately before editing")
        })?;
        let lease = &self.leases[index];
        if !lease.owner.same_executor(owner) {
            return Err(refused(
                "workspace belongs to another executor; inspect workspace_lock_status or acquire a parallel workspace",
            ));
        }
        if generation.is_some_and(|generation| generation != lease.generation) {
            return Err(refused(
                "stale workspace generation; reacquire and revalidate the base",
            ));
        }
        Ok(index)
    }
}

/// Resolve the shared jj repository without running jj (which could snapshot).
pub fn repository(repo: &RepoPath) -> Result<(PathBuf, PathBuf), OwnershipError> {
    let workspace = workspace_root(repo);
    let entry = workspace.join(".jj/repo");
    let shared = if entry.is_file() {
        let target = std::fs::read_to_string(&entry)?;
        let target = PathBuf::from(target.trim());
        if target.is_absolute() {
            target
        } else {
            workspace.join(".jj").join(target)
        }
    } else {
        entry
    };
    Ok((shared.canonicalize()?, workspace))
}

/// Adapter-facing hooks bracket filesystem edits and build/test processes that
/// MCP cannot observe. A crash retains the activity until an explicit end event.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookInput {
    pub repo: String,
    pub metadata: RequestMetaObject,
    pub activity_id: String,
    pub phase: String,
}

pub fn hook(ownership: &Ownership, input: HookInput) -> Result<(), OwnershipError> {
    let owner = caller(&input.metadata)?;
    if input.activity_id.trim().is_empty() || input.activity_id.len() > 512 {
        return Err(refused(
            "activity_id must be non-empty and at most 512 bytes",
        ));
    }
    let repo = RepoPath::new(&input.repo).map_err(|err| refused(err.to_string()))?;
    let (_, workspace) = repository(&repo)?;
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut transaction = loop {
        if let Some(transaction) = ownership.try_transaction()? {
            break transaction;
        }
        if std::time::Instant::now() >= deadline {
            return Err(refused("ownership backend busy; retry the hook"));
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    let generation = input
        .metadata
        .get("jujutsu-mcp/generation")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| {
            refused(
                "external hooks require the adapter-managed workspace generation from acquisition",
            )
        })?;
    let index = transaction.require(&workspace, &owner, Some(generation))?;
    let lease = &mut transaction.state.leases[index];
    if lease.state != "active" {
        return Err(refused("lease requires recovery before external writes"));
    }
    match input.phase.as_str() {
        "begin" => {
            lease
                .external_activity
                .insert(input.activity_id, lease.generation);
        }
        "end" => {
            if lease.external_activity.remove(&input.activity_id).is_none() {
                return Err(refused(
                    "unknown activity; refusing to clear another operation",
                ));
            }
        }
        _ => return Err(refused("hook phase must be begin or end")),
    }
    lease.last_activity = now();
    transaction.save()
}

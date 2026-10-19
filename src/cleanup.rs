//! Verify a released workspace against saved bytes before removing any source files.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path, PathBuf};

use rmcp::RoleServer;
use rmcp::service::RequestContext;

use crate::jj::JjRunner;
use crate::ownership::OwnershipError;
use crate::repo::RepoPath;
use crate::tools::{ToolError, literal_path_fileset};

pub(crate) struct VerifiedWorkspace {
    root: PathBuf,
    files: Vec<PathBuf>,
    directories: Vec<PathBuf>,
    metadata_files: Vec<PathBuf>,
}

impl VerifiedWorkspace {
    pub fn remove_files(&self) -> Result<(), OwnershipError> {
        for file in &self.files {
            std::fs::remove_file(self.root.join(file))?;
        }
        for directory in &self.directories {
            std::fs::remove_dir(self.root.join(directory))?;
        }
        Ok(())
    }

    pub fn remove_metadata(&self) -> Result<(), OwnershipError> {
        for file in &self.metadata_files {
            std::fs::remove_file(self.root.join(file))?;
        }
        std::fs::remove_dir(self.root.join(".jj/working_copy"))?;
        std::fs::remove_dir(self.root.join(".jj"))?;
        std::fs::remove_dir(&self.root)?;
        Ok(())
    }
}

/// Never follow symlinks or recursively delete user content. Unknown/ignored
/// files and empty directories count as local work and retain the whole copy.
pub(crate) async fn verify(
    runner: &JjRunner,
    repo: &RepoPath,
    saved: &str,
    context: &RequestContext<RoleServer>,
) -> Result<Result<VerifiedWorkspace, String>, ToolError> {
    let root = repo.as_path();
    for directory in [".jj", ".jj/working_copy"] {
        if !std::fs::symlink_metadata(root.join(directory))
            .map_err(OwnershipError::from)?
            .file_type()
            .is_dir()
        {
            return Ok(Err(
                "workspace metadata is not an ordinary managed directory".into(),
            ));
        }
    }
    let raw = runner
        .run(
            repo,
            &[
                "--ignore-working-copy".into(),
                "--at-operation=@".into(),
                "file".into(),
                "list".into(),
                format!("--revision={saved}"),
                "--template=json(path) ++ \"\\t\" ++ file_type ++ \"\\t\" ++ executable ++ \"\\n\""
                    .into(),
            ],
        )
        .await?;
    let mut tracked = BTreeMap::new();
    let mut expected_directories = BTreeSet::new();
    for line in raw.stdout.lines() {
        let fields: Vec<_> = line.split('\t').collect();
        if fields.len() != 3 {
            return Ok(Err("cannot read saved file metadata".into()));
        }
        let relative: String = serde_json::from_str(fields[0])?;
        let relative = PathBuf::from(relative);
        if relative.as_os_str().is_empty()
            || !relative
                .components()
                .all(|part| matches!(part, Component::Normal(_)))
            || relative.starts_with(".jj")
            || fields[1] != "file"
            || !["true", "false"].contains(&fields[2])
        {
            return Ok(Err(
                "saved tree contains unsupported file types or paths; copy retained".into(),
            ));
        }
        for parent in relative
            .ancestors()
            .skip(1)
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            expected_directories.insert(parent.to_path_buf());
        }
        tracked.insert(relative, fields[2] == "true");
    }
    let mut files = BTreeSet::new();
    let mut directories = BTreeSet::new();
    let mut pending = vec![PathBuf::new()];
    while let Some(relative) = pending.pop() {
        if context.ct.is_cancelled() {
            return Err(ToolError::Cancelled);
        }
        for entry in std::fs::read_dir(root.join(&relative)).map_err(OwnershipError::from)? {
            let entry = entry.map_err(OwnershipError::from)?;
            let path = relative.join(entry.file_name());
            if path == Path::new(".jj") {
                continue;
            }
            let kind = entry.file_type().map_err(OwnershipError::from)?;
            if kind.is_file() {
                if !tracked.contains_key(&path) {
                    return Ok(Err("workspace contains untracked or ignored files".into()));
                }
                files.insert(path);
            } else if kind.is_dir() {
                if !expected_directories.contains(&path) {
                    return Ok(Err(
                        "workspace contains untracked or ignored directories".into()
                    ));
                }
                directories.insert(path.clone());
                pending.push(path);
            } else {
                return Ok(Err(
                    "workspace contains symlinks or special files; copy retained".into(),
                ));
            }
        }
    }
    if files != tracked.keys().cloned().collect() || directories != expected_directories {
        return Ok(Err(
            "workspace contains missing, untracked or ignored files/directories".into(),
        ));
    }
    for (relative, executable) in &tracked {
        if context.ct.is_cancelled() {
            return Err(ToolError::Cancelled);
        }
        let actual = std::fs::read(root.join(relative)).map_err(OwnershipError::from)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::symlink_metadata(root.join(relative))
                .map_err(OwnershipError::from)?
                .permissions()
                .mode();
            if (mode & 0o111 != 0) != *executable {
                return Ok(Err(
                    "tracked executable permissions changed since release".into()
                ));
            }
        }
        #[cfg(not(unix))]
        let _ = executable;
        let path = relative.to_str().ok_or_else(|| {
            crate::ownership::refused("cleanup requires representable saved paths")
        })?;
        let args = [
            "--ignore-working-copy".into(),
            "--at-operation=@".into(),
            "file".into(),
            "show".into(),
            format!("--revision={saved}"),
            "--template=\"\"".into(),
            "--".into(),
            literal_path_fileset(path)?,
        ];
        let expected = tokio::select! {
            () = context.ct.cancelled() => return Err(ToolError::Cancelled),
            output = runner.run_bytes(repo, &args) => output?,
        };
        if actual != expected {
            return Ok(Err("tracked file contents changed since release".into()));
        }
    }
    let mut metadata_files = Vec::new();
    for (directory, allowed_files) in [
        (".jj", &["repo", ".gitignore"][..]),
        (
            ".jj/working_copy",
            &["checkout", "tree_state", "type", "lock"][..],
        ),
    ] {
        for entry in std::fs::read_dir(root.join(directory)).map_err(OwnershipError::from)? {
            let entry = entry.map_err(OwnershipError::from)?;
            let name = entry.file_name();
            let kind = entry.file_type().map_err(OwnershipError::from)?;
            if directory == ".jj" && name == "working_copy" && kind.is_dir() {
                continue;
            }
            if !kind.is_file() || !allowed_files.iter().any(|allowed| name == *allowed) {
                return Ok(Err(
                    "workspace has unknown jj metadata or local configuration".into(),
                ));
            }
            metadata_files.push(PathBuf::from(directory).join(name));
        }
    }
    let mut directories: Vec<_> = directories.into_iter().collect();
    directories.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
    Ok(Ok(VerifiedWorkspace {
        root: root.to_path_buf(),
        files: files.into_iter().collect(),
        directories,
        metadata_files,
    }))
}

//! jujutsu-mcp: MCP server for jj over stdio. stdout carries JSON-RPC only;
//! logs go to stderr, filtered by `RUST_LOG`.

use std::process::ExitCode;

use jujutsu_mcp::jj::{JjRunner, TESTED_JJ_VERSION};
use jujutsu_mcp::server::JjServer;
use jujutsu_mcp::setup::{Environment, SetupError, parse_setup_args, run_setup};
use jujutsu_mcp::split_editor::SplitEditorError;
use rmcp::ServiceExt;
use rmcp::service::ServerInitializeError;
use rmcp::transport::stdio;
use tracing_subscriber::EnvFilter;

#[derive(Debug, thiserror::Error)]
enum FatalError {
    #[error("could not start the async runtime: {0}")]
    Runtime(#[source] std::io::Error),
    #[error("MCP handshake failed: {0}")]
    Handshake(String),
    #[error("server task failed: {0}")]
    Serve(#[source] tokio::task::JoinError),
    #[error(transparent)]
    Ownership(#[from] jujutsu_mcp::ownership::OwnershipError),
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args_os()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect();
    match args.get(1).map(String::as_str) {
        None => serve(None),
        Some("--ownership-state") => {
            if let Some(path) = args
                .get(2)
                .filter(|_| args.len() == 3 || (args.len() == 5 && args[3] == "--idle-limit"))
            {
                let limit = args.get(4).map_or(Ok(1), |value| value.parse::<usize>());
                match limit {
                    Ok(limit) => serve(Some((path.into(), limit))),
                    Err(_) => {
                        eprintln!("--idle-limit must be an unsigned integer");
                        ExitCode::from(2)
                    }
                }
            } else {
                eprintln!(
                    "usage: jujutsu-mcp --ownership-state ABSOLUTE_DIRECTORY [--idle-limit N]"
                );
                ExitCode::from(2)
            }
        }
        Some("setup") => setup(&args[2..]),
        Some("workspace-hook") => workspace_hook(&args[2..]),
        Some(jujutsu_mcp::split_editor::COMMAND) => split_editor(&args[2..]),
        Some(other) => {
            eprintln!("jujutsu-mcp: unknown argument: {other}");
            eprintln!("{}", jujutsu_mcp::setup::USAGE);
            ExitCode::from(2)
        }
    }
}

/// Narrow JSON input, intended for integration-owned Pre/Post tool hooks.
fn workspace_hook(args: &[String]) -> ExitCode {
    use std::io::Read;
    let result = (|| -> Result<(), jujutsu_mcp::ownership::OwnershipError> {
        if args.len() != 2 || args[0] != "--ownership-state" {
            return Err(jujutsu_mcp::ownership::OwnershipError::Refused(
                "usage: jujutsu-mcp workspace-hook --ownership-state DIRECTORY (JSON on stdin)"
                    .into(),
            ));
        }
        let ownership = jujutsu_mcp::ownership::Ownership::new(args[1].clone().into(), 1)?;
        let mut bytes = Vec::new();
        std::io::stdin().take(16385).read_to_end(&mut bytes)?;
        if bytes.len() > 16384 {
            return Err(jujutsu_mcp::ownership::OwnershipError::Refused(
                "hook input exceeds 16 KiB".into(),
            ));
        }
        jujutsu_mcp::ownership::hook(&ownership, serde_json::from_slice(&bytes)?)
    })();
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("jujutsu-mcp: {err}");
            ExitCode::FAILURE
        }
    }
}

/// `jujutsu-mcp setup`: no tracing and no async runtime, it only runs a few
/// short commands.
fn setup(args: &[String]) -> ExitCode {
    let result = parse_setup_args(args).and_then(|options| {
        let env = Environment::from_process()?;
        run_setup(&env, &options, &mut std::io::stdout())
    });
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("jujutsu-mcp: {err}");
            ExitCode::from(if matches!(err, SetupError::Usage(_)) {
                2
            } else {
                1
            })
        }
    }
}

/// `jujutsu-mcp split-editor`: the diff editor jj runs for `split` with
/// `contents`. No tracing and no async runtime, jj waits on it.
fn split_editor(args: &[String]) -> ExitCode {
    match jujutsu_mcp::split_editor::run(args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("jujutsu-mcp: {err}");
            ExitCode::from(if matches!(err, SplitEditorError::Usage) {
                2
            } else {
                1
            })
        }
    }
}

fn serve(ownership: Option<(std::path::PathBuf, usize)>) -> ExitCode {
    init_tracing();
    let result = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(FatalError::Runtime)
        .and_then(|runtime| runtime.block_on(run(ownership)));
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("jujutsu-mcp: {err}");
            ExitCode::FAILURE
        }
    }
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    // try_init: a second global subscriber is not worth failing the server for.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .try_init();
}

async fn run(ownership: Option<(std::path::PathBuf, usize)>) -> Result<(), FatalError> {
    let runner = JjRunner::new();
    log_versions(&runner).await;

    let mut server = JjServer::new(runner);
    if let Some((directory, limit)) = ownership {
        server = server.with_ownership(jujutsu_mcp::ownership::Ownership::new(directory, limit)?);
    }
    let service = match server.serve(stdio()).await {
        Ok(service) => service,
        // stdin closed before the client sent `initialize`: the client went
        // away, which is a normal shutdown, not a protocol failure.
        Err(ServerInitializeError::ConnectionClosed(reason)) => {
            tracing::info!("stdin closed before the handshake: {reason}");
            return Ok(());
        }
        Err(err) => return Err(FatalError::Handshake(err.to_string())),
    };
    service.waiting().await.map_err(FatalError::Serve)?;
    Ok(())
}

/// Logs the startup banner and warns about a jj older than the tested one.
/// Never fails: an unknown version must not keep the server from starting.
async fn log_versions(runner: &JjRunner) {
    let crate_version = env!("CARGO_PKG_VERSION");
    match runner.version().await {
        Ok(Some(jj)) => {
            tracing::info!("jujutsu-mcp {crate_version} starting, jj {jj}");
            if jj < TESTED_JJ_VERSION {
                tracing::warn!(
                    "jj {jj} is older than {TESTED_JJ_VERSION}, the version jujutsu-mcp was tested against; some tools may fail"
                );
            }
        }
        Ok(None) => {
            tracing::info!("jujutsu-mcp {crate_version} starting");
            tracing::warn!("could not parse the jj version from `jj --version`");
        }
        Err(err) => {
            tracing::info!("jujutsu-mcp {crate_version} starting");
            tracing::warn!("could not determine the jj version: {err}");
        }
    }
}

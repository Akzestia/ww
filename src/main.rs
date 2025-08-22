//! # ww — Scylla-backed secrets daemon + CLI
//!
//! `ww` provides a Doppler-like workflow for managing per-project environment
//! variables (aka “secrets”). A background **daemon** connects to ScyllaDB and
//! serves secrets over a Unix domain socket. The **CLI** talks to that socket
//! to:
//!
//! - inject env vars into a process (`ww run ...`),
//! - read or download secrets (`ww secrets ...`),
//! - upsert a single key (`ww set-env ...`),
//! - and manage a **global** client config that maps filesystem paths to
//!   logical project names (`ww client ...`).
//!
//! ## Architecture
//!
//! - **Storage:** ScyllaDB keyspace `ww`, table `projects(name, env_variables map<text,text>)`.
//! - **Transport:** Unix socket (default: `/tmp/ww.sock`) with a minimal
//!   line-oriented protocol.
//! - **Client config:** `~/.config/ww/config.toml` mapping directories → projects.
//!
//! ## Quick start
//!
//! 1. Start the daemon (adjust node/user/pass as needed):
//!    ```bash
//!    ww serve --node 127.0.0.1:9042 --user cassandra --pass cassandra
//!    ```
//! 2. Bind your repo to a project and ensure the project exists:
//!    ```bash
//!    cd /path/to/repo
//!    ww client setup my_project
//!    ```
//! 3. Add a secret:
//!    ```bash
//!    ww set-env --name API_KEY --val abc123
//!    ```
//! 4. Run your app with secrets injected:
//!    ```bash
//!    ww run -- node app.js
//!    ```
//!
//! ## Socket protocol
//!
//! - `QUERY_PROJECTS:<project>\n` → returns `KEY=VALUE\n...`
//! - `ENSURE_PROJECT:<project>\n` → returns `OK\n` or `ERROR:...`
//! - `SET_ENV:<project>:<key>=<value>\n` → returns `OK\n` or `ERROR:...`
//!
//! ## Safety & security
//!
//! - Socket permissions are set to `0600` on Unix.
//! - No encryption/auth over the socket—assumes local, single-user trust.
//! - Secrets are not printed by default; use `secrets get --plain` for raw value
//!   output when needed.
//!
//! ## Feature flags & editions
//!
//! - Rust 2021 edition.
//! - Async runtime: Tokio.
//!
//! ## Error handling
//!
//! - Library/daemon errors surface via `anyhow::Result` with context.
//! - CLI exit codes:
//!   - `0` on success,
//!   - Non-zero on errors; `run` exits with child process status when applicable.

use anyhow::{Context, Result, anyhow};
use clap::{Parser, Subcommand};
use scylla::{
    DeserializeRow,
    client::{session::Session, session_builder::SessionBuilder},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    env, fs,
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
    time::Instant,
};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

/// ===== Constants =====

/// Default Unix socket path used by both daemon and client.
const DEFAULT_SOCKET_PATH: &str = "/tmp/ww.sock";

/// Default project name when no path binding or explicit project is found.
const DEFAULT_PROJECT: &str = "demo_project";

/// ===== Global Client Config (paths <-> project) =====
/// Required location: ~/.config/ww/config.toml

/// Global client configuration stored under `~/.config/ww/config.toml`.
///
/// The config maps *directories* to *project* names and also carries a
/// `default_project` fallback.
///
/// # Format
///
/// ```toml
/// default_project = "demo_project"
///
/// [[paths]]
/// dir = "/abs/path/to/repo"
/// project = "my_project"
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct ClientConfig {
    /// Project used when neither `WW_PROJECT` nor a path binding applies.
    #[serde(default = "default_project_fallback")]
    default_project: String,
    /// Path → project bindings. Longer (more specific) paths take precedence.
    #[serde(default)]
    paths: Vec<PathBinding>,
}

/// A single binding from an absolute directory path to a project name.
///
/// Bindings are stored canonically (no trailing slash) and compared via
/// longest-prefix match.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct PathBinding {
    /// Absolute directory path. Prefer canonicalized paths (no symlinks).
    dir: String,
    /// Project name to use whenever the current working directory is under `dir`.
    project: String,
}

/// Fallback used by Serde when `default_project` is missing.
fn default_project_fallback() -> String {
    DEFAULT_PROJECT.into()
}

/// Returns the path to the client config TOML file.
///
/// # Errors
///
/// - Fails if the home directory cannot be resolved.
fn config_file_path() -> Result<PathBuf> {
    let home = dirs::home_dir().ok_or_else(|| anyhow!("cannot locate home directory"))?;
    Ok(home.join(".config/ww/config.toml"))
}

/// Loads the client configuration from disk, or returns defaults if missing/invalid.
///
/// This function is intentionally forgiving—any parse or I/O error results in a
/// default config so that CLI commands remain usable.
fn load_client_config() -> ClientConfig {
    let path = match config_file_path() {
        Ok(p) => p,
        Err(_) => return ClientConfig::default(),
    };
    if !path.exists() {
        return ClientConfig::default();
    }
    match fs::read_to_string(&path) {
        Ok(s) => toml::from_str::<ClientConfig>(&s).unwrap_or_default(),
        Err(_) => ClientConfig::default(),
    }
}

/// Persists the client configuration to disk (pretty TOML).
///
/// The bindings are sorted longest-path-first for readability.
///
/// # Errors
///
/// - Returns any underlying filesystem or serialization error.
fn save_client_config(mut cfg: ClientConfig) -> Result<()> {
    // ensure dirs exist
    let path = config_file_path()?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    // sort bindings by dir length desc so humans see most-specific first
    cfg.paths.sort_by_key(|b| usize::MAX - b.dir.len());
    let text = toml::to_string_pretty(&cfg)?;
    fs::write(path, text)?;
    Ok(())
}

/// Binds the **current working directory** to the provided `project`.
///
/// Creates or updates a `PathBinding` in the global config.
///
/// # Errors
///
/// - Fails if the current directory cannot be resolved or the config cannot be written.
fn bind_cwd_to_project(project: &str) -> Result<()> {
    let cwd = env::current_dir()?.canonicalize()?;
    let dir = cwd.to_string_lossy().to_string();

    let mut cfg = load_client_config();
    if let Some(existing) = cfg.paths.iter_mut().find(|b| same_dir(&b.dir, &dir)) {
        existing.project = project.to_string();
    } else {
        cfg.paths.push(PathBinding {
            dir,
            project: project.to_string(),
        });
    }
    save_client_config(cfg)?;
    Ok(())
}

/// Returns true if `a` and `b` reference the same directory path (ignoring a single trailing slash).
fn same_dir(a: &str, b: &str) -> bool {
    let na = a.trim_end_matches('/');
    na == b.trim_end_matches('/')
}

/// Resolves the most specific (longest) binding whose path is a prefix of `cwd`.
///
/// Returns `Some(project)` on match, otherwise `None`.
fn resolve_project_from_bindings(cfg: &ClientConfig, cwd: &Path) -> Option<String> {
    let cwd_s = cwd.to_string_lossy();
    let mut best: Option<&PathBinding> = None;
    for b in &cfg.paths {
        let pat = b.dir.trim_end_matches('/');
        if cwd_s.starts_with(pat) {
            best = match best {
                None => Some(b),
                Some(prev) if b.dir.len() > prev.dir.len() => Some(b),
                Some(prev) => Some(prev),
            };
        }
    }
    best.map(|b| b.project.clone())
}

/// ===== CLI =====

/// Top-level CLI entrypoint generated by `clap`.
///
/// Run `ww --help` for full usage.
#[derive(Parser, Debug)]
#[command(
    name = "ww",
    version,
    about = "Scylla daemon + CLI over a Unix socket - Doppler-like interface (global client config)",
    disable_help_subcommand = true
)]
struct Cli {
    /// Subcommand to execute. With no subcommand, prints usage and exits.
    #[command(subcommand)]
    command: Option<Commands>,
}

/// Supported subcommands for `ww`.
#[derive(Subcommand, Debug)]
enum Commands {
    /// Start the daemon server (connects to Scylla and listens on a Unix socket).
    ///
    /// Example:
    /// ```bash
    /// ww serve --node 127.0.0.1:9042 --user cassandra --pass cassandra --socket /tmp/ww.sock
    /// ```
    Serve {
        /// Scylla contact point (host:port).
        #[arg(long, default_value = "127.0.0.1:9042")]
        node: String,
        /// DB username.
        #[arg(long, default_value = "cassandra")]
        user: String,
        /// DB password.
        #[arg(long, default_value = "cassandra")]
        pass: String,
        /// Socket path for daemon (defaults to /tmp/ww.sock).
        #[arg(long, default_value = DEFAULT_SOCKET_PATH)]
        socket: String,
    },

    /// Run a command with **project secrets injected** as environment variables.
    ///
    /// If `--project` is omitted, the project is resolved from:
    /// 1. `--project` (if provided),
    /// 2. `WW_PROJECT` environment variable (if set),
    /// 3. path bindings in `~/.config/ww/config.toml`,
    /// 4. `default_project`.
    ///
    /// Example:
    /// ```bash
    /// ww run -- bash -lc 'env | sort'
    /// ```
    Run {
        /// Project name (overrides resolution).
        #[arg(short, long)]
        project: Option<String>,
        /// Socket path to talk to the daemon (defaults to /tmp/ww.sock).
        #[arg(long)]
        socket: Option<String>,
        /// Command to run (e.g., `node app.js`).
        #[arg(required = true)]
        command: Vec<String>,
    },

    /// Download or read secrets for the resolved project.
    ///
    /// `download` supports env/json/yaml output; `get` reads a single key.
    Secrets {
        #[command(subcommand)]
        action: SecretsAction,
    },

    /// Upsert a single env var for the project in the DB.
    ///
    /// Example:
    /// ```bash
    /// ww set-env --name API_URL --val https://example.com
    /// ```
    SetEnv {
        /// Project name (overrides resolution).
        #[arg(short, long)]
        project: Option<String>,
        /// Socket path to talk to the daemon (defaults to /tmp/ww.sock).
        #[arg(long)]
        socket: Option<String>,
        /// Env key to upsert.
        name: String,
        /// Env value to upsert.
        val: String,
    },

    /// Client config commands (GLOBAL config under ~/.config/ww/config.toml).
    Client {
        #[command(subcommand)]
        cmd: ClientCmd,
    },

    /// Treat any other invocation as: `ww run <resolved-project> <cmd...>`.
    ///
    /// This enables shorthand like:
    /// ```bash
    /// ww npm run dev
    /// ```
    #[command(external_subcommand)]
    External(Vec<String>),
}

/// Subcommands under `ww secrets`.
#[derive(Subcommand, Debug)]
enum SecretsAction {
    /// Download all project secrets in various formats.
    ///
    /// Example:
    /// ```bash
    /// ww secrets download --format env --output .env
    /// ```
    Download {
        /// Project name (overrides resolution).
        #[arg(short, long)]
        project: Option<String>,
        /// Socket path to talk to the daemon (defaults to /tmp/ww.sock).
        #[arg(long)]
        socket: Option<String>,
        /// Output format: `env`, `json`, or `yaml`.
        #[arg(long, default_value = "env")]
        format: String,
        /// Output file (defaults to stdout).
        #[arg(short, long)]
        output: Option<String>,
    },
    /// Get a single secret value.
    ///
    /// Example:
    /// ```bash
    /// ww secrets get --name API_KEY --plain
    /// ```
    Get {
        /// Project name (overrides resolution).
        #[arg(short, long)]
        project: Option<String>,
        /// Socket path to talk to the daemon (defaults to /tmp/ww.sock).
        #[arg(long)]
        socket: Option<String>,
        /// Secret name to read.
        name: String,
        /// Output only the plain value (no `KEY=value` prefix).
        #[arg(long)]
        plain: bool,
    },
}

/// Client-only subcommands (operate on global config).
#[derive(Subcommand, Debug)]
enum ClientCmd {
    /// Bind the **current directory** to a project and ensure it exists in DB.
    ///
    /// This writes to `~/.config/ww/config.toml` and then asks the daemon to
    /// create the project row if it does not exist.
    Setup {
        /// Project name to bind the current directory to.
        project: String,
        /// Socket path to talk to the daemon (defaults to /tmp/ww.sock).
        #[arg(long)]
        socket: Option<String>,
    },
    /// Print the project name that would be resolved for the current directory.
    Which,
}

/// ===== Entry Point =====

/// Program entrypoint. Parses CLI and dispatches to subcommands.
///
/// The function returns only on error paths; many branches `std::process::exit`
/// explicitly to propagate child exit codes or usage failures.
#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Some(Commands::Serve {
            node,
            user,
            pass,
            socket,
        }) => serve(&node, &user, &pass, &socket).await,

        Some(Commands::Run {
            project,
            socket,
            command,
        }) => {
            let project = resolve_project(project)?;
            let socket_path = socket.unwrap_or_else(|| DEFAULT_SOCKET_PATH.to_string());
            run_with_env_using_socket(&project, command, &socket_path).await
        }

        Some(Commands::Secrets { action }) => match action {
            SecretsAction::Download {
                project,
                socket,
                format,
                output,
            } => {
                let project = resolve_project(project)?;
                let socket_path = socket.unwrap_or_else(|| DEFAULT_SOCKET_PATH.to_string());
                let env_vars = fetch_project_env_vars_over_socket(&project, &socket_path).await?;
                let content = format_secrets(&env_vars, &format)?;
                if let Some(file_path) = output {
                    tokio::fs::write(&file_path, content)
                        .await
                        .with_context(|| format!("Failed to write to {}", file_path))?;
                    eprintln!("Secrets downloaded to {}", file_path);
                } else {
                    print!("{}", content);
                }
                Ok(())
            }
            SecretsAction::Get {
                project,
                socket,
                name,
                plain,
            } => {
                let project = resolve_project(project)?;
                let socket_path = socket.unwrap_or_else(|| DEFAULT_SOCKET_PATH.to_string());
                let env_vars = fetch_project_env_vars_over_socket(&project, &socket_path).await?;
                match env_vars.get(&name) {
                    Some(value) => {
                        if plain {
                            print!("{}", value);
                        } else {
                            println!("{}={}", name, value);
                        }
                    }
                    None => {
                        eprintln!("Secret '{}' not found in project '{}'", name, project);
                        std::process::exit(1);
                    }
                }
                Ok(())
            }
        },

        Some(Commands::SetEnv {
            project,
            socket,
            name,
            val,
        }) => {
            let project = resolve_project(project)?;
            let socket_path = socket.unwrap_or_else(|| DEFAULT_SOCKET_PATH.to_string());
            ensure_project_over_socket(&project, &socket_path).await?; // ensure exists
            set_env_over_socket(&project, &name, &val, &socket_path).await?;
            eprintln!("Set {} for project {}", name, project);
            Ok(())
        }

        Some(Commands::Client { cmd }) => match cmd {
            ClientCmd::Setup { project, socket } => {
                bind_cwd_to_project(&project)?;
                let socket_path = socket.unwrap_or_else(|| DEFAULT_SOCKET_PATH.to_string());
                ensure_project_over_socket(&project, &socket_path).await?;
                let path = config_file_path()?;
                eprintln!(
                    "Bound current directory to project '{}' in {}\nProject ensured in DB.",
                    project,
                    path.display()
                );
                Ok(())
            }
            ClientCmd::Which => {
                let p = resolve_project(None)?;
                println!("{}", p);
                Ok(())
            }
        },

        Some(Commands::External(cmd)) => {
            if cmd.is_empty() {
                eprintln!("Error: No command specified");
                std::process::exit(1);
            }
            let project = resolve_project(None)?;
            run_with_env_using_socket(&project, cmd, DEFAULT_SOCKET_PATH).await
        }

        None => {
            eprintln!("Usage: ww <command> [args]\nTry --help for more info.");
            std::process::exit(2);
        }
    }
}

/// ===== Project Resolution =====

/// Resolves the project to operate on, using (in order):
///
/// 1. `cli_project` if provided,
/// 2. the `WW_PROJECT` environment variable if non-empty,
/// 3. a path binding for the current directory,
/// 4. the config’s `default_project`.
///
/// # Errors
///
/// - Returns errors from filesystem operations (e.g., resolving `cwd`).
fn resolve_project(cli_project: Option<String>) -> Result<String> {
    if let Some(p) = cli_project {
        return Ok(p);
    }
    if let Ok(p) = env::var("WW_PROJECT") {
        if !p.trim().is_empty() {
            return Ok(p);
        }
    }
    let cfg = load_client_config();
    let cwd = env::current_dir()?.canonicalize()?;
    if let Some(p) = resolve_project_from_bindings(&cfg, &cwd) {
        return Ok(p);
    }
    Ok(cfg.default_project)
}

/// ===== Run + Secrets =====

/// Spawns `command` with all env vars for `project` injected from the daemon.
///
/// This function **execs a child process** and then exits the current process
/// with the child’s status code.
///
/// # Errors
///
/// - Fails if secrets cannot be fetched or the command cannot be started.
async fn run_with_env_using_socket(
    project: &str,
    command: Vec<String>,
    socket_path: &str,
) -> Result<()> {
    if command.is_empty() {
        eprintln!("Error: No command specified");
        std::process::exit(1);
    }

    let env_vars = fetch_project_env_vars_over_socket(project, socket_path).await?;

    let (cmd, args) = command.split_first().unwrap();
    let mut child = Command::new(cmd);
    child.args(args);

    for (key, value) in env_vars {
        child.env(key, value);
    }

    let status = child.status().context("Failed to execute command")?;
    std::process::exit(status.code().unwrap_or(1));
}

/// Serializes a map of env vars into one of the supported formats.
///
/// - `env`: `KEY=VALUE` pairs separated by newlines,
/// - `json`: pretty JSON object,
/// - `yaml`: YAML mapping.
///
/// # Errors
///
/// - Returns an error for unsupported formats or serialization failures.
fn format_secrets(env_vars: &HashMap<String, String>, format: &str) -> Result<String> {
    match format {
        "env" => {
            let mut result = String::new();
            for (key, value) in env_vars {
                result.push_str(&format!("{}={}\n", key, value));
            }
            Ok(result)
        }
        "json" => Ok(serde_json::to_string_pretty(env_vars)?),
        "yaml" => Ok(serde_yaml::to_string(env_vars)?),
        _ => Err(anyhow!("Unsupported format: {}", format)),
    }
}

/// ===== Daemon (Server) =====

/// Starts the daemon: connects to Scylla, ensures keyspace, and listens on a Unix socket.
///
/// This is a long-running async loop that accepts socket connections and handles
/// simple line-based requests. Each connection is served on a Tokio task.
///
/// # Parameters
///
/// - `node`: Scylla contact point (`host:port`)
/// - `user` / `pass`: Scylla credentials
/// - `socket_path`: Unix socket path to bind
///
/// # Errors
///
/// - Any connection, query, or socket binding error bubbles up as `anyhow::Error`.
async fn serve(node: &str, user: &str, pass: &str, socket_path: &str) -> Result<()> {
    let t0 = Instant::now();
    let mut builder = SessionBuilder::new().known_node(node);
    builder = builder.user(user, pass);

    let session: Arc<Session> = Arc::new(builder.build().await.context("build session")?);
    eprintln!("Connected to Scylla in {:?}.", t0.elapsed());

    session
        .use_keyspace("ww", false)
        .await
        .context("use keyspace")?;

    if Path::new(socket_path).exists() {
        tokio::fs::remove_file(socket_path).await.ok();
    }
    let listener =
        UnixListener::bind(socket_path).with_context(|| format!("bind {}", socket_path))?;
    eprintln!("Daemon listening on {}", socket_path);

    // Tighten permissions: rw for owner only
    #[cfg(unix)]
    {
        let perms = std::fs::Permissions::from_mode(0o600);
        tokio::fs::set_permissions(socket_path, perms).await.ok();
    }

    loop {
        let (mut stream, _addr) = listener.accept().await?;

        let session = Arc::clone(&session);

        tokio::spawn(async move {
            let mut buf = vec![0u8; 4096];
            let n = match stream.read(&mut buf).await {
                Ok(n) if n > 0 => n,
                _ => return,
            };
            let req = String::from_utf8_lossy(&buf[..n]).trim().to_string();

            // Protocol:
            // - "QUERY_PROJECTS:<project>"
            // - "SET_ENV:<project>:<key>=<value>"
            // - "ENSURE_PROJECT:<project>"
            let reply = if let Some(project) = req.strip_prefix("QUERY_PROJECTS:") {
                match run_projects_query(&*session, project).await {
                    Ok(text) => text,
                    Err(e) => format!("ERROR: {e}\n"),
                }
            } else if let Some(rest) = req.strip_prefix("SET_ENV:") {
                match handle_set_env(&*session, rest).await {
                    Ok(()) => "OK\n".to_string(),
                    Err(e) => format!("ERROR: {e}\n"),
                }
            } else if let Some(project) = req.strip_prefix("ENSURE_PROJECT:") {
                match ensure_project_exists(&*session, project).await {
                    Ok(()) => "OK\n".to_string(),
                    Err(e) => format!("ERROR: {e}\n"),
                }
            } else if req == "QUERY_PROJECTS" {
                // Back-compat or test path: default project
                match run_projects_query(&*session, DEFAULT_PROJECT).await {
                    Ok(text) => text,
                    Err(e) => format!("ERROR: {e}\n"),
                }
            } else {
                "UNKNOWN COMMAND\n".to_string()
            };

            let _ = stream.write_all(reply.as_bytes()).await;
        });
    }
}

/// Row mapping for Scylla `ww.projects` query.
#[derive(DeserializeRow)]
struct wwVars {
    /// Map of environment variables for the project.
    env_variables: HashMap<String, String>,
}

/// Returns all env variables for `project_name` as `KEY=VALUE\n` text.
///
/// If the project does not exist, it is created with an empty map and an empty
/// string is returned (no keys).
async fn run_projects_query(session: &Session, project_name: &str) -> Result<String> {
    // If project doesn't exist, create it with an empty map and return empty.
    if !project_exists(session, project_name).await? {
        ensure_project_exists(session, project_name).await?;
        return Ok(String::new());
    }

    let stmt = session
        .prepare("SELECT env_variables FROM ww.projects WHERE name = ?")
        .await?;
    let pager = session.execute_unpaged(&stmt, (project_name,)).await?;
    let rows = pager.into_rows_result()?;

    let mut out = String::new();
    for row in rows.rows::<wwVars>()? {
        let vars: wwVars = row?;
        for (key, value) in vars.env_variables.iter() {
            out.push_str(&format!("{}={}\n", key, value));
        }
    }
    Ok(out)
}

/// Checks whether a project row exists.
///
/// Returns `Ok(true)` if present, `Ok(false)` if absent.
async fn project_exists(session: &Session, name: &str) -> Result<bool> {
    // Prepare and execute your query
    let stmt = session
        .prepare("SELECT name FROM ww.projects WHERE name = ?")
        .await?;
    let qr = session.execute_unpaged(&stmt, (name,)).await?;

    // Convert to a rows result
    let rows_result = qr.into_rows_result()?;

    // Use maybe_first_row — returns Option<RowT> (None if no row)
    let exists = rows_result.maybe_first_row::<(String,)>()?.is_some();
    Ok(exists)
}

/// Ensures a project exists by inserting an empty row if missing.
///
/// This operation is idempotent.
async fn ensure_project_exists(session: &Session, name: &str) -> Result<()> {
    if project_exists(session, name).await? {
        return Ok(());
    }
    let stmt = session
        .prepare("INSERT INTO ww.projects (name, env_variables) VALUES (?, ?)")
        .await?;
    let empty: HashMap<String, String> = HashMap::new();
    session.execute_unpaged(&stmt, (name, empty)).await?;
    Ok(())
}

/// Parses a `SET_ENV` request and upserts the single key in the map column.
///
/// Input format: `"<project>:<key>=<value>"`.
async fn handle_set_env(session: &Session, rest: &str) -> Result<()> {
    // rest format: "<project>:<key>=<value>"
    let (project, kv) = rest
        .split_once(':')
        .ok_or_else(|| anyhow!("malformed SET_ENV (missing project separator)"))?;
    let (key, value) = kv
        .split_once('=')
        .ok_or_else(|| anyhow!("malformed SET_ENV (expected key=value)"))?;

    // Ensure project exists, then upsert env var into map column
    ensure_project_exists(session, project).await?;

    let stmt = session
        .prepare(
            "UPDATE ww.projects SET env_variables[?] = ? \
             WHERE name = ?",
        )
        .await?;
    session
        .execute_unpaged(&stmt, (key, value, project))
        .await?;
    Ok(())
}

/// ===== Socket Client Helpers =====

/// Fetches project env variables via the Unix socket.
///
/// Returns a `HashMap<String, String>` parsed from the daemon response.
///
/// # Errors
///
/// - I/O errors on the socket,
/// - Protocol errors returned by the daemon (`ERROR:` lines).
async fn fetch_project_env_vars_over_socket(
    project: &str,
    socket_path: &str,
) -> Result<HashMap<String, String>> {
    let request_msg = format!("QUERY_PROJECTS:{}\n", project);
    let response = request_over_socket(&request_msg, socket_path).await?;
    if response.starts_with("ERROR:") {
        return Err(anyhow!(response.trim().to_string()));
    }

    let mut env_vars = HashMap::new();
    for line in response.lines() {
        if let Some((key, value)) = line.split_once('=') {
            env_vars.insert(key.to_string(), value.to_string());
        }
    }
    Ok(env_vars)
}

/// Asks the daemon to ensure a project exists (idempotent).
async fn ensure_project_over_socket(project: &str, socket_path: &str) -> Result<()> {
    let msg = format!("ENSURE_PROJECT:{}\n", project);
    let response = request_over_socket(&msg, socket_path).await?;
    if response.starts_with("ERROR:") {
        Err(anyhow!(response.trim().to_string()))
    } else {
        Ok(())
    }
}

/// Sends a single `SET_ENV` operation to the daemon.
///
/// Minimal validation is performed on `name` and `project` to avoid delimiter
/// collisions with the line protocol.
async fn set_env_over_socket(
    project: &str,
    name: &str,
    val: &str,
    socket_path: &str,
) -> Result<()> {
    // Minimal validation for this simple line protocol
    if name.contains(['\n', ':', '=']) || project.contains(['\n', ':']) {
        return Err(anyhow!(
            "invalid characters in name/project (no newline, ':', '=' allowed)"
        ));
    }
    let msg = format!("SET_ENV:{}:{}={}\n", project, name, val);
    let response = request_over_socket(&msg, socket_path).await?;
    if response.starts_with("ERROR:") {
        Err(anyhow!(response.trim().to_string()))
    } else {
        Ok(())
    }
}

/// Sends a single request and reads the **entire** response until EOF.
///
/// # Protocol note
///
/// The server closes the connection after writing a single response; therefore
/// a simple `read_to_end` suffices here.
///
/// # Errors
///
/// - Returns any socket or UTF-8 error encountered.
async fn request_over_socket(msg: &str, socket_path: &str) -> Result<String> {
    let mut stream = UnixStream::connect(socket_path)
        .await
        .with_context(|| format!("daemon not reachable at {socket_path}. Is it running?"))?;
    stream.write_all(msg.as_bytes()).await?;
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await?;
    Ok(String::from_utf8_lossy(&buf).to_string())
}

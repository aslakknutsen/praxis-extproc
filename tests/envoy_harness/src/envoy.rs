//! Spawn and manage a local Envoy process.
//!
//! Bootstrap templates must include these placeholders (substituted at spawn):
//! `{{LISTENER_PORT}}`, `{{ADMIN_PORT}}`, `{{EXTPROC_PORT}}`, `{{BACKEND_PORT}}`,
//! `{{ACCESS_LOG_PATH}}`. Optional: `{{COMPONENT_LOG_LEVEL}}`.

use std::{
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    net::TcpStream,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::atomic::{AtomicU32, Ordering},
    time::{Duration, Instant},
};

use tempfile::TempDir;

static BASE_ID: AtomicU32 = AtomicU32::new(1);

/// Default Envoy bootstrap used when the builder does not override it.
pub const DEFAULT_ENVOY_BOOTSTRAP: &str = include_str!("../templates/envoy.base.yaml");

/// Ports, paths, and bootstrap template used to render Envoy config.
pub struct EnvoyRenderParams<'a> {
    /// Downstream HTTP listener port.
    pub listener_port: u16,
    /// Admin interface port.
    pub admin_port: u16,
    /// ExtProc gRPC port (Praxis server).
    pub extproc_port: u16,
    /// Upstream backend port.
    pub backend_port: u16,
    /// `--component-log-level` value.
    pub component_log_level: String,
    /// Access log file path.
    pub access_log_path: PathBuf,
    /// Envoy bootstrap YAML template (with placeholders).
    pub bootstrap_template: &'a str,
}

/// Running Envoy process plus log/config artifacts.
pub struct EnvoyProcess {
    child: Option<Child>,
    /// Admin port.
    pub admin_port: u16,
    /// Listener port.
    pub listener_port: u16,
    stderr_path: PathBuf,
    _workdir: TempDir,
}

impl std::fmt::Debug for EnvoyProcess {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EnvoyProcess")
            .field("admin_port", &self.admin_port)
            .field("listener_port", &self.listener_port)
            .finish_non_exhaustive()
    }
}

impl Drop for EnvoyProcess {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl EnvoyProcess {
    /// Terminate Envoy.
    pub fn shutdown(&mut self) {
        let Some(mut child) = self.child.take() else {
            return;
        };
        let _ = child.kill();
        let _ = child.wait();
    }

    /// Whether captured stderr contains `needle`.
    pub fn log_contains(&self, needle: &str) -> bool {
        self.stderr().contains(needle)
    }

    /// Full stderr capture (best-effort).
    pub fn stderr(&self) -> String {
        let mut file = match File::open(&self.stderr_path) {
            Ok(f) => f,
            Err(e) => return format!("<failed to read stderr: {e}>"),
        };
        let mut buf = String::new();
        let _ = file.seek(SeekFrom::Start(0));
        let _ = file.read_to_string(&mut buf);
        buf
    }
}

/// Resolve the Envoy binary path from `ENVOY_BIN`.
///
/// # Panics
///
/// Panics if `ENVOY_BIN` is unset or not a file. Outside CI, prints a hint first.
pub fn require_envoy_bin() -> PathBuf {
    match std::env::var_os("ENVOY_BIN") {
        Some(p) => {
            let path = PathBuf::from(p);
            if path.is_file() {
                return path;
            }
            fail_missing_envoy(&format!("ENVOY_BIN={} is not a file", path.display()));
        },
        None => fail_missing_envoy("ENVOY_BIN is unset; run `make ensure-envoy`"),
    }
}

fn fail_missing_envoy(msg: &str) -> ! {
    if std::env::var_os("CI").is_none() {
        eprintln!("envoy e2e unavailable: {msg}");
    }
    panic!("{msg}");
}

/// Whether `ENVOY_BIN` points at an existing file.
pub fn envoy_available() -> bool {
    std::env::var_os("ENVOY_BIN")
        .map(PathBuf::from)
        .is_some_and(|p| p.is_file())
}

/// Render the bootstrap template into a temp file and spawn Envoy.
///
/// # Panics
///
/// Panics if Envoy cannot be spawned or does not become ready.
pub fn spawn_envoy(params: &EnvoyRenderParams<'_>) -> EnvoyProcess {
    let bin = require_envoy_bin();
    let workdir = tempfile::tempdir().expect("envoy workdir");
    let config_path = workdir.path().join("envoy.yaml");
    let stderr_path = workdir.path().join("envoy.stderr");
    let stdout_path = workdir.path().join("envoy.stdout");

    let yaml = params
        .bootstrap_template
        .replace("{{LISTENER_PORT}}", &params.listener_port.to_string())
        .replace("{{ADMIN_PORT}}", &params.admin_port.to_string())
        .replace("{{EXTPROC_PORT}}", &params.extproc_port.to_string())
        .replace("{{BACKEND_PORT}}", &params.backend_port.to_string())
        .replace("{{COMPONENT_LOG_LEVEL}}", &params.component_log_level)
        .replace("{{ACCESS_LOG_PATH}}", &params.access_log_path.display().to_string());

    std::fs::write(&config_path, yaml).expect("write envoy.yaml");

    let stderr_file = File::create(&stderr_path).expect("create stderr file");
    let stdout_file = File::create(&stdout_path).expect("create stdout file");
    let base_id = BASE_ID.fetch_add(1, Ordering::Relaxed);

    let child = Command::new(&bin)
        .arg("-c")
        .arg(&config_path)
        .arg("--base-id")
        .arg(base_id.to_string())
        .arg("--concurrency")
        .arg("1")
        .arg("-l")
        .arg("warning")
        .arg("--component-log-level")
        .arg(&params.component_log_level)
        .stdout(Stdio::from(stdout_file))
        .stderr(Stdio::from(stderr_file))
        .spawn()
        .unwrap_or_else(|e| panic!("spawn envoy {}: {e}", bin.display()));

    let mut proc = EnvoyProcess {
        child: Some(child),
        admin_port: params.admin_port,
        listener_port: params.listener_port,
        stderr_path,
        _workdir: workdir,
    };

    wait_for_ready(params.admin_port, &mut proc);
    proc
}

fn wait_for_ready(admin_port: u16, proc: &mut EnvoyProcess) {
    let deadline = Instant::now() + Duration::from_secs(10);

    while Instant::now() < deadline {
        if let Some(child) = proc.child.as_mut()
            && let Ok(Some(status)) = child.try_wait()
        {
            panic!("envoy exited early with {status}; stderr:\n{}", proc.stderr());
        }
        if admin_ready(admin_port) {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    panic!("envoy admin /ready not ready within 10s; stderr:\n{}", proc.stderr());
}

fn admin_ready(admin_port: u16) -> bool {
    let Ok(mut stream) = TcpStream::connect(("127.0.0.1", admin_port)) else {
        return false;
    };
    let _ = stream.set_read_timeout(Some(Duration::from_millis(300)));
    let _ = stream.set_write_timeout(Some(Duration::from_millis(300)));
    let req = format!("GET /ready HTTP/1.1\r\nHost: 127.0.0.1:{admin_port}\r\nConnection: close\r\n\r\n");
    if stream.write_all(req.as_bytes()).is_err() {
        return false;
    }
    let mut buf = Vec::new();
    let _ = stream.read_to_end(&mut buf);
    let text = String::from_utf8_lossy(&buf);
    text.contains("HTTP/1.1 200") || text.contains("HTTP/1.0 200")
}

/// Create an empty access log file under `dir`.
pub fn access_log_path(dir: &Path) -> PathBuf {
    let path = dir.join("access.log");
    std::fs::write(&path, "").expect("create access log");
    path
}

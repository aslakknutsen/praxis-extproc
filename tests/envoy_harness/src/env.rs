//! High-level Envoy + Praxis ExtProc server + backend test environment.
//!
//! - **Praxis YAML** (`praxis_yaml`): filter chains for the in-process Praxis server.
//! - **Envoy bootstrap** (`envoy_bootstrap`): Envoy static config template. Must include `{{LISTENER_PORT}}`,
//!   `{{ADMIN_PORT}}`, `{{EXTPROC_PORT}}`, `{{BACKEND_PORT}}`, `{{ACCESS_LOG_PATH}}` (see [`crate::envoy`]).

use std::{path::PathBuf, time::Duration};

use crate::{
    backend::{RecordingBackend, RequestLog, start_recording_backend},
    envoy::{DEFAULT_ENVOY_BOOTSTRAP, EnvoyProcess, EnvoyRenderParams, access_log_path, envoy_available, spawn_envoy},
    extproc::{ExtProcHandle, start_extproc},
    ports::free_port_guard,
    stats::{counter_value, fetch_stats},
};

/// Builder for [`EnvoyEnv`].
pub struct EnvoyEnvBuilder {
    praxis_yaml: String,
    backend_body: String,
    component_log_level: String,
    envoy_bootstrap: Option<String>,
}

impl EnvoyEnvBuilder {
    /// Start from a Praxis filter-chain YAML document.
    pub fn new(praxis_yaml: impl Into<String>) -> Self {
        Self {
            praxis_yaml: praxis_yaml.into(),
            backend_body: "ok".to_owned(),
            component_log_level: "ext_proc:debug".to_owned(),
            envoy_bootstrap: None,
        }
    }

    /// Fixed backend response body.
    #[must_use]
    pub fn backend_body(mut self, body: impl Into<String>) -> Self {
        self.backend_body = body.into();
        self
    }

    /// Envoy `--component-log-level`.
    #[must_use]
    pub fn component_log_level(mut self, level: impl Into<String>) -> Self {
        self.component_log_level = level.into();
        self
    }

    /// Override the Envoy bootstrap YAML template (placeholders required; see module docs).
    #[must_use]
    pub fn envoy_bootstrap(mut self, yaml: impl Into<String>) -> Self {
        self.envoy_bootstrap = Some(yaml.into());
        self
    }

    /// Spawn backend, Praxis ExtProc server, and Envoy.
    ///
    /// # Panics
    ///
    /// Panics if Envoy is unavailable or any component fails to start.
    pub async fn start(self) -> EnvoyEnv {
        if !envoy_available() {
            if std::env::var_os("CI").is_some() {
                panic!("ENVOY_BIN missing in CI; run make ensure-envoy");
            }
            panic!("ENVOY_BIN missing; run `make ensure-envoy` and export ENVOY_BIN");
        }

        let backend = start_recording_backend(&self.backend_body).await;
        let extproc = start_extproc(&self.praxis_yaml).await;

        let listener_guard = free_port_guard();
        let admin_guard = free_port_guard();
        let listener_port = listener_guard.port();
        let admin_port = admin_guard.port();

        let workdir = tempfile::tempdir().expect("env workdir");
        let access_log = access_log_path(workdir.path());

        // Release listener/admin ports immediately before Envoy binds them.
        drop(listener_guard);
        drop(admin_guard);

        let bootstrap = self.envoy_bootstrap.as_deref().unwrap_or(DEFAULT_ENVOY_BOOTSTRAP);

        let envoy = spawn_envoy(&EnvoyRenderParams {
            listener_port,
            admin_port,
            extproc_port: extproc.port,
            backend_port: backend.port,
            component_log_level: self.component_log_level,
            access_log_path: access_log,
            bootstrap_template: bootstrap,
        });

        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .expect("http client");

        EnvoyEnv {
            http,
            backend,
            extproc,
            envoy,
            _workdir: workdir,
        }
    }
}

/// Running local Envoy e2e stack.
pub struct EnvoyEnv {
    http: reqwest::Client,
    backend: RecordingBackend,
    extproc: ExtProcHandle,
    envoy: EnvoyProcess,
    _workdir: tempfile::TempDir,
}

impl std::fmt::Debug for EnvoyEnv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EnvoyEnv")
            .field("listener_port", &self.envoy.listener_port)
            .field("admin_port", &self.envoy.admin_port)
            .field("extproc_port", &self.extproc.port)
            .field("backend_port", &self.backend.port)
            .finish_non_exhaustive()
    }
}

impl EnvoyEnv {
    /// Shared HTTP client.
    pub fn http(&self) -> &reqwest::Client {
        &self.http
    }

    /// Absolute URL for a path on the Envoy listener.
    pub fn url(&self, path: &str) -> String {
        let path = if path.starts_with('/') {
            path.to_owned()
        } else {
            format!("/{path}")
        };
        format!("http://127.0.0.1:{}{path}", self.envoy.listener_port)
    }

    /// Absolute admin URL for a path.
    pub fn admin_url(&self, path: &str) -> String {
        let path = if path.starts_with('/') {
            path.to_owned()
        } else {
            format!("/{path}")
        };
        format!("http://127.0.0.1:{}{path}", self.envoy.admin_port)
    }

    /// Captured upstream requests.
    pub fn backend(&self) -> &RequestLog {
        &self.backend.log
    }

    /// Path to the Praxis filter-chain config file.
    pub fn praxis_config_path(&self) -> &PathBuf {
        &self.extproc.config_path
    }

    /// Overwrite Praxis config on disk (for hot-reload tests).
    pub async fn overwrite_praxis_config(&self, yaml: &str) {
        self.extproc.overwrite_config(yaml).await;
    }

    /// Whether Envoy stderr contains `needle`.
    pub fn envoy_log_contains(&self, needle: &str) -> bool {
        self.envoy.log_contains(needle)
    }

    /// Fetch a named counter from Envoy admin stats.
    pub async fn stats_counter(&self, name: &str) -> Option<u64> {
        let stats = fetch_stats(self.envoy.admin_port).await;
        counter_value(&stats, name)
    }
}

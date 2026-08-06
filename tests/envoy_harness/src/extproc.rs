//! In-process ExtProc gRPC server for Envoy e2e tests.

use std::{path::PathBuf, time::Duration};

use praxis_extproc::{config, server::PraxisExtProc};
use praxis_proto::envoy::service::ext_proc::v3::external_processor_server::ExternalProcessorServer;
use tokio::net::TcpListener;
use tonic::transport::Server;

use crate::ports::free_port_guard;

/// Running in-process ExtProc server backed by a Praxis config file on disk.
pub struct ExtProcHandle {
    /// gRPC listen port.
    pub port: u16,
    /// Path to the Praxis filter-chain YAML (rewritable for reload tests).
    pub config_path: PathBuf,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    _temp_dir: tempfile::TempDir,
}

impl std::fmt::Debug for ExtProcHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExtProcHandle")
            .field("port", &self.port)
            .field("config_path", &self.config_path)
            .finish_non_exhaustive()
    }
}

impl Drop for ExtProcHandle {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}

impl ExtProcHandle {
    /// Overwrite the on-disk Praxis config (hot-reload contract for #17).
    pub async fn overwrite_config(&self, yaml: &str) {
        tokio::fs::write(&self.config_path, yaml)
            .await
            .unwrap_or_else(|e| panic!("write {}: {e}", self.config_path.display()));
    }
}

/// Start the ExtProc gRPC server from Praxis filter-chain YAML.
pub async fn start_extproc(praxis_yaml: &str) -> ExtProcHandle {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let config_path = temp_dir.path().join("praxis.yaml");
    std::fs::write(&config_path, praxis_yaml).expect("write initial Praxis config");

    let cfg: config::ExtProcConfig = serde_yaml::from_str(praxis_yaml).expect("parse Praxis config");
    let registry = praxis_ai_filters::build_ai_registry();
    let pipeline = config::build_pipeline(&cfg, &registry).expect("build pipeline");

    let port = free_port_guard().release();
    let listener = TcpListener::bind(("127.0.0.1", port))
        .await
        .unwrap_or_else(|e| panic!("bind extproc on {port}: {e}"));

    let svc = PraxisExtProc::new(pipeline);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    tokio::spawn(async move {
        Server::builder()
            .add_service(ExternalProcessorServer::new(svc))
            .serve_with_incoming_shutdown(tokio_stream::wrappers::TcpListenerStream::new(listener), async {
                drop(shutdown_rx.await);
            })
            .await
            .expect("extproc server failed");
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    ExtProcHandle {
        port,
        config_path,
        shutdown: Some(shutdown_tx),
        _temp_dir: temp_dir,
    }
}

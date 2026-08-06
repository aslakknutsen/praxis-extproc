// SPDX-License-Identifier: MIT
//! Binary entry point for the Praxis ExtProc server.

#![deny(unsafe_code)]
#![deny(unreachable_pub)]

use std::{process, sync::Arc};

use arc_swap::ArcSwap;
use clap::Parser;
use praxis_extproc::{
    config::{self, ExtProcConfig},
    server::PraxisExtProc,
    tls,
    watcher::{self, WatcherParams},
};
use praxis_proto::envoy::service::ext_proc::v3::external_processor_server::ExternalProcessorServer;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tracing::{error, info};

// -----------------------------------------------------------------------------
// CLI
// -----------------------------------------------------------------------------

/// Praxis ExtProc server: run Praxis filter pipelines as an Envoy
/// external processor.
#[derive(Debug, Parser)]
#[command(name = "praxis-extproc", version, about)]
struct Cli {
    /// Path to the YAML configuration file.
    #[arg(short, long, default_value = "praxis-extproc.yaml")]
    config: String,

    /// Override the gRPC listen address.
    #[arg(long)]
    grpc_address: Option<String>,

    /// Override the health check listen address.
    #[arg(long)]
    health_address: Option<String>,

    /// Override the metrics listen address.
    #[arg(long)]
    metrics_address: Option<String>,

    /// Validate configuration and exit.
    #[arg(short = 't', long)]
    validate: bool,
}

// -----------------------------------------------------------------------------
// Main
// -----------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    init_tracing();
    let cli = Cli::parse();

    if let Err(e) = Box::pin(run(cli)).await {
        error!(error = %e, "fatal");
        process::exit(1);
    }
}

// -----------------------------------------------------------------------------
// Startup
// -----------------------------------------------------------------------------

/// Top-level application logic.
async fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    let content = std::fs::read_to_string(&cli.config)?;
    let cfg = config::parse_config(&content)?;
    let registry = Arc::new(praxis_ai_filters::build_ai_registry());
    let pipeline = config::build_pipeline(&cfg, &registry)?;
    let content_hash = watcher::hash_content(&content);

    if cli.validate {
        info!("configuration is valid");
        return Ok(());
    }

    let addrs = resolve_addresses(&cli, &cfg)?;
    let pipelines = Arc::new(ArcSwap::from(pipeline));

    info!(
        grpc = %addrs.0, health = %addrs.1,
        metrics = %addrs.2, filters = pipelines.load().len(),
        "starting ExtProc server"
    );

    let watcher_shutdown = CancellationToken::new();
    let _watcher = watcher::spawn_config_watcher(WatcherParams {
        config_path: std::path::PathBuf::from(&cli.config),
        initial_content_hash: content_hash,
        initial_server: cfg.server.clone(),
        pipelines: Arc::clone(&pipelines),
        registry: Arc::clone(&registry),
        shutdown: watcher_shutdown.clone(),
    });

    let result = Box::pin(start_services(addrs, pipelines, &cfg.server.tls)).await;
    watcher_shutdown.cancel();
    result
}

/// Start gRPC, health, and metrics servers concurrently.
#[expect(
    clippy::large_stack_frames,
    clippy::cognitive_complexity,
    reason = "async state machine for server startup"
)]
async fn start_services(
    addrs: (std::net::SocketAddr, std::net::SocketAddr, std::net::SocketAddr),
    pipelines: praxis_extproc::server::PipelineSlot,
    tls_cfg: &tls::TlsConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);

    let health_rx = shutdown_tx.subscribe();
    let health_handle =
        tokio::spawn(async move { praxis_extproc::health::serve(addrs.1, wait_broadcast(health_rx)).await });

    let metrics_rx = shutdown_tx.subscribe();
    let metrics_handle =
        tokio::spawn(async move { praxis_extproc::metrics::serve(addrs.2, wait_broadcast(metrics_rx)).await });

    serve_grpc(addrs.0, pipelines, tls_cfg).await?;

    drop(shutdown_tx);

    if let Err(e) = health_handle.await {
        error!(error = %e, "health server task failed");
    }
    if let Err(e) = metrics_handle.await {
        error!(error = %e, "metrics server task failed");
    }

    info!("server shut down");
    Ok(())
}

/// Start the main gRPC ExtProc server.
async fn serve_grpc(
    addr: std::net::SocketAddr,
    pipelines: praxis_extproc::server::PipelineSlot,
    tls_cfg: &tls::TlsConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let svc = PraxisExtProc::new(pipelines);
    let tls = tls::build_tls_config(tls_cfg)?;

    let mut builder = Server::builder();
    if let Some(tls_config) = tls {
        builder = builder.tls_config(tls_config)?;
    }

    builder
        .add_service(ExternalProcessorServer::new(svc))
        .serve_with_shutdown(addr, shutdown_signal())
        .await?;

    Ok(())
}

// -----------------------------------------------------------------------------
// Shutdown
// -----------------------------------------------------------------------------

/// Wait for SIGTERM or SIGINT for graceful shutdown.
#[expect(
    clippy::cognitive_complexity,
    reason = "platform-specific signal select is intentionally inline"
)]
async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();

    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let Ok(mut sigterm) = signal(SignalKind::terminate()) else {
            error!("failed to install SIGTERM handler");
            return;
        };

        tokio::select! {
            _ = ctrl_c => info!("received SIGINT"),
            _ = sigterm.recv() => info!("received SIGTERM"),
        }
    }

    #[cfg(not(unix))]
    {
        if ctrl_c.await.is_err() {
            error!("ctrl-c handler failed");
        } else {
            info!("received SIGINT");
        }
    }
}

// -----------------------------------------------------------------------------
// Utilities
// -----------------------------------------------------------------------------

/// Initialize the tracing subscriber with env-filter support.
fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
}

/// Resolve all three listen addresses from CLI overrides or config.
fn resolve_addresses(
    cli: &Cli,
    cfg: &ExtProcConfig,
) -> Result<(std::net::SocketAddr, std::net::SocketAddr, std::net::SocketAddr), Box<dyn std::error::Error>> {
    let grpc = parse_addr(&cli.grpc_address, &cfg.server.grpc_address)?;
    let health = parse_addr(&cli.health_address, &cfg.server.health_address)?;
    let metrics = parse_addr(&cli.metrics_address, &cfg.server.metrics_address)?;
    Ok((grpc, health, metrics))
}

/// Wait for a broadcast shutdown signal.
async fn wait_broadcast(mut rx: tokio::sync::broadcast::Receiver<()>) {
    drop(rx.recv().await);
}

/// Parse a socket address from CLI override or config default.
fn parse_addr(
    cli_override: &Option<String>,
    config_default: &str,
) -> Result<std::net::SocketAddr, Box<dyn std::error::Error>> {
    let s = cli_override.as_deref().unwrap_or(config_default);
    Ok(s.parse()?)
}

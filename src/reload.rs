// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Shane Utt

//! Atomic filter-pipeline reload with last-known-good retention.
//!
//! Parse and build a new pipeline before touching the live
//! [`ArcSwap`] slot. On failure the previous generation stays
//! in place. Listen / TLS field changes are logged as
//! restart-required and are not applied to running servers.
//!
//! [`ArcSwap`]: arc_swap::ArcSwap

use arc_swap::ArcSwap;
use praxis_filter::{FilterPipeline, FilterRegistry};
use tracing::{info, warn};

use crate::{
    config::{self, ExtProcConfig, ServerConfig},
    error::{ExtProcError, Result},
};

// -----------------------------------------------------------------------------
// Public API
// -----------------------------------------------------------------------------

/// Rebuild the filter pipeline from an on-disk YAML path and swap it in.
///
/// On success the live slot is updated and `applied_server` is replaced
/// with the newly parsed server section (for future restart-required
/// diffs). On failure the slot is left untouched (last-known-good).
///
/// # Errors
///
/// Returns [`ExtProcError::Config`] or [`ExtProcError::Pipeline`] when
/// read, parse, or build fails.
pub fn reload_from_path(
    path: &str,
    registry: &FilterRegistry,
    pipelines: &ArcSwap<FilterPipeline>,
    applied_server: &mut ServerConfig,
) -> Result<()> {
    let cfg = config::load_config(path)?;
    apply_config(&cfg, registry, pipelines, applied_server)
}

/// Rebuild the filter pipeline from YAML bytes and swap it in.
///
/// Same LKG / restart-required semantics as [`reload_from_path`].
///
/// # Errors
///
/// Returns [`ExtProcError::Config`] or [`ExtProcError::Pipeline`] when
/// parse or build fails.
pub fn reload_from_yaml(
    yaml: &str,
    registry: &FilterRegistry,
    pipelines: &ArcSwap<FilterPipeline>,
    applied_server: &mut ServerConfig,
) -> Result<()> {
    let cfg = config::parse_config(yaml)?;
    apply_config(&cfg, registry, pipelines, applied_server)
}

/// Build a pipeline from an already-parsed config and store it.
///
/// # Errors
///
/// Returns [`ExtProcError::Pipeline`] when pipeline construction fails.
pub fn apply_config(
    cfg: &ExtProcConfig,
    registry: &FilterRegistry,
    pipelines: &ArcSwap<FilterPipeline>,
    applied_server: &mut ServerConfig,
) -> Result<()> {
    let new_pipeline = config::build_pipeline(cfg, registry)?;

    log_restart_required(applied_server, &cfg.server);

    pipelines.store(new_pipeline);
    *applied_server = cfg.server.clone();

    info!(
        filters = pipelines.load().len(),
        "filter pipeline reloaded successfully"
    );
    Ok(())
}

// -----------------------------------------------------------------------------
// Restart-required detection
// -----------------------------------------------------------------------------

/// Log listen / TLS settings that differ from the live process and
/// require a restart to take effect.
fn log_restart_required(live: &ServerConfig, next: &ServerConfig) {
    if live == next {
        return;
    }

    warn_field_change("grpc_address", &live.grpc_address, &next.grpc_address);
    warn_field_change("health_address", &live.health_address, &next.health_address);
    warn_field_change("metrics_address", &live.metrics_address, &next.metrics_address);

    if live.tls != next.tls {
        warn!("server.tls changed; restart required to apply listen identity / TLS mode");
    }
}

/// Warn when a listen-address field differs between live and config.
fn warn_field_change(field: &str, live: &str, next: &str) {
    if live != next {
        warn!(
            field,
            live,
            config = next,
            "listen address changed; restart required to apply"
        );
    }
}

/// Map a failed reload into a logged LKG retention (callers keep the slot).
pub fn log_reload_failure(err: &ExtProcError) {
    warn!(error = %err, "config reload failed; keeping last-known-good pipeline");
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

#[cfg(test)]
#[expect(clippy::unwrap_used, clippy::needless_raw_strings, reason = "tests")]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::tls::{TlsConfig, TlsMode};

    const GEN_A: &str = r#"
filter_chains:
  - name: main
    filters:
      - filter: headers
        response_add:
          - name: x-reload-gen
            value: a
"#;

    const GEN_B: &str = r#"
filter_chains:
  - name: main
    filters:
      - filter: headers
        response_add:
          - name: x-reload-gen
            value: b
"#;

    const INVALID: &str = r#"
filter_chains:
  - name: main
    filters:
      - filter: nonexistent_filter
"#;

    fn slot_from_yaml(yaml: &str) -> (Arc<ArcSwap<FilterPipeline>>, FilterRegistry, ServerConfig) {
        let cfg: ExtProcConfig = serde_yaml::from_str(yaml).unwrap();
        let registry = praxis_ai_filters::build_ai_registry();
        let pipeline = config::build_pipeline(&cfg, &registry).unwrap();
        let server = cfg.server;
        (Arc::new(ArcSwap::from(pipeline)), registry, server)
    }

    #[test]
    fn successful_reload_swaps_pipeline() {
        let (slot, registry, mut server) = slot_from_yaml(GEN_A);
        let before = Arc::as_ptr(&slot.load_full());

        reload_from_yaml(GEN_B, &registry, &slot, &mut server).unwrap();

        let after = Arc::as_ptr(&slot.load_full());
        assert_ne!(before, after, "successful reload should store a new pipeline Arc");
    }

    #[test]
    fn failed_reload_keeps_last_known_good() {
        let (slot, registry, mut server) = slot_from_yaml(GEN_A);
        let before = Arc::as_ptr(&slot.load_full());

        let err = reload_from_yaml(INVALID, &registry, &slot, &mut server).unwrap_err();
        assert!(matches!(err, ExtProcError::Pipeline(_)), "expected pipeline error");

        let after = Arc::as_ptr(&slot.load_full());
        assert_eq!(before, after, "failed reload must leave LKG pipeline in place");
    }

    #[test]
    fn restart_required_still_swaps_filters() {
        let (slot, registry, mut server) = slot_from_yaml(GEN_A);
        let before = Arc::as_ptr(&slot.load_full());

        let yaml = r#"
server:
  grpc_address: "127.0.0.1:9999"
filter_chains:
  - name: main
    filters:
      - filter: headers
        response_add:
          - name: x-reload-gen
            value: b
"#;
        reload_from_yaml(yaml, &registry, &slot, &mut server).unwrap();

        assert_ne!(
            before,
            Arc::as_ptr(&slot.load_full()),
            "filter chains should still swap when listen address differs"
        );
        assert_eq!(server.grpc_address, "127.0.0.1:9999");
    }

    #[test]
    fn tls_diff_detected() {
        let live = ServerConfig {
            tls: TlsConfig {
                mode: TlsMode::None,
                ..TlsConfig::default()
            },
            ..ServerConfig::default()
        };
        let next = ServerConfig {
            tls: TlsConfig {
                mode: TlsMode::SelfSigned,
                ..TlsConfig::default()
            },
            ..ServerConfig::default()
        };
        assert_ne!(live.tls, next.tls);
    }
}

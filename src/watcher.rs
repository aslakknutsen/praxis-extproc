// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Shane Utt

//! Config-file watcher for filter-chain hot reload.
//!
//! Watches the config file's parent directory (`ConfigMap` atomic
//! rename safe), debounces filesystem events, and triggers
//! [`reload_from_path`] on content changes. Failed reloads leave
//! the last-known-good pipeline in place and apply exponential
//! backoff.
//!
//! [`reload_from_path`]: crate::reload::reload_from_path

use std::{
    collections::hash_map::DefaultHasher,
    hash::Hasher as _,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use arc_swap::ArcSwap;
use notify::{EventKind, RecommendedWatcher, RecursiveMode, Watcher as _};
use praxis_filter::{FilterPipeline, FilterRegistry};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::{
    config::ServerConfig,
    reload::{self, log_reload_failure},
};

// -----------------------------------------------------------------------------
// Constants
// -----------------------------------------------------------------------------

/// Debounce window for filesystem events (matches Praxis proxy).
const DEBOUNCE_MS: u64 = 500;

/// Initial backoff delay after a failed reload.
const BACKOFF_BASE_SECS: u64 = 1;

/// Maximum backoff delay between reload attempts.
const BACKOFF_MAX_SECS: u64 = 60;

// -----------------------------------------------------------------------------
// WatcherParams
// -----------------------------------------------------------------------------

/// Parameters for [`spawn_config_watcher`].
pub struct WatcherParams {
    /// Path to the ExtProc YAML config file.
    pub config_path: PathBuf,

    /// Content hash at process start (startup catch-up).
    pub initial_content_hash: u64,

    /// Server section applied at process start (restart-required diffs).
    pub initial_server: ServerConfig,

    /// Live pipeline slot swapped on successful reload.
    pub pipelines: Arc<ArcSwap<FilterPipeline>>,

    /// Filter registry retained for rebuilds.
    pub registry: Arc<FilterRegistry>,

    /// Cancels the watcher loop.
    pub shutdown: CancellationToken,
}

// -----------------------------------------------------------------------------
// Spawn
// -----------------------------------------------------------------------------

/// Spawn a background task that watches the config file and reloads
/// filter chains on change.
///
/// The task runs until [`WatcherParams::shutdown`] is cancelled.
pub fn spawn_config_watcher(params: WatcherParams) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        watch_loop(params).await;
    })
}

/// Core watch loop: notify events → debounce → reload.
async fn watch_loop(params: WatcherParams) {
    let (tx, mut rx) = mpsc::channel::<()>(16);
    let watch_dir = watch_dir_for_path(&params.config_path);

    let _watcher = match setup_watcher(tx, &watch_dir) {
        Ok(w) => w,
        Err(e) => {
            error!(error = %e, "failed to start config file watcher");
            return;
        },
    };

    info!(path = %params.config_path.display(), "config file watcher started");
    run_event_loop(&mut rx, &params).await;
}

/// Process filesystem events until shutdown.
#[expect(clippy::too_many_lines, reason = "debounce / backoff / select loop")]
async fn run_event_loop(rx: &mut mpsc::Receiver<()>, params: &WatcherParams) {
    let applied_server = Arc::new(Mutex::new(params.initial_server.clone()));
    let mut content_hash = params.initial_content_hash;
    let mut consecutive_failures: u32 = 0;
    let mut last_failure: Option<Instant> = None;

    // Catch changes between initial load and watcher readiness.
    handle_reload(
        &params.config_path,
        &mut content_hash,
        &params.registry,
        &params.pipelines,
        &applied_server,
    );

    loop {
        tokio::select! {
            Some(()) = rx.recv() => {
                tracing::debug!(debounce_ms = DEBOUNCE_MS, "config file change detected, debouncing");
                drain_and_debounce(rx).await;

                if should_skip_for_backoff(consecutive_failures, last_failure) {
                    continue;
                }

                let ok = handle_reload(
                    &params.config_path,
                    &mut content_hash,
                    &params.registry,
                    &params.pipelines,
                    &applied_server,
                );
                if ok {
                    consecutive_failures = 0;
                    last_failure = None;
                } else {
                    consecutive_failures = consecutive_failures.saturating_add(1);
                    last_failure = Some(Instant::now());
                }
            }
            () = params.shutdown.cancelled() => {
                info!("config file watcher shutting down");
                return;
            }
        }
    }
}

/// Read the config file and reload when content changed.
///
/// Returns `true` on success or unchanged content; `false` on error.
fn handle_reload(
    config_path: &Path,
    content_hash: &mut u64,
    registry: &FilterRegistry,
    pipelines: &ArcSwap<FilterPipeline>,
    applied_server: &Mutex<ServerConfig>,
) -> bool {
    let content = match std::fs::read_to_string(config_path) {
        Ok(c) => c,
        Err(e) => {
            error!(
                path = %config_path.display(),
                error = %e,
                "failed to read config file for reload"
            );
            return false;
        },
    };

    let new_hash = hash_content(&content);
    if new_hash == *content_hash {
        tracing::debug!("config file content unchanged, skipping reload");
        return true;
    }
    // Advance hash before build so identical bad content is not retried
    // until bytes change (matches Praxis proxy).
    *content_hash = new_hash;

    let path_str = config_path.to_string_lossy();
    let mut server_guard = match applied_server.lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };

    match reload::reload_from_path(path_str.as_ref(), registry, pipelines, &mut server_guard) {
        Ok(()) => true,
        Err(e) => {
            log_reload_failure(&e);
            false
        },
    }
}

/// Set up a [`RecommendedWatcher`] that notifies on relevant events.
fn setup_watcher(tx: mpsc::Sender<()>, watch_dir: &Path) -> Result<RecommendedWatcher, notify::Error> {
    let mut watcher = notify::recommended_watcher(move |res: Result<notify::Event, notify::Error>| match res {
        Ok(event) if is_relevant_event(event.kind) && tx.try_send(()).is_err() => {
            tracing::trace!("config watcher channel full, event coalesced by debounce");
        },
        Err(e) => {
            tracing::warn!(error = %e, "config file watcher error");
        },
        _ => {},
    })?;

    watcher.watch(watch_dir, RecursiveMode::NonRecursive)?;
    Ok(watcher)
}

/// Drain pending events and sleep for the debounce window.
async fn drain_and_debounce(rx: &mut mpsc::Receiver<()>) {
    tokio::time::sleep(Duration::from_millis(DEBOUNCE_MS)).await;
    while rx.try_recv().is_ok() {}
}

/// Whether a notify event kind should trigger a reload attempt.
fn is_relevant_event(kind: EventKind) -> bool {
    matches!(kind, EventKind::Create(_) | EventKind::Modify(_) | EventKind::Remove(_))
}

/// Resolve the directory to watch for a config path.
///
/// Falls back to `.` for bare filenames where [`Path::parent`] is empty.
fn watch_dir_for_path(path: &Path) -> PathBuf {
    path.parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf()
}

/// Whether the backoff window after failures has not yet elapsed.
fn should_skip_for_backoff(consecutive_failures: u32, last_failure: Option<Instant>) -> bool {
    let Some(last) = last_failure else {
        return false;
    };
    let backoff = backoff_duration(consecutive_failures);
    let elapsed = last.elapsed();
    if elapsed < backoff {
        let remaining = backoff - elapsed;
        warn!(
            consecutive_failures,
            backoff_secs = backoff.as_secs(),
            remaining_secs = remaining.as_secs(),
            "config reload skipped, backing off after repeated failures",
        );
        return true;
    }
    false
}

/// Exponential backoff capped at [`BACKOFF_MAX_SECS`].
fn backoff_duration(consecutive_failures: u32) -> Duration {
    let exp = consecutive_failures.saturating_sub(1).min(63);
    let secs = BACKOFF_BASE_SECS
        .saturating_mul(1_u64.checked_shl(exp).unwrap_or(u64::MAX))
        .min(BACKOFF_MAX_SECS);
    Duration::from_secs(secs)
}

/// Hash file content for no-op change detection.
pub fn hash_content(content: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    hasher.write(content.as_bytes());
    hasher.finish()
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

#[cfg(test)]
#[expect(clippy::unwrap_used, clippy::too_many_lines, reason = "tests")]
mod tests {
    use super::*;
    use crate::config;

    const VALID_YAML: &str = "
filter_chains:
  - name: main
    filters:
      - filter: headers
        response_add:
          - name: x-reload-gen
            value: a
";

    const VALID_YAML_CHANGED: &str = "
filter_chains:
  - name: main
    filters:
      - filter: headers
        response_add:
          - name: x-reload-gen
            value: b
";

    #[test]
    fn is_relevant_event_kinds() {
        assert!(is_relevant_event(EventKind::Create(notify::event::CreateKind::File)));
        assert!(is_relevant_event(EventKind::Modify(notify::event::ModifyKind::Data(
            notify::event::DataChange::Content
        ))));
        assert!(is_relevant_event(EventKind::Remove(notify::event::RemoveKind::File)));
        assert!(!is_relevant_event(EventKind::Access(notify::event::AccessKind::Read)));
    }

    #[test]
    fn watch_dir_falls_back_for_bare_name() {
        assert_eq!(watch_dir_for_path(Path::new("praxis.yaml")), PathBuf::from("."));
    }

    #[test]
    fn backoff_grows_then_caps() {
        assert_eq!(backoff_duration(1), Duration::from_secs(1));
        assert_eq!(backoff_duration(2), Duration::from_secs(2));
        assert_eq!(backoff_duration(3), Duration::from_secs(4));
        assert_eq!(backoff_duration(100), Duration::from_secs(BACKOFF_MAX_SECS));
    }

    #[tokio::test]
    async fn watcher_reloads_on_file_change() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("praxis.yaml");
        std::fs::write(&config_path, VALID_YAML).unwrap();

        let cfg = config::parse_config(VALID_YAML).unwrap();
        let registry = Arc::new(praxis_ai_filters::build_ai_registry());
        let pipeline = config::build_pipeline(&cfg, &registry).unwrap();
        let pipelines = Arc::new(ArcSwap::from(pipeline));
        let old_ptr = Arc::as_ptr(&pipelines.load_full());
        let shutdown = CancellationToken::new();

        let handle = spawn_config_watcher(WatcherParams {
            config_path: config_path.clone(),
            initial_content_hash: hash_content(VALID_YAML),
            initial_server: cfg.server,
            pipelines: Arc::clone(&pipelines),
            registry: Arc::clone(&registry),
            shutdown: shutdown.clone(),
        });

        tokio::time::sleep(Duration::from_millis(200)).await;
        std::fs::write(&config_path, VALID_YAML_CHANGED).unwrap();

        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if Arc::as_ptr(&pipelines.load_full()) != old_ptr {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        assert_ne!(
            old_ptr,
            Arc::as_ptr(&pipelines.load_full()),
            "pipeline should swap after config file change"
        );

        shutdown.cancel();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn watcher_keeps_lkg_on_invalid_config() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("praxis.yaml");
        std::fs::write(&config_path, VALID_YAML).unwrap();

        let cfg = config::parse_config(VALID_YAML).unwrap();
        let registry = Arc::new(praxis_ai_filters::build_ai_registry());
        let pipeline = config::build_pipeline(&cfg, &registry).unwrap();
        let pipelines = Arc::new(ArcSwap::from(pipeline));
        let old_ptr = Arc::as_ptr(&pipelines.load_full());
        let shutdown = CancellationToken::new();

        let handle = spawn_config_watcher(WatcherParams {
            config_path: config_path.clone(),
            initial_content_hash: hash_content(VALID_YAML),
            initial_server: cfg.server,
            pipelines: Arc::clone(&pipelines),
            registry: Arc::clone(&registry),
            shutdown: shutdown.clone(),
        });

        tokio::time::sleep(Duration::from_millis(200)).await;
        std::fs::write(
            &config_path,
            "
filter_chains:
  - name: main
    filters:
      - filter: nonexistent_filter
",
        )
        .unwrap();
        tokio::time::sleep(Duration::from_millis(800)).await;

        assert_eq!(
            old_ptr,
            Arc::as_ptr(&pipelines.load_full()),
            "invalid config must leave LKG pipeline"
        );

        shutdown.cancel();
        handle.await.unwrap();
    }
}

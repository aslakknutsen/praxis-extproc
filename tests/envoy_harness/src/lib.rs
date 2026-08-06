//! Local Envoy e2e harness for praxis-extproc.
//!
//! Spawns an in-process ExtProc server, a recording HTTP backend, and a
//! real Envoy process (see `ENVOY_BIN`) with ports allocated for parallel
//! tests.

#![allow(missing_docs)]

pub mod backend;
pub mod env;
pub mod envoy;
pub mod extproc;
pub mod ports;
pub mod stats;

pub use backend::{CapturedRequest, RecordingBackend, RequestLog};
pub use env::{EnvoyEnv, EnvoyEnvBuilder};
pub use envoy::{DEFAULT_ENVOY_BOOTSTRAP, envoy_available, require_envoy_bin};

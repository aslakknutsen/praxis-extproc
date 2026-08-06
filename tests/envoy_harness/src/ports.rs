//! Port allocation for parallel Envoy e2e tests.

use std::{
    collections::HashSet,
    net::TcpListener,
    sync::{LazyLock, Mutex, PoisonError},
};

static ALLOCATED_PORTS: LazyLock<Mutex<HashSet<u16>>> = LazyLock::new(|| Mutex::new(HashSet::new()));

/// Bind an OS-assigned port that is unique within this process.
///
/// # Panics
///
/// Panics if a unique port cannot be bound after 256 attempts.
pub fn bind_unique_port() -> (TcpListener, u16) {
    for _ in 0..256 {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind 127.0.0.1:0");
        let port = listener.local_addr().expect("local_addr").port();
        if ALLOCATED_PORTS
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(port)
        {
            return (listener, port);
        }
    }
    panic!("failed to bind a unique port after 256 attempts");
}

/// Held port that keeps its listener open until [`PortGuard::release`].
pub struct PortGuard {
    port: u16,
    _listener: TcpListener,
}

impl PortGuard {
    /// Allocated port number.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Drop the listener so another process can bind the port.
    pub fn release(self) -> u16 {
        self.port
    }
}

/// Allocate a free port and hold the listener until [`PortGuard::release`].
pub fn free_port_guard() -> PortGuard {
    let (listener, port) = bind_unique_port();
    PortGuard {
        port,
        _listener: listener,
    }
}

/// Allocate a free port (listener closed immediately — TOCTOU possible).
pub fn free_port() -> u16 {
    let (_listener, port) = bind_unique_port();
    port
}

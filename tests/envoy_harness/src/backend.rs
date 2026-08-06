//! Recording HTTP backend for asserting upstream requests.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Request, Response, body::Incoming, server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

use crate::ports::free_port_guard;

/// A single HTTP request captured by the backend.
#[derive(Debug, Clone)]
pub struct CapturedRequest {
    /// HTTP method string.
    pub method: String,
    /// Request path including query.
    pub path: String,
    /// Request headers (lowercased names).
    pub headers: HashMap<String, String>,
    /// Raw request body.
    pub body: Vec<u8>,
}

/// Shared log of captured upstream requests.
#[derive(Clone, Default)]
pub struct RequestLog {
    inner: Arc<Mutex<Vec<CapturedRequest>>>,
}

impl std::fmt::Debug for RequestLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RequestLog").finish_non_exhaustive()
    }
}

impl RequestLog {
    /// Snapshot of all captured requests.
    pub fn requests(&self) -> Vec<CapturedRequest> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner).clone()
    }

    /// Most recent captured request, if any.
    pub fn last(&self) -> Option<CapturedRequest> {
        self.inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .last()
            .cloned()
    }

    /// Clear the capture log.
    pub fn clear(&self) {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner).clear();
    }

    fn push(&self, req: CapturedRequest) {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner).push(req);
    }
}

/// Handle for a running recording backend.
pub struct RecordingBackend {
    /// Bound port.
    pub port: u16,
    /// Captured upstream requests.
    pub log: RequestLog,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
}

impl std::fmt::Debug for RecordingBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecordingBackend")
            .field("port", &self.port)
            .finish_non_exhaustive()
    }
}

impl Drop for RecordingBackend {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}

/// Start a recording backend that returns a fixed body.
pub async fn start_recording_backend(body: &str) -> RecordingBackend {
    let port = free_port_guard().release();
    let listener = TcpListener::bind(("127.0.0.1", port))
        .await
        .unwrap_or_else(|e| panic!("bind backend on {port}: {e}"));

    let log = RequestLog::default();
    let log_clone = log.clone();
    let response_body = body.to_owned();
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown_rx => break,
                accept = listener.accept() => {
                    let Ok((stream, _)) = accept else { break };
                    let log = log_clone.clone();
                    let response_body = response_body.clone();
                    tokio::spawn(async move {
                        let io = TokioIo::new(stream);
                        let service = service_fn(move |req: Request<Incoming>| {
                            let log = log.clone();
                            let response_body = response_body.clone();
                            async move { handle_request(req, &log, &response_body).await }
                        });
                        let _ = http1::Builder::new().serve_connection(io, service).await;
                    });
                }
            }
        }
    });

    // Brief settle so the accept loop is scheduled.
    tokio::time::sleep(Duration::from_millis(10)).await;

    RecordingBackend {
        port,
        log,
        shutdown: Some(shutdown_tx),
    }
}

async fn handle_request(
    req: Request<Incoming>,
    log: &RequestLog,
    response_body: &str,
) -> Result<Response<Full<Bytes>>, hyper::Error> {
    let method = req.method().as_str().to_owned();
    let path = req
        .uri()
        .path_and_query()
        .map(|p| p.as_str().to_owned())
        .unwrap_or_else(|| req.uri().path().to_owned());

    let mut headers = HashMap::new();
    for (name, value) in req.headers() {
        if let Ok(v) = value.to_str() {
            headers.insert(name.as_str().to_ascii_lowercase(), v.to_owned());
        }
    }

    let body = req.collect().await?.to_bytes().to_vec();

    log.push(CapturedRequest {
        method,
        path,
        headers,
        body,
    });

    Ok(Response::builder()
        .status(200)
        .header("content-type", "text/plain")
        .body(Full::new(Bytes::from(response_body.to_owned())))
        .expect("response builder"))
}

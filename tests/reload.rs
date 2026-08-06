// SPDX-License-Identifier: MIT
//! In-process tests for filter-chain hot reload and stream pinning.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::tests_outside_test_module,
    clippy::missing_assert_message,
    clippy::needless_raw_strings,
    clippy::needless_raw_string_hashes,
    clippy::needless_update,
    clippy::too_many_lines,
    clippy::missing_docs_in_private_items,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    reason = "tests"
)]
#![allow(missing_docs, reason = "test module")]

use std::sync::Arc;

use arc_swap::ArcSwap;
use praxis_extproc::{
    config::{self, ServerConfig},
    reload,
    server::PraxisExtProc,
};
use praxis_proto::envoy::service::{
    common::v3::HeaderValue,
    ext_proc::v3::{
        HeaderMap, HttpHeaders, ProcessingRequest, ProcessingResponse,
        external_processor_server::ExternalProcessorServer, processing_request::Request as ReqVariant,
        processing_response::Response as RespVariant,
    },
};
use tokio::net::TcpListener;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Channel, Server};

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

type ExtProcClient =
    praxis_proto::envoy::service::ext_proc::v3::external_processor_client::ExternalProcessorClient<Channel>;

struct TestServer {
    client: ExtProcClient,
    pipelines: Arc<ArcSwap<praxis_filter::FilterPipeline>>,
    registry: praxis_filter::FilterRegistry,
    applied_server: ServerConfig,
    _shutdown: tokio::sync::oneshot::Sender<()>,
}

#[tokio::test]
async fn successful_reload_applies_to_new_streams() {
    let mut env = start_server(GEN_A).await;

    let first = send_full_roundtrip(&mut env.client, "GET", "/").await;
    assert_eq!(reload_gen(&first), Some("a"), "precondition: generation a");

    reload::reload_from_yaml(GEN_B, &env.registry, &env.pipelines, &mut env.applied_server).unwrap();

    let second = send_full_roundtrip(&mut env.client, "GET", "/").await;
    assert_eq!(reload_gen(&second), Some("b"), "new stream should see generation b");
}

#[tokio::test]
async fn failed_reload_keeps_last_known_good_for_new_streams() {
    let mut env = start_server(GEN_A).await;

    let err = reload::reload_from_yaml(INVALID, &env.registry, &env.pipelines, &mut env.applied_server).unwrap_err();
    assert!(
        matches!(err, praxis_extproc::error::ExtProcError::Pipeline(_)),
        "invalid filter should fail pipeline build"
    );

    let responses = send_full_roundtrip(&mut env.client, "GET", "/").await;
    assert_eq!(
        reload_gen(&responses),
        Some("a"),
        "LKG pipeline must keep serving generation a"
    );
}

#[tokio::test]
async fn stream_pins_pipeline_across_midflight_swap() {
    let mut env = start_server(GEN_A).await;

    let (tx, rx) = tokio::sync::mpsc::channel(16);
    let stream = ReceiverStream::new(rx);
    let response = env.client.process(stream).await.expect("process");
    let mut inbound = response.into_inner();

    // Open stream (pins generation A), complete request phase.
    tx.send(make_request_headers("GET", "/", true))
        .await
        .expect("send request headers");
    let req_resp = inbound.message().await.expect("msg").expect("request headers response");
    assert!(
        matches!(req_resp.response, Some(RespVariant::RequestHeaders(_))),
        "expected request headers response"
    );

    // Swap process-wide slot to generation B while stream is live.
    reload::reload_from_yaml(GEN_B, &env.registry, &env.pipelines, &mut env.applied_server).unwrap();

    // Response phase on the pinned stream must still use generation A.
    tx.send(make_response_headers(true))
        .await
        .expect("send response headers");
    let mut pinned_responses = vec![req_resp];
    while let Ok(Some(msg)) = inbound.message().await {
        pinned_responses.push(msg);
        if pinned_responses
            .iter()
            .any(|r| matches!(r.response, Some(RespVariant::ResponseHeaders(_))))
        {
            break;
        }
    }
    assert_eq!(
        reload_gen(&pinned_responses),
        Some("a"),
        "in-flight stream must keep generation a after mid-flight swap"
    );

    drop(tx);

    // A new stream must observe generation B.
    let fresh = send_full_roundtrip(&mut env.client, "GET", "/fresh").await;
    assert_eq!(reload_gen(&fresh), Some("b"), "new stream should see generation b");
}

async fn start_server(config_yaml: &str) -> TestServer {
    let cfg = config::parse_config(config_yaml).expect("parse config");
    let registry = praxis_ai_filters::build_ai_registry();
    let pipeline = config::build_pipeline(&cfg, &registry).expect("build pipeline");
    let pipelines = Arc::new(ArcSwap::from(pipeline));
    let applied_server = cfg.server;

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");

    let svc = PraxisExtProc::new(Arc::clone(&pipelines));
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    tokio::spawn(async move {
        Server::builder()
            .add_service(ExternalProcessorServer::new(svc))
            .serve_with_incoming_shutdown(tokio_stream::wrappers::TcpListenerStream::new(listener), async {
                drop(shutdown_rx.await);
            })
            .await
            .expect("server failed");
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let channel = Channel::from_shared(format!("http://{addr}"))
        .expect("uri")
        .connect()
        .await
        .expect("connect");

    TestServer {
        client: ExtProcClient::new(channel),
        pipelines,
        registry,
        applied_server,
        _shutdown: shutdown_tx,
    }
}

async fn send_full_roundtrip(client: &mut ExtProcClient, method: &str, path: &str) -> Vec<ProcessingResponse> {
    let (tx, rx) = tokio::sync::mpsc::channel(16);
    let stream = ReceiverStream::new(rx);

    let response = client.process(stream).await.expect("process call failed");
    let mut inbound = response.into_inner();

    tx.send(make_request_headers(method, path, true))
        .await
        .expect("send request headers");
    tx.send(make_response_headers(true))
        .await
        .expect("send response headers");

    drop(tx);
    collect_responses(&mut inbound).await
}

fn make_request_headers(method: &str, path: &str, end_of_stream: bool) -> ProcessingRequest {
    ProcessingRequest {
        request: Some(ReqVariant::RequestHeaders(HttpHeaders {
            headers: Some(HeaderMap {
                headers: vec![
                    make_header(":method", method),
                    make_header(":path", path),
                    make_header(":authority", "localhost"),
                    make_header(":scheme", "http"),
                ],
            }),
            end_of_stream,
        })),
        ..Default::default()
    }
}

fn make_response_headers(end_of_stream: bool) -> ProcessingRequest {
    ProcessingRequest {
        request: Some(ReqVariant::ResponseHeaders(HttpHeaders {
            headers: Some(HeaderMap {
                headers: vec![make_header(":status", "200")],
            }),
            end_of_stream,
        })),
        ..Default::default()
    }
}

fn make_header(key: &str, value: &str) -> HeaderValue {
    HeaderValue {
        key: key.to_owned(),
        value: value.to_owned(),
        raw_value: Vec::new(),
    }
}

async fn collect_responses(inbound: &mut tonic::Streaming<ProcessingResponse>) -> Vec<ProcessingResponse> {
    let mut out = Vec::new();
    while let Ok(Some(msg)) = inbound.message().await {
        out.push(msg);
    }
    out
}

fn reload_gen(responses: &[ProcessingResponse]) -> Option<&str> {
    for resp in responses {
        let Some(RespVariant::ResponseHeaders(hr)) = &resp.response else {
            continue;
        };
        let Some(common) = &hr.response else {
            continue;
        };
        let Some(mutation) = &common.header_mutation else {
            continue;
        };
        for h in &mutation.set_headers {
            if let Some(hv) = &h.header
                && hv.key.eq_ignore_ascii_case("x-reload-gen")
            {
                return Some(hv.value.as_str());
            }
        }
    }
    None
}

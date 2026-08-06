//! Local Envoy e2e tests (requires `ENVOY_BIN` from `make ensure-envoy`).
//!
//! Run with: `make test-envoy`
//! Ignored / failing acceptance: `make test-envoy-failing`

#![cfg(feature = "envoy")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::tests_outside_test_module,
    clippy::missing_assert_message,
    clippy::needless_raw_strings,
    clippy::needless_raw_string_hashes,
    missing_docs,
    reason = "envoy e2e tests"
)]

use std::time::Duration;

use envoy_harness::EnvoyEnvBuilder;

// -----------------------------------------------------------------------------
// Config fixtures
// -----------------------------------------------------------------------------

const CONFIG_GEN_A: &str = r#"
filter_chains:
  - name: test
    filters:
      - filter: headers
        request_add:
          - name: X-Test
            value: extproc
        response_set:
          - name: x-reload-gen
            value: "a"
insecure_options:
  allow_unbounded_body: true
"#;

const CONFIG_GEN_B: &str = r#"
filter_chains:
  - name: test
    filters:
      - filter: headers
        request_add:
          - name: X-Test
            value: extproc
        response_set:
          - name: x-reload-gen
            value: "b"
insecure_options:
  allow_unbounded_body: true
"#;

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

#[tokio::test]
async fn envoy_extproc_applies_headers() {
    let env = EnvoyEnvBuilder::new(CONFIG_GEN_A).start().await;

    let resp = env
        .http()
        .get(env.url("/"))
        .send()
        .await
        .expect("request through envoy");

    assert_eq!(resp.status(), 200, "backend should respond 200");
    let reload_gen = resp.headers().get("x-reload-gen").and_then(|v| v.to_str().ok());
    assert_eq!(reload_gen, Some("a"), "response header from ExtProc headers filter");

    let upstream = env.backend().last().expect("backend should see a request");
    assert_eq!(upstream.path, "/");
    assert!(
        upstream.headers.contains_key("x-test"),
        "request header mutation should reach backend, got {:?}",
        upstream.headers
    );
}

#[tokio::test]
#[ignore = "opendatahub-io/praxis-extproc#17: filter-chain hot-reload not implemented"]
async fn config_file_change_hot_reloads() {
    let env = EnvoyEnvBuilder::new(CONFIG_GEN_A).start().await;

    let resp = env.http().get(env.url("/")).send().await.expect("initial request");
    assert_eq!(
        resp.headers().get("x-reload-gen").and_then(|v| v.to_str().ok()),
        Some("a"),
        "precondition: generation a"
    );

    env.overwrite_praxis_config(CONFIG_GEN_B).await;
    // Future config-watcher debounce window (see Praxis proxy watcher).
    tokio::time::sleep(Duration::from_millis(600)).await;

    let resp = env
        .http()
        .get(env.url("/reload-check"))
        .send()
        .await
        .expect("post-reload request");

    assert_eq!(
        resp.headers().get("x-reload-gen").and_then(|v| v.to_str().ok()),
        Some("b"),
        "hot-reload should apply config B without process restart (#17)"
    );
}

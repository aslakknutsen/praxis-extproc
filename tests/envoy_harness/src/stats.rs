//! Minimal Envoy admin `/stats` helpers.

use std::time::Duration;

/// Fetch raw stats text from the Envoy admin port.
pub async fn fetch_stats(admin_port: u16) -> String {
    let url = format!("http://127.0.0.1:{admin_port}/stats");
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .expect("stats client");
    client
        .get(&url)
        .send()
        .await
        .expect("stats request")
        .text()
        .await
        .expect("stats body")
}

/// Parse a counter value from Envoy stats text (`name: value`).
pub fn counter_value(stats: &str, name: &str) -> Option<u64> {
    let prefix = format!("{name}:");
    for line in stats.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix(&prefix) {
            return rest.trim().parse().ok();
        }
    }
    None
}

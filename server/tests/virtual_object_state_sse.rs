// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;
use enumset::EnumSet;
use futures_util::StreamExt;
use googletest::IntoTestResult;
use http::header::ACCEPT;
use restate_local_cluster_runner::cluster::Cluster;
use restate_local_cluster_runner::node::{BinarySource, NodeSpec};
use restate_types::config::Configuration;
use restate_types::net::address::PeerNetAddress;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

/// Verify that `GET /restate/objects/{service}/{key}/state` returns an SSE stream and delivers
/// kv-sync events when state is mutated via the admin `POST /services/{service}/state` endpoint.
///
/// Scenarios covered:
/// - RPL: initial empty snapshot on first connect
/// - ASN: multi-key assign in one transaction produces a single batched ASN event
/// - DEL + ASN: deletions and assignments batched in the same SSE event (Patch)
/// - CLR: `new_state: null` produces a single CLR event
/// - Reconnect with stale Last-Event-ID triggers a fresh RPL snapshot
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn virtual_object_state_sse_delivers_events() -> googletest::Result<()> {
    let mut base_config = Configuration::new_unix_sockets();
    base_config.common.default_num_partitions = 1.try_into()?;

    let mut cluster = Cluster::builder()
        .cluster_name("virtual-object-state-sse-events-test")
        .nodes(vec![NodeSpec::new_test_node(
            "node-1",
            base_config,
            BinarySource::CargoTest,
            EnumSet::all(),
        )])
        .temp_base_dir("virtual_object_state_sse_test")
        .build()
        .start()
        .await?;

    cluster.wait_healthy(Duration::from_secs(120)).await?;

    let node = &cluster.nodes[0];

    // Build admin and ingress clients.
    let admin_uds = node
        .admin_address()
        .clone()
        .unwrap()
        .into_address()
        .unwrap();
    let PeerNetAddress::Uds(admin_uds) = admin_uds else {
        panic!("admin address must be a unix domain socket");
    };
    let admin_client = reqwest::Client::builder().unix_socket(admin_uds).build()?;

    let ingress_uds = node
        .ingress_address()
        .clone()
        .unwrap()
        .into_address()
        .unwrap();
    let PeerNetAddress::Uds(ingress_uds) = ingress_uds else {
        panic!("ingress address must be a unix domain socket");
    };
    let ingress_client = reqwest::Client::builder()
        .unix_socket(ingress_uds)
        // Disable connection pooling: each request gets a fresh connection so that
        // probe requests dropped without consuming the SSE body don't leave stale
        // connections that would corrupt subsequent reads.
        .pool_max_idle_per_host(0)
        .build()?;

    // Register the Counter virtual object (required for the admin state-mutation endpoint).
    let mock_port = start_mock_service().await;
    retry_until(
        Duration::from_secs(30),
        Duration::from_millis(500),
        || async {
            admin_client
                .post("http://localhost/deployments")
                .header("content-type", "application/json")
                .json(&serde_json::json!({ "uri": format!("http://127.0.0.1:{mock_port}") }))
                .send()
                .await
                .map(|r| r.status().is_success())
                .unwrap_or(false)
        },
    )
    .await
    .into_test_result()?;

    // Open the SSE stream. Retry with backoff until the partition processor is ready
    // (returns 200 rather than 503).
    retry_until(
        Duration::from_secs(30),
        Duration::from_millis(500),
        || async {
            ingress_client
                .get("http://localhost/restate/objects/Counter/test-key/state")
                .header(ACCEPT, "text/event-stream")
                .send()
                .await
                .map(|r| r.status() == 200)
                .unwrap_or(false)
        },
    )
    .await
    .into_test_result()?;

    // Open the long-lived SSE stream before mutating state.
    let sse_resp = ingress_client
        .get("http://localhost/restate/objects/Counter/test-key/state")
        .header(ACCEPT, "text/event-stream")
        .send()
        .await?;
    assert_eq!(
        sse_resp.status(),
        200,
        "expected 200 from long-lived SSE connection"
    );
    let mut sse = sse_resp.bytes_stream();

    // --- RPL: initial empty snapshot ---
    let (id, data) = recv_sse_event(&mut sse).await.into_test_result()?;
    assert_eq!(id, "0", "expected initial RPL with id 0");
    assert_eq!(data, "RPL {}", "expected empty initial snapshot");

    // --- ASN (batched): set two keys in one admin mutation → single ASN with both keys ---
    // bytes::Bytes serializes as a JSON array of u8 values; b"1" = [49].
    // encode_value([49]) → valid UTF-8 "1" → valid JSON number 1, etc.
    admin_client
        .post("http://localhost/services/Counter/state")
        .json(&serde_json::json!({
            "object_key": "test-key",
            "version": null,
            "new_state": { "a": b"1".to_vec(), "b": b"hello\nworld".to_vec() }
        }))
        .send()
        .await?
        .error_for_status()?;

    let (id, data) = recv_sse_event(&mut sse).await.into_test_result()?;
    assert_eq!(id, "1", "expected revision 1 after first multi-key put");
    // Both keys assigned in one transaction → single ASN event.
    let asn: serde_json::Value = data
        .strip_prefix("ASN ")
        .and_then(|s| serde_json::from_str(s).ok())
        .expect("expected ASN JSON object");
    assert_eq!(asn["a"], serde_json::json!(1));
    assert_eq!(asn["b"], serde_json::json!("hello\nworld"));

    // --- DEL + ASN (batched): replace {a,b} with {a,c} in one mutation ---
    // "b" is deleted, "c" is added, "a" value unchanged (no-op assign skipped).
    // The Replace operation produces a Patch event with both ASN and DEL data lines.
    admin_client
        .post("http://localhost/services/Counter/state")
        .json(&serde_json::json!({
            "object_key": "test-key",
            "version": null,
            "new_state": { "a": b"1".to_vec(), "c": b"3".to_vec() }
        }))
        .send()
        .await?
        .error_for_status()?;

    // ASN and DEL are emitted in the same SSE event (same id) as separate data lines.
    let (id, data) = recv_sse_event(&mut sse).await.into_test_result()?;
    assert_eq!(id, "2", "expected revision 2 for replace");

    let mut lines = data.lines();
    let asn_line = lines.next().expect("expected ASN line");
    let del_line = lines.next().expect("expected DEL line");

    let asn: serde_json::Value = asn_line
        .strip_prefix("ASN ")
        .and_then(|s| serde_json::from_str(s).ok())
        .expect("expected ASN JSON object for added key c");
    assert_eq!(asn["c"], serde_json::json!(3));
    assert!(
        !asn.as_object().unwrap().contains_key("a"),
        "a was unchanged, no ASN expected"
    );

    let del: serde_json::Value = del_line
        .strip_prefix("DEL ")
        .and_then(|s| serde_json::from_str(s).ok())
        .expect("expected DEL JSON array");
    assert_eq!(del, serde_json::json!(["b"]));

    // --- CLR: null new_state → single CLR event ---
    admin_client
        .post("http://localhost/services/Counter/state")
        .json(&serde_json::json!({
            "object_key": "test-key",
            "version": null,
            "new_state": null
        }))
        .send()
        .await?
        .error_for_status()?;

    let (id, data) = recv_sse_event(&mut sse).await.into_test_result()?;
    assert_eq!(id, "3", "expected revision 3 for clear-all");
    assert_eq!(data, "CLR");

    // Set one more key so the revision advances past what we'll use for Last-Event-ID.
    drop(sse);
    admin_client
        .post("http://localhost/services/Counter/state")
        .json(&serde_json::json!({
            "object_key": "test-key",
            "version": null,
            "new_state": { "x": b"5".to_vec() }
        }))
        .send()
        .await?
        .error_for_status()?;

    // --- Reconnect: stale Last-Event-ID=3, current revision is 4 → fresh RPL snapshot ---
    let sse_resp = ingress_client
        .get("http://localhost/restate/objects/Counter/test-key/state")
        .header(ACCEPT, "text/event-stream")
        .header("Last-Event-ID", "3")
        .send()
        .await?;
    let mut sse = sse_resp.bytes_stream();

    let (id, data) = recv_sse_event(&mut sse).await.into_test_result()?;
    assert_eq!(id, "4", "expected snapshot at current revision 4");
    assert_eq!(data, r#"RPL {"x":5}"#, "expected snapshot with x key");

    cluster.graceful_shutdown(Duration::from_secs(10)).await?;
    Ok(())
}

/// Retry `check` every `interval` until it returns `true` or `timeout` elapses.
async fn retry_until<F, Fut>(timeout: Duration, interval: Duration, check: F) -> anyhow::Result<()>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if check().await {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(anyhow::anyhow!("timed out after {:?}", timeout));
        }
        tokio::time::sleep(interval).await;
    }
}

/// Start the mock service endpoint and return the port it is listening on.
async fn start_mock_service() -> u16 {
    let addr: SocketAddr = ([127, 0, 0, 1], 0).into();
    let listener = TcpListener::bind(addr).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (ready_tx, ready_rx) = oneshot::channel::<()>();
    tokio::spawn(async move {
        mock_service_endpoint::listener::run_listener(listener, || {
            let _ = ready_tx.send(());
        })
        .await
        .expect("mock service endpoint failed");
    });
    ready_rx.await.unwrap();
    port
}

/// Read bytes from an SSE byte-stream until a complete event block (`\n\n`) is received.
/// Returns `(id, data)`. Fails if no complete event arrives within 10 seconds.
async fn recv_sse_event(
    stream: &mut (impl futures_util::Stream<Item = reqwest::Result<Bytes>> + Unpin),
) -> anyhow::Result<(String, String)> {
    let mut buf = String::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);

    loop {
        let chunk = tokio::time::timeout_at(deadline, stream.next())
            .await
            .map_err(|_| anyhow::anyhow!("timed out waiting for SSE event"))?
            .ok_or_else(|| anyhow::anyhow!("SSE stream ended unexpectedly"))?
            .map_err(|e| anyhow::anyhow!("SSE body read error: {e:#?}"))?;

        buf.push_str(std::str::from_utf8(&chunk)?);

        // Each SSE event block is separated by a blank line (\n\n).
        let mut remaining = buf.as_str();
        while let Some(end) = remaining.find("\n\n") {
            let block = &remaining[..end];
            remaining = &remaining[end + 2..];

            let mut id = String::new();
            let mut data_lines = Vec::new();
            for line in block.lines() {
                if let Some(v) = line.strip_prefix("id: ") {
                    id = v.to_owned();
                } else if let Some(v) = line.strip_prefix("data: ") {
                    data_lines.push(v.to_owned());
                }
            }

            // Skip keep-alive comment blocks (id and data both empty).
            if !id.is_empty() || !data_lines.is_empty() {
                return Ok((id, data_lines.join("\n")));
            }
        }
        buf = remaining.to_owned();
    }
}

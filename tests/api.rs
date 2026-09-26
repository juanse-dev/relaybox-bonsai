use reqwest::Client;
use serde_json::json;
use tempfile::tempdir;

async fn ready(client: &Client, base: &str) -> bool {
    match client.get(format!("{base}/health")).send().await {
        Ok(resp) => resp.status().as_u16() == 200,
        Err(_) => false,
    }
}

async fn start_from(url: &str) -> (Client, String, tokio::task::JoinHandle<()>) {
    let app = relaybox::app::App::create(url).await.unwrap();
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0u16))
        .await
        .unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let router = app.router;
    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });

    let client = Client::new();
    for _ in 0..300 {
        if ready(&client, &base).await {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    (client, base, handle)
}

async fn start() -> (Client, String, String, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let url = format!("sqlite://{}", db_path.display());
    let (client, base, _) = start_from(&url).await;
    (client, base, url, dir)
}

fn error_code(body: &serde_json::Value) -> &str {
    body.get("error")
        .and_then(|e| e.get("code"))
        .and_then(|c| c.as_str())
        .expect("response must contain an error code")
}

fn s<'a>(body: &'a serde_json::Value, key: &str) -> &'a str {
    body.get(key).and_then(|v| v.as_str()).expect(key)
}

fn u64_of(body: &serde_json::Value, key: &str) -> u64 {
    body.get(key).and_then(|v| v.as_u64()).expect(key)
}

fn value_of<'a>(body: &'a serde_json::Value, key: &str) -> &'a serde_json::Value {
    body.get(key).expect(key)
}

async fn get(client: &Client, base: &str, path: &str) -> (reqwest::StatusCode, serde_json::Value) {
    let resp = client.get(format!("{base}{path}")).send().await.unwrap();
    let status = resp.status();
    let body: serde_json::Value = resp.json().await.unwrap_or(json!(null));
    (status, body)
}

async fn enqueue(
    client: &Client,
    base: &str,
    key: &str,
    target_url: &str,
    payload: &serde_json::Value,
) -> (reqwest::StatusCode, serde_json::Value) {
    let body = json!({
        "target_url": target_url,
        "payload": payload,
    });
    let resp = client
        .post(format!("{base}/v1/deliveries"))
        .header("idempotency-key", key)
        .json(&body)
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let value: serde_json::Value = resp.json().await.unwrap_or(json!(null));
    (status, value)
}

async fn enqueue_raw(
    client: &Client,
    base: &str,
    key: Option<&str>,
    body: &str,
) -> (reqwest::StatusCode, serde_json::Value) {
    let resp = match key {
        Some(k) => {
            client
                .post(format!("{base}/v1/deliveries"))
                .header("idempotency-key", k)
                .body(body.as_bytes().to_vec())
                .send()
                .await
        }
        None => {
            client
                .post(format!("{base}/v1/deliveries"))
                .body(body.as_bytes().to_vec())
                .send()
                .await
        }
    }
    .unwrap();
    let status = resp.status();
    let value: serde_json::Value = resp.json().await.unwrap_or(json!(null));
    (status, value)
}

/// Enqueue with an idempotency key supplied as raw bytes.
///
/// Unlike `enqueue_raw`, the key is not required to be UTF-8. This lets the
/// suite exercise the lossless handling of non-UTF-8 key bytes.
async fn enqueue_bytes(
    client: &Client,
    base: &str,
    key: Option<&[u8]>,
    body: &str,
) -> (reqwest::StatusCode, serde_json::Value) {
    let resp = match key {
        Some(k) => {
            client
                .post(format!("{base}/v1/deliveries"))
                .header("idempotency-key", k)
                .body(body.as_bytes().to_vec())
                .send()
                .await
        }
        None => {
            client
                .post(format!("{base}/v1/deliveries"))
                .body(body.as_bytes().to_vec())
                .send()
                .await
        }
    }
    .unwrap();
    let status = resp.status();
    let value: serde_json::Value = resp.json().await.unwrap_or(json!(null));
    (status, value)
}

#[tokio::test]
async fn health_returns_ok() {
    let (client, base, _, _) = start().await;
    let (status, body) = get(&client, &base, "/health").await;
    assert_eq!(status, reqwest::StatusCode::OK);
    assert_eq!(body.get("status").unwrap(), &json!("ok"));
}

#[tokio::test]
async fn starts_healthy_when_database_file_does_not_exist() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("missing.db");
    let url = format!("sqlite://{}", db_path.display());

    // The database file must genuinely be absent before startup, matching a
    // fresh install where no `.db` file exists yet.
    assert!(!db_path.exists());

    let (client, base, handle) = start_from(&url).await;
    assert!(db_path.exists());

    let (status, body) = get(&client, &base, "/health").await;
    assert_eq!(status, reqwest::StatusCode::OK);
    assert_eq!(body.get("status").unwrap(), &json!("ok"));

    // Migrations must have run against the newly created database.
    let (status, body) = enqueue(
        &client,
        &base,
        "missing-k",
        "https://example.test/webhooks",
        &json!({"a": 1}),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::CREATED);
    assert!(body.get("id").is_some());

    handle.abort();
    drop(dir);
}

#[tokio::test]
async fn new_enqueue_returns_201_with_representation() {
    let (client, base, _, dir) = start().await;
    let (status, body) = enqueue(
        &client,
        &base,
        "rk",
        "https://example.test/webhooks",
        &json!({"event": "invoice.created", "invoice_id": "inv_123"}),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::CREATED);
    assert!(body.get("id").is_some());
    assert_eq!(s(&body, "status"), "pending");
    assert_eq!(u64_of(&body, "attempts"), 0);
    assert_eq!(s(&body, "target_url"), "https://example.test/webhooks");
    assert_eq!(
        value_of(&body, "payload"),
        &json!({"event": "invoice.created", "invoice_id": "inv_123"})
    );
    assert!(body.get("created_at").is_some());
    drop(dir);
}

#[tokio::test]
async fn target_url_stored_verbatim() {
    let (client, base, _, dir) = start().await;
    let (status, body) = enqueue(
        &client,
        &base,
        "tvk",
        "https://a.test/webhooks/?b=2&a=1",
        &json!({"a": 1}),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::CREATED);
    let id = body.get("id").unwrap().as_str().unwrap().to_string();
    let (status2, body2) = get(&client, &base, &format!("/v1/deliveries/{id}")).await;
    assert_eq!(status2, reqwest::StatusCode::OK);
    assert_eq!(s(&body2, "target_url"), "https://a.test/webhooks/?b=2&a=1");
    assert_eq!(value_of(&body2, "payload"), &json!({"a": 1}));
    drop(dir);
}

#[tokio::test]
async fn payload_accepts_various_json_types() {
    let (client, base, _, dir) = start().await;
    let payloads = [
        json!("hello"),
        json!(42),
        json!(true),
        json!(null),
        json!([1, 2, 3]),
        json!({"nested": {"x": 1}}),
    ];
    for (i, p) in payloads.iter().enumerate() {
        let (status, body) = enqueue(
            &client,
            &base,
            &format!("pk{i}"),
            "https://example.test/webhooks",
            p,
        )
        .await;
        assert_eq!(status, reqwest::StatusCode::CREATED);
        assert_eq!(value_of(&body, "payload"), p);
    }
    drop(dir);
}

#[tokio::test]
async fn large_integer_payload_round_trips() {
    let big: u128 = 123456789012345678901234567890;
    let (client, base, _, dir) = start().await;
    let (status, body) = enqueue(
        &client,
        &base,
        "bigk",
        "https://example.test/webhooks",
        &json!({"n": big}),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::CREATED);
    assert_eq!(value_of(&body, "payload"), &json!({"n": big}));
    let id = body.get("id").unwrap().as_str().unwrap().to_string();
    let (status2, body2) = get(&client, &base, &format!("/v1/deliveries/{id}")).await;
    assert_eq!(status2, reqwest::StatusCode::OK);
    assert_eq!(value_of(&body2, "payload"), &json!({"n": big}));
    drop(dir);
}

#[tokio::test]
async fn get_existing_returns_200_and_fields() {
    let (client, base, _, dir) = start().await;
    let (status, body) = enqueue(
        &client,
        &base,
        "gk",
        "https://example.test/webhooks",
        &json!({"a": 1}),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::CREATED);
    let id = body.get("id").unwrap().as_str().unwrap().to_string();
    let (status2, body2) = get(&client, &base, &format!("/v1/deliveries/{id}")).await;
    assert_eq!(status2, reqwest::StatusCode::OK);
    assert_eq!(s(&body2, "id"), id.as_str());
    assert_eq!(s(&body2, "status"), "pending");
    assert_eq!(u64_of(&body2, "attempts"), 0);
    assert_eq!(s(&body2, "target_url"), "https://example.test/webhooks");
    assert_eq!(value_of(&body2, "payload"), &json!({"a": 1}));
    assert!(body2.get("created_at").is_some());
    drop(dir);
}

#[tokio::test]
async fn get_unknown_returns_404() {
    let (client, base, _, _) = start().await;
    let (status, body) = get(
        &client,
        &base,
        "/v1/deliveries/00000000-0000-0000-0000-000000000000",
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::NOT_FOUND);
    assert_eq!(error_code(&body), "delivery_not_found");
}

#[tokio::test]
async fn get_malformed_uuid_returns_404() {
    let (client, base, _, _) = start().await;
    let (status, body) = get(&client, &base, "/v1/deliveries/not-a-uuid").await;
    assert_eq!(status, reqwest::StatusCode::NOT_FOUND);
    assert_eq!(error_code(&body), "delivery_not_found");
}

#[tokio::test]
async fn uri_percent_encoded_non_uuid_returns_404() {
    let (client, base, _, _) = start().await;
    let (status, body) = get(&client, &base, "/v1/deliveries/%FF").await;
    assert_eq!(status, reqwest::StatusCode::NOT_FOUND);
    assert_eq!(error_code(&body), "delivery_not_found");
}

#[tokio::test]
async fn replay_same_content_returns_200_same_id() {
    let (client, base, _, dir) = start().await;
    let (s1, b1) = enqueue(
        &client,
        &base,
        "rk",
        "https://example.test/webhooks",
        &json!({"a": 1}),
    )
    .await;
    assert_eq!(s1, reqwest::StatusCode::CREATED);
    let id1 = b1.get("id").unwrap().as_str().unwrap().to_string();
    let (s2, b2) = enqueue(
        &client,
        &base,
        "rk",
        "https://example.test/webhooks",
        &json!({"a": 1}),
    )
    .await;
    assert_eq!(s2, reqwest::StatusCode::OK);
    assert_eq!(s(&b2, "id"), id1.as_str());
    drop(dir);
}

#[tokio::test]
async fn replay_payload_key_order_insensitive() {
    let (client, base, _, dir) = start().await;
    let (s1, b1) = enqueue(
        &client,
        &base,
        "rk",
        "https://example.test/webhooks",
        &json!({"a": 1, "b": 2}),
    )
    .await;
    assert_eq!(s1, reqwest::StatusCode::CREATED);
    let id1 = b1.get("id").unwrap().as_str().unwrap().to_string();
    let (s2, b2) = enqueue(
        &client,
        &base,
        "rk",
        "https://example.test/webhooks",
        &json!({"b": 2, "a": 1}),
    )
    .await;
    assert_eq!(s2, reqwest::StatusCode::OK);
    assert_eq!(s(&b2, "id"), id1.as_str());
    drop(dir);
}

#[tokio::test]
async fn conflict_different_url_returns_409() {
    let (client, base, _, dir) = start().await;
    enqueue(
        &client,
        &base,
        "ck",
        "https://a.test/webhooks",
        &json!({"a": 1}),
    )
    .await;
    let (status, body) = enqueue(
        &client,
        &base,
        "ck",
        "https://b.test/webhooks",
        &json!({"a": 1}),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::CONFLICT);
    assert_eq!(error_code(&body), "idempotency_conflict");
    drop(dir);
}

#[tokio::test]
async fn conflict_different_payload_returns_409() {
    let (client, base, _, dir) = start().await;
    enqueue(
        &client,
        &base,
        "ck",
        "https://a.test/webhooks",
        &json!({"a": 1}),
    )
    .await;
    let (status, body) = enqueue(
        &client,
        &base,
        "ck",
        "https://a.test/webhooks",
        &json!({"a": 2}),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::CONFLICT);
    assert_eq!(error_code(&body), "idempotency_conflict");
    drop(dir);
}

#[tokio::test]
async fn conflict_leaves_original_unchanged() {
    let (client, base, _, dir) = start().await;
    let (s1, b1) = enqueue(
        &client,
        &base,
        "ck",
        "https://a.test/webhooks",
        &json!({"a": 1}),
    )
    .await;
    assert_eq!(s1, reqwest::StatusCode::CREATED);
    let id = b1.get("id").unwrap().as_str().unwrap().to_string();
    enqueue(
        &client,
        &base,
        "ck",
        "https://b.test/webhooks",
        &json!({"a": 9}),
    )
    .await;
    let (status2, body2) = get(&client, &base, &format!("/v1/deliveries/{id}")).await;
    assert_eq!(status2, reqwest::StatusCode::OK);
    assert_eq!(s(&body2, "target_url"), "https://a.test/webhooks");
    assert_eq!(value_of(&body2, "payload"), &json!({"a": 1}));
    drop(dir);
}

#[tokio::test]
async fn concurrent_same_key_single_row() {
    let (client, base, db_url, dir) = start().await;
    let key = "concurrent-key";
    let url = "https://example.test/webhooks";
    let payload = json!({"event": "x"});
    let mut handles = Vec::new();
    for i in 0..20 {
        let client = client.clone();
        let base = base.clone();
        let key = key.to_string();
        let url = url.to_string();
        let payload = payload.clone();
        let _ = i;
        handles.push(tokio::spawn(async move {
            enqueue(&client, &base, &key, &url, &payload).await
        }));
    }
    for h in handles {
        let (status, _) = h.await.unwrap();
        assert!(status == reqwest::StatusCode::OK || status == reqwest::StatusCode::CREATED);
    }
    let pool = relaybox::infrastructure::sqlite::open_pool(&db_url)
        .await
        .unwrap();
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) AS n FROM deliveries")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
    drop(dir);
}

#[tokio::test]
async fn persists_across_restart() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let url = format!("sqlite://{}", db_path.display());
    let (client, base, handle) = start_from(&url).await;

    let (status, body) = enqueue(
        &client,
        &base,
        "restart-key",
        "https://example.test/webhooks",
        &json!({"a": 1}),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::CREATED);
    let id = body.get("id").unwrap().as_str().unwrap().to_string();

    handle.abort();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let (client2, base2, _) = start_from(&url).await;
    let (status2, body2) = get(&client2, &base2, &format!("/v1/deliveries/{id}")).await;
    assert_eq!(status2, reqwest::StatusCode::OK);
    assert_eq!(s(&body2, "id"), id.as_str());
}

#[tokio::test]
async fn missing_idempotency_key_returns_400() {
    let (client, base, _, _) = start().await;
    let (status, body) = enqueue_raw(
        &client,
        &base,
        None,
        r#"{"target_url":"https://example.test/webhooks","payload":{"a":1}}"#,
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(error_code(&body), "missing_idempotency_key");
}

#[tokio::test]
async fn empty_idempotency_key_returns_400() {
    let (client, base, _, _) = start().await;
    let (status, body) = enqueue_raw(
        &client,
        &base,
        Some(""),
        r#"{"target_url":"https://example.test/webhooks","payload":{"a":1}}"#,
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(error_code(&body), "invalid_idempotency_key");
}

#[tokio::test]
async fn whitespace_idempotency_key_returns_400() {
    let (client, base, _, _) = start().await;
    let (status, body) = enqueue_raw(
        &client,
        &base,
        Some("   "),
        r#"{"target_url":"https://example.test/webhooks","payload":{"a":1}}"#,
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(error_code(&body), "invalid_idempotency_key");
}

#[tokio::test]
async fn long_idempotency_key_returns_400() {
    let (client, base, _, _) = start().await;
    let long = "a".repeat(129);
    let (status, body) = enqueue_raw(
        &client,
        &base,
        Some(&long),
        r#"{"target_url":"https://example.test/webhooks","payload":{"a":1}}"#,
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(error_code(&body), "invalid_idempotency_key");
}

#[tokio::test]
async fn boundary_idempotency_key_of_128_bytes_is_accepted() {
    let (client, base, _, dir) = start().await;
    let key = "a".repeat(128);
    let (status, _) = enqueue_raw(
        &client,
        &base,
        Some(&key),
        r#"{"target_url":"https://example.test/webhooks","payload":{"a":1}}"#,
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::CREATED);
    drop(dir);
}

#[tokio::test]
async fn tab_containing_idempotency_key_is_accepted() {
    let (client, base, _, dir) = start().await;
    let key = "foo\tbar";
    // A legal Idempotency-Key may embed a horizontal tab; the first use
    // must create a delivery rather than be treated as a missing header.
    let (status, _) = enqueue_raw(
        &client,
        &base,
        Some(key),
        r#"{"target_url":"https://example.test/webhooks","payload":{"a":1}}"#,
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::CREATED);

    // A replay with the same tab-containing key is stable.
    let (status, _) = enqueue_raw(
        &client,
        &base,
        Some(key),
        r#"{"target_url":"https://example.test/webhooks","payload":{"a":1}}"#,
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::OK);
    drop(dir);
}

#[tokio::test]
async fn idempotency_key_normalizes_surrounding_ascii_whitespace() {
    let (client, base, _, dir) = start().await;
    // Surrounding (space + tab) ASCII whitespace is trimmed; "  \tfoo\t  " -> "foo".
    let (status, _) = enqueue_raw(
        &client,
        &base,
        Some("  \tfoo\t  "),
        r#"{"target_url":"https://example.test/webhooks","payload":{"a":1}}"#,
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::CREATED);

    // A differently whitespace-padded key with the same trimmed value replays.
    let (status, _) = enqueue_raw(
        &client,
        &base,
        Some(" foo "),
        r#"{"target_url":"https://example.test/webhooks","payload":{"a":1}}"#,
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::OK);
    drop(dir);
}

#[tokio::test]
async fn non_utf8_idempotency_key_is_accepted_and_replayed() {
    let (client, base, _, dir) = start().await;
    let body = r#"{"target_url":"https://example.test/webhooks","payload":{"a":1}}"#;
    // "foo\x80bar": 0x80 is a legal header byte but is not valid UTF-8.
    let key = [0x66, 0x6f, 0x6f, 0x80, 0x62, 0x61, 0x72];
    let (status, _) = enqueue_bytes(&client, &base, Some(&key), body).await;
    assert_eq!(status, reqwest::StatusCode::CREATED);

    // A replay with the same raw bytes is stable.
    let (status, _) = enqueue_bytes(&client, &base, Some(&key), body).await;
    assert_eq!(status, reqwest::StatusCode::OK);
    drop(dir);
}

#[tokio::test]
async fn non_utf8_keys_distinct_when_bytes_differ() {
    let (client, base, _, dir) = start().await;
    let body = r#"{"target_url":"https://example.test/webhooks","payload":{"a":1}}"#;
    // Two keys that differ by a single non-UTF-8 byte must remain distinct;
    // this proves the key bytes are preserved losslessly, not collapsed.
    let key_a = [0x66, 0x6f, 0x6f, 0x80, 0x62, 0x61, 0x72];
    let key_b = [0x66, 0x6f, 0x6f, 0x81, 0x62, 0x61, 0x72];
    let (status, _) = enqueue_bytes(&client, &base, Some(&key_a), body).await;
    assert_eq!(status, reqwest::StatusCode::CREATED);
    let (status, _) = enqueue_bytes(&client, &base, Some(&key_b), body).await;
    assert_eq!(status, reqwest::StatusCode::CREATED);
    drop(dir);
}

#[tokio::test]
async fn malformed_json_returns_400() {
    let (client, base, _, _) = start().await;
    let (status, body) = enqueue_raw(&client, &base, Some("k"), r#"{not json"#).await;
    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(error_code(&body), "invalid_json");
}

#[tokio::test]
async fn invalid_target_url_returns_422() {
    let (client, base, _, _) = start().await;
    let (status, body) = enqueue(
        &client,
        &base,
        "k",
        "ftp://example.test/webhooks",
        &json!({"a": 1}),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(error_code(&body), "invalid_target_url");
}

#[tokio::test]
async fn target_url_no_host_returns_422() {
    let (client, base, _, _) = start().await;
    let (status, body) = enqueue(&client, &base, "k", "http://", &json!({"a": 1})).await;
    assert_eq!(status, reqwest::StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(error_code(&body), "invalid_target_url");
}

#[tokio::test]
async fn target_url_without_nonempty_host_returns_422() {
    let (client, base, _, dir) = start().await;
    let invalid_urls = [
        "http:///path",
        "https:///path",
        "http:example.com",
        "http:/example.com",
        "http://@/",
        "http://:8080/",
    ];
    for (i, url) in invalid_urls.iter().enumerate() {
        let (status, body) =
            enqueue(&client, &base, &format!("mh{i}"), url, &json!({"a": 1})).await;
        assert_eq!(
            status,
            reqwest::StatusCode::UNPROCESSABLE_ENTITY,
            "url {url:?}"
        );
        assert_eq!(error_code(&body), "invalid_target_url", "url {url:?}");
    }
    drop(dir);
}

#[tokio::test]
async fn missing_target_url_returns_422() {
    let (client, base, _, _) = start().await;
    let (status, body) = enqueue_raw(&client, &base, Some("k"), r#"{"payload":{"a":1}}"#).await;
    assert_eq!(status, reqwest::StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(error_code(&body), "invalid_target_url");
}

#[tokio::test]
async fn missing_payload_returns_422() {
    let (client, base, _, _) = start().await;
    let (status, body) = enqueue_raw(
        &client,
        &base,
        Some("k"),
        r#"{"target_url":"https://example.test/webhooks"}"#,
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(error_code(&body), "invalid_payload");
}

#[tokio::test]
async fn uri_percent_encoded_uuid_returns_200() {
    let (client, base, _, dir) = start().await;
    let (status, body) = enqueue(
        &client,
        &base,
        "enc-k",
        "https://example.test/webhooks",
        &json!({"a": 1}),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::CREATED);
    let id = body.get("id").unwrap().as_str().unwrap().to_string();
    let last = id.as_bytes().last().unwrap();
    let encoded = format!("{}%{:02X}", &id[..id.len() - 1], last);
    let (status2, body2) = get(&client, &base, &format!("/v1/deliveries/{encoded}")).await;
    assert_eq!(status2, reqwest::StatusCode::OK);
    assert_eq!(s(&body2, "id"), id.as_str());
    drop(dir);
}

#[tokio::test]
async fn large_payload_over_default_limit_is_accepted() {
    let (client, base, _, dir) = start().await;
    let big = "a".repeat(3_200_000);
    let body = format!(
        r#"{{"target_url":"https://example.test/webhooks","payload":"{}"}}"#,
        big
    );
    let (status, _) = enqueue_raw(&client, &base, Some("large-k"), &body).await;
    assert_eq!(status, reqwest::StatusCode::CREATED);
    drop(dir);
}

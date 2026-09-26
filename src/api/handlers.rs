use axum::body::Bytes;
use axum::extract::{FromRequest, Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use serde_json::Value;
use url::Url;
use uuid::Uuid;

use crate::api::errors::ApiError;
use crate::api::state::AppState;
use crate::domain::delivery::{Delivery, EnqueueOutcome};

fn hex_to_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn percent_decode_segment(segment: &str) -> Option<Vec<u8>> {
    let bytes = segment.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'%' {
            if i + 2 >= bytes.len() {
                return None;
            }
            let hi = hex_to_val(bytes[i + 1])?;
            let lo = hex_to_val(bytes[i + 2])?;
            out.push((hi << 4) | lo);
            i += 3;
        } else {
            out.push(b);
            i += 1;
        }
    }
    Some(out)
}

/// Encode a byte slice as a lowercase hex string.
///
/// The `Idempotency-Key` is a byte string; hex is a lossless encoding that can
/// be stored in a UTF-8 `TEXT` column without imposing a UTF-8 requirement on the
/// key.
fn hex_encode(bytes: &[u8]) -> String {
    const HEX: [u8; 16] = *b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

pub async fn health() -> (StatusCode, Json<Value>) {
    (StatusCode::OK, Json(serde_json::json!({ "status": "ok" })))
}

fn delivery_json(delivery: &Delivery) -> Value {
    serde_json::json!({
        "id": delivery.id.to_string(),
        "status": delivery.status_str(),
        "attempts": delivery.attempts,
        "target_url": delivery.target_url,
        "payload": &delivery.payload,
        "created_at": delivery.created_at.to_rfc3339(),
    })
}

fn err(e: ApiError) -> (StatusCode, Json<Value>) {
    let (status, body) = e.response();
    (status, Json(body))
}

fn has_nonempty_host(raw: &str, scheme_len: usize) -> bool {
    let after_scheme = match raw.get(scheme_len..) {
        Some(after_scheme) => after_scheme,
        None => return false,
    };
    let rest = match after_scheme.strip_prefix("://") {
        Some(rest) => rest,
        None => return false,
    };
    let mut authority_end = rest.len();
    for c in ['/', '?', '#'] {
        if let Some(i) = rest.find(c) {
            authority_end = authority_end.min(i);
        }
    }
    let authority = &rest[..authority_end];
    if authority.is_empty() {
        return false;
    }
    let host_and_port = match authority.rfind('@') {
        Some(i) => &authority[i + 1..],
        None => authority,
    };
    let host = if host_and_port.starts_with('[') {
        match host_and_port.find(']') {
            Some(i) => &host_and_port[..i],
            None => return false,
        }
    } else {
        match host_and_port.find(':') {
            Some(i) => &host_and_port[..i],
            None => host_and_port,
        }
    };
    !host.is_empty()
}

pub async fn enqueue(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> (StatusCode, Json<Value>) {
    let key = match headers.get("idempotency-key").map(|value| value.as_bytes()) {
        Some(bytes) => {
            let trimmed = bytes.trim_ascii();
            if trimmed.is_empty() {
                return err(ApiError::InvalidIdempotencyKey);
            }
            if trimmed.len() > 128 {
                return err(ApiError::InvalidIdempotencyKey);
            }
            // The key is a byte string with no UTF-8 requirement. Encode it
            // losslessly so it can be persisted in the UTF-8 `TEXT` column.
            hex_encode(trimmed)
        }
        None => return err(ApiError::MissingIdempotencyKey),
    };

    let mut parsed: Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(_) => return err(ApiError::InvalidJson),
    };
    if parsed.as_object().is_none() {
        return err(ApiError::InvalidTargetUrl);
    }

    let target_url = match parsed.get("target_url").and_then(|value| value.as_str()) {
        Some(url) => url.to_string(),
        None => return err(ApiError::InvalidTargetUrl),
    };

    let payload = match parsed
        .as_object_mut()
        .and_then(|object| object.remove("payload"))
    {
        Some(payload) => payload,
        None => return err(ApiError::InvalidPayload),
    };

    let parsed_url = match Url::parse(&target_url) {
        Ok(url) => url,
        Err(_) => return err(ApiError::InvalidTargetUrl),
    };
    let scheme_ok = parsed_url.scheme() == "http" || parsed_url.scheme() == "https";
    let host_ok = has_nonempty_host(&target_url, parsed_url.scheme().len());
    if !scheme_ok || !host_ok {
        return err(ApiError::InvalidTargetUrl);
    }

    let outcome = match state
        .enqueue
        .enqueue(key.as_str(), target_url.as_str(), payload)
        .await
    {
        Ok(outcome) => outcome,
        Err(_) => return err(ApiError::Internal),
    };

    match outcome {
        EnqueueOutcome::Created(delivery) => (StatusCode::CREATED, Json(delivery_json(&delivery))),
        EnqueueOutcome::Replayed(delivery) => (StatusCode::OK, Json(delivery_json(&delivery))),
        EnqueueOutcome::Conflict => err(ApiError::IdempotencyConflict),
    }
}

#[derive(Clone, Copy, Debug)]
pub struct DeliveryId(Uuid);

impl<S> FromRequest<S> for DeliveryId
where
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request(req: Request, _state: &S) -> Result<Self, ApiError> {
        let raw_segment = req.uri().path().split('/').next_back().unwrap_or("");
        let decoded_bytes =
            percent_decode_segment(raw_segment).ok_or(ApiError::DeliveryNotFound)?;
        let decoded = String::from_utf8(decoded_bytes).map_err(|_| ApiError::DeliveryNotFound)?;
        let id = decoded
            .parse::<Uuid>()
            .map_err(|_| ApiError::DeliveryNotFound)?;
        Ok(Self(id))
    }
}

pub async fn get_delivery(
    State(state): State<AppState>,
    id: DeliveryId,
) -> (StatusCode, Json<Value>) {
    match state.query.get(id.0).await {
        Ok(Some(delivery)) => (StatusCode::OK, Json(delivery_json(&delivery))),
        Ok(None) => err(ApiError::DeliveryNotFound),
        Err(_) => err(ApiError::Internal),
    }
}

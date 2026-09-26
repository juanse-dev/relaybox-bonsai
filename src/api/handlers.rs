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

pub async fn enqueue(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> (StatusCode, Json<Value>) {
    let key = match headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
    {
        Some(raw) if !raw.trim().is_empty() => {
            let trimmed = raw.trim().to_string();
            if trimmed.len() > 128 {
                return err(ApiError::InvalidIdempotencyKey);
            }
            trimmed
        }
        Some(_) => return err(ApiError::InvalidIdempotencyKey),
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
    let host_ok = parsed_url.host_str().is_some();
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
        let id = raw_segment
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

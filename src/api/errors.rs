use axum::http::StatusCode;
use axum::response::IntoResponse;

pub enum ApiError {
    InvalidJson,
    MissingIdempotencyKey,
    InvalidIdempotencyKey,
    InvalidTargetUrl,
    InvalidPayload,
    IdempotencyConflict,
    DeliveryNotFound,
    Internal,
}

impl ApiError {
    pub fn status(&self) -> StatusCode {
        match self {
            Self::InvalidJson | Self::MissingIdempotencyKey | Self::InvalidIdempotencyKey => {
                StatusCode::BAD_REQUEST
            }
            Self::InvalidTargetUrl | Self::InvalidPayload => StatusCode::UNPROCESSABLE_ENTITY,
            Self::IdempotencyConflict => StatusCode::CONFLICT,
            Self::DeliveryNotFound => StatusCode::NOT_FOUND,
            Self::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    pub fn code(&self) -> &'static str {
        match self {
            Self::InvalidJson => "invalid_json",
            Self::MissingIdempotencyKey => "missing_idempotency_key",
            Self::InvalidIdempotencyKey => "invalid_idempotency_key",
            Self::InvalidTargetUrl => "invalid_target_url",
            Self::InvalidPayload => "invalid_payload",
            Self::IdempotencyConflict => "idempotency_conflict",
            Self::DeliveryNotFound => "delivery_not_found",
            Self::Internal => "internal_error",
        }
    }

    pub fn message(&self) -> &'static str {
        match self {
            Self::InvalidJson => "request body is not valid JSON",
            Self::MissingIdempotencyKey => "Idempotency-Key header is required",
            Self::InvalidIdempotencyKey => {
                "Idempotency-Key must be non-empty and at most 128 bytes"
            }
            Self::InvalidTargetUrl => {
                "target_url must be an absolute http or https URL with a host"
            }
            Self::InvalidPayload => "payload is required and must be a JSON value",
            Self::IdempotencyConflict => {
                "this Idempotency-Key has already been used with a different request"
            }
            Self::DeliveryNotFound => "delivery not found",
            Self::Internal => "internal server error",
        }
    }

    pub fn response(self) -> (StatusCode, serde_json::Value) {
        (
            self.status(),
            serde_json::json!({
                "error": {
                    "code": self.code(),
                    "message": self.message(),
                }
            }),
        )
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let (status, body) = self.response();
        (status, axum::Json(body)).into_response()
    }
}

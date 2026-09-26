use chrono::{DateTime, Utc};
use serde_json::Value;
use uuid::Uuid;

/// A delivery that Relaybox will retry until it succeeds.
#[derive(Debug, Clone, PartialEq)]
pub struct Delivery {
    pub id: Uuid,
    pub status: Status,
    pub attempts: u32,
    pub target_url: String,
    pub payload: Value,
    pub created_at: DateTime<Utc>,
}

/// The lifecycle status of a delivery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Status {
    Pending,
}

impl Status {
    pub fn status_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
        }
    }
}

impl Delivery {
    pub fn status_str(&self) -> &'static str {
        self.status.status_str()
    }
}

/// The outcome of enqueuing a delivery.
#[derive(Debug, Clone, PartialEq)]
pub enum EnqueueOutcome {
    Created(Delivery),
    Replayed(Delivery),
    Conflict,
}

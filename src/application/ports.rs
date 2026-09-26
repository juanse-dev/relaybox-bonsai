use async_trait::async_trait;

use crate::application::errors::RepositoryError;
use crate::domain::delivery::Delivery;
use chrono::{DateTime, Utc};
use serde_json::Value;
use uuid::Uuid;

/// Metadata for a freshly inserted delivery.
///
/// The repository persists the serialized payload but does not hand back the
/// owned JSON tree (that would force a clone). The service moves its owned
/// `payload` into the `Delivery` it builds.
#[derive(Debug, Clone)]
pub struct InsertedDelivery {
    pub id: Uuid,
    pub created_at: DateTime<Utc>,
}

/// The raw result of the atomic "insert or report existing" operation.
#[derive(Debug, Clone)]
pub enum EnqueueInsertResult {
    Inserted(InsertedDelivery),
    AlreadyExists(Delivery),
}

/// The contract the application services use to talk to a persistent store.
///
/// A trait so that tests can substitute a fake implementation, and because the
/// real implementation is I/O-bound (async).
#[async_trait]
pub trait DeliveryRepository: Send + Sync {
    /// Persist a delivery under `idempotency_key`.
    ///
    /// Returns the atomic result: either a newly-inserted delivery or the
    /// delivery that already occupied the key. Deciding replay vs. conflict is
    /// the application service's job.
    async fn enqueue(
        &self,
        idempotency_key: &str,
        target_url: &str,
        payload: &Value,
    ) -> Result<EnqueueInsertResult, RepositoryError>;

    /// Return the delivery with the given id, or `None` if it does not exist.
    async fn get(&self, id: Uuid) -> Result<Option<Delivery>, RepositoryError>;
}

use async_trait::async_trait;

use crate::application::errors::RepositoryError;
use crate::domain::delivery::{Delivery, EnqueueOutcome};
use serde_json::Value;
use uuid::Uuid;

/// The contract the application services use to talk to a persistent store.
///
/// A trait so that tests can substitute a fake implementation, and because the
/// real implementation is I/O-bound (async).
#[async_trait]
pub trait DeliveryRepository: Send + Sync {
    /// Persist a delivery under `idempotency_key` and report which of the three
    /// idempotency outcomes occurred:
    ///
    /// - `Created`: the key was new, a fresh delivery was stored.
    /// - `Replayed`: the key existed with *identical* content; the existing
    ///   delivery is returned (no new row).
    /// - `Conflict`: the key existed with *different* content; nothing was
    ///   written and the caller should answer 409.
    async fn enqueue(
        &self,
        idempotency_key: &str,
        target_url: &str,
        payload: Value,
    ) -> Result<EnqueueOutcome, RepositoryError>;

    /// Return the delivery with the given id, or `None` if it does not exist.
    async fn get(&self, id: Uuid) -> Result<Option<Delivery>, RepositoryError>;
}

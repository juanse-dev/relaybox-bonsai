use crate::application::errors::ApplicationError;
use crate::application::ports::DeliveryRepository;
use crate::domain::delivery::EnqueueOutcome;
use serde_json::Value;
use std::sync::Arc;

pub struct EnqueueService {
    repo: Arc<dyn DeliveryRepository>,
}

impl EnqueueService {
    pub fn new(repo: Arc<dyn DeliveryRepository>) -> Self {
        Self { repo }
    }

    pub async fn enqueue(
        &self,
        idempotency_key: &str,
        target_url: &str,
        payload: Value,
    ) -> Result<EnqueueOutcome, ApplicationError> {
        Ok(self
            .repo
            .enqueue(idempotency_key, target_url, payload)
            .await?)
    }
}

use crate::application::errors::ApplicationError;
use crate::application::ports::{DeliveryRepository, EnqueueInsertResult};
use crate::domain::delivery::{Delivery, EnqueueOutcome, Status};
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
        let insert = self
            .repo
            .enqueue(idempotency_key, target_url, &payload)
            .await?;

        Ok(match insert {
            EnqueueInsertResult::Inserted(meta) => EnqueueOutcome::Created(Delivery {
                id: meta.id,
                status: Status::Pending,
                attempts: 0,
                target_url: target_url.to_string(),
                payload,
                created_at: meta.created_at,
            }),
            EnqueueInsertResult::AlreadyExists(existing) => {
                if existing.target_url == target_url && existing.payload == payload {
                    EnqueueOutcome::Replayed(existing)
                } else {
                    EnqueueOutcome::Conflict
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::EnqueueService;
    use crate::application::errors::RepositoryError;
    use crate::application::ports::{DeliveryRepository, EnqueueInsertResult, InsertedDelivery};
    use crate::domain::delivery::{Delivery, EnqueueOutcome, Status};
    use async_trait::async_trait;
    use chrono::Utc;
    use serde_json::{json, Value};
    use std::sync::Arc;
    use uuid::Uuid;

    fn delivery(target_url: &str, payload: Value) -> Delivery {
        Delivery {
            id: Uuid::new_v4(),
            status: Status::Pending,
            attempts: 0,
            target_url: target_url.to_string(),
            payload,
            created_at: Utc::now(),
        }
    }

    #[derive(Debug)]
    struct FakeRepo {
        result: EnqueueInsertResult,
    }

    #[async_trait]
    impl DeliveryRepository for FakeRepo {
        async fn enqueue(
            &self,
            _idempotency_key: &str,
            _target_url: &str,
            _payload: &Value,
        ) -> Result<EnqueueInsertResult, RepositoryError> {
            Ok(self.result.clone())
        }

        async fn get(&self, _id: Uuid) -> Result<Option<Delivery>, RepositoryError> {
            Ok(None)
        }
    }

    async fn run(result: EnqueueInsertResult, target_url: &str, payload: Value) -> EnqueueOutcome {
        let service = EnqueueService::new(Arc::new(FakeRepo { result }));
        service.enqueue("k", target_url, payload).await.unwrap()
    }

    #[tokio::test]
    async fn inserted_maps_to_created() {
        let d = delivery("https://a.test/x", json!({"a": 1}));
        let outcome = run(
            EnqueueInsertResult::Inserted(InsertedDelivery {
                id: d.id,
                created_at: d.created_at,
            }),
            "https://a.test/x",
            json!({"a": 1}),
        )
        .await;
        match &outcome {
            EnqueueOutcome::Created(delivery) => {
                assert_eq!(delivery.id, d.id);
                assert_eq!(delivery.target_url, "https://a.test/x");
                assert_eq!(delivery.payload, json!({"a": 1}));
            }
            other => panic!("expected Created, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn already_exists_same_maps_to_replayed() {
        let existing = delivery("https://a.test/x", json!({"a": 1}));
        let outcome = run(
            EnqueueInsertResult::AlreadyExists(existing.clone()),
            "https://a.test/x",
            json!({"a": 1}),
        )
        .await;
        assert_eq!(outcome, EnqueueOutcome::Replayed(existing));
    }

    #[tokio::test]
    async fn already_exists_diff_target_maps_to_conflict() {
        let existing = delivery("https://a.test/x", json!({"a": 1}));
        let outcome = run(
            EnqueueInsertResult::AlreadyExists(existing.clone()),
            "https://b.test/x",
            json!({"a": 1}),
        )
        .await;
        assert_eq!(outcome, EnqueueOutcome::Conflict);
    }

    #[tokio::test]
    async fn already_exists_diff_payload_maps_to_conflict() {
        let existing = delivery("https://a.test/x", json!({"a": 1}));
        let outcome = run(
            EnqueueInsertResult::AlreadyExists(existing.clone()),
            "https://a.test/x",
            json!({"a": 2}),
        )
        .await;
        assert_eq!(outcome, EnqueueOutcome::Conflict);
    }
}

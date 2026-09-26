use crate::application::errors::ApplicationError;
use crate::application::ports::DeliveryRepository;
use crate::domain::delivery::Delivery;
use std::sync::Arc;
use uuid::Uuid;

pub struct QueryService {
    repo: Arc<dyn DeliveryRepository>,
}

impl QueryService {
    pub fn new(repo: Arc<dyn DeliveryRepository>) -> Self {
        Self { repo }
    }

    pub async fn get(&self, id: Uuid) -> Result<Option<Delivery>, ApplicationError> {
        Ok(self.repo.get(id).await?)
    }
}

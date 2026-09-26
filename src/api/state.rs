use std::sync::Arc;

use crate::application::{EnqueueService, QueryService};

#[derive(Clone)]
pub struct AppState {
    pub enqueue: Arc<EnqueueService>,
    pub query: Arc<QueryService>,
}

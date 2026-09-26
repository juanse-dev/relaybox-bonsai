use std::net::SocketAddr;

use anyhow::Result;
use axum::Router;

use crate::api::state::AppState;
use crate::application::{EnqueueService, QueryService};
use crate::infrastructure::migrations;
use crate::infrastructure::sqlite::{open_pool, SqliteDeliveryRepository};

pub struct App {
    pub router: Router,
}

impl App {
    pub async fn create(db_url: &str) -> Result<Self> {
        let pool = open_pool(db_url).await?;
        migrations::run_migrations(&pool).await?;

        let repo: std::sync::Arc<dyn crate::application::ports::DeliveryRepository> =
            std::sync::Arc::new(SqliteDeliveryRepository::new(pool.clone()));
        let enqueue = std::sync::Arc::new(EnqueueService::new(repo.clone()));
        let query = std::sync::Arc::new(QueryService::new(repo.clone()));

        let state = AppState { enqueue, query };
        let router = axum::Router::new()
            .route(
                "/v1/deliveries",
                axum::routing::post(crate::api::handlers::enqueue),
            )
            .route(
                "/v1/deliveries/{delivery_id}",
                axum::routing::get(crate::api::handlers::get_delivery),
            )
            .route("/health", axum::routing::get(crate::api::handlers::health))
            .layer(axum::extract::DefaultBodyLimit::disable())
            .with_state(state);

        Ok(Self { router })
    }

    pub async fn run(self, _bind: SocketAddr) -> Result<()> {
        let listener = tokio::net::TcpListener::bind(_bind).await?;
        let app = self.router;
        axum::serve(listener, app).await?;
        Ok(())
    }
}

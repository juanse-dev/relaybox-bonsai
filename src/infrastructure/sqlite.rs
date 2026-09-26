use anyhow::Context;
use async_trait::async_trait;

use crate::application::errors::RepositoryError;
use crate::application::ports::{DeliveryRepository, EnqueueInsertResult, InsertedDelivery};
use crate::domain::delivery::{Delivery, Status};

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::sqlite::{Sqlite, SqliteConnectOptions, SqliteRow};
use sqlx::{Pool, Row};
use std::time::Duration;
use uuid::Uuid;

/// Durable delivery store backed by SQLite through the `sqlx` pool.
pub struct SqliteDeliveryRepository {
    pool: Pool<Sqlite>,
}

impl SqliteDeliveryRepository {
    /// Create a repository on top of an already-opened pool.
    ///
    /// The pool is owned and shared across requests; each `enqueue`/`get`
    /// acquires a short-lived connection.
    pub fn new(pool: Pool<Sqlite>) -> Self {
        Self { pool }
    }
}

/// Open a SQLite pool for the given URL.
///
/// The URL must be a `sqlite://` URL. A generous busy timeout is set so that
/// concurrent writes (e.g. idempotency races) do not surface as spurious
/// "database is locked" errors.
pub async fn open_pool(db_url: &str) -> anyhow::Result<Pool<Sqlite>> {
    let opts: SqliteConnectOptions = db_url
        .parse::<SqliteConnectOptions>()
        .with_context(|| format!("invalid sqlite url {db_url:?}"))?
        .create_if_missing(true)
        .busy_timeout(Duration::from_secs(15));

    sqlx::Pool::connect_with(opts)
        .await
        .with_context(|| format!("connect to sqlite {db_url:?}"))
}

/// True if the `sqlx` error is a SQLite unique-constraint violation.
fn is_unique_violation(e: &sqlx::Error) -> bool {
    e.as_database_error()
        .is_some_and(|de| de.is_unique_violation())
}

/// Decode one row from the `deliveries` table into a `Delivery`.
fn row_to_delivery(row: &SqliteRow) -> Result<Delivery, RepositoryError> {
    let id = row
        .get::<String, _>("id")
        .parse()
        .map_err(|e| RepositoryError::Db(format!("bad id: {e}")))?;

    let status = match row.get::<String, _>("status").as_str() {
        "pending" => Status::Pending,
        other => return Err(RepositoryError::Db(format!("unknown status {other:?}"))),
    };

    let attempts = row.get::<i64, _>("attempts") as u32;

    let target_url = row.get::<String, _>("target_url");

    let payload = serde_json::from_str(&row.get::<String, _>("payload"))
        .map_err(|e| RepositoryError::Db(format!("bad payload: {e}")))?;

    // `parse_from_rfc3339` yields a `DateTime<FixedOffset>`; normalize to UTC.
    let created_at_raw = row.get::<String, _>("created_at");
    let created_at = DateTime::parse_from_rfc3339(&created_at_raw)
        .map_err(|e| RepositoryError::Db(format!("bad created_at: {e}")))?
        .with_timezone(&Utc);

    Ok(Delivery {
        id,
        status,
        attempts,
        target_url,
        payload,
        created_at,
    })
}

#[async_trait]
impl DeliveryRepository for SqliteDeliveryRepository {
    async fn enqueue(
        &self,
        idempotency_key: &str,
        target_url: &str,
        payload: &Value,
    ) -> Result<EnqueueInsertResult, RepositoryError> {
        let id = Uuid::new_v4();
        let now = Utc::now();

        let payload_json = serde_json::to_string(payload)
            .map_err(|e| RepositoryError::Db(format!("serialize payload: {e}")))?;

        let mut conn = self
            .pool
            .acquire()
            .await
            .map_err(|e| RepositoryError::Db(format!("acquire connection: {e}")))?;

        let result = sqlx::query(
            "INSERT INTO deliveries
                 (id, idempotency_key, target_url, payload, status, attempts, created_at)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        )
        .bind(id.to_string())
        .bind(idempotency_key)
        .bind(target_url)
        .bind(payload_json.as_str())
        .bind("pending")
        .bind(0)
        .bind(now.to_rfc3339())
        .execute(&mut *conn)
        .await;

        match result {
            Ok(_) => Ok(EnqueueInsertResult::Inserted(InsertedDelivery {
                id,
                created_at: now,
            })),
            Err(e) => {
                if !is_unique_violation(&e) {
                    return Err(RepositoryError::Db(format!("insert failed: {e}")));
                }
                // The winner of the idempotency race has already committed, so
                // the existing row is now visible (READ COMMITTED). Re-read it.
                let row = sqlx::query(
                    "SELECT id, status, attempts, target_url, payload, created_at
                     FROM deliveries
                     WHERE idempotency_key = ?1",
                )
                .bind(idempotency_key)
                .fetch_one(&mut *conn)
                .await
                .map_err(|e| RepositoryError::Db(format!("fetch existing row: {e}")))?;

                Ok(EnqueueInsertResult::AlreadyExists(row_to_delivery(&row)?))
            }
        }
    }

    async fn get(&self, id: Uuid) -> Result<Option<Delivery>, RepositoryError> {
        let row = sqlx::query(
            "SELECT id, status, attempts, target_url, payload, created_at
             FROM deliveries
             WHERE id = ?1",
        )
        .bind(id.to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| RepositoryError::Db(format!("get delivery: {e}")))?;

        match row {
            Some(row) => Ok(Some(row_to_delivery(&row)?)),
            None => Ok(None),
        }
    }
}

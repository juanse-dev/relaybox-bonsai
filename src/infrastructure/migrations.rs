use sqlx::sqlite::Sqlite;
use sqlx::{Pool, Row};

/// Apply all bundled migrations to `pool`, in filename order.
///
/// Each migration is recorded in the `schema_migrations` table so it is not
/// re-applied on restart. A migration that fails returns an error and does not
/// proceed to the next one.
pub async fn run_migrations(pool: &Pool<Sqlite>) -> anyhow::Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS schema_migrations (\n\
              sql_version TEXT PRIMARY KEY,\n\
              applied_at TIMESTAMP NOT NULL\n\
          )",
    )
    .execute(pool)
    .await?;

    for (version, sql) in MIGRATIONS.iter() {
        let applied =
            sqlx::query("SELECT COUNT(*) AS n FROM schema_migrations WHERE sql_version = ?1")
                .bind(version)
                .fetch_one(pool)
                .await?
                .get::<i64, _>("n");

        if applied > 0 {
            continue;
        }

        sqlx::query(sql).execute(pool).await?;

        sqlx::query("INSERT INTO schema_migrations (sql_version, applied_at) VALUES (?1, ?2)")
            .bind(version)
            .bind(chrono::Utc::now().to_rfc3339())
            .execute(pool)
            .await?;
    }

    Ok(())
}

/// The ordered list of `(version, script)` pairs.
///
/// The version is the primary key in `schema_migrations`; the numeric prefix
/// guarantees a stable application order.
const MIGRATIONS: &[(&str, &str)] = &[(
    "20250101_001_create_deliveries",
    include_str!("../../migrations/001_create_deliveries.sql"),
)];

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn run_migrations_applies_and_is_idempotent() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        std::fs::write(&db_path, b"").unwrap();
        let db_url = format!("sqlite://{}", db_path.display());

        let pool = crate::infrastructure::sqlite::open_pool(&db_url)
            .await
            .unwrap();
        run_migrations(&pool).await.unwrap();
        run_migrations(&pool).await.unwrap();

        // The `deliveries` table must exist after migrations ran.
        let n = sqlx::query(
            "SELECT COUNT(*) AS n FROM sqlite_master WHERE type = 'table' AND name = 'deliveries'",
        )
        .fetch_one(&pool)
        .await
        .unwrap()
        .get::<i64, _>("n");
        assert_eq!(n, 1);
    }
}

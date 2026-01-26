use chrono::{DateTime, Utc};
use log::debug;
use sqlx::{FromRow, SqliteConnection};

/// Maximum number of block headers to store for reorg detection
const MAX_STORED_HEADERS: i64 = 2000;

#[derive(Debug, Clone, FromRow)]
pub struct BlockHeader {
    pub height: i64,
    pub header_hash: String,
    pub prev_hash: String,
    pub created_at: DateTime<Utc>,
}

impl BlockHeader {
    /// Inserts a new block header into the database.
    pub async fn upsert(
        pool: &mut SqliteConnection,
        height: u64,
        header_hash: &str,
        prev_hash: &str,
    ) -> Result<(), sqlx::Error> {
        let height = height as i64;
        sqlx::query!(
            r#"
            INSERT INTO block_headers (height, header_hash, prev_hash)
            VALUES (?, ?, ?)
            ON CONFLICT(height) DO UPDATE SET
                header_hash = excluded.header_hash,
                prev_hash = excluded.prev_hash,
                created_at = CURRENT_TIMESTAMP
            "#,
            height,
            header_hash,
            prev_hash
        )
        .execute(&mut *pool)
        .await?;
        Ok(())
    }

    /// Gets a block header by its height.
    pub async fn get_by_height(pool: &mut SqliteConnection, height: u64) -> Result<Option<Self>, sqlx::Error> {
        let height = height as i64;
        sqlx::query_as!(
            BlockHeader,
            r#"
            SELECT height, header_hash, prev_hash, created_at as "created_at: DateTime<Utc>"
            FROM block_headers WHERE height = ?
            "#,
            height
        )
        .fetch_optional(pool)
        .await
    }

    /// Gets a block header by its hash.
    pub async fn get_by_hash(pool: &mut SqliteConnection, header_hash: &str) -> Result<Option<Self>, sqlx::Error> {
        sqlx::query_as!(
            BlockHeader,
            r#"
            SELECT height, header_hash, prev_hash, created_at as "created_at: DateTime<Utc>"
            FROM block_headers WHERE header_hash = ?
            "#,
            header_hash
        )
        .fetch_optional(pool)
        .await
    }

    /// Gets the highest stored block header.
    pub async fn get_tip(pool: &mut SqliteConnection) -> Result<Option<Self>, sqlx::Error> {
        sqlx::query_as!(
            BlockHeader,
            r#"
            SELECT height, header_hash, prev_hash, created_at as "created_at: DateTime<Utc>"
            FROM block_headers ORDER BY height DESC LIMIT 1
            "#
        )
        .fetch_optional(pool)
        .await
    }

    /// Deletes all block headers at or above the given height.
    pub async fn delete_at_and_above_height(pool: &mut SqliteConnection, height: u64) -> Result<u64, sqlx::Error> {
        let height = height as i64;
        let result = sqlx::query!("DELETE FROM block_headers WHERE height >= ?", height)
            .execute(&mut *pool)
            .await?;
        Ok(result.rows_affected())
    }

    /// Prunes old block headers, keeping only the most recent MAX_STORED_HEADERS.
    pub async fn prune_old_headers(pool: &mut SqliteConnection) -> Result<u64, sqlx::Error> {
        let result = sqlx::query!(
            r#"DELETE FROM block_headers WHERE height < (SELECT MAX(height) - ? FROM block_headers)"#,
            MAX_STORED_HEADERS
        )
        .execute(&mut *pool)
        .await?;
        if result.rows_affected() > 0 {
            debug!(pruned = result.rows_affected(); "Pruned old block headers");
        }
        Ok(result.rows_affected())
    }

    /// Gets the count of stored headers.
    pub async fn count(pool: &mut SqliteConnection) -> Result<i64, sqlx::Error> {
        let result = sqlx::query_scalar!(r#"SELECT COUNT(*) as "count: i64" FROM block_headers"#)
            .fetch_one(pool)
            .await?;
        Ok(result.unwrap_or(0))
    }
}

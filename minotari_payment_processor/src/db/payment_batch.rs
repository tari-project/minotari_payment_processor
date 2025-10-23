use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{Connection, FromRow, SqliteConnection};
use std::fmt;
use utoipa::ToSchema;
use uuid::Uuid;

use crate::db::payment::{Payment, PaymentStatus};

const MAX_RETRIES: i64 = 10;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PaymentBatchStatus {
    PendingBatching,
    AwaitingSignature,
    SigningInProgress,
    AwaitingBroadcast,
    Broadcasting,
    AwaitingConfirmation,
    Confirmed,
    Failed,
}

impl From<String> for PaymentBatchStatus {
    fn from(s: String) -> Self {
        match s.as_str() {
            "PENDING_BATCHING" => PaymentBatchStatus::PendingBatching,
            "AWAITING_SIGNATURE" => PaymentBatchStatus::AwaitingSignature,
            "SIGNING_IN_PROGRESS" => PaymentBatchStatus::SigningInProgress,
            "AWAITING_BROADCAST" => PaymentBatchStatus::AwaitingBroadcast,
            "BROADCASTING" => PaymentBatchStatus::Broadcasting,
            "AWAITING_CONFIRMATION" => PaymentBatchStatus::AwaitingConfirmation,
            "CONFIRMED" => PaymentBatchStatus::Confirmed,
            "FAILED" => PaymentBatchStatus::Failed,
            _ => panic!("Unknown PaymentBatchStatus: {}", s),
        }
    }
}

impl fmt::Display for PaymentBatchStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PaymentBatchStatus::PendingBatching => write!(f, "PENDING_BATCHING"),
            PaymentBatchStatus::AwaitingSignature => write!(f, "AWAITING_SIGNATURE"),
            PaymentBatchStatus::SigningInProgress => write!(f, "SIGNING_IN_PROGRESS"),
            PaymentBatchStatus::AwaitingBroadcast => write!(f, "AWAITING_BROADCAST"),
            PaymentBatchStatus::Broadcasting => write!(f, "BROADCASTING"),
            PaymentBatchStatus::AwaitingConfirmation => write!(f, "AWAITING_CONFIRMATION"),
            PaymentBatchStatus::Confirmed => write!(f, "CONFIRMED"),
            PaymentBatchStatus::Failed => write!(f, "FAILED"),
        }
    }
}

#[derive(Debug, Clone, FromRow)]
pub struct PaymentBatch {
    pub id: String,
    pub account_name: String,
    pub status: PaymentBatchStatus,
    pub pr_idempotency_key: String,
    pub unsigned_tx_json: Option<String>,
    pub signed_tx_json: Option<String>,
    pub error_message: Option<String>,
    pub retry_count: i64,
    pub mined_height: Option<i64>,
    pub mined_header_hash: Option<String>,
    pub mined_timestamp: Option<i64>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Default)]
pub struct PaymentBatchUpdate<'a> {
    pub status: Option<PaymentBatchStatus>,
    pub unsigned_tx_json: Option<&'a str>,
    pub signed_tx_json: Option<&'a str>,
    pub error_message: Option<&'a str>,
    pub mined_height: Option<i64>,
    pub mined_header_hash: Option<&'a str>,
    pub mined_timestamp: Option<i64>,
}

impl PaymentBatch {
    /// Finds a payment batch by its ID.
    pub async fn find_by_id(pool: &mut SqliteConnection, batch_id: &str) -> Result<Option<Self>, sqlx::Error> {
        sqlx::query_as!(
            PaymentBatch,
            r#"
            SELECT
                id,
                account_name,
                status,
                pr_idempotency_key,
                unsigned_tx_json,
                signed_tx_json,
                error_message,
                retry_count,
                mined_height,
                mined_header_hash,
                mined_timestamp,
                created_at as "created_at: DateTime<Utc>",
                updated_at as "updated_at: DateTime<Utc>"
            FROM payment_batches
            WHERE id = ?
            "#,
            batch_id
        )
        .fetch_optional(pool)
        .await
    }

    /// Creates a new payment batch and updates the associated payments.
    pub async fn create_with_payments(
        pool: &mut SqliteConnection,
        account_name: &str,
        pr_idempotency_key: &str,
        payment_ids: &[String],
    ) -> Result<Self, sqlx::Error> {
        let mut tx = pool.begin().await?;
        let batch_id = Uuid::new_v4().to_string();
        let status = PaymentBatchStatus::PendingBatching.to_string();

        let batch = sqlx::query_as!(
            PaymentBatch,
            r#"
            INSERT INTO payment_batches (id, account_name, pr_idempotency_key, status)
            VALUES (?, ?, ?, ?)
            RETURNING
                id,
                account_name,
                status,
                pr_idempotency_key,
                unsigned_tx_json,
                signed_tx_json,
                error_message,
                retry_count,
                mined_height,
                mined_header_hash,
                mined_timestamp,
                created_at as "created_at: DateTime<Utc>",
                updated_at as "updated_at: DateTime<Utc>"
            "#,
            batch_id,
            account_name,
            pr_idempotency_key,
            status
        )
        .fetch_one(&mut *tx)
        .await?;

        let json = serde_json::to_string(payment_ids).unwrap();
        let status_batched = PaymentStatus::Batched.to_string();
        sqlx::query!(
            r#"
            UPDATE payments
            SET status = ?, payment_batch_id = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id IN (SELECT value FROM json_each(?))
            "#,
            status_batched,
            batch_id,
            json,
        )
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(batch)
    }

    /// Finds payment batches by their status.
    pub async fn find_by_status(
        pool: &mut SqliteConnection,
        status: PaymentBatchStatus,
    ) -> Result<Vec<Self>, sqlx::Error> {
        let status = status.to_string();
        sqlx::query_as!(
            PaymentBatch,
            r#"
            SELECT
                id,
                account_name,
                status,
                pr_idempotency_key,
                unsigned_tx_json,
                signed_tx_json,
                error_message,
                retry_count,
                mined_height,
                mined_header_hash,
                mined_timestamp,
                created_at as "created_at: DateTime<Utc>",
                updated_at as "updated_at: DateTime<Utc>"
            FROM payment_batches
            WHERE status = ?
            "#,
            status
        )
        .fetch_all(pool)
        .await
    }

    async fn update_payment_batch_status(
        pool: &mut SqliteConnection,
        batch_id: &str,
        update: &PaymentBatchUpdate<'_>,
        increment_retry_count: bool,
    ) -> Result<(), sqlx::Error> {
        let mut qb = sqlx::QueryBuilder::new("UPDATE payment_batches SET");
        let mut needs_comma = false;

        let mut separator = |qb: &mut sqlx::QueryBuilder<sqlx::Sqlite>| {
            if needs_comma {
                qb.push(", ");
            } else {
                qb.push(" ");
                needs_comma = true;
            }
        };

        // Always update the timestamp.
        separator(&mut qb);
        qb.push("updated_at = CURRENT_TIMESTAMP");

        if let Some(status) = &update.status {
            separator(&mut qb);
            qb.push("status = ").push_bind(status.to_string());
        }
        if let Some(json) = update.unsigned_tx_json {
            separator(&mut qb);
            qb.push("unsigned_tx_json = ").push_bind(json);
        }
        if let Some(json) = update.signed_tx_json {
            separator(&mut qb);
            qb.push("signed_tx_json = ").push_bind(json);
        }
        if let Some(msg) = update.error_message {
            separator(&mut qb);
            qb.push("error_message = ").push_bind(msg);
        }
        if let Some(height) = update.mined_height {
            separator(&mut qb);
            qb.push("mined_height = ").push_bind(height);
        }
        if let Some(hash) = update.mined_header_hash {
            separator(&mut qb);
            qb.push("mined_header_hash = ").push_bind(hash);
        }
        if let Some(timestamp) = update.mined_timestamp {
            separator(&mut qb);
            qb.push("mined_timestamp = ").push_bind(timestamp);
        }
        if increment_retry_count {
            separator(&mut qb);
            qb.push("retry_count = retry_count + 1");
        }

        qb.push(" WHERE id = ").push_bind(batch_id);
        qb.build().execute(pool).await?;

        Ok(())
    }

    /// Updates a payment batch to 'AWAITING_SIGNATURE' status with unsigned transaction details.
    pub async fn update_to_awaiting_signature(
        pool: &mut SqliteConnection,
        batch_id: &str,
        unsigned_tx_json: &str,
    ) -> Result<(), sqlx::Error> {
        let update = PaymentBatchUpdate {
            status: Some(PaymentBatchStatus::AwaitingSignature),
            unsigned_tx_json: Some(unsigned_tx_json),
            ..Default::default()
        };
        Self::update_payment_batch_status(pool, batch_id, &update, false).await
    }

    /// Updates a payment batch to 'SIGNING_IN_PROGRESS' status.
    pub async fn update_to_signing_in_progress(pool: &mut SqliteConnection, batch_id: &str) -> Result<(), sqlx::Error> {
        let update = PaymentBatchUpdate {
            status: Some(PaymentBatchStatus::SigningInProgress),
            ..Default::default()
        };
        Self::update_payment_batch_status(pool, batch_id, &update, false).await
    }

    /// Updates a payment batch to 'AWAITING_BROADCAST' status with signed transaction details.
    pub async fn update_to_awaiting_broadcast(
        pool: &mut SqliteConnection,
        batch_id: &str,
        signed_tx_json: &str,
    ) -> Result<(), sqlx::Error> {
        let update = PaymentBatchUpdate {
            status: Some(PaymentBatchStatus::AwaitingBroadcast),
            signed_tx_json: Some(signed_tx_json),
            ..Default::default()
        };
        Self::update_payment_batch_status(pool, batch_id, &update, false).await
    }

    /// Updates a payment batch to 'BROADCASTING' status.
    pub async fn update_to_broadcasting(pool: &mut SqliteConnection, batch_id: &str) -> Result<(), sqlx::Error> {
        let update = PaymentBatchUpdate {
            status: Some(PaymentBatchStatus::Broadcasting),
            ..Default::default()
        };
        Self::update_payment_batch_status(pool, batch_id, &update, false).await
    }

    /// Updates a payment batch to 'AWAITING_CONFIRMATION' status with the on-chain transaction hash.
    pub async fn update_to_awaiting_confirmation(
        pool: &mut SqliteConnection,
        batch_id: &str,
    ) -> Result<(), sqlx::Error> {
        let update = PaymentBatchUpdate {
            status: Some(PaymentBatchStatus::AwaitingConfirmation),
            ..Default::default()
        };
        Self::update_payment_batch_status(pool, batch_id, &update, false).await
    }

    /// Updates a payment batch to 'CONFIRMED' status.
    pub async fn update_to_confirmed(
        pool: &mut SqliteConnection,
        batch_id: &str,
        mined_height: u64,
        mined_header_hash: Vec<u8>,
        mined_timestamp: u64,
    ) -> Result<(), sqlx::Error> {
        let update = PaymentBatchUpdate {
            status: Some(PaymentBatchStatus::Confirmed),
            mined_height: Some(mined_height as i64),
            mined_header_hash: Some(&hex::encode(mined_header_hash)),
            mined_timestamp: Some(mined_timestamp as i64),
            ..Default::default()
        };
        Self::update_payment_batch_status(pool, batch_id, &update, false).await
    }

    /// Updates a payment batch to 'FAILED' status with an error message.
    pub async fn update_to_failed(
        pool: &mut SqliteConnection,
        batch_id: &str,
        error_message: &str,
    ) -> Result<(), sqlx::Error> {
        let mut tx = pool.begin().await?;

        let update = PaymentBatchUpdate {
            status: Some(PaymentBatchStatus::Failed),
            error_message: Some(error_message),
            ..Default::default()
        };
        Self::update_payment_batch_status(&mut tx, batch_id, &update, false).await?;
        Payment::fail_payments_in_batch(&mut tx, batch_id, error_message).await?;

        tx.commit().await?;
        Ok(())
    }

    /// Increments the retry count for a payment batch, or sets to FAILED if max retries reached.
    pub async fn increment_retry_count(
        pool: &mut SqliteConnection,
        batch_id: &str,
        error_message: &str,
    ) -> Result<(), sqlx::Error> {
        let mut tx = pool.begin().await?;

        let batch = Self::find_by_id(&mut tx, batch_id)
            .await?
            .ok_or_else(|| sqlx::Error::RowNotFound)?;

        if batch.retry_count + 1 >= MAX_RETRIES {
            let status_failed = PaymentBatchStatus::Failed;
            let update = PaymentBatchUpdate {
                status: Some(status_failed),
                error_message: Some(error_message),
                ..Default::default()
            };
            Self::update_payment_batch_status(&mut tx, batch_id, &update, false).await?;
            Payment::fail_payments_in_batch(&mut tx, batch_id, error_message).await?;
        } else {
            // No fields to update other than incrementing retry_count.
            let update = PaymentBatchUpdate::default();
            Self::update_payment_batch_status(&mut tx, batch_id, &update, true).await?;
        }

        tx.commit().await?;
        Ok(())
    }
}

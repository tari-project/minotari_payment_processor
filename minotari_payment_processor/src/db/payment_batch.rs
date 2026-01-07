use anyhow::Context;
use chrono::{DateTime, Utc};
use log::{debug, info, warn};
use serde::{Deserialize, Serialize};
use sqlx::{Connection, FromRow, SqliteConnection};
use std::fmt;
use tari_common_types::transaction::TxId;
use utoipa::ToSchema;
use uuid::Uuid;

use crate::{
    db::payment::{Payment, PaymentStatus},
    utils::log::mask_string,
};

const MAX_RETRIES: i64 = 10;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum StepPayload {
    /// The payload needed by the Console Wallet to generate a signature.
    Unsigned(String),
    /// The payload returned by the Console Wallet, ready for Broadcast.
    Signed(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionStep {
    pub step_index: usize,
    /// If true, this TX is consolidating inputs (spending to self).
    /// If false, this is the final payment to the client.
    pub is_consolidation: bool,
    pub payload: StepPayload,
    pub tx_id: TxId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchPayload {
    pub steps: Vec<TransactionStep>,
}

impl BatchPayload {
    pub fn from_json(json: &str) -> anyhow::Result<Self> {
        serde_json::from_str(json).context("Failed to deserialize BatchPayload")
    }

    pub fn to_json(&self) -> anyhow::Result<String> {
        serde_json::to_string(self).context("Failed to serialize BatchPayload")
    }
}

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
    Cancelled,
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
            "CANCELLED" => PaymentBatchStatus::Cancelled,
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
            PaymentBatchStatus::Cancelled => write!(f, "CANCELLED"),
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
    pub intermediate_context_json: Option<String>,
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
    pub intermediate_context_json: Option<&'a str>,
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
                intermediate_context_json,
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
        debug!(
            account = &*mask_string(account_name);
            "DB: Creating new payment batch"
        );
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
                intermediate_context_json,
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

        let json = serde_json::to_string(payment_ids).map_err(|e| sqlx::Error::Configuration(Box::new(e)))?;
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

        info!(
            target: "audit",
            batch_id = &*batch.id,
            account = account_name,
            count = payment_ids.len();
            "DB: Payment Batch Created"
        );
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
                intermediate_context_json,
                mined_height,
                mined_header_hash,
                mined_timestamp,
                created_at as "created_at: DateTime<Utc>",
                updated_at as "updated_at: DateTime<Utc>"
            FROM payment_batches
            WHERE status = ?
            ORDER BY created_at
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
        let status_log = update
            .status
            .as_ref()
            .map(|s| s.to_string())
            .unwrap_or_else(|| "No Change".to_string());
        let has_unsigned = update.unsigned_tx_json.is_some();
        let has_signed = update.signed_tx_json.is_some();
        let has_error = update.error_message.is_some();

        debug!(
            batch_id = batch_id,
            status = &*status_log,
            has_unsigned = has_unsigned,
            has_signed = has_signed,
            has_error = has_error,
            increment_retry_count = increment_retry_count;
            "DB: Updating Batch"
        );

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
        if let Some(context_json) = update.intermediate_context_json {
            separator(&mut qb);
            if context_json.is_empty() {
                qb.push("intermediate_context_json = NULL");
            } else {
                qb.push("intermediate_context_json = ").push_bind(context_json);
            }
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
        } else if let Some(new_status) = &update.status
            && !matches!(new_status, PaymentBatchStatus::Failed | PaymentBatchStatus::Cancelled)
        {
            separator(&mut qb);
            qb.push("retry_count = 0");
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
        intermediate_context_json: Option<&str>,
    ) -> Result<(), sqlx::Error> {
        let update = PaymentBatchUpdate {
            status: Some(PaymentBatchStatus::AwaitingBroadcast),
            signed_tx_json: Some(signed_tx_json),
            intermediate_context_json,
            ..Default::default()
        };
        Self::update_payment_batch_status(pool, batch_id, &update, false).await
    }

    /// Updates a payment batch to 'AWAITING_BROADCAST' status for retry.
    pub async fn update_to_awaiting_broadcast_for_retry(
        pool: &mut SqliteConnection,
        batch_id: &str,
    ) -> Result<(), sqlx::Error> {
        let update = PaymentBatchUpdate {
            status: Some(PaymentBatchStatus::AwaitingBroadcast),
            ..Default::default()
        };
        Self::update_payment_batch_status(pool, batch_id, &update, true).await
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
            intermediate_context_json: Some(""),
            ..Default::default()
        };
        Self::update_payment_batch_status(pool, batch_id, &update, false).await
    }

    pub async fn reset_to_pending_batching(pool: &mut SqliteConnection, batch_id: &str) -> Result<(), sqlx::Error> {
        let update = PaymentBatchUpdate {
            status: Some(PaymentBatchStatus::PendingBatching),
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
        warn!(
            batch_id = batch_id,
            reason = error_message;
            "DB: Marking Batch as FAILED"
        );
        let mut tx = pool.begin().await?;

        let update = PaymentBatchUpdate {
            status: Some(PaymentBatchStatus::Failed),
            error_message: Some(error_message),
            ..Default::default()
        };
        Self::update_payment_batch_status(&mut tx, batch_id, &update, false).await?;
        Payment::fail_payments_in_batch(&mut tx, batch_id, error_message).await?;

        tx.commit().await?;

        info!(
            target: "audit",
            batch_id = batch_id,
            reason = error_message;
            "DB: Batch FAILED"
        );
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
            warn!(
                batch_id = batch_id,
                max_retries = MAX_RETRIES;
                "DB: Batch reached MAX retries. Marking FAILED."
            );
            let status_failed = PaymentBatchStatus::Failed;
            let update = PaymentBatchUpdate {
                status: Some(status_failed),
                error_message: Some(error_message),
                ..Default::default()
            };
            Self::update_payment_batch_status(&mut tx, batch_id, &update, false).await?;
            Payment::fail_payments_in_batch(&mut tx, batch_id, error_message).await?;

            info!(
                target: "audit",
                batch_id = batch_id,
                retries = MAX_RETRIES,
                error = error_message;
                "DB: Batch FAILED after retries"
            );
        } else {
            // No fields to update other than incrementing retry_count.
            debug!(
                batch_id = batch_id,
                new_count = batch.retry_count + 1,
                error = error_message;
                "DB: Batch incrementing retry count"
            );
            let update = PaymentBatchUpdate::default();
            Self::update_payment_batch_status(&mut tx, batch_id, &update, true).await?;
        }

        tx.commit().await?;
        Ok(())
    }

    // Internal helper used by Payment::cancel_single_payment
    pub async fn cancel_batch_internal(tx: &mut SqliteConnection, batch_id: &str) -> Result<(), sqlx::Error> {
        info!(
            target: "audit",
            batch_id = batch_id;
            "DB: Cancelling Batch (Empty batch after payment cancellation)"
        );
        let update = PaymentBatchUpdate {
            status: Some(PaymentBatchStatus::Cancelled),
            ..Default::default()
        };
        Self::update_payment_batch_status(tx, batch_id, &update, false).await
    }

    /// Used when a payment is removed/cancelled from an active batch.
    pub async fn recalc_batch_after_modification(
        pool: &mut SqliteConnection,
        batch_id: &str,
    ) -> Result<(), sqlx::Error> {
        debug!(
            batch_id = batch_id;
            "DB: Recalculating Batch status after modification (Reverting to PendingBatching)."
        );
        let status_pending_batching = PaymentBatchStatus::PendingBatching.to_string();
        sqlx::query!(
            r#"
            UPDATE payment_batches
            SET status = ?,
                unsigned_tx_json = NULL,
                signed_tx_json = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            "#,
            status_pending_batching,
            batch_id
        )
        .execute(pool)
        .await?;

        Ok(())
    }
}

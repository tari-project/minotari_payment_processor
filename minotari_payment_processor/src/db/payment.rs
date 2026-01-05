use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::Connection;
use sqlx::{FromRow, SqliteConnection};
use std::fmt;
use utoipa::ToSchema;
use uuid::Uuid;

use crate::db::payment_batch::{PaymentBatch, PaymentBatchStatus};

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PaymentStatus {
    Received,
    Batched,
    Confirmed,
    Failed,
    Cancelled,
}

impl From<String> for PaymentStatus {
    fn from(s: String) -> Self {
        match s.as_str() {
            "RECEIVED" => PaymentStatus::Received,
            "BATCHED" => PaymentStatus::Batched,
            "CONFIRMED" => PaymentStatus::Confirmed,
            "FAILED" => PaymentStatus::Failed,
            "CANCELLED" => PaymentStatus::Cancelled,
            _ => panic!("Unknown PaymentStatus: {}", s),
        }
    }
}

impl fmt::Display for PaymentStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PaymentStatus::Received => write!(f, "RECEIVED"),
            PaymentStatus::Batched => write!(f, "BATCHED"),
            PaymentStatus::Confirmed => write!(f, "CONFIRMED"),
            PaymentStatus::Failed => write!(f, "FAILED"),
            PaymentStatus::Cancelled => write!(f, "CANCELLED"),
        }
    }
}

#[derive(Debug, Clone, FromRow)]
pub struct Payment {
    pub id: String,
    pub client_id: String,
    pub account_name: String,
    pub status: PaymentStatus,
    pub payment_batch_id: Option<String>,
    pub recipient_address: String,
    pub amount: i64,
    pub payment_id: Option<String>,
    pub payref: Option<String>,
    pub failure_reason: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Payment {
    /// Creates a new payment record in the database.
    pub async fn create(
        pool: &mut SqliteConnection,
        client_id: &str,
        account_name: &str,
        recipient_address: &str,
        amount: i64,
        payment_id: Option<String>,
        payref: Option<String>,
    ) -> Result<Self, sqlx::Error> {
        let id = Uuid::new_v4().to_string();
        let status = PaymentStatus::Received.to_string();

        sqlx::query_as!(
            Payment,
            r#"
            INSERT INTO payments (id, client_id, account_name, status, recipient_address, amount, payment_id, payref)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            RETURNING
                id,
                client_id,
                account_name,
                status,
                payment_batch_id,
                recipient_address,
                amount,
                payment_id,
                payref,
                failure_reason,
                created_at as "created_at: DateTime<Utc>",
                updated_at as "updated_at: DateTime<Utc>"
            "#,
            id,
            client_id,
            account_name,
            status,
            recipient_address,
            amount,
            payment_id,
            payref
        )
        .fetch_one(pool)
        .await
    }

    /// Retrieves a payment by its ID.
    pub async fn get_by_id(pool: &mut SqliteConnection, id: &str) -> Result<Option<Self>, sqlx::Error> {
        sqlx::query_as!(
            Payment,
            r#"
            SELECT
                id,
                client_id,
                account_name,
                status,
                payment_batch_id,
                recipient_address,
                amount,
                payment_id,
                failure_reason,
                created_at as "created_at: DateTime<Utc>",
                updated_at as "updated_at: DateTime<Utc>",
                payref
            FROM payments
            WHERE id = ?
            "#,
            id
        )
        .fetch_optional(pool)
        .await
    }

    /// Retrieves a payment by client_id and account_name for idempotency checks.
    pub async fn get_by_client_id(
        pool: &mut SqliteConnection,
        client_id: &str,
        account_name: &str,
    ) -> Result<Option<Self>, sqlx::Error> {
        sqlx::query_as!(
            Payment,
            r#"
            SELECT
                id,
                client_id,
                account_name,
                status,
                payment_batch_id,
                recipient_address,
                amount,
                payment_id,
                failure_reason,
                created_at as "created_at: DateTime<Utc>",
                updated_at as "updated_at: DateTime<Utc>",
                payref
            FROM payments
            WHERE client_id = ? AND account_name = ?
            "#,
            client_id,
            account_name
        )
        .fetch_optional(pool)
        .await
    }

    /// Retrieves multiple payments by a list of client_ids for a specific account.
    /// Used for bulk batch idempotency checks.
    pub async fn find_by_client_ids(
        pool: &mut SqliteConnection,
        client_ids: &[String],
        account_name: &str,
    ) -> Result<Vec<Self>, sqlx::Error> {
        if client_ids.is_empty() {
            return Ok(vec![]);
        }

        let json = serde_json::to_string(client_ids).map_err(|e| sqlx::Error::Configuration(Box::new(e)))?;

        sqlx::query_as!(
            Payment,
            r#"
            SELECT
                id,
                client_id,
                account_name,
                status,
                payment_batch_id,
                recipient_address,
                amount,
                payment_id,
                failure_reason,
                created_at as "created_at: DateTime<Utc>",
                updated_at as "updated_at: DateTime<Utc>",
                payref
            FROM payments
            WHERE account_name = ?
              AND client_id IN (SELECT value FROM json_each(?))
            "#,
            account_name,
            json
        )
        .fetch_all(pool)
        .await
    }

    /// Finds payments with status 'RECEIVED' for batching.
    pub async fn find_receivable_payments(pool: &mut SqliteConnection, limit: i64) -> Result<Vec<Self>, sqlx::Error> {
        sqlx::query_as!(
            Payment,
            r#"
            SELECT
                id,
                client_id,
                account_name,
                status,
                payment_batch_id,
                recipient_address,
                amount,
                payment_id,
                failure_reason,
                created_at as "created_at: DateTime<Utc>",
                updated_at as "updated_at: DateTime<Utc>",
                payref
            FROM payments
            WHERE status = 'RECEIVED'
            LIMIT ?
            "#,
            limit
        )
        .fetch_all(pool)
        .await
    }

    /// Generic function to update payment status and optional fields.
    async fn update_payment_status(
        pool: &mut SqliteConnection,
        payment_ids: &[String],
        status: PaymentStatus,
        payment_batch_id: Option<&str>,
        failure_reason: Option<&str>,
    ) -> Result<(), sqlx::Error> {
        let json = serde_json::to_string(payment_ids).map_err(|e| sqlx::Error::Configuration(Box::new(e)))?;
        let status = status.to_string();
        sqlx::query!(
            r#"
            UPDATE payments
            SET 
                status = ?, 
                payment_batch_id = COALESCE(?, payment_batch_id), 
                failure_reason = ?, 
                updated_at = CURRENT_TIMESTAMP
            WHERE id IN (SELECT value FROM json_each(?))
            "#,
            status,
            payment_batch_id,
            failure_reason,
            json,
        )
        .execute(pool)
        .await?;
        Ok(())
    }

    /// Updates the status and payment_batch_id for a list of payments.
    pub async fn update_payments_to_batched(
        pool: &mut SqliteConnection,
        payment_ids: &[String],
        batch_id: &str,
    ) -> Result<(), sqlx::Error> {
        Self::update_payment_status(pool, payment_ids, PaymentStatus::Batched, Some(batch_id), None).await
    }

    /// Updates the status of a single payment to 'CONFIRMED' and sets the payref.
    pub async fn update_payment_to_confirmed(
        pool: &mut SqliteConnection,
        payment_id: &str,
        payref: &str,
    ) -> Result<(), sqlx::Error> {
        let status = PaymentStatus::Confirmed.to_string();
        sqlx::query!(
            r#"
            UPDATE payments
              SET status = ?, payref = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            "#,
            status,
            payref,
            payment_id
        )
        .execute(pool)
        .await?;
        Ok(())
    }

    /// Updates the status of a list of payments to 'FAILED' with a reason.
    pub async fn update_payments_to_failed(
        pool: &mut SqliteConnection,
        payment_ids: &[String],
        reason: &str,
    ) -> Result<(), sqlx::Error> {
        Self::update_payment_status(pool, payment_ids, PaymentStatus::Failed, None, Some(reason)).await
    }

    /// Updates the status of a payment to 'CANCELLED'.
    pub async fn update_to_cancelled(pool: &mut SqliteConnection, payment_id: &str) -> Result<(), sqlx::Error> {
        Self::update_payment_status(pool, &[payment_id.to_string()], PaymentStatus::Cancelled, None, None).await
    }

    pub async fn cancel_single_payment(
        pool: &mut SqliteConnection,
        payment_id: &str,
    ) -> Result<PaymentStatus, anyhow::Error> {
        let mut tx = pool.begin().await?;

        let (payment, batch_opt) = Self::get_by_id_with_batch_info(&mut tx, payment_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Payment not found"))?;

        if let Some(ref batch) = batch_opt {
            match batch.status {
                PaymentBatchStatus::PendingBatching | PaymentBatchStatus::AwaitingSignature => {},
                _ => return Err(anyhow::anyhow!("Batch is too far along to cancel payment")),
            }
        } else if matches!(
            payment.status,
            PaymentStatus::Confirmed | PaymentStatus::Failed | PaymentStatus::Cancelled
        ) {
            return Err(anyhow::anyhow!("Payment is already in final state"));
        }

        Self::update_to_cancelled(&mut tx, payment_id).await?;

        if let Some(batch) = batch_opt {
            let remaining = Self::find_by_batch_id(&mut tx, &batch.id).await?;
            if remaining.is_empty() {
                PaymentBatch::cancel_batch_internal(&mut tx, &batch.id).await?;
            } else {
                PaymentBatch::recalc_batch_after_modification(&mut tx, &batch.id).await?;
            }
        }

        tx.commit().await?;
        Ok(PaymentStatus::Cancelled)
    }

    /// Updates the status of all payments in a batch to 'FAILED' with a reason.
    pub async fn fail_payments_in_batch(
        pool: &mut SqliteConnection,
        batch_id: &str,
        reason: &str,
    ) -> Result<(), sqlx::Error> {
        let status_failed = PaymentStatus::Failed.to_string();
        sqlx::query!(
            r#"
            UPDATE payments
            SET status = ?, failure_reason = ?, updated_at = CURRENT_TIMESTAMP
            WHERE payment_batch_id = ?
            "#,
            status_failed,
            reason,
            batch_id,
        )
        .execute(pool)
        .await?;
        Ok(())
    }

    /// Finds payments associated with a specific payment batch ID.
    pub async fn find_by_batch_id(pool: &mut SqliteConnection, batch_id: &str) -> Result<Vec<Self>, sqlx::Error> {
        let status_cancelled = PaymentStatus::Cancelled.to_string();
        let status_failed = PaymentStatus::Failed.to_string();
        sqlx::query_as!(
            Payment,
            r#"
            SELECT
                id,
                client_id,
                account_name,
                status,
                payment_batch_id,
                recipient_address,
                amount,
                payment_id,
                failure_reason,
                created_at as "created_at: DateTime<Utc>",
                updated_at as "updated_at: DateTime<Utc>",
                payref
            FROM payments
            WHERE payment_batch_id = ?
              AND status NOT IN (?, ?)
            ORDER BY id
            "#,
            batch_id,
            status_cancelled,
            status_failed,
        )
        .fetch_all(pool)
        .await
    }

    fn from_payment_with_batch(row: PaymentWithBatch) -> (Self, Option<PaymentBatch>) {
        let payment = Payment {
            id: row.id,
            client_id: row.client_id,
            account_name: row.account_name,
            status: row.status.into(),
            payment_batch_id: row.payment_batch_id,
            recipient_address: row.recipient_address,
            amount: row.amount,
            payment_id: row.payment_id,
            failure_reason: row.failure_reason,
            created_at: row.created_at,
            updated_at: row.updated_at,
            payref: row.payref,
        };
        let payment_batch = if let Some(batch_id) = row.batch_id {
            Some(PaymentBatch {
                id: batch_id,
                account_name: row
                    .batch_account_name
                    .expect("batch_account_name is set if batch exists"),
                status: row.batch_status.expect("batch_status is set if batch exists").into(),
                pr_idempotency_key: row
                    .batch_pr_idempotency_key
                    .expect("pr_idempotency_key is set if batch exists"),
                unsigned_tx_json: row.batch_unsigned_tx_json,
                signed_tx_json: row.batch_signed_tx_json,
                error_message: row.batch_error_message,
                retry_count: row.batch_retry_count.expect("retry_count is set if batch exists"),
                intermediate_context_json: row.batch_intermediate_context_json,
                mined_height: row.batch_mined_height,
                mined_header_hash: row.batch_mined_header_hash,
                mined_timestamp: row.batch_mined_timestamp,
                created_at: row.batch_created_at.expect("created_at is set if batch exists"),
                updated_at: row.batch_updated_at.expect("updated_at is set if batch exists"),
            })
        } else {
            None
        };
        (payment, payment_batch)
    }

    /// Retrieves a payment by its ID, joining with payment_batches for more details.
    pub async fn get_by_id_with_batch_info(
        pool: &mut SqliteConnection,
        id: &str,
    ) -> Result<Option<(Self, Option<PaymentBatch>)>, sqlx::Error> {
        sqlx::query_as!(
            PaymentWithBatch,
            r#"
            SELECT
                p.id,
                p.client_id,
                p.account_name,
                p.status,
                p.payment_batch_id,
                p.recipient_address,
                p.amount,
                p.payment_id,
                p.failure_reason,
                p.created_at as "created_at: DateTime<Utc>",
                p.updated_at as "updated_at: DateTime<Utc>",
                p.payref,
                pb.id as batch_id,
                pb.account_name as batch_account_name,
                pb.status as batch_status,
                pb.pr_idempotency_key as batch_pr_idempotency_key,
                pb.unsigned_tx_json as batch_unsigned_tx_json,
                pb.signed_tx_json as batch_signed_tx_json,
                pb.error_message as batch_error_message,
                pb.retry_count as batch_retry_count,
                pb.intermediate_context_json as batch_intermediate_context_json,
                pb.mined_height as batch_mined_height,
                pb.mined_header_hash as batch_mined_header_hash,
                pb.mined_timestamp as batch_mined_timestamp,
                pb.created_at as "batch_created_at: DateTime<Utc>",
                pb.updated_at as "batch_updated_at: DateTime<Utc>"
            FROM payments p
            LEFT JOIN payment_batches pb ON p.payment_batch_id = pb.id
            WHERE p.id = ?
            "#,
            id
        )
        .fetch_optional(pool)
        .await
        .map(|opt| opt.map(Self::from_payment_with_batch))
    }

    /// Retrieves a payment by its payref, joining with payment_batches for more details.
    pub async fn get_by_payref_with_batch_info(
        pool: &mut SqliteConnection,
        payref: &str,
    ) -> Result<Option<(Self, Option<PaymentBatch>)>, sqlx::Error> {
        sqlx::query_as!(
            PaymentWithBatch,
            r#"
            SELECT
                p.id,
                p.client_id,
                p.account_name,
                p.status,
                p.payment_batch_id,
                p.recipient_address,
                p.amount,
                p.payment_id,
                p.failure_reason,
                p.created_at as "created_at: DateTime<Utc>",
                p.updated_at as "updated_at: DateTime<Utc>",
                p.payref,
                pb.id as batch_id,
                pb.account_name as batch_account_name,
                pb.status as batch_status,
                pb.pr_idempotency_key as batch_pr_idempotency_key,
                pb.unsigned_tx_json as batch_unsigned_tx_json,
                pb.signed_tx_json as batch_signed_tx_json,
                pb.error_message as batch_error_message,
                pb.retry_count as batch_retry_count,
                pb.intermediate_context_json as batch_intermediate_context_json,
                pb.mined_height as batch_mined_height,
                pb.mined_header_hash as batch_mined_header_hash,
                pb.mined_timestamp as batch_mined_timestamp,
                pb.created_at as "batch_created_at: DateTime<Utc>",
                pb.updated_at as "batch_updated_at: DateTime<Utc>"
            FROM payments p
            LEFT JOIN payment_batches pb ON p.payment_batch_id = pb.id
            WHERE p.payref = ?
            "#,
            payref
        )
        .fetch_optional(pool)
        .await
        .map(|opt| opt.map(Self::from_payment_with_batch))
    }
}

// Helper struct for the joined query
#[derive(FromRow)]
struct PaymentWithBatch {
    id: String,
    client_id: String,
    account_name: String,
    status: String,
    payment_batch_id: Option<String>,
    recipient_address: String,
    amount: i64,
    payment_id: Option<String>,
    failure_reason: Option<String>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    payref: Option<String>,
    batch_id: Option<String>,
    batch_account_name: Option<String>,
    batch_status: Option<String>,
    batch_pr_idempotency_key: Option<String>,
    batch_unsigned_tx_json: Option<String>,
    batch_signed_tx_json: Option<String>,
    batch_error_message: Option<String>,
    batch_intermediate_context_json: Option<String>,
    batch_retry_count: Option<i64>,
    batch_mined_height: Option<i64>,
    batch_mined_header_hash: Option<String>,
    batch_mined_timestamp: Option<i64>,
    batch_created_at: Option<DateTime<Utc>>,
    batch_updated_at: Option<DateTime<Utc>>,
}

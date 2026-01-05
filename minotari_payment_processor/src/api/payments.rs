use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use utoipa::ToSchema;
use uuid::Uuid;

use crate::{
    MAX_BATCH_SIZE,
    api::{AppState, error::ApiError},
    db::{
        payment::{Payment, PaymentStatus},
        payment_batch::PaymentBatch,
    },
};

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct PaymentRequest {
    pub client_id: String, // Idempotency key
    pub account_name: String,
    pub recipient_address: String,
    pub amount: i64,
    pub payment_id: Option<String>, // Payment Memo
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct BulkPaymentItem {
    pub client_id: String, // Idempotency key
    pub recipient_address: String,
    pub amount: i64,
    pub payment_id: Option<String>, // Payment Memo
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct BulkPaymentRequest {
    pub account_name: String,
    pub items: Vec<BulkPaymentItem>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct BulkPaymentResponse {
    pub batch_id: String,
    pub account_name: String,
    pub status: String,
    pub payments: Vec<PaymentResponse>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct PaymentResponse {
    pub payment_id: String,
    pub status: PaymentStatus,
    pub client_id: String,
    pub account_name: String,
    pub recipient_address: String,
    pub amount: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mined_height: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mined_header_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mined_timestamp: Option<i64>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl PaymentResponse {
    pub fn from_payment_and_batch(payment: Payment, payment_batch: Option<PaymentBatch>) -> Self {
        let (mined_height, mined_header_hash, mined_timestamp) = if let Some(batch) = payment_batch {
            (batch.mined_height, batch.mined_header_hash, batch.mined_timestamp)
        } else {
            (None, None, None)
        };

        PaymentResponse {
            payment_id: payment.id,
            status: payment.status,
            client_id: payment.client_id,
            account_name: payment.account_name,
            recipient_address: payment.recipient_address,
            amount: payment.amount,
            payref: payment.payref,
            failure_reason: payment.failure_reason,
            mined_height,
            mined_header_hash,
            mined_timestamp,
            created_at: payment.created_at,
            updated_at: payment.updated_at,
        }
    }
}

impl From<Payment> for PaymentResponse {
    fn from(payment: Payment) -> Self {
        PaymentResponse::from_payment_and_batch(payment, None)
    }
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct PaymentCancelResponse {
    pub payment_id: String,
    pub status: PaymentStatus,
}

#[utoipa::path(
    post,
    path = "/v1/payments",
    request_body = PaymentRequest,
    responses(
        (status = 202, description = "Payment request accepted for processing", body = PaymentResponse),
        (status = 200, description = "Payment request already exists (idempotent)", body = PaymentResponse),
        (status = 400, description = "Bad request (Invalid amount or Account not found)", body = ApiError),
        (status = 500, description = "Internal server error", body = ApiError)
    )
)]
pub async fn api_create_payment(
    State(state): State<AppState>,
    Json(request): Json<PaymentRequest>,
) -> Result<impl IntoResponse, ApiError> {
    if !state.env.accounts.contains_key(&request.account_name.to_lowercase()) {
        return Err(ApiError::BadRequest(format!(
            "Account '{}' not found in configuration",
            request.account_name
        )));
    }

    if request.amount <= 0 {
        return Err(ApiError::BadRequest("Amount must be positive".to_string()));
    }

    let mut transaction = state.db_pool.begin().await?;

    if let Some(existing_payment) =
        Payment::get_by_client_id(&mut transaction, &request.client_id, &request.account_name).await?
    {
        transaction.commit().await?;
        return Ok((StatusCode::OK, Json(PaymentResponse::from(existing_payment))));
    }

    let new_payment = Payment::create(
        &mut transaction,
        &request.client_id,
        &request.account_name,
        &request.recipient_address,
        request.amount,
        request.payment_id,
        None,
    )
    .await?;

    transaction.commit().await?;

    Ok((StatusCode::ACCEPTED, Json(PaymentResponse::from(new_payment))))
}

#[utoipa::path(
    post,
    path = "/v1/payment-batches",
    request_body = BulkPaymentRequest,
    responses(
        (status = 202, description = "Bulk payment batch created successfully", body = BulkPaymentResponse),
        (status = 200, description = "Bulk payment batch already exists (idempotent)", body = BulkPaymentResponse),
        (status = 400, description = "Bad request (Account not found, limits exceeded, or duplicate payments)", body = ApiError),
        (status = 500, description = "Internal server error", body = ApiError)
    )
)]
pub async fn api_create_payment_batch(
    State(state): State<AppState>,
    Json(request): Json<BulkPaymentRequest>,
) -> Result<impl IntoResponse, ApiError> {
    if !state.env.accounts.contains_key(&request.account_name.to_lowercase()) {
        return Err(ApiError::BadRequest(format!(
            "Account '{}' not found in configuration",
            request.account_name
        )));
    }

    if request.items.is_empty() {
        return Err(ApiError::BadRequest("Batch cannot be empty".to_string()));
    }

    if request.items.len() > MAX_BATCH_SIZE {
        return Err(ApiError::BadRequest(format!(
            "Batch size exceeds limit of {}",
            MAX_BATCH_SIZE
        )));
    }

    for (idx, item) in request.items.iter().enumerate() {
        if item.amount <= 0 {
            return Err(ApiError::BadRequest(format!(
                "Item at index {} has invalid amount",
                idx
            )));
        }
    }

    let mut tx = state.db_pool.begin().await?;

    let item_client_ids: Vec<String> = request.items.iter().map(|i| i.client_id.clone()).collect();
    let existing_payments = Payment::find_by_client_ids(&mut tx, &item_client_ids, &request.account_name).await?;

    if existing_payments.len() == request.items.len() {
        let first_batch_id = existing_payments[0].payment_batch_id.clone();

        let all_same_batch = existing_payments.iter().all(|p| p.payment_batch_id == first_batch_id);

        if let (true, Some(batch_id)) = (all_same_batch, first_batch_id) {
            let batch = PaymentBatch::find_by_id(&mut tx, &batch_id)
                .await?
                .ok_or_else(|| ApiError::InternalServerError("Referenced batch not found".to_string()))?;

            let response_payments: Vec<PaymentResponse> =
                existing_payments.into_iter().map(PaymentResponse::from).collect();

            let response = BulkPaymentResponse {
                batch_id: batch.id,
                account_name: batch.account_name,
                status: batch.status.to_string(),
                payments: response_payments,
            };

            tx.commit().await?;
            return Ok((StatusCode::OK, Json(response)));
        } else {
            return Err(ApiError::BadRequest(
                "Duplicate payments found, but they do not form a single consistent batch.".to_string(),
            ));
        }
    }

    if !existing_payments.is_empty() {
        return Err(ApiError::BadRequest(format!(
            "Request contains {} duplicate client_ids (out of {}). Partial batches are not allowed.",
            existing_payments.len(),
            request.items.len()
        )));
    }

    let mut created_payments = Vec::new();
    let mut payment_ids_for_batch = Vec::new();

    for item in request.items {
        let new_payment = Payment::create(
            &mut tx,
            &item.client_id,
            &request.account_name,
            &item.recipient_address,
            item.amount,
            item.payment_id,
            None,
        )
        .await?;

        payment_ids_for_batch.push(new_payment.id.clone());
        created_payments.push(new_payment);
    }

    let pr_idempotency_key = Uuid::new_v4().to_string();

    let batch = PaymentBatch::create_with_payments(
        &mut tx,
        &request.account_name,
        &pr_idempotency_key,
        &payment_ids_for_batch,
    )
    .await?;

    tx.commit().await?;

    for p in &mut created_payments {
        p.status = PaymentStatus::Batched;
        p.payment_batch_id = Some(batch.id.clone());
    }
    let response_payments: Vec<PaymentResponse> = created_payments.into_iter().map(PaymentResponse::from).collect();

    let response = BulkPaymentResponse {
        batch_id: batch.id,
        account_name: batch.account_name,
        status: batch.status.to_string(),
        payments: response_payments,
    };

    Ok((StatusCode::ACCEPTED, Json(response)))
}

#[utoipa::path(
    get,
    path = "/v1/payments/{payment_id}",
    params(
        ("payment_id" = String, Path, description = "Unique identifier of the payment")
    ),
    responses(
        (status = 200, description = "Payment status retrieved successfully", body = PaymentResponse),
        (status = 404, description = "Payment not found", body = ApiError),
        (status = 500, description = "Internal server error", body = ApiError)
    )
)]
pub async fn api_get_payment(
    State(db_pool): State<SqlitePool>,
    Path(payment_id): Path<String>,
) -> Result<Json<PaymentResponse>, ApiError> {
    let mut conn = db_pool.acquire().await?;

    let (payment, payment_batch) = Payment::get_by_id_with_batch_info(&mut conn, &payment_id)
        .await?
        .ok_or_else(|| ApiError::NotFound("Payment not found".to_string()))?;

    Ok(Json(PaymentResponse::from_payment_and_batch(payment, payment_batch)))
}

#[utoipa::path(
    post,
    path = "/v1/payments/{payment_id}/cancel",
    params(
        ("payment_id" = String, Path, description = "Unique identifier of the payment")
    ),
    responses(
        (status = 200, description = "Payment cancelled successfully", body = PaymentCancelResponse),
        (status = 400, description = "Bad request (Payment cannot be cancelled in current state)", body = ApiError),
        (status = 404, description = "Payment not found", body = ApiError),
        (status = 500, description = "Internal server error", body = ApiError)
    )
)]
pub async fn api_cancel_payment(
    State(db_pool): State<SqlitePool>,
    Path(payment_id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let mut conn = db_pool.acquire().await?;

    match Payment::cancel_single_payment(&mut conn, &payment_id).await {
        Ok(status) => Ok((StatusCode::OK, Json(PaymentCancelResponse { payment_id, status }))),
        Err(e) => {
            let err_msg = e.to_string();
            if err_msg.contains("Payment not found") {
                Err(ApiError::NotFound(err_msg))
            } else {
                Err(ApiError::BadRequest(err_msg))
            }
        },
    }
}

#[utoipa::path(
    get,
    path = "/v1/payments/ref/{payref}",
    params(
        ("payref" = String, Path, description = "The payment reference (e.g. on-chain identifier)")
    ),
    responses(
        (status = 200, description = "Payment retrieved successfully", body = PaymentResponse),
        (status = 404, description = "Payment not found", body = ApiError),
        (status = 500, description = "Internal server error", body = ApiError)
    )
)]
pub async fn api_get_payment_by_payref(
    State(db_pool): State<SqlitePool>,
    Path(payref): Path<String>,
) -> Result<Json<PaymentResponse>, ApiError> {
    let mut conn = db_pool.acquire().await?;

    let (payment, payment_batch) = Payment::get_by_payref_with_batch_info(&mut conn, &payref)
        .await?
        .ok_or_else(|| ApiError::NotFound("Payment not found".to_string()))?;

    Ok(Json(PaymentResponse::from_payment_and_batch(payment, payment_batch)))
}

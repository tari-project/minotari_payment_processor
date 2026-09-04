use axum::{
    Router,
    extract::FromRef,
    routing::{get, post},
};
use sqlx::SqlitePool;
use std::{collections::HashMap, sync::Arc};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use crate::config::PaymentReceiverAccount;

mod error;
mod events;
mod payments;
mod version;

/// The state shared with every request handler.
///
/// This deliberately carries only what the handlers actually read, rather than the whole
/// [`crate::config::PaymentProcessorEnv`], so that secrets held by the configuration (such as the
/// offline signer passphrase) never reach the HTTP layer. The accounts map is behind an `Arc`
/// because Axum clones the state for every request that extracts it.
#[derive(Clone)]
pub struct AppState {
    pub db_pool: SqlitePool,
    pub accounts: Arc<HashMap<String, PaymentReceiverAccount>>,
}

impl FromRef<AppState> for SqlitePool {
    fn from_ref(state: &AppState) -> Self {
        state.db_pool.clone()
    }
}

#[derive(OpenApi)]
#[openapi(
    paths(
        version::api_get_version,
        payments::api_create_payment,
        payments::api_create_payment_batch,
        payments::api_get_payment,
        payments::api_get_payment_by_payref,
        payments::api_cancel_payment,
        events::api_get_events,
    ),
    components(
        schemas(
            version::ServiceVersion,
            payments::PaymentRequest,
            payments::BulkPaymentRequest,
            payments::BulkPaymentItem,
            payments::BulkPaymentResponse,
            payments::PaymentResponse,
            payments::PaymentCancelResponse,
            crate::db::event::Event,
            crate::db::event::EventType,
            crate::api::events::EventListResponse,
            crate::db::payment::PaymentStatus,
            error::ApiError,
        )
    ),
    tags(
        (name = "minotari-payment-processor", description = "Minotari Payment Processor API"),
    )
)]
pub struct ApiDoc;

pub fn create_router(db_pool: SqlitePool, accounts: Arc<HashMap<String, PaymentReceiverAccount>>) -> Router {
    let app_state = AppState { db_pool, accounts };

    Router::new()
        .merge(SwaggerUi::new("/swagger-ui").url("/openapi.json", ApiDoc::openapi()))
        .route("/health/version", get(version::api_get_version))
        .route("/v1/payments", post(payments::api_create_payment))
        .route("/v1/payment-batches", post(payments::api_create_payment_batch))
        .route("/v1/payments/{payment_id}", get(payments::api_get_payment))
        .route("/v1/payments/ref/{payref}", get(payments::api_get_payment_by_payref))
        .route("/v1/payments/{payment_id}/cancel", post(payments::api_cancel_payment))
        .route("/v1/events", get(events::api_get_events))
        .with_state(app_state)
}

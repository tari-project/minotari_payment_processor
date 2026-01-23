use axum::{
    Json,
    extract::{Query, State},
    response::IntoResponse,
};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use sqlx::SqlitePool;
use utoipa::{IntoParams, ToSchema};

use crate::api::error::ApiError;
use crate::db::event::{Event, EventFilter};

#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct GetEventsQuery {
    #[param(required = false)]
    pub account_name: Option<String>,
    #[param(required = false)]
    pub payment_id: Option<String>,
    #[param(required = false)]
    pub batch_id: Option<String>,
    #[param(required = false)]
    pub event_type: Option<String>,
    #[param(required = false)]
    pub from: Option<DateTime<Utc>>,
    #[param(required = false)]
    pub to: Option<DateTime<Utc>>,
    #[param(required = false)]
    pub limit: Option<i64>,
    #[param(required = false)]
    pub offset: Option<i64>,
}

#[derive(Debug, serde::Serialize, ToSchema)]
pub struct EventListResponse {
    pub events: Vec<Event>,
    pub total_count: i64,
}

#[utoipa::path(
    get,
    path = "/v1/events",
    params(GetEventsQuery),
    responses(
        (status = 200, description = "List of events", body = EventListResponse),
        (status = 500, description = "Internal server error", body = ApiError)
    )
)]
pub async fn api_get_events(
    State(db_pool): State<SqlitePool>,
    Query(params): Query<GetEventsQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let filter = EventFilter {
        account_name: params.account_name,
        payment_id: params.payment_id,
        batch_id: params.batch_id,
        event_type: params.event_type,
        from_date: params.from,
        to_date: params.to,
        limit: params.limit,
        offset: params.offset,
    };

    let (events, total_count) = Event::query(&db_pool, filter).await?;

    Ok(Json(EventListResponse { events, total_count }))
}

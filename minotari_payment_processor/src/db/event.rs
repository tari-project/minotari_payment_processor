use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, QueryBuilder, Sqlite, SqliteConnection};
use std::fmt;
use utoipa::ToSchema;

const DEFAULT_PAGE_SIZE: i64 = 50;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub enum EventType {
    PaymentReceived,
    PaymentCancelled,
    BatchCreated,
    BatchSigned,
    TransactionBroadcast,
    TransactionBroadcastFailed,
    TransactionMempoolDetected,
    TransactionConfirmed,
    TransactionReorged,
    BatchFailed,
}

impl fmt::Display for EventType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug, Clone, Serialize, FromRow, ToSchema)]
pub struct Event {
    pub id: i64,
    pub event_type: String,
    pub description: String,
    pub metadata_json: Option<String>,
    pub account_name: String,
    pub payment_id: Option<String>,
    pub batch_id: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct EventFilter {
    pub account_name: Option<String>,
    pub payment_id: Option<String>,
    pub batch_id: Option<String>,
    pub event_type: Option<String>,
    pub from_date: Option<DateTime<Utc>>,
    pub to_date: Option<DateTime<Utc>>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

impl Event {
    pub async fn insert(
        conn: &mut SqliteConnection,
        event_type: EventType,
        description: String,
        metadata: Option<serde_json::Value>,
        account_name: String,
        payment_id: Option<String>,
        batch_id: Option<String>,
    ) -> Result<i64, sqlx::Error> {
        let type_str = event_type.to_string();
        let metadata_str = metadata.map(|v| v.to_string());

        let id = sqlx::query!(
            r#"
            INSERT INTO events (event_type, description, metadata_json, account_name, payment_id, batch_id)
            VALUES (?, ?, ?, ?, ?, ?)
            RETURNING id
            "#,
            type_str,
            description,
            metadata_str,
            account_name,
            payment_id,
            batch_id
        )
        .fetch_one(conn)
        .await?
        .id;

        Ok(id)
    }

    fn apply_filters<'a>(qb: &mut QueryBuilder<'a, Sqlite>, filter: &'a EventFilter) {
        if let Some(acc) = &filter.account_name {
            qb.push(" AND account_name = ").push_bind(acc);
        }
        if let Some(pid) = &filter.payment_id {
            qb.push(" AND payment_id = ").push_bind(pid);
        }
        if let Some(bid) = &filter.batch_id {
            qb.push(" AND batch_id = ").push_bind(bid);
        }
        if let Some(et) = &filter.event_type {
            qb.push(" AND event_type = ").push_bind(et);
        }
        if let Some(from) = filter.from_date {
            qb.push(" AND created_at >= ").push_bind(from);
        }
        if let Some(to) = filter.to_date {
            qb.push(" AND created_at <= ").push_bind(to);
        }
    }

    pub async fn query(pool: &sqlx::SqlitePool, filter: EventFilter) -> Result<(Vec<Event>, i64), sqlx::Error> {
        let mut count_qb = QueryBuilder::new("SELECT COUNT(*) FROM events WHERE 1=1");
        Self::apply_filters(&mut count_qb, &filter);

        let count: i64 = count_qb.build_query_scalar().fetch_one(pool).await?;

        let mut select_qb = QueryBuilder::new("SELECT * FROM events WHERE 1=1");
        Self::apply_filters(&mut select_qb, &filter);

        select_qb.push(" ORDER BY created_at DESC");

        let limit = filter.limit.unwrap_or(DEFAULT_PAGE_SIZE);
        select_qb.push(" LIMIT ").push_bind(limit);

        if let Some(offset) = filter.offset {
            select_qb.push(" OFFSET ").push_bind(offset);
        }

        let events = select_qb.build_query_as::<Event>().fetch_all(pool).await?;

        Ok((events, count))
    }
}

pub mod event;
pub mod payment;
pub mod payment_batch;

use sqlx::{SqlitePool, sqlite::SqlitePoolOptions};

pub async fn init_db(db_url: &str) -> Result<SqlitePool, anyhow::Error> {
    let pool = SqlitePoolOptions::new().max_connections(5).connect(db_url).await?;

    // Run migrations
    sqlx::migrate!("../migrations").run(&pool).await?;
    Ok(pool)
}

use anyhow::anyhow;
use dotenv::dotenv;
use minotari_client::apis::configuration::Configuration as MinotariConfiguration;
use minotari_node_wallet_client::http::Client as BaseNodeClient;
use minotari_payment_processor::{api, db, workers};
use std::sync::Arc;
use tokio::{net::TcpListener, signal};
use url::Url;

struct PaymentProcessorEnv {
    pub database_url: String,
    pub payment_receiver: String,
    pub base_node: String,
    pub console_wallet_path: String,
    pub console_wallet_password: String,
    pub listen_ip: String,
    pub listen_port: u16,
    pub batch_creator_sleep_secs: Option<u64>,
    pub unsigned_tx_creator_sleep_secs: Option<u64>,
    pub transaction_signer_sleep_secs: Option<u64>,
    pub broadcaster_sleep_secs: Option<u64>,
    pub confirmation_checker_sleep_secs: Option<u64>,
}

impl PaymentProcessorEnv {
    pub fn from_env() -> anyhow::Result<Self> {
        let database_url =
            std::env::var("DATABASE_URL").map_err(|_| anyhow!("DATABASE_URL environment variable not set"))?;
        let payment_receiver =
            std::env::var("PAYMENT_RECEIVER").map_err(|_| anyhow!("PAYMENT_RECEIVER environment variable not set"))?;
        let base_node = std::env::var("BASE_NODE").map_err(|_| anyhow!("BASE_NODE environment variable not set"))?;
        let console_wallet_path = std::env::var("CONSOLE_WALLET_PATH")
            .map_err(|_| anyhow!("CONSOLE_WALLET_PATH environment variable not set"))?;
        let console_wallet_password = std::env::var("CONSOLE_WALLET_PASSWORD")
            .map_err(|_| anyhow!("CONSOLE_WALLET_PASSWORD environment variable not set"))?;
        let listen_ip = std::env::var("LISTEN_IP").unwrap_or_else(|_| "0.0.0.0".to_string());
        let listen_port = std::env::var("LISTEN_PORT")
            .unwrap_or_else(|_| "9145".to_string())
            .parse::<u16>()?;

        let batch_creator_sleep_secs = std::env::var("BATCH_CREATOR_SLEEP_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok());
        let unsigned_tx_creator_sleep_secs = std::env::var("UNSIGNED_TX_CREATOR_SLEEP_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok());
        let transaction_signer_sleep_secs = std::env::var("TRANSACTION_SIGNER_SLEEP_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok());
        let broadcaster_sleep_secs = std::env::var("BROADCASTER_SLEEP_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok());
        let confirmation_checker_sleep_secs = std::env::var("CONFIRMATION_CHECKER_SLEEP_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok());

        Ok(Self {
            database_url,
            payment_receiver,
            base_node,
            console_wallet_path,
            console_wallet_password,
            listen_ip,
            listen_port,
            batch_creator_sleep_secs,
            unsigned_tx_creator_sleep_secs,
            transaction_signer_sleep_secs,
            broadcaster_sleep_secs,
            confirmation_checker_sleep_secs,
        })
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();
    let env = PaymentProcessorEnv::from_env()?;

    println!("Starting Minotari Payment Processor...");

    let db_pool = db::init_db(&env.database_url).await?;
    println!("Database initialized.");

    let client_config = Arc::new(MinotariConfiguration {
        base_path: env.payment_receiver,
        ..MinotariConfiguration::default()
    });

    let base_node_url = Url::parse(&env.base_node)?;
    let base_node_client = BaseNodeClient::new(base_node_url.clone(), base_node_url.clone());

    // Spawn workers
    tokio::spawn(workers::batch_creator::run(
        db_pool.clone(),
        env.batch_creator_sleep_secs,
    ));
    tokio::spawn(workers::unsigned_tx_creator::run(
        db_pool.clone(),
        client_config.clone(),
        env.unsigned_tx_creator_sleep_secs,
    ));
    tokio::spawn(workers::transaction_signer::run(
        db_pool.clone(),
        env.console_wallet_path.clone(),
        env.console_wallet_password.clone(),
        env.transaction_signer_sleep_secs,
    ));
    tokio::spawn(workers::broadcaster::run(
        db_pool.clone(),
        base_node_client.clone(),
        env.broadcaster_sleep_secs,
    ));
    tokio::spawn(workers::confirmation_checker::run(
        db_pool.clone(),
        base_node_client.clone(),
        env.confirmation_checker_sleep_secs,
    ));
    println!("Minotari Payment Processor started. Press Ctrl+C to shut down.");

    // Create Axum API router
    let app = api::create_router(db_pool.clone());
    let addr = format!("{}:{}", env.listen_ip, env.listen_port);
    let listener = TcpListener::bind(&addr).await?;
    println!("Axum API server listening on {}", addr);
    tokio::spawn(async move {
        axum::serve(listener, app.into_make_service()).await.unwrap();
    });

    signal::ctrl_c().await?;
    println!("Ctrl+C received, shutting down.");

    Ok(())
}

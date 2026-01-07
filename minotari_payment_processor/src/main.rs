use dotenv::dotenv;
use log::info;
use log4rs::config::RawConfig;
use minotari_client::apis::configuration::Configuration as MinotariConfiguration;
use minotari_node_wallet_client::http::Client as BaseNodeClient;
use minotari_payment_processor::{api, config::PaymentProcessorEnv, db, workers};
use std::{path::Path, sync::Arc};
use tokio::{net::TcpListener, signal};
use url::Url;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logging();

    dotenv().ok();
    let env = PaymentProcessorEnv::load()?;
    let app_env = env.clone();

    info!("Starting Minotari Payment Processor...");

    let db_pool = db::init_db(&env.database_url).await?;
    info!("Database initialized.");

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
        env.tari_network,
        env.accounts.clone(),
        env.max_input_count_per_tx,
        env.unsigned_tx_creator_sleep_secs,
    ));
    tokio::spawn(workers::transaction_signer::run(
        db_pool.clone(),
        env.tari_network,
        env.console_wallet_path.clone(),
        env.console_wallet_base_path.clone(),
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
        env.confirmation_checker_required_confirmations.unwrap_or(10),
    ));
    info!("Minotari Payment Processor started. Press Ctrl+C to shut down.");

    // Create Axum API router
    let app = api::create_router(db_pool.clone(), app_env);
    let addr = format!("{}:{}", env.listen_ip, env.listen_port);
    let listener = TcpListener::bind(&addr).await?;
    info!(
        address = &*addr;
        "Axum API server listening"
    );
    tokio::spawn(async move {
        axum::serve(listener, app.into_make_service()).await.unwrap();
    });

    signal::ctrl_c().await?;
    info!("Ctrl+C received, shutting down.");

    Ok(())
}

fn init_logging() {
    let config_path = "log4rs.yml";
    let path = Path::new(config_path);

    if path.exists() {
        match log4rs::init_file(path, Default::default()) {
            Ok(_) => {
                info!(
                    path = config_path;
                    "Logging initialized from external configuration"
                );
                return;
            },
            Err(e) => {
                panic!("Failed to load external log4rs.yml: {}", e);
            },
        }
    }

    let yaml_content = include_str!("../resources/default_log4rs.yml");
    let config: RawConfig = serde_yaml::from_str(yaml_content).expect("Embedded logging configuration is invalid YAML");
    log4rs::init_raw_config(config).expect("Failed to initialize logging from embedded config");
    info!("Logging initialized from embedded defaults (no external log4rs.yml found)");
}

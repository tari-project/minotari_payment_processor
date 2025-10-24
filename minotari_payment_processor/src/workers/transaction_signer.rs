use sqlx::SqlitePool;
use std::io::Write;
use tempfile::NamedTempFile;
use tokio::fs;
use tokio::time::{self, Duration};

use crate::db::payment_batch::{PaymentBatch, PaymentBatchStatus};

const DEFAULT_SLEEP_SECS: u64 = 10;

pub async fn run(
    db_pool: SqlitePool,
    console_wallet_path: String,
    console_wallet_password: String,
    sleep_secs: Option<u64>,
) {
    let sleep_secs = sleep_secs.unwrap_or(DEFAULT_SLEEP_SECS);
    let mut interval = time::interval(Duration::from_secs(sleep_secs));
    loop {
        interval.tick().await;
        if let Err(e) = process_transactions_to_sign(&db_pool, &console_wallet_path, &console_wallet_password).await {
            eprintln!("Transaction Signer worker error: {:?}", e);
        }
    }
}

async fn process_transactions_to_sign(
    db_pool: &SqlitePool,
    console_wallet_path: &str,
    console_wallet_password: &str,
) -> Result<(), anyhow::Error> {
    let mut conn = db_pool.acquire().await?;
    let batches = PaymentBatch::find_by_status(&mut conn, PaymentBatchStatus::AwaitingSignature).await?;

    for batch in batches {
        // Update its status to `SIGNING_IN_PROGRESS` to prevent other workers from picking it up.
        PaymentBatch::update_to_signing_in_progress(&mut conn, &batch.id).await?;

        let batch_id = batch.id.clone();
        let unsigned_tx_json = batch
            .unsigned_tx_json
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Batch {} has no unsigned_tx_json", batch_id))?;

        // Create temporary input file
        let mut input_file = NamedTempFile::with_prefix("unsigned-tx-")?;
        input_file.write_all(unsigned_tx_json.as_bytes())?;
        let input_file_path = input_file.path().to_path_buf();

        // Create temporary output file
        let output_file = NamedTempFile::with_prefix("signed-tx-")?;
        let output_file_path = output_file.path().to_path_buf();

        let batch_id_clone = batch_id.clone();
        let input_path_clone = input_file_path.clone();
        let output_path_clone = output_file_path.clone();

        let console_wallet_path: String = console_wallet_path.to_string().clone();
        let console_wallet_password = console_wallet_password.to_string().clone();
        let signing_result = tokio::task::spawn_blocking(move || {
            std::process::Command::new(console_wallet_path)
                .env("MINOTARI_WALLET_PASSWORD", console_wallet_password)
                .arg("sign-one-sided-transaction")
                .arg("--input-file")
                .arg(&input_path_clone)
                .arg("--output-file")
                .arg(&output_path_clone)
                .output()
        })
        .await?;

        match signing_result {
            Ok(output) => {
                if output.status.success() {
                    // On CLI Success (exit code 0)
                    let signed_tx_json = fs::read_to_string(&output_file_path).await?;
                    PaymentBatch::update_to_awaiting_broadcast(&mut conn, &batch_id_clone, &signed_tx_json).await?;
                } else {
                    // On CLI Failure (non-zero exit code)
                    let error_message = String::from_utf8_lossy(&output.stderr).to_string();
                    eprintln!("CLI signing failed for batch {}: {}", batch_id_clone, error_message);
                    PaymentBatch::update_to_failed(&mut conn, &batch_id_clone, &error_message).await?;
                }
            },
            Err(e) => {
                eprintln!(
                    "Failed to execute minotari_console_wallet for batch {}: {:?}",
                    batch_id_clone, e
                );
                PaymentBatch::update_to_failed(&mut conn, &batch_id_clone, &format!("CLI execution error: {:?}", e))
                    .await?;
            },
        }
    }

    Ok(())
}

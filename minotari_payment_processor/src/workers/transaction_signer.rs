use anyhow::{Context, anyhow};
use log::{debug, error, info};
use sqlx::{SqliteConnection, SqlitePool};
use std::io::Write;
use tari_common::configuration::Network;
use tari_transaction_components::key_manager::SerializedKeyString;
use tari_transaction_components::key_manager::TariKeyId;
use tari_transaction_components::offline_signing::models::{SignedOneSidedTransactionResult, TransactionResult};
use tempfile::NamedTempFile;
use tokio::fs;
use tokio::process::Command;
use tokio::time::{self, Duration};

use crate::db::payment_batch::StepPayload;
use crate::db::payment_batch::{BatchPayload, PaymentBatch, PaymentBatchStatus};
use crate::workers::types::IntermediateContext;

const DEFAULT_SLEEP_SECS: u64 = 10;

pub async fn run(
    db_pool: SqlitePool,
    network: Network,
    console_wallet_path: String,
    console_wallet_base_path: String,
    console_wallet_password: String,
    sleep_secs: Option<u64>,
) {
    let sleep_secs = sleep_secs.unwrap_or(DEFAULT_SLEEP_SECS);
    info!(
        interval = sleep_secs;
        "Transaction Signer worker started"
    );

    let mut interval = time::interval(Duration::from_secs(sleep_secs));

    loop {
        interval.tick().await;
        if let Err(e) = process_transactions_to_sign(
            &db_pool,
            network,
            &console_wallet_path,
            &console_wallet_base_path,
            &console_wallet_password,
        )
        .await
        {
            error!(
                error:? = e;
                "Transaction Signer worker error"
            );
        }
    }
}

async fn process_transactions_to_sign(
    db_pool: &SqlitePool,
    network: Network,
    console_wallet_path: &str,
    console_wallet_base_path: &str,
    console_wallet_password: &str,
) -> Result<(), anyhow::Error> {
    let mut conn = db_pool.acquire().await?;

    let batches = PaymentBatch::find_by_status(&mut conn, PaymentBatchStatus::AwaitingSignature).await?;

    if !batches.is_empty() {
        info!(
            count = batches.len();
            "Found batches awaiting signature"
        );
    }

    for batch in batches {
        if let Err(e) = process_single_batch(
            &mut conn,
            network,
            console_wallet_path,
            console_wallet_base_path,
            console_wallet_password,
            &batch,
        )
        .await
        {
            let error_message = format!("{:#}", e);
            error!(
                batch_id = &*batch.id,
                error = &*error_message;
                "Error signing batch. Attempting to revert status..."
            );

            let revert_result = if let Some(json) = &batch.unsigned_tx_json {
                PaymentBatch::update_to_awaiting_signature(&mut conn, &batch.id, json).await
            } else {
                Err(anyhow::anyhow!("Cannot revert: Batch missing unsigned_tx_json"))?
            };

            match revert_result {
                Ok(_) => info!(batch_id = &*batch.id; "Batch reverted to 'AwaitingSignature'"),
                Err(revert_e) => error!(
                    batch_id = &*batch.id,
                    error:? = revert_e;
                    "Failed to revert batch status"
                ),
            }

            if let Err(db_err) = PaymentBatch::increment_retry_count(&mut conn, &batch.id, &error_message).await {
                error!(
                    batch_id = &*batch.id,
                    error:? = db_err;
                    "Failed to update retry count for batch"
                );
            }
        }
    }

    Ok(())
}

async fn process_single_batch(
    conn: &mut SqliteConnection,
    network: Network,
    console_wallet_path: &str,
    console_wallet_base_path: &str,
    console_wallet_password: &str,
    batch: &PaymentBatch,
) -> Result<(), anyhow::Error> {
    let batch_id = &batch.id;
    info!(batch_id = batch_id.as_str(); "Starting processing for Batch");

    PaymentBatch::update_to_signing_in_progress(conn, batch_id)
        .await
        .context("Failed to update status to SigningInProgress")?;

    info!(batch_id = batch_id.as_str(); "Batch Status updated to 'SigningInProgress'");

    let unsigned_json_str = batch
        .unsigned_tx_json
        .clone()
        .ok_or_else(|| anyhow!("Batch {} has no unsigned_tx_json", batch_id))?;

    let mut payload = BatchPayload::from_json(&unsigned_json_str)?;
    let steps_count = payload.steps.len();

    info!(
        batch_id = batch_id.as_str(),
        steps = steps_count;
        "Batch found steps to sign"
    );

    let mut consolidated_wallet_outputs = vec![];
    for (i, step) in payload.steps.iter_mut().enumerate() {
        info!(
            batch_id = batch_id.as_str(),
            step = i + 1,
            total = steps_count,
            tx_id:? = step.tx_id;
            "Signing Step"
        );

        let unsigned_json = match &step.payload {
            StepPayload::Unsigned(s) => s,
            StepPayload::Signed(_) => return Err(anyhow!("Step {} is already signed!", i)),
        };

        let mut input_file = NamedTempFile::with_prefix(format!("unsigned-tx-{}-step{}-", batch_id, i))
            .context("Failed to create temp input file")?;
        let input_path = input_file.path().to_path_buf();

        input_file
            .write_all(unsigned_json.as_bytes())
            .context("Failed to write unsigned tx to temp file")?;
        input_file.flush().context("Failed to flush input file")?;

        let output_file = NamedTempFile::with_prefix(format!("signed-tx-{}-step{}-", batch_id, i))
            .context("Failed to create temp output file")?;
        let output_path = output_file.path().to_path_buf();

        sign_with_cli(
            network,
            console_wallet_path,
            console_wallet_password,
            console_wallet_base_path,
            &input_path,
            &output_path,
        )
        .await
        .context(format!("External signing process failed for step {}", i))?;

        let signed_json = fs::read_to_string(&output_path)
            .await
            .context("Failed to read signed transaction from output file")?;
        let signed_tx_wrapper = SignedOneSidedTransactionResult::from_json(&signed_json)
            .map_err(|e| anyhow!("Failed to deserialize signed tx for step {}: {}", i, e))?;

        if step.is_consolidation {
            for output in &signed_tx_wrapper.signed_transaction.outputs {
                let mut cloned_output = output.clone();
                let script_key_id = TariKeyId::Derived {
                    key: SerializedKeyString::from(output.commitment_mask_key_id().to_string()),
                };
                cloned_output.set_script_key_id(script_key_id);
                consolidated_wallet_outputs.push(cloned_output);
            }
        }

        step.payload = StepPayload::Signed(signed_json);
    }

    info!(batch_id = batch_id.as_str(); "All steps signed successfully.");

    let intermediate_context = if consolidated_wallet_outputs.is_empty() {
        None
    } else {
        let ctx = IntermediateContext {
            utxos: consolidated_wallet_outputs,
        };
        Some(ctx.to_json()?)
    };

    let signed_payload_json = payload.to_json()?;
    PaymentBatch::update_to_awaiting_broadcast(conn, batch_id, &signed_payload_json, intermediate_context.as_deref())
        .await
        .context("Failed to update status to AwaitingBroadcast")?;

    info!(
        target: "audit",
        batch_id = batch_id.as_str();
        "Signing complete. Status updated to 'AwaitingBroadcast'."
    );

    Ok(())
}

/// Executes the Minotari Console Wallet.
async fn sign_with_cli(
    network: Network,
    executable_path: &str,
    password: &str,
    base_path: &str,
    input_path: &std::path::Path,
    output_path: &std::path::Path,
) -> Result<(), anyhow::Error> {
    let mut cmd = Command::new(executable_path);
    cmd.current_dir(base_path)
        .env("MINOTARI_WALLET_PASSWORD", password)
        .arg("--command-mode-auto-exit")
        .arg("--base-path")
        .arg(base_path)
        .arg("--network")
        .arg(network.to_string())
        .arg("--skip-recovery")
        .arg("sign-one-sided-transaction")
        .arg("--input-file")
        .arg(input_path)
        .arg("--output-file")
        .arg(output_path);

    let command_string = format!(
        "MINOTARI_WALLET_PASSWORD=*** {} {}",
        cmd.as_std().get_program().to_string_lossy(),
        cmd.as_std()
            .get_args()
            .map(|arg| arg.to_string_lossy())
            .collect::<Vec<_>>()
            .join(" ")
    );

    debug!(command = &*command_string; "Executing Command");

    let cmd_output = cmd.output().await.context("Failed to execute console wallet command")?;

    if !cmd_output.status.success() {
        let stderr = String::from_utf8_lossy(&cmd_output.stderr);
        let stdout = String::from_utf8_lossy(&cmd_output.stdout);
        return Err(anyhow!(
            "CLI exited with error code: {}.\nStderr: {}\nStdout: {}",
            cmd_output.status,
            stderr,
            stdout
        ));
    } else {
        let stdout = String::from_utf8_lossy(&cmd_output.stdout);
        if !stdout.trim().is_empty() {
            debug!(stdout = &*stdout; "CLI Stdout");
        }
    }

    Ok(())
}

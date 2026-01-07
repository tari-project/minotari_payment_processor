use anyhow::{Context, anyhow};
use log::{error, info, warn};
use minotari_node_wallet_client::{BaseNodeWalletClient, http::Client};
use sqlx::{SqliteConnection, SqlitePool};
use tari_transaction_components::rpc::models::TxLocation;
use tari_transaction_components::{
    offline_signing::models::SignedOneSidedTransactionResult, transaction_components::Transaction,
};
use tari_utilities::ByteArray;
use tari_utilities::message_format::MessageFormat;
use tokio::time::{self, Duration};

use crate::db::payment_batch::{BatchPayload, PaymentBatch, PaymentBatchStatus, StepPayload};

const DEFAULT_SLEEP_SECS: u64 = 15;
const MEMPOOL_CHECK_RETRIES: usize = 10;
const MEMPOOL_CHECK_DELAY: Duration = Duration::from_secs(2);

pub async fn run(db_pool: SqlitePool, base_node_client: Client, sleep_secs: Option<u64>) {
    let sleep_secs = sleep_secs.unwrap_or(DEFAULT_SLEEP_SECS);
    info!(
        interval = sleep_secs;
        "Transaction Broadcaster worker started"
    );

    let mut interval = time::interval(Duration::from_secs(sleep_secs));

    loop {
        interval.tick().await;
        if let Err(e) = process_transactions_to_broadcast(&db_pool, &base_node_client).await {
            error!(
                error:? = e;
                "Transaction Broadcaster worker error"
            );
        }
    }
}

async fn process_transactions_to_broadcast(
    db_pool: &SqlitePool,
    base_node_client: &Client,
) -> Result<(), anyhow::Error> {
    let mut conn = db_pool.acquire().await?;

    let batches = PaymentBatch::find_by_status(&mut conn, PaymentBatchStatus::AwaitingBroadcast).await?;

    if !batches.is_empty() {
        info!(
            count = batches.len();
            "Found batches awaiting broadcast"
        );
    }

    for batch in batches {
        if let Err(e) = process_single_batch(&mut conn, base_node_client, &batch).await {
            let error_message = e.to_string();
            error!(
                batch_id = &*batch.id,
                error = &*error_message;
                "Error broadcasting batch. Attempting to revert status..."
            );

            match PaymentBatch::update_to_awaiting_broadcast_for_retry(&mut conn, &batch.id).await {
                Ok(_) => info!(batch_id = &*batch.id; "Batch reverted to 'AwaitingBroadcast'"),
                Err(revert_e) => {
                    error!(
                        batch_id = &*batch.id,
                        error:? = revert_e;
                        "Failed to revert batch status"
                    )
                },
            }
        }
    }

    Ok(())
}

async fn process_single_batch(
    conn: &mut SqliteConnection,
    base_node_client: &Client,
    batch: &PaymentBatch,
) -> Result<(), anyhow::Error> {
    let batch_id = &batch.id;
    info!(batch_id = batch_id.as_str(); "Starting broadcast sequence");

    PaymentBatch::update_to_broadcasting(conn, batch_id)
        .await
        .context("Failed to set status to broadcasting")?;

    let signed_json_str = batch
        .signed_tx_json
        .clone()
        .ok_or_else(|| anyhow!("Batch {} has no signed_tx_json", batch_id))?;

    let payload = BatchPayload::from_json(&signed_json_str)?;
    let is_consolidation_cycle = payload.steps.first().map(|s| s.is_consolidation).unwrap_or(false);

    info!(
        batch_id = batch_id.as_str(),
        tx_count = payload.steps.len(),
        consolidation = is_consolidation_cycle;
        "Broadcasting transactions"
    );

    let mut step_tx_objects = Vec::new();

    for (i, step) in payload.steps.iter().enumerate() {
        let signed_json = match &step.payload {
            StepPayload::Signed(s) => s,
            StepPayload::Unsigned(_) => return Err(anyhow!("Step {} is not signed!", i)),
        };
        let signed_tx_wrapper = SignedOneSidedTransactionResult::from_json(signed_json)
            .map_err(|e| anyhow!("Failed to deserialize signed tx for step {}: {}", i, e))?;

        let tx = signed_tx_wrapper.signed_transaction.transaction.clone();
        step_tx_objects.push(tx.clone());

        info!(
            batch_id = batch_id.as_str(),
            step = i + 1,
            total_steps = payload.steps.len(),
            internal_tx_id:? = step.tx_id;
            "Submitting TX"
        );

        let response = base_node_client
            .submit_transaction(tx)
            .await
            .context("Network error submitting transaction to Base Node")?;

        if response.accepted {
            info!(
                target: "audit",
                batch_id = batch_id.as_str(),
                step = i + 1,
                total_steps = payload.steps.len(),
                internal_tx_id:? = step.tx_id;
                "Transaction ACCEPTED by Base Node"
            );
        } else {
            warn!(
                batch_id = batch_id.as_str(),
                step = i + 1,
                reason:% = response.rejection_reason;
                "Transaction REJECTED by Base Node"
            );
            return Err(anyhow!(
                "Tari base node rejected transaction in step {}: {}",
                i + 1,
                response.rejection_reason
            ));
        }
    }

    if is_consolidation_cycle {
        // === SPLIT CYCLE DETECTED ===
        info!(
            batch_id = batch_id.as_str();
            "Split Cycle detected. Verifying Mempool propagation..."
        );

        verify_txs_in_mempool(base_node_client, &step_tx_objects).await?;

        info!(
            target: "audit",
            batch_id = batch_id.as_str();
            "Split transactions in Mempool. LOOPING BACK state to 'PendingBatching' for Cycle 2."
        );

        PaymentBatch::reset_to_pending_batching(conn, batch_id)
            .await
            .context("Failed to reset batch to PendingBatching")?;
    } else {
        // === NORMAL / FINAL CYCLE ===
        info!(
            target: "audit",
            batch_id = batch_id.as_str();
            "Broadcast complete. Status updated to 'AwaitingConfirmation'."
        );

        PaymentBatch::update_to_awaiting_confirmation(conn, batch_id)
            .await
            .context("Failed to update status to AwaitingConfirmation")?;
    }

    Ok(())
}

/// Polls the base node to ensure the submitted transactions are visible in the mempool.
async fn verify_txs_in_mempool(base_node_client: &Client, txs: &[Transaction]) -> Result<(), anyhow::Error> {
    for (i, tx) in txs.iter().enumerate() {
        let kernel = tx
            .body
            .kernels()
            .first()
            .ok_or_else(|| anyhow!("Transaction {} has no kernels", i))?;

        let excess_public = kernel.excess_sig.get_compressed_public_nonce().to_vec();
        let excess_sig = kernel.excess_sig.get_signature().to_vec();

        let mut retries = 0;
        let mut found = false;

        while retries < MEMPOOL_CHECK_RETRIES {
            let response = base_node_client
                .transaction_query(excess_public.clone(), excess_sig.clone())
                .await
                .context("Failed to query transaction status")?;

            match response.location {
                TxLocation::InMempool => {
                    found = true;
                    break;
                },
                TxLocation::Mined => {
                    found = true;
                    break;
                },
                TxLocation::NotStored | TxLocation::None => {
                    time::sleep(MEMPOOL_CHECK_DELAY).await;
                    retries += 1;
                },
            }
        }

        if !found {
            return Err(anyhow!(
                "Transaction {} (Step {}) did not appear in mempool after retries. Aborting loop-back.",
                i,
                i + 1
            ));
        }
    }

    Ok(())
}

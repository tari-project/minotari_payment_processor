use anyhow::{Context, anyhow};
use log::{debug, error, info, warn};
use minotari_node_wallet_client::{BaseNodeWalletClient, http::Client};
use sqlx::SqlitePool;
use tari_common_types::payment_reference::generate_payment_reference;
use tari_common_types::types::FixedHash;
use tari_transaction_components::offline_signing::models::{SignedOneSidedTransactionResult, TransactionResult};
use tari_transaction_components::rpc::models::TxLocation;
use tari_utilities::byte_array::ByteArray;
use tokio::time::{self, Duration};

use crate::db::payment::Payment;
use crate::db::payment_batch::BatchPayload;
use crate::db::payment_batch::StepPayload;
use crate::db::payment_batch::{PaymentBatch, PaymentBatchStatus};

const DEFAULT_SLEEP_SECS: u64 = 60;

pub async fn run(db_pool: SqlitePool, base_node_client: Client, sleep_secs: Option<u64>, required_confirmations: u64) {
    let sleep_secs = sleep_secs.unwrap_or(DEFAULT_SLEEP_SECS);
    info!(
        interval = sleep_secs,
        required_confirmations = required_confirmations;
        "Confirmation Checker worker started"
    );

    let mut interval = time::interval(Duration::from_secs(sleep_secs));

    loop {
        interval.tick().await;
        if let Err(e) = check_transaction_confirmations(&db_pool, &base_node_client, required_confirmations).await {
            error!(
                error:? = e;
                "Confirmation Checker worker error"
            );
        }
    }
}

async fn check_transaction_confirmations(
    db_pool: &SqlitePool,
    base_node_client: &Client,
    required_confirmations: u64,
) -> Result<(), anyhow::Error> {
    let mut conn = db_pool.acquire().await?;

    let batches = PaymentBatch::find_by_status(&mut conn, PaymentBatchStatus::AwaitingConfirmation).await?;

    if !batches.is_empty() {
        info!(
            count = batches.len();
            "Found batches awaiting confirmation"
        );
    }

    for batch in batches {
        if let Err(e) = process_single_batch(db_pool, base_node_client, &batch, required_confirmations).await {
            let error_message = e.to_string();
            error!(
                batch_id = &*batch.id,
                error = &*error_message;
                "Error checking confirmation for batch. Incrementing retry count."
            );

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
    db_pool: &SqlitePool,
    base_node_client: &Client,
    batch: &PaymentBatch,
    required_confirmations: u64,
) -> Result<(), anyhow::Error> {
    let batch_id = &batch.id;

    info!(batch_id = batch_id.as_str(); "Checking status for Batch");

    let payload = match &batch.signed_tx_json {
        Some(payload) => BatchPayload::from_json(payload)?,
        None => return Err(anyhow!("Batch {} has no signed_tx_json", batch_id)),
    };
    let signed_tx_json = match &payload.steps[..] {
        [step] => match &step.payload {
            StepPayload::Signed(s) => s,
            StepPayload::Unsigned(_) => return Err(anyhow!("Payload is not signed!")),
        },
        _ => return Err(anyhow!("Batch {} does not have exactly one step", batch_id)),
    };

    let signed_tx = SignedOneSidedTransactionResult::from_json(signed_tx_json)?;

    let kernel = signed_tx
        .signed_transaction
        .transaction
        .body
        .kernels()
        .first()
        .ok_or_else(|| anyhow!("Transaction has no kernels"))?;

    let excess_sig_nonce = kernel.excess_sig.get_compressed_public_nonce().to_vec();
    let excess_sig_sig = kernel.excess_sig.get_signature().to_vec();

    debug!(
        batch_id = batch_id.as_str(),
        nonce_preview:? = &excess_sig_nonce[0..4];
        "Querying Base Node for Kernel Signature"
    );

    let tx_query_response = base_node_client
        .transaction_query(excess_sig_nonce, excess_sig_sig)
        .await
        .context("Failed to query transaction from Base Node")?;

    match tx_query_response.location {
        TxLocation::Mined => {
            info!(batch_id = batch_id.as_str(); "Location 'Mined'. Processing confirmations...");
            handle_mined_transaction(
                db_pool,
                base_node_client,
                batch_id,
                &tx_query_response,
                &signed_tx,
                required_confirmations,
            )
            .await?
        },
        TxLocation::InMempool => {
            info!(batch_id = batch_id.as_str(); "Batch is currently in the mempool, awaiting mining.");
        },
        TxLocation::None | TxLocation::NotStored => {
            warn!(
                batch_id = batch_id.as_str(),
                location:? = tx_query_response.location;
                "Batch location returned as Not Found/None"
            );
            return Err(anyhow!(
                "Transaction not found on Base Node (Location: {:?}). It may have been dropped or reorged.",
                tx_query_response.location
            ));
        },
    }

    Ok(())
}

async fn handle_mined_transaction(
    db_pool: &SqlitePool,
    base_node_client: &Client,
    batch_id: &str,
    tx_query_response: &tari_transaction_components::rpc::models::TxQueryResponse,
    signed_tx: &SignedOneSidedTransactionResult,
    required_confirmations: u64,
) -> Result<(), anyhow::Error> {
    let mined_height = tx_query_response
        .mined_height
        .ok_or_else(|| anyhow!("Mined transaction missing mined_height"))?;

    let tip_info = base_node_client
        .get_tip_info()
        .await
        .context("Failed to get tip info from Base Node")?;

    let best_block_height = tip_info
        .metadata
        .ok_or_else(|| anyhow!("Tip info missing metadata"))?
        .best_block_height();

    let confirmations = best_block_height.saturating_sub(mined_height) + 1;

    info!(
        batch_id = batch_id,
        mined_height = mined_height,
        tip_height = best_block_height,
        confirmations = confirmations,
        required = required_confirmations;
        "Batch Confirmation Status"
    );

    if confirmations >= required_confirmations {
        info!(batch_id = batch_id; "Confirmation threshold reached. Finalizing...");

        let mined_header_hash = tx_query_response
            .mined_header_hash
            .clone()
            .ok_or_else(|| anyhow!("Mined transaction missing mined_header_hash"))?;
        let mined_timestamp = tx_query_response
            .mined_timestamp
            .ok_or_else(|| anyhow!("Mined transaction missing mined_timestamp"))?;

        let mut tx = db_pool.begin().await.context("Failed to begin DB transaction")?;

        PaymentBatch::update_to_confirmed(
            &mut tx,
            batch_id,
            mined_height,
            mined_header_hash.clone(),
            mined_timestamp,
        )
        .await
        .context("Failed to update batch to Confirmed")?;

        let associated_payments = Payment::find_by_batch_id(&mut tx, batch_id)
            .await
            .context("Failed to fetch associated payments")?;

        info!(
            batch_id = batch_id,
            count = associated_payments.len();
            "Marking associated payments as confirmed"
        );

        let sent_hashes = &signed_tx.signed_transaction.sent_hashes;
        anyhow::ensure!(
            associated_payments.len() == sent_hashes.len(),
            "Mismatch between associated payments count ({}) and sent hashes count ({})",
            associated_payments.len(),
            sent_hashes.len()
        );

        let mined_header_hash = FixedHash::try_from(mined_header_hash)?;
        for (payment, sent_hash) in associated_payments.iter().zip(sent_hashes) {
            let payref = hex::encode(generate_payment_reference(&mined_header_hash, sent_hash));
            Payment::update_payment_to_confirmed(&mut tx, &payment.id, &payref).await?;
        }
        tx.commit().await.context("Failed to commit DB transaction")?;

        info!(
            target: "audit",
            batch_id = batch_id,
            height = mined_height,
            timestamp = mined_timestamp;
            "Batch successfully CONFIRMED"
        );
    } else {
        info!(
            batch_id = batch_id,
            current = confirmations,
            required = required_confirmations;
            "Batch awaiting more confirmations"
        );
    }

    Ok(())
}

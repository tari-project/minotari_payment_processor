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

use crate::db::block_header::BlockHeader;
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
    // First, check for reorgs by comparing the new tip with our stored chain
    let reorg_height = check_for_reorgs_and_update_headers(db_pool, base_node_client).await?;

    // If a reorg was detected, handle all affected transactions
    if let Some(reorg_height) = reorg_height {
        handle_reorg_at_height(db_pool, base_node_client, reorg_height).await?;
    }

    // Then process batches awaiting confirmation
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

/// Checks for chain reorgs by comparing the new tip with our stored headers.
/// Returns the reorg height if a reorg was detected, None otherwise.
/// Also updates the stored headers to match the current chain.
async fn check_for_reorgs_and_update_headers(
    db_pool: &SqlitePool,
    base_node_client: &Client,
) -> Result<Option<u64>, anyhow::Error> {
    let tip_info = base_node_client
        .get_tip_info()
        .await
        .context("Failed to get tip info from Base Node")?;

    let metadata = tip_info.metadata.ok_or_else(|| anyhow!("Tip info missing metadata"))?;

    let new_tip_height = metadata.best_block_height();
    let new_tip_hash = hex::encode(metadata.best_block_hash());
    let new_tip_prev_hash = hex::encode(metadata.prev_hash());

    let mut conn = db_pool.acquire().await?;

    // Get our stored tip
    let stored_tip = BlockHeader::get_tip(&mut conn).await?;

    let reorg_height = match stored_tip {
        None => {
            // First time running - no stored headers yet
            info!(
                tip_height = new_tip_height,
                tip_hash = new_tip_hash.as_str();
                "No stored headers - initializing header tracking"
            );
            None
        },
        Some(stored) => {
            if stored.header_hash == new_tip_hash {
                // Same tip - no change
                debug!(tip_height = new_tip_height; "Chain tip unchanged");
                return Ok(None);
            }

            if stored.height as u64 == new_tip_height - 1 && stored.header_hash == new_tip_prev_hash {
                // Normal case: new block extends our known chain
                debug!(
                    old_height = stored.height,
                    new_height = new_tip_height;
                    "Chain extended by 1 block"
                );
                None
            } else {
                // Potential reorg - need to find where chains diverge
                info!(
                    stored_height = stored.height,
                    stored_hash = stored.header_hash.as_str(),
                    new_tip_height = new_tip_height,
                    new_tip_hash = new_tip_hash.as_str();
                    "Potential reorg detected - finding common ancestor"
                );

                find_reorg_height(db_pool, base_node_client, &stored, new_tip_height).await?
            }
        },
    };

    // Update stored headers with new chain data (deletes invalidated headers first if reorg detected)
    update_stored_headers(
        db_pool,
        base_node_client,
        new_tip_height,
        &new_tip_hash,
        &new_tip_prev_hash,
        reorg_height,
    )
    .await?;

    Ok(reorg_height)
}

/// Finds the height at which a reorg occurred by walking back from the new tip
/// until we find a block hash that matches our stored chain.
async fn find_reorg_height(
    db_pool: &SqlitePool,
    base_node_client: &Client,
    stored_tip: &BlockHeader,
    new_tip_height: u64,
) -> Result<Option<u64>, anyhow::Error> {
    let mut conn = db_pool.acquire().await?;

    // Start from the minimum of stored tip height and new tip height
    let mut check_height = std::cmp::min(stored_tip.height as u64, new_tip_height);

    // Walk back through the chain to find where they diverge
    loop {
        // Get the header at this height from the base node
        let headers = base_node_client
            .get_header_by_height(check_height)
            .await
            .context("Failed to get header from base node")?;

        let node_header = headers
            .first()
            .ok_or_else(|| anyhow!("No header returned for height {}", check_height))?;

        let node_hash = hex::encode(&node_header.hash);

        // Check if we have this header stored
        if let Some(stored_header) = BlockHeader::get_by_height(&mut conn, check_height).await? {
            if stored_header.header_hash == node_hash {
                // Found the common ancestor - reorg happened at the next block
                let reorg_at = check_height + 1;
                info!(
                    common_ancestor_height = check_height,
                    reorg_height = reorg_at;
                    "Found common ancestor - reorg detected"
                );
                return Ok(Some(reorg_at));
            }
        } else {
            // We don't have this height stored - we've gone back further than our history
            // This shouldn't normally happen if we store 2000 blocks
            warn!(
                height = check_height;
                "No stored header at height - cannot determine exact reorg point"
            );
            // Consider all our stored history as potentially reorged
            let oldest_stored = sqlx::query_scalar!(r#"SELECT MIN(height) as "height: i64" FROM block_headers"#)
                .fetch_one(&mut *conn)
                .await?;

            if let Some(oldest) = oldest_stored {
                return Ok(Some(oldest as u64));
            }
            return Ok(None);
        }

        if check_height == 0 {
            // Reached genesis - this shouldn't happen in practice
            return Err(anyhow!(
                "Reorg detection reached genesis without finding common ancestor"
            ));
        }

        check_height -= 1;
    }
}

/// Updates the stored block headers with the current chain state.
/// If a reorg_height is provided, deletes all headers at and above that height first.
async fn update_stored_headers(
    db_pool: &SqlitePool,
    base_node_client: &Client,
    tip_height: u64,
    tip_hash: &str,
    tip_prev_hash: &str,
    reorg_height: Option<u64>,
) -> Result<(), anyhow::Error> {
    let mut conn = db_pool.acquire().await?;

    // If a reorg was detected, delete invalidated headers first
    if let Some(reorg_at) = reorg_height {
        let deleted = BlockHeader::delete_at_and_above_height(&mut conn, reorg_at).await?;
        info!(
            deleted_headers = deleted,
            from_height = reorg_at;
            "Deleted invalidated block headers due to reorg"
        );
    }

    // Get our current stored tip (after potential deletion)
    let stored_tip = BlockHeader::get_tip(&mut conn).await?;
    let stored_height = stored_tip.as_ref().map(|h| h.height as u64).unwrap_or(0);

    // If we're behind, we need to catch up
    if stored_height < tip_height {
        let start_height = if stored_height == 0 {
            // First time - just store the tip
            tip_height
        } else {
            // Catch up from where we left off
            stored_height + 1
        };

        // Fetch and store missing headers
        for height in start_height..=tip_height {
            let (hash, prev_hash) = if height == tip_height {
                // Use the tip info we already have
                (tip_hash.to_string(), tip_prev_hash.to_string())
            } else {
                // Fetch from base node
                let headers = base_node_client
                    .get_header_by_height(height)
                    .await
                    .context("Failed to get header from base node")?;

                let header = headers
                    .first()
                    .ok_or_else(|| anyhow!("No header returned for height {}", height))?;

                (hex::encode(&header.hash), hex::encode(&header.prev_hash))
            };

            BlockHeader::upsert(&mut conn, height, &hash, &prev_hash).await?;
        }

        debug!(
            from_height = start_height,
            to_height = tip_height;
            "Updated stored headers"
        );
    }

    // Prune old headers to keep only the last 2000
    BlockHeader::prune_old_headers(&mut conn).await?;

    Ok(())
}

/// Handles a reorg at the specified height by checking all transactions that may have been affected.
async fn handle_reorg_at_height(
    db_pool: &SqlitePool,
    base_node_client: &Client,
    reorg_height: u64,
) -> Result<(), anyhow::Error> {
    info!(
        target: "audit",
        reorg_height = reorg_height;
        "Handling reorg at height - checking all affected transactions"
    );

    let mut conn = db_pool.acquire().await?;

    // Find all confirmed batches that were mined at or after the reorg height
    let affected_batches = PaymentBatch::find_confirmed_at_or_after_height(&mut conn, reorg_height).await?;

    if affected_batches.is_empty() {
        info!(reorg_height = reorg_height; "No confirmed batches affected by reorg");
        return Ok(());
    }

    warn!(
        count = affected_batches.len(),
        reorg_height = reorg_height;
        "Found confirmed batches potentially affected by reorg"
    );

    // Check each affected batch
    for batch in affected_batches {
        if let Err(e) = check_and_handle_batch_reorg(db_pool, base_node_client, &batch).await {
            error!(
                batch_id = &*batch.id,
                error:? = e;
                "Error handling reorg for batch"
            );
        }
    }

    Ok(())
}

/// Checks a single batch to see if it was affected by a reorg and handles it accordingly.
async fn check_and_handle_batch_reorg(
    db_pool: &SqlitePool,
    base_node_client: &Client,
    batch: &PaymentBatch,
) -> Result<(), anyhow::Error> {
    let batch_id = &batch.id;

    let payload = match &batch.signed_tx_json {
        Some(payload) => BatchPayload::from_json(payload)?,
        None => return Err(anyhow!("Confirmed batch {} has no signed_tx_json", batch_id)),
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

    let tx_query_response = base_node_client
        .transaction_query(excess_sig_nonce, excess_sig_sig)
        .await
        .context("Failed to query transaction from Base Node for reorg check")?;

    match tx_query_response.location {
        TxLocation::Mined => {
            // Transaction is still mined - check if it's in the same block
            let current_header_hash = tx_query_response.mined_header_hash.as_ref().map(hex::encode);
            let stored_header_hash = batch.mined_header_hash.as_ref();

            if let (Some(current_hash), Some(stored_hash)) = (&current_header_hash, stored_header_hash) {
                if current_hash != stored_hash {
                    // Re-mined in a different block
                    warn!(
                        batch_id = batch_id,
                        stored_hash = stored_hash.as_str(),
                        current_hash = current_hash.as_str();
                        "Transaction re-mined in different block after reorg"
                    );
                    handle_reorg_and_remine(db_pool, batch_id, &tx_query_response).await?;
                } else {
                    debug!(batch_id = batch_id; "Transaction still in same block after reorg check");
                }
            }
        },
        TxLocation::InMempool => {
            warn!(
                batch_id = batch_id,
                original_height:? = batch.mined_height;
                "Confirmed transaction found in mempool - REORG DETECTED"
            );
            handle_reorg(db_pool, batch).await?;
        },
        TxLocation::None | TxLocation::NotStored => {
            warn!(
                batch_id = batch_id,
                original_height:? = batch.mined_height,
                location:? = tx_query_response.location;
                "Confirmed transaction not found on chain - REORG DETECTED"
            );
            handle_reorg(db_pool, batch).await?;
        },
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

/// Handles a reorg by reverting the batch and its payments back to their pre-confirmation state.
async fn handle_reorg(db_pool: &SqlitePool, batch: &PaymentBatch) -> Result<(), anyhow::Error> {
    let batch_id = &batch.id;
    let original_height = batch.mined_height;

    info!(
        target: "audit",
        batch_id = batch_id,
        original_height:? = original_height;
        "Handling reorg - reverting batch and payments"
    );

    let mut tx = db_pool
        .begin()
        .await
        .context("Failed to begin DB transaction for reorg handling")?;

    // Revert payments in this batch back to BATCHED status
    Payment::revert_payments_to_batched_due_to_reorg(&mut tx, batch_id)
        .await
        .context("Failed to revert payments due to reorg")?;

    // Revert the batch back to AWAITING_CONFIRMATION status
    PaymentBatch::revert_to_awaiting_confirmation_due_to_reorg(&mut tx, batch_id, original_height)
        .await
        .context("Failed to revert batch due to reorg")?;

    tx.commit()
        .await
        .context("Failed to commit reorg handling transaction")?;

    warn!(
        target: "audit",
        batch_id = batch_id,
        original_height:? = original_height;
        "REORG HANDLED - batch reverted to AWAITING_CONFIRMATION, payments reverted to BATCHED"
    );

    Ok(())
}

/// Handles the case where a transaction was reorged but then re-mined in a different block.
async fn handle_reorg_and_remine(
    db_pool: &SqlitePool,
    batch_id: &str,
    tx_query_response: &tari_transaction_components::rpc::models::TxQueryResponse,
) -> Result<(), anyhow::Error> {
    let new_height = tx_query_response
        .mined_height
        .ok_or_else(|| anyhow!("Missing mined_height in response"))?;
    let new_header_hash = tx_query_response
        .mined_header_hash
        .clone()
        .ok_or_else(|| anyhow!("Missing mined_header_hash in response"))?;
    let new_timestamp = tx_query_response
        .mined_timestamp
        .ok_or_else(|| anyhow!("Missing mined_timestamp in response"))?;

    info!(
        target: "audit",
        batch_id = batch_id,
        new_height = new_height,
        new_header_hash = hex::encode(&new_header_hash);
        "Transaction re-mined after reorg - updating mined info"
    );

    let mut conn = db_pool.acquire().await?;

    // Update the batch with new mined info (keeping CONFIRMED status)
    let header_hash_hex = hex::encode(&new_header_hash);
    let new_height_i64 = new_height as i64;
    let new_timestamp_i64 = new_timestamp as i64;
    sqlx::query!(
        r#"
        UPDATE payment_batches
        SET mined_height = ?, mined_header_hash = ?, mined_timestamp = ?, updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
        "#,
        new_height_i64,
        header_hash_hex,
        new_timestamp_i64,
        batch_id
    )
    .execute(&mut *conn)
    .await
    .context("Failed to update batch with new mined info after reorg")?;

    // Update payref for all payments since it depends on the header hash
    let batch = PaymentBatch::find_by_id(&mut conn, batch_id)
        .await?
        .ok_or_else(|| anyhow!("Batch not found"))?;

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
    let associated_payments = Payment::find_by_batch_id(&mut conn, batch_id).await?;
    let sent_hashes = &signed_tx.signed_transaction.sent_hashes;

    if associated_payments.len() == sent_hashes.len() {
        let new_header_hash_fixed = FixedHash::try_from(new_header_hash)?;
        for (payment, sent_hash) in associated_payments.iter().zip(sent_hashes) {
            let new_payref = hex::encode(generate_payment_reference(&new_header_hash_fixed, sent_hash));
            sqlx::query!(
                r#"
                UPDATE payments
                SET payref = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                "#,
                new_payref,
                payment.id
            )
            .execute(&mut *conn)
            .await?;
        }
    }

    info!(
        target: "audit",
        batch_id = batch_id,
        new_height = new_height;
        "Batch mined info updated after reorg re-mine"
    );

    Ok(())
}

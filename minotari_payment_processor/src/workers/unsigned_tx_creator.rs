use anyhow::{Context, anyhow};
use log::{debug, error, info, warn};
use minotari_client::apis::{Error as ApiError, accounts_api, configuration::Configuration};
use minotari_client::models::LockFundsRequest;
use sqlx::{SqliteConnection, SqlitePool};
use std::collections::HashMap;
use std::sync::Arc;
use tari_common::configuration::Network;
use tari_common_types::tari_address::TariAddress;
use tari_common_types::transaction::TxId;
use tari_script::TariScript;
use tari_transaction_components::consensus::ConsensusConstantsBuilder;
use tari_transaction_components::key_manager::{
    KeyManager,
    wallet_types::{ViewWallet, WalletType},
};
use tari_transaction_components::offline_signing::{PaymentRecipient, prepare_one_sided_transaction_for_signing};
use tari_transaction_components::{
    TransactionBuilder,
    fee::Fee,
    helpers::borsh::SerializedSize,
    tari_amount::MicroMinotari,
    transaction_components::{MemoField, OutputFeatures, WalletOutput, covenants::Covenant, memo_field::TxType},
    weight::TransactionWeight,
};
use tokio::time::{self, Duration};

use crate::config::PaymentReceiverAccount;
use crate::db::payment::Payment;
use crate::db::payment_batch::{BatchPayload, PaymentBatch, PaymentBatchStatus, StepPayload, TransactionStep};
use crate::utils::log::{mask_amount, mask_string};
use crate::workers::types::IntermediateContext;

const DEFAULT_SLEEP_SECS: u64 = 15;
const FEE_PER_GRAM: u64 = 5;
// Buffer to ensure we have enough funds left for the final payment after paying for split fees.
const FEE_BUFFER_AMOUNT: i64 = 200_000;

pub async fn run(
    db_pool: SqlitePool,
    client_config: Arc<Configuration>,
    network: Network,
    accounts: HashMap<String, PaymentReceiverAccount>,
    max_input_count_per_tx: usize,
    sleep_secs: Option<u64>,
) {
    let sleep_secs = sleep_secs.unwrap_or(DEFAULT_SLEEP_SECS);
    info!(
        interval = sleep_secs;
        "Unsigned Transaction Creator worker started"
    );

    let mut interval = time::interval(Duration::from_secs(sleep_secs));

    loop {
        interval.tick().await;
        if let Err(e) =
            process_unsigned_transactions(&db_pool, &client_config, network, &accounts, max_input_count_per_tx).await
        {
            error!(
                error:? = e;
                "Unsigned Transaction Creator worker error"
            );
        }
    }
}

async fn process_unsigned_transactions(
    db_pool: &SqlitePool,
    client_config: &Configuration,
    network: Network,
    accounts: &HashMap<String, PaymentReceiverAccount>,
    max_input_count_per_tx: usize,
) -> Result<(), anyhow::Error> {
    let mut conn = db_pool.acquire().await?;

    let batches = PaymentBatch::find_by_status(&mut conn, PaymentBatchStatus::PendingBatching).await?;

    if !batches.is_empty() {
        info!(
            count = batches.len();
            "Found batches pending unsigned transaction creation"
        );
    }

    for batch in batches {
        if let Err(e) = process_single_batch(
            &mut conn,
            client_config,
            network,
            accounts,
            &batch,
            max_input_count_per_tx,
        )
        .await
        {
            let error_message = e.to_string();
            error!(
                batch_id = &*batch.id,
                error = &*error_message;
                "Error processing batch. Incrementing retry count."
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
    conn: &mut SqliteConnection,
    client_config: &Configuration,
    network: Network,
    accounts: &HashMap<String, PaymentReceiverAccount>,
    batch: &PaymentBatch,
    max_input_count_per_tx: usize,
) -> Result<(), anyhow::Error> {
    let batch_id = &batch.id;
    info!(batch_id = batch_id.as_str(); "Starting processing for Batch");

    let associated_payments = Payment::find_by_batch_id(conn, batch_id)
        .await
        .context("Failed to fetch payments for batch")?;

    if associated_payments.is_empty() {
        warn!(batch_id = batch_id.as_str(); "Batch has no active payments. Marking batch as CANCELLED.");
        PaymentBatch::update_to_failed(conn, batch_id, "No active payments found in batch").await?;
        return Ok(());
    }

    let account_name = &batch.account_name;
    let sender_account = accounts
        .get(&account_name.to_lowercase())
        .ok_or_else(|| anyhow!("Account '{}' not found in local configuration", account_name))?;

    // --- CYCLE 2 (Finalize) OR CYCLE 1 (Inputs Check) ---
    if let Some(context_json) = &batch.intermediate_context_json {
        // === CYCLE 2: FINALIZE ===
        info!(
            batch_id = batch_id.as_str();
            "Found intermediate context. Executing CYCLE 2 (Finalize)."
        );

        let context = IntermediateContext::from_json(context_json)?;
        let inputs = context.utxos;

        info!(
            batch_id = batch_id.as_str(),
            input_count = inputs.len();
            "Using intermediate inputs for final transaction"
        );

        let final_step = create_transaction_step(network, sender_account, inputs, &associated_payments, 0).await?;

        let payload = BatchPayload {
            steps: vec![final_step],
        };
        let payload_json = payload.to_json()?;

        PaymentBatch::update_to_awaiting_signature(conn, batch_id, &payload_json)
            .await
            .context("Failed to update batch to AwaitingSignature (Cycle 2)")?;

        info!(batch_id = batch_id.as_str(); "Cycle 2 preparation complete. Ready for signature.");
    } else {
        // === CYCLE 1: FETCH & ANALYZE ===
        info!(batch_id = batch_id.as_str(); "No context found. Fetching fresh UTXOs from API.");

        let payment_total: i64 = associated_payments.iter().map(|p| p.amount).sum();
        let amount_to_lock = payment_total + FEE_BUFFER_AMOUNT;
        let account_balance = accounts_api::api_get_balance(client_config, account_name).await?;
        let balance = account_balance.available;

        if balance < amount_to_lock {
            warn!(
                batch_id = batch_id.as_str(),
                account = &*mask_string(account_name.as_str()),
                requested = &*mask_amount(amount_to_lock),
                actual = &*mask_amount(balance);
                "Not enough funds in wallet"
            );
            return Ok(());
        }

        let lock_request = LockFundsRequest {
            amount: amount_to_lock,
            idempotency_key: Some(Some(batch.pr_idempotency_key.clone())),
            ..Default::default()
        };

        let locked_funds = match accounts_api::api_lock_funds(client_config, account_name, lock_request).await {
            Ok(res) => res,
            Err(ApiError::ResponseError(c)) => return Err(anyhow!("PR API Error: {} - {}", c.status, c.content)),
            Err(e) => return Err(anyhow!("Network error calling PR API: {:?}", e)),
        };

        let mut inputs: Vec<WalletOutput> = Vec::new();
        for utxo_val in locked_funds.utxos {
            let utxo: WalletOutput =
                serde_json::from_value(utxo_val).map_err(|e| anyhow!("Failed to deserialize UTXO: {}", e))?;
            inputs.push(utxo);
        }

        info!(
            target: "audit",
            batch_id = batch_id.as_str(),
            input_count = inputs.len();
            "Successfully locked funds. API returned UTXOs."
        );

        if inputs.len() > max_input_count_per_tx {
            // === SPLIT LOGIC ===
            info!(
                batch_id = batch_id.as_str(),
                input_count = inputs.len(),
                limit = max_input_count_per_tx;
                "Input count exceeds limit. Initiating SPLIT (CoinJoin)."
            );

            let chunks = inputs.chunks(max_input_count_per_tx);
            let mut steps = Vec::new();

            for (i, chunk) in chunks.enumerate() {
                let tx_step = create_self_spend_step(network, sender_account, chunk.to_vec(), i).await?;
                steps.push(tx_step);
            }

            let payload = BatchPayload { steps };
            let payload_json = payload.to_json()?;

            PaymentBatch::update_to_awaiting_signature(conn, batch_id, &payload_json)
                .await
                .context("Failed to update batch to AwaitingSignature (Split Cycle)")?;

            info!(
                batch_id = batch_id.as_str(),
                steps_count = payload.steps.len();
                "Split Cycle preparation complete."
            );
        } else {
            // === NORMAL LOGIC ===
            info!(
                batch_id = batch_id.as_str();
                "Input count within limits. creating standard transaction."
            );

            let step = create_transaction_step(network, sender_account, inputs, &associated_payments, 0).await?;

            let payload = BatchPayload { steps: vec![step] };
            let payload_json = payload.to_json()?;

            PaymentBatch::update_to_awaiting_signature(conn, batch_id, &payload_json)
                .await
                .context("Failed to update batch to AwaitingSignature (Normal)")?;

            info!(
                target: "audit",
                batch_id = batch_id.as_str();
                "Transaction construction complete. Updated to 'AwaitingSignature'."
            );
        }
    }

    Ok(())
}

async fn prepare_signing_request(
    network: Network,
    tx_id: TxId,
    sender_account: &PaymentReceiverAccount,
    inputs: &[WalletOutput],
    recipients: &[PaymentRecipient],
) -> Result<String, anyhow::Error> {
    let view_wallet = ViewWallet::new(
        sender_account.public_spend_key.clone(),
        sender_account.view_key.clone(),
        None,
    );
    let key_manager = KeyManager::new(WalletType::ViewWallet(view_wallet)).context("Failed to create KeyManager")?;

    let consensus_constants = ConsensusConstantsBuilder::new(network).build();
    let mut tx_builder = TransactionBuilder::new(consensus_constants, key_manager, network)
        .context("Failed to create TransactionBuilder")?;

    tx_builder.with_fee_per_gram(MicroMinotari(FEE_PER_GRAM));

    for input in inputs {
        tx_builder.with_input(input.clone()).context("Failed to add input")?;
    }

    let prepare_result = prepare_one_sided_transaction_for_signing(
        tx_id,
        tx_builder,
        recipients,
        MemoField::new_empty(),
        sender_account.address.clone(),
    )
    .context("Failed to prepare one-sided transaction")?;

    let tx_json = serde_json::to_string(&prepare_result)?;
    Ok(tx_json)
}

fn get_single_output_metadata_size(fee_calc: &Fee) -> Result<usize, anyhow::Error> {
    let output_features_size = OutputFeatures::default()
        .get_serialized_size()
        .map_err(|e| anyhow!("Serialization error: {}", e))?;
    let tari_script_size = TariScript::default()
        .get_serialized_size()
        .map_err(|e| anyhow!("Serialization error: {}", e))?;
    let covenant_size = Covenant::default()
        .get_serialized_size()
        .map_err(|e| anyhow!("Serialization error: {}", e))?;

    Ok(fee_calc
        .weighting()
        .round_up_features_and_scripts_size(output_features_size + tari_script_size + covenant_size))
}

async fn create_transaction_step(
    network: Network,
    sender_account: &PaymentReceiverAccount,
    inputs: Vec<WalletOutput>,
    payments: &[Payment],
    step_index: usize,
) -> Result<TransactionStep, anyhow::Error> {
    let tx_id = TxId::new_random();
    let output_features = OutputFeatures::default();
    let recipients: Vec<PaymentRecipient> = payments
        .iter()
        .map(|p| -> Result<PaymentRecipient, anyhow::Error> {
            let payment_id = match &p.payment_id {
                Some(s) => MemoField::new_open_from_string(s, TxType::PaymentToOther)
                    .map_err(|e| anyhow!(e))
                    .context("Failed to create payment ID memo")?,
                None => MemoField::new_empty(),
            };

            let recipient_address = TariAddress::from_base58(&p.recipient_address)
                .map_err(|e| anyhow!(e.to_string()))
                .context("Invalid recipient address")?;

            Ok(PaymentRecipient {
                amount: MicroMinotari(p.amount as u64),
                output_features: output_features.clone(),
                address: recipient_address,
                payment_id,
            })
        })
        .collect::<Result<Vec<PaymentRecipient>, anyhow::Error>>()?;
    let tx_json = prepare_signing_request(network, tx_id, sender_account, &inputs, &recipients).await?;

    Ok(TransactionStep {
        step_index,
        is_consolidation: false,
        payload: StepPayload::Unsigned(tx_json),
        tx_id,
    })
}

async fn create_self_spend_step(
    network: Network,
    sender_account: &PaymentReceiverAccount,
    inputs: Vec<WalletOutput>,
    step_index: usize,
) -> Result<TransactionStep, anyhow::Error> {
    let tx_id = TxId::new_random();

    let total_input_value: MicroMinotari = inputs.iter().map(|p| p.value()).sum();
    let fee_calc = Fee::new(TransactionWeight::latest());
    let output_metadata_size = get_single_output_metadata_size(&fee_calc)?;
    let calculated_fee = fee_calc.calculate(MicroMinotari(FEE_PER_GRAM), 1, inputs.len(), 1, output_metadata_size);

    if calculated_fee >= total_input_value {
        return Err(anyhow!(
            "Input value {} is too small to cover fees {}",
            total_input_value,
            calculated_fee
        ));
    }

    let amount_to_self = total_input_value - calculated_fee;

    debug!(
        step_index = step_index,
        inputs_sum = &*mask_amount(total_input_value.as_u64() as i64),
        inputs_count = inputs.len(),
        fee = &*mask_amount(calculated_fee.as_u64() as i64),
        net_output = &*mask_amount(amount_to_self.as_u64() as i64);
        "Self-Spend Step"
    );

    let output_features = OutputFeatures::default();
    let recipient = PaymentRecipient {
        amount: amount_to_self,
        output_features,
        address: sender_account.address.clone(),
        payment_id: MemoField::new_empty(),
    };

    let recipients = vec![recipient];
    let tx_json = prepare_signing_request(network, tx_id, sender_account, &inputs, &recipients).await?;

    Ok(TransactionStep {
        step_index,
        is_consolidation: true,
        payload: StepPayload::Unsigned(tx_json),
        tx_id,
    })
}

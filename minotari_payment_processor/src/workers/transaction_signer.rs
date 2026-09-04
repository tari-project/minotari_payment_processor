//! Signs prepared transactions by shelling out to the standalone `minotari_offline_signer` binary.
//!
//! # Payload format coupling
//!
//! The unsigned JSON handed to the signer, and the signed JSON it hands back, are versioned by the
//! `tari_transaction_components` offline-signing payload format, and both sides validate that
//! version strictly: [`TransactionResult::from_json`] rejects outright any payload whose version is
//! not exactly the one it was compiled against.
//!
//! This crate pins `tari_transaction_components` to a fixed revision (see `Cargo.toml`), which in
//! turn fixes the payload format it speaks. The `minotari_offline_signer` binary configured via
//! `OFFLINE_SIGNER_PATH` is built and deployed separately, so **it must come from a tari revision
//! that speaks the same payload format**. Tari v5.4.0 moved the format from `4.0.0` to `5.0.0` and
//! added a mandatory `payload_signature` field; a signer built from v5.4.0 or later can therefore
//! neither consume the payloads produced here nor produce output this crate can parse, and every
//! signing attempt will fail. See the "Offline Signer Setup" section of `README.md` for the
//! revisions that are known to match.

use anyhow::{Context, anyhow};
use log::{debug, error, info};
use sqlx::{SqliteConnection, SqlitePool};
use std::ffi::OsString;
use std::io::Write;
use std::path::Path;
use tari_common::configuration::Network;
use tari_transaction_components::key_manager::SerializedKeyString;
use tari_transaction_components::key_manager::TariKeyId;
use tari_transaction_components::offline_signing::models::{
    SignedOneSidedTransactionResult, TransactionResult, get_latest_version,
};
use tari_transaction_components::transaction_components::TransactionError;
use tempfile::NamedTempFile;
use tokio::fs;
use tokio::process::Command;
use tokio::task;
use tokio::time::{self, Duration};

use crate::config::Passphrase;
use crate::db::payment_batch::StepPayload;
use crate::db::payment_batch::{BatchPayload, PaymentBatch, PaymentBatchStatus};
use crate::workers::types::IntermediateContext;

const DEFAULT_SLEEP_SECS: u64 = 10;

/// The environment variable the offline signer reads the keystore passphrase from. It is passed
/// this way, rather than as a `--passphrase` argument, so that the passphrase never ends up in the
/// process' argv where other users could read it (e.g. via `ps`).
const PASSPHRASE_ENV_VAR: &str = "TARI_PASSPHRASE";

pub async fn run(
    db_pool: SqlitePool,
    network: Network,
    offline_signer_path: String,
    offline_signer_passphrase: Passphrase,
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
        if let Err(e) =
            process_transactions_to_sign(&db_pool, network, &offline_signer_path, &offline_signer_passphrase).await
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
    offline_signer_path: &str,
    offline_signer_passphrase: &Passphrase,
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
            offline_signer_path,
            offline_signer_passphrase,
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
    offline_signer_path: &str,
    offline_signer_passphrase: &Passphrase,
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

        // Both temp files are kept alive for the duration of the signing call; dropping them
        // removes the files from disk.
        let input_file = NamedTempFile::with_prefix(format!("unsigned-tx-{}-step{}-", batch_id, i))
            .context("Failed to create temp input file")?;

        let input_file = write_temp_file(input_file, unsigned_json.as_bytes().to_vec())
            .await
            .context("Failed to write unsigned tx to temp file")?;
        let input_path = input_file.path();

        let output_file = NamedTempFile::with_prefix(format!("signed-tx-{}-step{}-", batch_id, i))
            .context("Failed to create temp output file")?;
        let output_path = output_file.path();

        sign_with_cli(
            network,
            offline_signer_path,
            offline_signer_passphrase,
            input_path,
            output_path,
        )
        .await
        .with_context(|| format!("External signing process failed for step {}", i))?;

        let signed_json = fs::read_to_string(output_path)
            .await
            .context("Failed to read signed transaction from output file")?;
        let signed_tx_wrapper = SignedOneSidedTransactionResult::from_json(&signed_json)
            .map_err(|e| signed_payload_error(i, offline_signer_path, e))?;

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

/// Writes `contents` into `file` and returns the same [`NamedTempFile`] back.
///
/// The write goes through the temp file's own file descriptor rather than re-opening its path by
/// name. The contents decide what the signer signs, so the write must not be vulnerable to the path
/// being swapped for a symlink between the file's creation and the write.
///
/// The blocking write is moved off the async executor with [`task::spawn_blocking`]. The
/// `NamedTempFile` is moved into the closure and handed back out of it so that its RAII cleanup is
/// preserved; on any error it is instead dropped inside the closure, which removes the file.
async fn write_temp_file(mut file: NamedTempFile, contents: Vec<u8>) -> Result<NamedTempFile, anyhow::Error> {
    task::spawn_blocking(move || -> std::io::Result<NamedTempFile> {
        file.as_file_mut().write_all(&contents)?;
        file.as_file_mut().flush()?;
        Ok(file)
    })
    .await
    .context("The task writing to the temp file panicked or was cancelled")?
    .context("Write to the temp file failed")
}

/// Wraps a failure to parse the signer's output in an explanation of the most likely cause.
///
/// A payload format mismatch between this crate's pinned `tari_transaction_components` and the
/// external signer binary (see the module documentation) surfaces here as an opaque
/// deserialization error, which would otherwise be retried and eventually fail the batch with no
/// hint as to why. The underlying error is kept as the source of the returned error.
fn signed_payload_error(step: usize, offline_signer_path: &str, error: TransactionError) -> anyhow::Error {
    anyhow::Error::new(error).context(format!(
        "Failed to deserialize the signed transaction for step {}. The most likely cause is that the offline \
         signer at '{}' was built from a tari revision incompatible with this service: this service only accepts \
         and produces offline-signing payload format {}, and any other version is rejected outright. Rebuild the \
         signer from a tari revision matching the pinned tari_transaction_components revision (see the \"Offline \
         Signer Setup\" section of README.md)",
        step,
        offline_signer_path,
        get_latest_version()
    ))
}

/// Builds the argument list for the `minotari_offline_signer sign` invocation.
///
/// The passphrase is deliberately not part of the arguments; it is supplied through the
/// [`PASSPHRASE_ENV_VAR`] environment variable instead.
fn build_sign_args(network: Network, input_path: &Path, output_path: &Path) -> Vec<OsString> {
    vec![
        OsString::from("sign"),
        OsString::from("--input-file"),
        input_path.as_os_str().to_os_string(),
        OsString::from("--output-file"),
        output_path.as_os_str().to_os_string(),
        OsString::from("--network"),
        OsString::from(network.to_string()),
    ]
}

/// Renders the command for logging purposes, with the passphrase redacted.
fn redacted_command_string(executable_path: &str, args: &[OsString]) -> String {
    let mut rendered = format!("{}=*** {}", PASSPHRASE_ENV_VAR, executable_path);
    for arg in args {
        rendered.push(' ');
        rendered.push_str(&arg.to_string_lossy());
    }
    rendered
}

/// Executes the Minotari Offline Signer to sign a single prepared transaction.
async fn sign_with_cli(
    network: Network,
    executable_path: &str,
    passphrase: &Passphrase,
    input_path: &Path,
    output_path: &Path,
) -> Result<(), anyhow::Error> {
    let args = build_sign_args(network, input_path, output_path);

    debug!(command = &*redacted_command_string(executable_path, &args); "Executing Command");

    let cmd_output = Command::new(executable_path)
        .env(PASSPHRASE_ENV_VAR, passphrase.reveal())
        .args(&args)
        .output()
        .await
        .context("Failed to execute offline signer command")?;

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

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn it_builds_the_offline_signer_sign_arguments() {
        let args = build_sign_args(
            Network::Esmeralda,
            &PathBuf::from("/tmp/unsigned.json"),
            &PathBuf::from("/tmp/signed.json"),
        );

        let args: Vec<String> = args.iter().map(|arg| arg.to_string_lossy().into_owned()).collect();
        assert_eq!(
            args,
            vec![
                "sign",
                "--input-file",
                "/tmp/unsigned.json",
                "--output-file",
                "/tmp/signed.json",
                "--network",
                &Network::Esmeralda.to_string(),
            ]
        );
    }

    #[test]
    fn it_never_puts_the_passphrase_in_the_arguments() {
        let args = build_sign_args(
            Network::MainNet,
            &PathBuf::from("/tmp/unsigned.json"),
            &PathBuf::from("/tmp/signed.json"),
        );

        assert!(!args.iter().any(|arg| arg == "--passphrase"));
    }

    #[test]
    fn it_redacts_the_passphrase_when_rendering_the_command() {
        let args = build_sign_args(
            Network::LocalNet,
            &PathBuf::from("/tmp/unsigned.json"),
            &PathBuf::from("/tmp/signed.json"),
        );

        let rendered = redacted_command_string("/usr/local/bin/minotari_offline_signer", &args);

        assert_eq!(
            rendered,
            format!(
                "TARI_PASSPHRASE=*** /usr/local/bin/minotari_offline_signer sign --input-file /tmp/unsigned.json \
                 --output-file /tmp/signed.json --network {}",
                Network::LocalNet
            )
        );
    }

    #[tokio::test]
    async fn it_writes_the_payload_through_the_temp_files_own_handle() {
        let file = NamedTempFile::with_prefix("unsigned-tx-test-").expect("temp file should be creatable");
        let path = file.path().to_path_buf();

        let file = write_temp_file(file, br#"{"version":"4.0.0"}"#.to_vec())
            .await
            .expect("writing to the temp file should succeed");

        assert_eq!(
            file.path(),
            path,
            "the temp file handle must be handed back, not re-created"
        );
        let contents = std::fs::read_to_string(&path).expect("the temp file should be readable");
        assert_eq!(contents, r#"{"version":"4.0.0"}"#);

        drop(file);
        assert!(!path.exists(), "dropping the temp file must remove it from disk");
    }

    #[test]
    fn it_explains_a_payload_format_mismatch_and_keeps_the_underlying_error() {
        let underlying =
            TransactionError::SerializationError("Unsupported version. Expected '4.0.0', got '5.0.0'".to_string());

        let err = signed_payload_error(2, "/usr/local/bin/minotari_offline_signer", underlying.clone());

        let rendered = format!("{:#}", err);
        assert!(
            rendered.contains(&get_latest_version().to_string()),
            "the expected payload format should be named: {}",
            rendered
        );
        assert!(
            rendered.contains("/usr/local/bin/minotari_offline_signer"),
            "the signer path should be named: {}",
            rendered
        );
        assert!(
            rendered.contains(&underlying.to_string()),
            "the underlying error must be preserved in the chain: {}",
            rendered
        );
        assert!(
            err.source().is_some(),
            "the underlying error must remain the source of the returned error"
        );
    }
}

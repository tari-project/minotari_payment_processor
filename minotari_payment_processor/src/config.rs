use anyhow::Context;
use config::{Config, Environment};
use serde::Deserialize;
use std::{collections::HashMap, str::FromStr};
use tari_common::configuration::Network;
use tari_common_types::{
    tari_address::{TariAddress, TariAddressFeatures},
    types::CompressedPublicKey,
};
use tari_crypto::keys::PublicKey;
use tari_crypto::{
    compressed_key::CompressedKey,
    ristretto::{RistrettoPublicKey, RistrettoSecretKey},
};
use tari_utilities::ByteArray;

#[derive(Debug, Clone)]
pub struct PaymentReceiverAccount {
    pub name: String,
    pub view_key: RistrettoSecretKey,
    pub public_spend_key: CompressedKey<RistrettoPublicKey>,
    pub address: TariAddress,
}

#[derive(Debug, Clone)]
pub struct PaymentProcessorEnv {
    pub tari_network: Network,
    pub database_url: String,
    pub payment_receiver: String,
    pub base_node: String,
    pub console_wallet_path: String,
    pub console_wallet_base_path: String,
    pub console_wallet_password: String,
    pub listen_ip: String,
    pub listen_port: u16,
    pub batch_creator_sleep_secs: Option<u64>,
    pub unsigned_tx_creator_sleep_secs: Option<u64>,
    pub transaction_signer_sleep_secs: Option<u64>,
    pub broadcaster_sleep_secs: Option<u64>,
    pub confirmation_checker_sleep_secs: Option<u64>,
    pub confirmation_checker_required_confirmations: Option<u64>,
    pub max_input_count_per_tx: usize,
    pub fee_per_gram: u64,
    pub accounts: HashMap<String, PaymentReceiverAccount>,
}

#[derive(Deserialize)]
struct RawAccount {
    name: String,
    view_key: String,
    public_spend_key: String,
}

#[derive(Deserialize)]
struct RawSettings {
    #[serde(default = "default_network_str")]
    tari_network: String,
    database_url: String,
    payment_receiver: String,
    base_node: String,
    console_wallet_path: String,
    console_wallet_base_path: String,
    console_wallet_password: String,
    #[serde(default = "default_ip")]
    listen_ip: String,
    #[serde(default = "default_port")]
    listen_port: u16,
    batch_creator_sleep_secs: Option<u64>,
    unsigned_tx_creator_sleep_secs: Option<u64>,
    transaction_signer_sleep_secs: Option<u64>,
    broadcaster_sleep_secs: Option<u64>,
    confirmation_checker_sleep_secs: Option<u64>,
    confirmation_checker_required_confirmations: Option<u64>,
    max_input_count_per_tx: Option<usize>,
    #[serde(default = "default_fee_per_gram")]
    fee_per_gram: u64,
    #[serde(default)]
    accounts: HashMap<String, RawAccount>,
}

fn default_ip() -> String {
    "0.0.0.0".to_string()
}
fn default_port() -> u16 {
    9145
}
fn default_network_str() -> String {
    "MainNet".to_string()
}
fn default_fee_per_gram() -> u64 {
    5
}

impl PaymentProcessorEnv {
    pub fn load() -> anyhow::Result<Self> {
        // For nested HashMaps (accounts), it supports "ACCOUNTS__KEY__FIELD" syntax.
        let s = Config::builder()
            .add_source(Environment::default().separator("__"))
            .build()?;

        let raw: RawSettings = s
            .try_deserialize()
            .context("Failed to read configuration from environment variables")?;

        Self::try_from(raw)
    }
}

impl TryFrom<RawSettings> for PaymentProcessorEnv {
    type Error = anyhow::Error;

    fn try_from(raw: RawSettings) -> Result<Self, Self::Error> {
        let tari_network = Network::from_str(&raw.tari_network)
            .context(format!("Failed to parse tari_network: {}", raw.tari_network))?;

        if raw.fee_per_gram == 0 {
            return Err(anyhow::anyhow!(
                "fee_per_gram must be greater than 0, otherwise transactions will never be mined"
            ));
        }

        let mut accounts = HashMap::new();
        for (_key, raw_acc) in raw.accounts {
            let view_key = parse_view_key(&raw_acc.view_key)
                .context(format!("Failed to parse view_key for account '{}'", raw_acc.name))?;

            let public_spend_key = parse_public_spend_key(&raw_acc.public_spend_key).context(format!(
                "Failed to parse public_spend_key for account '{}'",
                raw_acc.name
            ))?;

            let address = TariAddress::new_dual_address(
                CompressedPublicKey::new_from_pk(RistrettoPublicKey::from_secret_key(&view_key)),
                public_spend_key.clone(),
                tari_network,
                TariAddressFeatures::create_one_sided_only(),
                None,
            )?;

            accounts.insert(
                raw_acc.name.clone().to_lowercase(),
                PaymentReceiverAccount {
                    name: raw_acc.name,
                    view_key,
                    public_spend_key,
                    address,
                },
            );
        }

        Ok(Self {
            tari_network,
            database_url: raw.database_url,
            payment_receiver: raw.payment_receiver,
            base_node: raw.base_node,
            console_wallet_path: raw.console_wallet_path,
            console_wallet_base_path: raw.console_wallet_base_path,
            console_wallet_password: raw.console_wallet_password,
            listen_ip: raw.listen_ip,
            listen_port: raw.listen_port,
            batch_creator_sleep_secs: raw.batch_creator_sleep_secs,
            unsigned_tx_creator_sleep_secs: raw.unsigned_tx_creator_sleep_secs,
            transaction_signer_sleep_secs: raw.transaction_signer_sleep_secs,
            broadcaster_sleep_secs: raw.broadcaster_sleep_secs,
            confirmation_checker_sleep_secs: raw.confirmation_checker_sleep_secs,
            confirmation_checker_required_confirmations: raw.confirmation_checker_required_confirmations,
            max_input_count_per_tx: raw.max_input_count_per_tx.unwrap_or(400).min(400),
            fee_per_gram: raw.fee_per_gram,
            accounts,
        })
    }
}

fn parse_view_key(view_key_hex: &str) -> anyhow::Result<RistrettoSecretKey> {
    let view_key_bytes = hex::decode(view_key_hex)?;
    let view_key = RistrettoSecretKey::from_canonical_bytes(&view_key_bytes).map_err(|e| anyhow::anyhow!(e))?;
    Ok(view_key)
}

fn parse_public_spend_key(public_spend_key_hex: &str) -> anyhow::Result<CompressedKey<RistrettoPublicKey>> {
    let spend_key_bytes = hex::decode(public_spend_key_hex)?;
    let spend_key =
        CompressedKey::<RistrettoPublicKey>::from_canonical_bytes(&spend_key_bytes).map_err(|e| anyhow::anyhow!(e))?;
    Ok(spend_key)
}

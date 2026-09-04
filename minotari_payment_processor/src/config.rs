use anyhow::Context;
use config::{Config, Environment};
use serde::Deserialize;
use std::{collections::HashMap, fmt, str::FromStr};
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

/// A secret string that must never end up in a log line.
///
/// The newtype exists so that the plaintext cannot leak through the `Debug` impl of any struct
/// that holds it: [`PaymentProcessorEnv`] derives `Debug`, and a bare `String` field would be
/// printed verbatim by any `{:?}` rendering of it. There is deliberately no `Display` impl, so the
/// only way to reach the plaintext is the explicit [`Passphrase::reveal`] accessor.
#[derive(Clone)]
pub struct Passphrase(String);

impl Passphrase {
    /// Returns the plaintext secret.
    ///
    /// Call this only where the secret is genuinely required (handing it to the offline signer
    /// through its environment), never to log, display or persist it.
    pub fn reveal(&self) -> &str {
        &self.0
    }
}

impl From<String> for Passphrase {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl fmt::Debug for Passphrase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Passphrase(***REDACTED***)")
    }
}

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
    pub offline_signer_path: String,
    pub offline_signer_passphrase: Passphrase,
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
    offline_signer_path: String,
    offline_signer_passphrase: String,
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
        Self::load_from(Environment::default().separator("__"))
    }

    /// Builds the settings from the given environment source. Kept separate from [`Self::load`] so
    /// that tests can supply a deterministic set of variables instead of the process environment.
    fn load_from(source: Environment) -> anyhow::Result<Self> {
        let s = Config::builder().add_source(source).build()?;

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
            offline_signer_path: raw.offline_signer_path,
            offline_signer_passphrase: raw.offline_signer_passphrase.into(),
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

#[cfg(test)]
mod tests {
    use config::Map;

    use super::*;

    fn base_vars() -> Map<String, String> {
        let mut vars = Map::new();
        vars.insert("DATABASE_URL".to_string(), "sqlite://data/payments.db".to_string());
        vars.insert("PAYMENT_RECEIVER".to_string(), "http://localhost:9000".to_string());
        vars.insert("BASE_NODE".to_string(), "http://localhost:18142".to_string());
        vars.insert(
            "OFFLINE_SIGNER_PATH".to_string(),
            "/usr/local/bin/minotari_offline_signer".to_string(),
        );
        vars.insert("OFFLINE_SIGNER_PASSPHRASE".to_string(), "s3cr3t".to_string());
        vars
    }

    fn load(vars: Map<String, String>) -> anyhow::Result<PaymentProcessorEnv> {
        PaymentProcessorEnv::load_from(Environment::default().separator("__").source(Some(vars)))
    }

    #[test]
    fn it_reads_the_offline_signer_settings_from_the_environment() {
        let env = load(base_vars()).expect("config with all mandatory variables should load");

        assert_eq!(env.offline_signer_path, "/usr/local/bin/minotari_offline_signer");
        assert_eq!(env.offline_signer_passphrase.reveal(), "s3cr3t");
        assert_eq!(env.tari_network, Network::MainNet);
    }

    #[test]
    fn it_fails_when_the_offline_signer_path_is_missing() {
        let mut vars = base_vars();
        vars.remove("OFFLINE_SIGNER_PATH");

        let err = load(vars).expect_err("missing OFFLINE_SIGNER_PATH should be rejected");
        assert!(
            format!("{:#}", err).contains("offline_signer_path"),
            "unexpected error: {:#}",
            err
        );
    }

    #[test]
    fn it_fails_when_the_offline_signer_passphrase_is_missing() {
        let mut vars = base_vars();
        vars.remove("OFFLINE_SIGNER_PASSPHRASE");

        let err = load(vars).expect_err("missing OFFLINE_SIGNER_PASSPHRASE should be rejected");
        assert!(
            format!("{:#}", err).contains("offline_signer_passphrase"),
            "unexpected error: {:#}",
            err
        );
    }

    #[test]
    fn it_rejects_a_zero_fee_per_gram() {
        let mut vars = base_vars();
        vars.insert("FEE_PER_GRAM".to_string(), "0".to_string());

        let err = load(vars).expect_err("a zero fee_per_gram should be rejected");
        assert!(
            format!("{:#}", err).contains("fee_per_gram"),
            "unexpected error: {:#}",
            err
        );
    }

    #[test]
    fn it_redacts_the_passphrase_when_debug_formatted() {
        let passphrase = Passphrase::from("s3cr3t".to_string());

        let rendered = format!("{:?}", passphrase);
        assert!(
            !rendered.contains("s3cr3t"),
            "the plaintext passphrase leaked into the Debug output: {}",
            rendered
        );
        assert_eq!(rendered, "Passphrase(***REDACTED***)");
        assert_eq!(passphrase.reveal(), "s3cr3t");
    }

    #[test]
    fn it_redacts_the_passphrase_when_the_whole_env_is_debug_formatted() {
        let env = load(base_vars()).expect("config with all mandatory variables should load");

        let rendered = format!("{:?}", env);
        assert!(
            !rendered.contains("s3cr3t"),
            "the plaintext passphrase leaked into the Debug output of PaymentProcessorEnv: {}",
            rendered
        );
        assert!(
            rendered.contains("***REDACTED***"),
            "unexpected Debug output: {}",
            rendered
        );
    }
}

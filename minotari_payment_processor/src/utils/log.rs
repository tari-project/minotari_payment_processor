use std::sync::OnceLock;

fn reveal_pii() -> bool {
    static REVEAL_PII_CACHE: OnceLock<bool> = OnceLock::new();

    *REVEAL_PII_CACHE.get_or_init(|| {
        std::env::var("REVEAL_PII")
            .map(|v| {
                let val = v.to_lowercase();
                val == "true" || val == "1"
            })
            .unwrap_or(false)
    })
}

/// Masks a string (like an address) showing only start and end characters.
/// If REVEAL_PII is true, returns the original string.
pub fn mask_string(s: &str) -> String {
    if reveal_pii() {
        return s.to_string();
    }

    if s.len() <= 12 {
        return "***".to_string();
    }

    format!("{}...{}", &s[0..6], &s[s.len() - 6..])
}

/// Returns a redacted placeholder for amounts.
/// If REVEAL_PII is true, returns the actual amount.
pub fn mask_amount(amount: i64) -> String {
    if reveal_pii() {
        return amount.to_string();
    }

    "<REDACTED>".to_string()
}

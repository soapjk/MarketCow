//! Offline administrator policy generator. No network or production mutation.
//! The generated secret is written only to an owner-private file, never stdout.
use anyhow::{Context, Result, ensure};
use clap::Parser;
use marketcow_contracts::typed_v3::common::{SdkAccessPolicy, SdkCustomer};
use prost::Message;
use std::{io::Write, path::PathBuf};
#[derive(Parser)]
struct Args {
    #[arg(long)]
    output: PathBuf,
    /// Optional owner-private key export for the operator's SDK test process.
    #[arg(long)]
    key_output: Option<PathBuf>,
    #[arg(long)]
    customer_id: String,
    #[arg(long)]
    expires_at: String,
    #[arg(long)]
    revision: u64,
    #[arg(long)]
    prices: bool,
    #[arg(long)]
    markets: bool,
    #[arg(long)]
    history: bool,
    #[arg(long)]
    evidence: bool,
    #[arg(long)]
    public_rest: bool,
    #[arg(long)]
    maximum_http_requests: u32,
    #[arg(long)]
    maximum_streams: u32,
    #[arg(long)]
    maximum_response_bytes_per_window: u64,
    #[arg(long)]
    quota_window_seconds: u32,
    #[arg(long)]
    maximum_requests_per_window: u32,
}
fn main() -> Result<()> {
    let args = Args::parse();
    if let Some(path)=&args.key_output {
        ensure!(path!=&args.output && !path.exists(),"key export must be a new separate private file");
    }
    let expires = chrono::DateTime::parse_from_rfc3339(&args.expires_at)?.timestamp();
    ensure!(
        expires > chrono::Utc::now().timestamp(),
        "expiry must be in the future"
    );
    ensure!(
        !args.customer_id.is_empty() && args.customer_id.len() <= 128 && args.revision > 0,
        "policy identity"
    );
    ensure!(
        (1..=16).contains(&args.maximum_http_requests)
            && (1..=16).contains(&args.maximum_streams)
            && args.maximum_response_bytes_per_window > 0
            && (1..=2_678_400).contains(&args.quota_window_seconds)
            && args.maximum_requests_per_window > 0,
        "explicit quota budgets required"
    );
    let token = format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    );
    let policy = SdkAccessPolicy {
        schema_version: "marketcow.sdk-access-policy.v1".into(),
        revision: args.revision,
        customers: vec![SdkCustomer {
            customer_id: args.customer_id,
            bearer_token: token.clone(),
            expires_at_unix_seconds: expires,
            prices: args.prices,
            markets: args.markets,
            history: args.history,
            evidence: args.evidence,
            public_rest: args.public_rest,
            maximum_http_requests: args.maximum_http_requests,
            maximum_streams: args.maximum_streams,
            maximum_response_bytes_per_window: args.maximum_response_bytes_per_window,
            quota_window_seconds: args.quota_window_seconds,
            maximum_requests_per_window: args.maximum_requests_per_window,
        }],
    };
    let mut options = std::fs::OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(&args.output)
        .context("create new private policy (no overwrite)")?;
    file.write_all(&policy.encode_to_vec())?;
    file.sync_all()?;
    if let Some(path)=&args.key_output {
        let mut key=options.open(path).context("create new private key export")?;
        key.write_all(token.as_bytes())?;
        key.sync_all()?;
    }
    println!(
        "Private SDK policy and requested private key export created; no secret printed, no service changed."
    );
    Ok(())
}

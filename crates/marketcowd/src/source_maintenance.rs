//! Offline-only explicit storage migration; never starts transports or listeners.
use anyhow::Result;
use clap::Parser;
use marketcow_runtime::discovery_source::PreparedSourceWriter;
use std::path::PathBuf;

#[derive(Parser)]
struct Args {
    #[arg(long)]
    root: PathBuf,
    #[arg(long)]
    maximum_batch_bytes: usize,
    #[arg(long)]
    bounded_history_bytes: usize,
    /// Only retire the verified committed-prefix copy in a prepared candidate.
    #[arg(long)]
    retire_candidate_legacy_log: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let mut writer = PreparedSourceWriter::open(&args.root, args.maximum_batch_bytes)?;
    let before = writer.cursor()?;
    writer.enable_bounded_history(args.bounded_history_bytes)?;
    anyhow::ensure!(writer.cursor()? == before, "migration changed cursor");
    drop(writer);
    let reopened = PreparedSourceWriter::open(&args.root, args.maximum_batch_bytes)?;
    anyhow::ensure!(reopened.cursor()? == before, "restart changed cursor");
    if args.retire_candidate_legacy_log {
        let report: serde_json::Value = serde_json::from_slice(&std::fs::read(
            args.root.join("bounded-preparation-report.json"),
        )?)?;
        anyhow::ensure!(
            report["complete"] == true
                && report["target"].as_str() == args.root.to_str()
                && report["source_unchanged"].as_str() != args.root.to_str(),
            "candidate provenance differs"
        );
        let original = PathBuf::from(
            report["source_unchanged"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("missing original"))?,
        )
        .join("events.jsonl");
        let candidate = args.root.join("events.jsonl");
        let expected = report["committed_log_bytes"]
            .as_u64()
            .ok_or_else(|| anyhow::anyhow!("missing size"))?;
        anyhow::ensure!(
            std::fs::metadata(&candidate)?.len() == expected
                && std::fs::metadata(&original)?.len() >= expected,
            "original recovery copy unavailable"
        );
        // Stream compare before deleting only the redundant candidate prefix.
        use std::io::Read;
        let mut a = std::fs::File::open(&candidate)?;
        let mut b = std::fs::File::open(&original)?;
        let mut left = expected;
        let mut x = vec![0; 1048576];
        let mut y = vec![0; 1048576];
        while left > 0 {
            let n = left.min(x.len() as u64) as usize;
            a.read_exact(&mut x[..n])?;
            b.read_exact(&mut y[..n])?;
            anyhow::ensure!(x[..n] == y[..n], "original committed prefix differs");
            left -= n as u64;
        }
        drop(a);
        drop(b);
        std::fs::remove_file(candidate)?;
        std::fs::File::open(&args.root)?.sync_all()?;
        println!(
            "candidate_legacy_log_retired bytes={expected} original_preserved={}",
            original.display()
        );
    }
    drop(reopened);
    anyhow::ensure!(
        PreparedSourceWriter::open(&args.root, args.maximum_batch_bytes)?.cursor()? == before,
        "post-retirement restart differs"
    );
    println!("bounded_history_migration_complete cursor={before}");
    Ok(())
}

use std::{env, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Public types come from marketcow-wire, never a second generated copy.
    let proto = PathBuf::from("../../proto");
    let files: Vec<_> = [3, 4, 5, 6].into_iter()
        .map(|version| proto.join(format!("marketcow/polymarket/btc5m_market_local_v{version}.proto")))
        .collect();
    for path in &files {
        println!("cargo:rerun-if-changed={}", path.display());
    }
    unsafe { env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?) };
    prost_build::Config::new().compile_protos(&files, &[proto])?;
    Ok(())
}

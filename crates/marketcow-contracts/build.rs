use std::{env, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = PathBuf::from("../../proto/marketcow/polymarket/btc5m_market_local_v2.proto");
    println!("cargo:rerun-if-changed={}", proto.display());
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    // Edition 2024 makes setting environment process-global unsafe. Build
    // scripts execute before the compiler and this is their only thread.
    unsafe { env::set_var("PROTOC", protoc) };
    prost_build::Config::new().compile_protos(&[proto], &["../../proto"])?;
    Ok(())
}

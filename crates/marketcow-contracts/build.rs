use std::{env, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_v3 = PathBuf::from("../../proto/marketcow/polymarket/btc5m_market_local_v3.proto");
    let proto_v4 = PathBuf::from("../../proto/marketcow/polymarket/btc5m_market_local_v4.proto");
    let proto_v5 = PathBuf::from("../../proto/marketcow/polymarket/btc5m_market_local_v5.proto");
    println!("cargo:rerun-if-changed={}", proto_v5.display());
    println!("cargo:rerun-if-changed={}", proto_v3.display());
    println!("cargo:rerun-if-changed={}", proto_v4.display());
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    // Edition 2024 makes setting environment process-global unsafe. Build
    // scripts execute before the compiler and this is their only thread.
    unsafe { env::set_var("PROTOC", protoc) };
    prost_build::Config::new().compile_protos(&[proto_v3, proto_v4, proto_v5], &["../../proto"])?;
    Ok(())
}

use std::{env, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?);
    let proto = root.join("proto");
    let names = [
        "common/v1/types.proto", "common/v1/access.proto",
        "btc/v3/price.proto", "btc/v3/history.proto", "btc/v3/service.proto",
        "btc/v3/bars.proto", "polymarket/btc5m_market_local_v7.proto",
        "facts/v1/common.proto", "facts/v1/reference.proto",
        "facts/v1/execution.proto", "facts/v1/finality.proto", "facts/v1/rest.proto",
    ];
    let files: Vec<_> = names.iter().map(|n| proto.join("marketcow").join(n)).collect();
    for path in &files {
        println!("cargo:rerun-if-changed={}", path.display());
    }
    // Build script is single-threaded; ordinary compilation never edits sources.
    unsafe { env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?) };
    prost_build::Config::new()
        .file_descriptor_set_path(PathBuf::from(env::var("OUT_DIR")?).join("schema.bin"))
        .compile_protos(&files, &[proto])?;
    Ok(())
}

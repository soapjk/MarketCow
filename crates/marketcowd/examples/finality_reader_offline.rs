//! Compile/run offline contract tests without any RPC endpoint or service route.
#[allow(dead_code)]
#[path="../src/source_finality_reader.rs"] mod reader;
fn main() { eprintln!("No configured transport: run cargo test --example finality_reader_offline"); }

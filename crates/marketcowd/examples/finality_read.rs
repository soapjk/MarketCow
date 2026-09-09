//! Explicit local config only; no endpoint defaults, no service routes or transactions.
#[allow(dead_code)]
#[path="../src/source_finality_reader.rs"] mod reader;
use anyhow::{ensure, Context, Result};
use serde_json::Value;
use std::{io::Read, path::PathBuf, time::{Duration, Instant}};
fn main()->Result<()> {
    let args:Vec<_>=std::env::args().collect();
    ensure!(args.len()==3,"explicit config path and new evidence path required");
    let config_path=PathBuf::from(&args[1]);let output=PathBuf::from(&args[2]);
    ensure!(config_path.is_absolute() && output.is_absolute(),"absolute paths");
    let mut raw=vec![];std::fs::File::open(config_path)?.take(16385).read_to_end(&mut raw)?;
    ensure!(raw.len()<=16384,"config cap");
    let v:Value=serde_json::from_slice(&raw)?;
    let s=|key:&str|->Result<String>{Ok(v[key].as_str().with_context(||format!("missing {key}"))?.to_owned())};
    let profile=reader::Profile {chain_id:s("chain_id")?,contract:s("contract")?,code_sha256:s("code_sha256")?,finality_policy:s("finality_policy")?,maximum_calls:7,maximum_bytes:131072};
    let endpoint=url::Url::parse(&s("rpc_endpoint")?)?;
    ensure!(endpoint.scheme()=="https" && endpoint.username().is_empty() && endpoint.password().is_none(),"HTTPS endpoint without userinfo required");
    let condition=s("condition_id")?;
    let mut file=std::fs::OpenOptions::new().create_new(true).write(true).open(output)?;
    let rt=tokio::runtime::Runtime::new()?;
    let _guard=rt.enter();
    let client=reqwest::Client::builder().redirect(reqwest::redirect::Policy::none()).retry(reqwest::retry::never()).build()?;
    let deadline=Instant::now()+Duration::from_secs(60);let mut total=0usize;
    let observation=reader::read(&profile,&condition,|request|rt.block_on(async {
        let remaining=deadline.checked_duration_since(Instant::now()).filter(|d|!d.is_zero()).context("total deadline")?;
        ensure!(total<131072,"total response cap");
        // Never return endpoint-bearing reqwest errors (endpoint may contain a secret).
        let mut response=client.post(endpoint.clone()).timeout(remaining.min(Duration::from_secs(10)))
            .json(request).send().await.map_err(|_|anyhow::anyhow!("RPC transport failed"))?;
        ensure!(response.status().is_success(),"RPC HTTP failure");
        let mut bytes=vec![];
        while let Some(chunk)=response.chunk().await.map_err(|_|anyhow::anyhow!("RPC body failed"))? {
            ensure!(bytes.len()+chunk.len()<=32768 && total+chunk.len()<=131072,"RPC bytes exceeded");
            total+=chunk.len();bytes.extend(chunk);
        }
        Ok(bytes)
    }));
    let result=match observation {Ok(v)=>v,Err(e)=>serde_json::json!({"status":"read_failed","error":e.to_string(),"settlement_import_allowed":false})};
    use std::io::Write;
    file.write_all(&serde_json::to_vec_pretty(&result)?)?;file.sync_all()?;
    ensure!(result["status"]!="read_failed","read failed; evidence file contains error");
    Ok(())
}

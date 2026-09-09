//! Two explicitly bound rule captures or synthetic fixtures; no service writes.
#[allow(dead_code)] #[path="../src/source_market_evidence.rs"] mod evidence;
use anyhow::{ensure, Result};
use serde_json::json;
use sha2::{Digest,Sha256};
use std::{path::PathBuf,time::Duration};
#[tokio::main] async fn main()->Result<()> {
    let args:Vec<String>=std::env::args().collect();ensure!(args.len()==3,"mode fixtures|capture and new absolute root required");
    let root=PathBuf::from(&args[2]);ensure!(root.is_absolute(),"absolute root");std::fs::create_dir(&root)?;
    if args[1]=="fixtures" {
        let condition=format!("0x{}", "a".repeat(64));
        let baseline=json!({"id":"1","conditionId":condition,"clobTokenIds":"[\"10\",\"20\"]","outcomes":"[\"Yes\",\"No\"]","question":"Synthetic threshold question","description":"Synthetic full text, no live rule claim.","resolutionSource":"https://example.invalid/rule","closed":false,"acceptingOrders":true});
        for name in ["complete-text","missing-description","terminal-reported","rule-change"] {
            let mut raw=baseline.clone();
            if name=="missing-description" {raw.as_object_mut().unwrap().remove("description");}
            if name=="terminal-reported" {raw["umaResolutionStatus"]=json!("resolved");raw["closed"]=json!(true);raw["acceptingOrders"]=json!(false);raw["outcomePrices"]=json!("[\"1\",\"0\"]");}
            if name=="rule-change" {raw["description"]=json!("Synthetic changed source outage extension clause.");}
            let v=evidence::project("1",&condition,&serde_json::to_vec(&raw)?,"2026-09-09T00:00:00Z")?;
            let bytes=serde_json::to_vec_pretty(&json!({"fixture_kind":"synthetic","response":v}))?;
            std::fs::write(root.join(format!("{name}.json")),&bytes)?;
            println!("{name} {}",hex::encode(Sha256::digest(bytes)));
        }
        return Ok(())
    }
    ensure!(args[1]=="capture","mode");
    let client=reqwest::Client::builder().timeout(Duration::from_secs(15)).redirect(reqwest::redirect::Policy::none()).retry(reqwest::retry::never()).build()?;
    for (id,condition) in [("1831352","0x6a98ff5d9296b7130ba3c6d5978e0777b98f0550341706bef86f7eb390def16b"),("1831353","0x0ab703c5bc04b87984cc9355d28a2de699d396b71a86a29991fa42bf9c96e798")] {
        let mut response=client.get(format!("https://gamma-api.polymarket.com/markets/{id}")).send().await?;
        let status=response.status().as_u16();let mut bytes=Vec::new();
        while let Some(chunk)=response.chunk().await? {ensure!(bytes.len()+chunk.len()<=262144,"cap");bytes.extend_from_slice(&chunk);}
        std::fs::write(root.join(format!("{id}.raw")),&bytes)?;
        ensure!(status==200,"upstream {status}; raw preserved, stopping");
        let v=evidence::project(id,condition,&bytes,&chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis,true))?;
        let output=serde_json::to_vec_pretty(&v)?;std::fs::write(root.join(format!("{id}.json")),&output)?;
        println!("{id} raw_sha={} response_sha={}",hex::encode(Sha256::digest(bytes)),hex::encode(Sha256::digest(output)));
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    Ok(())
}

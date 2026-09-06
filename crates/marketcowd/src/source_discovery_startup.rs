//! Hash-bound explicit Discovery preparation. Startup never traverses Gamma.
use anyhow::{Context,Result,ensure};
use serde::Deserialize;
use serde_json::{Value,json};
use sha2::{Digest,Sha256};
use std::{collections::{BTreeMap,BTreeSet},path::Path};
use crate::{source_discovery_projection::DiscoveryConfig,source_discovery_quote::QuotePolicy};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Seed {
    schema_version:String,
    catalog_revision:String,
    catalog_manifest_sha256:String,
    universe_revision:String,
    markets:Vec<Value>,
    relations:Vec<Value>,
    settlements:BTreeMap<String,Value>,
    catalog_source:Value,
    depth_quantities:Vec<String>,
    maximum_book_age_ms:i64,
}
pub fn load(root:&Path,path:&Path,expected_hash:&str,byte_cap:usize,expected_markets:&[super::Market])->Result<(DiscoveryConfig,Value)> {
    let root=root.canonicalize()?;let path=path.canonicalize()?;
    ensure!(path.starts_with(&root),"Discovery preparation escapes root");
    ensure!(std::fs::metadata(&path)?.len()<=byte_cap as u64,"Discovery preparation byte cap");
    let bytes=std::fs::read(path)?;
    ensure!(hex::encode(Sha256::digest(&bytes))==expected_hash,"Discovery preparation checksum differs");
    let seed:Seed=serde_json::from_slice(&bytes)?;
    ensure!(seed.schema_version=="marketcow.polymarket.discovery-public-seed.v1","Discovery preparation schema");
    let manifest_bytes=std::fs::read(root.join("catalog.json"))?;
    ensure!(hex::encode(Sha256::digest(&manifest_bytes))==seed.catalog_manifest_sha256,"catalog manifest differs");
    let manifest:Value=serde_json::from_slice(&manifest_bytes)?;
    ensure!(manifest["catalog_revision"]==seed.catalog_revision,"catalog revision differs");
    let mut universe=manifest["realtime_universe"].clone();
    let revision=universe.as_object_mut().context("explicit universe missing")?.remove("universe_id").context("universe id missing")?;
    ensure!(revision==seed.universe_revision&&marketcow_polymarket::discovery_source::canonical_hash(&universe)==seed.universe_revision,"universe content hash differs");
    let declared:BTreeSet<_>=universe["market_ids"].as_array().context("universe market ids")?.iter().map(|id|id.as_str().context("universe id")).collect::<Result<_>>()?;
    ensure!(declared.len()==seed.markets.len()&&declared.len()==expected_markets.len(),"Discovery market count differs");
    let mut found=BTreeSet::new();
    for market in &seed.markets {
        let id=market["identity"]["market_id"].as_str().context("metadata market id")?;
        ensure!(found.insert(id)&&declared.contains(id),"metadata identity outside universe");
        let expected=expected_markets.iter().find(|m|m.market_id==id).context("metadata outside collector plan")?;
        ensure!(market["identity"]["condition_id"]==expected.condition_id,"condition mismatch");
        let tokens:BTreeSet<_>=market["identity"]["outcomes"].as_array().context("metadata outcomes")?.iter()
            .map(|o|o["token_id"].as_str().context("metadata token")).collect::<Result<_>>()?;
        ensure!(tokens==expected.token_ids.iter().map(String::as_str).collect(),"metadata token mismatch");
    }
    ensure!(found==declared,"prepared metadata incomplete");
    let projection_id=marketcow_polymarket::discovery_source::canonical_hash(&json!({
        "implementation":"rust-discovery-v3.1","seed_sha256":expected_hash,"instance":uuid::Uuid::new_v4().simple().to_string()}));
    let config=DiscoveryConfig{projection_id,catalog_revision:seed.catalog_revision.clone(),universe_revision:seed.universe_revision,
        market_ids:found.into_iter().map(str::to_owned).collect(),relations:seed.relations,settlements:seed.settlements,
        policy:QuotePolicy{quantities:seed.depth_quantities,maximum_book_age_ms:seed.maximum_book_age_ms}};
    let base=crate::durable_bootstrap::bootstrap_discovery(root,seed.catalog_revision,seed.catalog_manifest_sha256,
        seed.markets,seed.catalog_source,byte_cap)?;
    Ok((config,base))
}

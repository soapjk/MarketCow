//! Read-only CTF observation core. No default endpoint, no verified-final promotion.
//! Transport supplies raw JSON-RPC bytes under its own explicit wall/byte limits.
use anyhow::{ensure, Context, Result};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

pub struct Profile {
    pub chain_id: String,
    pub contract: String,
    pub code_sha256: String,
    pub finality_policy: String,
    pub maximum_calls: usize,
    pub maximum_bytes: usize,
}
pub struct TokenBinding {
    pub collateral: String,
    // ABI uint256 token IDs, in payout slot order; no u128 truncation.
    pub token_ids_hex: [String; 2],
}
fn hex_field(s: &str, n: usize) -> bool {
    s.len() == 2 + n && s.starts_with("0x") && s[2..].bytes().all(|b| b.is_ascii_hexdigit())
}
fn word(v: &Value) -> Result<u128> {
    let s = v.as_str().context("ABI result string")?;
    ensure!(hex_field(s,64), "ABI uint256 width");
    // Deliberately fail closed outside supported arithmetic range, never truncate.
    ensure!(s[2..34].bytes().all(|b| b == b'0'), "uint256 exceeds supported u128 range");
    Ok(u128::from_str_radix(&s[34..],16)?)
}
pub fn read<F>(profile: &Profile, condition: &str, transport: F) -> Result<Value>
where F: FnMut(&Value) -> Result<Vec<u8>> {
    read_inner(profile, condition, None, transport)
}
pub fn read_bound<F>(profile: &Profile, condition: &str, binding: &TokenBinding, transport: F) -> Result<Value>
where F: FnMut(&Value) -> Result<Vec<u8>> {
    ensure!(hex_field(&binding.collateral,40), "collateral identity");
    ensure!(binding.token_ids_hex.iter().all(|s|hex_field(s,64)), "token ABI width");
    ensure!(!binding.token_ids_hex[0].eq_ignore_ascii_case(&binding.token_ids_hex[1]), "duplicate token");
    read_inner(profile, condition, Some(binding), transport)
}
fn read_inner<F>(profile: &Profile, condition: &str, binding: Option<&TokenBinding>, mut transport: F) -> Result<Value>
where F: FnMut(&Value) -> Result<Vec<u8>> {
    ensure!(hex_field(condition,64) && hex_field(&profile.contract,40), "identity format");
    ensure!(profile.finality_policy == "rpc_finalized_hash_pinned_v1", "unsupported finality policy");
    ensure!(!profile.chain_id.is_empty() && profile.code_sha256.len()==64, "profile missing");
    let mut evidence = vec![];
    let mut bytes = 0usize;
    let mut call = |method: &str, params: Value| -> Result<Value> {
        ensure!(evidence.len() < profile.maximum_calls && bytes < profile.maximum_bytes, "RPC budget");
        let id = evidence.len()+1;
        let request = json!({"jsonrpc":"2.0","id":id,"method":method,"params":params});
        let raw = transport(&request)?;
        bytes = bytes.checked_add(raw.len()).context("byte overflow")?;
        ensure!(bytes <= profile.maximum_bytes, "RPC response bytes exceeded");
        let response: Value = serde_json::from_slice(&raw)?;
        ensure!(response["jsonrpc"]=="2.0" && response["id"]==id && response.get("error").is_none(), "RPC error/identity");
        let result = response.get("result").context("RPC result missing")?.clone();
        evidence.push(json!({"request":request,"raw_sha256":hex::encode(Sha256::digest(&raw)),"raw_bytes":raw.len(),"raw_utf8":std::str::from_utf8(&raw)?}));
        Ok(result)
    };
    ensure!(call("eth_chainId",json!([]))? == profile.chain_id, "chain mismatch");
    let block = call("eth_getBlockByNumber",json!(["finalized",false]))?;
    let hash = block["hash"].as_str().context("finalized block unavailable")?;
    ensure!(hex_field(hash,64) && block["number"].is_string() && block["timestamp"].is_string(), "block identity");
    let pinned = json!({"blockHash":hash,"requireCanonical":true});
    let code = call("eth_getCode",json!([profile.contract,pinned]))?;
    let code = code.as_str().context("code hex")?.strip_prefix("0x").context("code prefix")?;
    let code = hex::decode(code)?;
    ensure!(!code.is_empty() && hex::encode(Sha256::digest(&code))==profile.code_sha256,"deployment code mismatch");
    let mut abi = |selector: &str, suffix: &str| -> Result<Value> {
        call("eth_call",json!([{"to":profile.contract,"data":format!("0x{selector}{}{suffix}",&condition[2..])},pinned]))
    };
    let slots = word(&abi("d42dc0c2", "")?)?;
    ensure!(slots==2,"only binary conditions supported");
    let denominator = word(&abi("dd34de67", "")?)?;
    let mut numerators = vec![];
    let mut sum = 0u128;
    for i in 0..slots {
        let n = word(&abi("0504c814", &format!("{i:064x}"))?)?;
        sum = sum.checked_add(n).context("payout overflow")?;
        numerators.push(n.to_string());
    }
    ensure!(sum == denominator, "payout sum mismatch");
    let mut bound_tokens = vec![];
    if let Some(binding) = binding {
        // Standard CTF positions with no parent collection. Negative-risk adapter
        // conversions are not inferred or certified by this reader.
        for i in 0..2usize {
            let collection = call("eth_call", json!([{"to":profile.contract,
                "data":format!("0x856296f7{}{}{:064x}","0".repeat(64),&condition[2..],1u64<<i)},pinned]))?;
            let collection = collection.as_str().context("collection result")?;
            ensure!(hex_field(collection,64),"collection ABI width");
            let position = call("eth_call", json!([{"to":profile.contract,
                "data":format!("0x39dd7530{}{}{}","0".repeat(24),&binding.collateral[2..],&collection[2..])},pinned]))?;
            let position = position.as_str().context("position result")?;
            ensure!(hex_field(position,64) && position.eq_ignore_ascii_case(&binding.token_ids_hex[i]), "outcome token mismatch");
            bound_tokens.push(json!({"slot":i,"index_set":1u64<<i,"token_id_hex":position,
                "collection_id":collection,"payout_numerator":numerators[i]}));
        }
    }
    // A provider's finalized tag plus code hash is not independent finality/token proof.
    Ok(json!({"schema_version":"marketcow.polymarket.ctf-observation.v1",
        "condition_id":condition,"chain_id":profile.chain_id,"ctf_address":profile.contract,
        "block":block,"finality_policy":profile.finality_policy,
        "status":if denominator==0 {"unresolved"} else {"resolved_unverified"},
        "payout_numerators":numerators,"payout_denominator":denominator.to_string(),
        "observed_at":chrono::Utc::now().to_rfc3339(),"evidence":evidence,
        "standard_ctf_token_binding_verified":binding.is_some(),"bound_tokens":bound_tokens,
        "collateral":binding.map(|b|b.collateral.as_str()),
        "missing_facts":if binding.is_some(){vec!["independent_finality_verification","adapter_redemption_semantics"]}
            else{vec!["outcome_token_collateral_adapter_binding","independent_finality_verification"]},
        "settlement_import_allowed":false}))
}

#[cfg(test)] mod tests {
    use super::*;
    fn profile() -> Profile { Profile{chain_id:"0x89".into(),contract:format!("0x{}","1".repeat(40)),code_sha256:hex::encode(Sha256::digest([1u8])),finality_policy:"rpc_finalized_hash_pinned_v1".into(),maximum_calls:7,maximum_bytes:8192} }
    fn result(i:usize)->Value { match i {1=>json!("0x89"),2=>json!({"hash":format!("0x{}","2".repeat(64)),"number":"0x10","timestamp":"0x20"}),3=>json!("0x01"),4=>json!(format!("0x{:064x}",2)),5|7=>json!(format!("0x{:064x}",1)),_=>json!(format!("0x{:064x}",0))} }
    #[test] fn bound_tokens_are_checked_at_same_block_without_finality_promotion() {
        let mut p=profile();p.maximum_calls=11;
        let binding=TokenBinding {collateral:format!("0x{}","3".repeat(40)),
            token_ids_hex:[format!("0x{}","e".repeat(64)),format!("0x{}","f".repeat(64))]};
        for wrong in [false,true] {
            let result=read_bound(&p,&format!("0x{}","a".repeat(64)),&binding,|q| {
                let i=q["id"].as_u64().unwrap() as usize;
                if i>=8 {assert_eq!(q["params"][1]["blockHash"],format!("0x{}","2".repeat(64)));}
                let r=match i {8|10=>json!(format!("0x{}","b".repeat(64))),
                    9=>json!(binding.token_ids_hex[0]),
                    11=>json!(if wrong {binding.token_ids_hex[0].clone()}else{binding.token_ids_hex[1].clone()}),
                    _=>result(i)};
                Ok(serde_json::to_vec(&json!({"jsonrpc":"2.0","id":i,"result":r}))?)
            });
            if wrong {assert!(result.is_err());}else{
                let v=result.unwrap();assert_eq!(v["standard_ctf_token_binding_verified"],true);
                assert_eq!(v["evidence"].as_array().unwrap().len(),11);
                assert_eq!(v["settlement_import_allowed"],false);
            }
        }
    }
    fn run(p:&Profile, bad:usize)->Result<Value> { read(p,&format!("0x{}","a".repeat(64)),|q| {let i=q["id"].as_u64().unwrap() as usize;if i>=3 {assert_eq!(q["params"][1]["requireCanonical"],true);} Ok(serde_json::to_vec(&json!({"jsonrpc":"2.0","id":i,"result":if i==bad {Value::Null} else {result(i)}}))?)}) }
    #[test] fn pinned_reader_does_not_authorize_settlement(){let v=run(&profile(),0).unwrap();assert_eq!(v["status"],"resolved_unverified");assert_eq!(v["settlement_import_allowed"],false);assert_eq!(v["evidence"].as_array().unwrap().len(),7);}
    #[test] fn failures_stop(){for i in 1..=7 {assert!(run(&profile(),i).is_err());}}
    #[test] fn missing_configuration_zero_calls(){let mut p=profile();p.finality_policy.clear();assert!(read(&p,"bad",|_|panic!("no call")).is_err());}
    #[test] fn budgets(){let mut p=profile();p.maximum_calls=0;assert!(read(&p,&format!("0x{}","a".repeat(64)),|_|panic!("no call")).is_err());p.maximum_calls=7;p.maximum_bytes=1;assert!(run(&p,0).is_err());}
    #[test] fn deployment_mismatch(){let mut p=profile();p.code_sha256="0".repeat(64);assert!(run(&p,0).is_err());}
    #[test] fn no_uint_truncation(){assert!(word(&json!(format!("0x{}","f".repeat(64)))).is_err());assert!(word(&json!("0x1")).is_err());}
    #[test] fn unresolved_and_fractional(){for (den,a,b,status) in [(0,0,0,"unresolved"),(2,1,1,"resolved_unverified")] {let v=read(&profile(),&format!("0x{}","a".repeat(64)),|q|{let i=q["id"].as_u64().unwrap() as usize;let r=match i {5=>json!(format!("0x{den:064x}")),6=>json!(format!("0x{a:064x}")),7=>json!(format!("0x{b:064x}")),_=>result(i)};Ok(serde_json::to_vec(&json!({"jsonrpc":"2.0","id":i,"result":r}))?)}).unwrap();assert_eq!(v["status"],status);}}
    #[test] fn wrong_sum_and_rpc_error() {
        for error in [false,true] {
            assert!(read(&profile(),&format!("0x{}","a".repeat(64)),|q| {
                let i=q["id"].as_u64().unwrap() as usize;
                let response=if error {json!({"jsonrpc":"2.0","id":i,"error":{"code":-1}})}
                else {json!({"jsonrpc":"2.0","id":i,"result":if i==6 {json!(format!("0x{:064x}",2))}else{result(i)}})};
                Ok(serde_json::to_vec(&response)?)
            }).is_err());
        }
    }
}

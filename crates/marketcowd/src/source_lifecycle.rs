//! Bounded Gamma lifecycle and resolution observations. Redeemability remains
//! outside this source: a resolved payout vector is never called redeemable.
use super::Market;
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::str::FromStr;
use std::{fs, io::Write, path::Path};

fn json_strings(raw: &Value, field: &str) -> Result<Vec<String>> {
    serde_json::from_str(
        raw[field]
            .as_str()
            .context(format!("missing Gamma {field}"))?,
    )
    .with_context(|| format!("invalid Gamma {field}"))
}

/// Gamma exposes the final binary payout vector only when UMA is resolved.
/// Accept exactly one 1 and one 0, bound positionally to the advertised token
/// and outcome arrays. Anything else remains lifecycle-only/fail-closed.
fn resolution(raw: &Value) -> Result<Option<Value>> {
    if raw["umaResolutionStatus"] != "resolved" {
        return Ok(None);
    }
    let outcomes = json_strings(raw, "outcomes")?;
    let tokens = json_strings(raw, "clobTokenIds")?;
    let prices = json_strings(raw, "outcomePrices")?;
    ensure!(
        outcomes.len() == 2 && tokens.len() == 2 && prices.len() == 2,
        "Gamma resolution must be binary"
    );
    ensure!(
        outcomes[0] != outcomes[1] && tokens[0] != tokens[1],
        "Gamma resolution identity is not unique"
    );
    let payouts = prices
        .iter()
        .map(|value| rust_decimal::Decimal::from_str(value).context("invalid Gamma payout"))
        .collect::<Result<Vec<_>>>()?;
    let winners: Vec<_> = payouts
        .iter()
        .enumerate()
        .filter(|(_, payout)| **payout == rust_decimal::Decimal::ONE)
        .map(|(index, _)| index)
        .collect();
    ensure!(
        winners.len() == 1
            && payouts.iter().enumerate().all(
                |(index, payout)| index == winners[0] || *payout == rust_decimal::Decimal::ZERO
            ),
        "Gamma resolution payout vector must contain one winner"
    );
    let winner = winners[0];
    Ok(Some(json!({
        "schema_version":"marketcow.polymarket.resolution-observation.v1",
        "source":"polymarket_gamma",
        "status":"resolved",
        "winning_outcome":outcomes[winner],
        "winning_token_id":tokens[winner],
        "payouts":[
            {"outcome":outcomes[0],"token_id":tokens[0],"payout":payouts[0].to_string()},
            {"outcome":outcomes[1],"token_id":tokens[1],"payout":payouts[1].to_string()}
        ]
    })))
}

fn validate(market: &Market, raw: &Value) -> Result<bool> {
    ensure!(
        raw["id"] == market.market_id && raw["conditionId"] == market.condition_id,
        "Gamma lifecycle identity differs for {}",
        market.market_id
    );
    let tokens: Vec<String> = serde_json::from_str(
        raw["clobTokenIds"]
            .as_str()
            .context("missing Gamma token IDs")?,
    )?;
    ensure!(
        tokens.len() == 2
            && tokens[0] != tokens[1]
            && tokens.iter().all(|t| market.token_ids.contains(t)),
        "Gamma lifecycle token binding differs"
    );
    let closed = raw["closed"]
        .as_bool()
        .context("missing Gamma closed flag")?;
    let accepting = raw["acceptingOrders"]
        .as_bool()
        .context("missing Gamma acceptingOrders flag")?;
    if !closed || accepting || raw["umaResolutionStatus"] != "resolved" {
        return Ok(false);
    }
    let timestamp = raw["closedTime"]
        .as_str()
        .context("missing Gamma closedTime")?;
    let timestamp = timestamp.replace(' ', "T");
    let timestamp = if timestamp.ends_with("+00") {
        format!("{timestamp}:00")
    } else {
        timestamp
    };
    let terminal_at = chrono::DateTime::parse_from_rfc3339(&timestamp)?;
    ensure!(
        terminal_at <= chrono::Utc::now(),
        "future Gamma terminal time"
    );
    // Prices alone never establish terminal status. The resolution helper also
    // requires this resolved status before accepting a payout vector.
    Ok(true)
}

pub(super) async fn refresh(
    client: reqwest::Client,
    market: Market,
    limit: usize,
) -> Result<Option<Value>> {
    ensure!(
        !market.market_id.is_empty() && market.market_id.bytes().all(|b| b.is_ascii_digit()),
        "invalid Gamma market ID"
    );
    let url = format!(
        "https://gamma-api.polymarket.com/markets/{}",
        market.market_id
    );
    let mut response = client.get(&url).send().await?.error_for_status()?;
    ensure!(
        response.content_length().is_none_or(|n| n <= limit as u64),
        "Gamma byte cap"
    );
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        ensure!(
            body.len()
                .checked_add(chunk.len())
                .is_some_and(|n| n <= limit),
            "Gamma byte cap"
        );
        body.extend_from_slice(&chunk);
    }
    let raw: Value = serde_json::from_slice(&body)?;
    if !validate(&market, &raw)? {
        return Ok(None);
    }
    let settlement = resolution(&raw)?;
    Ok(Some(json!({
        "schema_version":"marketcow.polymarket.lifecycle-observation.v1",
        "market_id":market.market_id,"source":"polymarket_gamma","source_url":url,
        "observed_at":chrono::Utc::now().to_rfc3339(),"lifecycle_state":"resolved",
        "raw_response_sha256":hex::encode(Sha256::digest(&body)),
        "raw_response":String::from_utf8(body)?,
        "policy":"suspend_book_requests_reject_dependent_computation",
        "settlement":settlement
    })))
}

pub(super) fn persist(root: &Path, catalog: &str, evidence: &Value) -> Result<()> {
    let directory = root.join("lifecycle-observations");
    fs::create_dir_all(&directory)?;
    ensure!(
        !fs::symlink_metadata(&directory)?.file_type().is_symlink()
            && fs::canonicalize(&directory)?.starts_with(fs::canonicalize(root)?),
        "lifecycle evidence escapes root"
    );
    let bytes = serde_json::to_vec(&json!({"catalog_revision":catalog,"evidence":evidence}))?;
    let hash = hex::encode(Sha256::digest(&bytes));
    let target = directory.join(format!("{hash}.json"));
    let temporary = directory.join(format!(".{}.tmp", uuid::Uuid::new_v4()));
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(&temporary, &target)?;
    fs::File::open(&directory)?.sync_all()?;
    Ok(())
}

pub(super) fn terminal_event(market: &Value, evidence: &Value, cursor: u64) -> Result<Value> {
    use chrono::Timelike;
    use marketcow_polymarket::discovery_source::canonical_hash;
    let raw: Value = serde_json::from_str(
        evidence["raw_response"]
            .as_str()
            .context("missing raw evidence")?,
    )?;
    ensure!(
        raw["id"] == market["identity"]["market_id"]
            && raw["conditionId"] == market["identity"]["condition_id"]
            && raw["closed"] == true
            && raw["acceptingOrders"] == false
            && raw["umaResolutionStatus"] == "resolved",
        "terminal evidence binding differs"
    );
    let mut terminal = market.clone();
    let settlement = resolution(&raw)?;
    ensure!(
        settlement.clone().unwrap_or(Value::Null) == evidence["settlement"],
        "resolution evidence differs from raw response"
    );
    if let Some(settlement) = settlement.as_ref() {
        let raw_tokens = json_strings(&raw, "clobTokenIds")?;
        let raw_outcomes = json_strings(&raw, "outcomes")?;
        let catalog_outcomes = market["identity"]["outcomes"]
            .as_array()
            .context("missing catalog outcomes")?;
        ensure!(
            catalog_outcomes.len() == 2
                && raw_tokens
                    .iter()
                    .zip(raw_outcomes.iter())
                    .all(|(token, outcome)| {
                        catalog_outcomes.iter().any(|item| {
                            item["token_id"] == token.as_str()
                                && item["outcome"] == outcome.as_str()
                        })
                    }),
            "resolution outcome identity differs from catalog"
        );
        terminal["lifecycle_state"] = json!("resolved");
        terminal["resolution"] = settlement["winning_outcome"].clone();
    }
    terminal["active"] = json!(false);
    terminal["closed"] = json!(true);
    terminal["accepting_orders"] = json!(false);
    if settlement.is_none() {
        terminal["lifecycle_state"] = json!("closed");
        terminal["resolution"] = Value::Null;
    }
    let at = raw["closedTime"]
        .as_str()
        .context("missing terminal time")?
        .replace(' ', "T");
    terminal["terminal_at"] = json!(if at.ends_with("+00") {
        format!("{at}:00")
    } else {
        at
    });
    terminal["lifecycle_source"] = json!("polymarket_gamma");
    terminal["lifecycle_source_url"] = evidence["source_url"].clone();
    terminal["lifecycle_evidence_sha256"] = evidence["raw_response_sha256"].clone();
    terminal["observed_at"] = evidence["observed_at"].clone();
    terminal["metadata_revision"] = json!(canonical_hash(
        &json!({"previous_revision":market["metadata_revision"],"terminal_evidence":evidence["raw_response_sha256"]})
    ));
    let payload = json!({"reason":if settlement.is_some() {"gamma_resolved_payout_vector"} else {"gamma_closed_resolution_unverified"},"market":terminal,
        "retired_token_ids":market["identity"]["outcomes"].as_array().context("missing outcomes")?.iter().map(|o| o["token_id"].clone()).collect::<Vec<_>>()});
    let at = chrono::DateTime::parse_from_rfc3339(
        evidence["observed_at"]
            .as_str()
            .context("missing observed time")?,
    )?
    .with_timezone(&chrono::Utc);
    let at = at
        .with_nanosecond(at.nanosecond() / 1000 * 1000)
        .context("invalid observed time")?;
    let at = at.to_rfc3339_opts(
        if at.nanosecond() == 0 {
            chrono::SecondsFormat::Secs
        } else {
            chrono::SecondsFormat::Micros
        },
        true,
    );
    let mut event = json!({"contract_version":"marketcow.prediction_market.v1","schema_version":"marketcow.polymarket.live.v2",
        "cursor":cursor,"event_type":"market_terminal","market_id":raw["id"],"condition_id":raw["conditionId"],
        "token_id":null,"book_epoch":null,"sequence":null,"exchange_at":at,"received_at":at,
        "canonical_payload_sha256":canonical_hash(&payload),"canonical_payload":payload,
        "raw_payload_sha256":canonical_hash(&raw),"raw_payload":raw,"applied":true,"fail_closed_reason":null,"gaps":[]});
    event["event_id"] = json!(canonical_hash(&event));
    Ok(event)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> (Market, Value) {
        (
            Market {
                market_id: "1".into(),
                condition_id: "condition".into(),
                token_ids: ["10".into(), "11".into()],
            },
            json!({"id":"1","conditionId":"condition","clobTokenIds":"[\"10\",\"11\"]","outcomes":"[\"Yes\",\"No\"]","outcomePrices":"[\"0\",\"1\"]","closed":true,"acceptingOrders":false,"umaResolutionStatus":"resolved","closedTime":"2026-01-01 00:00:00+00"}),
        )
    }
    #[test]
    fn terminal_requires_explicit_bound_evidence() {
        let (market, raw) = fixture();
        assert!(validate(&market, &raw).unwrap());
        for key in [
            "id",
            "conditionId",
            "clobTokenIds",
            "closed",
            "acceptingOrders",
            "closedTime",
        ] {
            let mut missing = raw.clone();
            missing.as_object_mut().unwrap().remove(key);
            assert!(validate(&market, &missing).is_err(), "{key}");
        }
        for (key, value) in [
            ("closed", json!(false)),
            ("acceptingOrders", json!(true)),
            ("umaResolutionStatus", json!("proposed")),
        ] {
            let mut changed = raw.clone();
            changed[key] = value;
            assert!(!validate(&market, &changed).unwrap());
        }
        let mut wrong = raw.clone();
        wrong["clobTokenIds"] = json!("[\"10\",\"10\"]");
        assert!(validate(&market, &wrong).is_err());
        let payout = resolution(&raw).unwrap().unwrap();
        assert_eq!(payout["winning_outcome"], "No");
        assert_eq!(payout["winning_token_id"], "11");
        for prices in ["[\"0.5\",\"0.5\"]", "[\"1\",\"1\"]", "[\"broken\",\"0\"]"] {
            let mut invalid = raw.clone();
            invalid["outcomePrices"] = json!(prices);
            assert!(resolution(&invalid).is_err());
        }
    }
    #[test]
    fn resolved_payout_is_bound_to_catalog_outcome_identity() {
        let (_, raw) = fixture();
        let settlement = resolution(&raw).unwrap().unwrap();
        let evidence = json!({
            "raw_response": serde_json::to_string(&raw).unwrap(),
            "raw_response_sha256": "a".repeat(64),
            "source_url": "https://gamma-api.polymarket.com/markets/1",
            "observed_at": "2026-09-05T00:00:00Z",
            "settlement": settlement,
        });
        let market = json!({
            "identity":{"market_id":"1","condition_id":"condition","outcomes":[
                {"outcome":"Yes","token_id":"10"},{"outcome":"No","token_id":"11"}
            ]},
            "metadata_revision":"previous"
        });
        let event = terminal_event(&market, &evidence, 7).unwrap();
        assert_eq!(
            event["canonical_payload"]["market"]["lifecycle_state"],
            "resolved"
        );
        assert_eq!(event["canonical_payload"]["market"]["resolution"], "No");
        assert_eq!(
            event["canonical_payload"]["reason"],
            "gamma_resolved_payout_vector"
        );
        let mut tampered = evidence;
        tampered["settlement"]["winning_outcome"] = json!("Yes");
        assert!(terminal_event(&market, &tampered, 8).is_err());
    }
    #[test]
    fn evidence_is_content_addressed_and_keeps_raw_response() {
        let dir = std::env::temp_dir().join(format!("lifecycle-test-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&dir).unwrap();
        let evidence = json!({"raw_response":"exact bytes", "settlement":null});
        persist(&dir, "catalog", &evidence).unwrap();
        let path = fs::read_dir(dir.join("lifecycle-observations"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let bytes = fs::read(&path).unwrap();
        assert_eq!(
            path.file_stem().unwrap().to_str().unwrap(),
            hex::encode(Sha256::digest(&bytes))
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&bytes).unwrap()["evidence"],
            evidence
        );
        fs::remove_dir_all(dir).unwrap();
    }
}

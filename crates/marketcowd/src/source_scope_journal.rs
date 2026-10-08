//! Small operator-owned restart state, not market history. Only the control
//! worker writes this file; the market publication loop never waits for it.
use anyhow::{Context, Result, ensure};
use marketcow_polymarket::discovery_source::canonical_hash;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Write,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct State {
    pub schema_version: String,
    pub catalog_revision: String,
    pub catalog_manifest_sha256: String,
    pub pool: String,
    pub active_config: Value,
    pub active_revision: u64,
    pub catalog_source: Value,
    pub records: BTreeMap<String, Value>,
    pub acquisition_market_ids: BTreeSet<String>,
    pub retiring_records: BTreeMap<String, Value>,
    pub retirement_ticket: Option<u64>,
    // None means an older pending state did not record prior hot membership.
    // Some(empty) is a known metadata-only retirement and may be cancelled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retiring_acquisition_market_ids: Option<BTreeSet<String>>,
}
impl State {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            matches!(self.schema_version.as_str(),
                "marketcow.runtime-scope-state.v1" | "marketcow.runtime-scope-state.v2")
                && matches!(self.pool.as_str(), "live" | "discovery")
                && self.active_revision > 0,
            "runtime state identity"
        );
        ensure!(
            self.active_config["catalog_revision"] == self.catalog_revision,
            "runtime active catalog"
        );
        ensure!(
            !self.records.is_empty() && self.records.len() + self.retiring_records.len() <= 8192,
            "runtime metadata capacity"
        );
        ensure!(
            self.retiring_records.is_empty() == self.retirement_ticket.is_none(),
            "runtime retirement binding"
        );
        ensure!(
            self.retiring_acquisition_market_ids.as_ref().is_none_or(|ids|
                ids.iter().all(|id| self.retiring_records.contains_key(id))),
            "runtime retiring acquisition binding"
        );
        let mut tokens = BTreeSet::new();
        for (id, record) in self.records.iter().chain(&self.retiring_records) {
            ensure!(
                !id.is_empty() && record["identity"]["market_id"] == *id,
                "runtime metadata identity"
            );
            let outcomes = record["identity"]["outcomes"]
                .as_array()
                .context("runtime outcomes")?;
            ensure!(
                outcomes.len() == 2
                    && record["identity"]["condition_id"]
                        .as_str()
                        .is_some_and(|v| !v.is_empty()),
                "runtime binary identity"
            );
            for outcome in outcomes {
                let token = outcome["token_id"].as_str().context("runtime token")?;
                ensure!(
                    !token.is_empty() && token.len() <= 128 && tokens.insert(token),
                    "runtime token ownership"
                );
            }
        }
        ensure!(
            self.acquisition_market_ids
                .iter()
                .all(|id| self.records.contains_key(id)),
            "runtime acquisition metadata absent"
        );
        Ok(())
    }
    pub fn markets(&self) -> Result<Vec<super::Market>> {
        self.markets_by_ids(&self.acquisition_market_ids)
    }
    /// A process-local lease does not survive restart. Historical admission is
    /// evidence of a past preparation, not a renewed hot acquisition request.
    /// Only the durable configured scope and its explicit relation dependencies
    /// may seed an on-demand owner before consumers acquire fresh leases.
    pub fn restart_markets(&self, on_demand: bool) -> Result<Vec<super::Market>> {
        if !on_demand {
            return self.markets();
        }
        let mut ids = BTreeSet::new();
        if self.pool == "live" {
            for market in self.active_config["configured_markets"]
                .as_array()
                .context("restart configured markets")?
            {
                ids.insert(market["market_id"].as_str().context("restart scope market")?.to_owned());
            }
            let roots = ids.clone();
            for id in roots {
                let record = self.records.get(&id).context("restart scope metadata missing")?;
                if let Some(relations) = record["relations"].as_array() {
                    for relation in relations {
                        for pair in relation["outcome_pairs"].as_array().context("restart relation pairs")? {
                            ids.insert(pair["market_id"].as_str().context("restart relation market")?.to_owned());
                        }
                    }
                }
            }
        } else {
            for market in self.active_config["market_ids"]
                .as_array()
                .context("restart discovery markets")?
            {
                ids.insert(market.as_str().context("restart discovery market")?.to_owned());
            }
            for relation in self.active_config["relations"].as_array().context("restart discovery relations")? {
                for member in relation["member_market_ids"].as_array().context("restart discovery members")? {
                    ids.insert(member.as_str().context("restart discovery member")?.to_owned());
                }
            }
        }
        ensure!(ids.is_subset(&self.acquisition_market_ids), "restart scope is not prepared");
        self.markets_by_ids(&ids)
    }
    fn markets_by_ids(&self, ids: &BTreeSet<String>) -> Result<Vec<super::Market>> {
        ids
            .iter()
            .map(|id| {
                let identity = &self.records.get(id).context("runtime market missing")?["identity"];
                let outcomes = identity["outcomes"]
                    .as_array()
                    .context("runtime outcomes")?;
                Ok(super::Market {
                    market_id: id.clone(),
                    condition_id: identity["condition_id"]
                        .as_str()
                        .context("runtime condition")?
                        .into(),
                    token_ids: [
                        outcomes[0]["token_id"]
                            .as_str()
                            .context("runtime token")?
                            .into(),
                        outcomes[1]["token_id"]
                            .as_str()
                            .context("runtime token")?
                            .into(),
                    ],
                })
            })
            .collect()
    }
}

pub struct Journal {
    pub state: State,
    path: PathBuf,
    maximum_bytes: usize,
}

/// Offline-only plan for a quiesced snapshot. This does not rewrite the input
/// journal or acquire supplier sockets. The caller must commit `cold_records`
/// into the cold catalog before publishing `hot_state` as its new journal.
pub struct ColdMigration {
    pub hot_state: State,
    pub cold_records: BTreeMap<String, Value>,
}

pub fn plan_offline_cold_migration(
    state: &State,
    additionally_required_hot: &BTreeSet<String>,
) -> Result<ColdMigration> {
    state.validate()?;
    ensure!(state.retirement_ticket.is_none()
        || state.retiring_acquisition_market_ids.is_some(),
        "legacy pending retirement has unknown prior hot membership");
    let static_hot: BTreeSet<String> = state.restart_markets(true)?
        .into_iter().map(|market| market.market_id).collect();
    let mut retained = static_hot;
    retained.extend(additionally_required_hot.iter().cloned());
    ensure!(retained.is_subset(&state.acquisition_market_ids),
        "offline migration cannot invent or revive hot admission");
    let mut hot_state = state.clone();
    let cold_records = hot_state.records.iter()
        .filter(|(id, _)| !retained.contains(*id))
        .map(|(id, record)| (id.clone(), record.clone()))
        .collect::<BTreeMap<_, _>>();
    hot_state.records.retain(|id, _| retained.contains(id));
    hot_state.acquisition_market_ids.retain(|id| retained.contains(id));
    hot_state.validate()?;
    ensure!(cold_records.len() + hot_state.records.len() == state.records.len(),
        "offline cold migration record conservation");
    for (id, record) in &cold_records {
        ensure!(record["identity"]["market_id"] == *id,
            "offline cold migration identity mismatch");
    }
    Ok(ColdMigration { hot_state, cold_records })
}
pub fn read(path: &Path, maximum_bytes: usize) -> Result<Option<State>> {
    ensure!(
        path.is_absolute() && (1024..=256 * 1024 * 1024).contains(&maximum_bytes),
        "runtime journal path/budget"
    );
    let meta = match std::fs::symlink_metadata(path) {
        Ok(meta) => meta,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    ensure!(
        meta.is_file()
            && meta.permissions().mode() & 0o077 == 0
            && meta.len() <= maximum_bytes as u64,
        "runtime journal file/bytes/permissions"
    );
    let bytes = std::fs::read(path)?;
    let value: Value = serde_json::from_slice(&bytes)?;
    ensure!(
        serde_json::to_vec(&value)? == bytes
            && value.as_object().is_some_and(|v| v.len() == 2
                && v.contains_key("state")
                && v.contains_key("sha256")),
        "runtime journal canonical envelope"
    );
    ensure!(
        value["sha256"] == canonical_hash(&value["state"]),
        "runtime journal hash"
    );
    let state: State = serde_json::from_value(value["state"].clone())?;
    state.validate()?;
    Ok(Some(state))
}
impl Journal {
    pub fn open(path: PathBuf, maximum_bytes: usize, initial: State) -> Result<Self> {
        let parent = path.parent().context("runtime journal parent")?;
        let meta = std::fs::symlink_metadata(parent)?;
        ensure!(
            path.is_absolute()
                && parent.canonicalize()? == parent
                && meta.is_dir()
                && meta.permissions().mode() & 0o077 == 0,
            "private runtime journal directory"
        );
        initial.validate()?;
        let saved = read(&path, maximum_bytes)?;
        if let Some(saved) = &saved {
            ensure!(
                saved.catalog_manifest_sha256 == initial.catalog_manifest_sha256
                    && saved.catalog_revision == initial.catalog_revision
                    && saved.pool == initial.pool,
                "runtime source binding differs"
            );
        }
        let mut journal = Self {
            state: saved.unwrap_or(initial),
            path,
            maximum_bytes,
        };
        journal.commit(journal.state.clone())?;
        Ok(journal)
    }
    pub fn commit(&mut self, next: State) -> Result<()> {
        next.validate()?;
        let state = serde_json::to_value(&next)?;
        let bytes = crate::source_public_api::encode_bounded(
            &json!({"sha256":canonical_hash(&state),"state":state}),
            self.maximum_bytes,
        )?;
        let parent = self.path.parent().context("runtime journal parent")?;
        let temporary = parent.join(format!(".runtime-scope-{}", uuid::Uuid::new_v4()));
        let result = (|| -> Result<()> {
            let mut file = std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .mode(0o600)
                .open(&temporary)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            std::fs::rename(&temporary, &self.path)?;
            std::fs::File::open(parent)?.sync_all()?;
            Ok(())
        })();
        if result.is_err() {
            let _ = std::fs::remove_file(&temporary);
        }
        result?;
        self.state = next;
        Ok(())
    }
    pub fn admit(&mut self, records: &[Value], acquisition: &BTreeSet<String>) -> Result<()> {
        let mut next = self.state.clone();
        for record in records {
            let id = record["identity"]["market_id"]
                .as_str()
                .context("admission identity")?;
            ensure!(
                !next.retiring_records.contains_key(id),
                "runtime retirement pending"
            );
            if let Some(old) = next.records.get(id) {
                ensure!(
                    old["identity"] == record["identity"],
                    "runtime identity changed"
                );
            } else {
                next.records.insert(id.into(), record.clone());
            }
        }
        next.acquisition_market_ids
            .extend(acquisition.iter().cloned());
        self.commit(next)
    }
    pub fn retire(&mut self, markets: &BTreeSet<String>, ticket: u64) -> Result<()> {
        if self.state.retirement_ticket == Some(ticket)
            && self
                .state
                .retiring_records
                .keys()
                .cloned()
                .collect::<BTreeSet<_>>()
                == *markets
        {
            return Ok(()); // explicit retry of the same durable, unqueued intent
        }
        ensure!(
            self.state.retiring_records.is_empty() && ticket > 0,
            "runtime retirement slot busy"
        );
        let mut next = self.state.clone();
        next.retiring_acquisition_market_ids = Some(markets.intersection(&next.acquisition_market_ids)
            .cloned().collect());
        for id in markets {
            let record = next
                .records
                .remove(id)
                .context("runtime retirement unknown market")?;
            next.retiring_records.insert(id.clone(), record);
            next.acquisition_market_ids.remove(id);
        }
        next.retirement_ticket = Some(ticket);
        self.commit(next)
    }
    pub fn settled(&mut self, ticket: u64) -> Result<()> {
        if self
            .state
            .retirement_ticket
            .is_some_and(|pending| pending <= ticket)
        {
            let mut next = self.state.clone();
            next.retiring_records.clear();
            next.retirement_ticket = None;
            next.retiring_acquisition_market_ids = None;
            self.commit(next)?;
        }
        Ok(())
    }
    /// Only the serial owner may attest that retirement was rejected before
    /// any wire, publication or durability effect. Restore the exact prior
    /// hot membership so a fresh lease cannot be lost on process restart.
    pub fn cancel_unstarted_retirement(
        &mut self,
        markets: &BTreeSet<String>,
        ticket: u64,
        expected_revision: u64,
        expected_catalog_revision: &str,
    ) -> Result<()> {
        ensure!(
            self.state.retirement_ticket == Some(ticket)
                && self.state.retiring_records.keys().cloned().collect::<BTreeSet<_>>() == *markets
                && self.state.active_revision == expected_revision
                && self.state.catalog_revision == expected_catalog_revision,
            "runtime retirement cancellation identity"
        );
        ensure!(self.state.retiring_acquisition_market_ids.is_some(),
            "runtime prior hot membership unknown; keep retirement pending");
        let mut next = self.state.clone();
        for (id, record) in std::mem::take(&mut next.retiring_records) {
            ensure!(next.records.insert(id, record).is_none(), "runtime cancellation duplicate");
        }
        next.acquisition_market_ids.extend(next.retiring_acquisition_market_ids
            .take().context("runtime prior hot membership")?);
        next.retirement_ticket = None;
        self.commit(next)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn record(id: &str) -> Value {
        json!({"identity":{"market_id":id,"condition_id":format!("condition{id}"),
        "outcomes":[{"token_id":format!("{id}1")},{"token_id":format!("{id}2")}]}})
    }
    #[test]
    fn restart_does_not_promote_historical_admission_to_hot_demand() {
        let mut first = record("1");
        first["relations"] = json!([{"outcome_pairs":[{"market_id":"2"}]}]);
        let state = State {
            schema_version: "marketcow.runtime-scope-state.v1".into(),
            catalog_revision: "c".into(), catalog_manifest_sha256: "m".into(),
            pool: "live".into(),
            active_config: json!({"catalog_revision":"c","configured_markets":[{"market_id":"1"}]}),
            active_revision: 1, catalog_source: Value::Null,
            records: BTreeMap::from([
                ("1".into(), first), ("2".into(), record("2")),
                ("3".into(), record("3")),
            ]),
            acquisition_market_ids: BTreeSet::from(["1".into(), "2".into(), "3".into()]),
            retiring_records: Default::default(), retirement_ticket: None,
            retiring_acquisition_market_ids: Default::default(),
        };
        state.validate().unwrap();
        let hot = state.restart_markets(true).unwrap();
        assert_eq!(hot.iter().map(|m| m.market_id.as_str()).collect::<Vec<_>>(), vec!["1", "2"]);
        assert_eq!(state.restart_markets(false).unwrap().len(), 3);
        let mut broken = state;
        broken.acquisition_market_ids.remove("2");
        assert!(broken.restart_markets(true).is_err());
    }
    #[test]
    fn saturated_cold_catalog_does_not_consume_restart_hot_token_budget() {
        let records: BTreeMap<String, Value> = (1..=2048)
            .map(|n| (n.to_string(), record(&n.to_string())))
            .collect();
        let state = State {
            schema_version: "marketcow.runtime-scope-state.v1".into(),
            catalog_revision: "c".into(), catalog_manifest_sha256: "m".into(),
            pool: "live".into(),
            active_config: json!({"catalog_revision":"c","configured_markets":[{"market_id":"1"}]}),
            active_revision: 1, catalog_source: Value::Null,
            acquisition_market_ids: records.keys().cloned().collect(), records,
            retiring_records: Default::default(), retirement_ticket: None,
            retiring_acquisition_market_ids: Default::default(),
        };
        state.validate().unwrap();
        assert_eq!(state.acquisition_market_ids.len() * 2, 4096);
        assert_eq!(state.restart_markets(true).unwrap().len() * 2, 2);
        assert_eq!(state.records.len(), 2048); // cold evidence remains addressable
    }
    #[test]
    fn restart_retains_admitted_metadata_and_pending_retirement_until_durable_receipt() {
        let directory = std::env::temp_dir()
            .canonicalize()
            .unwrap()
            .join(format!("mc-journal-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).unwrap();
        let path = directory.join("state.json");
        let initial = State {
            schema_version: "marketcow.runtime-scope-state.v1".into(),
            catalog_revision: "c".into(),
            catalog_manifest_sha256: "m".into(),
            pool: "live".into(),
            active_config: json!({"catalog_revision":"c"}),
            active_revision: 1,
            catalog_source: Value::Null,
            records: BTreeMap::from([("1".into(), record("1"))]),
            acquisition_market_ids: BTreeSet::from(["1".into()]),
            retiring_records: Default::default(),
            retirement_ticket: None,
            retiring_acquisition_market_ids: Default::default(),
        };
        let mut journal = Journal::open(path.clone(), 65536, initial.clone()).unwrap();
        journal
            .admit(&[record("2")], &BTreeSet::from(["2".into()]))
            .unwrap();
        let loaded = read(&path, 65536).unwrap().unwrap();
        assert_eq!(loaded.markets().unwrap().len(), 2);
        journal.retire(&BTreeSet::from(["1".into()]), 3).unwrap();
        let loaded = read(&path, 65536).unwrap().unwrap();
        assert_eq!(loaded.markets().unwrap().len(), 1);
        assert_eq!(loaded.retiring_records.len(), 1);
        assert_eq!(loaded.retirement_ticket, Some(3));
        assert_eq!(
            canonical_hash(&loaded.retiring_records["1"]),
            canonical_hash(&initial.records["1"]),
        );
        // Simulate a process crash before the asynchronous retirement receipt.
        // Reopening must keep the exact cold identity and pending ticket;
        // reopening alone must not claim that the wire or writer completed.
        drop(journal);
        let mut journal = Journal::open(path.clone(), 65536, initial).unwrap();
        assert_eq!(journal.state.retirement_ticket, Some(3));
        assert_eq!(journal.state.retiring_records.len(), 1);
        journal.settled(2).unwrap();
        assert_eq!(journal.state.retiring_records.len(), 1);
        journal.settled(3).unwrap();
        assert!(
            read(&path, 65536)
                .unwrap()
                .unwrap()
                .retiring_records
                .is_empty()
        );
        let before = std::fs::read(&path).unwrap();
        assert!(
            journal
                .admit(&[record("3")], &BTreeSet::from(["unknown".into()]))
                .is_err()
        );
        assert_eq!(std::fs::read(&path).unwrap(), before);
        std::fs::write(&path, b"{}").unwrap();
        assert!(read(&path, 65536).is_err());
        std::fs::remove_file(path).unwrap();
        std::fs::remove_dir(directory).unwrap();
    }
    #[test]
    fn pre_effect_lease_conflict_restores_exact_hot_membership() {
        let directory = std::env::temp_dir().canonicalize().unwrap()
            .join(format!("mc-journal-cancel-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).unwrap();
        let path = directory.join("state.json");
        let initial = State {
            schema_version: "marketcow.runtime-scope-state.v1".into(),
            catalog_revision: "c".into(), catalog_manifest_sha256: "m".into(),
            pool: "live".into(),
            active_config: json!({"catalog_revision":"c"}),
            active_revision: 1, catalog_source: Value::Null,
            records: BTreeMap::from([("1".into(), record("1")),
                ("2".into(), record("2")), ("3".into(), record("3"))]),
            acquisition_market_ids: BTreeSet::from(["1".into()]),
            retiring_records: Default::default(), retirement_ticket: None,
            retiring_acquisition_market_ids: Default::default(),
        };
        let before = canonical_hash(&serde_json::to_value(&initial).unwrap());
        let mut journal = Journal::open(path.clone(), 65536, initial).unwrap();
        let both = BTreeSet::from(["1".into(), "2".into()]);
        journal.retire(&both, 7).unwrap();
        assert_eq!(journal.state.retiring_acquisition_market_ids,
            Some(BTreeSet::from(["1".into()])));
        assert!(journal.cancel_unstarted_retirement(&both, 6, 1, "c").is_err());
        assert!(journal.cancel_unstarted_retirement(&BTreeSet::from(["1".into()]), 7, 1, "c").is_err());
        assert!(journal.cancel_unstarted_retirement(&both, 7, 2, "c").is_err());
        assert!(journal.cancel_unstarted_retirement(&both, 7, 1, "other").is_err());
        journal.cancel_unstarted_retirement(&both, 7, 1, "c").unwrap();
        assert_eq!(canonical_hash(&serde_json::to_value(&journal.state).unwrap()), before);
        assert_eq!(journal.state.acquisition_market_ids, BTreeSet::from(["1".into()]));
        assert!(journal.state.retiring_records.is_empty());
        assert_eq!(journal.state.retirement_ticket, None);
        assert_eq!(read(&path, 65536).unwrap().unwrap().acquisition_market_ids,
            BTreeSet::from(["1".into()]));
        std::fs::remove_file(path).unwrap();
        std::fs::remove_dir(directory).unwrap();
    }
    #[test]
    fn legacy_unknown_prior_membership_cannot_be_cancelled_as_explicit_empty() {
        let directory = std::env::temp_dir().canonicalize().unwrap()
            .join(format!("mc-journal-legacy-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).unwrap();
        let path = directory.join("state.json");
        let initial = State {
            schema_version: "marketcow.runtime-scope-state.v1".into(),
            catalog_revision: "c".into(), catalog_manifest_sha256: "m".into(),
            pool: "live".into(), active_config: json!({"catalog_revision":"c"}),
            active_revision: 1, catalog_source: Value::Null,
            records: BTreeMap::from([("1".into(), record("1")),
                ("2".into(), record("2"))]),
            acquisition_market_ids: BTreeSet::from(["1".into()]),
            retiring_records: Default::default(), retirement_ticket: None,
            retiring_acquisition_market_ids: None,
        };
        let metadata_only = BTreeSet::from(["2".into()]);
        let mut journal = Journal::open(path.clone(), 65536, initial.clone()).unwrap();
        journal.retire(&metadata_only, 8).unwrap();
        assert_eq!(journal.state.retiring_acquisition_market_ids, Some(BTreeSet::new()));
        journal.cancel_unstarted_retirement(&metadata_only, 8, 1, "c").unwrap();
        assert_eq!(journal.state.acquisition_market_ids, BTreeSet::from(["1".into()]));

        // Simulate a real older v1 pending file: its envelope hash is valid,
        // but the newly added prior-membership field was absent on disk.
        journal.retire(&BTreeSet::from(["1".into()]), 9).unwrap();
        let mut state = serde_json::to_value(&journal.state).unwrap();
        state.as_object_mut().unwrap().remove("retiring_acquisition_market_ids");
        let old_bytes = serde_json::to_vec(&json!({"sha256":canonical_hash(&state),"state":state})).unwrap();
        std::fs::write(&path, &old_bytes).unwrap();
        drop(journal);
        let mut old = Journal::open(path.clone(), 65536, initial).unwrap();
        assert_eq!(old.state.retiring_acquisition_market_ids, None);
        let pending = BTreeSet::from(["1".into()]);
        let before = std::fs::read(&path).unwrap();
        assert!(old.cancel_unstarted_retirement(&pending, 9, 1, "c").is_err());
        assert_eq!(std::fs::read(&path).unwrap(), before);
        assert_eq!(old.state.retirement_ticket, Some(9));
        assert_eq!(old.state.retiring_records.len(), 1);
        std::fs::remove_file(path).unwrap();
        std::fs::remove_dir(directory).unwrap();
    }
    #[test]
    fn offline_plan_cools_saturated_legacy_admission_without_losing_catalog_records() {
        let records = (0..2048).map(|n| (n.to_string(), record(&n.to_string())))
            .collect::<BTreeMap<_, _>>();
        let hot = records.keys().cloned().collect();
        let state = State {
            schema_version:"marketcow.runtime-scope-state.v1".into(),
            catalog_revision:"c".into(), catalog_manifest_sha256:"m".into(),
            pool:"live".into(), active_config:json!({"catalog_revision":"c",
                "configured_markets":[{"market_id":"0"}]}), active_revision:1,
            catalog_source:Value::Null, records, acquisition_market_ids:hot,
            retiring_records:Default::default(), retirement_ticket:None,
            retiring_acquisition_market_ids:None,
        };
        let before = state.records.iter().map(|(id, row)| (id.clone(), canonical_hash(row)))
            .collect::<BTreeMap<_, _>>();
        let migrated = plan_offline_cold_migration(&state, &BTreeSet::new()).unwrap();
        assert_eq!(migrated.hot_state.acquisition_market_ids, BTreeSet::from(["0".into()]));
        assert_eq!(migrated.cold_records.len(), 2047);
        for (id, row) in migrated.hot_state.records.iter().chain(&migrated.cold_records) {
            assert_eq!(canonical_hash(row), before[id]);
        }
        let mut legacy_pending = state;
        legacy_pending.retiring_records.insert("2048".into(), record("2048"));
        legacy_pending.retirement_ticket = Some(1);
        assert!(plan_offline_cold_migration(&legacy_pending, &BTreeSet::new()).is_err());
        legacy_pending.retiring_acquisition_market_ids = Some(BTreeSet::new());
        assert!(plan_offline_cold_migration(&legacy_pending, &BTreeSet::new()).is_ok());
    }
}

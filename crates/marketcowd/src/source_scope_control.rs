//! Private same-user publication control. No TCP listener, source paths,
//! executable names, account operations, or acquisition claims in this wire.
//! The authenticated outer control plane must prepare acquisition first.
use crate::{
    source_discovery_api::DiscoveryScopeControl,
    source_discovery_projection::DiscoveryConfig,
    source_public_api::{ConfiguredScope, PublicScopeControl},
};
use anyhow::{Context, Result, ensure};
use serde::Deserialize;
use serde_json::{Value, json};
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub enum Backend {
    Live(PublicScopeControl),
    Discovery(DiscoveryScopeControl),
}

pub enum AcquisitionAction {
    Scope {
        validate_only: bool,
        catalog_revision: String,
        records: Vec<Value>,
        evidence_sha256: String,
        acquisition_market_ids: std::collections::BTreeSet<String>,
        retire_market_ids: std::collections::BTreeSet<String>,
    },
    LeaseStatus {
        eligible_market_ids: std::collections::BTreeSet<String>,
    },
    RetirementStatus {
        market_ids: std::collections::BTreeSet<String>,
    },
    AcquireLease {
        lease_id: String,
        ttl_seconds: u64,
        market_ids: std::collections::BTreeSet<String>,
        eligible_market_ids: std::collections::BTreeSet<String>,
    },
    RenewLease {
        lease_id: String,
        ttl_seconds: u64,
    },
    RefreshLeaseMarkets {
        lease_id: String,
        market_ids: std::collections::BTreeSet<String>,
    },
    ReplaceLease {
        lease_id: String,
        ttl_seconds: u64,
        expected_market_ids: std::collections::BTreeSet<String>,
        market_ids: std::collections::BTreeSet<String>,
        eligible_market_ids: std::collections::BTreeSet<String>,
        removed_token_ids: [String; 2],
        added_token_ids: [String; 2],
    },
    ReleaseLease {
        lease_id: String,
    },
}
pub struct AcquisitionRequest {
    pub action: AcquisitionAction,
    pub receipt: tokio::sync::oneshot::Sender<Result<Value>>,
}
pub type AcquisitionSender = tokio::sync::mpsc::Sender<AcquisitionRequest>;

fn select_unreferenced_hot(
    state: &crate::source_scope_journal::State,
    protected: &BTreeSet<String>,
) -> Vec<String> {
    state.acquisition_market_ids.difference(protected).take(16).cloned().collect()
}

/// A prepared BTC 5m candidate is hot before its profile CAS. The manager
/// writes its reviewed current+next records to the durable runtime journal
/// first, so this protection survives a process restart without introducing
/// a second reservation store. It expires at the market end; abandoned
/// preparations therefore cannot hold the hot budget indefinitely.
fn prepared_near_term_hot(
    state: &crate::source_scope_journal::State,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<BTreeSet<String>> {
    let mut protected = BTreeSet::new();
    for id in &state.acquisition_market_ids {
        let record = state.records.get(id).context("hot market record missing")?;
        let (Some(start), Some(end)) = (record["start_at"].as_str(), record["end_at"].as_str()) else {
            continue;
        };
        let start = chrono::DateTime::parse_from_rfc3339(start)
            .context("hot market start time")?.with_timezone(&chrono::Utc);
        let end = chrono::DateTime::parse_from_rfc3339(end)
            .context("hot market end time")?.with_timezone(&chrono::Utc);
        let outcomes = record["identity"]["outcomes"].as_array();
        let btc_5m_shape = end.signed_duration_since(start) == chrono::Duration::minutes(5)
            && start.timestamp().rem_euclid(300) == 0
            && outcomes.is_some_and(|values| values.len() == 2
                && values[0]["outcome"] == "Up" && values[1]["outcome"] == "Down");
        if btc_5m_shape && now < end
            && start <= now + chrono::Duration::minutes(10)
        {
            protected.insert(id.clone());
        }
    }
    Ok(protected)
}

#[derive(Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
enum Command {
    Status,
    Btc5mStatus,
    PublishBtc5mProfile {
        expected_profile_id: String,
        expected_revision: u64,
        lease_id: String,
        lease_ttl_seconds: u64,
        profile: Value,
    },
    PreparePublication {
        expected_scope_id: String,
        expected_revision: u64,
        config: Value,
    },
    PublishScope {
        expected_scope_id: String,
        expected_revision: u64,
        config: Value,
    },
    PrepareAcquisition {
        expected_scope_id: String,
        expected_revision: u64,
        catalog_revision: String,
        records: Vec<Value>,
        evidence_sha256: String,
        acquisition_market_ids: Vec<String>,
    },
    RetireAcquisition {
        expected_scope_id: String,
        expected_revision: u64,
        market_ids: Vec<String>,
    },
    AcquisitionLeaseStatus,
    /// Bounded, read-only candidate selection. The serial owner rechecks all
    /// references again when the matching RetireAcquisition is applied.
    RetirementCandidates,
    AcquireAcquisitionLease {
        expected_scope_id: String,
        expected_revision: u64,
        lease_id: String,
        ttl_seconds: u64,
        market_ids: Vec<String>,
    },
    RenewAcquisitionLease {
        expected_scope_id: String,
        expected_revision: u64,
        lease_id: String,
        ttl_seconds: u64,
    },
    RefreshBtc5mCurrent {
        lease_id: String,
    },
    ReplaceAcquisitionLease {
        expected_scope_id: String,
        expected_revision: u64,
        lease_id: String,
        ttl_seconds: u64,
        expected_market_ids: Vec<String>,
        market_ids: Vec<String>,
        removed_token_ids: [String; 2],
        added_token_ids: [String; 2],
    },
    ReleaseAcquisitionLease {
        expected_scope_id: String,
        expected_revision: u64,
        lease_id: String,
    },
}

impl Backend {
    fn status(&self) -> Result<Value> {
        match self {
            Self::Live(control) => control.status(),
            Self::Discovery(control) => control.status(),
        }
    }
    fn execute(
        &self,
        body: &[u8],
        acquisition: Option<&AcquisitionSender>,
        btc_5m: Option<&crate::source_btc5m_api::Control>,
        mut journal: Option<&mut crate::source_scope_journal::Journal>,
    ) -> Result<Value> {
        if let Some(journal) = journal.as_mut() {
            if let Some(ticket) = journal.state.retirement_ticket {
                let persisted = self.status()?["retirement_persisted"].as_u64().unwrap_or(0);
                if persisted >= ticket {
                    let markets = journal.state.retiring_records.keys().cloned().collect();
                    let receipt = send_acquisition(acquisition,
                        AcquisitionAction::RetirementStatus { market_ids: markets })?;
                    if receipt["draining_market_ids"].as_array().is_some_and(|rows| rows.is_empty()) {
                        journal.settled(ticket)?;
                    }
                }
            }
        }
        // Canonical compact object ordering also prevents duplicate-key input
        // from being silently collapsed by Value deserialization.
        let value: Value = serde_json::from_slice(body)?;
        ensure!(
            serde_json::to_vec(&value)? == body,
            "noncanonical control JSON"
        );
        let command: Command = serde_json::from_value(value)?;
        let (expected, revision, config, publish) = match command {
            Command::Status => return self.status(),
            Command::Btc5mStatus => {
                return btc_5m.context("BTC 5m control unavailable")?.status();
            }
            Command::PublishBtc5mProfile {
                expected_profile_id,
                expected_revision,
                lease_id,
                lease_ttl_seconds,
                profile,
            } => {
                ensure!(
                    valid_lease_id(&lease_id),
                    "invalid BTC 5m acquisition lease id"
                );
                let profile: crate::source_btc5m_api::Profile = serde_json::from_value(profile)?;
                // Reject a candidate that crossed its UTC boundary before
                // changing any acquisition lease or profile state.
                profile.validate_current_at(chrono::Utc::now())?;
                let control = btc_5m.context("BTC 5m control unavailable")?;
                let incumbent = control.profile()?;
                let base = self.referenced_markets()?;
                let desired = profile.market_ids_owned();
                let expected = incumbent.market_ids_owned();
                let mut available = base;
                available.extend(expected.iter().cloned());
                available.extend(desired.iter().cloned());
                let lease_status = send_acquisition(
                    acquisition,
                    AcquisitionAction::LeaseStatus {
                        eligible_market_ids: available.clone(),
                    },
                )?;
                let mut restart_reconciled = false;
                let lease_receipt = if lease_status["required"] == true {
                    let existing = lease_status["leases"]
                        .as_array()
                        .context("acquisition lease status")?
                        .iter()
                        .find(|lease| lease["lease_id"] == lease_id);
                    if let Some(existing) = existing {
                        profile.validate_rotation_from(&incumbent)?;
                        let installed: std::collections::BTreeSet<String> =
                            serde_json::from_value(existing["market_ids"].clone())?;
                        let removed_market = expected
                            .difference(&desired)
                            .next()
                            .context("BTC 5m rotation removed market")?;
                        let added_market = desired
                            .difference(&expected)
                            .next()
                            .context("BTC 5m rotation added market")?;
                        ensure!(
                            expected.difference(&desired).count() == 1
                                && desired.difference(&expected).count() == 1,
                            "BTC 5m rotation must replace one market"
                        );
                        let removed = incumbent
                            .markets
                            .iter()
                            .find(|market| &market.market_id == removed_market)
                            .context("BTC 5m removed binding")?;
                        let added = profile
                            .markets
                            .iter()
                            .find(|market| &market.market_id == added_market)
                            .context("BTC 5m added binding")?;
                        let replacement = if installed == desired {
                            // The prior request may have lost its control reply
                            // after the upstream membership and lease CAS both
                            // completed but before the profile CAS.  Reconcile
                            // from exact lease state instead of replaying the
                            // non-idempotent wire replacement.
                            json!({"schema_version":"marketcow.acquisition-lease-replacement.v1",
                                "status":"complete","lease_id":lease_id,
                                "market_ids":desired,"reconciled_after_uncertain_response":true,
                                "maximum_token_budget_preserved":true})
                        } else {
                            ensure!(
                                installed == expected,
                                "BTC 5m manager lease incumbent differs"
                            );
                            send_acquisition(
                                acquisition,
                                AcquisitionAction::ReplaceLease {
                                    lease_id: lease_id.clone(),
                                    ttl_seconds: lease_ttl_seconds,
                                    expected_market_ids: expected.clone(),
                                    market_ids: desired.clone(),
                                    eligible_market_ids: available.clone(),
                                    removed_token_ids: [
                                        removed.outcomes[0].token_id.clone(),
                                        removed.outcomes[1].token_id.clone(),
                                    ],
                                    added_token_ids: [
                                        added.outcomes[0].token_id.clone(),
                                        added.outcomes[1].token_id.clone(),
                                    ],
                                },
                            )?
                        };
                        if replacement["status"] != "complete" {
                            return Ok(json!({"profile_published":false,
                                "profile_id":profile.profile_id,"profile_revision":profile.profile_revision,
                                "acquisition_membership":replacement}));
                        }
                        Some(replacement)
                    } else {
                        // Leases intentionally do not survive a process
                        // restart.  A stale startup profile must therefore be
                        // replaceable by the manager's freshly reviewed
                        // current/next/retiring set before acquisition starts.
                        // This is not a hot-rotation bypass: any existing
                        // manager lease above still requires the exact adjacent
                        // transition.
                        restart_reconciled = true;
                        Some(send_acquisition(
                            acquisition,
                            AcquisitionAction::AcquireLease {
                                lease_id: lease_id.clone(),
                                ttl_seconds: lease_ttl_seconds,
                                market_ids: desired.clone(),
                                eligible_market_ids: available.clone(),
                            },
                        )?)
                    }
                } else {
                    None
                };
                // The wire operation above may itself span the UTC boundary.
                // Recheck immediately before the profile CAS, and let the API
                // recheck once more while holding its mutation lock.
                let publish_attempt =
                    profile
                        .validate_current_at(chrono::Utc::now())
                        .and_then(|_| {
                            if restart_reconciled {
                                control.publish_restart_reconciled(
                                    &expected_profile_id,
                                    expected_revision,
                                    profile.clone(),
                                )
                            } else {
                                control.publish(
                                    &expected_profile_id,
                                    expected_revision,
                                    profile.clone(),
                                )
                            }
                        });
                let mut receipt = match publish_attempt {
                    Ok(receipt) => receipt,
                    Err(error) if lease_status["required"] == true => {
                        // Do not leave a component lease on a market set whose
                        // profile CAS was rejected. Releasing this one lease
                        // preserves every other consumer's union membership;
                        // the manager will perform fresh discovery and exact
                        // acquire on its next iteration.
                        let release = send_acquisition(
                            acquisition,
                            AcquisitionAction::ReleaseLease {
                                lease_id: lease_id.clone(),
                            },
                        );
                        let release_status = release
                            .as_ref()
                            .ok()
                            .and_then(|value| value["status"].as_str())
                            .unwrap_or("release_failed_or_unknown");
                        anyhow::bail!(
                            "BTC 5m publish crossed boundary after acquisition; component lease {release_status}; original: {error:#}"
                        );
                    }
                    Err(error) => return Err(error),
                };
                receipt["acquisition_membership"] = json!({
                    "mode":if lease_status["required"]==true {"component_lease_shared_union"} else {"always_on_shared_acquisition"},
                    "lease_id":if lease_status["required"]==true {Value::String(lease_id)} else {Value::Null},
                    "lease_receipt":lease_receipt,
                    "rotation_order":"remove_expired_pair_then_add_new_next_then_profile_cas",
                    "profile_published_after_wire_receipt":true
                });
                receipt["restart_reconciled"] = json!(restart_reconciled);
                return Ok(receipt);
            }
            Command::AcquisitionLeaseStatus => {
                return send_acquisition(
                    acquisition,
                    AcquisitionAction::LeaseStatus {
                        eligible_market_ids: self.referenced_markets_with_btc(btc_5m)?,
                    },
                );
            }
            Command::RetirementCandidates => {
                let journal = journal.as_ref().context("runtime journal unavailable")?;
                if journal.state.retirement_ticket.is_some() {
                    return Ok(json!({"market_ids":[],"pending_ticket":journal.state.retirement_ticket}));
                }
                let mut protected = self.referenced_markets_with_btc(btc_5m)?;
                if btc_5m.is_some() {
                    protected.extend(prepared_near_term_hot(&journal.state, chrono::Utc::now())?);
                }
                let status = send_acquisition(acquisition,
                    AcquisitionAction::LeaseStatus { eligible_market_ids: protected.clone() })?;
                protected.extend(lease_market_ids(&status)?);
                let market_ids = select_unreferenced_hot(&journal.state, &protected);
                return Ok(json!({"market_ids":market_ids,"pending_ticket":null}));
            }
            Command::AcquireAcquisitionLease {
                expected_scope_id,
                expected_revision,
                lease_id,
                ttl_seconds,
                market_ids,
            } => {
                self.check_incumbent(&expected_scope_id, expected_revision)?;
                ensure!(
                    !market_ids.is_empty()
                        && market_ids.len() <= 4096
                        && market_ids.windows(2).all(|p| p[0] < p[1]),
                    "sorted unique lease market ids required"
                );
                ensure!(valid_lease_id(&lease_id), "invalid acquisition lease id");
                return send_acquisition(
                    acquisition,
                    AcquisitionAction::AcquireLease {
                        lease_id,
                        ttl_seconds,
                        market_ids: market_ids.into_iter().collect(),
                        eligible_market_ids: self.referenced_markets_with_btc(btc_5m)?,
                    },
                );
            }
            Command::RenewAcquisitionLease {
                expected_scope_id,
                expected_revision,
                lease_id,
                ttl_seconds,
            } => {
                self.check_incumbent(&expected_scope_id, expected_revision)?;
                ensure!(valid_lease_id(&lease_id), "invalid acquisition lease id");
                return send_acquisition(
                    acquisition,
                    AcquisitionAction::RenewLease {
                        lease_id,
                        ttl_seconds,
                    },
                );
            }
            Command::RefreshBtc5mCurrent { lease_id } => {
                ensure!(valid_lease_id(&lease_id), "invalid acquisition lease id");
                let control = btc_5m.context("BTC 5m control unavailable")?;
                let profile = control.profile()?;
                profile.validate_current_at(chrono::Utc::now())?;
                let market_id = profile
                    .markets
                    .iter()
                    .find(|market| market.role == crate::source_btc5m_api::WindowRole::Current)
                    .context("BTC 5m current market unavailable")?
                    .market_id
                    .clone();
                return send_acquisition(
                    acquisition,
                    AcquisitionAction::RefreshLeaseMarkets {
                        lease_id,
                        market_ids: BTreeSet::from([market_id]),
                    },
                );
            }
            Command::ReplaceAcquisitionLease {
                expected_scope_id,
                expected_revision,
                lease_id,
                ttl_seconds,
                expected_market_ids,
                market_ids,
                removed_token_ids,
                added_token_ids,
            } => {
                self.check_incumbent(&expected_scope_id, expected_revision)?;
                ensure!(valid_lease_id(&lease_id), "invalid acquisition lease id");
                for ids in [&expected_market_ids, &market_ids] {
                    ensure!(
                        !ids.is_empty()
                            && ids.len() <= 4096
                            && ids.windows(2).all(|pair| pair[0] < pair[1]),
                        "sorted unique replacement market ids required"
                    );
                }
                let expected_market_ids: BTreeSet<_> = expected_market_ids.into_iter().collect();
                let market_ids: BTreeSet<_> = market_ids.into_iter().collect();
                let available = self.referenced_markets_with_btc(btc_5m)?;
                ensure!(
                    expected_market_ids.is_subset(&available)
                        && market_ids.is_subset(&available)
                        && expected_market_ids.difference(&market_ids).count() == 1
                        && market_ids.difference(&expected_market_ids).count() == 1,
                    "replacement component lease must rotate one eligible market"
                );
                return send_acquisition(
                    acquisition,
                    AcquisitionAction::ReplaceLease {
                        lease_id,
                        ttl_seconds,
                        expected_market_ids,
                        eligible_market_ids: available,
                        market_ids,
                        removed_token_ids,
                        added_token_ids,
                    },
                );
            }
            Command::ReleaseAcquisitionLease {
                expected_scope_id,
                expected_revision,
                lease_id,
            } => {
                self.check_incumbent(&expected_scope_id, expected_revision)?;
                ensure!(valid_lease_id(&lease_id), "invalid acquisition lease id");
                return send_acquisition(acquisition, AcquisitionAction::ReleaseLease { lease_id });
            }
            Command::RetireAcquisition {
                expected_scope_id,
                expected_revision,
                market_ids,
            } => {
                let active = self.status()?;
                let identity = if active["pool"] == "discovery" {
                    &active["projection_id"]
                } else {
                    &active["scope_id"]
                };
                ensure!(
                    identity == &expected_scope_id && active["revision"] == expected_revision,
                    "retirement incumbent conflict"
                );
                ensure!(
                    !market_ids.is_empty()
                        && market_ids.len() <= 4096
                        && market_ids.windows(2).all(|p| p[0] < p[1]),
                    "sorted unique retirement ids required"
                );
                let mut protected = self.referenced_markets_with_btc(btc_5m)?;
                if let (Some(journal), Some(_)) = (journal.as_ref(), btc_5m) {
                    protected.extend(prepared_near_term_hot(&journal.state, chrono::Utc::now())?);
                }
                // A different consumer may still share the market even after
                // this profile has rotated away. Never use only the active
                // scope/profile to decide whether its hot identity may retire.
                let lease_status = send_acquisition(
                    acquisition,
                    AcquisitionAction::LeaseStatus {
                        eligible_market_ids: protected.clone(),
                    },
                )?;
                protected.extend(lease_market_ids(&lease_status)?);
                ensure!(
                    market_ids.iter().all(|id| !protected.contains(id)),
                    "active or grace-period dependency still referenced"
                );
                let ids = market_ids.into_iter().collect();
                let journal_ticket = if journal.is_some() {
                    Some(active["retirement_submitted"]
                        .as_u64()
                        .context("retirement status")?
                        .checked_add(1)
                        .context("retirement overflow")?)
                } else {
                    None
                };
                let expected_revision = active["revision"].as_u64().context("retirement revision")?;
                let expected_catalog_revision = active["catalog_revision"]
                    .as_str().context("retirement catalog revision")?;
                let mut staged_this_operation = false;
                for validate_only in [true, false] {
                    if !validate_only {
                        if let (Some(journal), Some(ticket)) = (journal.as_mut(), journal_ticket) {
                            staged_this_operation = journal.state.retirement_ticket.is_none();
                            journal.retire(&ids, ticket)?;
                        }
                    }
                    let (receipt, done) = tokio::sync::oneshot::channel();
                    let enqueue = acquisition
                        .context("managed acquisition unavailable")?
                        .try_send(AcquisitionRequest {
                            action: AcquisitionAction::Scope {
                                validate_only,
                                catalog_revision: String::new(),
                                records: vec![],
                                evidence_sha256: String::new(),
                                acquisition_market_ids: Default::default(),
                                retire_market_ids: ids.clone(),
                            },
                            receipt,
                        });
                    if enqueue.is_err() {
                        // Nothing reached the serial owner. This is the other
                        // provably pre-effect rejection after intent staging.
                        if !validate_only && staged_this_operation {
                            if let (Some(journal), Some(ticket)) = (journal.as_mut(), journal_ticket) {
                                journal.cancel_unstarted_retirement(&ids, ticket,
                                    expected_revision, expected_catalog_revision)?;
                            }
                        }
                        anyhow::bail!("acquisition command busy or stopped");
                    }
                    let result = done
                        .blocking_recv()
                        .context("acquisition owner stopped")??;
                    if !validate_only {
                        if let Some(reason) = result["retirement_rejected_before_apply"].as_str() {
                            ensure!(matches!(reason, "active_lease" | "wire_retirement_preflight"),
                                "unknown retirement rejection; keep durable intent pending");
                            if staged_this_operation {
                                if let (Some(journal), Some(ticket)) = (journal.as_mut(), journal_ticket) {
                                    journal.cancel_unstarted_retirement(&ids, ticket,
                                        expected_revision, expected_catalog_revision)?;
                                }
                            }
                            anyhow::bail!("retirement rejected before apply: {reason}");
                        }
                        return Ok(result);
                    }
                }
                unreachable!();
            }
            Command::PrepareAcquisition {
                expected_scope_id,
                expected_revision,
                catalog_revision,
                records,
                evidence_sha256,
                acquisition_market_ids,
            } => {
                let active = self.status()?;
                let identity = if active["pool"] == "discovery" {
                    &active["projection_id"]
                } else {
                    &active["scope_id"]
                };
                ensure!(
                    identity == &expected_scope_id
                        && active["revision"] == expected_revision
                        && active["catalog_revision"] == catalog_revision,
                    "acquisition incumbent conflict"
                );
                ensure!(
                    acquisition_market_ids.windows(2).all(|p| p[0] < p[1])
                        && acquisition_market_ids.iter().all(|s| !s.is_empty()),
                    "sorted unique acquisition ids required"
                );
                let ids = acquisition_market_ids.into_iter().collect();
                for validate_only in [true, false] {
                    if !validate_only {
                        if let Some(journal) = journal.as_mut() {
                            journal.admit(&records, &ids)?;
                        }
                    }
                    let (receipt, done) = tokio::sync::oneshot::channel();
                    acquisition
                        .context("managed acquisition unavailable")?
                        .try_send(AcquisitionRequest {
                            action: AcquisitionAction::Scope {
                                validate_only,
                                catalog_revision: catalog_revision.clone(),
                                records: records.clone(),
                                evidence_sha256: evidence_sha256.clone(),
                                acquisition_market_ids: ids.clone(),
                                retire_market_ids: Default::default(),
                            },
                            receipt,
                        })
                        .map_err(|_| anyhow::anyhow!("acquisition command busy or stopped"))?;
                    let result = done
                        .blocking_recv()
                        .context("acquisition owner stopped")??;
                    if !validate_only {
                        return Ok(result);
                    }
                }
                unreachable!();
            }
            Command::PreparePublication {
                expected_scope_id,
                expected_revision,
                config,
            } => (expected_scope_id, expected_revision, config, false),
            Command::PublishScope {
                expected_scope_id,
                expected_revision,
                config,
            } => (expected_scope_id, expected_revision, config, true),
        };
        ensure!(
            !expected.is_empty() && expected.len() <= 256 && revision > 0,
            "control identity"
        );
        // Durable control intent precedes publication. A crash without a
        // response is reconciled to this intent before reopening listeners.
        match self {
            Self::Live(control) => control.prepare(
                &expected,
                revision,
                &serde_json::from_value::<ConfiguredScope>(config.clone())?,
            )?,
            Self::Discovery(control) => control.prepare(
                &expected,
                revision,
                &serde_json::from_value::<DiscoveryConfig>(config.clone())?,
            )?,
        }
        let previous = journal.as_ref().map(|j| j.state.clone());
        if publish {
            if let Some(journal) = journal.as_mut() {
                let mut next = journal.state.clone();
                next.active_config = config.clone();
                next.active_revision =
                    revision.checked_add(1).context("scope revision overflow")?;
                journal.commit(next)?;
            }
        }
        let applied = (|| -> Result<()> {
            match self {
                Self::Live(control) => {
                    let scope: ConfiguredScope = serde_json::from_value(config)?;
                    if publish {
                        control.activate(&expected, revision, scope)?;
                    } else {
                        control.prepare(&expected, revision, &scope)?;
                    }
                }
                Self::Discovery(control) => {
                    let scope: DiscoveryConfig = serde_json::from_value(config)?;
                    if publish {
                        control.activate(&expected, revision, Arc::new(scope))?;
                    } else {
                        control.prepare(&expected, revision, &scope)?;
                    }
                }
            }
            Ok(())
        })();
        if let Err(error) = applied {
            if let (Some(journal), Some(previous)) = (journal.as_mut(), previous) {
                journal.commit(previous)?;
            }
            return Err(error);
        }
        Ok(
            json!({"publication_applied":publish,"actual":self.status()?,
            "acquisition_prepared_by_this_operation":false}),
        )
    }
}

impl Backend {
    fn referenced_markets(&self) -> Result<std::collections::BTreeSet<String>> {
        match self {
            Self::Live(control) => control.referenced_markets(),
            Self::Discovery(control) => control.referenced_markets(),
        }
    }
    fn referenced_markets_with_btc(
        &self,
        btc_5m: Option<&crate::source_btc5m_api::Control>,
    ) -> Result<std::collections::BTreeSet<String>> {
        let mut markets = self.referenced_markets()?;
        if let Some(control) = btc_5m {
            markets.extend(control.referenced_markets()?);
        }
        Ok(markets)
    }
    fn check_incumbent(&self, expected: &str, revision: u64) -> Result<()> {
        let active = self.status()?;
        let identity = if active["pool"] == "discovery" {
            &active["projection_id"]
        } else {
            &active["scope_id"]
        };
        ensure!(
            identity == expected && active["revision"] == revision,
            "acquisition lease incumbent conflict"
        );
        Ok(())
    }
}
fn lease_market_ids(status: &Value) -> Result<std::collections::BTreeSet<String>> {
    ensure!(status["schema_version"] == "marketcow.acquisition-lease-status.v1",
        "acquisition lease status schema");
    let mut ids = std::collections::BTreeSet::new();
    for lease in status["leases"].as_array().context("acquisition leases")? {
        for market in lease["market_ids"].as_array().context("lease market ids")? {
            ids.insert(market.as_str().context("lease market id")?.to_owned());
        }
    }
    Ok(ids)
}
fn valid_lease_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b':'))
}
fn send_acquisition(
    sender: Option<&AcquisitionSender>,
    action: AcquisitionAction,
) -> Result<Value> {
    let (receipt, done) = tokio::sync::oneshot::channel();
    sender
        .context("managed acquisition unavailable")?
        .try_send(AcquisitionRequest { action, receipt })
        .map_err(|_| anyhow::anyhow!("acquisition command busy or stopped"))?;
    done.blocking_recv().context("acquisition owner stopped")?
}

struct SocketOwner {
    path: PathBuf,
    device: u64,
    inode: u64,
}
impl Drop for SocketOwner {
    fn drop(&mut self) {
        if let Ok(meta) = std::fs::symlink_metadata(&self.path) {
            if meta.file_type().is_socket() && meta.dev() == self.device && meta.ino() == self.inode
            {
                let _ = std::fs::remove_file(&self.path);
            }
        }
    }
}

pub fn start(
    path: &Path,
    maximum_bytes: usize,
    timeout: Duration,
    backend: Backend,
    acquisition: Option<AcquisitionSender>,
    btc_5m: Option<crate::source_btc5m_api::Control>,
    journal: Option<crate::source_scope_journal::Journal>,
) -> Result<tokio::task::JoinHandle<Result<()>>> {
    ensure!(
        path.is_absolute()
            && (1024..=16 * 1024 * 1024).contains(&maximum_bytes)
            && !timeout.is_zero()
            && timeout <= Duration::from_secs(30),
        "explicit control budgets/path required"
    );
    let parent = path.parent().context("control directory")?;
    let metadata = std::fs::symlink_metadata(parent)?;
    ensure!(
        metadata.is_dir()
            && metadata.permissions().mode() & 0o077 == 0
            && std::fs::canonicalize(parent)? == parent,
        "control directory must be private and canonical"
    );
    // Never unlink an existing socket on startup: it may have a live owner.
    let listener = tokio::net::UnixListener::bind(path)?;
    let metadata = std::fs::symlink_metadata(path)?;
    let owner = SocketOwner {
        path: path.to_owned(),
        device: metadata.dev(),
        inode: metadata.ino(),
    };
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    let backend = Arc::new(Mutex::new((backend, journal)));
    Ok(tokio::spawn(async move {
        let _owner = owner;
        loop {
            let (mut socket, _) = listener.accept().await?;
            let body = tokio::time::timeout(timeout, async {
                let length = socket.read_u32().await? as usize;
                ensure!(
                    length > 0 && length <= maximum_bytes,
                    "control request byte budget"
                );
                let mut body = vec![0; length];
                socket.read_exact(&mut body).await?;
                Ok::<_, anyhow::Error>(body)
            })
            .await;
            let result = match body {
                Ok(Ok(body)) => {
                    let backend = backend.clone();
                    let acquisition = acquisition.clone();
                    let btc_5m = btc_5m.clone();
                    // One command at a time, off the async ingestion loop.
                    // Do not abandon a running CAS if the caller disconnects.
                    tokio::task::spawn_blocking(move || {
                        let mut owner = backend
                            .lock()
                            .map_err(|_| anyhow::anyhow!("control owner poisoned"))?;
                        let (backend, journal) = &mut *owner;
                        backend.execute(
                            &body,
                            acquisition.as_ref(),
                            btc_5m.as_ref(),
                            journal.as_mut(),
                        )
                    })
                    .await?
                }
                Ok(Err(error)) => Err(error),
                Err(_) => Err(anyhow::anyhow!("control read deadline")),
            };
            let response = match result {
                Ok(value) => json!({"ok":true,"result":value}),
                Err(error) => json!({"ok":false,"error":error.to_string()}),
            };
            let bytes = serde_json::to_vec(&response)?;
            if bytes.len() > maximum_bytes {
                continue;
            }
            let _ = tokio::time::timeout(timeout, async {
                socket.write_u32(bytes.len() as u32).await?;
                socket.write_all(&bytes).await?;
                socket.shutdown().await
            })
            .await;
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn retirement_selection_is_bounded_and_never_evicts_shared_hot_demand() {
        let state = crate::source_scope_journal::State {
            schema_version:"marketcow.runtime-scope-state.v1".into(),
            catalog_revision:"catalog".into(), catalog_manifest_sha256:"hash".into(),
            pool:"live".into(), active_config:json!({}), active_revision:1,
            catalog_source:json!({}), records:Default::default(),
            acquisition_market_ids:(0..100).map(|n|format!("{n:03}")).collect(),
            retiring_records:Default::default(), retirement_ticket:None,
            retiring_acquisition_market_ids:None,
        };
        let protected = BTreeSet::from(["000".into(), "020".into(), "099".into()]);
        let first = select_unreferenced_hot(&state, &protected);
        assert_eq!(first.len(), 16);
        assert_eq!(first[0], "001");
        assert!(first.iter().all(|id| !protected.contains(id)));
        assert!(select_unreferenced_hot(&state, &state.acquisition_market_ids).is_empty());
    }
    #[test]
    fn prepared_next_is_protected_before_cas_but_abandoned_hot_record_expires() {
        use chrono::TimeZone;
        let record = |start: &str, end: &str| json!({
            "start_at":start,"end_at":end,
            "identity":{"outcomes":[{"outcome":"Up"},{"outcome":"Down"}]}
        });
        let state = crate::source_scope_journal::State {
            schema_version:"marketcow.runtime-scope-state.v1".into(),
            catalog_revision:"catalog".into(), catalog_manifest_sha256:"hash".into(),
            pool:"live".into(),active_config:json!({}),active_revision:1,
            catalog_source:json!({}),
            records:BTreeMap::from([
                ("prepared".into(),record("2026-10-02T22:15:00Z","2026-10-02T22:20:00Z")),
                ("expired".into(),record("2026-10-02T22:05:00Z","2026-10-02T22:10:00Z")),
                ("distant".into(),record("2026-10-02T23:00:00Z","2026-10-02T23:05:00Z")),
            ]),
            acquisition_market_ids:["prepared".into(),"expired".into(),"distant".into()].into(),
            retiring_records:Default::default(),retirement_ticket:None,
            retiring_acquisition_market_ids:None,
        };
        let at = chrono::Utc.with_ymd_and_hms(2026,10,2,22,10,2).unwrap();
        let protected = prepared_near_term_hot(&state,at).unwrap();
        assert_eq!(protected,BTreeSet::from(["prepared".into()]));
        assert!(!select_unreferenced_hot(&state,&protected).contains(&"prepared".into()));
        let after = chrono::Utc.with_ymd_and_hms(2026,10,2,22,20,1).unwrap();
        assert!(!prepared_near_term_hot(&state,after).unwrap().contains("prepared"));
        assert!(select_unreferenced_hot(&state,&prepared_near_term_hot(&state,after).unwrap())
            .contains(&"prepared".into()));
    }
    #[tokio::test]
    async fn prepared_next_survives_candidate_and_retirement_control_until_profile_cas() {
        let start = chrono::Utc::now().timestamp().div_euclid(300) * 300;
        let bindings = [
            btc_binding(WindowRole::Current,"m0",'1',"101","102",start-300),
            btc_binding(WindowRole::Next,"m1",'2',"201","202",start),
            btc_binding(WindowRole::Next,"m2",'3',"301","302",start+300),
        ];
        let record = |index:usize| {
            let binding = &bindings[index];
            let mut value = btc_market(&binding.market_id,
                ['1','2','3'][index],&binding.outcomes[0].token_id,&binding.outcomes[1].token_id);
            value["start_at"] = json!(binding.start_at);
            value["end_at"] = json!(binding.end_at);
            value
        };
        let records = [record(0),record(1),record(2)];
        let publication = Publication::start(0,
            Some(json!({"latest_cursor":0,"books":[],"markets":records,
                "gaps":[],"catalog_revision":"b".repeat(64)})),
            BTreeMap::from([("101".into(),"m0".into()),("102".into(),"m0".into()),
                ("201".into(),"m1".into()),("202".into(),"m1".into()),
                ("301".into(),"m2".into()),("302".into(),"m2".into())]),
            8,131072,65536,|_|Ok(())).unwrap();
        let (_,discovery) = router_managed(publication.reader(),
            Arc::new(serde_json::from_value(config("a","c")).unwrap()),
            DiscoveryLimits {full_sync_bytes:65536,frame_bytes:65536,
                state_bytes:131072,replay_bytes:65536,clients:2,
                cached_baselines:2,send_timeout:Duration::from_secs(1)},
            Duration::from_millis(50)).unwrap();
        let incumbent = btc_profile(1,vec![bindings[0].clone(),bindings[1].clone()]);
        let candidate = btc_profile(2,vec![
            MarketBinding {role:WindowRole::Current,..bindings[1].clone()},bindings[2].clone()]);
        let (_,btc) = crate::source_btc5m_api::Api::router(publication.reader(),incumbent.clone(),
            "epoch-test".into(),Btc5mLimits {snapshot_bytes:65536,frame_bytes:65536,
                replay_bytes:65536,clients:1,send_timeout:Duration::from_secs(1),
                snapshot_lease_capacity:1}).unwrap();
        let directory = std::env::temp_dir().canonicalize().unwrap()
            .join(format!("mc-prepared-next-{}",uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        std::fs::set_permissions(&directory,std::fs::Permissions::from_mode(0o700)).unwrap();
        let path = directory.join("state.json");
        let initial = crate::source_scope_journal::State {
            schema_version:"marketcow.runtime-scope-state.v1".into(),
            catalog_revision:"b".repeat(64),catalog_manifest_sha256:"m".into(),
            pool:"discovery".into(),active_config:config("a","c"),active_revision:1,
            catalog_source:Value::Null,
            records:BTreeMap::from([("m0".into(),records[0].clone()),
                ("m1".into(),records[1].clone())]),
            acquisition_market_ids:["m0".into(),"m1".into()].into(),
            retiring_records:Default::default(),retirement_ticket:None,
            retiring_acquisition_market_ids:None,
        };
        let journal = crate::source_scope_journal::Journal::open(path.clone(),65536,initial).unwrap();
        let (send,mut receive) = tokio::sync::mpsc::channel::<AcquisitionRequest>(1);
        let responder = tokio::spawn(async move {
            for call in 0..4 {
                let request = receive.recv().await.unwrap();
                let value = match call {
                    0|1 => {
                        assert!(matches!(request.action,AcquisitionAction::Scope{retire_market_ids,
                            ..} if retire_market_ids.is_empty()));
                        json!({"acquisition_prepared":true})
                    },
                    _ => {
                        assert!(matches!(request.action,AcquisitionAction::LeaseStatus{..}));
                        json!({"schema_version":"marketcow.acquisition-lease-status.v1","leases":[]})
                    },
                };
                request.receipt.send(Ok(value)).unwrap();
            }
        });
        let prepared_records = vec![records[1].clone(),records[2].clone()];
        let prepare = serde_json::to_vec(&json!({"operation":"prepare_acquisition",
            "expected_scope_id":"a".repeat(64),"expected_revision":1,
            "catalog_revision":"b".repeat(64),"records":prepared_records,
            "evidence_sha256":"e".repeat(64),
            "acquisition_market_ids":["m1","m2"]})).unwrap();
        let candidates = serde_json::to_vec(&json!({"operation":"retirement_candidates"})).unwrap();
        let retire = serde_json::to_vec(&json!({"operation":"retire_acquisition",
            "expected_scope_id":"a".repeat(64),"expected_revision":1,
            "market_ids":["m2"]})).unwrap();
        let (result,journal) = tokio::task::spawn_blocking(move || {
            let backend = Backend::Discovery(discovery);
            let mut journal = journal;
            backend.execute(&prepare,Some(&send),Some(&btc),Some(&mut journal)).unwrap();
            let selected = backend.execute(&candidates,Some(&send),Some(&btc),Some(&mut journal)).unwrap();
            assert!(!selected["market_ids"].as_array().unwrap().contains(&json!("m2")));
            let error = backend.execute(&retire,Some(&send),Some(&btc),Some(&mut journal))
                .unwrap_err();
            assert!(error.to_string().contains("still referenced"),"{error:#}");
            let published = btc.publish(&incumbent.profile_id,incumbent.profile_revision,candidate);
            (published,journal)
        }).await.unwrap();
        assert!(result.is_ok());
        responder.await.unwrap();
        assert_eq!(journal.state.retirement_ticket,None);
        assert!(journal.state.records.contains_key("m2"));
        std::fs::remove_file(path).unwrap();
        std::fs::remove_dir(directory).unwrap();
        publication.finish().await.unwrap();
    }
    #[test]
    fn retirement_protection_unions_all_component_leases() {
        let status = json!({"schema_version":"marketcow.acquisition-lease-status.v1",
            "leases":[{"lease_id":"btc","market_ids":["1","2"]},
                      {"lease_id":"shared","market_ids":["2","3"]}]});
        assert_eq!(lease_market_ids(&status).unwrap(),
            std::collections::BTreeSet::from(["1".into(), "2".into(), "3".into()]));
        assert!(lease_market_ids(&json!({"schema_version":"other","leases":[]})).is_err());
    }
    #[tokio::test]
    async fn late_owner_lease_conflict_cancels_only_unstarted_retirement_intent() {
        let record = |id: &str, up: &str, down: &str| json!({
            "identity":{"market_id":id,"condition_id":format!("condition-{id}"),
                "event_id":"e","neg_risk":false,"outcomes":[
                    {"outcome":"Yes","token_id":up},{"outcome":"No","token_id":down}]},
            "metadata_revision":"d".repeat(64),"active":true,"closed":false,
            "accepting_orders":true,"lifecycle_state":"active","start_at":null,"end_at":null,
            "relations":[],"rules":{"instrument":{"price_increment":"0.01",
                "minimum_order_size":"5"},"fee_schedule":{"complete":true,"schedule_id":"fee"}}
        });
        let first = record("1", "11", "12");
        let second = record("2", "21", "22");
        let publication = Publication::start(0,
            Some(json!({"latest_cursor":0,"books":[],"markets":[first.clone(),second.clone()],
                "gaps":[],"catalog_revision":"b".repeat(64)})),
            BTreeMap::from([("11".into(),"1".into()),("12".into(),"1".into()),
                ("21".into(),"2".into()),("22".into(),"2".into())]),
            8, 131072, 65536, |_| Ok(())).unwrap();
        let (_, control) = router_managed(publication.reader(),
            Arc::new(serde_json::from_value(config("a", "c")).unwrap()),
            DiscoveryLimits { full_sync_bytes:65536, frame_bytes:65536,
                state_bytes:131072, replay_bytes:65536, clients:2,
                cached_baselines:2, send_timeout:Duration::from_secs(1) },
            Duration::from_millis(50)).unwrap();
        let directory = std::env::temp_dir().canonicalize().unwrap()
            .join(format!("mc-retire-race-{}",uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        std::fs::set_permissions(&directory,std::fs::Permissions::from_mode(0o700)).unwrap();
        let path = directory.join("state.json");
        let initial = crate::source_scope_journal::State {
            schema_version:"marketcow.runtime-scope-state.v1".into(),
            catalog_revision:"b".repeat(64),catalog_manifest_sha256:"m".into(),
            pool:"discovery".into(), active_config:config("a","c"),
            active_revision:1,catalog_source:Value::Null,
            records:BTreeMap::from([("1".into(),first),("2".into(),second)]),
            acquisition_market_ids:["1".into(),"2".into()].into(),
            retiring_records:Default::default(),retirement_ticket:None,
            retiring_acquisition_market_ids:Default::default(),
        };
        let journal = crate::source_scope_journal::Journal::open(path.clone(),65536,initial).unwrap();
        let (send,mut receive) = tokio::sync::mpsc::channel::<AcquisitionRequest>(1);
        let responder = tokio::spawn(async move {
            let mut calls = 0;
            while let Some(request) = receive.recv().await {
                calls += 1;
                let value = match request.action {
                    AcquisitionAction::LeaseStatus{..} => json!({
                        "schema_version":"marketcow.acquisition-lease-status.v1","leases":[]}),
                    AcquisitionAction::Scope{validate_only:true,..} =>
                        json!({"retirement_validated":true,"publication_applied":false}),
                    AcquisitionAction::Scope{validate_only:false,..} =>
                        json!({"retirement_rejected_before_apply":"active_lease"}),
                    _ => panic!("unexpected acquisition action"),
                };
                request.receipt.send(Ok(value)).unwrap();
                if calls == 3 { break; }
            }
            assert_eq!(calls,3);
        });
        let body = serde_json::to_vec(&json!({"operation":"retire_acquisition",
            "expected_scope_id":"a".repeat(64),"expected_revision":1,"market_ids":["2"]})).unwrap();
        let (result,journal,backend) = tokio::task::spawn_blocking(move || {
            let mut journal = journal;
            let backend = Backend::Discovery(control);
            let result = backend.execute(&body,Some(&send),None,Some(&mut journal));
            (result,journal,backend)
        }).await.unwrap();
        assert!(result.unwrap_err().to_string().contains("active_lease"));
        responder.await.unwrap();
        assert!(journal.state.retiring_records.is_empty());
        assert_eq!(journal.state.retirement_ticket,None);
        assert_eq!(journal.state.acquisition_market_ids,
            BTreeSet::from(["1".into(),"2".into()]));
        assert_eq!(crate::source_scope_journal::read(&path,65536).unwrap().unwrap().records.len(),2);
        // The real shard can reject because a prior retirement command is
        // still in flight. Its structured pre-effect receipt must also undo
        // this attempt's durable journal intent, so preparation can retry.
        let (send,mut receive) = tokio::sync::mpsc::channel::<AcquisitionRequest>(1);
        let responder = tokio::spawn(async move {
            for call in 0..3 {
                let request = receive.recv().await.unwrap();
                let value = match call {
                    0 => json!({"schema_version":"marketcow.acquisition-lease-status.v1","leases":[]}),
                    1 => json!({"retirement_validated":true,"publication_applied":false}),
                    _ => json!({"retirement_rejected_before_apply":"wire_retirement_preflight"}),
                };
                request.receipt.send(Ok(value)).unwrap();
            }
        });
        let body = serde_json::to_vec(&json!({"operation":"retire_acquisition",
            "expected_scope_id":"a".repeat(64),"expected_revision":1,"market_ids":["2"]})).unwrap();
        let (result,next_journal,next_backend) = tokio::task::spawn_blocking(move || {
            let mut journal = journal;
            let result = backend.execute(&body,Some(&send),None,Some(&mut journal));
            (result,journal,backend)
        }).await.unwrap();
        assert!(result.unwrap_err().to_string().contains("wire_retirement_preflight"));
        responder.await.unwrap();
        let mut journal = next_journal;
        let backend = next_backend;
        assert_eq!(journal.state.retirement_ticket,None);
        assert!(journal.state.retiring_records.is_empty());
        assert_eq!(journal.state.acquisition_market_ids,
            BTreeSet::from(["1".into(),"2".into()]));
        assert_eq!(crate::source_scope_journal::read(&path,65536).unwrap().unwrap().retirement_ticket,None);
        // An earlier intent with the same ticket and market must not be
        // cancelled merely because this retry cannot enqueue its apply call.
        let retired = BTreeSet::from(["2".into()]);
        journal.retire(&retired,1).unwrap();
        let (send,mut receive) = tokio::sync::mpsc::channel::<AcquisitionRequest>(1);
        let responder = tokio::spawn(async move {
            for call in 0..2 {
                let request = receive.recv().await.unwrap();
                let value = if call == 0 {
                    assert!(matches!(request.action,AcquisitionAction::LeaseStatus{..}));
                    json!({"schema_version":"marketcow.acquisition-lease-status.v1","leases":[]})
                } else {
                    assert!(matches!(request.action,AcquisitionAction::Scope{validate_only:true,..}));
                    receive.close(); // apply try_send cannot enter the owner
                    json!({"retirement_validated":true,"publication_applied":false})
                };
                request.receipt.send(Ok(value)).unwrap();
            }
        });
        let body = serde_json::to_vec(&json!({"operation":"retire_acquisition",
            "expected_scope_id":"a".repeat(64),"expected_revision":1,"market_ids":["2"]})).unwrap();
        let (result,journal,backend) = tokio::task::spawn_blocking(move || {
            let result = backend.execute(&body,Some(&send),None,Some(&mut journal));
            (result,journal,backend)
        }).await.unwrap();
        assert!(result.unwrap_err().to_string().contains("busy or stopped"));
        responder.await.unwrap();
        assert_eq!(journal.state.retirement_ticket,Some(1));
        assert_eq!(journal.state.retiring_records.len(),1);
        assert_eq!(crate::source_scope_journal::read(&path,65536).unwrap().unwrap().retirement_ticket,
            Some(1));
        // A pre-existing ticket is not this attempt's intent. Even a fresh
        // pre-effect rejection cannot prove that its earlier owner call had
        // no side effects, so restart/retry must preserve the durable ticket.
        let (send,mut receive) = tokio::sync::mpsc::channel::<AcquisitionRequest>(1);
        let responder = tokio::spawn(async move {
            for call in 0..3 {
                let request = receive.recv().await.unwrap();
                let value = match call {
                    0 => json!({"schema_version":"marketcow.acquisition-lease-status.v1","leases":[]}),
                    1 => json!({"retirement_validated":true,"publication_applied":false}),
                    _ => json!({"retirement_rejected_before_apply":"wire_retirement_preflight"}),
                };
                request.receipt.send(Ok(value)).unwrap();
            }
        });
        let body = serde_json::to_vec(&json!({"operation":"retire_acquisition",
            "expected_scope_id":"a".repeat(64),"expected_revision":1,"market_ids":["2"]})).unwrap();
        let (result,journal) = tokio::task::spawn_blocking(move || {
            let mut journal = journal;
            let result = backend.execute(&body,Some(&send),None,Some(&mut journal));
            (result,journal)
        }).await.unwrap();
        assert!(result.unwrap_err().to_string().contains("wire_retirement_preflight"));
        responder.await.unwrap();
        assert_eq!(journal.state.retirement_ticket,Some(1));
        assert_eq!(crate::source_scope_journal::read(&path,65536).unwrap().unwrap().retirement_ticket,
            Some(1));
        std::fs::remove_file(path).unwrap();
        std::fs::remove_dir(directory).unwrap();
        publication.finish().await.unwrap();
    }
    use crate::{
        source_btc5m_api::{
            Limits as Btc5mLimits, MarketBinding, OutcomeBinding, Profile, WindowRole,
        },
        source_discovery_api::{DiscoveryLimits, router_managed},
        source_publication::Publication,
    };
    use chrono::{SecondsFormat, TimeZone};
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn config(id: &str, universe: &str) -> Value {
        json!({"projection_id":id.repeat(64),
        "catalog_revision":"b".repeat(64),"universe_revision":universe.repeat(64),
        "market_ids":["1"],"relations":[],"settlements":{"1":null},
        "policy":{"quantities":["10"],"maximum_book_age_ms":5000}})
    }
    async fn command(path: &Path, body: Vec<u8>) -> Value {
        let mut socket = tokio::net::UnixStream::connect(path).await.unwrap();
        socket.write_u32(body.len() as u32).await.unwrap();
        socket.write_all(&body).await.unwrap();
        let n = socket.read_u32().await.unwrap();
        assert!(n <= 65536);
        let mut response = vec![0; n as usize];
        socket.read_exact(&mut response).await.unwrap();
        serde_json::from_slice(&response).unwrap()
    }
    #[tokio::test]
    async fn private_socket_reports_actual_cas_and_releases_its_path() {
        // Synthetic source; exercises actual Unix framing, not an upstream load claim.
        let market = json!({"identity":{"market_id":"1","condition_id":"condition","event_id":"e","neg_risk":false,
            "outcomes":[{"outcome":"Yes","token_id":"11"},{"outcome":"No","token_id":"12"}]},
            "metadata_revision":"d".repeat(64),"active":true,"closed":false,"accepting_orders":true,"lifecycle_state":"active",
            "start_at":null,"end_at":null,"relations":[],"rules":{"instrument":{"price_increment":"0.01","minimum_order_size":"5"},
            "fee_schedule":{"complete":true,"schedule_id":"fee"}}});
        let p = Publication::start(
            0,
            Some(json!({"latest_cursor":0,"books":[],"markets":[market],
            "gaps":[],"catalog_revision":"b".repeat(64)})),
            BTreeMap::from([("11".into(), "1".into()), ("12".into(), "1".into())]),
            8,
            131072,
            65536,
            |_| Ok(()),
        )
        .unwrap();
        let (_, control) = router_managed(
            p.reader(),
            Arc::new(serde_json::from_value(config("a", "c")).unwrap()),
            DiscoveryLimits {
                full_sync_bytes: 65536,
                frame_bytes: 65536,
                state_bytes: 131072,
                replay_bytes: 65536,
                clients: 2,
                cached_baselines: 2,
                send_timeout: Duration::from_secs(1),
            },
            Duration::from_millis(50),
        )
        .unwrap();
        // Unix-domain socket paths have a small platform limit. Keep the test
        // path independent of a potentially very long Cargo worktree/TMPDIR.
        let short_tmp = if cfg!(target_os = "macos") {
            "/private/tmp"
        } else {
            "/tmp"
        };
        let directory =
            PathBuf::from(short_tmp).join(format!("mc-control-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).unwrap();
        let path = directory.join("s");
        let (send, mut receive) = tokio::sync::mpsc::channel::<AcquisitionRequest>(1);
        let responder = tokio::spawn(async move {
            let request = receive.recv().await.unwrap();
            assert!(matches!(
                request.action,
                AcquisitionAction::AcquireLease {
                    ttl_seconds: 30,
                    ..
                }
            ));
            request
                .receipt
                .send(Ok(json!({"source_ready":false})))
                .unwrap();
        });
        let task = start(
            &path,
            65536,
            Duration::from_secs(1),
            Backend::Discovery(control),
            Some(send),
            None,
            None,
        )
        .unwrap();
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        let status = command(&path, br#"{"operation":"status"}"#.to_vec()).await;
        assert_eq!(status["result"]["projection_id"], "a".repeat(64));
        let lease = json!({"operation":"acquire_acquisition_lease","expected_scope_id":"a".repeat(64),
            "expected_revision":1,"lease_id":"paper:1","ttl_seconds":30,"market_ids":["1"]});
        assert_eq!(
            command(&path, serde_json::to_vec(&lease).unwrap()).await["result"]["source_ready"],
            false
        );
        responder.await.unwrap();
        let rejected = command(
            &path,
            br#"{"operation":"status","operation":"status"}"#.to_vec(),
        )
        .await;
        assert_eq!(rejected["ok"], false);
        let mut request = json!({"operation":"prepare_publication","expected_scope_id":"a".repeat(64),
            "expected_revision":1,"config":config("e","f")});
        let prepared = command(&path, serde_json::to_vec(&request).unwrap()).await;
        assert_eq!(prepared["ok"], true);
        assert_eq!(
            prepared["result"]["actual"]["projection_id"],
            "a".repeat(64)
        );
        assert_eq!(
            prepared["result"]["acquisition_prepared_by_this_operation"],
            false
        );
        request["operation"] = json!("publish_scope");
        let published = command(&path, serde_json::to_vec(&request).unwrap()).await;
        assert_eq!(
            published["result"]["actual"]["projection_id"],
            "e".repeat(64)
        );
        assert_eq!(published["result"]["actual"]["revision"], 2);
        let stale = command(&path, serde_json::to_vec(&request).unwrap()).await;
        assert_eq!(stale["ok"], false);
        task.abort();
        let _ = task.await;
        assert!(!path.exists());
        std::fs::remove_dir(&directory).unwrap();
        p.finish().await.unwrap();
    }

    fn btc_market(id: &str, condition: char, up: &str, down: &str) -> Value {
        json!({"identity":{"market_id":id,"condition_id":format!("0x{}",condition.to_string().repeat(64)),
            "event_id":format!("event-{id}"),"neg_risk":false,
            "outcomes":[{"outcome":"Up","token_id":up},{"outcome":"Down","token_id":down}]},
            "metadata_revision":"d".repeat(64),"active":true,"closed":false,"accepting_orders":true,
            "lifecycle_state":"active","start_at":null,"end_at":null,"relations":[],
            "rules":{"instrument":{"price_increment":"0.01","minimum_order_size":"5"},
            "fee_schedule":{"complete":true,"schedule_id":"fee"}}})
    }

    fn btc_binding(
        role: WindowRole,
        id: &str,
        condition: char,
        up: &str,
        down: &str,
        start: i64,
    ) -> MarketBinding {
        let at = |seconds| {
            chrono::Utc
                .timestamp_opt(seconds, 0)
                .single()
                .unwrap()
                .to_rfc3339_opts(SecondsFormat::Secs, true)
        };
        MarketBinding {
            role,
            window_id: format!("{}/{}", at(start), at(start + 300)),
            start_at: at(start),
            end_at: at(start + 300),
            market_id: id.into(),
            condition_id: format!("0x{}", condition.to_string().repeat(64)),
            outcomes: [
                OutcomeBinding {
                    outcome: "Up".into(),
                    token_id: up.into(),
                },
                OutcomeBinding {
                    outcome: "Down".into(),
                    token_id: down.into(),
                },
            ],
            rule_sha256: condition.to_string().repeat(64),
        }
    }

    fn btc_profile(revision: u64, markets: Vec<MarketBinding>) -> Profile {
        let mut profile = Profile {
            schema_version: "marketcow.polymarket.btc-5m-profile.v1".into(),
            profile_id: String::new(),
            profile_revision: revision,
            catalog_revision: "b".repeat(64),
            markets,
            maximum_markets: 3,
            maximum_quote_age_ms: 5_000,
            maximum_tokens: 6,
        };
        profile.profile_id = profile.expected_id();
        profile
    }

    #[tokio::test]
    async fn slow_lease_receipt_crossing_window_rejects_profile_and_releases_component_lease() {
        let base = btc_market("1", '9', "11", "12");
        let markets = [
            btc_market("m1", '1', "101", "102"),
            btc_market("m2", '2', "201", "202"),
            btc_market("m3", '3', "301", "302"),
        ];
        let publication = Publication::start(
            0,
            Some(json!({"latest_cursor":0,"books":[],
                "markets":[base,markets[0].clone(),markets[1].clone(),markets[2].clone()],
                "gaps":[],"catalog_revision":"b".repeat(64)})),
            BTreeMap::from([
                ("11".into(), "1".into()),
                ("12".into(), "1".into()),
                ("101".into(), "m1".into()),
                ("102".into(), "m1".into()),
                ("201".into(), "m2".into()),
                ("202".into(), "m2".into()),
                ("301".into(), "m3".into()),
                ("302".into(), "m3".into()),
            ]),
            8,
            131_072,
            65_536,
            |_| Ok(()),
        )
        .unwrap();
        let (_, discovery) = router_managed(
            publication.reader(),
            Arc::new(serde_json::from_value(config("a", "c")).unwrap()),
            DiscoveryLimits {
                full_sync_bytes: 65_536,
                frame_bytes: 65_536,
                state_bytes: 131_072,
                replay_bytes: 65_536,
                clients: 2,
                cached_baselines: 2,
                send_timeout: Duration::from_secs(1),
            },
            Duration::from_millis(50),
        )
        .unwrap();
        // The candidate current window ends a few seconds from now. The fake
        // acquisition owner changes the component lease, then deliberately
        // withholds its receipt until that boundary has passed.
        let boundary = chrono::Utc::now().timestamp() + 3;
        let initial = btc_profile(
            1,
            vec![
                btc_binding(WindowRole::Current, "m1", '1', "101", "102", boundary - 600),
                btc_binding(WindowRole::Next, "m2", '2', "201", "202", boundary - 300),
            ],
        );
        let candidate = btc_profile(
            2,
            vec![
                btc_binding(WindowRole::Current, "m2", '2', "201", "202", boundary - 300),
                btc_binding(WindowRole::Next, "m3", '3', "301", "302", boundary),
            ],
        );
        candidate.validate_current_at(chrono::Utc::now()).unwrap();
        let (_, btc_control) = crate::source_btc5m_api::Api::router(
            publication.reader(),
            initial.clone(),
            "epoch-test".into(),
            Btc5mLimits {
                snapshot_bytes: 65_536,
                frame_bytes: 65_536,
                replay_bytes: 65_536,
                clients: 1,
                send_timeout: Duration::from_secs(1),
                snapshot_lease_capacity: 1,
            },
        )
        .unwrap();
        let short_tmp = if cfg!(target_os = "macos") {
            "/private/tmp"
        } else {
            "/tmp"
        };
        let directory =
            PathBuf::from(short_tmp).join(format!("mc-btc-cas-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).unwrap();
        let socket = directory.join("s");
        let (send, mut receive) = tokio::sync::mpsc::channel::<AcquisitionRequest>(1);
        let released = Arc::new(AtomicBool::new(false));
        let observed_release = released.clone();
        let responder = tokio::spawn(async move {
            let status = receive.recv().await.unwrap();
            assert!(matches!(
                status.action,
                AcquisitionAction::LeaseStatus { .. }
            ));
            status
                .receipt
                .send(Ok(json!({"required":true,"leases":[{
                "lease_id":"marketcow-btc5m-manager-v1","market_ids":["m1","m2"]}]})))
                .unwrap();
            let replacement = receive.recv().await.unwrap();
            assert!(matches!(
                replacement.action,
                AcquisitionAction::ReplaceLease { .. }
            ));
            let wait = (boundary - chrono::Utc::now().timestamp()).max(0) as u64;
            tokio::time::sleep(Duration::from_secs(wait) + Duration::from_millis(150)).await;
            replacement
                .receipt
                .send(Ok(json!({"status":"complete"})))
                .unwrap();
            let release = receive.recv().await.unwrap();
            assert!(matches!(
                release.action,
                AcquisitionAction::ReleaseLease { .. }
            ));
            observed_release.store(true, Ordering::SeqCst);
            release
                .receipt
                .send(Ok(json!({"status":"released"})))
                .unwrap();
        });
        let task = start(
            &socket,
            65_536,
            Duration::from_secs(5),
            Backend::Discovery(discovery),
            Some(send),
            Some(btc_control.clone()),
            None,
        )
        .unwrap();
        let request = json!({"operation":"publish_btc5m_profile",
            "expected_profile_id":initial.profile_id,"expected_revision":1,
            "lease_id":"marketcow-btc5m-manager-v1","lease_ttl_seconds":120,
            "profile":candidate});
        let response = command(&socket, serde_json::to_vec(&request).unwrap()).await;
        assert_eq!(response["ok"], false);
        assert!(
            response["error"]
                .as_str()
                .unwrap()
                .contains("publish crossed boundary after acquisition")
        );
        assert!(released.load(Ordering::SeqCst));
        assert_eq!(
            btc_control.profile().unwrap().profile_id,
            initial.profile_id
        );
        responder.await.unwrap();
        task.abort();
        let _ = task.await;
        std::fs::remove_dir(&directory).unwrap();
        publication.finish().await.unwrap();
    }
}

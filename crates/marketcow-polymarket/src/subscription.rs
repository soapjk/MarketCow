//! Bounded acquisition membership commands. A receipt means wire writes
//! completed, not that the venue acknowledged them or supplied fresh books.
use std::collections::BTreeSet;
use tokio::sync::{mpsc, oneshot};
use crate::TransportError;

pub struct SubscriptionUpdate {
    pub expected_tokens: BTreeSet<String>,
    pub token_ids: BTreeSet<String>,
    pub receipt: oneshot::Sender<Result<(), TransportError>>,
}

/// Only one outstanding command may wait behind the transport owner. The
/// caller must validate market pairs and metadata before submitting it.
pub fn subscription_commands() -> (mpsc::Sender<SubscriptionUpdate>, mpsc::Receiver<SubscriptionUpdate>) {
    mpsc::channel(1)
}

pub(crate) fn validate_tokens(tokens: &BTreeSet<String>) -> Result<(), TransportError> {
    if tokens.is_empty() || tokens.len() > 500 || tokens.iter().any(|t| t.is_empty() || t.len() > 128) {
        return Err(TransportError::InvalidConfig);
    }
    Ok(())
}

pub(crate) fn diff(current: &BTreeSet<String>, update: &SubscriptionUpdate) -> Result<(Vec<String>, Vec<String>), TransportError> {
    validate_tokens(&update.token_ids)?;
    // Add before remove: never take retained subscriptions offline to make
    // room. The temporary union is subject to the same existing shard cap.
    if current != &update.expected_tokens {
        return Err(TransportError::SubscriptionConflict);
    }
    if current.union(&update.token_ids).count() > 500 {
        return Err(TransportError::InvalidConfig);
    }
    Ok((update.token_ids.difference(current).cloned().collect(),
        current.difference(&update.token_ids).cloned().collect()))
}

/// A late, entirely unsubscribed event is no longer owned by this shard.
/// Never rewrite an atomic price-change body or suppress malformed/unknown
/// protocol messages: those still reach the normalizer's failure path.
pub fn owned_or_unclassified(value:&serde_json::Value,tokens:&BTreeSet<String>)->bool {
    if matches!(value["event_type"].as_str(),Some("book"|"tick_size_change"|"last_trade_price"|"best_bid_ask"|"source_gap"))
        && let Some(token)=value["asset_id"].as_str() {
        return tokens.contains(token);
    }
    if value["event_type"]=="price_change" {
        if let Some(changes)=value["price_changes"].as_array() {
            if !changes.is_empty() && changes.iter().all(|change|change["asset_id"].as_str().is_some()) {
                return changes.iter().any(|change|tokens.contains(change["asset_id"].as_str().unwrap()));
            }
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn commands_compare_membership_and_bound_temporary_union() {
        let current=BTreeSet::from(["a".into(),"b".into()]);
        let (tx,_)=oneshot::channel();
        let mut update=SubscriptionUpdate {expected_tokens:current.clone(),token_ids:BTreeSet::from(["b".into(),"c".into()]),receipt:tx};
        assert_eq!(diff(&current,&update).unwrap(),(vec!["c".into()],vec!["a".into()]));
        update.expected_tokens.clear();
        assert_eq!(diff(&current,&update).unwrap_err(),TransportError::SubscriptionConflict);
        update.expected_tokens=current.clone();
        update.token_ids=(0..500).map(|n|n.to_string()).collect();
        assert_eq!(diff(&current,&update).unwrap_err(),TransportError::InvalidConfig);
        let (queue,_receiver)=subscription_commands();
        assert_eq!(queue.max_capacity(),1);
    }
    #[test]
    fn late_retired_frames_do_not_require_unbounded_tombstones_or_rewrite_atomic_events() {
        let tokens=BTreeSet::from(["current".into()]);
        assert!(!owned_or_unclassified(&serde_json::json!({"event_type":"book","asset_id":"retired"}),&tokens));
        let mixed=serde_json::json!({"event_type":"price_change","price_changes":[{"asset_id":"retired"},{"asset_id":"current"}]});
        let original=mixed.clone();
        assert!(owned_or_unclassified(&mixed,&tokens));assert_eq!(mixed,original);
        assert!(owned_or_unclassified(&serde_json::json!({"event_type":"price_change","price_changes":[{}]}),&tokens));
        assert!(owned_or_unclassified(&serde_json::json!({"event_type":"unknown"}),&tokens));
    }
}

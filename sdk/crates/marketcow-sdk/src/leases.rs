//! Client-owned known session identities; close can release a job even when
//! its iterator is still retained by the application.
use crate::{Client, Result, error::require, wire_history as wire};
use prost::Message;
use reqwest::Method;
#[derive(Clone)]
pub(crate) enum Lease {
    Trades(wire::ReleaseRequest),
    Observations(wire::ObservationReleaseRequest),
}
impl Lease {
    fn id(&self) -> &str {
        match self {
            Self::Trades(r) => &r.session_id,
            Self::Observations(r) => &r.session_id,
        }
    }
    async fn release(&self, client: &Client) -> Result<()> {
        match self {
            Self::Trades(r) => {
                let (receipt, _) = client
                    .request::<wire::ReleaseReceipt>(
                        Method::DELETE,
                        "/v3/btc/trade-history/session",
                        &[],
                        Some(r.encode_to_vec()),
                        false,
                        true,
                    )
                    .await?;
                require(
                    receipt.schema_version == "marketcow.btc-history-release.v3"
                        && receipt.session_id == r.session_id
                        && receipt.range == r.range
                        && receipt.request_id == r.request_id,
                    "shutdown history release identity",
                )
            }
            Self::Observations(r) => {
                let (receipt, _) = client
                    .request::<wire::ObservationReleaseReceipt>(
                        Method::DELETE,
                        "/v3/btc/observation-history/session",
                        &[],
                        Some(r.encode_to_vec()),
                        false,
                        true,
                    )
                    .await?;
                require(
                    receipt.schema_version == "marketcow.btc-observation-history-release.v3"
                        && receipt.session_id == r.session_id
                        && receipt.range == r.range
                        && receipt.request_id == r.request_id,
                    "shutdown observation release identity",
                )
            }
        }
    }
}
impl Client {
    pub(crate) fn register_lease(&self, lease: Lease) -> Result<()> {
        let mut leases = self
            .0
            .leases
            .lock()
            .map_err(|_| crate::Error::Protocol("lease registry poisoned".into()))?;
        if leases.len() >= 128 && !leases.contains_key(lease.id()) {
            return Err(crate::Error::Limit("client history sessions"));
        }
        leases.insert(lease.id().into(), lease);
        Ok(())
    }
    pub(crate) fn forget_lease(&self, id: &str) {
        if let Ok(mut leases) = self.0.leases.lock() {
            leases.remove(id);
        }
    }
    pub(crate) async fn release_leases(&self) -> Result<()> {
        use futures_util::{StreamExt, stream};
        let leases = self
            .0
            .leases
            .lock()
            .map_err(|_| crate::Error::Protocol("lease registry poisoned".into()))?
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut pending = stream::iter(leases)
            .map(|lease| async move {
                let result =
                    tokio::time::timeout(crate::history::CLEANUP_TIME, lease.release(self))
                        .await
                        .unwrap_or_else(|_| {
                            Err(crate::Error::Limit("client session release deadline"))
                        });
                if result.is_ok() {
                    self.forget_lease(lease.id());
                }
                result
            })
            .buffer_unordered(4);
        let mut failure = None;
        while let Some(result) = pending.next().await {
            if let Err(error) = result {
                failure = Some(error);
            }
        }
        failure.map_or(Ok(()), Err)
    }
}

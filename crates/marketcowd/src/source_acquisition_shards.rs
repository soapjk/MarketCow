//! Collector-owned, bounded upstream connections. Retained shards are not
//! restarted when new market pairs are installed. No detached socket tasks.
use anyhow::{Result,ensure,Context};
use std::{collections::{BTreeMap,BTreeSet},time::Duration};
use marketcow_polymarket::{PolymarketTransportConfig,RawTransportFrame,TransportError,
    supervise_polymarket_transport_managed,subscription::{subscription_commands,SubscriptionUpdate}};
use tokio::sync::{mpsc,watch,oneshot};

struct Shard {
    tokens:BTreeSet<String>,stop:watch::Sender<bool>,commands:mpsc::Sender<SubscriptionUpdate>,
    pending:Option<(BTreeSet<String>,oneshot::Receiver<Result<(),TransportError>>)>,stopping:bool,
    retirement_target:Option<BTreeSet<String>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn additions_preserve_owned_shards_and_capacity_rejects_before_mutation() {
        let (output,_)=mpsc::channel(1);
        let mut pool=Shards::new(PolymarketTransportConfig::production(),output,6,2,4).unwrap();
        let original=pool.add_pairs(&[["11".into(),"12".into()]]).unwrap()[0];
        pool.add_pairs(&[["21".into(),"22".into()]]).unwrap();
        assert_eq!(pool.shards[&original].tokens,BTreeSet::from(["11".into(),"12".into()]));
        let before=pool.tokens();
        assert!(pool.add_pairs(&[["31".into(),"32".into()]]).is_err());
        assert_eq!(pool.tokens(),before);assert_eq!(pool.jobs.len(),2);
        assert!(pool.add_pairs(&[["11".into(),"99".into()]]).is_err());
        // No await before abort: these are ownership tests, not Internet tests.
        pool.stop().await;assert!(pool.tokens().is_empty());assert!(pool.jobs.is_empty());
    }
    #[tokio::test]
    async fn retirement_counts_tokens_until_actual_success_and_failed_receipt_keeps_them() {
        let (output,_)=mpsc::channel(1);
        let mut pool=Shards::new(PolymarketTransportConfig::production(),output,4,2,4).unwrap();
        let (commands,mut receiver)=subscription_commands();let (stop,_)=watch::channel(false);
        let original=BTreeSet::from(["11".into(),"12".into(),"21".into(),"22".into()]);
        pool.shards.insert(0,Shard{tokens:original.clone(),stop,commands,pending:None,stopping:false,retirement_target:None});
        let removed=BTreeSet::from(["11".into(),"12".into()]);
        assert_eq!(pool.begin_retire(&removed).unwrap(),vec![0]);
        assert_eq!(pool.tokens(),original);assert!(pool.begin_retire(&removed).is_err());
        let command=receiver.try_recv().unwrap();assert_eq!(command.expected_tokens,original);
        command.receipt.send(Err(TransportError::SendFailed)).unwrap();
        assert!(pool.poll_retirements().is_err());assert_eq!(pool.tokens(),original);
        assert!(pool.begin_retire(&removed).is_err());
        pool.poll_retirements().unwrap();let command=receiver.try_recv().unwrap();
        command.receipt.send(Ok(())).unwrap();
        assert_eq!(pool.poll_retirements().unwrap(),vec![0]);
        assert_eq!(pool.tokens(),BTreeSet::from(["21".into(),"22".into()]));
        pool.stop().await;
    }
}
pub struct Shards {
    config:PolymarketTransportConfig,output:mpsc::Sender<Vec<RawTransportFrame>>,
    maximum_tokens:usize,maximum_shards:usize,shard_tokens:usize,next_id:u64,
    shards:BTreeMap<u64,Shard>,
    jobs:tokio::task::JoinSet<(u64,Result<(),TransportError>)>,
}
impl Shards {
    pub fn new(config:PolymarketTransportConfig,output:mpsc::Sender<Vec<RawTransportFrame>>,
        maximum_tokens:usize,maximum_shards:usize,shard_tokens:usize)->Result<Self> {
        ensure!((2..=500).contains(&shard_tokens) && (2..=8192).contains(&maximum_tokens)
            && maximum_shards>0 && maximum_shards<=1024,"explicit acquisition shard budgets");
        Ok(Self{config,output,maximum_tokens,maximum_shards,shard_tokens,next_id:0,
            shards:BTreeMap::new(),jobs:tokio::task::JoinSet::new()})
    }
    pub fn tokens(&self)->BTreeSet<String> {
        self.shards.values().flat_map(|s|s.tokens.iter().cloned()).collect()
    }
    pub fn statistics(&self)->(usize,usize,usize) {
        (self.shards.values().map(|s|s.tokens.len()).sum(),self.jobs.len(),
            self.shards.values().filter(|s|s.retirement_target.is_some()||s.stopping).count())
    }
    pub fn add_pairs(&mut self,pairs:&[[String;2]])->Result<Vec<u64>> {
        let batches=self.validate_add_pairs(pairs)?;
        self.reap()?;
        ensure!(self.jobs.len()+batches.len()<=self.maximum_shards,"acquisition socket capacity");
        self.next_id.checked_add(batches.len() as u64).context("shard identity overflow")?;
        let mut added=vec![];
        for tokens in batches {
            let id=self.next_id;self.next_id+=1;
            let (stop,shutdown)=watch::channel(false);
            let (commands,updates)=subscription_commands();
            let config=self.config.clone();let output=self.output.clone();
            let initial=tokens.iter().cloned().collect();
            self.jobs.spawn(async move {
                (id,supervise_polymarket_transport_managed(config,initial,output,shutdown,updates,Duration::from_secs(1)).await)
            });
            self.shards.insert(id,Shard{tokens,stop,commands,pending:None,stopping:false,retirement_target:None});added.push(id);
        }
        Ok(added)
    }
    pub fn validate_add_pairs(&self,pairs:&[[String;2]])->Result<Vec<BTreeSet<String>>> {
        let mut tokens=self.tokens();
        let mut batches:Vec<BTreeSet<String>>=vec![];
        for pair in pairs {
            ensure!(pair.iter().all(|t|!t.is_empty()&&t.len()<=128)&&pair[0]!=pair[1],"market token pair");
            for token in pair {ensure!(tokens.insert(token.clone()),"already-owned acquisition token");}
            if batches.last().is_none_or(|batch|batch.len()+2>self.shard_tokens) {batches.push(BTreeSet::new());}
            batches.last_mut().unwrap().extend(pair.iter().cloned());
        }
        ensure!(tokens.len()<=self.maximum_tokens,"acquisition token capacity");
        // Retiring sockets count until their tasks have actually exited.
        ensure!(self.jobs.len()+batches.len()<=self.maximum_shards,"acquisition socket capacity");
        self.next_id.checked_add(batches.len() as u64).context("shard identity overflow")?;
        Ok(batches)
    }
    /// Fencing reducers/metadata is the caller's responsibility. This only
    /// schedules bounded wire changes and returns every receipt; it does not
    /// claim that an unsubscribe was acknowledged by the exchange.
    pub fn begin_retire(&mut self,removed:&BTreeSet<String>)->Result<Vec<u64>> {
        ensure!(!removed.is_empty()&&removed.is_subset(&self.tokens()),"unknown retirement token");
        let mut changes=vec![];
        // Reserve all affected command slots before changing local ownership.
        for (id,shard) in &self.shards {
            let next:BTreeSet<_>=shard.tokens.difference(removed).cloned().collect();
            if next==shard.tokens {continue;}
            ensure!(shard.retirement_target.is_none()&&!shard.stopping,"shard retirement in flight");
            let permit=if next.is_empty(){None}else{Some(shard.commands.clone().try_reserve_owned().context("shard update in flight")?)};
            changes.push((*id,next,permit));
        }
        let mut receipts=vec![];
        for (id,next,permit) in changes {
            if let Some(permit)=permit {
                let shard=self.shards.get_mut(&id).unwrap();
                let (receipt,done)=oneshot::channel();
                permit.send(SubscriptionUpdate{expected_tokens:shard.tokens.clone(),token_ids:next.clone(),receipt});
                shard.retirement_target=Some(next.clone());shard.pending=Some((next,done));
            } else {
                let shard=self.shards.get_mut(&id).unwrap();shard.stopping=true;let _=shard.stop.send(true);
            }
            receipts.push(id);
        }
        Ok(receipts)
    }
    /// Only actual successful wire receipts release partial-shard ownership.
    /// Until then, the old token set continues counting against capacity.
    pub fn poll_retirements(&mut self)->Result<Vec<u64>> {
        let mut completed=vec![];
        for (id,shard) in &mut self.shards {
            if shard.pending.is_none() && let Some(next)=&shard.retirement_target {
                let (receipt,done)=oneshot::channel();
                shard.commands.try_send(SubscriptionUpdate{expected_tokens:shard.tokens.clone(),token_ids:next.clone(),receipt})
                    .map_err(|_|anyhow::anyhow!("retirement retry busy or stopped"))?;
                shard.pending=Some((next.clone(),done));
            }
            if let Some((next,receipt))=&mut shard.pending {
                match receipt.try_recv() {
                    Ok(Ok(()))=>{shard.tokens=next.clone();shard.pending=None;shard.retirement_target=None;completed.push(*id);},
                    Ok(Err(error))=>{shard.pending=None;anyhow::bail!("shard unsubscribe failed: {error:?}");},
                    Err(oneshot::error::TryRecvError::Empty)=>{},
                    Err(oneshot::error::TryRecvError::Closed)=>{shard.pending=None;anyhow::bail!("shard unsubscribe receipt lost");},
                }
            }
        }
        self.reap()?;
        Ok(completed)
    }
    fn reap(&mut self)->Result<()> {
        while let Some(result)=self.jobs.try_join_next() {
            let (id,result)=result?;
            ensure!(self.shards.get(&id).is_none_or(|s|s.stopping),"active acquisition shard ended: {result:?}");
            self.shards.remove(&id);
        }
        Ok(())
    }
    pub async fn failure(&mut self)->Result<()> {
        loop {
            let Some(result)=self.jobs.join_next().await else {return std::future::pending().await;};
            let (id,result)=result?;
            ensure!(self.shards.get(&id).is_none_or(|s|s.stopping),"active acquisition shard ended: {result:?}");
            self.shards.remove(&id);
        }
    }
    pub async fn stop(&mut self) {
        for shard in self.shards.values(){let _=shard.stop.send(true);}
        self.shards.clear();self.jobs.abort_all();
        while self.jobs.join_next().await.is_some(){}
    }
}

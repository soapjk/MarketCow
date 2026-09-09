//! Same-process REST acquisition membership. Fixed bounded batches retain their
//! own deadlines. Incarnation fencing rejects late responses after retirement.
use anyhow::{Context,Result,ensure};
use serde_json::{Value,json};
use std::{collections::{BTreeMap,BTreeSet},sync::{Arc,RwLock},time::Duration};
use tokio::{sync::{mpsc,Semaphore},task::JoinSet};
use crate::{Market,Args,Plan,fetch_identified,source_publication::Publication,
    source_scope_control::AcquisitionRequest};

#[derive(Default)]
struct Members {next:u64, rows:BTreeMap<String,(Market,u64)>}
impl Members {
    fn add(&mut self,markets:Vec<Market>)->Result<Vec<(Market,u64)>> {
        let mut seen=BTreeSet::new();
        for m in &markets {
            ensure!(seen.insert(m.market_id.clone()),"duplicate REST admission");
            if let Some((old,_))=self.rows.get(&m.market_id) {
                ensure!(old.condition_id==m.condition_id&&old.token_ids==m.token_ids,"REST retained identity differs");
            }
        }
        self.next.checked_add(markets.len() as u64).context("REST incarnation overflow")?;
        let mut added=vec![];
        for m in markets {
            if self.rows.contains_key(&m.market_id){continue;}
            self.next+=1;self.rows.insert(m.market_id.clone(),(m.clone(),self.next));added.push((m,self.next));
        }
        Ok(added)
    }
    fn owns(&self,id:&str,epoch:u64)->bool {self.rows.get(id).is_some_and(|(_,current)|*current==epoch)}
}
enum Message {
    Membership {job_id:u64,owned:Vec<(String,u64)>},
    Books {epochs:BTreeMap<String,u64>,values:Vec<(String,Result<Vec<marketcow_polymarket::discovery_source::PreparedSnapshot>>)>,elapsed_us:u128},
    Terminal {epoch:u64,evidence:Value},
}
struct Pool {
    members:Arc<RwLock<Members>>,jobs:JoinSet<Result<u64>>,owned:BTreeMap<u64,Vec<(String,u64)>>,next_job:u64,
    network:Arc<Semaphore>,client:reqwest::Client,
    sender:mpsc::Sender<Message>,batch:usize,maximum_jobs:usize,maximum_tokens:usize,
    limit:usize,poll:u64,refresh:Option<u64>,cycles:Option<u64>,
}
impl Pool {
    fn validate(&mut self,markets:&[Market])->Result<()> {
        while let Some(result)=self.jobs.try_join_next(){let id=result??;self.owned.remove(&id);}
        let members=self.members.read().map_err(|_|anyhow::anyhow!("REST owner poisoned"))?;
        let mut seen=BTreeSet::new();let mut count=0;
        for m in markets {
            ensure!(seen.insert(&m.market_id),"duplicate REST admission");
            if let Some((old,_))=members.rows.get(&m.market_id) {
                ensure!(old.condition_id==m.condition_id&&old.token_ids==m.token_ids,"REST retained identity differs");
            }else{count+=1;}
        }
        ensure!((members.rows.len()+count)*2<=self.maximum_tokens,"REST token capacity");
        ensure!(self.jobs.len()+count.div_ceil(self.batch)<=self.maximum_jobs,"REST batch worker capacity");
        self.next_job.checked_add(count.div_ceil(self.batch) as u64).context("REST job overflow")?;
        Ok(())
    }
    fn add(&mut self,markets:Vec<Market>)->Result<()> {
        self.validate(&markets)?;
        let added=self.members.write().map_err(|_|anyhow::anyhow!("REST owner poisoned"))?.add(markets)?;
        for chunk in added.chunks(self.batch) {
            let job_id=self.next_job;self.next_job+=1;
            self.owned.insert(job_id,chunk.iter().map(|(m,e)|(m.market_id.clone(),*e)).collect());
            let owned=chunk.to_vec();let members=self.members.clone();let network=self.network.clone();
            let client=self.client.clone();let sender=self.sender.clone();
            let (limit,poll,refresh,cycles)=(self.limit,self.poll,self.refresh,self.cycles);
            self.jobs.spawn(async move {
                let mut cycle=0;let mut terminal=BTreeMap::<String,tokio::time::Instant>::new();
                loop {
                    terminal.retain(|_,at|refresh.is_some_and(|s|at.elapsed()<Duration::from_secs(s)));
                    let eligible:Vec<_>={let state=members.read().map_err(|_|anyhow::anyhow!("REST owner poisoned"))?;
                        owned.iter().filter(|(m,e)|state.owns(&m.market_id,*e)).cloned().collect()};
                    if eligible.is_empty(){break;}
                    sender.send(Message::Membership{job_id,owned:eligible.iter().map(|(m,e)|(m.market_id.clone(),*e)).collect()}).await?;
                    let active:Vec<_>=eligible.iter().filter(|(m,_)|!terminal.contains_key(&m.market_id)).map(|(m,_)|m.clone()).collect();
                    let epochs:BTreeMap<_,_>=eligible.iter().map(|(m,e)|(m.market_id.clone(),*e)).collect();
                    let slot=sender.clone().reserve_owned().await?;
                    let started=tokio::time::Instant::now();
                    let values=if active.is_empty(){vec![]}else{
                        let _permit=network.acquire().await?;fetch_identified(client.clone(),active,limit).await?
                    };
                    let failed:Vec<_>=values.iter().filter(|(_,v)|v.is_err()).map(|(id,_)|id.clone()).collect();
                    slot.send(Message::Books{epochs:epochs.clone(),values,elapsed_us:started.elapsed().as_micros()});
                    if refresh.is_some() {
                        for id in failed {
                            let Some((m,epoch))=eligible.iter().find(|(m,_)|m.market_id==id)else{continue;};
                            if !members.read().map_err(|_|anyhow::anyhow!("REST owner poisoned"))?.owns(&id,*epoch){continue;}
                            let observation={let _permit=network.acquire().await?;
                                crate::source_lifecycle::refresh(client.clone(),m.clone(),limit).await};
                            match observation {
                                Ok(Some(evidence))=>{sender.send(Message::Terminal{epoch:*epoch,evidence}).await?;terminal.insert(id,tokio::time::Instant::now());},
                                Ok(None)=>{},Err(error)=>eprintln!("{}",json!({"REST_lifecycle_error":error.to_string(),"market_id":id})),
                            }
                        }
                    }
                    cycle+=1;if cycles.is_some_and(|n|cycle>=n){break;}
                    tokio::time::sleep_until(started+Duration::from_secs(poll)).await;
                }
                Ok(job_id)
            });
        }
        Ok(())
    }
    fn statistics(&self)->Result<(usize,usize,usize)> {
        let members=self.members.read().map_err(|_|anyhow::anyhow!("REST owner poisoned"))?;
        let count=members.rows.len();
        let retiring=self.owned.values().filter(|batch|batch.iter().any(|(id,e)|!members.owns(id,*e))).count();
        // REST has no unsubscribe handshake; old HTTP tasks remain bounded by
        // network permits and their original request deadlines, and are fenced.
        Ok((count*2,self.jobs.len(),retiring))
    }
}

pub async fn run(args:&Args,plan:&Plan,publication:&mut Publication,
    mut acquisition:mpsc::Receiver<AcquisitionRequest>)->Result<()> {
    let (sender,mut receiver)=mpsc::channel(args.concurrency);
    let mut pool=Pool{members:Arc::new(RwLock::new(Members::default())),jobs:JoinSet::new(),owned:BTreeMap::new(),next_job:0,
        network:Arc::new(Semaphore::new(args.concurrency)),client:reqwest::Client::builder()
            .timeout(Duration::from_secs(args.request_timeout_seconds)).build()?,sender,
        batch:args.request_market_batch_size,maximum_jobs:args.acquisition_socket_budget.context("REST worker budget")?,
        maximum_tokens:args.acquisition_token_budget.context("REST token budget")?,limit:args.response_byte_limit,
        poll:args.poll_seconds,refresh:args.lifecycle_refresh_seconds,cycles:args.cycles};
    pool.add(plan.markets.clone())?;
    publication.set_acquisition_statistics(pool.statistics()?);
    let shutdown=crate::shutdown_signal();tokio::pin!(shutdown);
    let result:Result<()>=async {loop {
        tokio::select! {
            _=&mut shutdown=>break,
            result=pool.jobs.join_next(),if !pool.jobs.is_empty()=>{let id=result.context("REST worker missing")???;pool.owned.remove(&id);
                publication.set_acquisition_statistics(pool.statistics()?);
                if args.cycles.is_some()&&pool.jobs.is_empty()&&receiver.is_empty(){break;}},
            request=acquisition.recv()=>{
                let Some(request)=request else{anyhow::bail!("REST control owner stopped");};
                let outcome=(||->Result<Value>{
                    if !request.retire_market_ids.is_empty(){
                        publication.check_retirement(request.retire_market_ids.clone())?;
                        if request.validate_only{return Ok(json!({"retirement_validated":true,"publication_applied":false}));}
                        {let mut state=pool.members.write().map_err(|_|anyhow::anyhow!("REST owner poisoned"))?;
                            for id in &request.retire_market_ids {state.rows.remove(id);}}
                        publication.retire_catalog_markets(request.retire_market_ids.clone())?;
                        publication.set_acquisition_statistics(pool.statistics()?);
                        return Ok(json!({"retirement_queued":true,"retired_market_ids":request.retire_market_ids,
                            "transport":"rest_poll","late_responses_fenced":true}));
                    }
                    publication.check_catalog_admission(&request.catalog_revision,request.records.clone(),&request.evidence_sha256,
                        &request.acquisition_market_ids,pool.maximum_tokens)?;
                    let mut markets=vec![];
                    for record in &request.records {
                        let id=record["identity"]["market_id"].as_str().context("REST market id")?;
                        if !request.acquisition_market_ids.contains(id){continue;}
                        let tokens:Vec<String>=record["identity"]["outcomes"].as_array().context("REST outcomes")?.iter()
                            .map(|o|o["token_id"].as_str().map(str::to_owned).context("REST token")).collect::<Result<_>>()?;
                        markets.push(Market{market_id:id.into(),condition_id:record["identity"]["condition_id"].as_str().context("REST condition")?.into(),
                            token_ids:tokens.try_into().map_err(|_|anyhow::anyhow!("REST binary pair"))?});
                    }
                    pool.validate(&markets)?;
                    if request.validate_only{return Ok(json!({"acquisition_validated":true,"publication_applied":false}));}
                    publication.admit_catalog_markets_scoped(&request.catalog_revision,request.records.clone(),&request.evidence_sha256,
                        &request.acquisition_market_ids,pool.maximum_tokens)?;
                    pool.add(markets)?;publication.set_acquisition_statistics(pool.statistics()?);
                    Ok(json!({"acquisition_installed":true,"publication_applied":false,"transport":"rest_poll",
                        "requested_market_ids":request.acquisition_market_ids,"book_readiness_not_implied":true}))
                })();let _=request.receipt.send(outcome);
            },
            message=receiver.recv()=>{
                let message=message.context("REST publication channel closed")?;
                match message {
                    Message::Membership{job_id,owned}=>{
                        if let Some(current)=pool.owned.get_mut(&job_id){*current=owned;}
                        publication.set_acquisition_statistics(pool.statistics()?);
                    },
                    Message::Books{epochs,values,elapsed_us}=>{
                        let mut accepted=0;let mut rejected=0;let mut retired=0;
                        for (id,value) in values {
                            if !pool.members.read().map_err(|_|anyhow::anyhow!("REST owner poisoned"))?.owns(&id,epochs[&id]){retired+=1;continue;}
                            match value {
                                Ok(drafts)=>{let cursor=publication.cursor()?;
                                    let events=drafts.into_iter().enumerate().map(|(i,d)|Ok(d.finalize(cursor.checked_add(i as u64+1).context("cursor overflow")?)?)).collect::<Result<Vec<_>>>()?;
                                    publication.publish(events,None)?;accepted+=1;},
                                Err(error)=>{rejected+=1;eprintln!("{}",json!({"market_id":id,"source_error":error.to_string()}));},
                            }
                        }
                        println!("{}",json!({"managed_rest":true,"accepted_markets":accepted,"rejected_markets":rejected,
                            "fenced_late_markets":retired,"request_wait_and_prepare_us":elapsed_us,"cursor":publication.cursor()?,"persisted_cursor":publication.persisted_cursor()}));
                    },
                    Message::Terminal{epoch,evidence}=>{
                        let id=evidence["market_id"].as_str().context("REST terminal id")?;
                        if !pool.members.read().map_err(|_|anyhow::anyhow!("REST owner poisoned"))?.owns(id,epoch){continue;}
                        let view=publication.reader().capture()?;
                        let event=crate::source_lifecycle::terminal_event(view.markets.get(id).context("REST lifecycle metadata")?,
                            &evidence,publication.cursor()?.checked_add(1).context("cursor overflow")?)?;
                        publication.publish(vec![event],Some(evidence))?;
                    }
                }
                if args.cycles.is_some()&&pool.jobs.is_empty()&&receiver.is_empty(){break;}
            }
        }
    }Ok(())}.await;
    pool.jobs.abort_all();while pool.jobs.join_next().await.is_some(){}
    result
}

#[cfg(test)]mod tests {
    use super::*;
    fn market(id:&str)->Market{Market{market_id:id.into(),condition_id:format!("c{id}"),token_ids:[format!("{id}1"),format!("{id}2")]}}
    #[test]fn late_rest_response_and_terminal_cannot_cross_readded_incarnation(){
        let mut m=Members::default();let a=m.add(vec![market("1"),market("2")]).unwrap();
        let epoch=a[0].1;let other=a[1].1;
        assert!(m.add(vec![market("1")]).unwrap().is_empty());assert!(m.owns("1",epoch));
        m.rows.remove("1");assert!(!m.owns("1",epoch));assert!(m.owns("2",other));
        let new=m.add(vec![market("1")]).unwrap();assert!(!m.owns("1",epoch));assert!(m.owns("1",new[0].1));
        let mut wrong=market("2");wrong.condition_id="different".into();assert!(m.add(vec![wrong]).is_err());
        assert!(m.owns("2",other));
    }
}

//! Private same-user publication control. No TCP listener, source paths,
//! executable names, account operations, or acquisition claims in this wire.
//! The authenticated outer control plane must prepare acquisition first.
use anyhow::{Context,Result,ensure};
use serde::Deserialize;
use serde_json::{Value,json};
use std::{path::{Path,PathBuf},sync::{Arc,Mutex},time::Duration};
use std::os::unix::fs::{PermissionsExt,MetadataExt,FileTypeExt};
use tokio::io::{AsyncReadExt,AsyncWriteExt};
use crate::{source_public_api::{PublicScopeControl,ConfiguredScope},
    source_discovery_api::DiscoveryScopeControl,source_discovery_projection::DiscoveryConfig};

pub enum Backend {Live(PublicScopeControl),Discovery(DiscoveryScopeControl)}

pub struct AcquisitionRequest {
    pub validate_only:bool,
    pub catalog_revision:String,pub records:Vec<Value>,pub evidence_sha256:String,
    pub acquisition_market_ids:std::collections::BTreeSet<String>,
    pub retire_market_ids:std::collections::BTreeSet<String>,
    pub receipt:tokio::sync::oneshot::Sender<Result<Value>>,
}
pub type AcquisitionSender=tokio::sync::mpsc::Sender<AcquisitionRequest>;

#[derive(Deserialize)]
#[serde(tag="operation",rename_all="snake_case",deny_unknown_fields)]
enum Command {
    Status,
    PreparePublication {expected_scope_id:String,expected_revision:u64,config:Value},
    PublishScope {expected_scope_id:String,expected_revision:u64,config:Value},
    PrepareAcquisition {expected_scope_id:String,expected_revision:u64,catalog_revision:String,
        records:Vec<Value>,evidence_sha256:String,acquisition_market_ids:Vec<String>},
    RetireAcquisition {expected_scope_id:String,expected_revision:u64,market_ids:Vec<String>},
}

impl Backend {
    fn status(&self)->Result<Value> {
        match self {Self::Live(control)=>control.status(),Self::Discovery(control)=>control.status()}
    }
    fn execute(&self,body:&[u8],acquisition:Option<&AcquisitionSender>,mut journal:Option<&mut crate::source_scope_journal::Journal>)->Result<Value> {
        if let Some(journal)=journal.as_mut() {
            if let Some(ticket)=self.status()?["retirement_persisted"].as_u64(){journal.settled(ticket)?;}
        }
        // Canonical compact object ordering also prevents duplicate-key input
        // from being silently collapsed by Value deserialization.
        let value:Value=serde_json::from_slice(body)?;
        ensure!(serde_json::to_vec(&value)?==body,"noncanonical control JSON");
        let command:Command=serde_json::from_value(value)?;
        let (expected,revision,config,publish)=match command {
            Command::Status=>return self.status(),
            Command::RetireAcquisition{expected_scope_id,expected_revision,market_ids}=>{
                let active=self.status()?;
                let identity=if active["pool"]=="discovery"{&active["projection_id"]}else{&active["scope_id"]};
                ensure!(identity==&expected_scope_id && active["revision"]==expected_revision,"retirement incumbent conflict");
                ensure!(!market_ids.is_empty()&&market_ids.len()<=4096&&market_ids.windows(2).all(|p|p[0]<p[1]),"sorted unique retirement ids required");
                let protected=match self {Self::Live(c)=>c.referenced_markets()?,Self::Discovery(c)=>c.referenced_markets()?};
                ensure!(market_ids.iter().all(|id|!protected.contains(id)),"active or grace-period dependency still referenced");
                let ids=market_ids.into_iter().collect();
                for validate_only in [true,false] {
                    if !validate_only {
                        if let Some(journal)=journal.as_mut(){journal.retire(&ids,active["retirement_submitted"].as_u64().context("retirement status")?.checked_add(1).context("retirement overflow")?)?;}
                    }
                    let (receipt,done)=tokio::sync::oneshot::channel();
                    acquisition.context("managed acquisition unavailable")?.try_send(AcquisitionRequest{
                        validate_only,catalog_revision:String::new(),records:vec![],evidence_sha256:String::new(),
                        acquisition_market_ids:Default::default(),retire_market_ids:ids.clone(),receipt})
                        .map_err(|_|anyhow::anyhow!("acquisition command busy or stopped"))?;
                    let result=done.blocking_recv().context("acquisition owner stopped")??;
                    if !validate_only{return Ok(result);}
                }
                unreachable!();
            },
            Command::PrepareAcquisition{expected_scope_id,expected_revision,catalog_revision,records,evidence_sha256,acquisition_market_ids}=>{
                let active=self.status()?;
                let identity=if active["pool"]=="discovery"{&active["projection_id"]}else{&active["scope_id"]};
                ensure!(identity==&expected_scope_id && active["revision"]==expected_revision
                    && active["catalog_revision"]==catalog_revision,"acquisition incumbent conflict");
                ensure!(acquisition_market_ids.windows(2).all(|p|p[0]<p[1])&&acquisition_market_ids.iter().all(|s|!s.is_empty()),"sorted unique acquisition ids required");
                let ids=acquisition_market_ids.into_iter().collect();
                for validate_only in [true,false] {
                    if !validate_only {if let Some(journal)=journal.as_mut(){journal.admit(&records,&ids)?;}}
                    let (receipt,done)=tokio::sync::oneshot::channel();
                    acquisition.context("managed acquisition unavailable")?.try_send(AcquisitionRequest{validate_only,
                        catalog_revision:catalog_revision.clone(),records:records.clone(),evidence_sha256:evidence_sha256.clone(),receipt,
                        acquisition_market_ids:ids.clone(),retire_market_ids:Default::default()})
                        .map_err(|_|anyhow::anyhow!("acquisition command busy or stopped"))?;
                    let result=done.blocking_recv().context("acquisition owner stopped")??;
                    if !validate_only{return Ok(result);}
                }
                unreachable!();
            },
            Command::PreparePublication{expected_scope_id,expected_revision,config}=>(expected_scope_id,expected_revision,config,false),
            Command::PublishScope{expected_scope_id,expected_revision,config}=>(expected_scope_id,expected_revision,config,true),
        };
        ensure!(!expected.is_empty()&&expected.len()<=256&&revision>0,"control identity");
        // Durable control intent precedes publication. A crash without a
        // response is reconciled to this intent before reopening listeners.
        match self {
            Self::Live(control)=>control.prepare(&expected,revision,&serde_json::from_value::<ConfiguredScope>(config.clone())?)?,
            Self::Discovery(control)=>control.prepare(&expected,revision,&serde_json::from_value::<DiscoveryConfig>(config.clone())?)?,
        }
        let previous=journal.as_ref().map(|j|j.state.clone());
        if publish {if let Some(journal)=journal.as_mut(){let mut next=journal.state.clone();next.active_config=config.clone();next.active_revision=revision.checked_add(1).context("scope revision overflow")?;journal.commit(next)?;}}
        let applied=(||->Result<()>{match self {
            Self::Live(control)=>{
                let scope:ConfiguredScope=serde_json::from_value(config)?;
                if publish {control.activate(&expected,revision,scope)?;}else{control.prepare(&expected,revision,&scope)?;}
            },
            Self::Discovery(control)=>{
                let scope:DiscoveryConfig=serde_json::from_value(config)?;
                if publish {control.activate(&expected,revision,Arc::new(scope))?;}else{control.prepare(&expected,revision,&scope)?;}
            },
        } Ok(())})();
        if let Err(error)=applied {
            if let (Some(journal),Some(previous))=(journal.as_mut(),previous){journal.commit(previous)?;}
            return Err(error);
        }
        Ok(json!({"publication_applied":publish,"actual":self.status()?,
            "acquisition_prepared_by_this_operation":false}))
    }
}

struct SocketOwner {path:PathBuf,device:u64,inode:u64}
impl Drop for SocketOwner {
    fn drop(&mut self) {
        if let Ok(meta)=std::fs::symlink_metadata(&self.path) {
            if meta.file_type().is_socket()&&meta.dev()==self.device&&meta.ino()==self.inode {
                let _=std::fs::remove_file(&self.path);
            }
        }
    }
}

pub fn start(path:&Path,maximum_bytes:usize,timeout:Duration,backend:Backend,acquisition:Option<AcquisitionSender>,journal:Option<crate::source_scope_journal::Journal>)->Result<tokio::task::JoinHandle<Result<()>>> {
    ensure!(path.is_absolute() && (1024..=16*1024*1024).contains(&maximum_bytes)
        && !timeout.is_zero()&&timeout<=Duration::from_secs(30),"explicit control budgets/path required");
    let parent=path.parent().context("control directory")?;
    let metadata=std::fs::symlink_metadata(parent)?;
    ensure!(metadata.is_dir() && metadata.permissions().mode()&0o077==0
        && std::fs::canonicalize(parent)?==parent,"control directory must be private and canonical");
    // Never unlink an existing socket on startup: it may have a live owner.
    let listener=tokio::net::UnixListener::bind(path)?;
    let metadata=std::fs::symlink_metadata(path)?;
    let owner=SocketOwner{path:path.to_owned(),device:metadata.dev(),inode:metadata.ino()};
    std::fs::set_permissions(path,std::fs::Permissions::from_mode(0o600))?;
    let backend=Arc::new(Mutex::new((backend,journal)));
    Ok(tokio::spawn(async move {
        let _owner=owner;
        loop {
            let (mut socket,_)=listener.accept().await?;
            let body=tokio::time::timeout(timeout,async {
                let length=socket.read_u32().await? as usize;
                ensure!(length>0 && length<=maximum_bytes,"control request byte budget");
                let mut body=vec![0;length];socket.read_exact(&mut body).await?;
                Ok::<_,anyhow::Error>(body)
            }).await;
            let result=match body {
                Ok(Ok(body))=>{
                    let backend=backend.clone();
                    let acquisition=acquisition.clone();
                    // One command at a time, off the async ingestion loop.
                    // Do not abandon a running CAS if the caller disconnects.
                    tokio::task::spawn_blocking(move||{
                        let mut owner=backend.lock().map_err(|_|anyhow::anyhow!("control owner poisoned"))?;
                        let (backend,journal)=&mut *owner;backend.execute(&body,acquisition.as_ref(),journal.as_mut())
                    }).await?
                },
                Ok(Err(error))=>Err(error),
                Err(_)=>Err(anyhow::anyhow!("control read deadline")),
            };
            let response=match result {
                Ok(value)=>json!({"ok":true,"result":value}),
                Err(error)=>json!({"ok":false,"error":error.to_string()}),
            };
            let bytes=serde_json::to_vec(&response)?;
            if bytes.len()>maximum_bytes {continue;}
            let _=tokio::time::timeout(timeout,async {
                socket.write_u32(bytes.len() as u32).await?;
                socket.write_all(&bytes).await?;socket.shutdown().await
            }).await;
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use crate::{source_publication::Publication,source_discovery_api::{router_managed,DiscoveryLimits}};

    fn config(id:&str,universe:&str)->Value {json!({"projection_id":id.repeat(64),
        "catalog_revision":"b".repeat(64),"universe_revision":universe.repeat(64),
        "market_ids":["1"],"relations":[],"settlements":{"1":null},
        "policy":{"quantities":["10"],"maximum_book_age_ms":5000}})}
    async fn command(path:&Path,body:Vec<u8>)->Value {
        let mut socket=tokio::net::UnixStream::connect(path).await.unwrap();
        socket.write_u32(body.len() as u32).await.unwrap();socket.write_all(&body).await.unwrap();
        let n=socket.read_u32().await.unwrap();assert!(n<=65536);
        let mut response=vec![0;n as usize];socket.read_exact(&mut response).await.unwrap();
        serde_json::from_slice(&response).unwrap()
    }
    #[tokio::test]
    async fn private_socket_reports_actual_cas_and_releases_its_path() {
        // Synthetic source; exercises actual Unix framing, not an upstream load claim.
        let market=json!({"identity":{"market_id":"1","condition_id":"condition","event_id":"e","neg_risk":false,
            "outcomes":[{"outcome":"Yes","token_id":"11"},{"outcome":"No","token_id":"12"}]},
            "metadata_revision":"d".repeat(64),"active":true,"closed":false,"accepting_orders":true,"lifecycle_state":"active",
            "start_at":null,"end_at":null,"relations":[],"rules":{"instrument":{"price_increment":"0.01","minimum_order_size":"5"},
            "fee_schedule":{"complete":true,"schedule_id":"fee"}}});
        let p=Publication::start(0,Some(json!({"latest_cursor":0,"books":[],"markets":[market],
            "gaps":[],"catalog_revision":"b".repeat(64)})),
            BTreeMap::from([("11".into(),"1".into()),("12".into(),"1".into())]),8,131072,65536, |_|Ok(())).unwrap();
        let (_,control)=router_managed(p.reader(),Arc::new(serde_json::from_value(config("a","c")).unwrap()),
            DiscoveryLimits{full_sync_bytes:65536,frame_bytes:65536,state_bytes:131072,replay_bytes:65536,
                clients:2,cached_baselines:2,send_timeout:Duration::from_secs(1)},Duration::from_millis(50)).unwrap();
        // Unix-domain socket paths have a small platform limit. Keep the test
        // path independent of a potentially very long Cargo worktree/TMPDIR.
        let short_tmp=if cfg!(target_os="macos") {"/private/tmp"} else {"/tmp"};
        let directory=PathBuf::from(short_tmp).join(format!("mc-control-{}",uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();std::fs::set_permissions(&directory,std::fs::Permissions::from_mode(0o700)).unwrap();
        let path=directory.join("s");
        let task=start(&path,65536,Duration::from_secs(1),Backend::Discovery(control),None,None).unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().permissions().mode()&0o777,0o600);
        let status=command(&path,br#"{"operation":"status"}"#.to_vec()).await;
        assert_eq!(status["result"]["projection_id"],"a".repeat(64));
        let rejected=command(&path,br#"{"operation":"status","operation":"status"}"#.to_vec()).await;
        assert_eq!(rejected["ok"],false);
        let mut request=json!({"operation":"prepare_publication","expected_scope_id":"a".repeat(64),
            "expected_revision":1,"config":config("e","f")});
        let prepared=command(&path,serde_json::to_vec(&request).unwrap()).await;
        assert_eq!(prepared["ok"],true);assert_eq!(prepared["result"]["actual"]["projection_id"],"a".repeat(64));
        assert_eq!(prepared["result"]["acquisition_prepared_by_this_operation"],false);
        request["operation"]=json!("publish_scope");
        let published=command(&path,serde_json::to_vec(&request).unwrap()).await;
        assert_eq!(published["result"]["actual"]["projection_id"],"e".repeat(64));
        assert_eq!(published["result"]["actual"]["revision"],2);
        let stale=command(&path,serde_json::to_vec(&request).unwrap()).await;
        assert_eq!(stale["ok"],false);
        task.abort();let _=task.await;assert!(!path.exists());
        std::fs::remove_dir(&directory).unwrap();p.finish().await.unwrap();
    }
}

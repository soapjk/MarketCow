//! Small operator-owned restart state, not market history. Only the control
//! worker writes this file; the market publication loop never waits for it.
use anyhow::{Context,Result,ensure};
use serde::{Serialize,Deserialize};
use serde_json::{Value,json};
use std::{collections::{BTreeMap,BTreeSet},path::{Path,PathBuf},io::Write,os::unix::fs::{OpenOptionsExt,PermissionsExt}};
use marketcow_polymarket::discovery_source::canonical_hash;

#[derive(Clone,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct State {
    pub schema_version:String,pub catalog_revision:String,pub catalog_manifest_sha256:String,
    pub pool:String,pub active_config:Value,pub active_revision:u64,pub catalog_source:Value,
    pub records:BTreeMap<String,Value>,pub acquisition_market_ids:BTreeSet<String>,
    pub retiring_records:BTreeMap<String,Value>,pub retirement_ticket:Option<u64>,
}
impl State {
    pub fn validate(&self)->Result<()> {
        ensure!(self.schema_version=="marketcow.runtime-scope-state.v1"&&matches!(self.pool.as_str(),"live"|"discovery")
            && self.active_revision>0,"runtime state identity");
        ensure!(self.active_config["catalog_revision"]==self.catalog_revision,"runtime active catalog");
        ensure!(!self.records.is_empty() && self.records.len()+self.retiring_records.len()<=8192,"runtime metadata capacity");
        ensure!(self.retiring_records.is_empty()==self.retirement_ticket.is_none(),"runtime retirement binding");
        let mut tokens=BTreeSet::new();
        for (id,record) in self.records.iter().chain(&self.retiring_records) {
            ensure!(!id.is_empty()&&record["identity"]["market_id"]==*id,"runtime metadata identity");
            let outcomes=record["identity"]["outcomes"].as_array().context("runtime outcomes")?;
            ensure!(outcomes.len()==2 && record["identity"]["condition_id"].as_str().is_some_and(|v|!v.is_empty()),"runtime binary identity");
            for outcome in outcomes {
                let token=outcome["token_id"].as_str().context("runtime token")?;
                ensure!(!token.is_empty()&&token.len()<=128&&tokens.insert(token),"runtime token ownership");
            }
        }
        ensure!(self.acquisition_market_ids.iter().all(|id|self.records.contains_key(id)),"runtime acquisition metadata absent");
        Ok(())
    }
    pub fn markets(&self)->Result<Vec<super::Market>> {
        self.acquisition_market_ids.iter().map(|id|{
            let identity=&self.records.get(id).context("runtime market missing")?["identity"];
            let outcomes=identity["outcomes"].as_array().context("runtime outcomes")?;
            Ok(super::Market{market_id:id.clone(),condition_id:identity["condition_id"].as_str().context("runtime condition")?.into(),
                token_ids:[outcomes[0]["token_id"].as_str().context("runtime token")?.into(),outcomes[1]["token_id"].as_str().context("runtime token")?.into()]})
        }).collect()
    }
}

pub struct Journal {pub state:State,path:PathBuf,maximum_bytes:usize}
pub fn read(path:&Path,maximum_bytes:usize)->Result<Option<State>> {
    ensure!(path.is_absolute()&&(1024..=256*1024*1024).contains(&maximum_bytes),"runtime journal path/budget");
    let meta=match std::fs::symlink_metadata(path) {
        Ok(meta)=>meta,Err(e) if e.kind()==std::io::ErrorKind::NotFound=>return Ok(None),Err(e)=>return Err(e.into()),
    };
    ensure!(meta.is_file()&&meta.permissions().mode()&0o077==0&&meta.len()<=maximum_bytes as u64,"runtime journal file/bytes/permissions");
    let bytes=std::fs::read(path)?;let value:Value=serde_json::from_slice(&bytes)?;
    ensure!(serde_json::to_vec(&value)?==bytes && value.as_object().is_some_and(|v|v.len()==2&&v.contains_key("state")&&v.contains_key("sha256")),"runtime journal canonical envelope");
    ensure!(value["sha256"]==canonical_hash(&value["state"]),"runtime journal hash");
    let state:State=serde_json::from_value(value["state"].clone())?;state.validate()?;Ok(Some(state))
}
impl Journal {
    pub fn open(path:PathBuf,maximum_bytes:usize,initial:State)->Result<Self> {
        let parent=path.parent().context("runtime journal parent")?;
        let meta=std::fs::symlink_metadata(parent)?;
        ensure!(path.is_absolute()&&parent.canonicalize()?==parent&&meta.is_dir()&&meta.permissions().mode()&0o077==0,"private runtime journal directory");
        initial.validate()?;
        let saved=read(&path,maximum_bytes)?;
        if let Some(saved)=&saved {ensure!(saved.catalog_manifest_sha256==initial.catalog_manifest_sha256&&saved.catalog_revision==initial.catalog_revision&&saved.pool==initial.pool,"runtime source binding differs");}
        let mut journal=Self{state:saved.unwrap_or(initial),path,maximum_bytes};
        journal.commit(journal.state.clone())?;Ok(journal)
    }
    pub fn commit(&mut self,next:State)->Result<()> {
        next.validate()?;
        let state=serde_json::to_value(&next)?;
        let bytes=crate::source_public_api::encode_bounded(&json!({"sha256":canonical_hash(&state),"state":state}),self.maximum_bytes)?;
        let parent=self.path.parent().context("runtime journal parent")?;
        let temporary=parent.join(format!(".runtime-scope-{}",uuid::Uuid::new_v4()));
        let result=(||->Result<()>{
            let mut file=std::fs::OpenOptions::new().create_new(true).write(true).mode(0o600).open(&temporary)?;
            file.write_all(&bytes)?;file.sync_all()?;
            std::fs::rename(&temporary,&self.path)?;std::fs::File::open(parent)?.sync_all()?;Ok(())
        })();
        if result.is_err(){let _=std::fs::remove_file(&temporary);}
        result?;self.state=next;Ok(())
    }
    pub fn admit(&mut self,records:&[Value],acquisition:&BTreeSet<String>)->Result<()> {
        let mut next=self.state.clone();
        for record in records {
            let id=record["identity"]["market_id"].as_str().context("admission identity")?;
            ensure!(!next.retiring_records.contains_key(id),"runtime retirement pending");
            if let Some(old)=next.records.get(id){ensure!(old["identity"]==record["identity"],"runtime identity changed");}
            else {next.records.insert(id.into(),record.clone());}
        }
        next.acquisition_market_ids.extend(acquisition.iter().cloned());self.commit(next)
    }
    pub fn retire(&mut self,markets:&BTreeSet<String>,ticket:u64)->Result<()> {
        if self.state.retirement_ticket==Some(ticket) && self.state.retiring_records.keys().cloned().collect::<BTreeSet<_>>()==*markets {
            return Ok(()); // explicit retry of the same durable, unqueued intent
        }
        ensure!(self.state.retiring_records.is_empty()&&ticket>0,"runtime retirement slot busy");
        let mut next=self.state.clone();
        for id in markets {
            let record=next.records.remove(id).context("runtime retirement unknown market")?;
            next.retiring_records.insert(id.clone(),record);next.acquisition_market_ids.remove(id);
        }
        next.retirement_ticket=Some(ticket);self.commit(next)
    }
    pub fn settled(&mut self,ticket:u64)->Result<()> {
        if self.state.retirement_ticket.is_some_and(|pending|pending<=ticket) {
            let mut next=self.state.clone();next.retiring_records.clear();next.retirement_ticket=None;self.commit(next)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn record(id:&str)->Value {json!({"identity":{"market_id":id,"condition_id":format!("condition{id}"),
        "outcomes":[{"token_id":format!("{id}1")},{"token_id":format!("{id}2")}]}})}
    #[test]
    fn restart_retains_admitted_metadata_and_pending_retirement_until_durable_receipt() {
        let directory=std::env::temp_dir().canonicalize().unwrap().join(format!("mc-journal-{}",uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();std::fs::set_permissions(&directory,std::fs::Permissions::from_mode(0o700)).unwrap();
        let path=directory.join("state.json");
        let initial=State{schema_version:"marketcow.runtime-scope-state.v1".into(),catalog_revision:"c".into(),
            catalog_manifest_sha256:"m".into(),pool:"live".into(),active_config:json!({"catalog_revision":"c"}),active_revision:1,
            catalog_source:Value::Null,records:BTreeMap::from([("1".into(),record("1"))]),
            acquisition_market_ids:BTreeSet::from(["1".into()]),retiring_records:Default::default(),retirement_ticket:None};
        let mut journal=Journal::open(path.clone(),65536,initial.clone()).unwrap();
        journal.admit(&[record("2")],&BTreeSet::from(["2".into()])).unwrap();
        let loaded=read(&path,65536).unwrap().unwrap();assert_eq!(loaded.markets().unwrap().len(),2);
        journal.retire(&BTreeSet::from(["1".into()]),3).unwrap();
        let loaded=read(&path,65536).unwrap().unwrap();assert_eq!(loaded.markets().unwrap().len(),1);
        assert_eq!(loaded.retiring_records.len(),1);assert_eq!(loaded.retirement_ticket,Some(3));
        journal.settled(2).unwrap();assert_eq!(journal.state.retiring_records.len(),1);
        journal.settled(3).unwrap();assert!(read(&path,65536).unwrap().unwrap().retiring_records.is_empty());
        let before=std::fs::read(&path).unwrap();
        assert!(journal.admit(&[record("3")],&BTreeSet::from(["unknown".into()])).is_err());
        assert_eq!(std::fs::read(&path).unwrap(),before);
        std::fs::write(&path,b"{}").unwrap();assert!(read(&path,65536).is_err());
        std::fs::remove_file(path).unwrap();std::fs::remove_dir(directory).unwrap();
    }
}

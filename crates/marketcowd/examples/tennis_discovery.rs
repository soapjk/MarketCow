//! Finite retrospective availability research, never a production catalog update.
use anyhow::{ensure, Result};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{collections::BTreeSet, path::PathBuf, time::{Duration, Instant}};
const ONE: usize = 1048576;
const HOST: &str = "https://gamma-api.polymarket.com";
fn digest(b: &[u8]) -> String { hex::encode(Sha256::digest(b)) }
fn config() -> Value { json!({"max_gets":6,"seconds":100,"start_spacing_seconds":1,
    "request_seconds":15,"response_bytes":ONE,"total_bytes":4*ONE,
    "start_time_min":"2026-06-02T00:00:00Z","start_time_max":"2026-09-02T00:00:00Z",
    "local_upper_exclusive":true,"tag_slug":"tennis","related_tags":false,
    "limit":10,"order":"id","ascending":true,"proxy":"reqwest system default",
    "retry":false,"redirect":false,"capture_semantics":"retrospective, not as-of"}) }
#[derive(Default)]
struct Stream { closed: bool, pages: usize, next: Option<String>, seen: BTreeSet<String> }
impl Stream {
    fn url(&self, supplied: Option<&str>) -> Result<url::Url> {
        ensure!(self.pages<2 && supplied==self.next.as_deref(),"cursor_binding");
        ensure!(self.pages==0 || supplied.is_some(),"stream_exhausted");
        let mut u=url::Url::parse(&format!("{HOST}/events/keyset"))?;
        u.query_pairs_mut().extend_pairs([
            ("tag_slug","tennis"),("related_tags","false"),("closed",if self.closed {"true"} else {"false"}),
            ("limit","10"),("order","id"),("ascending","true"),
            ("start_time_min","2026-06-02T00:00:00Z"),("start_time_max","2026-09-02T00:00:00Z")]);
        if let Some(c)=supplied {u.query_pairs_mut().append_pair("after_cursor",c);}
        Ok(u)
    }
    fn accept(&mut self,v:&Value)->Result<()> {
        let events=v["events"].as_array().ok_or_else(||anyhow::anyhow!("events_missing"))?;
        ensure!(events.len()<=10,"page_count");
        let next=match v.get("next_cursor") {
            None | Some(Value::Null)=>None,
            Some(Value::String(s)) if !s.is_empty() && s.len()<=8192=>Some(s.clone()),
            _=>anyhow::bail!("cursor_schema")
        };
        if let Some(c)=&next { ensure!(!events.is_empty() && self.seen.insert(c.clone()),"repeated_cursor"); }
        self.next=next; self.pages+=1; Ok(())
    }
}
fn admission(elapsed:Duration, count:usize, bytes:usize)->Result<Duration> {
    ensure!(elapsed<Duration::from_secs(100) && count<6 && bytes<4*ONE,"total_budget");
    Ok((Duration::from_secs(100)-elapsed).min(Duration::from_secs(15)))
}
struct Probe {client:reqwest::Client, root:PathBuf, start:Instant, last:Option<Instant>, bytes:usize, rows:Vec<Value>, failed:bool}
impl Probe {
    async fn get(&mut self,u:url::Url)->Result<Value> {
        ensure!(!self.failed,"previous_failure");
        if let Some(t)=self.last {
            if let Some(wait)=Duration::from_secs(1).checked_sub(t.elapsed()) {tokio::time::sleep(wait).await;}
        }
        let timeout=admission(self.start.elapsed(),self.rows.len(),self.bytes)?;
        self.last=Some(Instant::now());
        let started=chrono::Utc::now(); let mut raw=Vec::new(); let mut status=None; let mut complete=false;
        let mut observed_bytes=0usize;
        let budget=ONE.min(4*ONE-self.bytes);
        let result=tokio::time::timeout(timeout,async {
            let mut response=self.client.get(u.clone()).timeout(timeout).send().await.map_err(|_|anyhow::anyhow!("transport_failed"))?;
            status=Some(response.status().as_u16());
            while let Some(chunk)=response.chunk().await.map_err(|_|anyhow::anyhow!("body_failed"))? {
                observed_bytes+=chunk.len();
                let keep=chunk.len().min(budget-raw.len()); raw.extend_from_slice(&chunk[..keep]);
                ensure!(observed_bytes<=budget,"body_budget");
            }
            complete=true; ensure!(status==Some(200),"http_rejected");
            Ok::<Value,anyhow::Error>(serde_json::from_slice(&raw)?)
        }).await;
        self.bytes+=observed_bytes;
        let error=match &result {Err(_)=>Some("deadline".to_string()),Ok(Err(e))=>Some(e.to_string()),_=>None};
        let name=format!("{:02}.raw",self.rows.len());
        std::fs::write(self.root.join(&name),&raw)?;
        self.rows.push(json!({"url":u.as_str(),"started_at":started,"received_at":chrono::Utc::now(),
            "status":status,"raw_complete":complete,"raw_file":name,"raw_sha256":digest(&raw),
            "retained_bytes":raw.len(),"observed_chunk_bytes":observed_bytes,"error":error}));
        self.failed=error.is_some();
        match result {Ok(v)=>v,Err(_)=>anyhow::bail!("deadline")}
    }
}
#[tokio::main]
async fn main()->Result<()> {
    let root=PathBuf::from(std::env::args().nth(1).expect("new absolute output directory"));
    ensure!(root.is_absolute(),"absolute output"); std::fs::create_dir(&root)?;
    let client=reqwest::Client::builder().redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never()).timeout(Duration::from_secs(15)).build()?;
    let mut p=Probe{client,root,start:Instant::now(),last:None,bytes:0,rows:vec![],failed:false};
    let mut streams=[Stream{closed:true,..Default::default()},Stream::default()];
    let result:Result<()>=async {
        let tag=p.get(url::Url::parse(&format!("{HOST}/tags/slug/tennis"))?).await?;
        ensure!(tag["slug"]=="tennis" && tag["id"].as_str().is_some_and(|s|!s.is_empty())
            && tag["label"].as_str().is_some_and(|s|!s.is_empty()),"tag_identity");
        let sports=p.get(url::Url::parse(&format!("{HOST}/sports"))?).await?;
        ensure!(sports.is_array(),"sports_schema");
        for page in 0..2 {
            for stream in &mut streams {
                if page==1 && stream.next.is_none() {continue;}
                let value=p.get(stream.url(stream.next.as_deref())?).await?;
                stream.accept(&value)?;
            }
        }
        Ok(())
    }.await;
    let report=json!({"config":config(),"config_sha256":digest(&serde_json::to_vec(&config())?),
        "code_sha256":digest(include_bytes!("tennis_discovery.rs")),"requests":p.rows,
        "elapsed_seconds":p.start.elapsed().as_secs_f64(),"observed_chunk_bytes":p.bytes,
        "error":result.as_ref().err().map(|e|e.to_string()),"coverage_complete":false,
        "cursors":streams.iter().map(|s|json!({"closed":s.closed,"pages":s.pages,"next_cursor":s.next})).collect::<Vec<_>>(),
        "budget_boundary":"overlimit chunk may be received; retained prefix capped; failure stops; no hard realtime guarantee"});
    let raw=serde_json::to_vec_pretty(&report)?;std::fs::write(p.root.join("report.json"),&raw)?;
    println!("report={} sha256={} requests={} error={:?}",p.root.display(),digest(&raw),p.rows.len(),result.as_ref().err());
    result
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test] fn budgets(){assert!(admission(Duration::from_secs(100),0,0).is_err());assert!(admission(Duration::ZERO,6,0).is_err());assert!(admission(Duration::ZERO,0,4*ONE).is_err());assert_eq!(admission(Duration::from_secs(99),1,0).unwrap(),Duration::from_secs(1));}
    #[test] fn independent_cursors(){let mut a=Stream{closed:true,..Default::default()};let mut b=Stream::default();a.accept(&json!({"events":[{}],"next_cursor":"a"})).unwrap();b.accept(&json!({"events":[{}],"next_cursor":"b"})).unwrap();assert!(a.url(Some("b")).is_err());assert!(b.url(Some("a")).is_err());assert!(a.url(Some("a")).unwrap().as_str().contains("closed=true"));assert!(a.accept(&json!({"events":[{}],"next_cursor":"a"})).is_err());}
    #[test] fn exhausted_and_malformed(){let mut s=Stream::default();assert!(s.accept(&json!({"events":[],"next_cursor":"x"})).is_err());s.accept(&json!({"events":[],"next_cursor":null})).unwrap();assert!(s.url(None).is_err());}
    #[test] fn omitted_terminal_cursor(){let mut s=Stream::default();s.accept(&json!({"events":[{"id":"1"}]})).unwrap();assert_eq!(s.pages,1);assert!(s.next.is_none());assert!(s.url(None).is_err());}
    #[tokio::test] async fn failure_stops_without_network(){let root=tempfile::tempdir().unwrap();let mut p=Probe{client:reqwest::Client::new(),root:root.path().into(),start:Instant::now(),last:None,bytes:0,rows:vec![],failed:true};assert!(p.get(url::Url::parse("http://127.0.0.1:1").unwrap()).await.is_err());assert!(p.rows.is_empty());}
    #[tokio::test] async fn complete_rejection_and_truncation(){
        for oversized in [false,true] {
            let raw=if oversized {vec![b'x';ONE+1]} else {b"denied".to_vec()};
            let app=axum::Router::new().route("/",axum::routing::get(move || {let raw=raw.clone();async move {(axum::http::StatusCode::FORBIDDEN,raw)}}));
            let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url=url::Url::parse(&format!("http://{}/",listener.local_addr().unwrap())).unwrap();
            let task=tokio::spawn(async move {axum::serve(listener,app).await.unwrap()});
            let root=tempfile::tempdir().unwrap();let mut p=Probe{client:reqwest::Client::builder().no_proxy().build().unwrap(),root:root.path().into(),start:Instant::now(),last:None,bytes:0,rows:vec![],failed:false};
            assert!(p.get(url.clone()).await.is_err());assert_eq!(p.rows[0]["raw_complete"],!oversized);
            assert_eq!(p.rows[0]["retained_bytes"],if oversized {ONE} else {6});
            assert!(p.get(url).await.is_err());assert_eq!(p.rows.len(),1);
            task.abort();let _=task.await;
        }
    }
}

//! Fixed ten-minute, bounded diagnostics. No file/process IO on a data send.
use serde_json::{Value,json};
use std::{path::PathBuf,sync::{Arc,atomic::{AtomicU64,Ordering},mpsc},
    time::{Duration,Instant},io::Write};
use tokio::io::AsyncReadExt;

const RECORDS:usize=256;
const FILE_BYTES:usize=64*1024*1024;
const TCP_BYTES:u64=64*1024;
const CAPTURE:Duration=Duration::from_secs(600);

#[derive(Clone)]
pub(super) struct Diagnostics {
    sender:Option<mpsc::SyncSender<Value>>,
    dropped:Arc<AtomicU64>,
    end:Instant,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn diagnostic_full_queue_drops_without_waiting() {
        let (sender,receiver)=mpsc::sync_channel(1);
        let diagnostics=Diagnostics{sender:Some(sender),dropped:Arc::new(AtomicU64::new(0)),
            end:Instant::now()+CAPTURE};
        diagnostics.record(json!({"first":true}));
        diagnostics.record(json!({"second":true}));
        assert_eq!(diagnostics.dropped.load(Ordering::Relaxed),1);
        assert_eq!(receiver.try_recv().unwrap(),json!({"first":true}));
    }
    #[test]
    fn expired_capture_does_not_enqueue() {
        let (sender,receiver)=mpsc::sync_channel(1);
        let diagnostics=Diagnostics{sender:Some(sender),dropped:Arc::new(AtomicU64::new(0)),
            end:Instant::now()};
        diagnostics.record(json!({"late":true}));
        assert!(receiver.try_recv().is_err());
        assert_eq!(diagnostics.dropped.load(Ordering::Relaxed),0);
    }
}
impl Diagnostics {
    pub(super) fn start(root:Option<PathBuf>)->Self {
        let end=Instant::now()+CAPTURE;
        let dropped=Arc::new(AtomicU64::new(0));
        let Some(root)=root else {return Self{sender:None,dropped,end};};
        let (tx,rx)=mpsc::sync_channel::<Value>(RECORDS);
        let counts=dropped.clone();
        let capture_id=uuid::Uuid::new_v4().simple().to_string();
        let id=capture_id.clone();
        let spawned=std::thread::Builder::new().name("market-send-evidence".into()).spawn(move||{
            let result=(||->std::io::Result<()> {
                use std::os::unix::fs::OpenOptionsExt;
                let dir=root.join("diagnostics");
                std::fs::create_dir_all(&dir)?;
                let path=dir.join(format!("market-send-{id}.jsonl"));
                let file=std::fs::OpenOptions::new().write(true).create_new(true)
                    .mode(0o600).open(&path)?;
                let mut writer=std::io::BufWriter::new(file);
                let mut bytes=0usize;
                let header=json!({"stage":"market_send_capture_started","capture_id":id,
                    "observed_at":chrono::Utc::now(),"duration_seconds":600,
                    "maximum_file_bytes":FILE_BYTES,"queue_records":RECORDS});
                writeln!(writer,"{header}")?;
                marketcow_polymarket::async_eprintln!("{}",json!({
                    "stage":"market_send_capture_file","path":path,"capture_id":id}));
                let mut budget_exhausted=false;
                loop {
                    let next=if Instant::now()>=end {
                        match rx.try_recv(){Ok(record)=>Ok(record),Err(_)=>break}
                    } else {rx.recv_timeout(Duration::from_secs(1))};
                    match next {
                        Ok(value)=>{
                            let encoded=serde_json::to_vec(&value).map_err(std::io::Error::other)?;
                            if bytes.saturating_add(encoded.len()+1)>FILE_BYTES-4096 {
                                budget_exhausted=true;break;
                            }
                            writer.write_all(&encoded)?;writer.write_all(b"\n")?;
                            bytes+=encoded.len()+1;
                        }
                        Err(mpsc::RecvTimeoutError::Timeout)=>{writer.flush()?;},
                        Err(mpsc::RecvTimeoutError::Disconnected)=>break,
                    }
                }
                writeln!(writer,"{}",json!({"stage":"market_send_capture_finished",
                    "capture_id":id,"observed_at":chrono::Utc::now(),
                    "budget_exhausted":budget_exhausted,"record_bytes":bytes,
                    "dropped_records":counts.load(Ordering::Relaxed)}))?;
                writer.flush()
            })();
            if let Err(error)=result {marketcow_polymarket::async_eprintln!("{}",json!({
                "stage":"market_send_capture_failed","error":error.to_string(),"capture_id":id}));}
        });
        let this=Self{sender:spawned.ok().map(|_|tx),dropped,end};
        if this.sender.is_some(){
            let mirror=this.clone();
            marketcow_polymarket::diagnostic::install_observer(move |line| {
                if ["polymarket_ws_hot_rotation", "polymarket_receive_lag_sample",
                    "polymarket_receive_stage_window", "market_hot_checkpoint",
                    "market_stream_slow_send", "btc_5m_market_stream_closed",
                    "diagnostic_log_dropped"].iter().any(|stage|line.contains(stage)) {
                    if let Ok(value)=serde_json::from_str::<Value>(line) { mirror.record(value); }
                }
            });
            this.start_sampler();
        }
        this
    }
    pub(super) fn record(&self, value:Value) {
        if Instant::now()>=self.end{return;}
        if let Some(tx)=&self.sender {
            if tx.try_send(value).is_err(){self.dropped.fetch_add(1,Ordering::Relaxed);}
        }
    }
    fn start_sampler(&self) {
        let this=self.clone();
        // Explicit formal listen port, read from existing process arguments.
        // No socket guessing or all-host credential/process dumps.
        let args=std::env::args().collect::<Vec<_>>();
        let port=args.windows(2).find(|pair|pair[0]=="--public-listen")
            .and_then(|pair|pair[1].parse::<std::net::SocketAddr>().ok()).map(|a|a.port());
        tokio::spawn(async move {
            let mut interval=tokio::time::interval(Duration::from_millis(100));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut max_late=0u128;let mut ticks=0u64;
            while Instant::now()<this.end {
                let scheduled=interval.tick().await;
                let now=tokio::time::Instant::now();
                max_late=max_late.max(now.saturating_duration_since(scheduled).as_micros());
                ticks+=1;
                if ticks%10==0 {
                    this.record(json!({"stage":"market_runtime_schedule_window",
                        "observed_at":chrono::Utc::now(),"ticks":10,"max_late_us":max_late}));
                    max_late=0;
                }
            }
        });
        let this=self.clone();
        if let Some(port)=port {tokio::spawn(async move {
            let mut interval=tokio::time::interval(Duration::from_secs(1));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            while Instant::now()<this.end {
                interval.tick().await;
                let sample=async {
                    let mut child=tokio::process::Command::new("ss")
                        .args(["-tinmH",&format!("sport = :{port}")])
                        .stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::null())
                        .kill_on_drop(true).spawn()?;
                    let mut bytes=Vec::new();
                    child.stdout.take().expect("piped ss stdout").take(TCP_BYTES+1)
                        .read_to_end(&mut bytes).await?;
                    if bytes.len() as u64>TCP_BYTES {return Ok::<_,std::io::Error>((bytes,true));}
                    let status=child.wait().await?;
                    if !status.success() {return Err(std::io::Error::other(format!("ss exited {status}")));}
                    Ok((bytes,false))
                };
                let value=match tokio::time::timeout(Duration::from_millis(800),sample).await {
                    Ok(Ok((bytes,truncated)))=>json!({"raw_ss":String::from_utf8_lossy(&bytes),
                        "truncated":truncated}),
                    Ok(Err(error))=>json!({"error":error.to_string()}),
                    Err(_)=>json!({"error":"ss_sampling_timeout"}),
                };
                this.record(json!({"stage":"market_tcp_sample","observed_at":chrono::Utc::now(),
                    "listen_port":port,"sample":value}));
            }
        });}
    }
}

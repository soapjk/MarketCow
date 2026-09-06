//! Market FIFO CPU actors and a single cursor allocator. Queue exhaustion is
//! an explicit market invalidation barrier; results from before it cannot land.
use super::*;
use crate::source_dispatch::{Completion, Dispatcher};
use marketcow_polymarket::RawTransportFrame;
use std::sync::{Arc, Mutex};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

const INPUT_BYTES: usize = 64 * 1024 * 1024;
const FRAME_BYTES: usize = 8 * 1024 * 1024;
const OUTPUT_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone, Default)]
struct Control {
    generation: u64,
    attempts: BTreeMap<String, String>,
}
struct State {
    adapter: Adapter,
    generation: u64,
    control: Arc<Mutex<Control>>,
}
struct Input {
    generation: u64,
    frame: RawTransportFrame,
    recovery: Vec<(String, String)>,
    rest_recovery: Option<Vec<Value>>,
    replacement_reason: Option<String>,
    _bytes: OwnedSemaphorePermit,
}
pub(super) struct Output {
    generation: u64,
    events: Vec<Value>,
}
pub(super) struct Pipeline {
    dispatch: Dispatcher<Input>,
    controls: BTreeMap<String, Arc<Mutex<Control>>>,
    bytes: Arc<Semaphore>,
}

// Measure without allocating another serialized copy of each full order book.
fn bounded_size(value: &impl serde::Serialize, limit: usize) -> Result<usize> {
    struct Counter {
        bytes: usize,
        limit: usize,
    }
    impl std::io::Write for Counter {
        fn write(&mut self, value: &[u8]) -> std::io::Result<usize> {
            if value.len() > self.limit.saturating_sub(self.bytes) {
                return Err(std::io::Error::other("frame byte limit"));
            }
            self.bytes += value.len();
            Ok(value.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter { bytes: 0, limit };
    serde_json::to_writer(&mut counter, value)?;
    Ok(counter.bytes.max(1))
}

impl Pipeline {
    pub(super) fn start(router: &Adapter) -> Result<(Self, mpsc::Receiver<Completion<Output>>)> {
        let markets: BTreeSet<_> = router.identities.values().map(|(m, _)| m.clone()).collect();
        let mut controls = BTreeMap::new();
        let states = markets
            .into_iter()
            .map(|market| {
                let control = Arc::new(Mutex::new(Control::default()));
                controls.insert(market.clone(), control.clone());
                (
                    market.clone(),
                    State {
                        adapter: router.market_actor(&market),
                        generation: 0,
                        control,
                    },
                )
            })
            .collect();
        // Four output reservations also bound maximum retained result bytes to
        // 4 * OUTPUT_BYTES. At most four CPU tasks and two queued inputs/market.
        let (dispatch, output) = Dispatcher::start(states, 2, 4, 4, |state, input: Input| {
            let control = state
                .control
                .lock()
                .map_err(|_| anyhow::anyhow!("market control poisoned"))?
                .clone();
            if input.generation != control.generation {
                return Ok(Output {
                    generation: input.generation,
                    events: vec![],
                });
            }
            if state.generation != control.generation {
                state.adapter.reset_connection();
                state.adapter.recoveries = control.attempts;
                state.generation = control.generation;
            }
            if !input.recovery.is_empty()
                && input
                    .recovery
                    .iter()
                    .all(|(token, attempt)| state.adapter.recoveries.get(token) != Some(attempt))
            {
                return Ok(Output {
                    generation: input.generation,
                    events: vec![],
                });
            }
            let events = if let Some(raw_books) = input.rest_recovery {
                if let Some(reason) = input.replacement_reason {
                    let tokens = state.adapter.identities.keys().cloned().collect::<Vec<_>>();
                    let mut events = Vec::new();
                    for token in tokens {
                        events.push(state.adapter.invalidate(
                            &token,
                            &reason,
                            input.frame.received_at,
                            events.len() as u64 + 1,
                        )?);
                    }
                    let attempts = state.adapter.recoveries.clone();
                    events.extend(state.adapter.apply_rest_recovery(
                        raw_books,
                        input.frame.received_at,
                        events.len() as u64,
                        &attempts,
                    )?);
                    events
                } else {
                    let attempts = input.recovery.into_iter().collect();
                    state.adapter.apply_rest_recovery(
                        raw_books,
                        input.frame.received_at,
                        0,
                        &attempts,
                    )?
                }
            } else {
                state
                    .adapter
                    .apply_isolated(input.frame.raw_payload, input.frame.received_at, 0)?
            };
            bounded_size(&events, OUTPUT_BYTES)?;
            Ok(Output {
                generation: input.generation,
                events,
            })
        })?;
        Ok((
            Self {
                dispatch,
                controls,
                bytes: Arc::new(Semaphore::new(INPUT_BYTES)),
            },
            output,
        ))
    }

    fn route<'a>(router: &'a Adapter, frame: &RawTransportFrame) -> Result<&'a str> {
        if let Some(token) = frame.raw_payload["asset_id"].as_str()
            && let Some((market, _)) = router.identities.get(token)
        {
            return Ok(market);
        }
        if let Some(condition) = frame.raw_payload["market"].as_str()
            && let Some((market, _)) = router.identities.values().find(|(_, c)| c == condition)
        {
            return Ok(market);
        }
        anyhow::bail!("unroutable source frame")
    }

    pub(super) async fn submit(
        &self,
        router: &mut Adapter,
        publication: &mut Publication,
        frame: RawTransportFrame,
        recovery: Option<(String, String)>,
    ) -> Result<()> {
        // Frozen scopes deliberately ignore new-market announcements.
        if frame.raw_payload["event_type"] == "new_market" {
            return Ok(());
        }
        let market = Self::route(router, &frame)?.to_owned();
        let generation = self.controls[&market]
            .lock()
            .map_err(|_| anyhow::anyhow!("market control poisoned"))?
            .generation;
        let bytes = bounded_size(&frame, FRAME_BYTES);
        if let Ok(bytes) = bytes
            && let Ok(permit) = self.bytes.clone().try_acquire_many_owned(bytes as u32)
        {
            let input = Input {
                generation,
                frame,
                recovery: recovery.into_iter().collect(),
                rest_recovery: None,
                replacement_reason: None,
                _bytes: permit,
            };
            if self.dispatch.try_submit(&market, input).is_ok() {
                return Ok(());
            }
        }
        self.invalidate_market(router, publication, &market, "market_queue_capacity")
            .await
    }

    pub(super) async fn submit_rest_recovery(
        &self,
        router: &mut Adapter,
        publication: &mut Publication,
        raw_books: Vec<Value>,
        received_at: chrono::DateTime<Utc>,
        attempts: Vec<(String, String)>,
    ) -> Result<()> {
        let received_at = chrono::DateTime::from_timestamp_micros(received_at.timestamp_micros())
            .context("REST recovery receipt timestamp")?;
        let token = attempts
            .first()
            .context("REST recovery attempt missing")?
            .0
            .clone();
        let market = router
            .identities
            .get(&token)
            .context("REST recovery token")?
            .0
            .clone();
        let generation = self.controls[&market]
            .lock()
            .map_err(|_| anyhow::anyhow!("market control poisoned"))?
            .generation;
        let bytes = bounded_size(&raw_books, FRAME_BYTES)?;
        let frame = RawTransportFrame {
            raw_payload: raw_books
                .first()
                .context("REST recovery pair missing")?
                .clone(),
            received_at,
        };
        let permit = self
            .bytes
            .clone()
            .try_acquire_many_owned(bytes as u32)
            .map_err(|_| anyhow::anyhow!("REST recovery byte capacity"))?;
        let input = Input {
            generation,
            frame,
            recovery: attempts,
            rest_recovery: Some(raw_books),
            replacement_reason: None,
            _bytes: permit,
        };
        match self.dispatch.try_submit(&market, input) {
            Ok(()) => Ok(()),
            Err(_) => {
                self.invalidate_market(router, publication, &market, "market_queue_capacity")
                    .await
            }
        }
    }

    pub(super) async fn submit_rest_replacement(
        &self,
        router: &mut Adapter,
        publication: &mut Publication,
        raw_books: Vec<Value>,
        received_at: chrono::DateTime<Utc>,
        market: &str,
        reason: &str,
    ) -> Result<()> {
        let received_at = chrono::DateTime::from_timestamp_micros(received_at.timestamp_micros())
            .context("REST replacement receipt timestamp")?;
        let generation = self.controls[market]
            .lock()
            .map_err(|_| anyhow::anyhow!("market control poisoned"))?
            .generation;
        let bytes = bounded_size(&raw_books, FRAME_BYTES)?;
        let frame = RawTransportFrame {
            raw_payload: raw_books
                .first()
                .context("REST replacement pair missing")?
                .clone(),
            received_at,
        };
        let permit = self
            .bytes
            .clone()
            .try_acquire_many_owned(bytes as u32)
            .map_err(|_| anyhow::anyhow!("REST replacement byte capacity"))?;
        let input = Input {
            generation,
            frame,
            recovery: Vec::new(),
            rest_recovery: Some(raw_books),
            replacement_reason: Some(reason.to_owned()),
            _bytes: permit,
        };
        match self.dispatch.try_submit(market, input) {
            Ok(()) => Ok(()),
            Err(_) => {
                self.invalidate_market(router, publication, market, "market_queue_capacity")
                    .await
            }
        }
    }

    pub(super) async fn invalidate_market(
        &self,
        router: &mut Adapter,
        publication: &mut Publication,
        market: &str,
        reason: &str,
    ) -> Result<()> {
        let tokens: Vec<_> = router
            .identities
            .iter()
            .filter(|(_, (m, _))| m == market)
            .map(|(t, _)| t.clone())
            .collect();
        let mut events = Vec::new();
        for token in &tokens {
            events.push(
                router.invalidate(
                    token,
                    reason,
                    Utc::now(),
                    publication
                        .cursor()?
                        .checked_add(events.len() as u64 + 1)
                        .context("cursor overflow")?,
                )?,
            );
        }
        publication.publish_with_capacity(events).await?;
        let mut control = self.controls[market]
            .lock()
            .map_err(|_| anyhow::anyhow!("market control poisoned"))?;
        control.generation = control
            .generation
            .checked_add(1)
            .context("market generation overflow")?;
        control.attempts = tokens
            .into_iter()
            .map(|token| {
                let attempt = router.recoveries[&token].clone();
                (token, attempt)
            })
            .collect();
        Ok(())
    }

    pub(super) fn retire_market(&self, router: &mut Adapter, market: &str) -> Result<()> {
        let mut control = self.controls[market]
            .lock()
            .map_err(|_| anyhow::anyhow!("market control poisoned"))?;
        control.generation = control
            .generation
            .checked_add(1)
            .context("market generation overflow")?;
        control.attempts.clear();
        let retired: Vec<_> = router
            .identities
            .iter()
            .filter(|(_, (owner, _))| owner == market)
            .map(|(token, _)| token.clone())
            .collect();
        for token in retired {
            router.recoveries.remove(&token);
        }
        Ok(())
    }

    pub(super) async fn publish(
        &self,
        router: &mut Adapter,
        publication: &mut Publication,
        completion: Completion<Output>,
    ) -> Result<()> {
        let market = completion.entity;
        let mut output = match completion.result {
            Ok(output) => output,
            Err(error) => {
                eprintln!(
                    "{}",
                    json!({"stage":"market_worker_rejected","market_id":market,"detail":error.to_string()})
                );
                return self
                    .invalidate_market(router, publication, &market, "market_worker_rejected")
                    .await;
            }
        };
        if output.generation
            != self.controls[&market]
                .lock()
                .map_err(|_| anyhow::anyhow!("market control poisoned"))?
                .generation
        {
            return Ok(()); // Older CPU results cannot cross the invalidation barrier.
        }
        if output.events.is_empty() {
            return Ok(());
        }
        rebase(&mut output.events, publication.cursor()?)?;
        // Keep only the scheduler's attempt map here; book reducers stay in
        // their actors. Publication performs the authoritative transition check.
        let transitions: Vec<_> = output
            .events
            .iter()
            .filter(|e| marketcow_runtime::discovery_source::is_token_recovery(e))
            .map(|e| {
                (
                    e["event_type"] == "recovery_started",
                    e["token_id"].as_str().unwrap().to_owned(),
                    e["canonical_payload"]["recovery_id"]
                        .as_str()
                        .unwrap()
                        .to_owned(),
                )
            })
            .collect();
        publication.publish_with_capacity(output.events).await?;
        let mut control = self.controls[&market]
            .lock()
            .map_err(|_| anyhow::anyhow!("market control poisoned"))?;
        for (started, token, attempt) in transitions {
            if started {
                router.recoveries.insert(token.clone(), attempt.clone());
                control.attempts.insert(token, attempt);
            } else {
                router.recoveries.remove(&token);
                control.attempts.remove(&token);
            }
        }
        Ok(())
    }

    pub(super) async fn finish(
        mut self,
        router: &mut Adapter,
        publication: &mut Publication,
        output: &mut mpsc::Receiver<Completion<Output>>,
    ) -> Result<()> {
        // Stop accepting actor input, then publish every already-accepted
        // completion while actors drain. Accepted work must not disappear just
        // because shutdown raced its CPU reducer.
        self.dispatch.close();
        while let Some(value) = output.recv().await {
            self.publish(router, publication, value).await?;
        }
        self.dispatch.join().await;
        Ok(())
    }
}

fn rebase(events: &mut [Value], after: u64) -> Result<()> {
    for event in events {
        let local = event["cursor"].as_u64().context("local cursor")?;
        event["cursor"] = json!(after.checked_add(local).context("cursor overflow")?);
        if event["event_type"] == "recovery_completed" {
            let snapshot = event["canonical_payload"]["snapshot_cursor"]
                .as_u64()
                .context("snapshot cursor")?;
            ensure!(
                snapshot > 0 && snapshot < local,
                "snapshot must belong to this actor result"
            );
            event["canonical_payload"]["snapshot_cursor"] =
                json!(after.checked_add(snapshot).context("snapshot overflow")?);
            event["canonical_payload_sha256"] = json!(canonical_hash(&event["canonical_payload"]));
        }
        event.as_object_mut().context("event")?.remove("event_id");
        event["event_id"] = json!(canonical_hash(event));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{SinkExt, StreamExt};
    fn fixture() -> Adapter {
        let plan = Plan {
            schema_version: "test".into(),
            catalog_revision: "catalog".into(),
            markets: vec![
                super::super::super::Market {
                    market_id: "1".into(),
                    condition_id: "c1".into(),
                    token_ids: ["11".into(), "12".into()],
                },
                super::super::super::Market {
                    market_id: "2".into(),
                    condition_id: "c2".into(),
                    token_ids: ["21".into(), "22".into()],
                },
            ],
        };
        Adapter::new(&plan, &json!({"scope_id":"scope","markets":[{"identity":{"market_id":"1"},"lifecycle_state":"active"},
            {"identity":{"market_id":"2"},"lifecycle_state":"active"}],"books":[{"token_id":"11","tick_size":"0.01"},
            {"token_id":"12","tick_size":"0.01"},{"token_id":"21","tick_size":"0.01"},{"token_id":"22","tick_size":"0.01"}]})).unwrap()
    }
    fn book(token: &str) -> RawTransportFrame {
        let now = Utc::now().with_nanosecond(0).unwrap();
        RawTransportFrame {
            received_at: now,
            raw_payload: json!({"event_type":"book","asset_id":token,
            "market":if token.starts_with('1') {"c1"} else {"c2"},"timestamp":now.timestamp_millis().to_string(),
            "hash":"source","bids":[{"price":"0.4","size":"10"}],"asks":[{"price":"0.6","size":"10"}]}),
        }
    }
    async fn next(output: &mut mpsc::Receiver<Completion<Output>>) -> Completion<Output> {
        tokio::time::timeout(std::time::Duration::from_secs(2), output.recv())
            .await
            .unwrap()
            .unwrap()
    }
    #[tokio::test]
    async fn rest_replacement_is_one_atomic_two_token_recovery_batch() {
        let mut router = fixture();
        let tokens = router
            .identities
            .iter()
            .map(|(token, (market, _))| (token.clone(), market.clone()))
            .collect();
        let written = Arc::new(Mutex::new(Vec::<Vec<Value>>::new()));
        let capture = written.clone();
        let base = json!({"latest_cursor":0,"books":[],"markets":[],"gaps":[],
            "schema_version":"marketcow.polymarket.live-stream.v1","type":"state"});
        let mut publication =
            Publication::start(0, Some(base), tokens, 32, 1048576, 65536, move |batches| {
                for batch in batches {
                    capture
                        .lock()
                        .unwrap()
                        .push(batch.validated.events().to_vec());
                }
                Ok(())
            })
            .unwrap();
        let (pipeline, mut output) = Pipeline::start(&router).unwrap();
        // Transport clocks commonly carry nanoseconds while the wire/durable
        // contract is microsecond precise. The pipeline owns that boundary
        // normalization for both REST and targeted-WS replacement inputs.
        let now = Utc::now().with_nanosecond(123_456_789).unwrap();
        let mut left = book("11");
        let mut right = book("12");
        for frame in [&mut left, &mut right] {
            frame.raw_payload["timestamp"] = json!(now.timestamp_millis().to_string());
            frame
                .raw_payload
                .as_object_mut()
                .unwrap()
                .remove("event_type");
            frame.raw_payload["tick_size"] = json!("0.01");
        }
        pipeline
            .submit_rest_replacement(
                &mut router,
                &mut publication,
                vec![left.raw_payload, right.raw_payload],
                now,
                "1",
                "rest_confirmation_mismatch",
            )
            .await
            .unwrap();
        let completion = next(&mut output).await;
        let events = &completion.result.as_ref().unwrap().events;
        assert_eq!(
            events
                .iter()
                .map(|event| event["event_type"].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec![
                "recovery_started",
                "recovery_started",
                "book",
                "recovery_completed",
                "book",
                "recovery_completed"
            ]
        );
        pipeline
            .publish(&mut router, &mut publication, completion)
            .await
            .unwrap();
        assert_eq!(publication.cursor().unwrap(), 6);
        assert!(router.recoveries.is_empty());
        assert!(pipeline.controls["1"].lock().unwrap().attempts.is_empty());
        pipeline
            .finish(&mut router, &mut publication, &mut output)
            .await
            .unwrap();
        publication.finish().await.unwrap();
        let batches = written.lock().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].len(), 6);
    }

    #[tokio::test]
    async fn exhausted_byte_budget_fences_old_output_and_other_market_continues() {
        let mut router = fixture();
        let tokens = router
            .identities
            .iter()
            .map(|(t, (m, _))| (t.clone(), m.clone()))
            .collect();
        let written = Arc::new(Mutex::new(Vec::<Value>::new()));
        let capture = written.clone();
        let base = json!({"latest_cursor":0,"books":[],"markets":[],"gaps":[],"schema_version":"marketcow.polymarket.live-stream.v1","type":"state"});
        let mut publication =
            Publication::start(0, Some(base), tokens, 32, 1048576, 65536, move |batches| {
                for batch in batches {
                    capture
                        .lock()
                        .unwrap()
                        .extend_from_slice(batch.validated.events());
                }
                Ok(())
            })
            .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = publication.router(131072, 1).unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{address}/"))
            .await
            .unwrap();
        ws.send(tokio_tungstenite::tungstenite::Message::Text(
            json!({"type":"subscribe"}).to_string().into(),
        ))
        .await
        .unwrap();
        for _ in 0..2 {
            ws.next().await.unwrap().unwrap();
        }
        let (pipeline, mut output) = Pipeline::start(&router).unwrap();
        pipeline
            .submit(&mut router, &mut publication, book("11"), None)
            .await
            .unwrap();
        let old = next(&mut output).await;
        // A completed CPU result is delayed in the coordinator while A's input
        // budget is exhausted. Its old generation must not resurrect A later.
        let all_bytes = pipeline
            .bytes
            .clone()
            .acquire_many_owned(INPUT_BYTES as u32)
            .await
            .unwrap();
        pipeline
            .submit(&mut router, &mut publication, book("11"), None)
            .await
            .unwrap();
        assert_eq!(publication.cursor().unwrap(), 2); // Explicit A gaps only.
        pipeline
            .publish(&mut router, &mut publication, old)
            .await
            .unwrap();
        assert_eq!(publication.cursor().unwrap(), 2);
        drop(all_bytes);
        pipeline
            .submit(&mut router, &mut publication, book("21"), None)
            .await
            .unwrap();
        pipeline
            .publish(&mut router, &mut publication, next(&mut output).await)
            .await
            .unwrap();
        assert_eq!(publication.cursor().unwrap(), 3);
        let attempt = router.recoveries["11"].clone();
        pipeline
            .submit(
                &mut router,
                &mut publication,
                book("11"),
                Some(("11".into(), "obsolete".into())),
            )
            .await
            .unwrap();
        pipeline
            .publish(&mut router, &mut publication, next(&mut output).await)
            .await
            .unwrap();
        assert_eq!(publication.cursor().unwrap(), 3);
        pipeline
            .submit(
                &mut router,
                &mut publication,
                book("11"),
                Some(("11".into(), attempt.clone())),
            )
            .await
            .unwrap();
        pipeline
            .publish(&mut router, &mut publication, next(&mut output).await)
            .await
            .unwrap();
        assert_eq!(publication.cursor().unwrap(), 5);
        assert!(!router.recoveries.contains_key("11"));
        assert!(router.recoveries.contains_key("12"));
        let received = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            let mut received = Vec::new();
            while received.len() < 5 {
                let frame: Value =
                    serde_json::from_str(ws.next().await.unwrap().unwrap().to_text().unwrap())
                        .unwrap();
                if frame["type"] == "events" {
                    received.extend(frame["events"].as_array().unwrap().iter().cloned());
                }
            }
            received
        })
        .await
        .unwrap();
        assert_eq!(
            received
                .iter()
                .map(|e| e["cursor"].as_u64().unwrap())
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5]
        );
        assert_eq!(received[2]["token_id"], "21");
        assert_eq!(received[4]["canonical_payload"]["snapshot_cursor"], 4);
        server.abort();
        pipeline
            .finish(&mut router, &mut publication, &mut output)
            .await
            .unwrap();
        publication.finish().await.unwrap();
        let events = written.lock().unwrap();
        assert_eq!(
            events
                .iter()
                .map(|e| e["cursor"].as_u64().unwrap())
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5]
        );
        assert_eq!(events[2]["token_id"], "21");
        assert_eq!(events[4]["canonical_payload"]["snapshot_cursor"], 4);
        assert_eq!(events[4]["canonical_payload"]["recovery_id"], attempt);
    }
    #[test]
    fn byte_measurement_and_cursor_rebase_are_bounded() {
        assert!(bounded_size(&json!({"large":"abcdef"}), 4).is_err());
        assert!(bounded_size(&json!({"small":1}), 64).is_ok());
        let mut a = fixture();
        let frame = book("11");
        a.invalidate("11", "test", frame.received_at, 1).unwrap();
        let mut events = a
            .apply_isolated(frame.raw_payload, frame.received_at, 0)
            .unwrap();
        rebase(&mut events, 99).unwrap();
        assert_eq!(events[0]["cursor"], 100);
        assert_eq!(events[1]["cursor"], 101);
        assert_eq!(events[1]["canonical_payload"]["snapshot_cursor"], 100);
        assert_eq!(
            events[1]["canonical_payload_sha256"],
            canonical_hash(&events[1]["canonical_payload"])
        );
    }
}

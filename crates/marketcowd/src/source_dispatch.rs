//! Bounded per-entity FIFO actors. CPU work is concurrent across entities;
//! failures are results for that entity, never a global worker termination.
use anyhow::{Result, ensure};
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::{Semaphore, mpsc};

pub struct Completion<O> {
    pub entity: String,
    pub incarnation: u64,
    pub result: Result<O>,
}

pub struct Dispatcher<I, S, O> {
    queues: BTreeMap<String, mpsc::Sender<I>>,
    jobs: tokio::task::JoinSet<()>,
    workers: Arc<Semaphore>,
    output: Option<mpsc::Sender<Completion<O>>>,
    process: Arc<dyn Fn(&mut S, I) -> Result<O> + Send + Sync>,
    obsolete: Arc<dyn Fn(&S, &I) -> bool + Send + Sync>,
    per_entity_capacity: usize,
    incarnations: BTreeMap<String,u64>,
    next_incarnation: u64,
}

impl<I: Send + 'static, S: Send + 'static, O: Send + 'static> Dispatcher<I, S, O> {
    pub fn start(
        states: BTreeMap<String, S>,
        per_entity_capacity: usize,
        cpu_workers: usize,
        output_capacity: usize,
        process: impl Fn(&mut S, I) -> Result<O> + Send + Sync + 'static,
    ) -> Result<(Self, mpsc::Receiver<Completion<O>>)> {
        Self::start_filtered(
            states,
            per_entity_capacity,
            cpu_workers,
            output_capacity,
            |_, _| false,
            process,
        )
    }

    pub fn start_filtered(
        states: BTreeMap<String, S>,
        per_entity_capacity: usize,
        cpu_workers: usize,
        output_capacity: usize,
        obsolete: impl Fn(&S, &I) -> bool + Send + Sync + 'static,
        process: impl Fn(&mut S, I) -> Result<O> + Send + Sync + 'static,
    ) -> Result<(Self, mpsc::Receiver<Completion<O>>)> {
        ensure!(!states.is_empty() && states.len() <= 2048, "entity limit");
        ensure!(
            (1..=64).contains(&per_entity_capacity),
            "entity queue limit"
        );
        ensure!((1..=32).contains(&cpu_workers), "CPU worker limit");
        ensure!((1..=256).contains(&output_capacity), "output queue limit");
        let process = Arc::new(process);
        let obsolete = Arc::new(obsolete);
        let workers = Arc::new(Semaphore::new(cpu_workers));
        let (output, receive) = mpsc::channel(output_capacity);
        let mut dispatcher=Self {queues:BTreeMap::new(),jobs:tokio::task::JoinSet::new(),workers,
            output:Some(output),process,obsolete,per_entity_capacity,
            incarnations:BTreeMap::new(),next_incarnation:1};
        for (entity,state) in states {dispatcher.add_entity(entity,state)?;}
        Ok((dispatcher,receive))
    }

    /// Add an admitted actor without restarting retained actors or replacing
    /// the shared CPU/output budgets. Retiring tasks also count toward the cap.
    pub fn add_entity(&mut self, entity:String, mut state:S)->Result<()> {
        while let Some(result)=self.jobs.try_join_next() {result?;}
        ensure!(!entity.is_empty()&&!self.queues.contains_key(&entity),"duplicate/empty actor");
        ensure!(self.jobs.len()<2048,"active and retiring actor limit");
        let output=self.output.as_ref().ok_or_else(||anyhow::anyhow!("dispatcher closed"))?.clone();
        let incarnation=self.next_incarnation;
        self.next_incarnation=incarnation.checked_add(1).ok_or_else(||anyhow::anyhow!("actor identity exhausted"))?;
        self.incarnations.insert(entity.clone(),incarnation);
        let (send,mut input)=mpsc::channel(self.per_entity_capacity);
        self.queues.insert(entity.clone(),send);
        let process=self.process.clone();
        let obsolete=self.obsolete.clone();
        let workers=self.workers.clone();
        self.jobs.spawn(async move {
                while let Some(item) = input.recv().await {
                    // Only caller-proven fenced work can bypass execution.
                    if obsolete(&state, &item) {
                        continue;
                    }
                    // Reserve bounded result capacity before allocating a result.
                    let Ok(slot) = output.reserve().await else {
                        break;
                    };
                    if obsolete(&state, &item) {
                        continue;
                    }
                    let Ok(permit) = workers.clone().acquire_owned().await else {
                        break;
                    };
                    let process = process.clone();
                    let processed = tokio::task::spawn_blocking(move || {
                        let _permit = permit;
                        let result = process(&mut state, item);
                        (state, result)
                    })
                    .await;
                    match processed {
                        Ok((next, result)) => {
                            state = next;
                            slot.send(Completion {
                                entity: entity.clone(),
                                incarnation,
                                result,
                            });
                        }
                        Err(error) => {
                            slot.send(Completion {
                                entity: entity.clone(),
                                incarnation,
                                result: Err(error.into()),
                            });
                            break; // Only this actor lost its state; others remain alive.
                        }
                    }
                }
        });
        Ok(())
    }

    /// Caller must fence old completions BEFORE retirement. Closing admission
    /// drains bounded queued work; it never kills shared workers or other actors.
    pub fn remove_entity(&mut self, entity:&str)->bool {
        self.incarnations.remove(entity);
        self.queues.remove(entity).is_some()
    }

    pub fn is_current(&self, completion:&Completion<O>)->bool {
        self.incarnations.get(&completion.entity)==Some(&completion.incarnation)
    }

    /// Full/unknown queues return ownership to the caller for explicit entity
    /// invalidation/recovery. Never wait for A's capacity before submitting B.
    pub fn try_submit(&self, entity: &str, item: I) -> std::result::Result<(), I> {
        match self.queues.get(entity) {
            Some(queue) => queue.try_send(item).map_err(|error| error.into_inner()),
            None => Err(item),
        }
    }

    pub fn queue_closed(&self, entity: &str) -> bool {
        self.queues.get(entity).is_none_or(|q| q.is_closed())
    }

    /// Diagnostic snapshot; waiting actors holding one input are not counted
    /// as queued. CPU permits bound admitted blocking work, not CPU utilization.
    pub fn occupancy(&self) -> (usize, usize, usize) {
        let queued = self
            .queues
            .values()
            .map(|q| q.max_capacity() - q.capacity());
        (
            queued.clone().sum(),
            queued.max().unwrap_or(0),
            self.workers.available_permits(),
        )
    }

    pub fn close(&mut self) {
        self.queues.clear();
        self.output.take();
    }

    pub async fn join(&mut self) {
        while self.jobs.join_next().await.is_some() {}
    }

    pub async fn finish(mut self) {
        self.close();
        self.join().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc as sync;

    #[tokio::test]
    async fn add_remove_keeps_retained_actor_state_and_budgets() {
        let (mut d,mut rx)=Dispatcher::start([("a".into(),0_usize)].into(),2,2,4,
            |state,item:usize|{*state+=item;Ok(*state)}).unwrap();
        d.try_submit("a",1).unwrap();
        assert_eq!(rx.recv().await.unwrap().result.unwrap(),1);
        d.add_entity("b".into(),10).unwrap();
        assert!(d.add_entity("a".into(),999).is_err());
        d.try_submit("b",2).unwrap();
        assert_eq!(rx.recv().await.unwrap().result.unwrap(),12);
        assert!(d.remove_entity("b"));
        assert_eq!(d.try_submit("b",3),Err(3));
        d.try_submit("a",4).unwrap();
        assert_eq!(rx.recv().await.unwrap().result.unwrap(),5);
        assert!(d.workers.available_permits()<=2);
        d.close();
        assert!(d.add_entity("c".into(),0).is_err());
        d.join().await;
        assert!(rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn retired_completion_cannot_land_after_same_market_readded() {
        let (mut d,mut rx)=Dispatcher::start([("a".into(),())].into(),2,2,4,
            |_,item:usize|Ok(item)).unwrap();
        d.try_submit("a",1).unwrap();
        let old=rx.recv().await.unwrap();
        assert!(d.is_current(&old));
        assert!(d.remove_entity("a"));
        d.add_entity("a".into(),()).unwrap();
        assert!(!d.is_current(&old));
        d.try_submit("a",2).unwrap();
        let new=rx.recv().await.unwrap();
        assert!(d.is_current(&new));
        assert_ne!(old.incarnation,new.incarnation);
        d.close();d.join().await;
    }

    // Diagnostic reproduction: input slots can reject a burst before any CPU
    // work starts. More CPU permits do not add same-entity admission capacity.
    #[tokio::test(flavor = "current_thread")]
    async fn burst_rejects_with_all_eight_cpu_permits_idle() {
        let (mut dispatch, mut output) = Dispatcher::start(
            [("a".into(), ()), ("b".into(), ())].into(),
            2, 8, 16, |_, item: usize| Ok(item),
        ).unwrap();
        dispatch.try_submit("a", 1).unwrap();
        dispatch.try_submit("a", 2).unwrap();
        assert_eq!(dispatch.occupancy(), (2, 2, 8));
        assert_eq!(dispatch.try_submit("a", 3), Err(3));
        dispatch.try_submit("b", 4).unwrap();
        dispatch.close();
        let mut a = Vec::new();
        let mut b = Vec::new();
        for _ in 0..3 {
            let item = output.recv().await.unwrap();
            if item.entity == "a" { a.push(item.result.unwrap()); }
            else { b.push(item.result.unwrap()); }
        }
        assert_eq!(a, [1, 2]);
        assert_eq!(b, [4]);
        dispatch.join().await;
    }

    #[tokio::test]
    async fn fenced_queued_work_is_discarded_before_cpu_and_fresh_work_survives() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let generation = Arc::new(AtomicUsize::new(0));
        let fence = generation.clone();
        let (release, gate) = sync::channel();
        let (entered, wait) = sync::channel();
        let states = [("a".into(), Some(gate))].into();
        let (mut dispatcher, mut output) = Dispatcher::start_filtered(
            states,
            2,
            6,
            12,
            move |_, item: &(usize, usize)| item.0 != fence.load(Ordering::SeqCst),
            move |gate, item| {
                if item.1 == 0 {
                    entered.send(()).unwrap();
                    gate.take()
                        .unwrap()
                        .recv_timeout(std::time::Duration::from_secs(2))
                        .unwrap();
                }
                Ok(item.1)
            },
        )
        .unwrap();
        dispatcher.try_submit("a", (0, 0)).unwrap();
        tokio::task::spawn_blocking(move || {
            wait.recv_timeout(std::time::Duration::from_secs(1))
                .unwrap()
        })
        .await
        .unwrap();
        dispatcher.try_submit("a", (0, 1)).unwrap();
        dispatcher.try_submit("a", (1, 2)).unwrap();
        generation.store(1, Ordering::SeqCst);
        release.send(()).unwrap();
        assert_eq!(output.recv().await.unwrap().result.unwrap(), 0);
        assert_eq!(output.recv().await.unwrap().result.unwrap(), 2);
        dispatcher.close();
        dispatcher.join().await;
        assert!(output.recv().await.is_none());
    }

    #[tokio::test]
    async fn slow_and_failed_a_do_not_hold_b_and_fifo_survives_failure() {
        let (release, gate) = sync::channel();
        let (entered, wait) = sync::channel();
        let states = [("a".into(), Some(gate)), ("b".into(), None)].into();
        let (dispatch, mut output) = Dispatcher::start(states, 4, 2, 8, move |gate, n| {
            if n == 1
                && let Some(gate) = gate
            {
                entered.send(()).unwrap();
                gate.recv_timeout(std::time::Duration::from_secs(2))
                    .unwrap();
                anyhow::bail!("A data rejected");
            }
            Ok(n)
        })
        .unwrap();
        dispatch.try_submit("a", 1).unwrap();
        tokio::task::spawn_blocking(move || {
            wait.recv_timeout(std::time::Duration::from_secs(1))
                .unwrap()
        })
        .await
        .unwrap();
        dispatch.try_submit("a", 2).unwrap();
        dispatch.try_submit("b", 3).unwrap();
        let b = tokio::time::timeout(std::time::Duration::from_millis(200), output.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(b.entity, "b");
        assert_eq!(b.result.unwrap(), 3);
        release.send(()).unwrap();
        let a = output.recv().await.unwrap();
        assert_eq!(a.entity, "a");
        assert!(a.result.is_err());
        assert_eq!(output.recv().await.unwrap().result.unwrap(), 2);
        dispatch.finish().await;
    }

    #[tokio::test]
    async fn full_entity_queue_returns_item_and_does_not_block_another_entity() {
        let (release, gate) = sync::channel();
        let (entered, wait) = sync::channel();
        let states = [("a".into(), Some(gate)), ("b".into(), None)].into();
        let (dispatch, mut output) = Dispatcher::start(states, 1, 2, 8, move |gate, n| {
            if n == 1
                && let Some(gate) = gate
            {
                entered.send(()).unwrap();
                gate.recv_timeout(std::time::Duration::from_secs(2))
                    .unwrap();
            }
            Ok(n)
        })
        .unwrap();
        dispatch.try_submit("a", 1).unwrap();
        tokio::task::spawn_blocking(move || {
            wait.recv_timeout(std::time::Duration::from_secs(1))
                .unwrap()
        })
        .await
        .unwrap();
        dispatch.try_submit("a", 2).unwrap();
        assert_eq!(dispatch.try_submit("a", 4), Err(4));
        dispatch.try_submit("b", 3).unwrap();
        assert_eq!(output.recv().await.unwrap().result.unwrap(), 3);
        release.send(()).unwrap();
        output.recv().await.unwrap();
        output.recv().await.unwrap();
        dispatch.finish().await;
    }
}

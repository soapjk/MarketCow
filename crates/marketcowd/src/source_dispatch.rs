//! Bounded per-entity FIFO actors. CPU work is concurrent across entities;
//! failures are results for that entity, never a global worker termination.
use anyhow::{Result, ensure};
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::{Semaphore, mpsc};

pub struct Completion<O> {
    pub entity: String,
    pub result: Result<O>,
}

pub struct Dispatcher<I> {
    queues: BTreeMap<String, mpsc::Sender<I>>,
    jobs: tokio::task::JoinSet<()>,
}

impl<I: Send + 'static> Dispatcher<I> {
    pub fn start<S: Send + 'static, O: Send + 'static>(
        states: BTreeMap<String, S>,
        per_entity_capacity: usize,
        cpu_workers: usize,
        output_capacity: usize,
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
        let workers = Arc::new(Semaphore::new(cpu_workers));
        let (output, receive) = mpsc::channel(output_capacity);
        let mut queues = BTreeMap::new();
        let mut jobs = tokio::task::JoinSet::new();
        for (entity, mut state) in states {
            let (send, mut input) = mpsc::channel(per_entity_capacity);
            queues.insert(entity.clone(), send);
            let output = output.clone();
            let process = process.clone();
            let workers = workers.clone();
            jobs.spawn(async move {
                while let Some(item) = input.recv().await {
                    // Reserve bounded result capacity before allocating a result.
                    let Ok(slot) = output.reserve().await else {
                        break;
                    };
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
                                result,
                            });
                        }
                        Err(error) => {
                            slot.send(Completion {
                                entity: entity.clone(),
                                result: Err(error.into()),
                            });
                            break; // Only this actor lost its state; others remain alive.
                        }
                    }
                }
            });
        }
        drop(output);
        Ok((Self { queues, jobs }, receive))
    }

    /// Full/unknown queues return ownership to the caller for explicit entity
    /// invalidation/recovery. Never wait for A's capacity before submitting B.
    pub fn try_submit(&self, entity: &str, item: I) -> std::result::Result<(), I> {
        match self.queues.get(entity) {
            Some(queue) => queue.try_send(item).map_err(|error| error.into_inner()),
            None => Err(item),
        }
    }

    pub fn close(&mut self) {
        self.queues.clear();
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

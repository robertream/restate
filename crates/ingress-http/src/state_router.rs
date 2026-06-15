// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::{RwLock, broadcast, mpsc};
use tokio::time::Instant;
use tracing::{error, warn};

use restate_core::Metadata;
use restate_partition_store::{StateChangeEvent, StateChangeOperation, SubscriptionRequest};
use restate_types::identifiers::{PartitionId, ServiceId, WithPartitionKey};
use restate_types::partition_table::FindPartition;

const SSE_SUBSCRIPTION_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const TIMER_INTERVAL: Duration = Duration::from_secs(5);
const BROADCAST_CAPACITY: usize = 64;

enum SubscriptionState {
    Pending,
    Active {
        cached_state: HashMap<String, Bytes>,
        revision: u64,
    },
}

struct SubscriptionEntry {
    state_tx: broadcast::Sender<Arc<StateChangeEvent>>,
    last_idle: Instant,
    state: SubscriptionState,
}

impl SubscriptionEntry {
    fn send(&mut self, event: Arc<StateChangeEvent>, now: Instant) {
        if self.state_tx.send(event).is_err() {
            self.last_idle = now;
        }
    }
}

#[derive(Clone, Default)]
pub struct StateRouter {
    objects: Arc<RwLock<HashMap<ServiceId, SubscriptionEntry>>>,
    partition_senders: Arc<RwLock<HashMap<PartitionId, mpsc::Sender<SubscriptionRequest>>>>,
}

impl StateRouter {
    pub fn timer_interval() -> Duration {
        TIMER_INTERVAL
    }

    pub async fn add_partition(
        &self,
        partition_id: PartitionId,
        cmd_tx: mpsc::Sender<SubscriptionRequest>,
    ) {
        self.partition_senders
            .write()
            .await
            .insert(partition_id, cmd_tx.clone());

        let commands: Vec<SubscriptionRequest> = {
            let objects = self.objects.read().await;
            objects
                .iter()
                .filter_map(|(service_id, entry)| {
                    let Ok(pid) = Metadata::with_current(|m| {
                        m.partition_table_ref()
                            .find_partition_id(service_id.partition_key())
                    }) else {
                        return None;
                    };
                    if pid != partition_id {
                        return None;
                    }
                    let cmd = match &entry.state {
                        SubscriptionState::Active { revision, .. } => {
                            SubscriptionRequest::Resubscribe {
                                service_id: service_id.clone(),
                                revision: *revision,
                            }
                        }
                        SubscriptionState::Pending => SubscriptionRequest::Subscribe {
                            service_id: service_id.clone(),
                        },
                    };
                    Some(cmd)
                })
                .collect()
        };

        for cmd in commands {
            if cmd_tx.send(cmd).await.is_err() {
                warn!("partition subscription task gone for partition {partition_id}");
                break;
            }
        }
    }

    pub async fn subscribe(
        &self,
        now: Instant,
        service_id: ServiceId,
        last_event_id: Option<u64>,
    ) -> anyhow::Result<(
        Option<Arc<StateChangeEvent>>,
        broadcast::Receiver<Arc<StateChangeEvent>>,
    )> {
        let partition_id = Metadata::with_current(|m| {
            m.partition_table_ref()
                .find_partition_id(service_id.partition_key())
        })?;

        let (snapshot, rx, cold_path) = {
            let mut objects = self.objects.write().await;
            match objects.get_mut(&service_id) {
                Some(entry) => {
                    entry.last_idle = now;
                    let rx = entry.state_tx.subscribe();
                    let snapshot = match &entry.state {
                        SubscriptionState::Active {
                            cached_state,
                            revision,
                        } => {
                            if !matches!(last_event_id, Some(lei) if lei == *revision) {
                                Some(Arc::new(StateChangeEvent {
                                    service_id: service_id.clone(),
                                    revision: *revision,
                                    operation: StateChangeOperation::Replace {
                                        state: cached_state.clone(),
                                    },
                                }))
                            } else {
                                None
                            }
                        }
                        SubscriptionState::Pending => None,
                    };
                    (snapshot, rx, false)
                }
                None => {
                    let (tx, rx) = broadcast::channel(BROADCAST_CAPACITY);
                    objects.insert(
                        service_id.clone(),
                        SubscriptionEntry {
                            state_tx: tx,
                            last_idle: now,
                            state: SubscriptionState::Pending,
                        },
                    );
                    (None, rx, true)
                }
            }
        };

        if cold_path {
            // Race: add_partition() may have already sent Subscribe for this entry between
            // the objects write-lock drop above and acquiring partition_senders here.
            // The duplicate Replace from the partition store is deduplicated by the revision
            // monotonicity guard in handle_state_change — no correctness impact.
            let senders = self.partition_senders.read().await;
            if let Some(cmd_tx) = senders.get(&partition_id) {
                if cmd_tx
                    .send(SubscriptionRequest::Subscribe {
                        service_id: service_id.clone(),
                    })
                    .await
                    .is_err()
                {
                    warn!("partition subscription task gone for partition {partition_id}");
                }
            } else {
                warn!("no partition sender for partition {partition_id}");
            }
        }

        Ok((snapshot, rx))
    }

    pub async fn handle_state_change(&self, now: Instant, event: Arc<StateChangeEvent>) {
        // Drop empty patches — no state changed, nothing to fan out.
        if let StateChangeOperation::Patch { assigned, deleted } = &event.operation
            && assigned.is_empty()
            && deleted.is_empty()
        {
            return;
        }

        let mut objects = self.objects.write().await;
        let Some(entry) = objects.get_mut(&event.service_id) else {
            return;
        };

        match &event.operation {
            StateChangeOperation::Replace { state } => {
                match &entry.state {
                    SubscriptionState::Active { revision, .. } => {
                        if event.revision <= *revision {
                            return;
                        }
                    }
                    SubscriptionState::Pending => {
                        if entry.state_tx.receiver_count() == 0 {
                            // All receivers disconnected before Replace arrived; drop entry.
                            objects.remove(&event.service_id);
                            return;
                        }
                    }
                }
                entry.state = SubscriptionState::Active {
                    cached_state: state.clone(),
                    revision: event.revision,
                };
                entry.send(event, now);
            }
            StateChangeOperation::Patch { assigned, deleted } => {
                let SubscriptionState::Active {
                    cached_state,
                    revision,
                } = &mut entry.state
                else {
                    return;
                };
                if event.revision <= *revision {
                    return;
                }
                *revision = event.revision;
                for (k, v) in assigned {
                    cached_state.insert(k.clone(), v.clone());
                }
                for k in deleted {
                    cached_state.remove(k);
                }
                entry.send(event, now);
            }
            StateChangeOperation::ClearAll => {
                let SubscriptionState::Active {
                    cached_state,
                    revision,
                } = &mut entry.state
                else {
                    return;
                };
                if event.revision <= *revision {
                    return;
                }
                *revision = event.revision;
                cached_state.clear();
                entry.send(event, now);
            }
        }
    }

    pub async fn on_partition_closed(&self, partition_id: PartitionId) {
        self.partition_senders.write().await.remove(&partition_id);
        // objects map: NO CHANGES — active subscriptions stay alive during repartition
    }

    pub async fn on_timer(&self, now: Instant) {
        let evicted: Vec<ServiceId> = {
            let mut objects = self.objects.write().await;
            let mut evicted = Vec::new();
            objects.retain(|sid, entry| {
                if entry.state_tx.receiver_count() > 0 {
                    entry.last_idle = now;
                    true
                } else {
                    let expired =
                        now.duration_since(entry.last_idle) >= SSE_SUBSCRIPTION_IDLE_TIMEOUT;
                    if expired {
                        evicted.push(sid.clone());
                    }
                    !expired
                }
            });
            evicted
        };

        if evicted.is_empty() {
            return;
        }

        let senders = self.partition_senders.read().await;
        for service_id in evicted {
            let Ok(partition_id) = Metadata::with_current(|m| {
                m.partition_table_ref()
                    .find_partition_id(service_id.partition_key())
            }) else {
                continue;
            };
            if let Some(cmd_tx) = senders.get(&partition_id)
                && cmd_tx
                    .send(SubscriptionRequest::Unsubscribe { service_id })
                    .await
                    .is_err()
            {
                error!(
                    "partition subscription task gone during idle eviction for partition {partition_id}"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use bytes::Bytes;
    use restate_core::{Metadata, TestCoreEnvBuilder};
    use restate_partition_store::{StateChangeEvent, StateChangeOperation, SubscriptionRequest};
    use restate_types::identifiers::{ServiceId, WithPartitionKey};
    use restate_types::partition_table::FindPartition;
    use tokio::sync::{broadcast, mpsc};

    use super::*;

    #[restate_core::test]
    async fn repartition_round_trip() {
        let _env = TestCoreEnvBuilder::with_incoming_only_connector()
            .build()
            .await;

        let router = StateRouter::default();
        let service_id = ServiceId::new(None, "counter", "my-counter");

        let partition_id = Metadata::with_current(|m| {
            m.partition_table_ref()
                .find_partition_id(service_id.partition_key())
        })
        .expect("partition table must be populated");

        // Insert a Pending entry with a live client receiver
        let (client_tx, mut client_rx) = {
            let (tx, rx) = broadcast::channel::<Arc<StateChangeEvent>>(16);
            let mut objects = router.objects.write().await;
            objects.insert(
                service_id.clone(),
                SubscriptionEntry {
                    state_tx: tx.clone(),
                    last_idle: tokio::time::Instant::now(),
                    state: SubscriptionState::Pending,
                },
            );
            (tx, rx)
        };

        // Replace event arrives → Pending promoted to Active, Replace fanned out to client
        let mut state = HashMap::new();
        state.insert("count".to_owned(), Bytes::from_static(b"42"));
        let replace_event = Arc::new(StateChangeEvent {
            service_id: service_id.clone(),
            revision: 7,
            operation: StateChangeOperation::Replace {
                state: state.clone(),
            },
        });
        router
            .handle_state_change(tokio::time::Instant::now(), replace_event)
            .await;

        // Client receives the Replace event
        let received = client_rx
            .try_recv()
            .expect("client must receive Replace on Pending→Active transition");
        assert_eq!(received.revision, 7);
        assert!(matches!(
            received.operation,
            StateChangeOperation::Replace { .. }
        ));

        // Entry is now Active with revision 7
        {
            let objects = router.objects.read().await;
            let entry = objects.get(&service_id).expect("entry must exist");
            match &entry.state {
                SubscriptionState::Active {
                    revision,
                    cached_state,
                } => {
                    assert_eq!(*revision, 7);
                    assert_eq!(*cached_state, state);
                }
                _ => panic!("expected Active entry after Replace"),
            }
        }

        // on_partition_closed → partition_senders cleared, but Active entry untouched
        router.on_partition_closed(partition_id).await;
        {
            let objects = router.objects.read().await;
            let entry = objects
                .get(&service_id)
                .expect("entry must survive on_partition_closed");
            assert!(matches!(entry.state, SubscriptionState::Active { .. }));
        }

        // add_partition → Resubscribe sent with revision matching the cached entry
        let (cmd_tx, mut cmd_rx) = mpsc::channel(16);
        router.add_partition(partition_id, cmd_tx).await;

        let cmd = cmd_rx.try_recv().expect("Resubscribe must be sent");
        match cmd {
            SubscriptionRequest::Resubscribe {
                service_id: sid,
                revision,
            } => {
                assert_eq!(sid, service_id);
                assert_eq!(revision, 7, "Resubscribe must carry the cached revision");
            }
            other => panic!("expected Resubscribe, got {other:?}"),
        }
        assert!(cmd_rx.try_recv().is_err());

        // Simulate revision-match: no further Replace sent to client
        assert!(
            client_rx.try_recv().is_err(),
            "client must not receive spurious Replace when revision matches"
        );

        // Client receiver is still open
        assert!(
            client_tx.receiver_count() > 0,
            "client receiver must still be open"
        );
    }

    /// REQ-003: new subscribe calls during the repartition gap window are queued as Pending
    /// and served when add_partition restores the partition sender.
    #[restate_core::test]
    async fn subscribe_during_partition_gap() {
        let _env = TestCoreEnvBuilder::with_incoming_only_connector()
            .build()
            .await;

        let router = StateRouter::default();
        let service_id = ServiceId::new(None, "counter", "gap-counter");

        let partition_id = Metadata::with_current(|m| {
            m.partition_table_ref()
                .find_partition_id(service_id.partition_key())
        })
        .expect("partition table must be populated");

        // Partition goes away — no sender in partition_senders.
        router.on_partition_closed(partition_id).await;

        // Subscribe during gap: entry must be Pending, no Subscribe sent (no partition sender).
        let (_snapshot, mut client_rx) = router
            .subscribe(tokio::time::Instant::now(), service_id.clone(), None)
            .await
            .expect("subscribe must succeed");

        {
            let objects = router.objects.read().await;
            let entry = objects.get(&service_id).expect("entry must exist");
            assert!(
                matches!(entry.state, SubscriptionState::Pending),
                "subscribe during gap must create a Pending entry"
            );
        }

        // add_partition: Subscribe must be sent for the Pending entry.
        let (cmd_tx, mut cmd_rx) = mpsc::channel(16);
        router.add_partition(partition_id, cmd_tx).await;

        let cmd = cmd_rx
            .try_recv()
            .expect("Subscribe must be sent on add_partition");
        assert!(
            matches!(cmd, SubscriptionRequest::Subscribe { service_id: ref sid } if *sid == service_id),
            "expected Subscribe for the pending service_id"
        );

        // Replace arrives (simulating partition store response to Subscribe).
        let mut state = HashMap::new();
        state.insert("count".to_owned(), Bytes::from_static(b"1"));
        let replace_event = Arc::new(StateChangeEvent {
            service_id: service_id.clone(),
            revision: 1,
            operation: StateChangeOperation::Replace {
                state: state.clone(),
            },
        });
        router
            .handle_state_change(tokio::time::Instant::now(), replace_event)
            .await;

        // Client receives the Replace and entry is promoted to Active.
        let received = client_rx
            .try_recv()
            .expect("client must receive Replace after Pending→Active promotion");
        assert_eq!(received.revision, 1);
        assert!(matches!(
            received.operation,
            StateChangeOperation::Replace { .. }
        ));

        let objects = router.objects.read().await;
        let entry = objects.get(&service_id).expect("entry must exist");
        assert!(
            matches!(entry.state, SubscriptionState::Active { revision, .. } if revision == 1),
            "entry must be Active with revision 1 after Replace"
        );
    }

    /// Multiple concurrent subscribers on the same Active entry:
    /// - Both receive broadcast events
    /// - Only a new subscriber with stale Last-Event-ID gets a snapshot; existing subscriber gets none
    /// - Dropping one receiver does not affect the other
    #[restate_core::test]
    async fn multi_subscriber_broadcast() {
        let _env = TestCoreEnvBuilder::with_incoming_only_connector()
            .build()
            .await;

        let router = StateRouter::default();
        let service_id = ServiceId::new(None, "counter", "multi-key");

        // Seed an Active entry at revision 5
        let mut initial_state = HashMap::new();
        initial_state.insert("x".to_owned(), Bytes::from_static(b"10"));
        {
            let (tx, _) = broadcast::channel(16);
            let mut objects = router.objects.write().await;
            objects.insert(
                service_id.clone(),
                SubscriptionEntry {
                    state_tx: tx,
                    last_idle: tokio::time::Instant::now(),
                    state: SubscriptionState::Active {
                        cached_state: initial_state.clone(),
                        revision: 5,
                    },
                },
            );
        }

        // First subscriber: Last-Event-ID matches revision → no snapshot
        let (snap_a, mut rx_a) = router
            .subscribe(tokio::time::Instant::now(), service_id.clone(), Some(5))
            .await
            .expect("subscribe must succeed");
        assert!(
            snap_a.is_none(),
            "up-to-date subscriber must not get snapshot"
        );

        // Second subscriber: stale Last-Event-ID → gets snapshot
        let (snap_b, mut rx_b) = router
            .subscribe(tokio::time::Instant::now(), service_id.clone(), Some(3))
            .await
            .expect("subscribe must succeed");
        let snap_b = snap_b.expect("stale subscriber must get snapshot");
        assert_eq!(snap_b.revision, 5);
        assert!(matches!(
            snap_b.operation,
            StateChangeOperation::Replace { .. }
        ));

        // Both receive the next broadcast event
        let patch_event = Arc::new(StateChangeEvent {
            service_id: service_id.clone(),
            revision: 6,
            operation: StateChangeOperation::Patch {
                assigned: [("x".to_owned(), Bytes::from_static(b"11"))].into(),
                deleted: Default::default(),
            },
        });
        router
            .handle_state_change(tokio::time::Instant::now(), patch_event)
            .await;

        let ev_a = rx_a.try_recv().expect("rx_a must receive patch");
        let ev_b = rx_b.try_recv().expect("rx_b must receive patch");
        assert_eq!(ev_a.revision, 6);
        assert_eq!(ev_b.revision, 6);

        // Drop rx_a — rx_b continues unaffected
        drop(rx_a);

        let patch_event2 = Arc::new(StateChangeEvent {
            service_id: service_id.clone(),
            revision: 7,
            operation: StateChangeOperation::ClearAll,
        });
        router
            .handle_state_change(tokio::time::Instant::now(), patch_event2)
            .await;

        let ev_b2 = rx_b
            .try_recv()
            .expect("rx_b must receive after rx_a dropped");
        assert_eq!(ev_b2.revision, 7);
        assert!(matches!(ev_b2.operation, StateChangeOperation::ClearAll));
    }
}

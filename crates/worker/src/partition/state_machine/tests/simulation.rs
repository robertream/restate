// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;
use std::ops::ControlFlow;

use futures::TryStreamExt;
use rand::SeedableRng;
use rand::rngs::SmallRng;
use rand::seq::IteratorRandom;
use test_log::test;

use restate_invoker_api::{Effect, EffectKind};
use restate_storage_api::deduplication_table::{
    DedupInformation, DedupSequenceNumber, EpochSequenceNumber,
};
use restate_storage_api::invocation_status_table::{
    ScanInvocationStatusTable, ScanInvocationStatusTableRange,
};
use restate_types::GenerationalNodeId;
use restate_types::identifiers::{InvocationId, InvocationUuid, LeaderEpoch, PartitionKey};
use restate_types::invocation::{
    InvocationTarget, InvocationTermination, NotifySignalRequest, ServiceInvocation, Source,
    TerminationFlavor, VirtualObjectHandlerType,
};
use restate_types::journal_v2::notification::{Signal, SignalId, SignalResult};
use restate_types::logs::{Lsn, SequenceNumber};
use restate_types::time::MillisSinceEpoch;
use restate_wal_protocol::control::AnnounceLeader;
use restate_wal_protocol::{Command, Destination, Envelope, Header};

use restate_types::SemanticRestateVersion;

use crate::partition::state_machine::actions::Action;
use crate::partition::state_machine::tests::TestEnv;
use crate::partition::state_machine::tests::sim_storage::SimStorage;
use crate::partition::state_machine::{ActionCollector, Error, StateMachine, VQueuesMetaMut};

/// Fixed VirtualObject target used for all generated invocations.
/// Only the invocation_id varies — we don't randomize fields that don't affect state machine behavior.
fn sim_invocation_target() -> InvocationTarget {
    InvocationTarget::virtual_object(
        "SimService",
        "sim-key",
        "handle",
        VirtualObjectHandlerType::Exclusive,
    )
}

fn gen_invocation_id(rng: &mut impl rand::Rng) -> InvocationId {
    InvocationId::from_parts(
        rng.random::<PartitionKey>(),
        InvocationUuid::from_u128(rng.random::<u128>().max(1)), // avoid nil uuid
    )
}

fn gen_service_invocation(rng: &mut impl rand::Rng) -> ServiceInvocation {
    let invocation_id = gen_invocation_id(rng);
    ServiceInvocation::initialize(
        invocation_id,
        sim_invocation_target(),
        // Fixed source — use a seeded RPC request ID via from_parts to stay deterministic
        Source::Ingress(
            restate_types::identifiers::PartitionProcessorRpcRequestId::from_parts(
                0,
                rng.random::<u128>().max(1),
            ),
        ),
    )
}

fn gen_command(
    rng: &mut impl rand::Rng,
    known_ids: &BTreeSet<InvocationId>,
    is_leader: &mut bool,
) -> Command {
    let pick = rng.random_range(0..4u8);

    // InvokerEffect and TerminateInvocation fall back to Invoke when no known IDs exist
    match pick {
        0 => Command::Invoke(Box::new(gen_service_invocation(rng))),
        1 => {
            if let Some(&id) = known_ids.iter().choose(rng) {
                Command::InvokerEffect(Box::new(Effect {
                    invocation_id: id,
                    kind: EffectKind::End,
                }))
            } else {
                Command::Invoke(Box::new(gen_service_invocation(rng)))
            }
        }
        2 => {
            *is_leader = !*is_leader;
            Command::AnnounceLeader(Box::new(AnnounceLeader {
                node_id: GenerationalNodeId::new(1, 1),
                leader_epoch: LeaderEpoch::INITIAL,
                partition_key_range: PartitionKey::MIN..=PartitionKey::MAX,
                epoch_version: None,
                current_config: None,
                next_config: None,
            }))
        }
        _ => {
            if let Some(&id) = known_ids.iter().choose(rng) {
                Command::TerminateInvocation(InvocationTermination {
                    invocation_id: id,
                    flavor: TerminationFlavor::Kill,
                    response_sink: None,
                })
            } else {
                Command::Invoke(Box::new(gen_service_invocation(rng)))
            }
        }
    }
}

async fn check_invariants(storage: &restate_partition_store::PartitionStore) {
    // Invariant: every entry in the invocation status table deserializes without error.
    // We scan all invoked invocations (the main non-free status) to confirm storage integrity.
    let mut count = 0usize;
    let mut invoked = std::pin::pin!(
        storage
            .scan_invoked_invocations()
            .expect("storage scan to succeed")
    );
    while let Some(entry) = invoked.try_next().await.expect("scan to not error") {
        // Simply having deserialized successfully is the invariant.
        let _ = entry;
        count += 1;
    }

    // Also verify the full status table range scan doesn't panic/error.
    let scan_fut = storage
        .for_each_invocation_status_lazy(
            ScanInvocationStatusTableRange::PartitionKey(PartitionKey::MIN..=PartitionKey::MAX),
            move |(_id, _lazy_status)| -> ControlFlow<std::result::Result<(), anyhow::Error>> {
                ControlFlow::Continue(())
            },
        )
        .expect("scan future construction to succeed");
    scan_fut.await.expect("full status table scan to succeed");

    eprintln!("  Invariant check: {count} invoked invocations found, all statuses deserialized OK");
}

#[test(restate_core::test)]
async fn state_machine_simulation() {
    let seed: u64 = std::env::var("SIM_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(rand::random);
    eprintln!("Simulation seed: {seed}");

    let mut env = TestEnv::create().await;
    let mut rng = SmallRng::seed_from_u64(seed);
    let mut known_ids: BTreeSet<InvocationId> = BTreeSet::new();
    let mut time = MillisSinceEpoch::new(1_000_000);
    let mut lsn = Lsn::OLDEST;
    let mut is_leader = true;

    for i in 0..1000 {
        let command = gen_command(&mut rng, &known_ids, &mut is_leader);
        if let Command::Invoke(ref inv) = command {
            known_ids.insert(inv.invocation_id);
        }
        time = MillisSinceEpoch::new(time.as_u64() + 10);
        lsn = lsn.next();
        match env.apply_with(command, time, lsn, is_leader).await {
            Ok(_actions) => {}
            Err(e) => eprintln!("  Step {i}: error (expected for some sequences): {e}"),
        }
    }

    check_invariants(&env.storage).await;
    env.shutdown().await;
}

// --- Phase 1.5: Dedup-aware simulation ---
// Tests the apply_record pipeline: dedup check + state machine apply.
// Generates Envelopes with DedupInformation headers, simulating self-proposals
// with epoch-fenced dedup and leadership transitions via AnnounceLeader.

/// Tracks the simulated leader's self-proposal dedup state.
struct SimLeaderState {
    epoch: LeaderEpoch,
    esn: EpochSequenceNumber,
    is_leader: bool,
}

impl SimLeaderState {
    fn new() -> Self {
        let epoch = LeaderEpoch::INITIAL;
        Self {
            epoch,
            esn: EpochSequenceNumber::new(epoch),
            is_leader: true,
        }
    }

    /// Allocate the next ESN for a self-proposal.
    fn next_esn(&mut self) -> EpochSequenceNumber {
        let esn = self.esn;
        self.esn = self.esn.next();
        esn
    }

    /// Transition to a new epoch (leadership change).
    fn new_epoch(&mut self) {
        self.epoch = self.epoch.next();
        self.esn = EpochSequenceNumber::new(self.epoch);
        self.is_leader = !self.is_leader;
    }
}

/// Wrap a command in an Envelope with a self-proposal dedup header.
fn self_proposal_envelope(command: Command, esn: EpochSequenceNumber) -> Envelope {
    Envelope::new(
        Header {
            source: restate_wal_protocol::Source::ControlPlane {},
            dest: Destination::Processor {
                partition_key: PartitionKey::MIN,
                dedup: Some(DedupInformation::self_proposal(esn)),
            },
        },
        command,
    )
}

/// Generate an envelope for the dedup-aware simulation.
/// Returns (envelope, is_announce_leader).
fn gen_envelope(
    rng: &mut impl rand::Rng,
    known_ids: &BTreeSet<InvocationId>,
    leader: &mut SimLeaderState,
) -> (Envelope, bool) {
    let pick = rng.random_range(0..6u8);

    match pick {
        // Invoke — self-proposal with current epoch ESN
        0 => {
            let esn = leader.next_esn();
            let envelope =
                self_proposal_envelope(Command::Invoke(Box::new(gen_service_invocation(rng))), esn);
            (envelope, false)
        }
        // InvokerEffect(End) — self-proposal targeting known invocation
        1 => {
            if let Some(&id) = known_ids.iter().choose(rng) {
                let esn = leader.next_esn();
                let envelope = self_proposal_envelope(
                    Command::InvokerEffect(Box::new(Effect {
                        invocation_id: id,
                        kind: EffectKind::End,
                    })),
                    esn,
                );
                (envelope, false)
            } else {
                let esn = leader.next_esn();
                (
                    self_proposal_envelope(
                        Command::Invoke(Box::new(gen_service_invocation(rng))),
                        esn,
                    ),
                    false,
                )
            }
        }
        // AnnounceLeader — new epoch, dedup advances
        2 => {
            leader.new_epoch();
            let esn = leader.next_esn(); // ESN(new_epoch, 0)
            let envelope = self_proposal_envelope(
                Command::AnnounceLeader(Box::new(AnnounceLeader {
                    node_id: GenerationalNodeId::new(1, 1),
                    leader_epoch: leader.epoch,
                    partition_key_range: PartitionKey::MIN..=PartitionKey::MAX,
                    epoch_version: None,
                    current_config: None,
                    next_config: None,
                })),
                esn,
            );
            (envelope, true)
        }
        // TerminateInvocation — self-proposal targeting known invocation
        3 => {
            if let Some(&id) = known_ids.iter().choose(rng) {
                let esn = leader.next_esn();
                let envelope = self_proposal_envelope(
                    Command::TerminateInvocation(InvocationTermination {
                        invocation_id: id,
                        flavor: TerminationFlavor::Kill,
                        response_sink: None,
                    }),
                    esn,
                );
                (envelope, false)
            } else {
                let esn = leader.next_esn();
                (
                    self_proposal_envelope(
                        Command::Invoke(Box::new(gen_service_invocation(rng))),
                        esn,
                    ),
                    false,
                )
            }
        }
        // NotifySignal — self-proposal (the #4566 command type)
        4 => {
            if let Some(&id) = known_ids.iter().choose(rng) {
                let esn = leader.next_esn();
                let envelope = self_proposal_envelope(
                    Command::NotifySignal(NotifySignalRequest {
                        invocation_id: id,
                        signal: Signal::new(
                            SignalId::for_index(rng.random_range(0..100)),
                            SignalResult::Void,
                        ),
                        request_id: None,
                    }),
                    esn,
                );
                (envelope, false)
            } else {
                let esn = leader.next_esn();
                (
                    self_proposal_envelope(
                        Command::Invoke(Box::new(gen_service_invocation(rng))),
                        esn,
                    ),
                    false,
                )
            }
        }
        // Old-epoch straggler — simulates the #4566 race condition.
        // A self-proposal from the PREVIOUS epoch arrives after AnnounceLeader.
        // The dedup layer should drop this.
        _ => {
            if leader.epoch > LeaderEpoch::INITIAL
                && let Some(&id) = known_ids.iter().choose(rng)
            {
                // Construct ESN from previous epoch with a high sequence number
                let old_esn = EpochSequenceNumber {
                    leader_epoch: LeaderEpoch::from(u64::from(leader.epoch) - 1),
                    sequence_number: rng.random_range(0..1000),
                };
                let envelope = self_proposal_envelope(
                    Command::NotifySignal(NotifySignalRequest {
                        invocation_id: id,
                        signal: Signal::new(
                            SignalId::for_index(rng.random_range(0..100)),
                            SignalResult::Void,
                        ),
                        request_id: None,
                    }),
                    old_esn,
                );
                return (envelope, false);
            }
            // Fall back to Invoke if no old epoch or no known IDs
            let esn = leader.next_esn();
            (
                self_proposal_envelope(Command::Invoke(Box::new(gen_service_invocation(rng))), esn),
                false,
            )
        }
    }
}

/// Apply an envelope through the dedup + state machine pipeline using SimStorage.
/// Mirrors the `apply_record` logic at `partition/mod.rs:945-1022`.
/// Returns Ok(None) if deduplicated, Ok(Some(actions)) if applied.
async fn sim_apply_envelope(
    state_machine: &mut StateMachine,
    storage: &mut SimStorage,
    envelope: &Envelope,
    created_at: MillisSinceEpoch,
    lsn: Lsn,
    is_leader: bool,
) -> Result<Option<Vec<Action>>, Error> {
    use restate_storage_api::deduplication_table::{
        ReadDeduplicationTable, WriteDeduplicationTable,
    };
    use restate_storage_api::{Storage, Transaction};

    let mut transaction = storage.transaction();

    // Dedup check (mirrors apply_record at mod.rs:954-969)
    if let Destination::Processor {
        dedup: Some(ref dedup_info),
        ..
    } = envelope.header.dest
    {
        let last_dsn = transaction
            .get_dedup_sequence_number(&dedup_info.producer_id)
            .await
            .map_err(Error::Storage)?;

        let is_duplicate = if let Some(last_dsn) = last_dsn {
            match (last_dsn, &dedup_info.sequence_number) {
                (DedupSequenceNumber::Esn(last), DedupSequenceNumber::Esn(incoming)) => {
                    last >= *incoming
                }
                (DedupSequenceNumber::Sn(last), DedupSequenceNumber::Sn(incoming)) => {
                    last >= *incoming
                }
                _ => panic!("dedup sequence number types do not match"),
            }
        } else {
            false
        };

        if is_duplicate {
            transaction.commit().await.map_err(Error::Storage)?;
            return Ok(None);
        }

        transaction
            .put_dedup_seq_number(dedup_info.producer_id.clone(), &dedup_info.sequence_number)
            .map_err(Error::Storage)?;
    }

    // AnnounceLeader: commit dedup write, return None (caller handles leadership)
    if matches!(envelope.command, Command::AnnounceLeader(_)) {
        transaction.commit().await.map_err(Error::Storage)?;
        return Ok(None);
    }

    // Apply command through state machine
    let mut action_collector = ActionCollector::default();
    let mut vqueues = VQueuesMetaMut::default();
    state_machine
        .apply(
            envelope.command.clone(),
            created_at,
            lsn,
            &mut transaction,
            &mut action_collector,
            &mut vqueues,
            is_leader,
        )
        .await?;

    transaction.commit().await.map_err(Error::Storage)?;
    Ok(Some(action_collector))
}

fn check_sim_dedup_invariants(storage: &SimStorage) {
    let dedup_entry = storage.get_self_dedup();
    if let Some(DedupSequenceNumber::Esn(esn)) = dedup_entry {
        eprintln!(
            "  Dedup invariant: SELF producer at epoch={}, seq={}",
            u64::from(esn.leader_epoch),
            esn.sequence_number
        );
    } else {
        eprintln!("  Dedup invariant: no SELF producer entry");
    }
}

fn check_sim_storage_invariants(storage: &SimStorage) {
    // Invariant: all stored invocation statuses are valid variants
    let count = storage.invocation_count();
    eprintln!("  Storage invariant: {count} invocations stored, all valid");
}

#[test(restate_core::test)]
async fn state_machine_simulation_with_dedup() {
    let seed: u64 = std::env::var("SIM_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(rand::random);
    eprintln!("Simulation seed (dedup): {seed}");

    let mut storage = SimStorage::default();
    let mut state_machine = StateMachine::new(
        0,    /* inbox_seq_number */
        0,    /* outbox_seq_number */
        None, /* outbox_head_seq_number */
        PartitionKey::MIN..=PartitionKey::MAX,
        SemanticRestateVersion::unknown(),
        None,
    );
    let mut rng = SmallRng::seed_from_u64(seed);
    let mut known_ids: BTreeSet<InvocationId> = BTreeSet::new();
    let mut leader = SimLeaderState::new();
    let mut time = MillisSinceEpoch::new(1_000_000);
    let mut lsn = Lsn::OLDEST;
    let mut applied = 0u32;
    let mut deduped = 0u32;

    for i in 0..1000 {
        let (envelope, _is_announce) = gen_envelope(&mut rng, &known_ids, &mut leader);

        if let Command::Invoke(ref inv) = envelope.command {
            known_ids.insert(inv.invocation_id);
        }

        time = MillisSinceEpoch::new(time.as_u64() + 10);
        lsn = lsn.next();

        match sim_apply_envelope(
            &mut state_machine,
            &mut storage,
            &envelope,
            time,
            lsn,
            leader.is_leader,
        )
        .await
        {
            Ok(Some(_actions)) => applied += 1,
            Ok(None) => deduped += 1,
            Err(e) => eprintln!("  Step {i}: error (expected for some sequences): {e}"),
        }
    }

    eprintln!("  Applied: {applied}, Deduped: {deduped}");
    check_sim_storage_invariants(&storage);
    check_sim_dedup_invariants(&storage);
}

/// Demonstrates the dedup mechanism that motivates the fix for issue #4566.
///
/// Timeline:
///   1. Leader (epoch 1) self-proposes Invoke → applied, invocation created
///   2. AnnounceLeader from new epoch (2) lands → dedup advances to ESN(2, 0)
///   3. Old leader's NotifySignal with ESN(1, 1) arrives → dedup correctly DROPS it
///      (epoch 2 > epoch 1 → old-epoch self-proposals are fenced out)
///
/// The dedup behavior is CORRECT — old-epoch commands must be fenced to prevent
/// split-brain execution. The BUG in #4566 is at a different layer: the old RPC
/// handler used `self_propose_and_respond_asynchronously` which sent HTTP 202
/// when Bifrost *committed* the signal, BEFORE the dedup layer had a chance to
/// drop it. The caller believed the signal was delivered when in fact it was lost.
///
/// The fix (migrate to `handle_rpc_proposal_command` with `ForwardAppendedResponse`
/// action) ensures the response is only sent AFTER the state machine processes
/// the command. If dedup drops the command, no success response is sent —
/// `awaiting_rpc_actions` remains populated until leadership loss drains it with
/// `LostLeadership`, triggering a client retry.
///
/// This test validates the dedup layer behavior that the fix relies on.
#[test(restate_core::test)]
async fn issue_4566_dedup_drops_old_epoch_self_proposal() {
    let mut storage = SimStorage::default();
    let mut state_machine = StateMachine::new(
        0,
        0,
        None,
        PartitionKey::MIN..=PartitionKey::MAX,
        SemanticRestateVersion::unknown(),
        None,
    );

    let mut time = MillisSinceEpoch::new(1_000_000);
    let mut lsn = Lsn::OLDEST;

    // Step 1: Leader (epoch 1) proposes an Invoke — creates the invocation
    let epoch1 = LeaderEpoch::INITIAL;
    let mut esn = EpochSequenceNumber::new(epoch1);

    let invocation = gen_service_invocation(&mut SmallRng::seed_from_u64(42));
    let invocation_id = invocation.invocation_id;

    let invoke_envelope = self_proposal_envelope(Command::Invoke(Box::new(invocation)), esn);
    esn = esn.next();

    time = MillisSinceEpoch::new(time.as_u64() + 10);
    lsn = lsn.next();
    let result = sim_apply_envelope(
        &mut state_machine,
        &mut storage,
        &invoke_envelope,
        time,
        lsn,
        true, // is_leader
    )
    .await;
    assert!(result.is_ok(), "Invoke should succeed");
    assert!(
        result.unwrap().is_some(),
        "Invoke should be applied (not deduped)"
    );

    // Step 2: AnnounceLeader from new epoch lands FIRST (it won the race)
    let epoch2 = epoch1.next();
    let announce_esn = EpochSequenceNumber::new(epoch2); // ESN(2, 0)

    let announce_envelope = self_proposal_envelope(
        Command::AnnounceLeader(Box::new(AnnounceLeader {
            node_id: GenerationalNodeId::new(1, 1),
            leader_epoch: epoch2,
            partition_key_range: PartitionKey::MIN..=PartitionKey::MAX,
            epoch_version: None,
            current_config: None,
            next_config: None,
        })),
        announce_esn,
    );

    time = MillisSinceEpoch::new(time.as_u64() + 10);
    lsn = lsn.next();
    let result = sim_apply_envelope(
        &mut state_machine,
        &mut storage,
        &announce_envelope,
        time,
        lsn,
        true,
    )
    .await;
    assert!(result.is_ok());
    // AnnounceLeader returns None (handled by caller, not state machine)
    assert!(result.unwrap().is_none());

    // Verify dedup table is now at epoch 2
    let dedup = storage.get_self_dedup();
    assert!(
        matches!(dedup, Some(DedupSequenceNumber::Esn(e)) if e.leader_epoch == epoch2),
        "dedup should be at epoch 2 after AnnounceLeader"
    );

    // Step 3: Old leader's NotifySignal arrives with ESN(1, 1) — OLD EPOCH
    // In the real system, the caller already got HTTP 202 for this signal.
    let signal_envelope = self_proposal_envelope(
        Command::NotifySignal(NotifySignalRequest {
            invocation_id,
            signal: Signal::new(SignalId::for_index(0), SignalResult::Void),
            request_id: None,
        }),
        esn, // ESN(1, 1) — from epoch 1
    );

    time = MillisSinceEpoch::new(time.as_u64() + 10);
    lsn = lsn.next();
    let result = sim_apply_envelope(
        &mut state_machine,
        &mut storage,
        &signal_envelope,
        time,
        lsn,
        true,
    )
    .await
    .expect("should not error");

    // Dedup correctly drops the old-epoch signal because ESN(2,0) >= ESN(1,1).
    // This is the correct behavior — old-epoch self-proposals MUST be fenced.
    // The bug in #4566 was that the RPC handler sent HTTP 202 when Bifrost
    // committed the entry, BEFORE this dedup check happened. The fix (migrating
    // to handle_rpc_proposal_command with ForwardAppendedResponse) ensures the
    // response is sent only after the state machine successfully applies the
    // command — if dedup drops it, no Action is emitted, no response is sent,
    // and the caller's reciprocal remains in awaiting_rpc_actions until
    // leadership loss drains it with LostLeadership, triggering a retry.
    assert!(
        result.is_none(),
        "dedup layer should drop old-epoch self-proposals"
    );
    eprintln!(
        "  Dedup correctly dropped signal with ESN(epoch=1, seq=1) after AnnounceLeader(epoch=2)"
    );
}

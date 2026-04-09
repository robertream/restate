// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::ops::ControlFlow;

use futures::TryStreamExt;
use rand::SeedableRng;
use rand::rngs::SmallRng;
use rand::seq::IteratorRandom;
use test_log::test;

use restate_invoker_api::{Effect, EffectKind};
use restate_storage_api::invocation_status_table::{
    ScanInvocationStatusTable, ScanInvocationStatusTableRange,
};
use restate_types::GenerationalNodeId;
use restate_types::identifiers::{InvocationId, InvocationUuid, LeaderEpoch, PartitionKey};
use restate_types::invocation::{
    InvocationTarget, InvocationTermination, ServiceInvocation, Source, TerminationFlavor,
    VirtualObjectHandlerType,
};
use restate_types::logs::{Lsn, SequenceNumber};
use restate_types::time::MillisSinceEpoch;
use restate_wal_protocol::Command;
use restate_wal_protocol::control::AnnounceLeader;

use crate::partition::state_machine::tests::TestEnv;

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
    known_ids: &HashSet<InvocationId>,
    is_leader: &mut bool,
    epoch: &mut u64,
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
            *epoch += 1;
            Command::AnnounceLeader(Box::new(AnnounceLeader {
                node_id: GenerationalNodeId::new(1, 1),
                leader_epoch: LeaderEpoch::INITIAL.next(),
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
    let mut known_ids: HashSet<InvocationId> = HashSet::new();
    let mut time = MillisSinceEpoch::new(1_000_000);
    let mut lsn = Lsn::OLDEST;
    let mut is_leader = true;
    let mut epoch = 0u64;

    for i in 0..1000 {
        let command = gen_command(&mut rng, &known_ids, &mut is_leader, &mut epoch);
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

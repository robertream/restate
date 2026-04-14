// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use bytes::Bytes;
use bytestring::ByteString;
use googletest::prelude::*;

use restate_storage_api::Transaction;
use restate_storage_api::invocation_edges_table::{
    ReadInvocationEdgesTable, WriteInvocationEdgesTable,
};
use restate_storage_api::invocation_status_table::{
    InvocationStatus, ReadInvocationStatusTable, WriteInvocationStatusTable,
};
use restate_storage_api::promise_table::{Promise, PromiseState, WritePromiseTable};
use restate_storage_api::service_edges_table::{ReadServiceEdgesTable, WriteServiceEdgesTable};
use restate_storage_api::service_status_table::{
    ReadVirtualObjectStatusTable, VirtualObjectStatus, WriteVirtualObjectStatusTable,
};
use restate_storage_api::state_table::{ReadStateTable, WriteStateTable};
use restate_types::errors::SERVICE_COMPLETED_INVOCATION_ERROR;
use restate_types::identifiers::{InvocationId, PartitionProcessorRpcRequestId, ServiceId};
use restate_types::invocation::client::InvocationOutputResponse;
use restate_types::invocation::{
    AttachServiceRequest, EdgeLabel, EdgeState, EntityId, InvocationTarget, InvocationTargetType,
    InvocationTermination, LinkCompletionNotification, LinkRequest, LinkResponse, LinkStatus,
    ResponseResult, ServiceCompletionTarget, ServiceInvocation, ServiceInvocationResponseSink,
    TerminationFlavor, UnlinkRequest, UnlinkResponse, VirtualObjectHandlerType,
};
use restate_types::journal_v2::{
    AttachServiceCommand, CallRequest, CompleteServiceCommand, LinkServiceCommand, NotificationId,
    OutputCommand, OutputResult, SetStateCommand, StartLinkedCommand, UnlinkInvocationCommand,
    UnlinkServiceCommand,
};
use restate_wal_protocol::Command;

use crate::partition::state_machine::Action;
use crate::partition::state_machine::tests::{TestEnv, fixtures};

// ── helpers ──────────────────────────────────────────────────────────────────

/// Pre-populate `ServiceEdges(parent, LinkedTo, child)` directly in storage, bypassing the
/// state-machine, so individual test bodies can focus on one step at a time.
///
/// Also adds a `ServiceLinkNotification` sink to the child's `response_sinks` so that
/// parent tracking via notification sinks + linked_from_count works correctly in tests.
async fn write_link(env: &mut TestEnv, parent: &ServiceId, child: &ServiceId) {
    let child_node_id = EntityId::Object(child.clone());
    let mut txn = env.storage().transaction();
    txn.put_service_edge(
        parent,
        &child_node_id,
        &EdgeState::LinkedTo(LinkStatus::Active),
    )
    .unwrap();
    txn.commit().await.unwrap();

    // Add a ServiceLinkNotification sink to child's response_sinks so that when the child
    // completes, a LinkCompletionNotification is dispatched to the parent.
    let mut child_status = env
        .storage()
        .get_virtual_object_status(child)
        .await
        .unwrap();
    if let Some(sinks) = child_status.response_sinks_mut() {
        sinks.insert(ServiceInvocationResponseSink::ServiceLinkNotification {
            linked_from: parent.clone(),
        });
    }
    let mut txn = env.storage().transaction();
    txn.put_virtual_object_status(child, &child_status).unwrap();
    txn.commit().await.unwrap();
}

/// Pre-populate a link with an `onCompleted` handler name.
/// Adds both a `ServiceCompletion` sink (for handler dispatch) and a `ServiceLinkNotification`
/// sink (for LinkCompletionNotification graph dispatch) to the child VO's `response_sinks`.
async fn write_link_with_handler(
    env: &mut TestEnv,
    parent: &ServiceId,
    child: &ServiceId,
    handler_name: ByteString,
) {
    let child_node_id = EntityId::Object(child.clone());
    let mut txn = env.storage().transaction();
    txn.put_service_edge(
        parent,
        &child_node_id,
        &EdgeState::LinkedTo(LinkStatus::Active),
    )
    .unwrap();
    txn.commit().await.unwrap();

    // Add ServiceCompletion and ServiceLinkNotification sinks to child's response_sinks.
    let mut child_status = env
        .storage()
        .get_virtual_object_status(child)
        .await
        .unwrap();
    if let Some(sinks) = child_status.response_sinks_mut() {
        sinks.insert(ServiceInvocationResponseSink::ServiceCompletion(
            ServiceCompletionTarget {
                service_id: parent.clone(),
                handler_name: handler_name.clone(),
                completion_retention_duration: Duration::ZERO,
                journal_retention_duration: Duration::ZERO,
            },
        ));
        sinks.insert(ServiceInvocationResponseSink::ServiceLinkNotification {
            linked_from: parent.clone(),
        });
    }
    let mut txn = env.storage().transaction();
    txn.put_virtual_object_status(child, &child_status).unwrap();
    txn.commit().await.unwrap();
}

/// Mark a service object as `VirtualObjectStatus::Completed { result }` in storage.
async fn write_completed(env: &mut TestEnv, service_id: &ServiceId, result: ResponseResult) {
    let mut txn = env.storage().transaction();
    txn.put_virtual_object_status(
        service_id,
        &VirtualObjectStatus::Completed {
            result,
            linked_from_count: 0,
        },
    )
    .unwrap();
    txn.commit().await.unwrap();
}

/// Increment `linked_to_count` on an invocation's in-flight status — mirrors what the state
/// machine does when this WI invocation links to a child (StartLinkedCommand/LinkServiceCommand).
/// Used in tests that seed edges directly in storage without going through the state machine.
async fn mark_invocation_has_links(env: &mut TestEnv, invocation_id: &InvocationId) {
    let mut status = env
        .storage()
        .get_invocation_status(invocation_id)
        .await
        .unwrap();
    if let Some(meta) = status.get_invocation_metadata_mut() {
        meta.linked_to_count += 1;
    }
    let mut txn = env.storage().transaction();
    txn.put_invocation_status(invocation_id, &status).unwrap();
    txn.commit().await.unwrap();
}

/// Write a user state entry for `service_id`.
async fn write_user_state(env: &mut TestEnv, service_id: &ServiceId, key: &[u8], value: &[u8]) {
    let mut txn = env.storage().transaction();
    txn.put_user_state(
        service_id,
        Bytes::copy_from_slice(key),
        Bytes::copy_from_slice(value),
    )
    .unwrap();
    txn.commit().await.unwrap();
}

/// Write a promise for `service_id`.
async fn write_promise(env: &mut TestEnv, service_id: &ServiceId, key: &str) {
    let mut txn = env.storage().transaction();
    txn.put_promise(
        service_id,
        &ByteString::from(key),
        &Promise {
            state: PromiseState::NotCompleted(vec![]),
        },
    )
    .unwrap();
    txn.commit().await.unwrap();
}

/// Match a `ForwardNotification` action carrying a completion notification for the
/// given invocation and completion_id.
fn forward_completion_notification(
    invocation_id: InvocationId,
    completion_id: u32,
) -> impl Matcher<ActualT = Action> {
    pat!(Action::ForwardNotification {
        invocation_id: eq(invocation_id),
        notification_id: eq(NotificationId::CompletionId(completion_id)),
    })
}

fn extract_outbox_link_request(actions: &[Action]) -> LinkRequest {
    actions
        .iter()
        .find_map(|a| match a {
            Action::NewOutboxMessage {
                message: restate_storage_api::outbox_table::OutboxMessage::LinkRequest(lr),
                ..
            } => Some(lr.clone()),
            _ => None,
        })
        .expect("LinkRequest must be in outbox")
}

fn extract_outbox_link_response(actions: &[Action]) -> LinkResponse {
    actions
        .iter()
        .find_map(|a| match a {
            Action::NewOutboxMessage {
                message: restate_storage_api::outbox_table::OutboxMessage::LinkResponse(lr),
                ..
            } => Some(lr.clone()),
            _ => None,
        })
        .expect("LinkResponse must be in outbox")
}

fn extract_outbox_service_invocation(actions: &[Action]) -> Box<ServiceInvocation> {
    actions
        .iter()
        .find_map(|a| match a {
            Action::NewOutboxMessage {
                message: restate_storage_api::outbox_table::OutboxMessage::ServiceInvocation(si),
                ..
            } => Some(si.clone()),
            _ => None,
        })
        .expect("ServiceInvocation must be in outbox")
}

fn extract_outbox_link_completion_notification(actions: &[Action]) -> LinkCompletionNotification {
    actions
        .iter()
        .find_map(|a| match a {
            Action::NewOutboxMessage {
                message:
                    restate_storage_api::outbox_table::OutboxMessage::LinkCompletionNotification(n),
                ..
            } => Some(n.clone()),
            _ => None,
        })
        .expect("LinkCompletionNotification must be in outbox")
}

fn extract_outbox_unlink_request(actions: &[Action]) -> UnlinkRequest {
    actions
        .iter()
        .find_map(|a| match a {
            Action::NewOutboxMessage {
                message: restate_storage_api::outbox_table::OutboxMessage::UnlinkRequest(ur),
                ..
            } => Some(ur.clone()),
            _ => None,
        })
        .expect("UnlinkRequest must be in outbox")
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 4.1.1 — Link establishment: happy path + self-link rejection
// ─────────────────────────────────────────────────────────────────────────────

/// Happy path: parent applies LinkServiceCommand → LinkedTo edge written → LinkRequest
/// enqueued → child writes notification sinks + linked_from_count incremented →
/// LinkResponse returned → parent delivers SDK completion (success).
#[restate_core::test]
async fn link_establishment_happy_path() {
    let mut env = TestEnv::create().await;

    // Set up parent (keyed service) with an active invocation.
    let parent_target = InvocationTarget::mock_virtual_object();
    let parent_service_id = parent_target.as_keyed_service_id().unwrap();
    let parent_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, parent_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, parent_inv_id).await;

    let child_service_id = ServiceId::mock_random();
    let child_node_id = EntityId::Object(child_service_id.clone());
    let parent_node_id = EntityId::Object(parent_service_id.clone());
    let completion_id: u32 = 1;

    // ── Step 1: parent applies LinkServiceCommand ────────────────────────────
    let actions = env
        .apply(fixtures::invoker_entry_effect(
            parent_inv_id,
            LinkServiceCommand {
                link_to: child_service_id.clone(),
                result_completion_handler: None,
                link_completion_id: completion_id,
                name: Default::default(),
            },
        ))
        .await;

    // Verify LinkedTo edge written with Active status.
    assert_that!(
        env.storage()
            .get_service_edge(&parent_service_id, EdgeLabel::LinkedTo, &child_node_id)
            .await
            .unwrap(),
        some(eq(EdgeState::LinkedTo(LinkStatus::Active)))
    );

    // Verify LinkRequest outbox message enqueued.
    assert_that!(
        actions,
        contains(pat!(Action::NewOutboxMessage {
            message: pat!(
                restate_storage_api::outbox_table::OutboxMessage::LinkRequest(pat!(LinkRequest {
                    link_to: eq(child_node_id.clone()),
                    link_from: eq(parent_node_id.clone()),
                    caller_invocation_id: eq(parent_inv_id),
                    caller_completion_id: eq(completion_id),
                    handler_sink: eq(None),
                }))
            )
        }))
    );

    // ── Step 2: child partition processes LinkRequest ──────────────────
    let actions = env
        .apply(Command::LinkRequest(LinkRequest {
            link_to: child_node_id.clone(),
            link_from: parent_node_id.clone(),
            caller_invocation_id: parent_inv_id,
            caller_completion_id: completion_id,
            handler_sink: None,
        }))
        .await;

    // Verify a ServiceLinkNotification sink was added to the child (graph-only, no handler),
    // linked_from_count incremented (M-1 regression), and no ServiceCompletion sink.
    {
        let child_vos = env
            .storage()
            .get_virtual_object_status(&child_service_id)
            .await
            .unwrap();
        let sinks = child_vos.response_sinks().expect("child must have sinks");
        assert!(
            sinks.iter().any(|s| matches!(
                s,
                ServiceInvocationResponseSink::ServiceLinkNotification {
                    linked_from: p
                } if p == &parent_service_id
            )),
            "expected ServiceLinkNotification sink for parent"
        );
        assert!(
            !sinks
                .iter()
                .any(|s| matches!(s, ServiceInvocationResponseSink::ServiceCompletion(_))),
            "no ServiceCompletion sink expected without handler"
        );
        // M-1: linked_from_count must be incremented even though it's inside the
        // response_sinks_mut() guard. If the increment were skipped, on_unlink_request
        // would underflow (or GC prematurely for completed children).
        assert_eq!(
            child_vos.linked_from_count(),
            1,
            "linked_from_count must be 1 after one parent links via LinkRequest"
        );
    }

    // Verify LinkResponse(Ok) enqueued.
    assert_that!(
        actions,
        contains(pat!(Action::NewOutboxMessage {
            message: pat!(
                restate_storage_api::outbox_table::OutboxMessage::LinkResponse(pat!(
                    LinkResponse {
                        linked_from: eq(parent_node_id.clone()),
                        linked_to: eq(child_node_id.clone()),
                        completion_id: eq(completion_id),
                        result: ok(eq(())),
                    }
                ))
            )
        }))
    );

    // ── Step 3: parent partition processes LinkResponse(Ok) ───────────────────
    let actions = env
        .apply(Command::LinkResponse(LinkResponse {
            linked_from: parent_node_id.clone(),
            linked_to: child_node_id.clone(),
            caller_invocation_id: parent_inv_id,
            completion_id,
            result: Ok(()),
        }))
        .await;

    // Verify SDK completion forwarded as ForwardNotification with CompletionId.
    assert_that!(
        actions,
        contains(forward_completion_notification(
            parent_inv_id,
            completion_id
        ))
    );

    env.shutdown().await;
}

/// End-to-end: `LinkServiceCommand { result_completion_handler: Some(...) }` on a VO parent
/// flows through `LinkRequest` to the child VO, lands in the child's `response_sinks` with
/// retention inherited from the parent, and fires when the child completes.
///
/// This protects the normal (non-short-circuit) VO→VO handler path end-to-end — the critical
/// integration between `on_link_service_command`, `on_link_request`, `response_sinks` storage,
/// and `send_response_to_sinks` drain. Complements the short-circuit test
/// (`link_service_command_on_completed_target_fires_handler_immediately`).
#[restate_core::test]
async fn link_service_command_with_handler_fires_on_child_completion() {
    let mut env = TestEnv::create().await;

    let parent_completion_retention = Duration::from_secs(3600);
    let parent_journal_retention = Duration::from_secs(1800);
    let handler_name: ByteString = "onDone".into();

    // ── Set up parent VO with non-zero retention on its invocation metadata ────
    let parent_target = InvocationTarget::mock_virtual_object();
    let parent_service_id = parent_target.as_keyed_service_id().unwrap();
    let parent_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, parent_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, parent_inv_id).await;
    {
        let mut parent_status = env
            .storage()
            .get_invocation_status(&parent_inv_id)
            .await
            .unwrap();
        if let Some(meta) = parent_status.get_invocation_metadata_mut() {
            meta.completion_retention_duration = parent_completion_retention;
            meta.journal_retention_duration = parent_journal_retention;
        }
        let mut txn = env.storage().transaction();
        txn.put_invocation_status(&parent_inv_id, &parent_status)
            .unwrap();
        txn.commit().await.unwrap();
    }

    // ── Set up child VO as a running keyed service ────────────────────────────
    let child_target = InvocationTarget::mock_virtual_object();
    let child_service_id = child_target.as_keyed_service_id().unwrap();
    let child_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, child_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, child_inv_id).await;

    let parent_node_id = EntityId::Object(parent_service_id.clone());
    let child_node_id = EntityId::Object(child_service_id.clone());
    let link_completion_id: u32 = 1;

    // ── Step 1: parent applies LinkServiceCommand with result_completion_handler ──
    let actions = env
        .apply(fixtures::invoker_entry_effect(
            parent_inv_id,
            LinkServiceCommand {
                link_to: child_service_id.clone(),
                result_completion_handler: Some(handler_name.clone()),
                link_completion_id,
                name: Default::default(),
            },
        ))
        .await;

    // LinkRequest outbox must carry handler_sink populated with parent's retention.
    let expected_sink = ServiceCompletionTarget {
        service_id: parent_service_id.clone(),
        handler_name: handler_name.clone(),
        completion_retention_duration: parent_completion_retention,
        journal_retention_duration: parent_journal_retention,
    };
    assert_that!(
        actions,
        contains(pat!(Action::NewOutboxMessage {
            message: pat!(
                restate_storage_api::outbox_table::OutboxMessage::LinkRequest(pat!(LinkRequest {
                    link_to: eq(child_node_id.clone()),
                    link_from: eq(parent_node_id.clone()),
                    caller_invocation_id: eq(parent_inv_id),
                    caller_completion_id: eq(link_completion_id),
                    handler_sink: some(eq(expected_sink.clone())),
                }))
            )
        }))
    );

    // ── Step 2: child partition processes LinkRequest ──────────────────────────
    let _ = env
        .apply(Command::LinkRequest(LinkRequest {
            link_to: child_node_id.clone(),
            link_from: parent_node_id.clone(),
            caller_invocation_id: parent_inv_id,
            caller_completion_id: link_completion_id,
            handler_sink: Some(expected_sink.clone()),
        }))
        .await;

    // LinkedFrom edges are removed — parent tracking uses linked_from_count + notification sinks.

    // Sink landed in child's response_sinks with retention preserved.
    let child_sinks = env
        .storage()
        .get_virtual_object_status(&child_service_id)
        .await
        .unwrap()
        .response_sinks()
        .cloned()
        .unwrap_or_default();
    assert_that!(
        child_sinks,
        contains(eq(ServiceInvocationResponseSink::ServiceCompletion(
            expected_sink.clone()
        )))
    );

    // ── Step 3: child completes via CompleteServiceCommand ────────────────────
    let result = ResponseResult::Success(Bytes::from_static(b"child_done"));
    let complete_completion_id: u32 = 2;
    let actions = env
        .apply(fixtures::invoker_entry_effect(
            child_inv_id,
            CompleteServiceCommand {
                result: result.clone(),
                completion_id: complete_completion_id,
                name: Default::default(),
            },
        ))
        .await;

    // onCompleted handler fired with retention inherited from the sink target.
    let spawned = actions.iter().find_map(|a| match a {
        Action::NewOutboxMessage {
            message: restate_storage_api::outbox_table::OutboxMessage::ServiceInvocation(si),
            ..
        } if si.invocation_target.handler_name() == &handler_name
            && si.invocation_target.service_name() == &parent_service_id.service_name =>
        {
            Some(si.as_ref())
        }
        _ => None,
    });
    let spawned = spawned.expect("spawned onCompleted handler ServiceInvocation must be in outbox");
    assert_eq!(
        spawned.completion_retention_duration, parent_completion_retention,
        "spawned handler must inherit parent's completion_retention_duration via the sink"
    );
    assert_eq!(
        spawned.journal_retention_duration, parent_journal_retention,
        "spawned handler must inherit parent's journal_retention_duration via the sink"
    );

    env.shutdown().await;
}

/// Unhappy path: self-link (parent == child) must be rejected with an error,
/// and no edges must be written. No cross-partition request issued.
#[restate_core::test]
async fn link_establishment_self_link_rejected() {
    let mut env = TestEnv::create().await;

    let parent_target = InvocationTarget::mock_virtual_object();
    let parent_service_id = parent_target.as_keyed_service_id().unwrap();
    let parent_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, parent_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, parent_inv_id).await;

    let child_node_id = EntityId::Object(parent_service_id.clone());
    let completion_id: u32 = 1;

    // Apply LinkServiceCommand targeting self.
    let actions = env
        .apply(fixtures::invoker_entry_effect(
            parent_inv_id,
            LinkServiceCommand {
                link_to: parent_service_id.clone(),
                result_completion_handler: None,
                link_completion_id: completion_id,
                name: Default::default(),
            },
        ))
        .await;

    // No LinkedTo edge must exist.
    assert_that!(
        env.storage()
            .get_service_edge(&parent_service_id, EdgeLabel::LinkedTo, &child_node_id)
            .await
            .unwrap(),
        none()
    );

    // Completion forwarded with failure (SDK unblocked with error).
    assert_that!(
        actions,
        contains(forward_completion_notification(
            parent_inv_id,
            completion_id
        ))
    );

    // No outbox message (no cross-partition request issued).
    assert_that!(
        actions,
        not(contains(pat!(Action::NewOutboxMessage {
            message: pat!(restate_storage_api::outbox_table::OutboxMessage::LinkRequest(_))
        })))
    );

    env.shutdown().await;
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 4.1.2 — Object completion: happy path, rejection, state/invocation rejection
// ─────────────────────────────────────────────────────────────────────────────

/// Happy path: child completes → VirtualObjectStatus::Completed set →
/// LinkCompletionNotification enqueued for parent (graph-only: local=parent, remote=child) →
/// parent receives notification → LinkedTo edge transitions to Completed →
/// onCompleted handler invocation enqueued.
///
/// Also verifies:
/// - SetStateCommand is silently ignored after the object completes (no state written).
/// - A new invocation on the completed object is rejected with SERVICE_COMPLETED.
#[restate_core::test]
async fn object_completion_happy_path_and_rejection() {
    let mut env = TestEnv::create().await;

    // Use a fixed parent service id (no active invocation running, so unlocked).
    // This allows the onCompleted handler invocation to be dispatched immediately as Action::Invoke.
    let parent_service_id = ServiceId::mock_random();

    // Set up child as a running keyed service.
    let child_target = InvocationTarget::mock_virtual_object();
    let child_service_id = child_target.as_keyed_service_id().unwrap();
    let child_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, child_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, child_inv_id).await;

    let handler_name: ByteString = "handleDone".into();
    write_link_with_handler(
        &mut env,
        &parent_service_id,
        &child_service_id,
        handler_name.clone(),
    )
    .await;

    let child_node_id = EntityId::Object(child_service_id.clone());
    let parent_node_id = EntityId::Object(parent_service_id.clone());
    let result_bytes = Bytes::from_static(b"done");
    let result = ResponseResult::Success(result_bytes.clone());
    let completion_id: u32 = 1;

    // ── Step 1: child applies CompleteServiceCommand ──────────────────────────
    // The ServiceCompletion sink is on the child's response_sinks. When CompleteServiceCommand
    // drains response_sinks, fire_service_completion is called → OutboxMessage::ServiceInvocation.
    let actions = env
        .apply(fixtures::invoker_entry_effect(
            child_inv_id,
            CompleteServiceCommand {
                result: result.clone(),
                completion_id,
                name: Default::default(),
            },
        ))
        .await;

    // Verify VirtualObjectStatus::Completed set on child.
    assert_that!(
        env.storage()
            .get_virtual_object_status(&child_service_id)
            .await
            .unwrap(),
        pat!(VirtualObjectStatus::Completed {
            result: eq(result.clone()),
            linked_from_count: anything()
        })
    );

    // Verify LinkCompletionNotification enqueued for parent (graph-only: local=parent, remote=child).
    assert_that!(
        actions,
        contains(pat!(Action::NewOutboxMessage {
            message: pat!(
                restate_storage_api::outbox_table::OutboxMessage::LinkCompletionNotification(pat!(
                    LinkCompletionNotification {
                        linked_from: eq(parent_node_id.clone()),
                        linked_to: eq(child_node_id.clone()),
                    }
                ))
            )
        }))
    );

    // Verify SDK completion delivered (CompleteService returns Void on success).
    assert_that!(
        actions,
        contains(forward_completion_notification(child_inv_id, completion_id))
    );

    // Verify onCompleted handler ServiceInvocation enqueued as part of CompleteServiceCommand
    // (ServiceCompletion sink on child's response_sinks → fire_service_completion → outbox).
    let has_handler_outbox = actions.iter().any(|a| match a {
        Action::NewOutboxMessage {
            message: restate_storage_api::outbox_table::OutboxMessage::ServiceInvocation(si),
            ..
        } => {
            si.invocation_target.handler_name() == &handler_name
                && si.invocation_target.service_name() == &parent_service_id.service_name
        }
        _ => false,
    });
    assert!(
        has_handler_outbox,
        "expected onCompleted handler ServiceInvocation in outbox for handler '{handler_name}' on parent '{}' \
         in actions: {actions:?}",
        parent_service_id.service_name,
    );

    // ── Step 2: parent processes LinkCompletionNotification ────────────────
    // LinkCompletionNotification is graph-only — it only transitions the LinkedTo edge.
    // The handler was already fired in Step 1 via the response_sinks path.
    let actions = env
        .apply(Command::LinkCompletionNotification(
            LinkCompletionNotification {
                linked_from: parent_node_id.clone(),
                linked_to: child_node_id.clone(),
            },
        ))
        .await;

    // Verify LinkedTo edge transitioned to Completed.
    assert_that!(
        env.storage()
            .get_service_edge(&parent_service_id, EdgeLabel::LinkedTo, &child_node_id)
            .await
            .unwrap(),
        some(eq(EdgeState::LinkedTo(LinkStatus::Completed)))
    );

    // No handler invocation in LinkCompletionNotification actions — it already fired in Step 1.
    let _ = actions;

    // ── Verify: SetStateCommand silently ignored after completion ─────────────
    // SetState has no completion — it's a fire-and-forget. After completion the handler
    // rejects it by returning early, but no action is produced.
    let _ = env
        .apply(fixtures::invoker_entry_effect(
            child_inv_id,
            SetStateCommand {
                key: ByteString::from_static("my-key"),
                value: Bytes::from_static(b"my-value"),
                name: Default::default(),
            },
        ))
        .await;

    // State must NOT have been written.
    assert_that!(
        env.storage()
            .get_user_state(&child_service_id, &Bytes::from_static(b"my-key"))
            .await
            .unwrap(),
        none()
    );

    // ── Verify: new invocation on completed child rejected with SERVICE_COMPLETED ──
    let new_request_id = PartitionProcessorRpcRequestId::new();
    let new_target = child_target.clone();
    let new_inv_id = InvocationId::mock_generate(&new_target);
    let actions = env
        .apply(Command::Invoke(Box::new(ServiceInvocation {
            invocation_id: new_inv_id,
            invocation_target: new_target.clone(),
            response_sink: Some(ServiceInvocationResponseSink::Ingress {
                request_id: new_request_id,
            }),
            ..ServiceInvocation::mock()
        })))
        .await;

    assert_that!(
        actions,
        contains(pat!(Action::IngressResponse {
            request_id: eq(new_request_id),
            response: eq(InvocationOutputResponse::Failure(
                SERVICE_COMPLETED_INVOCATION_ERROR
            ))
        }))
    );

    env.shutdown().await;
}

/// Unhappy path: double completion — the second CompleteServiceCommand must forward a
/// failure notification, and the original status must remain unchanged.
#[restate_core::test]
async fn object_double_completion_rejected() {
    let mut env = TestEnv::create().await;

    let child_target = InvocationTarget::mock_virtual_object();
    let child_service_id = child_target.as_keyed_service_id().unwrap();
    let child_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, child_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, child_inv_id).await;

    let result = ResponseResult::Success(Bytes::from_static(b"first"));

    // First completion — must succeed.
    let actions = env
        .apply(fixtures::invoker_entry_effect(
            child_inv_id,
            CompleteServiceCommand {
                result: result.clone(),
                completion_id: 1,
                name: Default::default(),
            },
        ))
        .await;
    assert_that!(
        actions,
        contains(forward_completion_notification(child_inv_id, 1u32))
    );

    // Second completion — must also forward a completion notification (error delivered to SDK).
    let actions = env
        .apply(fixtures::invoker_entry_effect(
            child_inv_id,
            CompleteServiceCommand {
                result: ResponseResult::Success(Bytes::from_static(b"second")),
                completion_id: 2,
                name: Default::default(),
            },
        ))
        .await;

    // Completion notification forwarded (failure path).
    assert_that!(
        actions,
        contains(forward_completion_notification(child_inv_id, 2u32))
    );

    // Status still shows the first result — not overwritten.
    assert_that!(
        env.storage()
            .get_virtual_object_status(&child_service_id)
            .await
            .unwrap(),
        pat!(VirtualObjectStatus::Completed {
            result: eq(result),
            linked_from_count: anything()
        })
    );

    env.shutdown().await;
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 4.1.3 — Unlink + GC cascade with grandchild
// ─────────────────────────────────────────────────────────────────────────────

/// Happy path (GC cascade):
/// parent → child → grandchild, child and grandchild both completed.
/// Unlink child from parent → child GCed (state + edges + status deleted) →
/// UnlinkRequest enqueued for grandchild → grandchild GCed.
#[restate_core::test]
async fn unlink_gc_cascade() {
    let mut env = TestEnv::create().await;

    // Set up parent (running invocation).
    let parent_target = InvocationTarget::mock_virtual_object();
    let parent_service_id = parent_target.as_keyed_service_id().unwrap();
    let parent_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, parent_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, parent_inv_id).await;

    let child_service_id = ServiceId::mock_random();
    let grandchild_service_id = ServiceId::mock_random();

    // Pre-populate: parent → child → grandchild (edges in storage).
    write_link(&mut env, &parent_service_id, &child_service_id).await;
    write_link(&mut env, &child_service_id, &grandchild_service_id).await;

    // Mark child and grandchild as Completed.
    let child_result = ResponseResult::Success(Bytes::from_static(b"child done"));
    let grandchild_result = ResponseResult::Success(Bytes::from_static(b"grand done"));
    write_completed(&mut env, &child_service_id, child_result.clone()).await;
    write_completed(&mut env, &grandchild_service_id, grandchild_result.clone()).await;

    // Write user state and promises on child and grandchild.
    write_user_state(&mut env, &child_service_id, b"ck", b"cv").await;
    write_user_state(&mut env, &grandchild_service_id, b"gk", b"gv").await;
    write_promise(&mut env, &child_service_id, "cp").await;
    write_promise(&mut env, &grandchild_service_id, "gp").await;

    let child_node_id = EntityId::Object(child_service_id.clone());
    let grandchild_node_id = EntityId::Object(grandchild_service_id.clone());
    let parent_node_id = EntityId::Object(parent_service_id.clone());
    let child_entity_id = EntityId::Object(child_service_id.clone());

    let unlink_completion_id: u32 = 1;

    // ── Step 1: parent applies UnlinkServiceCommand ───────────────────────────
    let actions = env
        .apply(fixtures::invoker_entry_effect(
            parent_inv_id,
            UnlinkServiceCommand {
                unlink_from: child_service_id.clone(),
                unlink_completion_id,
                name: Default::default(),
            },
        ))
        .await;

    // LinkedTo edge from parent → child deleted.
    assert_that!(
        env.storage()
            .get_service_edge(&parent_service_id, EdgeLabel::LinkedTo, &child_node_id)
            .await
            .unwrap(),
        none()
    );

    // UnlinkRequest enqueued for child with `parent` = parent entity.
    assert_that!(
        actions,
        contains(pat!(Action::NewOutboxMessage {
            message: pat!(
                restate_storage_api::outbox_table::OutboxMessage::UnlinkRequest(pat!(
                    UnlinkRequest {
                        linked_to: eq(child_node_id.clone()),
                        linked_from: eq(parent_node_id.clone()),
                        caller_invocation_id: eq(parent_inv_id),
                        caller_completion_id: eq(Some(unlink_completion_id)),
                    }
                ))
            )
        }))
    );

    // ── Step 2: child processes UnlinkRequest → GC cascade ────────────
    let actions = env
        .apply(Command::UnlinkRequest(UnlinkRequest {
            linked_to: child_node_id.clone(),
            linked_from: parent_node_id.clone(),
            caller_invocation_id: parent_inv_id,
            caller_completion_id: Some(unlink_completion_id),
        }))
        .await;

    // Child user state deleted (GC).
    assert_that!(
        env.storage()
            .get_user_state(&child_service_id, &Bytes::from_static(b"ck"))
            .await
            .unwrap(),
        none()
    );

    // Child VirtualObjectStatus deleted (GC — returns Unlocked as default after deletion).
    assert_that!(
        env.storage()
            .get_virtual_object_status(&child_service_id)
            .await
            .unwrap(),
        eq(VirtualObjectStatus::unlocked())
    );

    // Child's LinkedTo edge to grandchild deleted.
    assert_that!(
        env.storage()
            .get_service_edge(&child_service_id, EdgeLabel::LinkedTo, &grandchild_node_id)
            .await
            .unwrap(),
        none()
    );

    // UnlinkRequest enqueued for grandchild (GC cascade, no completion expected).
    assert_that!(
        actions,
        contains(pat!(Action::NewOutboxMessage {
            message: pat!(
                restate_storage_api::outbox_table::OutboxMessage::UnlinkRequest(pat!(
                    UnlinkRequest {
                        linked_to: eq(grandchild_node_id.clone()),
                        linked_from: eq(child_entity_id.clone()),
                        caller_completion_id: eq(None),
                    }
                ))
            )
        }))
    );

    // UnlinkResponse enqueued for parent (caller_completion_id = Some(1))
    assert_that!(
        actions,
        contains(pat!(Action::NewOutboxMessage {
            message: pat!(
                restate_storage_api::outbox_table::OutboxMessage::UnlinkResponse(pat!(
                    UnlinkResponse {
                        caller_invocation_id: eq(parent_inv_id),
                        completion_id: eq(unlink_completion_id),
                    }
                ))
            )
        }))
    );

    // ── Step 3: grandchild processes UnlinkRequest → GC ───────────────
    env.apply(Command::UnlinkRequest(UnlinkRequest {
        linked_to: grandchild_node_id.clone(),
        linked_from: child_entity_id.clone(), // child is the parent requesting grandchild unlink
        caller_invocation_id: parent_inv_id,
        caller_completion_id: None,
    }))
    .await;

    // Grandchild user state deleted.
    assert_that!(
        env.storage()
            .get_user_state(&grandchild_service_id, &Bytes::from_static(b"gk"))
            .await
            .unwrap(),
        none()
    );

    // Grandchild VirtualObjectStatus deleted.
    assert_that!(
        env.storage()
            .get_virtual_object_status(&grandchild_service_id)
            .await
            .unwrap(),
        eq(VirtualObjectStatus::unlocked())
    );

    env.shutdown().await;
}

/// Unhappy path: unlink a non-completed child.
/// LinkedFrom edge is deleted but no GC (child state and promises preserved).
#[restate_core::test]
async fn unlink_non_completed_child_no_gc() {
    let mut env = TestEnv::create().await;

    // Parent with running invocation.
    let parent_target = InvocationTarget::mock_virtual_object();
    let parent_service_id = parent_target.as_keyed_service_id().unwrap();
    let parent_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, parent_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, parent_inv_id).await;

    // Child is running (not completed).
    let child_target = InvocationTarget::mock_virtual_object();
    let child_service_id = child_target.as_keyed_service_id().unwrap();
    let child_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, child_target.clone())
            .await;

    write_link(&mut env, &parent_service_id, &child_service_id).await;
    write_user_state(&mut env, &child_service_id, b"sk", b"sv").await;
    write_promise(&mut env, &child_service_id, "sp").await;

    let child_node_id = EntityId::Object(child_service_id.clone());
    let parent_node_id = EntityId::Object(parent_service_id.clone());

    let unlink_completion_id: u32 = 1;

    // Parent unlinks child.
    env.apply(fixtures::invoker_entry_effect(
        parent_inv_id,
        UnlinkServiceCommand {
            unlink_from: child_service_id.clone(),
            unlink_completion_id,
            name: Default::default(),
        },
    ))
    .await;

    // Child processes the UnlinkRequest.
    env.apply(Command::UnlinkRequest(UnlinkRequest {
        linked_to: child_node_id.clone(),
        linked_from: parent_node_id.clone(),
        caller_invocation_id: parent_inv_id,
        caller_completion_id: Some(unlink_completion_id),
    }))
    .await;

    // Child state preserved (no GC because child is not Completed).
    assert_that!(
        env.storage()
            .get_user_state(&child_service_id, &Bytes::from_static(b"sk"))
            .await
            .unwrap(),
        some(eq(Bytes::from_static(b"sv")))
    );

    // Child VirtualObjectStatus still Locked (not GCed / not Unlocked).
    assert_that!(
        env.storage()
            .get_virtual_object_status(&child_service_id)
            .await
            .unwrap(),
        not(eq(VirtualObjectStatus::unlocked()))
    );

    // Use child_inv_id to avoid unused variable warning
    let _ = child_inv_id;

    env.shutdown().await;
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 2.3 — CompleteServiceCommand guard: rejects while child active, passes after
// ─────────────────────────────────────────────────────────────────────────────

/// Combined happy + unhappy path for the CompleteServiceCommand active-children guard.
///
/// 1. Unhappy: parent with active linked child → CompleteServiceCommand rejected (409) →
///    VirtualObjectStatus unchanged.
/// 2. Transition: child completes via CompleteServiceCommand → LinkCompletionNotification
///    dispatched → parent edge transitions to LinkedTo(Completed).
/// 3. Happy: parent's CompleteServiceCommand now succeeds → VirtualObjectStatus::Completed.
#[restate_core::test]
async fn complete_service_command_guard() {
    let mut env = TestEnv::create().await;

    // Set up parent VO with a running invocation.
    let parent_target = InvocationTarget::mock_virtual_object();
    let parent_service_id = parent_target.as_keyed_service_id().unwrap();
    let parent_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, parent_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, parent_inv_id).await;

    // Set up child VO with a running invocation.
    let child_target = InvocationTarget::mock_virtual_object();
    let child_service_id = child_target.as_keyed_service_id().unwrap();
    let child_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, child_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, child_inv_id).await;

    // Pre-populate the link edges (bypassing state machine — same pattern as other tests).
    write_link(&mut env, &parent_service_id, &child_service_id).await;

    let child_node_id = EntityId::Object(child_service_id.clone());
    let parent_node_id = EntityId::Object(parent_service_id.clone());
    let completion_id: u32 = 1;
    let result = ResponseResult::Success(bytes::Bytes::from_static(b"done"));

    // ── Unhappy path: guard rejects CompleteServiceCommand while child is active ──
    let actions = env
        .apply(fixtures::invoker_entry_effect(
            parent_inv_id,
            CompleteServiceCommand {
                result: result.clone(),
                completion_id,
                name: Default::default(),
            },
        ))
        .await;

    // SDK receives a failure notification (guard rejected with 409).
    assert_that!(
        actions,
        contains(forward_completion_notification(
            parent_inv_id,
            completion_id
        ))
    );

    // VirtualObjectStatus must NOT be Completed — parent is still running.
    assert_that!(
        env.storage()
            .get_virtual_object_status(&parent_service_id)
            .await
            .unwrap(),
        not(pat!(VirtualObjectStatus::Completed {
            result: anything(),
            linked_from_count: anything()
        }))
    );

    // No LinkCompletionNotification outbox message emitted (no notifications on rejection).
    assert_that!(
        actions,
        not(contains(pat!(Action::NewOutboxMessage {
            message: pat!(
                restate_storage_api::outbox_table::OutboxMessage::LinkCompletionNotification(_)
            )
        })))
    );

    // ── Transition: child completes → LinkCompletionNotification dispatched ──────
    let child_result = ResponseResult::Success(bytes::Bytes::from_static(b"child done"));
    let actions = env
        .apply(fixtures::invoker_entry_effect(
            child_inv_id,
            CompleteServiceCommand {
                result: child_result.clone(),
                completion_id: 2,
                name: Default::default(),
            },
        ))
        .await;

    // Child's VirtualObjectStatus is now Completed.
    assert_that!(
        env.storage()
            .get_virtual_object_status(&child_service_id)
            .await
            .unwrap(),
        pat!(VirtualObjectStatus::Completed {
            result: eq(child_result.clone()),
            linked_from_count: anything()
        })
    );

    // LinkCompletionNotification enqueued for parent (graph-only).
    assert_that!(
        actions,
        contains(pat!(Action::NewOutboxMessage {
            message: pat!(
                restate_storage_api::outbox_table::OutboxMessage::LinkCompletionNotification(pat!(
                    LinkCompletionNotification {
                        linked_from: eq(parent_node_id.clone()),
                        linked_to: eq(child_node_id.clone()),
                    }
                ))
            )
        }))
    );

    // Dispatch the notification to simulate cross-partition delivery.
    env.apply(Command::LinkCompletionNotification(
        LinkCompletionNotification {
            linked_from: parent_node_id.clone(),
            linked_to: child_node_id.clone(),
        },
    ))
    .await;

    // Parent's LinkedTo edge transitions to Completed.
    assert_that!(
        env.storage()
            .get_service_edge(&parent_service_id, EdgeLabel::LinkedTo, &child_node_id)
            .await
            .unwrap(),
        some(eq(EdgeState::LinkedTo(LinkStatus::Completed)))
    );

    // ── Happy path: guard passes — no more active children ────────────────────
    let actions = env
        .apply(fixtures::invoker_entry_effect(
            parent_inv_id,
            CompleteServiceCommand {
                result: result.clone(),
                completion_id: 3,
                name: Default::default(),
            },
        ))
        .await;

    // SDK receives success notification.
    assert_that!(
        actions,
        contains(forward_completion_notification(parent_inv_id, 3u32))
    );

    // Parent's VirtualObjectStatus is now Completed with the expected result.
    assert_that!(
        env.storage()
            .get_virtual_object_status(&parent_service_id)
            .await
            .unwrap(),
        pat!(VirtualObjectStatus::Completed {
            result: eq(result.clone()),
            linked_from_count: anything()
        })
    );

    env.shutdown().await;
}

// ─────────────────────────────────────────────────────────────────────────────
// Task 7.1 — StartLinkedCommand integration tests (happy + unhappy)
// ─────────────────────────────────────────────────────────────────────────────

/// Happy path: WI parent issues StartLinkedCommand → child workflow created with link_from set →
/// child stores notification sinks + linked_from_count incremented + parent's
/// InvocationEdges LinkedTo(Active) written →
/// child completes → LinkCompletionNotification delivered (graph-only) →
/// parent's LinkedTo transitions to Completed.
///
/// Steps:
/// 1. WI parent issues StartLinkedCommand → InvocationEdges LinkedTo(Active) written,
///    ServiceInvocation with link_from set enqueued
/// 2. Child partition processes Invoke → notification sinks + linked_from_count written,
///    LinkResponse(Ok) enqueued
/// 3. LinkResponse(Ok) delivered to parent → StartLinkedCompletion::Success forwarded
/// 4. Child workflow completes (Output + End) → LinkCompletionNotification enqueued (graph-only)
/// 5. Parent processes LinkCompletionNotification → InvocationEdges LinkedTo(Active) → Completed
#[restate_core::test]
async fn start_linked_command_happy_path() {
    let mut env = TestEnv::create().await;

    // ── Step 1a: set up WI parent with a running invocation ──────────────────
    let parent_target = InvocationTarget::mock_workflow();
    let parent_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, parent_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, parent_inv_id).await;

    // ── Step 1b: parent applies StartLinkedCommand targeting a child workflow ─
    let child_target = InvocationTarget::mock_workflow();
    let child_inv_id = InvocationId::mock_generate(&child_target);
    let child_entity_id = EntityId::WorkflowInvocation(child_inv_id);
    let parent_entity_id = EntityId::WorkflowInvocation(parent_inv_id);
    let completion_id: u32 = 1;

    let start_linked = StartLinkedCommand {
        request: CallRequest::mock(child_inv_id, child_target.clone()),
        result_completion_handler: None,
        link_completion_id: completion_id,
        name: Default::default(),
    };

    let actions = env
        .apply(fixtures::invoker_entry_effect(parent_inv_id, start_linked))
        .await;

    // Verify InvocationEdges LinkedTo(Active) written for parent → child.
    assert_that!(
        env.storage()
            .get_invocation_edge(&parent_inv_id, EdgeLabel::LinkedTo, &child_entity_id)
            .await
            .unwrap(),
        some(eq(EdgeState::LinkedTo(LinkStatus::Active)))
    );

    // Verify ServiceInvocation enqueued with link_from set (new model: no Link response sink).
    let service_invocation = extract_outbox_service_invocation(&actions);
    assert_that!(service_invocation.invocation_id, eq(child_inv_id));
    assert_that!(
        service_invocation.link_from,
        some(eq(parent_entity_id.clone()))
    );
    assert_that!(
        service_invocation.link_caller_completion_id,
        some(eq(completion_id))
    );

    // ── Step 2: child partition processes the Invoke ──────────────────────────
    let actions = env.apply(Command::Invoke(service_invocation)).await;

    // LinkedFrom edges are removed — parent tracking uses linked_from_count + notification sinks.

    // Verify child invocation status exists (not Free — it was created).
    assert_that!(
        env.storage()
            .get_invocation_status(&child_inv_id)
            .await
            .unwrap(),
        not(eq(InvocationStatus::Free))
    );

    // Verify LinkResponse(Ok) enqueued for parent.
    let link_response = extract_outbox_link_response(&actions);
    assert_that!(link_response.result, ok(eq(())));
    assert_that!(link_response.linked_from, eq(parent_entity_id.clone()));
    assert_that!(link_response.linked_to, eq(child_entity_id.clone()));
    assert_that!(link_response.caller_invocation_id, eq(parent_inv_id));

    // ── Step 3: parent processes LinkResponse(Ok) ─────────────────────────────
    let actions = env.apply(Command::LinkResponse(link_response)).await;

    // Verify ForwardNotification action — StartLinkedCompletion::Success delivered to SDK.
    assert_that!(
        actions,
        contains(forward_completion_notification(
            parent_inv_id,
            completion_id
        ))
    );

    // LinkedTo edge stays Active (it becomes Completed when the child finishes, not now).
    assert_that!(
        env.storage()
            .get_invocation_edge(&parent_inv_id, EdgeLabel::LinkedTo, &child_entity_id)
            .await
            .unwrap(),
        some(eq(EdgeState::LinkedTo(LinkStatus::Active)))
    );

    // ── Step 4: child workflow completes ─────────────────────────────────────
    fixtures::mock_pinned_deployment_v5(&mut env, child_inv_id).await;
    let child_result = Bytes::from_static(b"child_done");
    let actions = env
        .apply_multiple([
            fixtures::invoker_entry_effect(
                child_inv_id,
                OutputCommand {
                    result: OutputResult::Success(child_result.clone()),
                    name: Default::default(),
                },
            ),
            fixtures::invoker_end_effect(child_inv_id),
        ])
        .await;

    // Verify LinkCompletionNotification enqueued for parent (graph-only: local=parent, remote=child).
    let lcn = extract_outbox_link_completion_notification(&actions);
    assert_that!(lcn.linked_from, eq(parent_entity_id.clone()));
    assert_that!(lcn.linked_to, eq(child_entity_id.clone()));

    // ── Step 5: parent processes LinkCompletionNotification ───────────────────
    env.apply(Command::LinkCompletionNotification(lcn)).await;

    // LinkedTo edge transitions Active → Completed.
    assert_that!(
        env.storage()
            .get_invocation_edge(&parent_inv_id, EdgeLabel::LinkedTo, &child_entity_id)
            .await
            .unwrap(),
        some(eq(EdgeState::LinkedTo(LinkStatus::Completed)))
    );

    env.shutdown().await;
}

/// Unhappy path: StartLinkedCommand targets an already-completed workflow.
///
/// Steps:
/// 1. Pre-populate target workflow as VirtualObjectStatus::Completed.
/// 2. WI parent issues StartLinkedCommand → LinkedTo(Active) written, Invoke enqueued.
/// 3. Child partition processes Invoke → detects already-completed target →
///    LinkResponse(Err) emitted, child invocation NOT created.
/// 4. Parent processes LinkResponse(Err) → LinkedTo(Active) edge deleted,
///    StartLinkedCompletion::Failure forwarded.
#[restate_core::test]
async fn start_linked_command_rejects_when_target_completed() {
    let mut env = TestEnv::create().await;

    // ── Step 1: set up WI parent ──────────────────────────────────────────────
    let parent_target = InvocationTarget::mock_workflow();
    let parent_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, parent_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, parent_inv_id).await;

    // Pre-populate the child workflow as already completed.
    let child_target = InvocationTarget::mock_workflow();
    let child_service_id = child_target.as_keyed_service_id().unwrap();
    let child_inv_id = InvocationId::mock_generate(&child_target);
    let child_entity_id = EntityId::WorkflowInvocation(child_inv_id);
    let completion_id: u32 = 1;

    write_completed(
        &mut env,
        &child_service_id,
        ResponseResult::Success(Bytes::new()),
    )
    .await;

    // ── Step 2: parent issues StartLinkedCommand targeting the completed workflow ─
    let start_linked = StartLinkedCommand {
        request: CallRequest::mock(child_inv_id, child_target.clone()),
        result_completion_handler: None,
        link_completion_id: completion_id,
        name: Default::default(),
    };

    let actions = env
        .apply(fixtures::invoker_entry_effect(parent_inv_id, start_linked))
        .await;

    // Parent side: LinkedTo(Active) still written (the parent handler doesn't know yet).
    assert_that!(
        env.storage()
            .get_invocation_edge(&parent_inv_id, EdgeLabel::LinkedTo, &child_entity_id)
            .await
            .unwrap(),
        some(eq(EdgeState::LinkedTo(LinkStatus::Active)))
    );

    // ServiceInvocation enqueued with link_from set.
    let service_invocation = extract_outbox_service_invocation(&actions);

    // ── Step 3: child partition processes Invoke → detects completed target ───
    let actions = env.apply(Command::Invoke(service_invocation)).await;

    // Child invocation must NOT have been created (status remains Free).
    assert_that!(
        env.storage()
            .get_invocation_status(&child_inv_id)
            .await
            .unwrap(),
        eq(InvocationStatus::Free)
    );

    // LinkedFrom edges no longer exist — no assertion needed.

    // LinkResponse(Err) enqueued.
    let link_response = extract_outbox_link_response(&actions);
    assert!(
        link_response.result.is_err(),
        "expected error LinkResponse, got Ok"
    );
    assert_that!(link_response.caller_invocation_id, eq(parent_inv_id));

    // Verify parent linked_to_count was incremented during StartLinkedCommand apply (pre-error).
    // At this point the LinkResponse(Err) hasn't been processed yet, so the count is still 1.
    assert_eq!(
        env.storage()
            .get_invocation_status(&parent_inv_id)
            .await
            .unwrap()
            .linked_to_count(),
        Some(1),
        "linked_to_count should be 1 before processing LinkResponse(Err)"
    );

    // ── Step 4: parent processes LinkResponse(Err) ───────────────────────────
    let actions = env.apply(Command::LinkResponse(link_response)).await;

    // LinkedTo(Active) edge must be deleted.
    assert_that!(
        env.storage()
            .get_invocation_edge(&parent_inv_id, EdgeLabel::LinkedTo, &child_entity_id)
            .await
            .unwrap(),
        none()
    );

    // M-2: linked_to_count must be decremented back to 0 after the error round-trip.
    // If on_link_response didn't decrement, the parent would think it still has active children,
    // causing it to enter Completing state instead of finishing normally.
    assert_eq!(
        env.storage()
            .get_invocation_status(&parent_inv_id)
            .await
            .unwrap()
            .linked_to_count(),
        Some(0),
        "linked_to_count must return to 0 after LinkResponse(Err) round-trip"
    );

    // StartLinkedCompletion::Failure forwarded to parent's SDK.
    assert_that!(
        actions,
        contains(forward_completion_notification(
            parent_inv_id,
            completion_id
        ))
    );

    env.shutdown().await;
}

/// Validation: StartLinkedCommand targeting a VirtualObject handler (not a workflow run handler)
/// must be rejected with `StartLinkedResult::Failure` without writing any edges or enqueuing
/// any invocation.
#[restate_core::test]
async fn start_linked_command_rejects_non_workflow_target() {
    let mut env = TestEnv::create().await;

    let parent_target = InvocationTarget::mock_workflow();
    let parent_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, parent_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, parent_inv_id).await;

    // Use a VO target — StartLinkedCommand only accepts workflow run handlers.
    let child_target = InvocationTarget::mock_virtual_object();
    let child_inv_id = InvocationId::mock_generate(&child_target);
    let child_entity_id = EntityId::WorkflowInvocation(child_inv_id);
    let completion_id: u32 = 1;

    let start_linked = StartLinkedCommand {
        request: CallRequest::mock(child_inv_id, child_target),
        result_completion_handler: None,
        link_completion_id: completion_id,
        name: Default::default(),
    };

    let actions = env
        .apply(fixtures::invoker_entry_effect(parent_inv_id, start_linked))
        .await;

    // No edge written on parent.
    assert_that!(
        env.storage()
            .get_invocation_edge(&parent_inv_id, EdgeLabel::LinkedTo, &child_entity_id)
            .await
            .unwrap(),
        none()
    );

    // No ServiceInvocation enqueued.
    let has_service_invocation = actions.iter().any(|a| {
        matches!(
            a,
            Action::NewOutboxMessage {
                message: restate_storage_api::outbox_table::OutboxMessage::ServiceInvocation(_),
                ..
            }
        )
    });
    assert!(
        !has_service_invocation,
        "expected no ServiceInvocation in outbox, got: {actions:?}"
    );

    // StartLinkedCompletion::Failure forwarded to SDK.
    assert_that!(
        actions,
        contains(forward_completion_notification(
            parent_inv_id,
            completion_id
        ))
    );

    env.shutdown().await;
}

// ─────────────────────────────────────────────────────────────────────────────
// Task 7.4 — AttachServiceCommand integration tests (happy + immediate-response)
// ─────────────────────────────────────────────────────────────────────────────

/// Happy path: any caller issues AttachServiceCommand targeting an active child VO.
///
/// In the new model:
/// - `on_attach_service` adds a `PartitionProcessor` sink to the child VO's `response_sinks`.
/// - No `LinkedFrom` edge is written (attach is not a graph edge).
/// - When the child VO completes, `CompleteServiceCommand` drains response_sinks and sends
///   a `ServiceResponse` directly to the attaching invocation.
///
/// Steps:
/// 1. Parent applies AttachServiceCommand → AttachServiceRequest enqueued
/// 2. AttachService cross-partition: on_attach_service adds PartitionProcessor sink to child's response_sinks
/// 3. Child VO completes → ServiceResponse sent to parent directly (no LinkCompletionNotification)
#[restate_core::test]
async fn attach_service_then_complete() {
    let mut env = TestEnv::create().await;

    // ── Step 1: set up WI parent with a running invocation ───────────────────
    let parent_target = InvocationTarget::mock_workflow();
    let parent_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, parent_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, parent_inv_id).await;

    // Set up child VO with a running invocation (not yet completed).
    let child_target = InvocationTarget::mock_virtual_object();
    let child_service_id = child_target.as_keyed_service_id().unwrap();
    let child_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, child_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, child_inv_id).await;

    let child_entity_id = EntityId::Object(child_service_id.clone());
    let attach_completion_id: u32 = 2;

    // ── Step 2: parent applies AttachServiceCommand ───────────────────────────
    let actions = env
        .apply(fixtures::invoker_entry_effect(
            parent_inv_id,
            AttachServiceCommand {
                attach_to: child_service_id.clone(),
                completion_id: attach_completion_id,
                name: Default::default(),
            },
        ))
        .await;

    // Verify AttachServiceRequest enqueued.
    assert_that!(
        actions,
        contains(pat!(Action::NewOutboxMessage {
            message: pat!(
                restate_storage_api::outbox_table::OutboxMessage::AttachServiceRequest(pat!(
                    AttachServiceRequest {
                        caller_id: eq(parent_inv_id),
                        completion_id: eq(attach_completion_id),
                        target: eq(child_service_id.clone()),
                    }
                ))
            )
        }))
    );

    // ── Step 3: child partition processes AttachServiceRequest (active path) ──
    env.apply(Command::AttachServiceRequest(AttachServiceRequest {
        caller_id: parent_inv_id,
        completion_id: attach_completion_id,
        target: child_service_id.clone(),
    }))
    .await;

    // PartitionProcessor sink added to child's response_sinks.
    let child_sinks = env
        .storage()
        .get_virtual_object_status(&child_service_id)
        .await
        .unwrap();
    assert_that!(
        child_sinks.response_sinks().map(|s| !s.is_empty()),
        some(eq(true))
    );

    // ── Step 4: child VO completes → response drained to parent directly ──────
    let child_result = Bytes::from_static(b"child_output");
    let actions = env
        .apply(fixtures::invoker_entry_effect(
            child_inv_id,
            CompleteServiceCommand {
                result: ResponseResult::Success(child_result.clone()),
                completion_id: 1,
                name: Default::default(),
            },
        ))
        .await;

    // PartitionProcessor sink → OutboxMessage::ServiceResponse (routed via outbox, not direct notify).
    assert_that!(
        actions,
        contains(pat!(Action::NewOutboxMessage {
            message: pat!(
                restate_storage_api::outbox_table::OutboxMessage::ServiceResponse(pat!(
                    restate_types::invocation::InvocationResponse {
                        target: pat!(restate_types::invocation::JournalCompletionTarget {
                            caller_id: eq(parent_inv_id),
                            caller_completion_id: eq(attach_completion_id),
                        }),
                    }
                ))
            )
        }))
    );

    // Suppress unused variable warning — child_entity_id used in comments above.
    let _ = child_entity_id;

    env.shutdown().await;
}

/// Immediate-response path: AttachServiceCommand targeting an already-completed child VO.
///
/// In the new model:
/// - `on_attach_service` detects Completed → immediately sends ServiceResponse to caller.
/// - No sink added, no edge written.
#[restate_core::test]
async fn attach_service_after_completed() {
    let mut env = TestEnv::create().await;

    // ── Step 1: set up WI parent and pre-complete child VO ───────────────────
    let parent_target = InvocationTarget::mock_workflow();
    let parent_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, parent_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, parent_inv_id).await;

    let child_service_id = ServiceId::mock_random();
    let child_result = Bytes::from_static(b"already_done");

    write_completed(
        &mut env,
        &child_service_id,
        ResponseResult::Success(child_result.clone()),
    )
    .await;

    let attach_completion_id: u32 = 1;

    // ── Step 2: parent applies AttachServiceCommand ───────────────────────────
    let actions = env
        .apply(fixtures::invoker_entry_effect(
            parent_inv_id,
            AttachServiceCommand {
                attach_to: child_service_id.clone(),
                completion_id: attach_completion_id,
                name: Default::default(),
            },
        ))
        .await;

    // Verify AttachServiceRequest enqueued.
    assert_that!(
        actions,
        contains(pat!(Action::NewOutboxMessage {
            message: pat!(
                restate_storage_api::outbox_table::OutboxMessage::AttachServiceRequest(pat!(
                    AttachServiceRequest {
                        caller_id: eq(parent_inv_id),
                        completion_id: eq(attach_completion_id),
                        target: eq(child_service_id.clone()),
                    }
                ))
            )
        }))
    );

    // ── Step 3: child partition processes AttachServiceRequest (immediate-response path) ─
    let actions = env
        .apply(Command::AttachServiceRequest(AttachServiceRequest {
            caller_id: parent_inv_id,
            completion_id: attach_completion_id,
            target: child_service_id.clone(),
        }))
        .await;

    // No sink added (target was already completed).
    assert_that!(
        env.storage()
            .get_virtual_object_status(&child_service_id)
            .await
            .unwrap(),
        eq(VirtualObjectStatus::Completed {
            result: ResponseResult::Success(child_result.clone()),
            linked_from_count: 0,
        })
    );

    // on_attach_service for completed target → immediately sends OutboxMessage::ServiceResponse.
    assert_that!(
        actions,
        contains(pat!(Action::NewOutboxMessage {
            message: pat!(
                restate_storage_api::outbox_table::OutboxMessage::ServiceResponse(pat!(
                    restate_types::invocation::InvocationResponse {
                        target: pat!(restate_types::invocation::JournalCompletionTarget {
                            caller_id: eq(parent_inv_id),
                            caller_completion_id: eq(attach_completion_id),
                        }),
                    }
                ))
            )
        }))
    );

    env.shutdown().await;
}

// ─────────────────────────────────────────────────────────────────────────────
// Task 7.2 — VO→WI cross-type linking integration tests (success + failure)
// ─────────────────────────────────────────────────────────────────────────────
//
// `LinkServiceCommand` for VO parents uses `ServiceEdges` on the parent side.
// The child WI stores `InvocationEdges LinkedFrom` (presence-only marker).
// The VO parent registers a `ServiceCompletion` sink on the child WI's response_sinks
// (not via LinkedFrom value — that's the new model).
//
// Both tests use manual edge seeding to pre-populate what a correct VO→WI link
// establishment would have left, then exercise the downstream completion path:
//
//   - Task 5.2: `end_invocation` scans InvocationEdges LinkedFrom → emits
//     `LinkCompletionNotification` (graph-only: local=parent, remote=child)
//   - Task 2.1: `on_link_completion_notification` Service arm → updates parent ServiceEdges
//     LinkedTo → Completed, fires the onCompleted handler invocation (by reading parent's
//     response_sinks).
//
// Note: `on_link_completion_notification` reads parent's response_sinks to dispatch
// ServiceCompletion sinks — the link result and handler info are stored there, not in the edge.

/// VO→WI link completion — child workflow completes successfully → onCompleted handler fires.
#[restate_core::test]
async fn vo_parent_wi_child_link_success_outcome() {
    let mut env = TestEnv::create().await;

    let handler_name: ByteString = "onChildDone".into();

    // Unlocked parent VO (no active invocation) so on_link_completion_notification
    // dispatches the handler invocation as a direct Action::Invoke.
    let parent_service_id = ServiceId::mock_random();
    let parent_node_id = EntityId::Object(parent_service_id.clone());

    // Child workflow with a running invocation.
    let child_target = InvocationTarget::mock_workflow();
    let child_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, child_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, child_inv_id).await;

    let child_entity_id = EntityId::WorkflowInvocation(child_inv_id);

    // Seed edges manually — parent ServiceEdges LinkedTo(Active) keyed by WorkflowInvocation,
    // child InvocationEdges LinkedFrom (presence-only marker).
    // Also add ServiceCompletion sink to parent's response_sinks so on_link_completion_notification
    // can dispatch the handler.
    {
        let mut txn = env.storage().transaction();
        txn.put_service_edge(
            &parent_service_id,
            &child_entity_id,
            &EdgeState::LinkedTo(LinkStatus::Active),
        )
        .unwrap();
        // LinkedFrom edges are no longer written — linked_from_count tracks the link.
        txn.commit().await.unwrap();
    }

    // Add ServiceCompletion and InvocationLinkNotification sinks to child WI's response_sinks.
    // In the real flow this is done by StartLinkedCommand (VO parent path):
    // the response_sink field on ServiceInvocation carries both sinks, which flow into
    // InFlightInvocationMetadata.response_sinks via from_service_invocation / on_link_from_invocation.
    {
        let mut child_status = env
            .storage()
            .get_invocation_status(&child_inv_id)
            .await
            .unwrap();
        if let Some(sinks) = child_status.get_response_sinks_mut() {
            sinks.insert(ServiceInvocationResponseSink::ServiceCompletion(
                ServiceCompletionTarget {
                    service_id: parent_service_id.clone(),
                    handler_name: handler_name.clone(),
                    completion_retention_duration: Duration::ZERO,
                    journal_retention_duration: Duration::ZERO,
                },
            ));
            // InvocationLinkNotification triggers graph-only LinkCompletionNotification
            // when the child WI completes (via send_response_to_sinks).
            sinks.insert(ServiceInvocationResponseSink::ServiceLinkNotification {
                linked_from: parent_service_id.clone(),
            });
        }
        let mut txn = env.storage().transaction();
        txn.put_invocation_status(&child_inv_id, &child_status)
            .unwrap();
        txn.commit().await.unwrap();
    }

    // Mark child WI as having links.
    mark_invocation_has_links(&mut env, &child_inv_id).await;

    // ── Drive child workflow to success completion ─────────────────────────────
    let child_result = Bytes::from_static(b"workflow_output");
    let actions = env
        .apply_multiple([
            fixtures::invoker_entry_effect(
                child_inv_id,
                OutputCommand {
                    result: OutputResult::Success(child_result.clone()),
                    name: Default::default(),
                },
            ),
            fixtures::invoker_end_effect(child_inv_id),
        ])
        .await;

    // Task 5.2: end_invocation dispatches ServiceCompletion sink → outbox ServiceInvocation
    // for the handler, AND scans InvocationEdges LinkedFrom → emits graph-only notification.

    // Handler ServiceInvocation enqueued via fire_service_completion.
    let has_handler_outbox = actions.iter().any(|a| match a {
        Action::NewOutboxMessage {
            message: restate_storage_api::outbox_table::OutboxMessage::ServiceInvocation(si),
            ..
        } => {
            si.invocation_target.handler_name() == &handler_name
                && si.invocation_target.service_name() == &parent_service_id.service_name
        }
        _ => false,
    });
    assert!(
        has_handler_outbox,
        "expected onCompleted handler ServiceInvocation in outbox for handler '{handler_name}' on parent '{}', got: {actions:?}",
        parent_service_id.service_name,
    );

    let lcn = extract_outbox_link_completion_notification(&actions);
    assert_that!(lcn.linked_from, eq(parent_node_id.clone()));
    assert_that!(lcn.linked_to, eq(child_entity_id.clone()));

    // ── Task 2.1: parent processes LinkCompletionNotification (Service arm) ────
    // Graph-only: just transitions the LinkedTo edge. Handler already fired above.
    env.apply(Command::LinkCompletionNotification(lcn)).await;

    // Parent's ServiceEdges LinkedTo transitions Active → Completed.
    assert_that!(
        env.storage()
            .get_service_edge(&parent_service_id, EdgeLabel::LinkedTo, &child_entity_id)
            .await
            .unwrap(),
        some(eq(EdgeState::LinkedTo(LinkStatus::Completed)))
    );

    env.shutdown().await;
}

/// VO→WI link completion — child workflow fails → onCompleted handler fires with failure bytes.
#[restate_core::test]
async fn vo_parent_wi_child_link_failure_outcome() {
    let mut env = TestEnv::create().await;

    let handler_name: ByteString = "onChildDone".into();

    let parent_service_id = ServiceId::mock_random();
    let parent_node_id = EntityId::Object(parent_service_id.clone());

    // Child workflow with a running invocation.
    let child_target = InvocationTarget::mock_workflow();
    let child_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, child_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, child_inv_id).await;

    let child_entity_id = EntityId::WorkflowInvocation(child_inv_id);

    // Seed edges (same pattern as success test).
    {
        let mut txn = env.storage().transaction();
        txn.put_service_edge(
            &parent_service_id,
            &child_entity_id,
            &EdgeState::LinkedTo(LinkStatus::Active),
        )
        .unwrap();
        // LinkedFrom edges are no longer written — linked_from_count tracks the link.
        txn.commit().await.unwrap();
    }

    // Add ServiceCompletion and ServiceLinkNotification sinks to child WI's response_sinks.
    // Same pattern as success test — mirrors what StartLinkedCommand (VO parent) would do.
    {
        let mut child_status = env
            .storage()
            .get_invocation_status(&child_inv_id)
            .await
            .unwrap();
        if let Some(sinks) = child_status.get_response_sinks_mut() {
            sinks.insert(ServiceInvocationResponseSink::ServiceCompletion(
                ServiceCompletionTarget {
                    service_id: parent_service_id.clone(),
                    handler_name: handler_name.clone(),
                    completion_retention_duration: Duration::ZERO,
                    journal_retention_duration: Duration::ZERO,
                },
            ));
            sinks.insert(ServiceInvocationResponseSink::ServiceLinkNotification {
                linked_from: parent_service_id.clone(),
            });
        }
        let mut txn = env.storage().transaction();
        txn.put_invocation_status(&child_inv_id, &child_status)
            .unwrap();
        txn.commit().await.unwrap();
    }

    // Mark child WI as having links.
    mark_invocation_has_links(&mut env, &child_inv_id).await;

    // ── Drive child workflow to failure completion ─────────────────────────────
    let failure_err = restate_types::errors::InvocationError::new(500u16, "something went wrong");
    let actions = env
        .apply_multiple([
            fixtures::invoker_entry_effect(
                child_inv_id,
                OutputCommand {
                    result: OutputResult::Failure(failure_err.clone().into()),
                    name: Default::default(),
                },
            ),
            fixtures::invoker_end_effect(child_inv_id),
        ])
        .await;

    // end_invocation dispatches ServiceCompletion sink (handler) AND emits LinkCompletionNotification.
    // Handler fires via fire_service_completion → OutboxMessage::ServiceInvocation.
    let has_handler_outbox = actions.iter().any(|a| match a {
        Action::NewOutboxMessage {
            message: restate_storage_api::outbox_table::OutboxMessage::ServiceInvocation(si),
            ..
        } => {
            si.invocation_target.handler_name() == &handler_name
                && si.invocation_target.service_name() == &parent_service_id.service_name
        }
        _ => false,
    });
    assert!(
        has_handler_outbox,
        "expected onCompleted handler ServiceInvocation in outbox for handler '{handler_name}' on parent '{}', got: {actions:?}",
        parent_service_id.service_name,
    );

    // LinkCompletionNotification carries graph-only info (no result or sink in the message).
    let lcn = extract_outbox_link_completion_notification(&actions);
    assert_that!(lcn.linked_from, eq(parent_node_id.clone()));
    assert_that!(lcn.linked_to, eq(child_entity_id.clone()));

    // ── Apply LinkCompletionNotification on parent ─────────────────────────────
    // Graph-only: just transitions the LinkedTo edge. Handler already fired above.
    env.apply(Command::LinkCompletionNotification(lcn)).await;

    // Parent's ServiceEdges LinkedTo transitions Active → Completed even on failure.
    assert_that!(
        env.storage()
            .get_service_edge(&parent_service_id, EdgeLabel::LinkedTo, &child_entity_id)
            .await
            .unwrap(),
        some(eq(EdgeState::LinkedTo(LinkStatus::Completed)))
    );

    env.shutdown().await;
}

// ─────────────────────────────────────────────────────────────────────────────
// Task 7.3 — InvocationStatus::Completing lifecycle integration tests
// ─────────────────────────────────────────────────────────────────────────────
//
// These tests exercise the full Completing lifecycle:
//   - Task 5.2: `end_invocation` detects active LinkedTo children and stores Completing
//   - Task 5.3: `on_link_completion_notification` Invocation arm updates edges and calls
//     `resume_completing_invocation` once all children have completed
//
// Both tests use manual edge seeding to bypass the establishment protocol
// and focus directly on the Completing state transitions.
//
// In the new model, `EdgeState::LinkedFrom` on child's ServiceEdges is presence-only.
// The WI parent information is derived from the LinkedFrom edge key (EntityId), not the value.

/// Single-child completing path: workflow run returns with one active linked child →
/// stores Completing → child completes → LinkCompletionNotification (graph-only) →
/// parent finalizes.
///
/// Steps:
/// 1. Seed InvocationEdges: parent LinkedTo(Active) → child, child LinkedFrom (presence-only) → parent
/// 2. Drive parent workflow run to return (Output + End) → Completing stored
/// 3. Assert Completing state: invocation status is Completing, edges still Active
/// 4. Child VO completes → LinkCompletionNotification emitted (graph-only)
/// 5. Apply LinkCompletionNotification → edge Completed, parent finalizes (status Free)
#[restate_core::test]
async fn workflow_completing_with_single_child() {
    let mut env = TestEnv::create().await;

    // ── Step 1: set up WI parent ──────────────────────────────────────────────
    let parent_target = InvocationTarget::mock_workflow();
    let parent_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, parent_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, parent_inv_id).await;

    // Set up child VO with a running invocation.
    let child_target = InvocationTarget::mock_virtual_object();
    let child_service_id = child_target.as_keyed_service_id().unwrap();
    let child_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, child_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, child_inv_id).await;

    let child_entity_id = EntityId::Object(child_service_id.clone());
    let parent_entity_id = EntityId::WorkflowInvocation(parent_inv_id);

    // Seed InvocationEdges: parent LinkedTo(Active) → child
    {
        let mut txn = env.storage().transaction();
        txn.put_invocation_edge(
            &parent_inv_id,
            &child_entity_id,
            &EdgeState::LinkedTo(LinkStatus::Active),
        )
        .unwrap();
        txn.commit().await.unwrap();
    }

    // Add InvocationLinkNotification sink to child VO's response_sinks so that when the child
    // completes, a LinkCompletionNotification is dispatched to the WI parent.
    {
        let mut child_status = env
            .storage()
            .get_virtual_object_status(&child_service_id)
            .await
            .unwrap();
        if let Some(sinks) = child_status.response_sinks_mut() {
            sinks.insert(ServiceInvocationResponseSink::InvocationLinkNotification {
                linked_from: parent_inv_id,
            });
        }
        let mut txn = env.storage().transaction();
        txn.put_virtual_object_status(&child_service_id, &child_status)
            .unwrap();
        txn.commit().await.unwrap();
    }

    // Mark parent WI as having links.
    mark_invocation_has_links(&mut env, &parent_inv_id).await;

    // ── Step 2: drive parent workflow run to return ───────────────────────────
    // OutputCommand + End triggers end_invocation. Because parent has a LinkedTo(Active) child,
    // end_invocation should store InvocationStatus::Completing and return early.
    let parent_result = Bytes::from_static(b"workflow_done");
    env.apply_multiple([
        fixtures::invoker_entry_effect(
            parent_inv_id,
            OutputCommand {
                result: OutputResult::Success(parent_result.clone()),
                name: Default::default(),
            },
        ),
        fixtures::invoker_end_effect(parent_inv_id),
    ])
    .await;

    // ── Step 3: assert Completing state ──────────────────────────────────────
    // Parent invocation status must be Completing (not Free, not Completed).
    assert_that!(
        env.storage()
            .get_invocation_status(&parent_inv_id)
            .await
            .unwrap(),
        pat!(InvocationStatus::Completing(_))
    );

    // Parent's InvocationEdges LinkedTo for child must still be Active.
    assert_that!(
        env.storage()
            .get_invocation_edge(&parent_inv_id, EdgeLabel::LinkedTo, &child_entity_id)
            .await
            .unwrap(),
        some(eq(EdgeState::LinkedTo(LinkStatus::Active)))
    );

    // ── Step 4: child VO completes → LinkCompletionNotification emitted ───────
    let child_result = Bytes::from_static(b"child_output");
    let actions = env
        .apply(fixtures::invoker_entry_effect(
            child_inv_id,
            CompleteServiceCommand {
                result: ResponseResult::Success(child_result.clone()),
                completion_id: 1,
                name: Default::default(),
            },
        ))
        .await;

    // Graph-only LinkCompletionNotification (local=parent, remote=child) must be enqueued.
    let lcn = extract_outbox_link_completion_notification(&actions);
    assert_that!(lcn.linked_from, eq(parent_entity_id.clone()));
    assert_that!(lcn.linked_to, eq(child_entity_id.clone()));

    // ── Step 5: apply LinkCompletionNotification → parent finalizes ───────────
    env.apply(Command::LinkCompletionNotification(lcn)).await;

    // Parent invocation must be finalized (Free since retention is zero in tests).
    assert_that!(
        env.storage()
            .get_invocation_status(&parent_inv_id)
            .await
            .unwrap(),
        eq(InvocationStatus::Free)
    );

    // InvocationEdges for parent are cleaned up by resume_completing_invocation.
    assert_that!(
        env.storage()
            .get_invocation_edge(&parent_inv_id, EdgeLabel::LinkedTo, &child_entity_id)
            .await
            .unwrap(),
        none()
    );

    env.shutdown().await;
}

/// Multi-child completing path: workflow run returns with two active children →
/// stores Completing → first child completes → still Completing → second completes → finalizes.
///
/// Steps:
/// 1. Seed two InvocationEdges LinkedTo(Active) and two child LinkedFrom edges (presence-only)
/// 2. Drive parent workflow run → Completing stored
/// 3. First child completes → LinkCompletionNotification → edge Active → Completed,
///    but second child still active → parent stays Completing
/// 4. Assert mid-state: parent still Completing, child1 edge Completed, child2 edge Active
/// 5. Second child completes → LinkCompletionNotification → all Completed → parent finalizes
#[restate_core::test]
async fn workflow_completing_with_multiple_children() {
    let mut env = TestEnv::create().await;

    // ── Step 1: set up WI parent ──────────────────────────────────────────────
    let parent_target = InvocationTarget::mock_workflow();
    let parent_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, parent_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, parent_inv_id).await;

    // Set up child 1 VO.
    let child1_target = InvocationTarget::mock_virtual_object();
    let child1_service_id = child1_target.as_keyed_service_id().unwrap();
    let child1_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, child1_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, child1_inv_id).await;

    // Set up child 2 VO.
    let child2_target = InvocationTarget::mock_virtual_object();
    let child2_service_id = child2_target.as_keyed_service_id().unwrap();
    let child2_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, child2_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, child2_inv_id).await;

    let child1_entity_id = EntityId::Object(child1_service_id.clone());
    let child2_entity_id = EntityId::Object(child2_service_id.clone());

    // Seed InvocationEdges: parent LinkedTo(Active) → child1 and child2
    {
        let mut txn = env.storage().transaction();
        txn.put_invocation_edge(
            &parent_inv_id,
            &child1_entity_id,
            &EdgeState::LinkedTo(LinkStatus::Active),
        )
        .unwrap();
        txn.put_invocation_edge(
            &parent_inv_id,
            &child2_entity_id,
            &EdgeState::LinkedTo(LinkStatus::Active),
        )
        .unwrap();
        txn.commit().await.unwrap();
    }

    // Add InvocationLinkNotification sinks to both child VOs so that when each completes,
    // a LinkCompletionNotification is dispatched to the WI parent.
    for child_service_id in [&child1_service_id, &child2_service_id] {
        let mut child_status = env
            .storage()
            .get_virtual_object_status(child_service_id)
            .await
            .unwrap();
        if let Some(sinks) = child_status.response_sinks_mut() {
            sinks.insert(ServiceInvocationResponseSink::InvocationLinkNotification {
                linked_from: parent_inv_id,
            });
        }
        let mut txn = env.storage().transaction();
        txn.put_virtual_object_status(child_service_id, &child_status)
            .unwrap();
        txn.commit().await.unwrap();
    }

    // Mark parent WI as having links (linked_from_count == 2 — two active children).
    mark_invocation_has_links(&mut env, &parent_inv_id).await;
    mark_invocation_has_links(&mut env, &parent_inv_id).await;

    // ── Step 2: drive parent workflow run to return → Completing stored ────────
    let parent_result = Bytes::from_static(b"workflow_done");
    env.apply_multiple([
        fixtures::invoker_entry_effect(
            parent_inv_id,
            OutputCommand {
                result: OutputResult::Success(parent_result.clone()),
                name: Default::default(),
            },
        ),
        fixtures::invoker_end_effect(parent_inv_id),
    ])
    .await;

    // Parent must be Completing (two active children).
    assert_that!(
        env.storage()
            .get_invocation_status(&parent_inv_id)
            .await
            .unwrap(),
        pat!(InvocationStatus::Completing(_))
    );

    // ── Step 3: child1 completes → LinkCompletionNotification emitted ─────────
    let child1_result = Bytes::from_static(b"child1_output");
    let actions = env
        .apply(fixtures::invoker_entry_effect(
            child1_inv_id,
            CompleteServiceCommand {
                result: ResponseResult::Success(child1_result.clone()),
                completion_id: 1,
                name: Default::default(),
            },
        ))
        .await;

    let lcn1 = extract_outbox_link_completion_notification(&actions);

    // Apply child1's notification — parent should still be Completing (child2 still active).
    env.apply(Command::LinkCompletionNotification(lcn1)).await;

    // ── Step 4: assert mid-state ──────────────────────────────────────────────
    // Parent must STILL be Completing.
    assert_that!(
        env.storage()
            .get_invocation_status(&parent_inv_id)
            .await
            .unwrap(),
        pat!(InvocationStatus::Completing(_))
    );

    // Child1 edge must now be Completed.
    assert_that!(
        env.storage()
            .get_invocation_edge(&parent_inv_id, EdgeLabel::LinkedTo, &child1_entity_id)
            .await
            .unwrap(),
        some(eq(EdgeState::LinkedTo(LinkStatus::Completed)))
    );

    // Child2 edge must still be Active.
    assert_that!(
        env.storage()
            .get_invocation_edge(&parent_inv_id, EdgeLabel::LinkedTo, &child2_entity_id)
            .await
            .unwrap(),
        some(eq(EdgeState::LinkedTo(LinkStatus::Active)))
    );

    // ── Step 5: child2 completes → parent finalizes ───────────────────────────
    let child2_result = Bytes::from_static(b"child2_output");
    let actions = env
        .apply(fixtures::invoker_entry_effect(
            child2_inv_id,
            CompleteServiceCommand {
                result: ResponseResult::Success(child2_result.clone()),
                completion_id: 1,
                name: Default::default(),
            },
        ))
        .await;

    let lcn2 = extract_outbox_link_completion_notification(&actions);

    // Apply child2's notification — all children now completed → parent finalizes.
    env.apply(Command::LinkCompletionNotification(lcn2)).await;

    // Parent invocation must be finalized (Free since retention is zero in tests).
    assert_that!(
        env.storage()
            .get_invocation_status(&parent_inv_id)
            .await
            .unwrap(),
        eq(InvocationStatus::Free)
    );

    // All InvocationEdges for parent are cleaned up by resume_completing_invocation.
    assert_that!(
        env.storage()
            .get_invocation_edge(&parent_inv_id, EdgeLabel::LinkedTo, &child1_entity_id)
            .await
            .unwrap(),
        none()
    );
    assert_that!(
        env.storage()
            .get_invocation_edge(&parent_inv_id, EdgeLabel::LinkedTo, &child2_entity_id)
            .await
            .unwrap(),
        none()
    );

    env.shutdown().await;
}

// ─────────────────────────────────────────────────────────────────────────────
// WI → VO linking via LinkServiceCommand
// ─────────────────────────────────────────────────────────────────────────────

/// Happy path: WI parent issues LinkServiceCommand targeting a VO child.
///
/// In the new model:
/// - WI parent uses InvocationEdges for its LinkedTo edge.
/// - LinkRequest is enqueued with `parent = WorkflowInvocation(parent_inv_id)`.
/// - Child VO gets notification sinks + linked_from_count incremented
///   (handler_sink = None for WI parent without handler).
/// - LinkResponse(Ok) delivered → SDK completion forwarded.
///
/// Steps:
/// 1. WI parent applies LinkServiceCommand → InvocationEdges LinkedTo(Active) written,
///    LinkRequest enqueued with parent = WorkflowInvocation(parent_inv_id)
/// 2. Child processes LinkRequest → notification sinks + linked_from_count written,
///    LinkResponse(Ok) enqueued
/// 3. Parent processes LinkResponse(Ok) → LinkServiceCompletion::Void forwarded to SDK
#[restate_core::test]
async fn wi_parent_vo_child_link_via_link_service_command() {
    let mut env = TestEnv::create().await;

    // Set up WI parent with a running invocation.
    let parent_target = InvocationTarget::mock_workflow();
    let parent_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, parent_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, parent_inv_id).await;

    let parent_entity_id = EntityId::WorkflowInvocation(parent_inv_id);

    // Child is a VO (keyed service).
    let child_service_id = ServiceId::mock_random();
    let child_entity_id = EntityId::Object(child_service_id.clone());
    let link_completion_id: u32 = 1;

    // ── Step 1: WI parent applies LinkServiceCommand ──────────────────────────
    let actions = env
        .apply(fixtures::invoker_entry_effect(
            parent_inv_id,
            LinkServiceCommand {
                link_to: child_service_id.clone(),
                // WI parent: result_completion_handler is ignored by the server
                result_completion_handler: None,
                link_completion_id,
                name: Default::default(),
            },
        ))
        .await;

    // Verify InvocationEdges LinkedTo(Active) written for parent → child.
    assert_that!(
        env.storage()
            .get_invocation_edge(&parent_inv_id, EdgeLabel::LinkedTo, &child_entity_id)
            .await
            .unwrap(),
        some(eq(EdgeState::LinkedTo(LinkStatus::Active)))
    );

    // No ServiceEdges for the parent (WI parents use InvocationEdges).
    assert_that!(
        env.storage()
            .get_service_edge(
                &parent_target.as_keyed_service_id().unwrap(),
                EdgeLabel::LinkedTo,
                &child_entity_id
            )
            .await
            .unwrap(),
        none()
    );

    // Verify LinkRequest enqueued with parent = WorkflowInvocation.
    assert_that!(
        actions,
        contains(pat!(Action::NewOutboxMessage {
            message: pat!(
                restate_storage_api::outbox_table::OutboxMessage::LinkRequest(pat!(LinkRequest {
                    link_to: eq(child_entity_id.clone()),
                    link_from: eq(parent_entity_id.clone()),
                    caller_invocation_id: eq(parent_inv_id),
                    caller_completion_id: eq(link_completion_id),
                    handler_sink: eq(None),
                }))
            )
        }))
    );

    // ── Step 2: child partition processes LinkRequest ──────────────────
    let actions = env
        .apply(Command::LinkRequest(LinkRequest {
            link_to: child_entity_id.clone(),
            link_from: parent_entity_id.clone(),
            caller_invocation_id: parent_inv_id,
            caller_completion_id: link_completion_id,
            handler_sink: None,
        }))
        .await;

    // An InvocationLinkNotification sink is added for the WI parent (graph-only, no handler).
    // No ServiceCompletion sink (no handler_sink).
    {
        let sinks = env
            .storage()
            .get_virtual_object_status(&child_service_id)
            .await
            .unwrap();
        let sinks = sinks.response_sinks().expect("child must have sinks");
        assert!(
            sinks.iter().any(|s| matches!(
                s,
                ServiceInvocationResponseSink::InvocationLinkNotification {
                    linked_from: id
                } if *id == parent_inv_id
            )),
            "expected InvocationLinkNotification sink for WI parent"
        );
        assert!(
            !sinks
                .iter()
                .any(|s| matches!(s, ServiceInvocationResponseSink::ServiceCompletion(_))),
            "no ServiceCompletion sink expected without handler"
        );
    }

    // Verify LinkResponse(Ok) enqueued with correct parent entity.
    assert_that!(
        actions,
        contains(pat!(Action::NewOutboxMessage {
            message: pat!(
                restate_storage_api::outbox_table::OutboxMessage::LinkResponse(pat!(
                    LinkResponse {
                        linked_from: eq(parent_entity_id.clone()),
                        linked_to: eq(child_entity_id.clone()),
                        completion_id: eq(link_completion_id),
                        result: ok(eq(())),
                    }
                ))
            )
        }))
    );

    // ── Step 3: parent processes LinkResponse(Ok) ─────────────────────────────
    let actions = env
        .apply(Command::LinkResponse(LinkResponse {
            linked_from: parent_entity_id.clone(),
            linked_to: child_entity_id.clone(),
            caller_invocation_id: parent_inv_id,
            completion_id: link_completion_id,
            result: Ok(()),
        }))
        .await;

    // Verify SDK completion forwarded (LinkServiceCompletion::Void) at link_completion_id.
    assert_that!(
        actions,
        contains(forward_completion_notification(
            parent_inv_id,
            link_completion_id
        ))
    );

    // LinkedTo edge remains Active (link established; child hasn't completed yet).
    assert_that!(
        env.storage()
            .get_invocation_edge(&parent_inv_id, EdgeLabel::LinkedTo, &child_entity_id)
            .await
            .unwrap(),
        some(eq(EdgeState::LinkedTo(LinkStatus::Active)))
    );

    env.shutdown().await;
}

/// VO→WI on_link_request: link already-completed child triggers immediate handler dispatch.
///
/// Tests the short-circuit in `on_link_request`: if the child VO is already Completed,
/// fire the handler immediately and emit a `LinkCompletionNotification` to transition
/// the parent's LinkedTo edge.
#[restate_core::test]
async fn link_service_command_on_completed_target_fires_handler_immediately() {
    let mut env = TestEnv::create().await;

    // Set up parent VO with a running invocation.
    let parent_target = InvocationTarget::mock_virtual_object();
    let parent_service_id = parent_target.as_keyed_service_id().unwrap();
    let parent_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, parent_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, parent_inv_id).await;

    let parent_node_id = EntityId::Object(parent_service_id.clone());
    let handler_name: ByteString = "onDone".into();

    // Pre-populate child VO as already completed.
    let child_service_id = ServiceId::mock_random();
    let child_node_id = EntityId::Object(child_service_id.clone());
    let child_result = ResponseResult::Success(Bytes::from_static(b"already_done"));
    write_completed(&mut env, &child_service_id, child_result.clone()).await;

    // Pre-populate parent's LinkedTo(Active) edge (set by LinkServiceCommand handler).
    {
        let mut txn = env.storage().transaction();
        txn.put_service_edge(
            &parent_service_id,
            &child_node_id,
            &EdgeState::LinkedTo(LinkStatus::Active),
        )
        .unwrap();
        txn.commit().await.unwrap();
    }

    let completion_id: u32 = 1;

    // ── Process LinkRequest targeting already-completed child ──────────────────
    let actions = env
        .apply(Command::LinkRequest(LinkRequest {
            link_to: child_node_id.clone(),
            link_from: parent_node_id.clone(),
            caller_invocation_id: parent_inv_id,
            caller_completion_id: completion_id,
            handler_sink: Some(ServiceCompletionTarget {
                service_id: parent_service_id.clone(),
                handler_name: handler_name.clone(),
                completion_retention_duration: Duration::ZERO,
                journal_retention_duration: Duration::ZERO,
            }),
        }))
        .await;

    // LinkResponse(Ok) enqueued (link accepted despite immediate completion).
    assert_that!(
        actions,
        contains(pat!(Action::NewOutboxMessage {
            message: pat!(
                restate_storage_api::outbox_table::OutboxMessage::LinkResponse(pat!(
                    LinkResponse {
                        linked_from: eq(parent_node_id.clone()),
                        linked_to: eq(child_node_id.clone()),
                        result: ok(eq(())),
                    }
                ))
            )
        }))
    );

    // onCompleted handler fired immediately via fire_service_completion → OutboxMessage::ServiceInvocation.
    let has_handler_outbox = actions.iter().any(|a| match a {
        Action::NewOutboxMessage {
            message: restate_storage_api::outbox_table::OutboxMessage::ServiceInvocation(si),
            ..
        } => {
            si.invocation_target.handler_name() == &handler_name
                && si.invocation_target.service_name() == &parent_service_id.service_name
        }
        _ => false,
    });
    assert!(
        has_handler_outbox,
        "expected immediate handler '{handler_name}' ServiceInvocation in outbox, got: {actions:?}"
    );

    // LinkCompletionNotification emitted (graph-only) to transition parent's LinkedTo edge.
    assert_that!(
        actions,
        contains(pat!(Action::NewOutboxMessage {
            message: pat!(
                restate_storage_api::outbox_table::OutboxMessage::LinkCompletionNotification(pat!(
                    LinkCompletionNotification {
                        linked_from: eq(parent_node_id.clone()),
                        linked_to: eq(child_node_id.clone()),
                    }
                ))
            )
        }))
    );

    env.shutdown().await;
}

/// VO unlink cleans up ServiceCompletion sink from child's response_sinks.
///
/// When a parent unlinks from a child, `on_unlink_request` must remove the matching
/// `ServiceCompletion` sink from the child's `response_sinks`. This prevents stale
/// handler invocations from firing after the parent has detached.
#[restate_core::test]
async fn vo_unlink_cleans_up_service_completion_sink() {
    let mut env = TestEnv::create().await;

    // Set up parent VO.
    let parent_target = InvocationTarget::mock_virtual_object();
    let parent_service_id = parent_target.as_keyed_service_id().unwrap();
    let parent_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, parent_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, parent_inv_id).await;

    let parent_node_id = EntityId::Object(parent_service_id.clone());
    let handler_name: ByteString = "onDone".into();

    // Set up child VO (active, not completed).
    let child_target = InvocationTarget::mock_virtual_object();
    let child_service_id = child_target.as_keyed_service_id().unwrap();
    let child_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, child_target.clone())
            .await;

    let child_node_id = EntityId::Object(child_service_id.clone());

    // Seed the link with handler.
    write_link_with_handler(
        &mut env,
        &parent_service_id,
        &child_service_id,
        handler_name.clone(),
    )
    .await;

    // Verify sink is present before unlink.
    assert_that!(
        env.storage()
            .get_virtual_object_status(&child_service_id)
            .await
            .unwrap()
            .response_sinks()
            .map(|s| !s.is_empty()),
        some(eq(true))
    );

    let unlink_completion_id: u32 = 1;

    // Parent applies UnlinkServiceCommand.
    env.apply(fixtures::invoker_entry_effect(
        parent_inv_id,
        UnlinkServiceCommand {
            unlink_from: child_service_id.clone(),
            unlink_completion_id,
            name: Default::default(),
        },
    ))
    .await;

    // Child processes UnlinkRequest.
    env.apply(Command::UnlinkRequest(UnlinkRequest {
        linked_to: child_node_id.clone(),
        linked_from: parent_node_id.clone(),
        caller_invocation_id: parent_inv_id,
        caller_completion_id: Some(unlink_completion_id),
    }))
    .await;

    // ServiceCompletion sink removed from child's response_sinks.
    assert_that!(
        env.storage()
            .get_virtual_object_status(&child_service_id)
            .await
            .unwrap()
            .response_sinks()
            .map(|s| s.is_empty()),
        some(eq(true))
    );

    let _ = child_inv_id;
    env.shutdown().await;
}

/// WI unlink preserves PartitionProcessor (attach) sinks on child's response_sinks.
///
/// When a WI parent unlinks from a child, `on_unlink_request` must only remove
/// `ServiceCompletion` sinks matching the parent, NOT `PartitionProcessor` sinks
/// that were added by separate `AttachServiceCommand` calls.
#[restate_core::test]
async fn wi_unlink_preserves_attach_sink() {
    let mut env = TestEnv::create().await;

    // Set up WI parent.
    let parent_target = InvocationTarget::mock_workflow();
    let parent_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, parent_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, parent_inv_id).await;

    let parent_entity_id = EntityId::WorkflowInvocation(parent_inv_id);

    // Set up child VO (active, not completed).
    let child_target = InvocationTarget::mock_virtual_object();
    let child_service_id = child_target.as_keyed_service_id().unwrap();
    let child_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, child_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, child_inv_id).await;

    let child_entity_id = EntityId::Object(child_service_id.clone());

    // Add an InvocationLinkNotification sink (simulating what LinkRequest would add for WI parent)
    // and a PartitionProcessor sink (simulating an AttachServiceCommand from elsewhere).
    let attach_completion_id: u32 = 99;
    let attach_inv_id = InvocationId::mock_random();
    let notification_sink = ServiceInvocationResponseSink::InvocationLinkNotification {
        linked_from: parent_inv_id,
    };
    let mut child_status = env
        .storage()
        .get_virtual_object_status(&child_service_id)
        .await
        .unwrap();
    let pp_sink = ServiceInvocationResponseSink::PartitionProcessor(
        restate_types::invocation::JournalCompletionTarget {
            caller_id: attach_inv_id,
            caller_completion_id: attach_completion_id,
        },
    );
    let sinks = child_status.response_sinks_mut().unwrap();
    sinks.insert(pp_sink.clone());
    sinks.insert(notification_sink.clone());
    *child_status.linked_from_count_mut() = 1;
    {
        let mut txn = env.storage().transaction();
        txn.put_virtual_object_status(&child_service_id, &child_status)
            .unwrap();
        txn.commit().await.unwrap();
    }

    // Verify pre-unlink: both sinks present, linked_from_count=1.
    let pre_status = env
        .storage()
        .get_virtual_object_status(&child_service_id)
        .await
        .unwrap();
    assert_eq!(pre_status.linked_from_count(), 1);
    assert_eq!(pre_status.response_sinks().unwrap().len(), 2);

    // Process UnlinkRequest from WI parent (no handler_sink, fire-and-forget).
    env.apply(Command::UnlinkRequest(UnlinkRequest {
        linked_to: child_entity_id.clone(),
        linked_from: parent_entity_id.clone(),
        caller_invocation_id: parent_inv_id,
        caller_completion_id: None,
    }))
    .await;

    // M-3: After WI parent unlink, verify:
    // 1. PartitionProcessor sink preserved (not removed by unlink)
    // 2. InvocationLinkNotification sink for this WI parent removed
    // 3. linked_from_count decremented to 0
    let post_status = env
        .storage()
        .get_virtual_object_status(&child_service_id)
        .await
        .unwrap();
    let remaining_sinks = post_status.response_sinks().unwrap();
    assert!(
        remaining_sinks.contains(&pp_sink),
        "PartitionProcessor sink must be preserved after WI parent unlink"
    );
    assert!(
        !remaining_sinks.contains(&notification_sink),
        "InvocationLinkNotification sink for the unlinking WI parent must be removed"
    );
    assert_eq!(
        remaining_sinks.len(),
        1,
        "only the PartitionProcessor sink should remain"
    );
    assert_eq!(
        post_status.linked_from_count(),
        0,
        "linked_from_count must be decremented to 0 after WI parent unlinks"
    );

    let _ = child_inv_id;
    env.shutdown().await;
}

// ─────────────────────────────────────────────────────────────────────────────
// CRITICAL-1 regression: dedup + link_from — StartLinkedCommand targeting an
// idempotent child that was already created must still establish the link.
// ─────────────────────────────────────────────────────────────────────────────

/// When a `StartLinkedCommand` targets a child workflow whose invocation already exists
/// (same deterministic invocation id → deduplication), `on_link_from_deduplicated_invocation`
/// must still write the `LinkedFrom` edge and return `LinkResponse(Ok)` to the parent.
/// Without the fix, the parent hangs forever with no error logged.
///
/// Steps:
/// 1. Parent A issues `StartLinkedCommand` → child workflow created.
/// 2. Parent B issues `StartLinkedCommand` targeting the SAME child invocation id.
///    Child partition deduplicates the second Invoke but must still establish the link.
/// 3. Assertions: `LinkedFrom(B)` edge exists, `LinkResponse(Ok)` for B enqueued.
#[restate_core::test]
async fn start_linked_command_on_idempotent_dedup_target_still_establishes_link() {
    let mut env = TestEnv::create().await;

    // ── Parent A — already has a running linked child ─────────────────────────
    let parent_a_target = InvocationTarget::mock_workflow();
    let parent_a_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, parent_a_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, parent_a_inv_id).await;

    let child_target = InvocationTarget::mock_workflow();
    let child_inv_id = InvocationId::mock_generate(&child_target);
    let child_entity_id = EntityId::WorkflowInvocation(child_inv_id);
    let completion_id_a: u32 = 1;

    let start_linked_a = StartLinkedCommand {
        request: restate_types::journal_v2::command::CallRequest::mock(
            child_inv_id,
            child_target.clone(),
        ),
        result_completion_handler: None,
        link_completion_id: completion_id_a,
        name: Default::default(),
    };
    let actions = env
        .apply(fixtures::invoker_entry_effect(
            parent_a_inv_id,
            start_linked_a,
        ))
        .await;

    let service_invocation_a = extract_outbox_service_invocation(&actions);

    // Child partition processes parent A's invoke → child created.
    env.apply(Command::Invoke(service_invocation_a)).await;

    // Sanity: child exists and LinkedFrom(A) edge is set.
    assert_that!(
        env.storage()
            .get_invocation_status(&child_inv_id)
            .await
            .unwrap(),
        not(eq(InvocationStatus::Free))
    );

    // ── Parent B — issues StartLinkedCommand for the same child ──────────────
    let parent_b_target = InvocationTarget::mock_workflow();
    let parent_b_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, parent_b_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, parent_b_inv_id).await;
    let parent_b_entity_id = EntityId::WorkflowInvocation(parent_b_inv_id);
    let completion_id_b: u32 = 2;

    let start_linked_b = StartLinkedCommand {
        request: restate_types::journal_v2::command::CallRequest::mock(
            child_inv_id,
            child_target.clone(),
        ),
        result_completion_handler: None,
        link_completion_id: completion_id_b,
        name: Default::default(),
    };
    let actions = env
        .apply(fixtures::invoker_entry_effect(
            parent_b_inv_id,
            start_linked_b,
        ))
        .await;

    let service_invocation_b = extract_outbox_service_invocation(&actions);

    assert_that!(service_invocation_b.invocation_id, eq(child_inv_id));
    assert_that!(
        service_invocation_b.link_from,
        some(eq(parent_b_entity_id.clone()))
    );

    // ── Child partition processes parent B's invoke → dedup + link establishment ──
    let actions = env.apply(Command::Invoke(service_invocation_b)).await;

    // Child status unchanged (dedup, not re-created).
    assert_that!(
        env.storage()
            .get_invocation_status(&child_inv_id)
            .await
            .unwrap(),
        not(eq(InvocationStatus::Free))
    );

    // LinkResponse(Ok) enqueued for parent B.
    let link_response = extract_outbox_link_response(&actions);
    assert_that!(link_response.result, ok(eq(())));
    assert_that!(link_response.linked_from, eq(parent_b_entity_id.clone()));
    assert_that!(link_response.linked_to, eq(child_entity_id.clone()));
    assert_that!(link_response.caller_invocation_id, eq(parent_b_inv_id));
    assert_that!(link_response.completion_id, eq(completion_id_b));

    // Parent B processes LinkResponse(Ok) → journal completion forwarded.
    let actions = env.apply(Command::LinkResponse(link_response)).await;
    assert_that!(
        actions,
        contains(forward_completion_notification(
            parent_b_inv_id,
            completion_id_b
        ))
    );

    env.shutdown().await;
}

// ─────────────────────────────────────────────────────────────────────────────
// CRITICAL-2 regression: fire_service_completion failure bytes are valid JSON
// even when the error message contains special characters.
// ─────────────────────────────────────────────────────────────────────────────

/// `fire_service_completion` must produce valid JSON for failure payloads even when the
/// error message contains `"` (quote), `\` (backslash), `\n` (newline), and non-ASCII chars.
///
/// Uses `CompleteServiceCommand` on a child VO with a `ServiceCompletion` sink, exercising
/// the full production path: drain sinks → `fire_service_completion` → outbox.
#[restate_core::test]
async fn fire_service_completion_failure_bytes_are_valid_json_with_special_characters() {
    let mut env = TestEnv::create().await;

    let child_target = InvocationTarget::mock_virtual_object();
    let child_service_id = child_target.as_keyed_service_id().unwrap();
    let child_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, child_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, child_inv_id).await;

    let parent_target = InvocationTarget::mock_virtual_object();
    let parent_service_id = parent_target.as_keyed_service_id().unwrap();
    let handler_name = ByteString::from("onCompleted");

    write_link_with_handler(
        &mut env,
        &parent_service_id,
        &child_service_id,
        handler_name.clone(),
    )
    .await;

    // Message with characters that break naive JSON string interpolation.
    let nasty_message = "error: \"quoted\", slash: \\value\\\nnewline\ttab\u{00e9}accent";
    let failure_err = restate_types::errors::InvocationError::new(500u16, nasty_message);
    let completion_id: u32 = 1;

    let actions = env
        .apply(fixtures::invoker_entry_effect(
            child_inv_id,
            CompleteServiceCommand {
                result: ResponseResult::Failure(failure_err),
                completion_id,
                name: Default::default(),
            },
        ))
        .await;

    // Find the handler invocation enqueued by fire_service_completion.
    let argument = actions
        .iter()
        .find_map(|a| match a {
            Action::NewOutboxMessage {
                message: restate_storage_api::outbox_table::OutboxMessage::ServiceInvocation(si),
                ..
            } if si.invocation_target.handler_name() == &handler_name => Some(si.argument.clone()),
            _ => None,
        })
        .expect("expected onCompleted handler ServiceInvocation in outbox");

    // Must be valid JSON — regression assertion.
    let parsed: serde_json::Value =
        serde_json::from_slice(&argument).expect("argument bytes are not valid JSON");

    assert_that!(
        parsed.get("error_code").and_then(|v| v.as_u64()),
        some(eq(500u64))
    );

    // Message must round-trip exactly without corruption.
    let parsed_message = parsed
        .get("message")
        .and_then(|v| v.as_str())
        .expect("missing 'message' field");
    assert_eq!(parsed_message, nasty_message);

    env.shutdown().await;
}

/// Spawned `ServiceCompletion` handler invocations inherit retention from the sink target,
/// which is populated from the linker parent's retention at `LinkServiceCommand` /
/// `StartLinkedCommand` apply time and persisted alongside the sink.
///
/// Tests two paths:
/// 1. `on_link_request` short-circuit (child VO already completed when link arrives)
/// 2. `end_invocation` (child WI completes with ServiceCompletion sink in response_sinks)
///
/// Both should produce an `OutboxMessage::ServiceInvocation` whose `completion_retention_duration`
/// and `journal_retention_duration` match the retention stored on the sink target.
#[restate_core::test]
async fn fire_service_completion_inherits_parent_retention() {
    let mut env = TestEnv::create().await;

    let parent_completion_retention = Duration::from_secs(3600); // 1 hour
    let parent_journal_retention = Duration::from_secs(1800); // 30 minutes
    let handler_name: ByteString = "onDone".into();

    // ── Path 1: on_link_request short-circuit (child VO already completed) ────

    // Set up parent VO with non-zero retention in its invocation metadata.
    let parent_target = InvocationTarget::mock_virtual_object();
    let parent_service_id = parent_target.as_keyed_service_id().unwrap();
    let parent_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, parent_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, parent_inv_id).await;

    // Set non-zero retention on parent's invocation metadata.
    {
        let mut parent_status = env
            .storage()
            .get_invocation_status(&parent_inv_id)
            .await
            .unwrap();
        if let Some(meta) = parent_status.get_invocation_metadata_mut() {
            meta.completion_retention_duration = parent_completion_retention;
            meta.journal_retention_duration = parent_journal_retention;
        }
        let mut txn = env.storage().transaction();
        txn.put_invocation_status(&parent_inv_id, &parent_status)
            .unwrap();
        txn.commit().await.unwrap();
    }

    let parent_node_id = EntityId::Object(parent_service_id.clone());

    // Set up child VO in Completed state with a success result.
    let child_service_id = ServiceId::mock_random();
    let child_node_id = EntityId::Object(child_service_id.clone());
    let child_result = ResponseResult::Success(Bytes::from_static(b"child_done"));
    {
        let mut txn = env.storage().transaction();
        txn.put_virtual_object_status(
            &child_service_id,
            &VirtualObjectStatus::Completed {
                result: child_result.clone(),
                linked_from_count: 0,
            },
        )
        .unwrap();
        txn.commit().await.unwrap();
    }

    // Process LinkRequest — child is already completed, so short-circuit path fires.
    let actions = env
        .apply(Command::LinkRequest(LinkRequest {
            link_to: child_node_id.clone(),
            link_from: parent_node_id.clone(),
            caller_invocation_id: parent_inv_id,
            caller_completion_id: 1,
            handler_sink: Some(ServiceCompletionTarget {
                service_id: parent_service_id.clone(),
                handler_name: handler_name.clone(),
                completion_retention_duration: parent_completion_retention,
                journal_retention_duration: parent_journal_retention,
            }),
        }))
        .await;

    // Find the spawned handler ServiceInvocation in outbox.
    let spawned = actions.iter().find_map(|a| match a {
        Action::NewOutboxMessage {
            message: restate_storage_api::outbox_table::OutboxMessage::ServiceInvocation(si),
            ..
        } if si.invocation_target.handler_name() == &handler_name
            && si.invocation_target.service_name() == &parent_service_id.service_name =>
        {
            Some(si.as_ref())
        }
        _ => None,
    });
    assert!(
        spawned.is_some(),
        "expected ServiceInvocation for handler '{}' in outbox from short-circuit path, got: {actions:?}",
        handler_name,
    );
    let spawned = spawned.unwrap();
    assert_eq!(
        spawned.completion_retention_duration, parent_completion_retention,
        "short-circuit path: completion_retention_duration must match parent's"
    );
    assert_eq!(
        spawned.journal_retention_duration, parent_journal_retention,
        "short-circuit path: journal_retention_duration must match parent's"
    );
    // M-4: fire_service_completion must spawn an Exclusive handler so it can mutate
    // the parent VO's state. Verify the handler invocation target is Exclusive.
    assert_eq!(
        spawned.invocation_target.invocation_target_ty(),
        InvocationTargetType::VirtualObject(VirtualObjectHandlerType::Exclusive),
        "completion handler must use Exclusive handler type to mutate parent state"
    );

    // ── Path 2: end_invocation (child WI completes with ServiceCompletion sink) ──

    let parent2_service_id = ServiceId::mock_random();

    // Set up child WI with a ServiceCompletion sink pointing to parent2.
    let child2_target = InvocationTarget::mock_workflow();
    let child2_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, child2_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, child2_inv_id).await;

    // Set non-zero retention on child2's invocation metadata and add ServiceCompletion sink.
    {
        let mut child2_status = env
            .storage()
            .get_invocation_status(&child2_inv_id)
            .await
            .unwrap();
        if let Some(meta) = child2_status.get_invocation_metadata_mut() {
            meta.completion_retention_duration = parent_completion_retention;
            meta.journal_retention_duration = parent_journal_retention;
            meta.linked_from_count += 1;
            meta.response_sinks
                .insert(ServiceInvocationResponseSink::ServiceCompletion(
                    ServiceCompletionTarget {
                        service_id: parent2_service_id.clone(),
                        handler_name: handler_name.clone(),
                        // Retention lives on the sink — inherited from the linker parent at link time.
                        completion_retention_duration: parent_completion_retention,
                        journal_retention_duration: parent_journal_retention,
                    },
                ));
        }
        let mut txn = env.storage().transaction();
        txn.put_invocation_status(&child2_inv_id, &child2_status)
            .unwrap();
        txn.commit().await.unwrap();
    }

    // Complete the child WI.
    let child2_result = Bytes::from_static(b"wi_output");
    let actions = env
        .apply_multiple([
            fixtures::invoker_entry_effect(
                child2_inv_id,
                OutputCommand {
                    result: OutputResult::Success(child2_result),
                    name: Default::default(),
                },
            ),
            fixtures::invoker_end_effect(child2_inv_id),
        ])
        .await;

    // Find the spawned handler ServiceInvocation in outbox.
    let spawned2 = actions.iter().find_map(|a| match a {
        Action::NewOutboxMessage {
            message: restate_storage_api::outbox_table::OutboxMessage::ServiceInvocation(si),
            ..
        } if si.invocation_target.handler_name() == &handler_name
            && si.invocation_target.service_name() == &parent2_service_id.service_name =>
        {
            Some(si.as_ref())
        }
        _ => None,
    });
    assert!(
        spawned2.is_some(),
        "expected ServiceInvocation for handler '{}' in outbox from end_invocation path, got: {actions:?}",
        handler_name,
    );
    let spawned2 = spawned2.unwrap();
    assert_eq!(
        spawned2.completion_retention_duration, parent_completion_retention,
        "end_invocation path: completion_retention_duration must match parent's"
    );
    assert_eq!(
        spawned2.journal_retention_duration, parent_journal_retention,
        "end_invocation path: journal_retention_duration must match parent's"
    );
    // M-4: Same Exclusive handler type check for the end_invocation path.
    assert_eq!(
        spawned2.invocation_target.invocation_target_ty(),
        InvocationTargetType::VirtualObject(VirtualObjectHandlerType::Exclusive),
        "end_invocation path: completion handler must use Exclusive handler type"
    );

    env.shutdown().await;
}

// ── Kill / Cancel of Completing invocations ──────────────────────────────────
//
// When a workflow invocation is in `Completing` state (handler returned but waiting
// for linked children), kill and cancel should force-complete it using the stored
// result — firing sinks, emitting notifications, and cleaning up edges.

async fn completing_invocation_force_completes_on_termination(flavor: TerminationFlavor) {
    let mut env = TestEnv::create().await;

    // Set up WI parent
    let parent_target = InvocationTarget::mock_workflow();
    let parent_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, parent_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, parent_inv_id).await;

    // Set up child VO with a running invocation
    let child_target = InvocationTarget::mock_virtual_object();
    let child_service_id = child_target.as_keyed_service_id().unwrap();
    let child_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, child_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, child_inv_id).await;

    let child_entity_id = EntityId::Object(child_service_id.clone());

    // Seed edges: parent LinkedTo(Active) → child
    {
        let mut txn = env.storage().transaction();
        txn.put_invocation_edge(
            &parent_inv_id,
            &child_entity_id,
            &EdgeState::LinkedTo(LinkStatus::Active),
        )
        .unwrap();
        txn.commit().await.unwrap();
    }
    mark_invocation_has_links(&mut env, &parent_inv_id).await;

    // Drive parent workflow run to return → Completing stored
    let parent_result = Bytes::from_static(b"workflow_done");
    env.apply_multiple([
        fixtures::invoker_entry_effect(
            parent_inv_id,
            OutputCommand {
                result: OutputResult::Success(parent_result.clone()),
                name: Default::default(),
            },
        ),
        fixtures::invoker_end_effect(parent_inv_id),
    ])
    .await;

    // Confirm Completing state
    assert_that!(
        env.storage()
            .get_invocation_status(&parent_inv_id)
            .await
            .unwrap(),
        pat!(InvocationStatus::Completing(_))
    );

    // Apply the termination (kill or cancel)
    env.apply(Command::TerminateInvocation(InvocationTermination {
        invocation_id: parent_inv_id,
        flavor,
        response_sink: None,
    }))
    .await;

    // Parent should now be finalized (Free or Completed, depending on retention)
    let status = env
        .storage()
        .get_invocation_status(&parent_inv_id)
        .await
        .unwrap();
    assert!(
        !matches!(status, InvocationStatus::Completing(_)),
        "expected invocation to be force-completed after {flavor:?}, got: {status:?}"
    );

    env.shutdown().await;
}

/// Kill a Completing invocation: the stored response result is used to finalize,
/// sinks fire, and the invocation transitions to Free/Completed.
#[restate_core::test]
async fn kill_completing_invocation_force_completes() {
    completing_invocation_force_completes_on_termination(TerminationFlavor::Kill).await;
}

/// Cancel a Completing invocation: same behavior as kill — force-complete with stored result.
#[restate_core::test]
async fn cancel_completing_invocation_force_completes() {
    completing_invocation_force_completes_on_termination(TerminationFlavor::Cancel).await;
}

// ─────────────────────────────────────────────────────────────────────────────
// Behavioral / end-to-end tests — no manual seeding of edges, sinks, or counts.
// These drive the full interaction through the state machine.
// ─────────────────────────────────────────────────────────────────────────────

/// VO parent links to VO child via LinkServiceCommand, child completes,
/// parent calls CompleteServiceCommand — should succeed because no active children remain.
///
/// Full flow (all through state machine, no manual seeding):
/// 1. Parent applies LinkServiceCommand → LinkedTo edge + LinkRequest
/// 2. Child processes LinkRequest → sinks + linked_from_count incremented, LinkResponse
/// 3. Parent processes LinkResponse(Ok) → journal completion
/// 4. Child completes (Output + End) → response_sinks dispatched, LinkCompletionNotification
/// 5. Parent processes LinkCompletionNotification → edge transitions Active→Completed
/// 6. Parent applies CompleteServiceCommand → should succeed (no active children)
#[restate_core::test]
async fn e2e_vo_parent_links_child_then_complete_service() {
    let mut env = TestEnv::create().await;

    // ── Set up parent VO ─────────────────────────────────────────────────────
    let parent_target = InvocationTarget::mock_virtual_object();
    let parent_service_id = parent_target.as_keyed_service_id().unwrap();
    let parent_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, parent_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, parent_inv_id).await;

    // ── Set up child VO ──────────────────────────────────────────────────────
    let child_target = InvocationTarget::mock_virtual_object();
    let child_service_id = child_target.as_keyed_service_id().unwrap();
    let child_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, child_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, child_inv_id).await;

    let parent_node_id = EntityId::Object(parent_service_id.clone());
    let child_node_id = EntityId::Object(child_service_id.clone());
    let link_completion_id: u32 = 1;
    let complete_completion_id: u32 = 2;

    // ── Step 1: parent applies LinkServiceCommand ────────────────────────────
    let actions = env
        .apply(fixtures::invoker_entry_effect(
            parent_inv_id,
            LinkServiceCommand {
                link_to: child_service_id.clone(),
                result_completion_handler: None,
                link_completion_id,
                name: Default::default(),
            },
        ))
        .await;

    // Extract LinkRequest from outbox
    let link_request = extract_outbox_link_request(&actions);

    // ── Step 2: child processes LinkRequest ───────────────────────────────────
    let actions = env.apply(Command::LinkRequest(link_request)).await;

    // Extract LinkResponse
    let link_response = extract_outbox_link_response(&actions);
    assert_that!(link_response.result, ok(eq(())));

    // Verify linked_from_count was incremented on child VOS
    let child_vos = env
        .storage()
        .get_virtual_object_status(&child_service_id)
        .await
        .unwrap();
    assert_eq!(
        child_vos.linked_from_count(),
        1,
        "child VOS linked_from_count should be 1 after link establishment"
    );

    // ── Step 3: parent processes LinkResponse(Ok) ────────────────────────────
    env.apply(Command::LinkResponse(link_response)).await;

    // ── Step 4: child handler completes (Output + End) ─────────────────────
    // For VO children, this unlocks the VO but does NOT drain VOS response_sinks.
    // VOS sinks (ServiceLinkNotification) only fire when CompleteServiceCommand is called.
    let child_result = Bytes::from_static(b"child_done");
    env.apply_multiple([
        fixtures::invoker_entry_effect(
            child_inv_id,
            OutputCommand {
                result: OutputResult::Success(child_result.clone()),
                name: Default::default(),
            },
        ),
        fixtures::invoker_end_effect(child_inv_id),
    ])
    .await;

    // ── Step 4b: child VO applies CompleteServiceCommand ─────────────────────
    // Start a new handler invocation on the child VO to issue CompleteService.
    let child_complete_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, child_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, child_complete_inv_id).await;

    let child_complete_cid: u32 = 10;
    let actions = env
        .apply(fixtures::invoker_entry_effect(
            child_complete_inv_id,
            CompleteServiceCommand {
                result: ResponseResult::Success(child_result.clone()),
                completion_id: child_complete_cid,
                name: Default::default(),
            },
        ))
        .await;

    // NOW the LinkCompletionNotification should be in the outbox (VOS sinks drained).
    let lcn = extract_outbox_link_completion_notification(&actions);
    assert_that!(lcn.linked_from, eq(parent_node_id.clone()));
    assert_that!(lcn.linked_to, eq(child_node_id.clone()));

    // ── Step 5: parent processes LinkCompletionNotification ───────────────────
    env.apply(Command::LinkCompletionNotification(lcn)).await;

    // Verify edge transitioned to Completed
    assert_that!(
        env.storage()
            .get_service_edge(&parent_service_id, EdgeLabel::LinkedTo, &child_node_id)
            .await
            .unwrap(),
        some(eq(EdgeState::LinkedTo(LinkStatus::Completed)))
    );

    // ── Step 6: parent applies CompleteServiceCommand ─────────────────────────
    let actions = env
        .apply(fixtures::invoker_entry_effect(
            parent_inv_id,
            CompleteServiceCommand {
                result: ResponseResult::Success(Bytes::from_static(b"parent_done")),
                completion_id: complete_completion_id,
                name: Default::default(),
            },
        ))
        .await;

    // CompleteServiceCommand should succeed (no active children).
    assert_that!(
        actions,
        contains(forward_completion_notification(
            parent_inv_id,
            complete_completion_id
        ))
    );

    // VOS should be Completed
    assert_that!(
        env.storage()
            .get_virtual_object_status(&parent_service_id)
            .await
            .unwrap(),
        pat!(VirtualObjectStatus::Completed { .. })
    );

    env.shutdown().await;
}

/// WI parent links to VO child via StartLinkedCommand, parent's run handler returns
/// while child is still active → Completing state. Child completes → parent resumes
/// and finalizes.
///
/// Full flow (all through state machine, no manual seeding):
/// 1. Parent applies StartLinkedCommand → LinkedTo edge + ServiceInvocation with link_from
/// 2. Child processes Invoke (link_from) → sinks + linked_from_count, LinkResponse
/// 3. Parent processes LinkResponse(Ok) → journal completion
/// 4. Parent's run handler returns (Output + End) → Completing (active child)
/// 5. Child completes (Output + End) → LinkCompletionNotification
/// 6. Parent processes LinkCompletionNotification → resumes, finalizes to Free
#[restate_core::test]
async fn e2e_wi_parent_completing_lifecycle() {
    let mut env = TestEnv::create().await;

    // ── Set up WI parent ─────────────────────────────────────────────────────
    let parent_target = InvocationTarget::mock_workflow();
    let parent_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, parent_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, parent_inv_id).await;

    // ── Set up child WI (StartLinkedCommand targets must be workflow handlers) ─
    let child_target = InvocationTarget::mock_workflow();
    let child_inv_id = InvocationId::generate(&child_target, None);
    let child_entity_id = EntityId::WorkflowInvocation(child_inv_id);
    let completion_id: u32 = 1;

    // ── Step 1: parent applies StartLinkedCommand ────────────────────────────
    let actions = env
        .apply(fixtures::invoker_entry_effect(
            parent_inv_id,
            StartLinkedCommand {
                request: CallRequest {
                    invocation_id: child_inv_id,
                    invocation_target: child_target.clone(),
                    span_context: Default::default(),
                    parameter: Default::default(),
                    headers: vec![],
                    idempotency_key: None,
                    completion_retention_duration: Duration::ZERO,
                    journal_retention_duration: Duration::ZERO,
                },
                result_completion_handler: None,
                link_completion_id: completion_id,
                name: Default::default(),
            },
        ))
        .await;

    // Verify linked_to_count incremented on parent IS
    let parent_status = env
        .storage()
        .get_invocation_status(&parent_inv_id)
        .await
        .unwrap();
    assert!(
        parent_status
            .get_invocation_metadata()
            .is_some_and(|m| m.linked_to_count > 0),
        "parent IS linked_to_count should be > 0 after StartLinkedCommand"
    );

    // Extract ServiceInvocation from outbox
    let service_invocation = extract_outbox_service_invocation(&actions);

    // ── Step 2: child processes Invoke with link_from ────────────────────────
    let actions = env.apply(Command::Invoke(service_invocation)).await;

    // Extract LinkResponse
    let link_response = extract_outbox_link_response(&actions);
    assert_that!(link_response.result, ok(eq(())));

    // Verify linked_from_count on child IS
    let child_status = env
        .storage()
        .get_invocation_status(&child_inv_id)
        .await
        .unwrap();
    assert!(
        child_status
            .get_response_sinks()
            .is_some_and(|s| s.iter().any(|sink| matches!(
                sink,
                ServiceInvocationResponseSink::InvocationLinkNotification { .. }
            ))),
        "child must have InvocationLinkNotification sink after link establishment"
    );

    // ── Step 3: parent processes LinkResponse(Ok) ────────────────────────────
    env.apply(Command::LinkResponse(link_response)).await;

    // ── Step 4: parent's run handler returns → should enter Completing ───────
    let parent_result = Bytes::from_static(b"workflow_done");
    env.apply_multiple([
        fixtures::invoker_entry_effect(
            parent_inv_id,
            OutputCommand {
                result: OutputResult::Success(parent_result.clone()),
                name: Default::default(),
            },
        ),
        fixtures::invoker_end_effect(parent_inv_id),
    ])
    .await;

    // Parent must be Completing (active child)
    let parent_status = env
        .storage()
        .get_invocation_status(&parent_inv_id)
        .await
        .unwrap();
    assert!(
        matches!(parent_status, InvocationStatus::Completing(_)),
        "expected Completing, got: {parent_status:?}"
    );

    // ── Step 5: child completes ──────────────────────────────────────────────
    fixtures::mock_pinned_deployment_v5(&mut env, child_inv_id).await;
    let child_result = Bytes::from_static(b"child_done");
    let actions = env
        .apply_multiple([
            fixtures::invoker_entry_effect(
                child_inv_id,
                OutputCommand {
                    result: OutputResult::Success(child_result.clone()),
                    name: Default::default(),
                },
            ),
            fixtures::invoker_end_effect(child_inv_id),
        ])
        .await;

    // Extract LinkCompletionNotification
    let lcn = extract_outbox_link_completion_notification(&actions);

    // ── Step 6: parent processes LinkCompletionNotification → resumes ────────
    env.apply(Command::LinkCompletionNotification(lcn)).await;

    // Parent should be finalized (Free or Completed, not Completing)
    let parent_status = env
        .storage()
        .get_invocation_status(&parent_inv_id)
        .await
        .unwrap();
    assert!(
        !matches!(parent_status, InvocationStatus::Completing(_)),
        "expected parent to be finalized after child completion, got: {parent_status:?}"
    );

    // InvocationEdges should be cleaned up
    assert_that!(
        env.storage()
            .get_invocation_edge(&parent_inv_id, EdgeLabel::LinkedTo, &child_entity_id)
            .await
            .unwrap(),
        none()
    );

    env.shutdown().await;
}

/// VO parent links two children. First child completes, CompleteService fails (active child).
/// Second child completes, CompleteService succeeds.
///
/// Tests that linked_to_count on VOS correctly reflects multiple children and that
/// CompleteServiceCommand's guard works with edges written through the state machine.
#[restate_core::test]
async fn e2e_vo_parent_two_children_complete_service_guard() {
    let mut env = TestEnv::create().await;

    // ── Set up parent VO ─────────────────────────────────────────────────────
    let parent_target = InvocationTarget::mock_virtual_object();
    let parent_service_id = parent_target.as_keyed_service_id().unwrap();
    let parent_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, parent_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, parent_inv_id).await;

    // ── Set up child A ───────────────────────────────────────────────────────
    let child_a_target = InvocationTarget::mock_virtual_object();
    let child_a_service_id = child_a_target.as_keyed_service_id().unwrap();
    let child_a_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, child_a_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, child_a_inv_id).await;
    let _child_a_node = EntityId::Object(child_a_service_id.clone());

    // ── Set up child B ───────────────────────────────────────────────────────
    let child_b_target = InvocationTarget::mock_virtual_object();
    let child_b_service_id = child_b_target.as_keyed_service_id().unwrap();
    let child_b_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, child_b_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, child_b_inv_id).await;
    let _child_b_node = EntityId::Object(child_b_service_id.clone());

    let link_a_cid: u32 = 1;
    let link_b_cid: u32 = 2;
    let complete_cid: u32 = 3;

    // ── Link child A ────────────────────────────────────────────────────────
    let actions = env
        .apply(fixtures::invoker_entry_effect(
            parent_inv_id,
            LinkServiceCommand {
                link_to: child_a_service_id.clone(),
                result_completion_handler: None,
                link_completion_id: link_a_cid,
                name: Default::default(),
            },
        ))
        .await;
    let link_req_a = extract_outbox_link_request(&actions);
    let actions = env.apply(Command::LinkRequest(link_req_a)).await;
    let link_resp_a = extract_outbox_link_response(&actions);
    env.apply(Command::LinkResponse(link_resp_a)).await;

    // ── Link child B ────────────────────────────────────────────────────────
    let actions = env
        .apply(fixtures::invoker_entry_effect(
            parent_inv_id,
            LinkServiceCommand {
                link_to: child_b_service_id.clone(),
                result_completion_handler: None,
                link_completion_id: link_b_cid,
                name: Default::default(),
            },
        ))
        .await;
    let link_req_b = extract_outbox_link_request(&actions);
    let actions = env.apply(Command::LinkRequest(link_req_b)).await;
    let link_resp_b = extract_outbox_link_response(&actions);
    env.apply(Command::LinkResponse(link_resp_b)).await;

    // ── Try CompleteService while both children active → should fail ─────────
    let actions = env
        .apply(fixtures::invoker_entry_effect(
            parent_inv_id,
            CompleteServiceCommand {
                result: ResponseResult::Success(Bytes::from_static(b"done")),
                completion_id: complete_cid,
                name: Default::default(),
            },
        ))
        .await;

    // Should get a failure completion (active children)
    assert_that!(
        actions,
        contains(forward_completion_notification(parent_inv_id, complete_cid))
    );
    // VOS should NOT be Completed yet
    assert!(
        !env.storage()
            .get_virtual_object_status(&parent_service_id)
            .await
            .unwrap()
            .is_completed(),
        "parent should not be Completed while children are active"
    );

    // ── Child A handler completes ───────────────────────────────────────────
    env.apply_multiple([
        fixtures::invoker_entry_effect(
            child_a_inv_id,
            OutputCommand {
                result: OutputResult::Success(Bytes::from_static(b"a_done")),
                name: Default::default(),
            },
        ),
        fixtures::invoker_end_effect(child_a_inv_id),
    ])
    .await;

    // Child A VO calls CompleteService (drains VOS sinks → LinkCompletionNotification)
    let child_a_complete_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, child_a_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, child_a_complete_inv_id).await;
    let actions = env
        .apply(fixtures::invoker_entry_effect(
            child_a_complete_inv_id,
            CompleteServiceCommand {
                result: ResponseResult::Success(Bytes::from_static(b"a_done")),
                completion_id: 20,
                name: Default::default(),
            },
        ))
        .await;
    let lcn_a = extract_outbox_link_completion_notification(&actions);
    env.apply(Command::LinkCompletionNotification(lcn_a)).await;

    // ── Try CompleteService on parent with one child still active → should still fail ──
    let complete_cid_2: u32 = 4;
    env.apply(fixtures::invoker_entry_effect(
        parent_inv_id,
        CompleteServiceCommand {
            result: ResponseResult::Success(Bytes::from_static(b"done")),
            completion_id: complete_cid_2,
            name: Default::default(),
        },
    ))
    .await;
    assert!(
        !env.storage()
            .get_virtual_object_status(&parent_service_id)
            .await
            .unwrap()
            .is_completed(),
        "parent should not be Completed while child B is still active"
    );

    // ── Child B handler completes ───────────────────────────────────────────
    env.apply_multiple([
        fixtures::invoker_entry_effect(
            child_b_inv_id,
            OutputCommand {
                result: OutputResult::Success(Bytes::from_static(b"b_done")),
                name: Default::default(),
            },
        ),
        fixtures::invoker_end_effect(child_b_inv_id),
    ])
    .await;

    // Child B VO calls CompleteService
    let child_b_complete_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, child_b_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, child_b_complete_inv_id).await;
    let actions = env
        .apply(fixtures::invoker_entry_effect(
            child_b_complete_inv_id,
            CompleteServiceCommand {
                result: ResponseResult::Success(Bytes::from_static(b"b_done")),
                completion_id: 21,
                name: Default::default(),
            },
        ))
        .await;
    let lcn_b = extract_outbox_link_completion_notification(&actions);
    env.apply(Command::LinkCompletionNotification(lcn_b)).await;

    // ── Now CompleteService should succeed ────────────────────────────────────
    let complete_cid_3: u32 = 5;
    env.apply(fixtures::invoker_entry_effect(
        parent_inv_id,
        CompleteServiceCommand {
            result: ResponseResult::Success(Bytes::from_static(b"done")),
            completion_id: complete_cid_3,
            name: Default::default(),
        },
    ))
    .await;

    assert!(
        env.storage()
            .get_virtual_object_status(&parent_service_id)
            .await
            .unwrap()
            .is_completed(),
        "parent should be Completed after all children finished"
    );

    env.shutdown().await;
}

/// Multi-parent: two VO parents link to the same child. First parent unlinks → child NOT GCed.
/// Second parent unlinks → child IS GCed (linked_from_count reaches 0).
///
/// Tests that linked_from_count correctly tracks multiple parents and GC cascade
/// only fires when all parents have unlinked.
#[restate_core::test]
async fn e2e_multi_parent_unlink_gc_cascade() {
    let mut env = TestEnv::create().await;

    // ── Set up two parents ───────────────────────────────────────────────────
    let parent_a_target = InvocationTarget::mock_virtual_object();
    let _parent_a_service_id = parent_a_target.as_keyed_service_id().unwrap();
    let parent_a_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, parent_a_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, parent_a_inv_id).await;

    let parent_b_target = InvocationTarget::mock_virtual_object();
    let _parent_b_service_id = parent_b_target.as_keyed_service_id().unwrap();
    let parent_b_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, parent_b_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, parent_b_inv_id).await;

    // ── Set up child VO ──────────────────────────────────────────────────────
    let child_target = InvocationTarget::mock_virtual_object();
    let child_service_id = child_target.as_keyed_service_id().unwrap();
    let child_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, child_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, child_inv_id).await;
    // Write some user state on child so we can verify GC
    write_user_state(&mut env, &child_service_id, b"key", b"value").await;

    // ── Parent A links to child ──────────────────────────────────────────────
    let actions = env
        .apply(fixtures::invoker_entry_effect(
            parent_a_inv_id,
            LinkServiceCommand {
                link_to: child_service_id.clone(),
                result_completion_handler: None,
                link_completion_id: 1,
                name: Default::default(),
            },
        ))
        .await;
    let link_req = extract_outbox_link_request(&actions);
    let actions = env.apply(Command::LinkRequest(link_req)).await;
    let link_resp = extract_outbox_link_response(&actions);
    env.apply(Command::LinkResponse(link_resp)).await;

    // ── Parent B links to same child ─────────────────────────────────────────
    let actions = env
        .apply(fixtures::invoker_entry_effect(
            parent_b_inv_id,
            LinkServiceCommand {
                link_to: child_service_id.clone(),
                result_completion_handler: None,
                link_completion_id: 1,
                name: Default::default(),
            },
        ))
        .await;
    let link_req = extract_outbox_link_request(&actions);
    let actions = env.apply(Command::LinkRequest(link_req)).await;
    let link_resp = extract_outbox_link_response(&actions);
    env.apply(Command::LinkResponse(link_resp)).await;

    // Verify linked_from_count == 2
    let child_vos = env
        .storage()
        .get_virtual_object_status(&child_service_id)
        .await
        .unwrap();
    assert_eq!(
        child_vos.linked_from_count(),
        2,
        "child should have linked_from_count=2 after two parents link"
    );

    // ── Complete the child ───────────────────────────────────────────────────
    let actions = env
        .apply_multiple([
            fixtures::invoker_entry_effect(
                child_inv_id,
                OutputCommand {
                    result: OutputResult::Success(Bytes::from_static(b"child_done")),
                    name: Default::default(),
                },
            ),
            fixtures::invoker_end_effect(child_inv_id),
        ])
        .await;

    // Process both LinkCompletionNotifications
    let lcns: Vec<_> = actions
        .iter()
        .filter_map(|a| match a {
            Action::NewOutboxMessage {
                message:
                    restate_storage_api::outbox_table::OutboxMessage::LinkCompletionNotification(n),
                ..
            } => Some(n.clone()),
            _ => None,
        })
        .collect();
    for lcn in lcns {
        env.apply(Command::LinkCompletionNotification(lcn)).await;
    }

    // Use CompleteServiceCommand to mark child as Completed (via child's own handler
    // — but since the child already completed via Output+End, it's already unlocked.
    // We need to manually set it Completed since we don't have a handler to call CompleteService.)
    write_completed(
        &mut env,
        &child_service_id,
        ResponseResult::Success(Bytes::from_static(b"child_done")),
    )
    .await;
    // Restore linked_from_count on the Completed status
    {
        let mut child_vos = env
            .storage()
            .get_virtual_object_status(&child_service_id)
            .await
            .unwrap();
        *child_vos.linked_from_count_mut() = 2;
        let mut txn = env.storage().transaction();
        txn.put_virtual_object_status(&child_service_id, &child_vos)
            .unwrap();
        txn.commit().await.unwrap();
    }

    // ── Parent A unlinks ─────────────────────────────────────────────────────
    let actions = env
        .apply(fixtures::invoker_entry_effect(
            parent_a_inv_id,
            UnlinkServiceCommand {
                unlink_from: child_service_id.clone(),
                unlink_completion_id: 10,
                name: Default::default(),
            },
        ))
        .await;
    let unlink_req = extract_outbox_unlink_request(&actions);
    env.apply(Command::UnlinkRequest(unlink_req)).await;

    // After first unlink: linked_from_count should be 1, child state preserved
    let child_vos = env
        .storage()
        .get_virtual_object_status(&child_service_id)
        .await
        .unwrap();
    assert_eq!(
        child_vos.linked_from_count(),
        1,
        "child linked_from_count should be 1 after first parent unlinks"
    );
    // State should still exist
    assert_that!(
        env.storage()
            .get_user_state(&child_service_id, &Bytes::from_static(b"key"))
            .await
            .unwrap(),
        some(eq(Bytes::from_static(b"value")))
    );

    // ── Parent B unlinks ─────────────────────────────────────────────────────
    let actions = env
        .apply(fixtures::invoker_entry_effect(
            parent_b_inv_id,
            UnlinkServiceCommand {
                unlink_from: child_service_id.clone(),
                unlink_completion_id: 10,
                name: Default::default(),
            },
        ))
        .await;
    let unlink_req = extract_outbox_unlink_request(&actions);
    env.apply(Command::UnlinkRequest(unlink_req)).await;

    // After second unlink: child should be GCed (state deleted, VOS gone)
    assert_that!(
        env.storage()
            .get_user_state(&child_service_id, &Bytes::from_static(b"key"))
            .await
            .unwrap(),
        none()
    );

    env.shutdown().await;
}

/// VO parent links to WI child via StartLinkedCommand. Child WI completes →
/// LinkCompletionNotification dispatched (via IS response_sinks). Parent calls
/// CompleteServiceCommand → succeeds.
///
/// Exercises the VO→WI cross-type link matrix.
#[restate_core::test]
async fn e2e_vo_parent_wi_child_start_linked() {
    let mut env = TestEnv::create().await;

    let parent_target = InvocationTarget::mock_virtual_object();
    let parent_service_id = parent_target.as_keyed_service_id().unwrap();
    let parent_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, parent_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, parent_inv_id).await;
    let parent_node_id = EntityId::Object(parent_service_id.clone());

    let child_target = InvocationTarget::mock_workflow();
    let child_inv_id = InvocationId::generate(&child_target, None);
    let child_entity_id = EntityId::WorkflowInvocation(child_inv_id);

    // Step 1: VO parent applies StartLinkedCommand
    let actions = env
        .apply(fixtures::invoker_entry_effect(
            parent_inv_id,
            StartLinkedCommand {
                request: CallRequest {
                    invocation_id: child_inv_id,
                    invocation_target: child_target.clone(),
                    span_context: Default::default(),
                    parameter: Default::default(),
                    headers: vec![],
                    idempotency_key: None,
                    completion_retention_duration: Duration::ZERO,
                    journal_retention_duration: Duration::ZERO,
                },
                result_completion_handler: None,
                link_completion_id: 1,
                name: Default::default(),
            },
        ))
        .await;
    let si = extract_outbox_service_invocation(&actions);

    // Step 2: child WI processes Invoke with link_from
    let actions = env.apply(Command::Invoke(si)).await;
    let lr = extract_outbox_link_response(&actions);
    assert_that!(lr.result, ok(eq(())));

    // Step 3: parent processes LinkResponse
    env.apply(Command::LinkResponse(lr)).await;

    // Step 4: child WI completes → LinkCompletionNotification
    fixtures::mock_pinned_deployment_v5(&mut env, child_inv_id).await;
    let actions = env
        .apply_multiple([
            fixtures::invoker_entry_effect(
                child_inv_id,
                OutputCommand {
                    result: OutputResult::Success(Bytes::from_static(b"done")),
                    name: Default::default(),
                },
            ),
            fixtures::invoker_end_effect(child_inv_id),
        ])
        .await;
    let lcn = extract_outbox_link_completion_notification(&actions);
    assert_that!(lcn.linked_from, eq(parent_node_id.clone()));
    assert_that!(lcn.linked_to, eq(child_entity_id.clone()));

    // Step 5: parent processes LCN
    env.apply(Command::LinkCompletionNotification(lcn)).await;
    assert_that!(
        env.storage()
            .get_service_edge(&parent_service_id, EdgeLabel::LinkedTo, &child_entity_id)
            .await
            .unwrap(),
        some(eq(EdgeState::LinkedTo(LinkStatus::Completed)))
    );

    // Step 6: parent CompleteServiceCommand → succeeds
    env.apply(fixtures::invoker_entry_effect(
        parent_inv_id,
        CompleteServiceCommand {
            result: ResponseResult::Success(Bytes::from_static(b"parent_done")),
            completion_id: 2,
            name: Default::default(),
        },
    ))
    .await;
    assert!(
        env.storage()
            .get_virtual_object_status(&parent_service_id)
            .await
            .unwrap()
            .is_completed()
    );

    env.shutdown().await;
}

/// WI parent links to existing VO child via LinkServiceCommand. WI parent's run handler
/// returns while child active → Completing. VO child completes via CompleteServiceCommand →
/// LinkCompletionNotification. Parent resumes and finalizes.
///
/// Exercises the WI→VO cross-type link matrix.
#[restate_core::test]
async fn e2e_wi_parent_vo_child_link_service() {
    let mut env = TestEnv::create().await;

    let parent_target = InvocationTarget::mock_workflow();
    let parent_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, parent_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, parent_inv_id).await;

    let child_target = InvocationTarget::mock_virtual_object();
    let child_service_id = child_target.as_keyed_service_id().unwrap();
    let child_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, child_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, child_inv_id).await;
    let child_node_id = EntityId::Object(child_service_id.clone());

    // Step 1: WI parent applies LinkServiceCommand to VO child
    let actions = env
        .apply(fixtures::invoker_entry_effect(
            parent_inv_id,
            LinkServiceCommand {
                link_to: child_service_id.clone(),
                result_completion_handler: None,
                link_completion_id: 1,
                name: Default::default(),
            },
        ))
        .await;
    let link_req = extract_outbox_link_request(&actions);

    // Step 2: VO child processes LinkRequest
    let actions = env.apply(Command::LinkRequest(link_req)).await;
    assert_eq!(
        env.storage()
            .get_virtual_object_status(&child_service_id)
            .await
            .unwrap()
            .linked_from_count(),
        1
    );
    let lr = extract_outbox_link_response(&actions);

    // Step 3: WI parent processes LinkResponse
    env.apply(Command::LinkResponse(lr)).await;

    // Step 4: WI parent's run handler returns → Completing
    env.apply_multiple([
        fixtures::invoker_entry_effect(
            parent_inv_id,
            OutputCommand {
                result: OutputResult::Success(Bytes::from_static(b"wf_done")),
                name: Default::default(),
            },
        ),
        fixtures::invoker_end_effect(parent_inv_id),
    ])
    .await;
    assert!(matches!(
        env.storage()
            .get_invocation_status(&parent_inv_id)
            .await
            .unwrap(),
        InvocationStatus::Completing(_)
    ));

    // Step 5: VO child handler completes, then CompleteServiceCommand drains VOS sinks
    env.apply_multiple([
        fixtures::invoker_entry_effect(
            child_inv_id,
            OutputCommand {
                result: OutputResult::Success(Bytes::from_static(b"child_done")),
                name: Default::default(),
            },
        ),
        fixtures::invoker_end_effect(child_inv_id),
    ])
    .await;
    let child_complete_inv =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, child_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, child_complete_inv).await;
    let actions = env
        .apply(fixtures::invoker_entry_effect(
            child_complete_inv,
            CompleteServiceCommand {
                result: ResponseResult::Success(Bytes::from_static(b"child_done")),
                completion_id: 10,
                name: Default::default(),
            },
        ))
        .await;
    let lcn = extract_outbox_link_completion_notification(&actions);

    // Step 6: WI parent processes LCN → resumes
    env.apply(Command::LinkCompletionNotification(lcn)).await;
    assert!(!matches!(
        env.storage()
            .get_invocation_status(&parent_inv_id)
            .await
            .unwrap(),
        InvocationStatus::Completing(_)
    ));
    assert_that!(
        env.storage()
            .get_invocation_edge(&parent_inv_id, EdgeLabel::LinkedTo, &child_node_id)
            .await
            .unwrap(),
        none()
    );

    env.shutdown().await;
}

/// BUG-1 regression: unlink a WI child in Completing state, then child completes.
///
/// After the parent unlinks the Completing child, and the child later completes
/// (because its grandchild completes), the parent should NOT receive a
/// LinkCompletionNotification. The notification sink should have been removed by the
/// unlink. With the bug, `on_unlink_request` fails to clean up sinks on a Completing
/// invocation, so the parent receives a stale notification.
///
/// Flow:
/// 1. WI parent creates WI child via StartLinkedCommand (full handshake)
/// 2. Child creates grandchild VO via LinkServiceCommand (full handshake)
/// 3. Child's run handler returns (Output + End) → enters Completing (grandchild still active)
/// 4. Parent unlinks child using UnlinkInvocationCommand → UnlinkRequest processed by child
/// 5. Grandchild handler completes (Output + End) + CompleteServiceCommand → drains VOS sinks
///    → LinkCompletionNotification for child
/// 6. Child processes grandchild's LinkCompletionNotification → resumes completing → completes
/// 7. Assert: No LinkCompletionNotification where local == parent_entity_id in step 6 actions
#[restate_core::test]
async fn e2e_unlink_completing_wi_child_no_stale_notification() {
    let mut env = TestEnv::create().await;

    // ── Set up WI parent ─────────────────────────────────────────────────────
    let parent_target = InvocationTarget::mock_workflow();
    let parent_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, parent_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, parent_inv_id).await;
    let parent_entity_id = EntityId::WorkflowInvocation(parent_inv_id);

    // ── Step 1: parent creates WI child via StartLinkedCommand ───────────────
    let child_target = InvocationTarget::mock_workflow();
    let child_inv_id = InvocationId::generate(&child_target, None);

    let actions = env
        .apply(fixtures::invoker_entry_effect(
            parent_inv_id,
            StartLinkedCommand {
                request: CallRequest {
                    invocation_id: child_inv_id,
                    invocation_target: child_target.clone(),
                    span_context: Default::default(),
                    parameter: Default::default(),
                    headers: vec![],
                    idempotency_key: None,
                    completion_retention_duration: Duration::ZERO,
                    journal_retention_duration: Duration::ZERO,
                },
                result_completion_handler: None,
                link_completion_id: 1,
                name: Default::default(),
            },
        ))
        .await;
    let si = extract_outbox_service_invocation(&actions);

    // Child processes Invoke with link_from → produces LinkResponse
    let actions = env.apply(Command::Invoke(si)).await;
    let lr = extract_outbox_link_response(&actions);
    env.apply(Command::LinkResponse(lr)).await;

    // ── Step 2: child creates a grandchild VO via LinkServiceCommand ─────────
    fixtures::mock_pinned_deployment_v5(&mut env, child_inv_id).await;
    let grandchild_target = InvocationTarget::mock_virtual_object();
    let grandchild_service_id = grandchild_target.as_keyed_service_id().unwrap();
    let grandchild_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, grandchild_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, grandchild_inv_id).await;

    let actions = env
        .apply(fixtures::invoker_entry_effect(
            child_inv_id,
            LinkServiceCommand {
                link_to: grandchild_service_id.clone(),
                result_completion_handler: None,
                link_completion_id: 10,
                name: Default::default(),
            },
        ))
        .await;
    let link_req = extract_outbox_link_request(&actions);
    let actions = env.apply(Command::LinkRequest(link_req)).await;
    let link_resp = extract_outbox_link_response(&actions);
    env.apply(Command::LinkResponse(link_resp)).await;

    // ── Step 3: child's run handler returns → enters Completing ──────────────
    env.apply_multiple([
        fixtures::invoker_entry_effect(
            child_inv_id,
            OutputCommand {
                result: OutputResult::Success(Bytes::from_static(b"child_done")),
                name: Default::default(),
            },
        ),
        fixtures::invoker_end_effect(child_inv_id),
    ])
    .await;

    // Behavioral assertion: child must be Completing (grandchild still active)
    let child_status = env
        .storage()
        .get_invocation_status(&child_inv_id)
        .await
        .unwrap();
    assert!(
        matches!(child_status, InvocationStatus::Completing(_)),
        "child should be Completing (grandchild still active), got: {child_status:?}"
    );

    // ── Step 4: parent unlinks child (WI) using UnlinkInvocationCommand ──────
    let actions = env
        .apply(fixtures::invoker_entry_effect(
            parent_inv_id,
            UnlinkInvocationCommand {
                unlink_from: child_inv_id,
                unlink_completion_id: 20,
                name: Default::default(),
            },
        ))
        .await;
    let unlink_req = extract_outbox_unlink_request(&actions);
    env.apply(Command::UnlinkRequest(unlink_req)).await;

    // BUG-1: linked_from_count should be 0 on the Completing child after unlink.
    // The bug: on_unlink_request uses get_invocation_metadata_mut() which returns None
    // for Completing, so linked_from_count stays at 1.
    let child_status = env
        .storage()
        .get_invocation_status(&child_inv_id)
        .await
        .unwrap();
    assert!(
        matches!(child_status, InvocationStatus::Completing(_)),
        "child should still be Completing after unlink"
    );
    assert_eq!(
        child_status.linked_from_count(),
        Some(0),
        "linked_from_count should be 0 after unlink of Completing WI child"
    );

    // ── Step 5: grandchild handler completes (Output + End) ──────────────────
    env.apply_multiple([
        fixtures::invoker_entry_effect(
            grandchild_inv_id,
            OutputCommand {
                result: OutputResult::Success(Bytes::from_static(b"grandchild_done")),
                name: Default::default(),
            },
        ),
        fixtures::invoker_end_effect(grandchild_inv_id),
    ])
    .await;

    // New grandchild handler issues CompleteServiceCommand → drains VOS sinks
    // → LinkCompletionNotification dispatched to child
    let grandchild_complete_inv =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, grandchild_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, grandchild_complete_inv).await;
    let actions = env
        .apply(fixtures::invoker_entry_effect(
            grandchild_complete_inv,
            CompleteServiceCommand {
                result: ResponseResult::Success(Bytes::from_static(b"grandchild_done")),
                completion_id: 50,
                name: Default::default(),
            },
        ))
        .await;
    let grandchild_lcn = extract_outbox_link_completion_notification(&actions);

    // ── Step 6: child processes grandchild's LinkCompletionNotification ───────
    // This resumes completing → child fully completes.
    let actions = env
        .apply(Command::LinkCompletionNotification(grandchild_lcn))
        .await;

    // ── Step 7: Assert no stale LinkCompletionNotification to parent ──────────
    // BUG-1: if on_unlink_request didn't clean up the InvocationLinkNotification sink
    // from the Completing child, resume_completing_invocation dispatches a stale
    // LinkCompletionNotification to the parent.
    let stale_lcn = actions.iter().find(|a| {
        matches!(
            a,
            Action::NewOutboxMessage {
                message: restate_storage_api::outbox_table::OutboxMessage::LinkCompletionNotification(n),
                ..
            } if n.linked_from == parent_entity_id
        )
    });
    assert!(
        stale_lcn.is_none(),
        "BUG-1: parent should NOT receive a LinkCompletionNotification after unlink, \
         but got: {stale_lcn:?}"
    );

    env.shutdown().await;
}

/// BUG-2 regression: two parents link to an already-Completed VO child.
/// First parent unlinks → should NOT trigger GC (second parent still linked).
/// Second parent unlinks → child user state should be deleted (GC'd).
///
/// The short-circuit path in on_link_request for Completed VOs fires the handler
/// immediately but fails to increment linked_from_count. With two parents linking
/// to the same completed child, linked_from_count stays 0. When the first parent
/// unlinks, the GC cascade fires prematurely.
///
/// Flow:
/// 1. Set up child VO with user state, complete it via Output+End then CompleteServiceCommand
/// 2. Parent A links to already-Completed child (short-circuit path) — full handshake
/// 3. Parent B links to same child (short-circuit path) — full handshake
/// 4. Parent A unlinks via UnlinkServiceCommand → UnlinkRequest
/// 5. Assert: child user state still exists (not GC'd)
/// 6. Parent B unlinks via UnlinkServiceCommand → UnlinkRequest
/// 7. Assert: child user state deleted (GC'd)
#[restate_core::test]
async fn e2e_link_to_completed_vo_then_unlink_no_premature_gc() {
    let mut env = TestEnv::create().await;

    // ── Step 1: Set up child VO with user state, then complete it ────────────
    let child_target = InvocationTarget::mock_virtual_object();
    let child_service_id = child_target.as_keyed_service_id().unwrap();
    let child_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, child_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, child_inv_id).await;

    // Write user state on child — survives as long as there are linked parents
    write_user_state(&mut env, &child_service_id, b"key", b"value").await;

    // Child run handler completes
    env.apply_multiple([
        fixtures::invoker_entry_effect(
            child_inv_id,
            OutputCommand {
                result: OutputResult::Success(Bytes::from_static(b"child_done")),
                name: Default::default(),
            },
        ),
        fixtures::invoker_end_effect(child_inv_id),
    ])
    .await;

    // New handler calls CompleteServiceCommand → child VOS transitions to Completed
    let child_complete_inv =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, child_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, child_complete_inv).await;
    env.apply(fixtures::invoker_entry_effect(
        child_complete_inv,
        CompleteServiceCommand {
            result: ResponseResult::Success(Bytes::from_static(b"child_done")),
            completion_id: 50,
            name: Default::default(),
        },
    ))
    .await;

    assert!(
        env.storage()
            .get_virtual_object_status(&child_service_id)
            .await
            .unwrap()
            .is_completed(),
        "child should be Completed before linking parents"
    );

    // ── Set up parent A and parent B ─────────────────────────────────────────
    let parent_a_target = InvocationTarget::mock_virtual_object();
    let parent_a_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, parent_a_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, parent_a_inv_id).await;

    let parent_b_target = InvocationTarget::mock_virtual_object();
    let parent_b_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, parent_b_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, parent_b_inv_id).await;

    // ── Step 2: Parent A links to already-completed child ────────────────────
    let actions = env
        .apply(fixtures::invoker_entry_effect(
            parent_a_inv_id,
            LinkServiceCommand {
                link_to: child_service_id.clone(),
                result_completion_handler: None,
                link_completion_id: 1,
                name: Default::default(),
            },
        ))
        .await;
    let link_req_a = extract_outbox_link_request(&actions);

    // Child processes LinkRequest → short-circuit (already Completed): LinkResponse + LCN
    let actions = env.apply(Command::LinkRequest(link_req_a)).await;
    let lr_a = extract_outbox_link_response(&actions);
    assert_that!(lr_a.result, ok(eq(())));
    let lcn_a = extract_outbox_link_completion_notification(&actions);

    env.apply(Command::LinkResponse(lr_a)).await;
    env.apply(Command::LinkCompletionNotification(lcn_a)).await;

    // ── Step 3: Parent B links to same already-completed child ───────────────
    let actions = env
        .apply(fixtures::invoker_entry_effect(
            parent_b_inv_id,
            LinkServiceCommand {
                link_to: child_service_id.clone(),
                result_completion_handler: None,
                link_completion_id: 1,
                name: Default::default(),
            },
        ))
        .await;
    let link_req_b = extract_outbox_link_request(&actions);
    let actions = env.apply(Command::LinkRequest(link_req_b)).await;
    let lr_b = extract_outbox_link_response(&actions);
    assert_that!(lr_b.result, ok(eq(())));
    let lcn_b = extract_outbox_link_completion_notification(&actions);

    env.apply(Command::LinkResponse(lr_b)).await;
    env.apply(Command::LinkCompletionNotification(lcn_b)).await;

    // ── Step 4: Parent A unlinks → should NOT trigger GC ─────────────────────
    let actions = env
        .apply(fixtures::invoker_entry_effect(
            parent_a_inv_id,
            UnlinkServiceCommand {
                unlink_from: child_service_id.clone(),
                unlink_completion_id: 30,
                name: Default::default(),
            },
        ))
        .await;
    let unlink_req_a = extract_outbox_unlink_request(&actions);
    env.apply(Command::UnlinkRequest(unlink_req_a)).await;

    // ── Step 5: child user state must still exist (second parent still linked) ─
    // BUG-2: linked_from_count was never incremented on the Completed VOS during short-circuit,
    // so on_unlink_request sees 0 parents → GC cascade fires prematurely → state deleted.
    // BUG-2: if short-circuit path never incremented linked_from_count, GC fires here
    assert_that!(
        env.storage()
            .get_user_state(&child_service_id, &Bytes::from_static(b"key"))
            .await
            .unwrap(),
        some(eq(Bytes::from_static(b"value")))
    );

    // ── Step 6: Parent B unlinks ──────────────────────────────────────────────
    let actions = env
        .apply(fixtures::invoker_entry_effect(
            parent_b_inv_id,
            UnlinkServiceCommand {
                unlink_from: child_service_id.clone(),
                unlink_completion_id: 31,
                name: Default::default(),
            },
        ))
        .await;
    let unlink_req_b = extract_outbox_unlink_request(&actions);
    env.apply(Command::UnlinkRequest(unlink_req_b)).await;

    // ── Step 7: child user state must now be deleted (no more linked parents) ─
    assert_that!(
        env.storage()
            .get_user_state(&child_service_id, &Bytes::from_static(b"key"))
            .await
            .unwrap(),
        none()
    );

    env.shutdown().await;
}

// ─────────────────────────────────────────────────────────────────────────────
// Regression tests for code review findings C-1, H-1, H-2
// ─────────────────────────────────────────────────────────────────────────────

/// C-1 regression: When a StartLinkedCommand targets a WI child that has already completed
/// (dedup path), `on_link_from_deduplicated_invocation` must send a `LinkCompletionNotification`
/// so the parent's LinkedTo(Active) edge transitions to Completed. Without this, the parent
/// hangs permanently in `Completing` state.
#[restate_core::test]
async fn c1_dedup_link_to_completed_wi_child_must_not_hang_parent() {
    let mut env = TestEnv::create().await;

    // ── Set up child WI — it will run to completion before any parent links to it ──
    let child_target = InvocationTarget::mock_workflow();
    let child_inv_id = InvocationId::mock_generate(&child_target);
    // Start child via a standalone invoke (no link_from).
    // Non-zero completion_retention ensures the child lands in InvocationStatus::Completed
    // (not Free), which is the precondition for the C-1 bug.
    let mut child_invocation = ServiceInvocation::initialize(
        child_inv_id,
        child_target.clone(),
        restate_types::invocation::Source::Ingress(PartitionProcessorRpcRequestId::default()),
    );
    child_invocation.completion_retention_duration = Duration::from_secs(3600);
    env.apply(Command::Invoke(Box::new(child_invocation))).await;
    fixtures::mock_pinned_deployment_v5(&mut env, child_inv_id).await;

    // Drive child to completion (Output + End).
    env.apply_multiple([
        fixtures::invoker_entry_effect(
            child_inv_id,
            OutputCommand {
                result: OutputResult::Success(Bytes::from_static(b"child_done")),
                name: Default::default(),
            },
        ),
        fixtures::invoker_end_effect(child_inv_id),
    ])
    .await;

    // Verify child is Completed (non-zero retention keeps it in Completed, not Free).
    let child_status = env
        .storage()
        .get_invocation_status(&child_inv_id)
        .await
        .unwrap();
    assert!(
        matches!(child_status, InvocationStatus::Completed(_)),
        "child must be Completed (non-zero retention), got: {child_status:?}"
    );

    // ── Set up WI parent ────────────────────────────────────────────────────
    let parent_target = InvocationTarget::mock_workflow();
    let parent_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, parent_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, parent_inv_id).await;
    let parent_entity_id = EntityId::WorkflowInvocation(parent_inv_id);
    let completion_id: u32 = 1;

    // ── Parent issues StartLinkedCommand targeting the already-completed child ──
    let actions = env
        .apply(fixtures::invoker_entry_effect(
            parent_inv_id,
            StartLinkedCommand {
                request: restate_types::journal_v2::command::CallRequest::mock(
                    child_inv_id,
                    child_target.clone(),
                ),
                result_completion_handler: None,
                link_completion_id: completion_id,
                name: Default::default(),
            },
        ))
        .await;

    let service_invocation = extract_outbox_service_invocation(&actions);
    assert_that!(service_invocation.invocation_id, eq(child_inv_id));
    assert_that!(
        service_invocation.link_from,
        some(eq(parent_entity_id.clone()))
    );

    // ── Child partition processes the dedup invoke ──────────────────────────
    let actions = env.apply(Command::Invoke(service_invocation)).await;

    // LinkResponse(Ok) must be enqueued.
    let link_response = extract_outbox_link_response(&actions);
    assert_that!(link_response.result, ok(eq(())));
    assert_that!(link_response.caller_invocation_id, eq(parent_inv_id));

    // CRITICAL ASSERTION: A LinkCompletionNotification must also be emitted because
    // the child is already completed. Without this, the parent will hang in Completing.
    let has_lcn = actions.iter().any(|a| {
        matches!(
            a,
            Action::NewOutboxMessage {
                message:
                    restate_storage_api::outbox_table::OutboxMessage::LinkCompletionNotification(_),
                ..
            }
        )
    });
    assert!(
        has_lcn,
        "C-1 regression: on_link_from_deduplicated_invocation must emit \
         LinkCompletionNotification when child is already Completed/Free. \
         Without this, the parent hangs permanently in Completing state."
    );

    // ── Parent processes LinkResponse ──────────────────────────────────────
    env.apply(Command::LinkResponse(link_response)).await;

    // If a LinkCompletionNotification was produced, apply it too.
    if has_lcn {
        let lcn = extract_outbox_link_completion_notification(&actions);
        env.apply(Command::LinkCompletionNotification(lcn)).await;
    }

    // ── Parent completes its run handler ────────────────────────────────────
    env.apply_multiple([
        fixtures::invoker_entry_effect(
            parent_inv_id,
            OutputCommand {
                result: OutputResult::Success(Bytes::from_static(b"parent_done")),
                name: Default::default(),
            },
        ),
        fixtures::invoker_end_effect(parent_inv_id),
    ])
    .await;

    // CRITICAL ASSERTION: parent must NOT be stuck in Completing.
    let parent_status = env
        .storage()
        .get_invocation_status(&parent_inv_id)
        .await
        .unwrap();
    assert!(
        !matches!(parent_status, InvocationStatus::Completing(_)),
        "C-1 regression: parent must not hang in Completing after linking to \
         an already-completed child. Got: {parent_status:?}"
    );

    env.shutdown().await;
}

/// H-1 regression: When a WI parent completes (transitions out of Completing), it must
/// cascade UnlinkRequests to its children so their `linked_from_count` is decremented
/// and completed VO children can be GC'd. Currently, `resume_completing_invocation`
/// bulk-deletes InvocationEdges but does not send UnlinkRequests.
#[restate_core::test]
async fn h1_wi_parent_completion_must_cascade_unlink_to_children() {
    let mut env = TestEnv::create().await;

    // ── Set up WI parent ────────────────────────────────────────────────────
    let parent_target = InvocationTarget::mock_workflow();
    let parent_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, parent_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, parent_inv_id).await;
    let parent_entity_id = EntityId::WorkflowInvocation(parent_inv_id);

    // ── Set up child VO with state (so we can check GC) ─────────────────────
    let child_target = InvocationTarget::mock_virtual_object();
    let child_service_id = child_target.as_keyed_service_id().unwrap();
    let child_inv_id =
        fixtures::mock_start_invocation_with_invocation_target(&mut env, child_target.clone())
            .await;
    fixtures::mock_pinned_deployment_v5(&mut env, child_inv_id).await;
    let child_entity_id = EntityId::Object(child_service_id.clone());

    // Write some user state on the child VO so we can verify GC later.
    write_user_state(&mut env, &child_service_id, b"child_key", b"child_val").await;

    // ── Link parent → child via InvocationEdges (simulating StartLinkedCommand) ──
    {
        let mut txn = env.storage().transaction();
        txn.put_invocation_edge(
            &parent_inv_id,
            &child_entity_id,
            &EdgeState::LinkedTo(LinkStatus::Active),
        )
        .unwrap();
        txn.commit().await.unwrap();
    }
    // Add InvocationLinkNotification sink to child VO's response_sinks.
    {
        let mut child_status = env
            .storage()
            .get_virtual_object_status(&child_service_id)
            .await
            .unwrap();
        if let Some(sinks) = child_status.response_sinks_mut() {
            sinks.insert(ServiceInvocationResponseSink::InvocationLinkNotification {
                linked_from: parent_inv_id,
            });
        }
        *child_status.linked_from_count_mut() = 1;
        let mut txn = env.storage().transaction();
        txn.put_virtual_object_status(&child_service_id, &child_status)
            .unwrap();
        txn.commit().await.unwrap();
    }
    mark_invocation_has_links(&mut env, &parent_inv_id).await;

    // ── Child VO completes via CompleteServiceCommand ────────────────────────
    let complete_cid: u32 = 10;
    let actions = env
        .apply(fixtures::invoker_entry_effect(
            child_inv_id,
            CompleteServiceCommand {
                result: ResponseResult::Success(Bytes::from_static(b"child_done")),
                completion_id: complete_cid,
                name: Default::default(),
            },
        ))
        .await;

    // LinkCompletionNotification dispatched to parent.
    let lcn = extract_outbox_link_completion_notification(&actions);
    assert_that!(lcn.linked_from, eq(parent_entity_id.clone()));

    // ── Drive parent to Completing, then resume via LinkCompletionNotification ──
    env.apply_multiple([
        fixtures::invoker_entry_effect(
            parent_inv_id,
            OutputCommand {
                result: OutputResult::Success(Bytes::from_static(b"parent_done")),
                name: Default::default(),
            },
        ),
        fixtures::invoker_end_effect(parent_inv_id),
    ])
    .await;

    let parent_status = env
        .storage()
        .get_invocation_status(&parent_inv_id)
        .await
        .unwrap();
    assert!(
        matches!(parent_status, InvocationStatus::Completing(_)),
        "parent should be Completing, got: {parent_status:?}"
    );

    // Apply the LinkCompletionNotification → parent resumes and finalizes.
    let actions = env.apply(Command::LinkCompletionNotification(lcn)).await;

    // Parent finalized.
    let parent_status = env
        .storage()
        .get_invocation_status(&parent_inv_id)
        .await
        .unwrap();
    assert!(
        !matches!(parent_status, InvocationStatus::Completing(_)),
        "parent should be finalized, got: {parent_status:?}"
    );

    // H-1 ASSERTION: An UnlinkRequest must have been sent to the child during
    // resume_completing_invocation, so the child's linked_from_count is decremented.
    let has_unlink = actions.iter().any(|a| {
        matches!(
            a,
            Action::NewOutboxMessage {
                message: restate_storage_api::outbox_table::OutboxMessage::UnlinkRequest(_),
                ..
            }
        )
    });
    assert!(
        has_unlink,
        "H-1 regression: resume_completing_invocation must cascade UnlinkRequests \
         to children so their linked_from_count is decremented. Without this, \
         completed VO children leak storage because linked_from_count never reaches 0."
    );

    env.shutdown().await;
}

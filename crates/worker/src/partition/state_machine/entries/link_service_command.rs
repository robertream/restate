// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use tracing::warn;

use restate_storage_api::LinkedServicesStorage;
use restate_storage_api::fsm_table::WriteFsmTable;
use restate_storage_api::outbox_table::{OutboxMessage, WriteOutboxTable};

use restate_types::errors::InvocationError;
use restate_types::invocation::{
    EdgeLabel, EdgeState, EntityId, InvocationTargetType, LinkRequest, LinkStatus,
    ServiceCompletionTarget, WorkflowHandlerType,
};
use restate_types::journal_v2::{
    EntryMetadata, LinkServiceCommand, LinkServiceCompletion, LinkServiceResult,
};

use crate::partition::state_machine::entries::ApplyJournalCommandEffect;
use crate::partition::state_machine::{CommandHandler, Error, StateMachineApplyContext};

pub(super) type ApplyLinkServiceCommand<'e> = ApplyJournalCommandEffect<'e, LinkServiceCommand>;

impl<'e, 'ctx: 'e, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for ApplyLinkServiceCommand<'e>
where
    S: LinkedServicesStorage + WriteOutboxTable + WriteFsmTable,
{
    async fn apply(mut self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        let invocation_metadata = self
            .invocation_status
            .get_invocation_metadata()
            .expect("In-Flight invocation metadata must be present");

        // Validate caller is a keyed service
        let Some(link_from) = invocation_metadata.invocation_target.as_keyed_service_id() else {
            warn!(
                "Trying to process entry {} for a target that is not a keyed service",
                self.entry.ty()
            );
            return Ok(());
        };

        let link_to = self.entry.link_to.clone();
        let link_to_node = EntityId::Object(link_to.clone());

        // Determine parent entity based on caller type.
        // WI callers (workflow run handler) use InvocationEdges.
        // VO callers (and workflow shared handlers) use ServiceEdges.
        let caller_target_ty = invocation_metadata.invocation_target.invocation_target_ty();
        let (link_from_entity, handler_sink) = match caller_target_ty {
            InvocationTargetType::Workflow(WorkflowHandlerType::Workflow) => {
                (EntityId::WorkflowInvocation(self.invocation_id), None)
            }
            _ => (
                EntityId::Object(link_from.clone()),
                // VO parents may register an onCompleted handler. Retention is inherited
                // from the parent's invocation metadata and carried on the sink so that
                // the spawned handler invocation is retained per the parent's policy
                // whenever and wherever the sink fires.
                self.entry
                    .result_completion_handler
                    .clone()
                    .map(|handler_name| ServiceCompletionTarget {
                        service_id: link_from.clone(),
                        handler_name,
                        completion_retention_duration: invocation_metadata
                            .completion_retention_duration,
                        journal_retention_duration: invocation_metadata.journal_retention_duration,
                    }),
            ),
        };

        // Validate no self-link
        if link_from_entity == link_to_node {
            self.then_apply_completion(LinkServiceCompletion {
                completion_id: self.entry.link_completion_id,
                result: LinkServiceResult::Failure(
                    InvocationError::new(400u16, "cannot link a service to itself").into(),
                ),
            });
            return Ok(());
        }

        // Validate no duplicate link (existing LinkedTo edge).
        // For VO parents: check ServiceEdges; for WI parents: check InvocationEdges.
        let existing = match &link_from_entity {
            EntityId::Object(sid) => {
                ctx.storage
                    .get_service_edge(sid, EdgeLabel::LinkedTo, &link_to_node)
                    .await?
            }
            EntityId::WorkflowInvocation(_) => {
                ctx.storage
                    .get_invocation_edge(&self.invocation_id, EdgeLabel::LinkedTo, &link_to_node)
                    .await?
            }
        };
        if existing.is_some() {
            self.then_apply_completion(LinkServiceCompletion {
                completion_id: self.entry.link_completion_id,
                result: LinkServiceResult::Failure(
                    InvocationError::new(409u16, "service is already linked").into(),
                ),
            });
            return Ok(());
        }

        // NOTE: 1-hop cycle detection was removed. Transitive cycles (A→B→C→A) are surfaced
        // by the Completing lifecycle — a deadlocked cycle will never have all children complete,
        // so the parent remains in Completing state indefinitely (visible via introspection/admin API).

        // Write LinkedTo(Active) edge to the appropriate table
        match &link_from_entity {
            EntityId::Object(sid) => {
                ctx.storage
                    .put_service_edge(sid, &link_to_node, &EdgeState::LinkedTo(LinkStatus::Active))
                    .map_err(Error::Storage)?;
            }
            EntityId::WorkflowInvocation(_) => {
                ctx.storage
                    .put_invocation_edge(
                        &self.invocation_id,
                        &link_to_node,
                        &EdgeState::LinkedTo(LinkStatus::Active),
                    )
                    .map_err(Error::Storage)?;
            }
        }

        // Increment linked_to_count on the parent — records that this invocation has linked
        // to one more child. Used by end_invocation to gate the Completing lifecycle check.
        if let Some(count) = self.invocation_status.linked_to_count_mut() {
            *count += 1;
        }

        // Enqueue LinkRequest to child's partition — the child inserts notification sinks,
        // increments linked_from_count, and sends LinkResponse back, completing the handshake.
        ctx.handle_outgoing_message(OutboxMessage::LinkRequest(LinkRequest {
            link_to: link_to_node,
            link_from: link_from_entity,
            caller_invocation_id: self.invocation_id,
            caller_completion_id: self.entry.link_completion_id,
            handler_sink,
        }))?;

        Ok(())
    }
}

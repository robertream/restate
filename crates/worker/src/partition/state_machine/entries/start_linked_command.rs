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

use restate_storage_api::fsm_table::WriteFsmTable;
use restate_storage_api::invocation_edges_table::WriteInvocationEdgesTable;
use restate_storage_api::outbox_table::{OutboxMessage, WriteOutboxTable};
use restate_storage_api::service_edges_table::WriteServiceEdgesTable;

use restate_types::errors::InvocationError;
use restate_types::invocation::{
    EdgeState, EntityId, InvocationTargetType, LinkStatus, ServiceCompletionTarget,
    ServiceInvocation, ServiceInvocationResponseSink, Source, WorkflowHandlerType,
};
use restate_types::journal_v2::{
    EntryMetadata, StartLinkedCommand, StartLinkedCompletion, StartLinkedResult,
};

use crate::partition::state_machine::entries::ApplyJournalCommandEffect;
use crate::partition::state_machine::{CommandHandler, Error, StateMachineApplyContext};

pub(super) type ApplyStartLinkedCommand<'e> = ApplyJournalCommandEffect<'e, StartLinkedCommand>;

impl<'e, 'ctx: 'e, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for ApplyStartLinkedCommand<'e>
where
    S: WriteServiceEdgesTable + WriteInvocationEdgesTable + WriteOutboxTable + WriteFsmTable,
{
    async fn apply(mut self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        let invocation_metadata = self
            .invocation_status
            .get_invocation_metadata()
            .expect("In-Flight invocation metadata must be present");

        // Validate: caller must be a keyed service (VO or Workflow)
        let Some(parent_service_id) = invocation_metadata.invocation_target.as_keyed_service_id()
        else {
            warn!(
                "Trying to process entry {} for a target that is not a keyed service",
                self.entry.ty()
            );
            return Ok(());
        };

        // Validate: target must be a workflow handler type — StartLinked is only for workflows.
        // For VO targets, LinkServiceCommand (Phase A) is the right tool.
        let target_ty = self.entry.request.invocation_target.invocation_target_ty();
        if !matches!(
            target_ty,
            InvocationTargetType::Workflow(WorkflowHandlerType::Workflow)
        ) {
            self.then_apply_completion(StartLinkedCompletion {
                completion_id: self.entry.link_completion_id,
                result: StartLinkedResult::Failure(
                    InvocationError::new(
                        400u16,
                        "StartLinked target must be a workflow run handler",
                    )
                    .into(),
                ),
            });
            return Ok(());
        }

        // Compute child entity id — always WorkflowInvocation for a workflow target
        let child_entity_id = EntityId::WorkflowInvocation(self.entry.request.invocation_id);

        // Build parent entity id and optional ServiceCompletion sink based on parent type.
        // VO parents with result_completion_handler get a ServiceCompletion sink on the child's response_sinks.
        // WI parents do not register completion sinks — they await directly via AttachLinkCommand.
        //
        // Retention on the sink is inherited from the parent's invocation metadata so the
        // spawned handler invocation is retained per the parent's policy whenever the sink fires.
        let (parent_entity_id, response_sink) = match invocation_metadata
            .invocation_target
            .invocation_target_ty()
        {
            InvocationTargetType::VirtualObject(_) => {
                let sink = self
                    .entry
                    .result_completion_handler
                    .clone()
                    .map(|handler_name| {
                        ServiceInvocationResponseSink::ServiceCompletion(ServiceCompletionTarget {
                            service_id: parent_service_id.clone(),
                            handler_name,
                            completion_retention_duration: invocation_metadata
                                .completion_retention_duration,
                            journal_retention_duration: invocation_metadata
                                .journal_retention_duration,
                        })
                    });
                (EntityId::Object(parent_service_id.clone()), sink)
            }
            InvocationTargetType::Workflow(_) => {
                (EntityId::WorkflowInvocation(self.invocation_id), None)
            }
            InvocationTargetType::Service => {
                // Can't happen since we validated as_keyed_service_id above
                warn!(
                    "Unexpected non-keyed parent service type for entry {}",
                    self.entry.ty()
                );
                return Ok(());
            }
        };

        // Validate no self-link: a keyed service cannot link to itself
        if parent_entity_id == child_entity_id {
            self.then_apply_completion(StartLinkedCompletion {
                completion_id: self.entry.link_completion_id,
                result: StartLinkedResult::Failure(
                    InvocationError::new(400u16, "cannot link a service to itself").into(),
                ),
            });
            return Ok(());
        }

        // Write LinkedTo(Active) edge to the appropriate table
        match &parent_entity_id {
            EntityId::Object(sid) => {
                ctx.storage
                    .put_service_edge(
                        sid,
                        &child_entity_id,
                        &EdgeState::LinkedTo(LinkStatus::Active),
                    )
                    .map_err(Error::Storage)?;
            }
            EntityId::WorkflowInvocation(_) => {
                ctx.storage
                    .put_invocation_edge(
                        &self.invocation_id,
                        &child_entity_id,
                        &EdgeState::LinkedTo(LinkStatus::Active),
                    )
                    .map_err(Error::Storage)?;
            }
        }

        // Build the child ServiceInvocation.
        // - link_from: carries parent entity so child-side on_link_from_invocation inserts
        //   notification sinks and increments linked_from_count
        // - link_caller_completion_id: routes LinkResponse to parent's journal entry
        // - response_sink: Some(ServiceCompletion) for VO parents with a handler, None otherwise
        let restate_types::journal_v2::command::CallRequest {
            invocation_id,
            invocation_target,
            span_context,
            parameter,
            headers,
            idempotency_key,
            completion_retention_duration,
            journal_retention_duration,
        } = self.entry.request;

        let service_invocation = ServiceInvocation {
            argument: parameter,
            headers,
            response_sink,
            span_context: span_context.clone(),
            execution_time: None,
            completion_retention_duration,
            journal_retention_duration,
            idempotency_key,
            link_from: Some(parent_entity_id),
            link_caller_completion_id: Some(self.entry.link_completion_id),
            ..ServiceInvocation::initialize(
                invocation_id,
                invocation_target,
                Source::Service(
                    self.invocation_id,
                    invocation_metadata.invocation_target.clone(),
                ),
            )
        };

        // Increment linked_to_count on the parent — records that this invocation has linked
        // to one more child. Used by end_invocation to gate the Completing lifecycle check.
        if let Some(count) = self.invocation_status.linked_to_count_mut() {
            *count += 1;
        }

        ctx.handle_outgoing_message(OutboxMessage::ServiceInvocation(Box::new(
            service_invocation,
        )))?;

        // Do NOT emit a completion here — the completion arrives asynchronously via
        // LinkResponse when the child partition confirms or rejects the link.

        Ok(())
    }
}

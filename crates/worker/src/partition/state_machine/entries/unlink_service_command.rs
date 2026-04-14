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
use restate_types::identifiers::ServiceId;
use restate_types::invocation::{
    EdgeLabel, EntityId, InvocationTargetType, UnlinkRequest, WorkflowHandlerType,
};
use restate_types::journal_v2::{EntryMetadata, UnlinkInvocationCommand, UnlinkServiceCommand};

use crate::partition::state_machine::entries::ApplyJournalCommandEffect;
use crate::partition::state_machine::{CommandHandler, Error, StateMachineApplyContext};

pub(super) type ApplyUnlinkServiceCommand<'e> = ApplyJournalCommandEffect<'e, UnlinkServiceCommand>;
pub(super) type ApplyUnlinkInvocationCommand<'e> =
    ApplyJournalCommandEffect<'e, UnlinkInvocationCommand>;

/// Shared logic for both UnlinkService and UnlinkInvocation commands.
/// Determines the parent entity, deletes the parent's LinkedTo edge,
/// and enqueues an UnlinkRequest to the child's partition.
fn apply_unlink<S>(
    ctx: &mut StateMachineApplyContext<'_, S>,
    invocation_id: restate_types::identifiers::InvocationId,
    invocation_status: &restate_storage_api::invocation_status_table::InvocationStatus,
    linked_to: EntityId,
    unlink_completion_id: restate_types::journal_v2::CompletionId,
    entry_ty: restate_types::journal_v2::EntryType,
) -> Result<(), Error>
where
    S: WriteServiceEdgesTable + WriteInvocationEdgesTable + WriteOutboxTable + WriteFsmTable,
{
    let invocation_metadata = invocation_status
        .get_invocation_metadata()
        .expect("In-Flight invocation metadata must be present");

    let Some(linked_from) = invocation_metadata.invocation_target.as_keyed_service_id() else {
        warn!(
            "Trying to process entry {} for a target that is not a keyed service",
            entry_ty
        );
        return Ok(());
    };

    // Determine parent entity based on caller type
    let caller_target_ty = invocation_metadata.invocation_target.invocation_target_ty();
    let linked_from_entity = match caller_target_ty {
        InvocationTargetType::Workflow(WorkflowHandlerType::Workflow) => {
            EntityId::WorkflowInvocation(invocation_id)
        }
        _ => EntityId::Object(linked_from.clone()),
    };

    // Delete the LinkedTo edge from parent to child
    match &linked_from_entity {
        EntityId::Object(sid) => {
            ctx.storage
                .delete_service_edge(sid, EdgeLabel::LinkedTo, &linked_to)
                .map_err(Error::Storage)?;
        }
        EntityId::WorkflowInvocation(_) => {
            ctx.storage
                .delete_invocation_edge(&invocation_id, EdgeLabel::LinkedTo, &linked_to)
                .map_err(Error::Storage)?;
        }
    }

    // Enqueue UnlinkRequest to child's partition — carries parent so the child can
    // decrement linked_from_count and remove notification sinks. The completion_id allows
    // the child to send an UnlinkResponse.
    ctx.handle_outgoing_message(OutboxMessage::UnlinkRequest(UnlinkRequest {
        linked_to,
        linked_from: linked_from_entity,
        caller_invocation_id: invocation_id,
        caller_completion_id: Some(unlink_completion_id),
    }))?;

    Ok(())
}

impl<'e, 'ctx: 'e, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for ApplyUnlinkServiceCommand<'e>
where
    S: WriteServiceEdgesTable + WriteInvocationEdgesTable + WriteOutboxTable + WriteFsmTable,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        let linked_to = EntityId::Object(ServiceId::new(
            self.entry.unlink_from.service_name.clone(),
            self.entry.unlink_from.key.clone(),
        ));
        // Decrement linked_to_count on the parent — the LinkedTo edge is about to be deleted.
        if let Some(count) = self.invocation_status.linked_to_count_mut() {
            *count = count.saturating_sub(1);
        }
        apply_unlink(
            ctx,
            self.invocation_id,
            self.invocation_status,
            linked_to,
            self.entry.unlink_completion_id,
            self.entry.ty(),
        )
    }
}

impl<'e, 'ctx: 'e, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for ApplyUnlinkInvocationCommand<'e>
where
    S: WriteServiceEdgesTable + WriteInvocationEdgesTable + WriteOutboxTable + WriteFsmTable,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        let linked_to = EntityId::WorkflowInvocation(self.entry.unlink_from);
        // Decrement linked_to_count on the parent — the LinkedTo edge is about to be deleted.
        if let Some(count) = self.invocation_status.linked_to_count_mut() {
            *count = count.saturating_sub(1);
        }
        apply_unlink(
            ctx,
            self.invocation_id,
            self.invocation_status,
            linked_to,
            self.entry.unlink_completion_id,
            self.entry.ty(),
        )
    }
}

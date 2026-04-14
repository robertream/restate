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
use restate_storage_api::outbox_table::WriteOutboxTable;
use restate_storage_api::service_edges_table::ReadServiceEdgesTable;
use restate_storage_api::service_status_table::{
    ReadVirtualObjectStatusTable, VirtualObjectStatus, WriteVirtualObjectStatusTable,
};
use restate_types::errors::SERVICE_COMPLETED_INVOCATION_ERROR;
use restate_types::invocation::{
    EdgeState, EntityId, InvocationTargetType, LinkStatus, VirtualObjectHandlerType,
};
use restate_types::journal_v2::{
    CompleteServiceCommand, CompleteServiceCompletion, CompleteServiceResult, EntryMetadata,
};

use crate::partition::state_machine::entries::ApplyJournalCommandEffect;
use crate::partition::state_machine::{CommandHandler, Error, StateMachineApplyContext};

pub(super) type ApplyCompleteServiceCommand<'e> =
    ApplyJournalCommandEffect<'e, CompleteServiceCommand>;

impl<'e, 'ctx: 'e, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for ApplyCompleteServiceCommand<'e>
where
    S: ReadVirtualObjectStatusTable
        + WriteVirtualObjectStatusTable
        + ReadServiceEdgesTable
        + WriteOutboxTable
        + WriteFsmTable,
{
    async fn apply(mut self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        let invocation_metadata = self
            .invocation_status
            .get_invocation_metadata()
            .expect("In-Flight invocation metadata must be present");

        let Some(service_id) = invocation_metadata.invocation_target.as_keyed_service_id() else {
            warn!(
                "Trying to process entry {} for a target that is not a keyed service",
                self.entry.ty()
            );
            return Ok(());
        };

        // Validate not called from a shared handler
        if matches!(
            invocation_metadata.invocation_target.invocation_target_ty(),
            InvocationTargetType::VirtualObject(VirtualObjectHandlerType::Shared)
        ) {
            self.then_apply_completion(CompleteServiceCompletion {
                completion_id: self.entry.completion_id,
                result: CompleteServiceResult::Failure(
                    restate_types::errors::InvocationError::new(
                        400u16,
                        "CompleteService cannot be called from a shared handler",
                    )
                    .into(),
                ),
            });
            return Ok(());
        }

        // Validate not already completed
        let current_status = ctx.storage.get_virtual_object_status(&service_id).await?;
        if matches!(current_status, VirtualObjectStatus::Completed { .. }) {
            self.then_apply_completion(CompleteServiceCompletion {
                completion_id: self.entry.completion_id,
                result: CompleteServiceResult::Failure(SERVICE_COMPLETED_INVOCATION_ERROR.into()),
            });
            return Ok(());
        }

        // Guard: reject if any LinkedTo children are still active.
        // We only check ServiceEdges here because CompleteService is VO-only: the caller must
        // be a VO exclusive handler (validated above), so InvocationEdges are never written for
        // VO nodes.
        let children = ctx.storage.get_service_linked_to(&service_id).await?;
        let has_active_children = children
            .iter()
            .any(|(_, edge_state)| matches!(edge_state, EdgeState::LinkedTo(LinkStatus::Active)));
        if has_active_children {
            self.then_apply_completion(CompleteServiceCompletion {
                completion_id: self.entry.completion_id,
                result: CompleteServiceResult::Failure(
                    restate_types::errors::InvocationError::new(
                        409u16,
                        "cannot complete service with active linked children",
                    )
                    .into(),
                ),
            });
            return Ok(());
        }

        let result = self.entry.result.clone();

        // Drain response_sinks and fire them before transitioning to Completed.
        // Extracts from current status (Locked or Unlocked — both have response_sinks).
        let current_linked_from_count = current_status.linked_from_count();
        let response_sinks = match current_status {
            VirtualObjectStatus::Locked { response_sinks, .. } => response_sinks,
            VirtualObjectStatus::Unlocked { response_sinks, .. } => response_sinks,
            VirtualObjectStatus::Completed { .. } => {
                unreachable!("already handled above")
            }
        };
        // Retention for any spawned ServiceCompletion handler invocations is carried on the
        // sink target itself, inherited from the linker parent at link time.
        // completing_entity is this VO itself — ServiceLinkNotification sinks use it as `remote`.
        ctx.send_response_to_sinks(
            response_sinks.into_iter(),
            result.clone(),
            None,
            None,
            Some(&invocation_metadata.invocation_target),
            Some(EntityId::Object(service_id.clone())),
        )?;

        // Set VirtualObjectStatus::Completed — sinks are implicitly dropped (terminal state)
        ctx.storage
            .put_virtual_object_status(
                &service_id,
                &VirtualObjectStatus::Completed {
                    result: result.clone(),
                    linked_from_count: current_linked_from_count,
                },
            )
            .map_err(Error::Storage)?;

        // Deliver success completion to SDK
        self.then_apply_completion(CompleteServiceCompletion {
            completion_id: self.entry.completion_id,
            result: CompleteServiceResult::Void,
        });

        Ok(())
    }
}

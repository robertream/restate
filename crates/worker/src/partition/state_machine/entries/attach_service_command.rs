// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_storage_api::fsm_table::WriteFsmTable;
use restate_storage_api::outbox_table::{OutboxMessage, WriteOutboxTable};

use restate_types::invocation::AttachServiceRequest;
use restate_types::journal_v2::AttachServiceCommand;

use crate::partition::state_machine::entries::ApplyJournalCommandEffect;
use crate::partition::state_machine::{CommandHandler, Error, StateMachineApplyContext};

pub(super) type ApplyAttachServiceCommand<'e> = ApplyJournalCommandEffect<'e, AttachServiceCommand>;

impl<'e, 'ctx: 'e, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for ApplyAttachServiceCommand<'e>
where
    S: WriteOutboxTable + WriteFsmTable,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        // AttachService is unrestricted: any caller may attach to any known VO handle.
        // Authorization is the SDK's responsibility.
        ctx.handle_outgoing_message(OutboxMessage::AttachServiceRequest(AttachServiceRequest {
            caller_id: self.invocation_id,
            completion_id: self.entry.completion_id,
            target: self.entry.attach_to.clone(),
        }))?;

        Ok(())
    }
}

// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;
use restate_types::identifiers::{InvocationId, PartitionProcessorRpcRequestId, WithPartitionKey};
use restate_types::invocation::NotifySignalRequest;
use restate_types::journal_v2::Signal;
use restate_types::net::partition_processor::PartitionProcessorRpcResponse;
use restate_wal_protocol::Command;

pub(super) struct Request {
    pub(super) request_id: PartitionProcessorRpcRequestId,
    pub(super) invocation_id: InvocationId,
    pub(super) signal: Signal,
}

impl<'a, TActuator: Actuator, TSchemas, TStorage> RpcHandler<Request>
    for RpcContext<'a, TActuator, TSchemas, TStorage>
{
    type Output = PartitionProcessorRpcResponse;
    type Error = ();

    async fn handle(
        self,
        Request {
            request_id,
            invocation_id,
            signal,
        }: Request,
        replier: Replier<Self::Output>,
    ) -> Result<(), Self::Error> {
        self.proposer
            .handle_rpc_proposal_command(
                invocation_id.partition_key(),
                Command::NotifySignal(NotifySignalRequest {
                    invocation_id,
                    signal,
                    request_id: Some(request_id),
                }),
                request_id,
                replier,
            )
            .await;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::FutureExt;
    use restate_test_util::let_assert;
    use restate_types::identifiers::InvocationUuid;
    use restate_types::journal_v2::{SignalId, SignalResult};
    use std::future::ready;
    use test_log::test;

    use crate::partition::rpc::MockActuator;

    #[test(restate_core::test)]
    async fn issue_4566_uses_handle_rpc_proposal_command() {
        let request_id = PartitionProcessorRpcRequestId::new();
        let invocation_id = InvocationId::from_parts(42, InvocationUuid::from_u128(1));

        let mut proposer = MockActuator::new();
        proposer
            .expect_self_propose_and_respond_asynchronously::<PartitionProcessorRpcResponse>()
            .never();
        proposer
            .expect_handle_rpc_proposal_command::<PartitionProcessorRpcResponse>()
            .return_once_st(move |_partition_key, cmd, req_id, _replier| {
                assert_eq!(req_id, request_id);
                let_assert!(Command::NotifySignal(notify_signal) = cmd);
                assert_eq!(notify_signal.request_id, Some(request_id));
                ready(()).boxed()
            });

        let (tx, _rx) = Reciprocal::mock();
        RpcHandler::handle(
            RpcContext::new(&mut proposer, &(), &mut ()),
            Request {
                request_id,
                invocation_id,
                signal: Signal::new(SignalId::for_index(0), SignalResult::Void),
            },
            Replier::new(tx),
        )
        .await
        .unwrap();
    }
}

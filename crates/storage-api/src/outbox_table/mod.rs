// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::RangeInclusive;

use restate_types::identifiers::{PartitionKey, WithPartitionKey};
use restate_types::invocation::{
    AttachInvocationRequest, AttachServiceRequest, InvocationResponse, InvocationTermination,
    LinkCompletionNotification, LinkRequest, LinkResponse, NotifySignalRequest, ServiceInvocation,
    UnlinkRequest, UnlinkResponse,
};

use crate::Result;
use crate::protobuf_types::PartitionStoreProtobufValue;

/// Types of outbox messages.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum OutboxMessage {
    /// Service invocation to send to another partition processor
    ServiceInvocation(Box<ServiceInvocation>),

    /// Service response to sent to another partition processor
    ServiceResponse(InvocationResponse),

    /// Terminate invocation to send to another partition processor
    InvocationTermination(InvocationTermination),

    /// Attach invocation
    AttachInvocation(AttachInvocationRequest),

    /// Notify signal request
    NotifySignal(NotifySignalRequest),

    /// Link request: parent partition → child partition to register notification sinks
    LinkRequest(LinkRequest),

    /// Link response: child partition → parent partition to confirm/reject
    LinkResponse(LinkResponse),

    /// Unlink request: parent partition → child partition to remove notification sinks
    UnlinkRequest(UnlinkRequest),

    /// Unlink response: child partition → parent partition to acknowledge unlink
    UnlinkResponse(UnlinkResponse),

    /// Notify a parent partition that a child entity completed (unified sink-based notification)
    LinkCompletionNotification(LinkCompletionNotification),

    /// Attach service request: parent → child VO partition to add a completion sink
    AttachServiceRequest(AttachServiceRequest),
}

impl PartitionStoreProtobufValue for OutboxMessage {
    type ProtobufType = crate::protobuf_types::v1::OutboxMessage;
}

impl WithPartitionKey for OutboxMessage {
    fn partition_key(&self) -> PartitionKey {
        match self {
            OutboxMessage::ServiceInvocation(si) => si.partition_key(),
            OutboxMessage::ServiceResponse(sr) => sr.partition_key(),
            OutboxMessage::InvocationTermination(it) => it.invocation_id.partition_key(),
            OutboxMessage::AttachInvocation(ai) => ai.partition_key(),
            OutboxMessage::NotifySignal(sig) => sig.partition_key(),
            OutboxMessage::LinkRequest(req) => req.partition_key(),
            OutboxMessage::LinkResponse(resp) => resp.partition_key(),
            OutboxMessage::UnlinkRequest(req) => req.partition_key(),
            OutboxMessage::UnlinkResponse(resp) => resp.partition_key(),
            OutboxMessage::LinkCompletionNotification(notif) => notif.partition_key(),
            OutboxMessage::AttachServiceRequest(req) => req.partition_key(),
        }
    }
}

pub trait ReadOutboxTable {
    fn get_outbox_head_seq_number(&mut self) -> impl Future<Output = Result<Option<u64>>> + Send;

    fn get_next_outbox_message(
        &mut self,
        next_sequence_number: u64,
    ) -> impl Future<Output = Result<Option<(u64, OutboxMessage)>>> + Send;

    fn get_outbox_message(
        &mut self,
        sequence_number: u64,
    ) -> impl Future<Output = Result<Option<OutboxMessage>>> + Send;
}

pub trait WriteOutboxTable {
    fn put_outbox_message(
        &mut self,
        message_index: u64,
        outbox_message: &OutboxMessage,
    ) -> Result<()>;

    fn truncate_outbox(&mut self, range: RangeInclusive<u64>) -> Result<()>;
}

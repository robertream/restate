// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::Future;

use restate_types::identifiers::ServiceId;
use restate_types::invocation::{EdgeLabel, EdgeState, EntityId};

use crate::Result;
use crate::protobuf_types::PartitionStoreProtobufValue;

/// Type tag for an entity in an edge key (used in key encoding).
pub mod entity_type {
    pub const OBJECT: u8 = 0x00;
    pub const WORKFLOW_INVOCATION: u8 = 0x01;
}

impl PartitionStoreProtobufValue for EdgeState {
    type ProtobufType = crate::protobuf_types::v1::ServiceEdgeState;
}

pub trait ReadServiceEdgesTable {
    /// Point read: get the edge value for a specific (service, label, entity) key.
    fn get_service_edge(
        &mut self,
        service_id: &ServiceId,
        label: EdgeLabel,
        entity: &EntityId,
    ) -> impl Future<Output = Result<Option<EdgeState>>> + Send;

    /// Prefix scan: all `LinkedTo` children of `service_id` with their edge values.
    fn get_service_linked_to(
        &mut self,
        service_id: &ServiceId,
    ) -> impl Future<Output = Result<Vec<(EntityId, EdgeState)>>> + Send;
}

pub trait WriteServiceEdgesTable {
    /// Write an edge. The edge label byte is derived from `value.edge_label()`.
    fn put_service_edge(
        &mut self,
        service_id: &ServiceId,
        entity: &EntityId,
        value: &EdgeState,
    ) -> Result<()>;

    /// Delete a single edge.
    fn delete_service_edge(
        &mut self,
        service_id: &ServiceId,
        label: EdgeLabel,
        entity: &EntityId,
    ) -> Result<()>;

    /// Bulk delete all edges for a node (used during GC).
    fn delete_all_service_edges(&mut self, service_id: &ServiceId) -> Result<()>;
}

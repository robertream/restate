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

use restate_types::identifiers::InvocationId;
use restate_types::invocation::{EdgeLabel, EdgeState, EntityId};

use crate::Result;

/// Type tag for an entity in an edge key (used in key encoding).
/// Shared with [`crate::service_edges_table::entity_type`] — canonical definition lives there.
pub use crate::service_edges_table::entity_type;

/// Read access to the InvocationEdges table.
///
/// Mirrors [`crate::service_edges_table::ReadServiceEdgesTable`] but keyed by
/// [`InvocationId`] instead of [`ServiceId`]. Used for WI parents and WI children
/// in the linked-services graph.
pub trait ReadInvocationEdgesTable {
    /// Point read: get the edge value for a specific (invocation, label, entity) key.
    fn get_invocation_edge(
        &mut self,
        invocation_id: &InvocationId,
        label: EdgeLabel,
        entity: &EntityId,
    ) -> impl Future<Output = Result<Option<EdgeState>>> + Send;

    /// Prefix scan: all `LinkedTo` children of `invocation_id` with their edge values.
    fn get_invocation_linked_to(
        &mut self,
        invocation_id: &InvocationId,
    ) -> impl Future<Output = Result<Vec<(EntityId, EdgeState)>>> + Send;
}

/// Write access to the InvocationEdges table.
///
/// Mirrors [`crate::service_edges_table::WriteServiceEdgesTable`] but keyed by
/// [`InvocationId`]. The edge label is derived from `value.edge_label()`.
pub trait WriteInvocationEdgesTable {
    /// Write an edge. The edge label byte is derived from `value.edge_label()`.
    fn put_invocation_edge(
        &mut self,
        invocation_id: &InvocationId,
        entity: &EntityId,
        value: &EdgeState,
    ) -> Result<()>;

    /// Delete a single edge.
    fn delete_invocation_edge(
        &mut self,
        invocation_id: &InvocationId,
        label: EdgeLabel,
        entity: &EntityId,
    ) -> Result<()>;

    /// Bulk delete all edges for an invocation (used during GC / completion cleanup).
    fn delete_all_invocation_edges(&mut self, invocation_id: &InvocationId) -> Result<()>;
}

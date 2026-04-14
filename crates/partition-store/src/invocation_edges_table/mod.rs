// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::Bytes;

use restate_rocksdb::RocksDbPerfGuard;
use restate_storage_api::Result;
use restate_storage_api::invocation_edges_table::{
    ReadInvocationEdgesTable, WriteInvocationEdgesTable,
};
use restate_storage_api::protobuf_types::PartitionStoreProtobufValue;
use restate_types::identifiers::{InvocationId, InvocationUuid, WithPartitionKey};
use restate_types::invocation::{EdgeLabel, EdgeState, EntityId};

use crate::edge_encoding::{decode_entity, encode_entity};
use crate::keys::{KeyKind, TableKey, define_table_key};
use crate::scan::TableScan;
use crate::{
    PartitionStore, PartitionStoreTransaction, StorageAccess, TableKind, TableScanIterationDecision,
};

define_table_key!(
    TableKind::InvocationEdges,
    KeyKind::InvocationEdges,
    InvocationEdgesKey(
        partition_key: u64,
        invocation_uuid: InvocationUuid,
        edge_label: u8,
        entity_type: u8,
        entity_key: Bytes
    )
);

fn edge_key(
    invocation_id: &InvocationId,
    label: EdgeLabel,
    entity: &EntityId,
) -> InvocationEdgesKey {
    let (rtype, rkey) = encode_entity(entity);
    InvocationEdgesKey {
        partition_key: invocation_id.partition_key(),
        invocation_uuid: invocation_id.invocation_uuid(),
        edge_label: label.number(),
        entity_type: rtype,
        entity_key: rkey,
    }
}

fn edge_prefix_for_label(
    invocation_id: &InvocationId,
    label: EdgeLabel,
) -> InvocationEdgesKeyBuilder {
    InvocationEdgesKey::builder()
        .partition_key(invocation_id.partition_key())
        .invocation_uuid(invocation_id.invocation_uuid())
        .edge_label(label.number())
}

fn edge_prefix_for_invocation(invocation_id: &InvocationId) -> InvocationEdgesKeyBuilder {
    InvocationEdgesKey::builder()
        .partition_key(invocation_id.partition_key())
        .invocation_uuid(invocation_id.invocation_uuid())
}

fn get_invocation_edge_inner<S: StorageAccess>(
    storage: &mut S,
    invocation_id: &InvocationId,
    label: EdgeLabel,
    entity: &EntityId,
) -> Result<Option<EdgeState>> {
    let _x = RocksDbPerfGuard::new("get-invocation-edge");
    storage.get_value_proto(edge_key(invocation_id, label, entity))
}

fn put_invocation_edge_inner<S: StorageAccess>(
    storage: &mut S,
    invocation_id: &InvocationId,
    entity: &EntityId,
    value: &EdgeState,
) -> Result<()> {
    storage.put_kv_proto(edge_key(invocation_id, value.edge_label(), entity), value)
}

fn delete_invocation_edge_inner<S: StorageAccess>(
    storage: &mut S,
    invocation_id: &InvocationId,
    label: EdgeLabel,
    entity: &EntityId,
) -> Result<()> {
    storage.delete_key(&edge_key(invocation_id, label, entity))
}

fn delete_all_invocation_edges_inner<S: StorageAccess>(
    storage: &mut S,
    invocation_id: &InvocationId,
) -> Result<()> {
    let prefix = edge_prefix_for_invocation(invocation_id);

    let keys = storage.for_each_key_value_in_place(
        TableScan::SinglePartitionKeyPrefix(invocation_id.partition_key(), prefix),
        |k, _| TableScanIterationDecision::Emit(Ok(Box::from(k))),
    )?;

    for k in keys {
        let key = k?;
        storage.delete_cf(TableKind::InvocationEdges, key)?;
    }
    Ok(())
}

fn decode_edge_key_value(k: &[u8], v: &[u8]) -> Result<(EntityId, EdgeState)> {
    let key = InvocationEdgesKey::deserialize_from(&mut Bytes::copy_from_slice(k))?;
    let state = EdgeState::decode(&mut &v[..])?;
    let entity = decode_entity(key.entity_type, key.entity_key)?;
    Ok((entity, state))
}

fn get_invocation_linked_to_inner<S: StorageAccess>(
    storage: &mut S,
    invocation_id: &InvocationId,
) -> Result<Vec<(EntityId, EdgeState)>> {
    let _x = RocksDbPerfGuard::new("get-invocation-linked-to");
    let prefix = edge_prefix_for_label(invocation_id, EdgeLabel::LinkedTo);

    storage
        .for_each_key_value_in_place(
            TableScan::SinglePartitionKeyPrefix(invocation_id.partition_key(), prefix),
            |k, v| TableScanIterationDecision::Emit(decode_edge_key_value(k, v)),
        )?
        .into_iter()
        .collect()
}

// PartitionStore (read-only) implementations

impl ReadInvocationEdgesTable for PartitionStore {
    async fn get_invocation_edge(
        &mut self,
        invocation_id: &InvocationId,
        label: EdgeLabel,
        entity: &EntityId,
    ) -> Result<Option<EdgeState>> {
        self.assert_partition_key(invocation_id)?;
        get_invocation_edge_inner(self, invocation_id, label, entity)
    }

    async fn get_invocation_linked_to(
        &mut self,
        invocation_id: &InvocationId,
    ) -> Result<Vec<(EntityId, EdgeState)>> {
        self.assert_partition_key(invocation_id)?;
        get_invocation_linked_to_inner(self, invocation_id)
    }
}

// PartitionStoreTransaction implementations

impl ReadInvocationEdgesTable for PartitionStoreTransaction<'_> {
    async fn get_invocation_edge(
        &mut self,
        invocation_id: &InvocationId,
        label: EdgeLabel,
        entity: &EntityId,
    ) -> Result<Option<EdgeState>> {
        self.assert_partition_key(invocation_id)?;
        get_invocation_edge_inner(self, invocation_id, label, entity)
    }

    async fn get_invocation_linked_to(
        &mut self,
        invocation_id: &InvocationId,
    ) -> Result<Vec<(EntityId, EdgeState)>> {
        self.assert_partition_key(invocation_id)?;
        get_invocation_linked_to_inner(self, invocation_id)
    }
}

impl WriteInvocationEdgesTable for PartitionStoreTransaction<'_> {
    fn put_invocation_edge(
        &mut self,
        invocation_id: &InvocationId,
        entity: &EntityId,
        value: &EdgeState,
    ) -> Result<()> {
        self.assert_partition_key(invocation_id)?;
        put_invocation_edge_inner(self, invocation_id, entity, value)
    }

    fn delete_invocation_edge(
        &mut self,
        invocation_id: &InvocationId,
        label: EdgeLabel,
        entity: &EntityId,
    ) -> Result<()> {
        self.assert_partition_key(invocation_id)?;
        delete_invocation_edge_inner(self, invocation_id, label, entity)
    }

    fn delete_all_invocation_edges(&mut self, invocation_id: &InvocationId) -> Result<()> {
        self.assert_partition_key(invocation_id)?;
        delete_all_invocation_edges_inner(self, invocation_id)
    }
}

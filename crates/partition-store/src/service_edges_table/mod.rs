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
use restate_storage_api::protobuf_types::PartitionStoreProtobufValue;
use restate_storage_api::service_edges_table::{ReadServiceEdgesTable, WriteServiceEdgesTable};
use restate_types::identifiers::{ServiceId, WithPartitionKey};
use restate_types::invocation::{EdgeLabel, EdgeState, EntityId};

use crate::edge_encoding::{decode_entity, encode_entity};
use crate::keys::{KeyKind, TableKey, define_table_key};
use crate::scan::TableScan;
use crate::{
    PartitionStore, PartitionStoreTransaction, StorageAccess, TableKind, TableScanIterationDecision,
};

define_table_key!(
    TableKind::ServiceEdges,
    KeyKind::ServiceEdges,
    ServiceEdgesKey(
        partition_key: u64,
        service_name: bytestring::ByteString,
        service_key: bytestring::ByteString,
        edge_label: u8,
        entity_type: u8,
        entity_key: Bytes
    )
);

fn edge_key(service_id: &ServiceId, label: EdgeLabel, entity: &EntityId) -> ServiceEdgesKey {
    let (rtype, rkey) = encode_entity(entity);
    ServiceEdgesKey {
        partition_key: service_id.partition_key(),
        service_name: service_id.service_name.clone(),
        service_key: service_id.key.clone(),
        edge_label: label.number(),
        entity_type: rtype,
        entity_key: rkey,
    }
}

fn edge_prefix_for_label(service_id: &ServiceId, label: EdgeLabel) -> ServiceEdgesKeyBuilder {
    ServiceEdgesKey::builder()
        .partition_key(service_id.partition_key())
        .service_name(service_id.service_name.clone())
        .service_key(service_id.key.clone())
        .edge_label(label.number())
}

fn edge_prefix_for_service(service_id: &ServiceId) -> ServiceEdgesKeyBuilder {
    ServiceEdgesKey::builder()
        .partition_key(service_id.partition_key())
        .service_name(service_id.service_name.clone())
        .service_key(service_id.key.clone())
}

fn get_service_edge_inner<S: StorageAccess>(
    storage: &mut S,
    service_id: &ServiceId,
    label: EdgeLabel,
    entity: &EntityId,
) -> Result<Option<EdgeState>> {
    let _x = RocksDbPerfGuard::new("get-service-edge");
    storage.get_value_proto(edge_key(service_id, label, entity))
}

fn put_service_edge_inner<S: StorageAccess>(
    storage: &mut S,
    service_id: &ServiceId,
    entity: &EntityId,
    value: &EdgeState,
) -> Result<()> {
    storage.put_kv_proto(edge_key(service_id, value.edge_label(), entity), value)
}

fn delete_service_edge_inner<S: StorageAccess>(
    storage: &mut S,
    service_id: &ServiceId,
    label: EdgeLabel,
    entity: &EntityId,
) -> Result<()> {
    storage.delete_key(&edge_key(service_id, label, entity))
}

fn delete_all_service_edges_inner<S: StorageAccess>(
    storage: &mut S,
    service_id: &ServiceId,
) -> Result<()> {
    let prefix = edge_prefix_for_service(service_id);

    let keys = storage.for_each_key_value_in_place(
        TableScan::SinglePartitionKeyPrefix(service_id.partition_key(), prefix),
        |k, _| TableScanIterationDecision::Emit(Ok(Box::from(k))),
    )?;

    for k in keys {
        let key = k?;
        storage.delete_cf(TableKind::ServiceEdges, key)?;
    }
    Ok(())
}

fn decode_edge_key_value(k: &[u8], v: &[u8]) -> Result<(EntityId, EdgeState)> {
    let key = ServiceEdgesKey::deserialize_from(&mut Bytes::copy_from_slice(k))?;
    let state = EdgeState::decode(&mut &v[..])?;
    let entity = decode_entity(key.entity_type, key.entity_key)?;
    Ok((entity, state))
}

fn get_service_linked_to_inner<S: StorageAccess>(
    storage: &mut S,
    service_id: &ServiceId,
) -> Result<Vec<(EntityId, EdgeState)>> {
    let _x = RocksDbPerfGuard::new("get-service-linked-to");
    let prefix = edge_prefix_for_label(service_id, EdgeLabel::LinkedTo);

    storage
        .for_each_key_value_in_place(
            TableScan::SinglePartitionKeyPrefix(service_id.partition_key(), prefix),
            |k, v| TableScanIterationDecision::Emit(decode_edge_key_value(k, v)),
        )?
        .into_iter()
        .collect()
}

// PartitionStore (read-only) implementations

impl ReadServiceEdgesTable for PartitionStore {
    async fn get_service_edge(
        &mut self,
        service_id: &ServiceId,
        label: EdgeLabel,
        entity: &EntityId,
    ) -> Result<Option<EdgeState>> {
        self.assert_partition_key(service_id)?;
        get_service_edge_inner(self, service_id, label, entity)
    }

    async fn get_service_linked_to(
        &mut self,
        service_id: &ServiceId,
    ) -> Result<Vec<(EntityId, EdgeState)>> {
        self.assert_partition_key(service_id)?;
        get_service_linked_to_inner(self, service_id)
    }
}

// PartitionStoreTransaction implementations

impl ReadServiceEdgesTable for PartitionStoreTransaction<'_> {
    async fn get_service_edge(
        &mut self,
        service_id: &ServiceId,
        label: EdgeLabel,
        entity: &EntityId,
    ) -> Result<Option<EdgeState>> {
        self.assert_partition_key(service_id)?;
        get_service_edge_inner(self, service_id, label, entity)
    }

    async fn get_service_linked_to(
        &mut self,
        service_id: &ServiceId,
    ) -> Result<Vec<(EntityId, EdgeState)>> {
        self.assert_partition_key(service_id)?;
        get_service_linked_to_inner(self, service_id)
    }
}

impl WriteServiceEdgesTable for PartitionStoreTransaction<'_> {
    fn put_service_edge(
        &mut self,
        service_id: &ServiceId,
        entity: &EntityId,
        value: &EdgeState,
    ) -> Result<()> {
        self.assert_partition_key(service_id)?;
        put_service_edge_inner(self, service_id, entity, value)
    }

    fn delete_service_edge(
        &mut self,
        service_id: &ServiceId,
        label: EdgeLabel,
        entity: &EntityId,
    ) -> Result<()> {
        self.assert_partition_key(service_id)?;
        delete_service_edge_inner(self, service_id, label, entity)
    }

    fn delete_all_service_edges(&mut self, service_id: &ServiceId) -> Result<()> {
        self.assert_partition_key(service_id)?;
        delete_all_service_edges_inner(self, service_id)
    }
}

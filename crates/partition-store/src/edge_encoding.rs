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

use restate_types::identifiers::{InvocationId, ServiceId};
use restate_types::invocation::EntityId;

use restate_storage_api::StorageError;

const ENTITY_TYPE_OBJECT: u8 = 0;
const ENTITY_TYPE_WORKFLOW_INVOCATION: u8 = 1;

pub(crate) fn encode_entity(entity: &EntityId) -> (u8, Bytes) {
    match entity {
        EntityId::Object(sid) => {
            let mut buf = Vec::new();
            buf.extend_from_slice(sid.service_name.as_bytes());
            buf.push(0); // separator
            buf.extend_from_slice(sid.key.as_bytes());
            (ENTITY_TYPE_OBJECT, Bytes::from(buf))
        }
        EntityId::WorkflowInvocation(iid) => (
            ENTITY_TYPE_WORKFLOW_INVOCATION,
            Bytes::copy_from_slice(&iid.to_bytes()),
        ),
    }
}

pub(crate) fn decode_entity(entity_type: u8, entity_key: Bytes) -> Result<EntityId, StorageError> {
    match entity_type {
        ENTITY_TYPE_OBJECT => {
            let bytes = entity_key.as_ref();
            let sep = bytes.iter().position(|&b| b == 0).ok_or_else(|| {
                StorageError::Generic(anyhow::anyhow!("missing separator in edge key"))
            })?;
            let service_name =
                std::str::from_utf8(&bytes[..sep]).map_err(|e| StorageError::Generic(e.into()))?;
            let key = std::str::from_utf8(&bytes[sep + 1..])
                .map_err(|e| StorageError::Generic(e.into()))?;
            Ok(EntityId::Object(ServiceId::new(service_name, key)))
        }
        ENTITY_TYPE_WORKFLOW_INVOCATION => {
            let iid = InvocationId::from_slice(&entity_key)
                .map_err(|e| StorageError::Generic(e.into()))?;
            Ok(EntityId::WorkflowInvocation(iid))
        }
        other => Err(StorageError::Generic(anyhow::anyhow!(
            "unknown entity type in edge key: {other}"
        ))),
    }
}

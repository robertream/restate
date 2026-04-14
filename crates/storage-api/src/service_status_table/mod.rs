// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::future::Future;

use restate_types::identifiers::{InvocationId, PartitionKey, ServiceId};
use restate_types::invocation::{ResponseResult, ServiceInvocationResponseSink};
use restate_types::sharding::KeyRange;

use crate::Result;
use crate::protobuf_types::PartitionStoreProtobufValue;

/// Status of a virtual object or workflow service instance.
///
/// `Unlocked` and `Locked` hold a `response_sinks` collection for pending completion callbacks.
/// `Completed` is a terminal state — no sinks (they were fired at completion time).
///
/// All variants carry `linked_from_count` — the number of linked parents referencing this entity.
/// Used by `on_unlink_request` to determine when an orphaned completed VO should be GC'd.
#[derive(Debug, Clone, PartialEq)]
pub enum VirtualObjectStatus {
    Locked {
        invocation_id: InvocationId,
        response_sinks: HashSet<ServiceInvocationResponseSink>,
        linked_from_count: u32,
    },
    Completed {
        result: ResponseResult,
        linked_from_count: u32,
    },
    Unlocked {
        response_sinks: HashSet<ServiceInvocationResponseSink>,
        linked_from_count: u32,
    },
}

impl Default for VirtualObjectStatus {
    fn default() -> Self {
        VirtualObjectStatus::Unlocked {
            response_sinks: HashSet::new(),
            linked_from_count: 0,
        }
    }
}

impl VirtualObjectStatus {
    /// Create a new `Locked` status with no sinks.
    pub fn locked(invocation_id: InvocationId) -> Self {
        VirtualObjectStatus::Locked {
            invocation_id,
            response_sinks: HashSet::new(),
            linked_from_count: 0,
        }
    }

    /// Create a new `Unlocked` status with no sinks.
    pub fn unlocked() -> Self {
        VirtualObjectStatus::Unlocked {
            response_sinks: HashSet::new(),
            linked_from_count: 0,
        }
    }

    /// Returns a reference to the response_sinks for non-terminal variants.
    pub fn response_sinks(&self) -> Option<&HashSet<ServiceInvocationResponseSink>> {
        match self {
            Self::Unlocked { response_sinks, .. } | Self::Locked { response_sinks, .. } => {
                Some(response_sinks)
            }
            Self::Completed { .. } => None,
        }
    }

    /// Returns a mutable reference to the response_sinks for non-terminal variants.
    ///
    /// Returns `None` for the `Completed` variant — once a VO is completed its sinks have
    /// already been fired. Callers must handle `None` by skipping the operation.
    pub fn response_sinks_mut(&mut self) -> Option<&mut HashSet<ServiceInvocationResponseSink>> {
        match self {
            Self::Unlocked { response_sinks, .. } | Self::Locked { response_sinks, .. } => {
                Some(response_sinks)
            }
            Self::Completed { .. } => None,
        }
    }

    /// Returns the number of linked parents referencing this entity.
    pub fn linked_from_count(&self) -> u32 {
        match self {
            Self::Locked {
                linked_from_count, ..
            }
            | Self::Unlocked {
                linked_from_count, ..
            }
            | Self::Completed {
                linked_from_count, ..
            } => *linked_from_count,
        }
    }

    /// Returns a mutable reference to the parent count.
    pub fn linked_from_count_mut(&mut self) -> &mut u32 {
        match self {
            Self::Locked {
                linked_from_count, ..
            }
            | Self::Unlocked {
                linked_from_count, ..
            }
            | Self::Completed {
                linked_from_count, ..
            } => linked_from_count,
        }
    }

    /// Returns true if this is the `Completed` variant.
    pub fn is_completed(&self) -> bool {
        matches!(self, Self::Completed { .. })
    }
}

impl PartitionStoreProtobufValue for VirtualObjectStatus {
    type ProtobufType = crate::protobuf_types::v1::VirtualObjectStatus;
}

pub trait ReadVirtualObjectStatusTable {
    fn get_virtual_object_status(
        &mut self,
        service_id: &ServiceId,
    ) -> impl Future<Output = Result<VirtualObjectStatus>> + Send;
}

pub trait ScanVirtualObjectStatusTable {
    fn for_each_virtual_object_status<
        F: FnMut((ServiceId, VirtualObjectStatus)) -> std::ops::ControlFlow<()>
            + Send
            + Sync
            + 'static,
    >(
        &self,
        range: KeyRange,
        f: F,
    ) -> Result<impl Future<Output = Result<()>> + Send>;
}

pub trait WriteVirtualObjectStatusTable {
    fn put_virtual_object_status(
        &mut self,
        service_id: &ServiceId,
        status: &VirtualObjectStatus,
    ) -> Result<()>;

    fn delete_virtual_object_status(&mut self, service_id: &ServiceId) -> Result<()>;
}

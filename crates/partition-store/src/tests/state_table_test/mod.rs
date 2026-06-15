// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::{assert_stream_eq, storage_test_environment};

use crate::PartitionStore;
use crate::partition_store::{StateChangeOperation, SubscriptionRequest};
use bytes::Bytes;
use restate_rocksdb::RocksDbManager;
use restate_storage_api::Transaction;
use restate_storage_api::state_table::{ReadStateTable, WriteStateTable};
use restate_types::identifiers::ServiceId;

fn populate_data<T: WriteStateTable>(table: &mut T) {
    table
        .put_user_state(
            &ServiceId::with_partition_key(1337, "svc-1", "key-1"),
            &Bytes::from_static(b"k1"),
            Bytes::from_static(b"v1"),
        )
        .expect("");

    table
        .put_user_state(
            &ServiceId::with_partition_key(1337, "svc-1", "key-1"),
            &Bytes::from_static(b"k2"),
            Bytes::from_static(b"v2"),
        )
        .unwrap();

    table
        .put_user_state(
            &ServiceId::with_partition_key(1337, "svc-1", "key-2"),
            &Bytes::from_static(b"k2"),
            Bytes::from_static(b"v2"),
        )
        .unwrap();
}

async fn point_lookup<T: ReadStateTable>(table: &mut T) {
    let result = table
        .get_user_state(
            &ServiceId::with_partition_key(1337, "svc-1", "key-1"),
            &Bytes::from_static(b"k1"),
        )
        .await
        .expect("should not fail");

    assert_eq!(result, Some(Bytes::from_static(b"v1")));
}

async fn prefix_scans<T: ReadStateTable>(table: &T) {
    let service_id = &ServiceId::with_partition_key(1337, "svc-1", "key-1");
    let result = table.get_all_user_states_for_service(service_id).unwrap();

    let expected = vec![
        (Bytes::from_static(b"k1"), Bytes::from_static(b"v1")),
        (Bytes::from_static(b"k2"), Bytes::from_static(b"v2")),
    ];

    assert_stream_eq(result, expected).await;
}

fn deletes<T: WriteStateTable>(table: &mut T) {
    table
        .delete_user_state(
            &ServiceId::with_partition_key(1337, "svc-1", "key-1"),
            &Bytes::from_static(b"k2"),
        )
        .unwrap();
}

async fn verify_delete<T: ReadStateTable>(table: &mut T) {
    let result = table
        .get_user_state(
            &ServiceId::with_partition_key(1337, "svc-1", "key-1"),
            &Bytes::from_static(b"k2"),
        )
        .await
        .expect("should not fail");

    assert!(result.is_none());
}

async fn verify_prefix_scan_after_delete<T: ReadStateTable>(table: &T) {
    let service_id = &ServiceId::with_partition_key(1337, "svc-1", "key-1");
    let result = table.get_all_user_states_for_service(service_id).unwrap();

    let expected = vec![(Bytes::from_static(b"k1"), Bytes::from_static(b"v1"))];

    assert_stream_eq(result, expected).await;
}

pub(crate) async fn run_tests(mut rocksdb: PartitionStore) {
    let mut txn = rocksdb.transaction();

    populate_data(&mut txn);
    point_lookup(&mut txn).await;
    prefix_scans(&txn).await;
    deletes(&mut txn);

    txn.commit().await.expect("should not fail");
    drop(txn);

    let mut txn = rocksdb.transaction();
    verify_delete(&mut txn).await;
    verify_prefix_scan_after_delete(&txn).await;
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_all() {
    let mut rocksdb = storage_test_environment().await;

    let mut txn = rocksdb.transaction();

    populate_data(&mut txn);
    txn.commit().await.expect("should not fail");
    drop(txn);

    // Do delete all
    let mut txn = rocksdb.transaction();
    txn.delete_all_user_state(&ServiceId::with_partition_key(1337, "svc-1", "key-1"))
        .unwrap();
    txn.commit().await.expect("should not fail");
    drop(txn);

    // No more state for key-1
    let mut txn = rocksdb.transaction();
    assert_stream_eq(
        txn.get_all_user_states_for_service(&ServiceId::with_partition_key(1337, "svc-1", "key-1"))
            .unwrap(),
        vec![],
    )
    .await;

    // key-2 should be untouched
    assert!(
        txn.get_user_state(
            &ServiceId::with_partition_key(1337, "svc-1", "key-2"),
            &Bytes::from_static(b"k2"),
        )
        .await
        .expect("should not fail")
        .is_some()
    );

    RocksDbManager::get().shutdown().await;
}

/// Verify that state change events are forwarded via the mpsc channel after a commit,
/// and that the receiver correctly reflects the operation type.
#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_state_change_events_via_mpsc() {
    let (manager, mut store) = super::storage_test_environment_with_manager().await;

    let service_id = ServiceId::with_partition_key(1337, "svc-1", "key-1");

    // Subscribe so the commit path forwards events; also sends initial Replace.
    store
        .handle_watch_command(SubscriptionRequest::Subscribe {
            service_id: service_id.clone(),
        })
        .await
        .expect("subscribe should not fail");

    // Take the receiver after subscribing.
    let mut rx = manager
        .take_state_change_receiver(restate_types::identifiers::PartitionId::MIN)
        .expect("receiver should be available");

    // Drain the initial Replace event from Subscribe.
    let replace = tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv())
        .await
        .expect("should not time out")
        .expect("channel should not be closed");
    assert_eq!(replace.service_id, service_id);
    assert!(matches!(
        replace.operation,
        StateChangeOperation::Replace { .. }
    ));

    // Commit a put — expect a Patch event.
    {
        let mut txn = store.transaction();
        txn.put_user_state(&service_id, &Bytes::from_static(b"k1"), b"v1")
            .unwrap();
        txn.commit().await.expect("commit should not fail");
    }

    let event = tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv())
        .await
        .expect("should not time out")
        .expect("channel should not be closed");
    assert_eq!(event.service_id, service_id);
    assert!(matches!(
        event.operation,
        StateChangeOperation::Patch { .. }
    ));

    // Commit a delete_all — expect a ClearAll event.
    {
        let mut txn = store.transaction();
        txn.delete_all_user_state(&service_id).unwrap();
        txn.commit().await.expect("commit should not fail");
    }

    let event = tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv())
        .await
        .expect("should not time out")
        .expect("channel should not be closed");
    assert_eq!(event.service_id, service_id);
    assert!(matches!(event.operation, StateChangeOperation::ClearAll));

    RocksDbManager::get().shutdown().await;
}

/// Verify that Resubscribe with a stale revision emits a Replace event, but a matching
/// revision emits nothing.
#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_resubscribe_revision_mismatch() {
    let (manager, mut store) = super::storage_test_environment_with_manager().await;

    let service_id = ServiceId::with_partition_key(1337, "svc-1", "key-1");

    // Commit 5 transactions to drive the revision to 5.
    for i in 0u8..5 {
        let mut txn = store.transaction();
        txn.put_user_state(&service_id, &Bytes::from_static(b"k1"), [i])
            .unwrap();
        txn.commit().await.expect("commit should not fail");
    }
    // Commit one more transaction with a second key; revision becomes 6.
    {
        let mut txn = store.transaction();
        txn.put_user_state(&service_id, &Bytes::from_static(b"k2"), b"v2")
            .unwrap();
        txn.commit().await.expect("commit should not fail");
    }
    // Revision is now 6; state: k1=4, k2=v2.

    // Take the receiver before subscribing so we capture all events.
    let mut rx = manager
        .take_state_change_receiver(restate_types::identifiers::PartitionId::MIN)
        .expect("receiver should be available");

    // Resubscribe with a stale revision (3) — current is 6 → expect Replace.
    store
        .handle_watch_command(SubscriptionRequest::Resubscribe {
            service_id: service_id.clone(),
            revision: 3,
        })
        .await
        .expect("resubscribe should not fail");

    let event = tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv())
        .await
        .expect("should not time out")
        .expect("channel should not be closed");
    assert_eq!(event.service_id, service_id);
    assert_eq!(event.revision, 6);
    assert!(
        matches!(&event.operation, StateChangeOperation::Replace { state } if state.len() == 2)
    );

    // Resubscribe with current revision (6) — no mismatch → no event.
    store
        .handle_watch_command(SubscriptionRequest::Resubscribe {
            service_id: service_id.clone(),
            revision: 6,
        })
        .await
        .expect("resubscribe should not fail");

    // Give the store a moment to emit anything it might send (it should send nothing).
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(
        rx.try_recv().is_err(),
        "no event expected for matching revision"
    );

    RocksDbManager::get().shutdown().await;
}

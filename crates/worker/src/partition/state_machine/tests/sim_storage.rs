// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, HashMap};
use std::ops::RangeInclusive;

use bytes::Bytes;
use bytestring::ByteString;
use futures::Stream;
use futures::stream;

use restate_memory::{IgnorePinnableMemoryStream, LocalMemoryLease, LocalMemoryPool};
use restate_storage_api::BudgetedReadError;
use restate_storage_api::Result;
use restate_storage_api::deduplication_table::{
    DedupSequenceNumber, ProducerId, ReadDeduplicationTable, WriteDeduplicationTable,
};
use restate_storage_api::fsm_table::{CachedEpochMetadata, PartitionDurability, WriteFsmTable};
use restate_storage_api::idempotency_table::{
    IdempotencyMetadata, IdempotencyTable, ReadOnlyIdempotencyTable,
};
use restate_storage_api::inbox_table::{InboxEntry, SequenceNumberInboxEntry, WriteInboxTable};
use restate_storage_api::invocation_status_table::{
    InvocationStatus, ReadInvocationStatusTable, WriteInvocationStatusTable,
};
use restate_storage_api::journal_events::{EventView, WriteJournalEventsTable};
use restate_storage_api::journal_table::{
    JournalEntry, ReadJournalTable as ReadJournalTableV1, WriteJournalTable as WriteJournalTableV1,
};
use restate_storage_api::journal_table_v2::{
    ReadJournalTable as ReadJournalTableV2, WriteJournalTable as WriteJournalTableV2,
};
use restate_storage_api::outbox_table::WriteOutboxTable;
use restate_storage_api::promise_table::{Promise, ReadPromiseTable, WritePromiseTable};
use restate_storage_api::service_status_table::{
    ReadVirtualObjectStatusTable, VirtualObjectStatus, WriteVirtualObjectStatusTable,
};
use restate_storage_api::state_table::{ReadStateTable, WriteStateTable};
use restate_storage_api::timer_table::{Timer, TimerKey, WriteTimerTable};
use restate_storage_api::vqueue_table::{
    AsEntryState, AsEntryStateHeader, EntryCard, EntryId, EntryKind, EntryStateKind,
    ReadVQueueTable, Stage, WriteVQueueTable,
};
use restate_storage_api::{IsolationLevel, Storage, Transaction};
use restate_types::SemanticRestateVersion;
use restate_types::clock::UniqueTimestamp;
use restate_types::identifiers::{
    EntryIndex, IdempotencyId, InvocationId, PartitionKey, ServiceId,
};
use restate_types::journal_v2::raw::RawCommand;
use restate_types::journal_v2::{CompletionId, NotificationId};
use restate_types::logs::Lsn;
use restate_types::message::MessageIndex;
use restate_types::schema::Schema;
use restate_types::storage::{StoredRawEntry, StoredRawEntryHeader};
use restate_types::vqueue::VQueueId;

// ---------------------------------------------------------------------------
// Storage state
// ---------------------------------------------------------------------------

#[derive(Default, Clone)]
pub(super) struct SimStorage {
    dedup: BTreeMap<Vec<u8>, DedupSequenceNumber>,
    invocations: BTreeMap<InvocationId, InvocationStatus>,
    virtual_objects: BTreeMap<ServiceId, VirtualObjectStatus>,
    inbox: BTreeMap<(ServiceId, MessageIndex), InboxEntry>,
    outbox: BTreeMap<u64, restate_storage_api::outbox_table::OutboxMessage>,
    journal_v2: BTreeMap<(InvocationId, u32), StoredRawEntry>,
    /// completion-id → entry-index index, per invocation
    journal_v2_completions: BTreeMap<InvocationId, HashMap<CompletionId, EntryIndex>>,
    timers: BTreeMap<TimerKey, Timer>,
    state: BTreeMap<(ServiceId, Vec<u8>), Bytes>,
    journal_v1: BTreeMap<(InvocationId, u32), JournalEntry>,
    promises: BTreeMap<(ServiceId, ByteString), Promise>,
    idempotency: HashMap<IdempotencyId, IdempotencyMetadata>,
    // FSM fields (written by the state machine, not read in simulation)
    fsm_applied_lsn: Option<Lsn>,
    fsm_inbox_seq: MessageIndex,
    fsm_outbox_seq: MessageIndex,
}

impl SimStorage {
    /// Read the SELF producer's dedup sequence number (for invariant checking).
    pub(super) fn get_self_dedup(&self) -> Option<DedupSequenceNumber> {
        let key = producer_id_key(&ProducerId::self_producer());
        self.dedup.get(&key).copied()
    }

    /// Count of stored invocations (for invariant checking).
    pub(super) fn invocation_count(&self) -> usize {
        self.invocations.len()
    }
}

/// Serialize a `ProducerId` into a stable byte key for BTreeMap lookup.
fn producer_id_key(id: &ProducerId) -> Vec<u8> {
    // Use debug representation as a simple stable key; fine for simulation.
    format!("{id:?}").into_bytes()
}

// ---------------------------------------------------------------------------
// Transaction wrapper
// ---------------------------------------------------------------------------

pub(super) struct SimTransaction<'a> {
    storage: &'a mut SimStorage,
}

impl Storage for SimStorage {
    type TransactionType<'a> = SimTransaction<'a>;

    fn transaction_with_isolation(
        &mut self,
        _read_isolation: IsolationLevel,
    ) -> SimTransaction<'_> {
        SimTransaction { storage: self }
    }
}

impl Transaction for SimTransaction<'_> {
    async fn commit(self) -> Result<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// deduplication_table
// ---------------------------------------------------------------------------

impl ReadDeduplicationTable for SimTransaction<'_> {
    async fn get_dedup_sequence_number(
        &mut self,
        producer_id: &ProducerId,
    ) -> Result<Option<DedupSequenceNumber>> {
        Ok(self
            .storage
            .dedup
            .get(&producer_id_key(producer_id))
            .copied())
    }
}

impl WriteDeduplicationTable for SimTransaction<'_> {
    fn put_dedup_seq_number(
        &mut self,
        producer_id: ProducerId,
        dedup_sequence_number: &DedupSequenceNumber,
    ) -> Result<()> {
        self.storage
            .dedup
            .insert(producer_id_key(&producer_id), *dedup_sequence_number);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// invocation_status_table
// ---------------------------------------------------------------------------

impl ReadInvocationStatusTable for SimTransaction<'_> {
    async fn get_invocation_status(
        &mut self,
        invocation_id: &InvocationId,
    ) -> Result<InvocationStatus> {
        Ok(self
            .storage
            .invocations
            .get(invocation_id)
            .cloned()
            .unwrap_or_default())
    }
}

impl WriteInvocationStatusTable for SimTransaction<'_> {
    fn put_invocation_status(
        &mut self,
        invocation_id: &InvocationId,
        status: &InvocationStatus,
    ) -> Result<()> {
        self.storage
            .invocations
            .insert(*invocation_id, status.clone());
        Ok(())
    }

    fn delete_invocation_status(&mut self, invocation_id: &InvocationId) -> Result<()> {
        self.storage.invocations.remove(invocation_id);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// service_status_table
// ---------------------------------------------------------------------------

impl ReadVirtualObjectStatusTable for SimTransaction<'_> {
    async fn get_virtual_object_status(
        &mut self,
        service_id: &ServiceId,
    ) -> Result<VirtualObjectStatus> {
        Ok(self
            .storage
            .virtual_objects
            .get(service_id)
            .cloned()
            .unwrap_or_default())
    }
}

impl WriteVirtualObjectStatusTable for SimTransaction<'_> {
    fn put_virtual_object_status(
        &mut self,
        service_id: &ServiceId,
        status: &VirtualObjectStatus,
    ) -> Result<()> {
        self.storage
            .virtual_objects
            .insert(service_id.clone(), status.clone());
        Ok(())
    }

    fn delete_virtual_object_status(&mut self, service_id: &ServiceId) -> Result<()> {
        self.storage.virtual_objects.remove(service_id);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// inbox_table
// ---------------------------------------------------------------------------

impl WriteInboxTable for SimTransaction<'_> {
    fn put_inbox_entry(
        &mut self,
        sequence_number: MessageIndex,
        inbox_entry: &InboxEntry,
    ) -> Result<()> {
        let key = (inbox_entry.service_id().clone(), sequence_number);
        self.storage.inbox.insert(key, inbox_entry.clone());
        Ok(())
    }

    fn delete_inbox_entry(&mut self, service_id: &ServiceId, sequence_number: u64) -> Result<()> {
        self.storage
            .inbox
            .remove(&(service_id.clone(), sequence_number));
        Ok(())
    }

    async fn pop_inbox(
        &mut self,
        service_id: &ServiceId,
    ) -> Result<Option<SequenceNumberInboxEntry>> {
        // Find the first (lowest sequence number) entry for this service_id.
        let key = self
            .storage
            .inbox
            .range((service_id.clone(), 0)..)
            .find(|((sid, _), _)| sid == service_id)
            .map(|((_, seq), _)| *seq);

        if let Some(seq) = key {
            let entry = self
                .storage
                .inbox
                .remove(&(service_id.clone(), seq))
                .expect("just found it");
            Ok(Some(SequenceNumberInboxEntry::new(seq, entry)))
        } else {
            Ok(None)
        }
    }
}

// ---------------------------------------------------------------------------
// outbox_table
// ---------------------------------------------------------------------------

impl WriteOutboxTable for SimTransaction<'_> {
    fn put_outbox_message(
        &mut self,
        message_index: u64,
        outbox_message: &restate_storage_api::outbox_table::OutboxMessage,
    ) -> Result<()> {
        self.storage
            .outbox
            .insert(message_index, outbox_message.clone());
        Ok(())
    }

    fn truncate_outbox(&mut self, range: RangeInclusive<u64>) -> Result<()> {
        let keys: Vec<u64> = self.storage.outbox.range(range).map(|(k, _)| *k).collect();
        for k in keys {
            self.storage.outbox.remove(&k);
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// journal_table_v2
// ---------------------------------------------------------------------------

impl WriteJournalTableV2 for SimTransaction<'_> {
    fn put_journal_entry(
        &mut self,
        invocation_id: InvocationId,
        index: u32,
        entry: &StoredRawEntry,
        related_completion_ids: &[CompletionId],
    ) -> Result<()> {
        self.storage
            .journal_v2
            .insert((invocation_id, index), entry.clone());
        if !related_completion_ids.is_empty() {
            let idx_map = self
                .storage
                .journal_v2_completions
                .entry(invocation_id)
                .or_default();
            for &completion_id in related_completion_ids {
                idx_map.insert(completion_id, index);
            }
        }
        Ok(())
    }

    fn delete_journal(&mut self, invocation_id: InvocationId, _length: EntryIndex) -> Result<()> {
        // Remove all entries for this invocation
        let keys: Vec<_> = self
            .storage
            .journal_v2
            .range((invocation_id, 0)..)
            .take_while(|((id, _), _)| *id == invocation_id)
            .map(|(k, _)| *k)
            .collect();
        for k in keys {
            self.storage.journal_v2.remove(&k);
        }
        self.storage.journal_v2_completions.remove(&invocation_id);
        Ok(())
    }
}

impl ReadJournalTableV2 for SimTransaction<'_> {
    async fn get_journal_entry(
        &mut self,
        invocation_id: InvocationId,
        index: u32,
    ) -> Result<Option<StoredRawEntry>> {
        Ok(self
            .storage
            .journal_v2
            .get(&(invocation_id, index))
            .cloned())
    }

    fn get_journal(
        &self,
        invocation_id: InvocationId,
        length: EntryIndex,
    ) -> Result<impl Stream<Item = Result<(EntryIndex, StoredRawEntry)>> + Send> {
        let entries: Vec<_> = self
            .storage
            .journal_v2
            .range((invocation_id, 0)..(invocation_id, length))
            .map(|((_, idx), entry)| Ok((*idx, entry.clone())))
            .collect();
        Ok(stream::iter(entries))
    }

    async fn get_notifications_index(
        &mut self,
        invocation_id: InvocationId,
    ) -> Result<HashMap<NotificationId, EntryIndex>> {
        Ok(self
            .storage
            .journal_v2_completions
            .get(&invocation_id)
            .map(|m| {
                m.iter()
                    .map(|(&completion_id, &entry_idx)| {
                        (NotificationId::CompletionId(completion_id), entry_idx)
                    })
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn get_command_by_completion_id(
        &mut self,
        invocation_id: InvocationId,
        notification_id: CompletionId,
    ) -> Result<Option<(StoredRawEntryHeader, RawCommand)>> {
        let idx = self
            .storage
            .journal_v2_completions
            .get(&invocation_id)
            .and_then(|m| m.get(&notification_id))
            .copied();

        if let Some(idx) = idx
            && let Some(entry) = self.storage.journal_v2.get(&(invocation_id, idx)).cloned()
            && let Some(cmd) = entry.inner.try_as_command()
        {
            return Ok(Some((entry.header.clone(), cmd.clone())));
        }
        Ok(None)
    }

    async fn has_completion(
        &mut self,
        invocation_id: InvocationId,
        completion_id: CompletionId,
    ) -> Result<bool> {
        Ok(self
            .storage
            .journal_v2_completions
            .get(&invocation_id)
            .map(|m| m.contains_key(&completion_id))
            .unwrap_or(false))
    }

    async fn get_journal_entry_budgeted(
        &mut self,
        invocation_id: InvocationId,
        journal_index: u32,
        _budget: &mut LocalMemoryPool,
    ) -> std::result::Result<Option<(StoredRawEntry, LocalMemoryLease)>, BudgetedReadError> {
        // Simulation: acquire a 0-byte lease and return the entry without budget tracking.
        let entry = self
            .storage
            .journal_v2
            .get(&(invocation_id, journal_index))
            .cloned();
        match entry {
            None => Ok(None),
            Some(e) => {
                // Simulation: acquire a minimal lease; budget exhaustion is unexpected.
                let lease = _budget
                    .try_reserve(1.try_into().expect("1 is NonZero"))
                    .expect("SimStorage: budget exhausted unexpectedly");
                Ok(Some((e, lease)))
            }
        }
    }

    fn get_journal_budgeted<'a>(
        &'a self,
        invocation_id: InvocationId,
        journal_length: EntryIndex,
        budget: &'a mut LocalMemoryPool,
    ) -> Result<
        impl Stream<
            Item = std::result::Result<
                (EntryIndex, StoredRawEntry, LocalMemoryLease),
                BudgetedReadError,
            >,
        > + Send
        + 'a,
    > {
        let entries: Vec<_> = self
            .storage
            .journal_v2
            .range((invocation_id, 0)..(invocation_id, journal_length))
            .map(|((_, idx), entry)| (*idx, entry.clone()))
            .collect();

        // Build a stream that yields each entry with a minimal budget lease.
        let iter = entries.into_iter().map(move |(idx, entry)| {
            let lease = budget
                .try_reserve(1.try_into().expect("1 is NonZero"))
                .expect("SimStorage: budget exhausted unexpectedly");
            Ok((idx, entry, lease))
        });
        Ok(stream::iter(iter))
    }
}

// ---------------------------------------------------------------------------
// journal_table (v1, deprecated) — unimplemented
// ---------------------------------------------------------------------------

impl WriteJournalTableV1 for SimTransaction<'_> {
    fn put_journal_entry(
        &mut self,
        invocation_id: &InvocationId,
        journal_index: u32,
        journal_entry: &JournalEntry,
    ) -> Result<()> {
        self.storage
            .journal_v1
            .insert((*invocation_id, journal_index), journal_entry.clone());
        Ok(())
    }

    fn delete_journal(
        &mut self,
        invocation_id: &InvocationId,
        journal_length: EntryIndex,
    ) -> Result<()> {
        let keys: Vec<_> = self
            .storage
            .journal_v1
            .range((*invocation_id, 0)..=(*invocation_id, journal_length))
            .map(|(k, _)| *k)
            .collect();
        for k in keys {
            self.storage.journal_v1.remove(&k);
        }
        Ok(())
    }
}

impl ReadJournalTableV1 for SimTransaction<'_> {
    async fn get_journal_entry(
        &mut self,
        invocation_id: &InvocationId,
        journal_index: u32,
    ) -> Result<Option<JournalEntry>> {
        Ok(self
            .storage
            .journal_v1
            .get(&(*invocation_id, journal_index))
            .cloned())
    }

    fn get_journal<'a>(
        &'a self,
        invocation_id: &InvocationId,
        journal_length: EntryIndex,
    ) -> Result<impl Stream<Item = Result<(EntryIndex, JournalEntry)>> + Send + 'a> {
        let id = *invocation_id;
        let iter: Vec<_> = (0..journal_length)
            .filter_map(|idx| {
                self.storage
                    .journal_v1
                    .get(&(id, idx))
                    .map(|e| Ok((idx, e.clone())))
            })
            .collect();
        Ok(stream::iter(iter))
    }

    async fn get_journal_entry_budgeted(
        &mut self,
        _invocation_id: &InvocationId,
        _journal_index: u32,
        _budget: &mut LocalMemoryPool,
    ) -> std::result::Result<Option<(JournalEntry, LocalMemoryLease)>, BudgetedReadError> {
        unimplemented!(
            "SimStorage: journal_table_v1::get_journal_entry_budgeted not yet implemented"
        )
    }

    fn get_journal_budgeted<'a>(
        &'a self,
        _invocation_id: &InvocationId,
        _journal_length: EntryIndex,
        _budget: &'a mut LocalMemoryPool,
    ) -> Result<
        impl Stream<
            Item = std::result::Result<
                (EntryIndex, JournalEntry, LocalMemoryLease),
                BudgetedReadError,
            >,
        > + Send
        + 'a,
    > {
        unimplemented!("SimStorage: journal_table_v1::get_journal_budgeted not yet implemented");
        #[allow(unreachable_code)]
        Ok(stream::empty())
    }
}

// ---------------------------------------------------------------------------
// fsm_table
// ---------------------------------------------------------------------------

impl WriteFsmTable for SimTransaction<'_> {
    fn put_applied_lsn(&mut self, lsn: Lsn) -> Result<()> {
        self.storage.fsm_applied_lsn = Some(lsn);
        Ok(())
    }

    fn put_inbox_seq_number(&mut self, seq_number: MessageIndex) -> Result<()> {
        self.storage.fsm_inbox_seq = seq_number;
        Ok(())
    }

    fn put_outbox_seq_number(&mut self, seq_number: MessageIndex) -> Result<()> {
        self.storage.fsm_outbox_seq = seq_number;
        Ok(())
    }

    fn put_min_restate_version(&mut self, _version: &SemanticRestateVersion) -> Result<()> {
        Ok(())
    }

    fn put_partition_durability(&mut self, _durability: &PartitionDurability) -> Result<()> {
        Ok(())
    }

    fn put_schema(&mut self, _schema: &Schema) -> Result<()> {
        Ok(())
    }

    fn put_partition_config_state(&mut self, _state: &CachedEpochMetadata) -> Result<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// timer_table
// ---------------------------------------------------------------------------

impl WriteTimerTable for SimTransaction<'_> {
    fn put_timer(&mut self, timer_key: &TimerKey, timer: &Timer) -> Result<()> {
        self.storage.timers.insert(timer_key.clone(), timer.clone());
        Ok(())
    }

    fn delete_timer(&mut self, timer_key: &TimerKey) -> Result<()> {
        self.storage.timers.remove(timer_key);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// idempotency_table
// ---------------------------------------------------------------------------

impl ReadOnlyIdempotencyTable for SimTransaction<'_> {
    async fn get_idempotency_metadata(
        &mut self,
        idempotency_id: &IdempotencyId,
    ) -> Result<Option<IdempotencyMetadata>> {
        Ok(self.storage.idempotency.get(idempotency_id).cloned())
    }
}

impl IdempotencyTable for SimTransaction<'_> {
    async fn put_idempotency_metadata(
        &mut self,
        idempotency_id: &IdempotencyId,
        metadata: &IdempotencyMetadata,
    ) -> Result<()> {
        self.storage
            .idempotency
            .insert(idempotency_id.clone(), metadata.clone());
        Ok(())
    }

    async fn delete_idempotency_metadata(&mut self, idempotency_id: &IdempotencyId) -> Result<()> {
        self.storage.idempotency.remove(idempotency_id);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// promise_table
// ---------------------------------------------------------------------------

impl ReadPromiseTable for SimTransaction<'_> {
    async fn get_promise(
        &mut self,
        service_id: &ServiceId,
        key: &ByteString,
    ) -> Result<Option<Promise>> {
        Ok(self
            .storage
            .promises
            .get(&(service_id.clone(), key.clone()))
            .cloned())
    }
}

impl WritePromiseTable for SimTransaction<'_> {
    fn put_promise(
        &mut self,
        service_id: &ServiceId,
        key: &ByteString,
        promise: &Promise,
    ) -> Result<()> {
        self.storage
            .promises
            .insert((service_id.clone(), key.clone()), promise.clone());
        Ok(())
    }

    fn delete_all_promises(&mut self, service_id: &ServiceId) -> Result<()> {
        self.storage
            .promises
            .retain(|(sid, _), _| sid != service_id);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// state_table
// ---------------------------------------------------------------------------

impl ReadStateTable for SimTransaction<'_> {
    async fn get_user_state(
        &mut self,
        service_id: &ServiceId,
        state_key: impl AsRef<[u8]> + Send,
    ) -> Result<Option<Bytes>> {
        let map_key = (service_id.clone(), state_key.as_ref().to_vec());
        Ok(self.storage.state.get(&map_key).cloned())
    }

    fn get_all_user_states_for_service<'a>(
        &'a self,
        service_id: &ServiceId,
    ) -> Result<impl Stream<Item = Result<(Bytes, Bytes)>> + Send + 'a> {
        let sid = service_id.clone();
        let entries: Vec<_> = self
            .storage
            .state
            .iter()
            .filter(move |((s, _), _)| *s == sid)
            .map(|((_, k), v)| Ok((Bytes::from(k.clone()), v.clone())))
            .collect();
        Ok(stream::iter(entries))
    }

    fn get_all_user_states_budgeted<'a>(
        &'a self,
        service_id: &ServiceId,
        _budget: &'a mut LocalMemoryPool,
    ) -> Result<
        impl restate_memory::PinnableMemoryStream<
            Item = std::result::Result<(Bytes, Bytes, LocalMemoryLease), BudgetedReadError>,
        > + Send
        + 'a,
    > {
        // Simulation: return entries without budget tracking using IgnorePinnableMemoryStream.
        // We can't acquire real leases here without a budget reference that outlives the entries,
        // so we return an empty stream and let the caller handle the no-budget case.
        let _ = service_id;
        Ok(IgnorePinnableMemoryStream::new(stream::empty()))
    }
}

impl WriteStateTable for SimTransaction<'_> {
    fn put_user_state(
        &mut self,
        service_id: &ServiceId,
        state_key: impl AsRef<[u8]> + Send,
        state_value: impl AsRef<[u8]> + Send,
    ) -> Result<()> {
        let map_key = (service_id.clone(), state_key.as_ref().to_vec());
        self.storage
            .state
            .insert(map_key, Bytes::copy_from_slice(state_value.as_ref()));
        Ok(())
    }

    fn delete_user_state(
        &mut self,
        service_id: &ServiceId,
        state_key: impl AsRef<[u8]> + Send,
    ) -> Result<()> {
        let map_key = (service_id.clone(), state_key.as_ref().to_vec());
        self.storage.state.remove(&map_key);
        Ok(())
    }

    fn delete_all_user_state(&mut self, service_id: &ServiceId) -> Result<()> {
        self.storage.state.retain(|(sid, _), _| sid != service_id);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// journal_events — no-op (events are not read by the state machine)
// ---------------------------------------------------------------------------

impl WriteJournalEventsTable for SimTransaction<'_> {
    fn put_journal_event(
        &mut self,
        _invocation_id: InvocationId,
        _event: EventView,
        _lsn: u64,
    ) -> Result<()> {
        Ok(())
    }

    fn delete_journal_events(&mut self, _invocation_id: InvocationId) -> Result<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// vqueue_table — unimplemented (experimental, not used in simulation)
// ---------------------------------------------------------------------------

impl WriteVQueueTable for SimTransaction<'_> {
    fn update_vqueue(
        &mut self,
        _qid: &VQueueId,
        _updates: &restate_storage_api::vqueue_table::metadata::VQueueMetaUpdates,
    ) {
        unimplemented!("SimStorage: vqueue_table::update_vqueue not yet implemented")
    }

    fn put_inbox_entry(&mut self, _qid: &VQueueId, _stage: Stage, _card: &EntryCard) {
        unimplemented!("SimStorage: vqueue_table::put_inbox_entry not yet implemented")
    }

    fn pop_inbox_entry(
        &mut self,
        _qid: &VQueueId,
        _stage: Stage,
        _card: &EntryCard,
    ) -> Result<bool> {
        unimplemented!("SimStorage: vqueue_table::pop_inbox_entry not yet implemented")
    }

    fn mark_vqueue_as_active(&mut self, _qid: &VQueueId) {
        unimplemented!("SimStorage: vqueue_table::mark_vqueue_as_active not yet implemented")
    }

    fn mark_vqueue_as_dormant(&mut self, _qid: &VQueueId) {
        unimplemented!("SimStorage: vqueue_table::mark_vqueue_as_dormant not yet implemented")
    }

    fn put_vqueue_entry_state<E>(
        &mut self,
        _qid: &VQueueId,
        _card: &EntryCard,
        _stage: Stage,
        _state: E,
    ) where
        E: EntryStateKind + bilrost::Message + bilrost::encoding::RawMessage,
        (): bilrost::encoding::EmptyState<(), E>,
    {
        unimplemented!("SimStorage: vqueue_table::put_vqueue_entry_state not yet implemented")
    }

    fn delete_vqueue_entry_state(&mut self, _qid: &VQueueId, _kind: EntryKind, _id: &EntryId) {
        unimplemented!("SimStorage: vqueue_table::delete_vqueue_entry_state not yet implemented")
    }

    fn put_item<E>(
        &mut self,
        _qid: &VQueueId,
        _created_at: UniqueTimestamp,
        _kind: EntryKind,
        _id: &EntryId,
        _item: E,
    ) where
        E: bilrost::Message,
    {
        unimplemented!("SimStorage: vqueue_table::put_item not yet implemented")
    }

    fn delete_item(
        &mut self,
        _qid: &VQueueId,
        _created_at: UniqueTimestamp,
        _kind: EntryKind,
        _id: &EntryId,
    ) {
        unimplemented!("SimStorage: vqueue_table::delete_item not yet implemented")
    }
}

impl ReadVQueueTable for SimTransaction<'_> {
    async fn get_vqueue(
        &mut self,
        _qid: &VQueueId,
    ) -> Result<Option<restate_storage_api::vqueue_table::metadata::VQueueMeta>> {
        unimplemented!("SimStorage: vqueue_table::get_vqueue not yet implemented")
    }

    async fn get_entry_state_header(
        &mut self,
        _kind: EntryKind,
        _partition_key: PartitionKey,
        _id: &EntryId,
    ) -> Result<Option<impl AsEntryStateHeader + 'static + Send>> {
        unimplemented!("SimStorage: vqueue_table::get_entry_state_header not yet implemented");
        #[allow(unreachable_code)]
        Ok(None::<NeverEntryStateHeader>)
    }

    async fn get_entry_state<E>(
        &mut self,
        _kind: EntryKind,
        _partition_key: PartitionKey,
        _id: &EntryId,
    ) -> Result<Option<impl AsEntryState<State = E> + 'static + Send>>
    where
        E: EntryStateKind
            + bilrost::OwnedMessage
            + bilrost::encoding::RawMessageDecoder
            + Sized
            + 'static,
        (): bilrost::encoding::EmptyState<(), E>,
    {
        unimplemented!("SimStorage: vqueue_table::get_entry_state not yet implemented");
        #[allow(unreachable_code)]
        Ok(None::<NeverEntryState<E>>)
    }

    async fn get_item<E>(
        &mut self,
        _qid: &VQueueId,
        _created_at: UniqueTimestamp,
        _kind: EntryKind,
        _id: &EntryId,
    ) -> Result<Option<E>>
    where
        E: bilrost::OwnedMessage,
    {
        unimplemented!("SimStorage: vqueue_table::get_item not yet implemented")
    }
}

// ---------------------------------------------------------------------------
// Phantom types for unimplemented vqueue return types
// ---------------------------------------------------------------------------

enum NeverEntryStateHeader {}

impl AsEntryStateHeader for NeverEntryStateHeader {
    fn kind(&self) -> EntryKind {
        match *self {}
    }
    fn stage(&self) -> Stage {
        match *self {}
    }
    fn queue_parent(&self) -> restate_types::vqueue::VQueueParent {
        match *self {}
    }
    fn queue_instance(&self) -> restate_types::vqueue::VQueueInstance {
        match *self {}
    }
    fn vqueue_id(&self) -> VQueueId {
        match *self {}
    }
    fn current_entry_card(&self) -> EntryCard {
        match *self {}
    }
}

struct NeverEntryState<E>(std::marker::PhantomData<E>, std::convert::Infallible);

impl<E> AsEntryStateHeader for NeverEntryState<E> {
    fn kind(&self) -> EntryKind {
        match self.1 {}
    }
    fn stage(&self) -> Stage {
        match self.1 {}
    }
    fn queue_parent(&self) -> restate_types::vqueue::VQueueParent {
        match self.1 {}
    }
    fn queue_instance(&self) -> restate_types::vqueue::VQueueInstance {
        match self.1 {}
    }
    fn vqueue_id(&self) -> VQueueId {
        match self.1 {}
    }
    fn current_entry_card(&self) -> EntryCard {
        match self.1 {}
    }
}

impl<E: EntryStateKind> AsEntryState for NeverEntryState<E> {
    type State = E;
    fn state(&self) -> &E {
        match self.1 {}
    }
}

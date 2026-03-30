/**
 * Memory-backed LiveSource and LiveQuery.
 *
 * Records live in a plain Map. SSE pushes updates. All query operations
 * are synchronous array scans — no IDB, no async, no Dexie.
 */
import type { IDBKey, LiveRecord, LiveSource, LiveQuery, LiveFrame, LiveFold, AggregatorDescriptor, Predicate, SortSpec, QuerySpec } from './types.js';
import { normalizeFilter, normalizeSort, matchRecord, sortRecords } from './predicates.js';
import { createFrame } from './frame.js';
import { createFold } from './fold.js';

type TupleRecord = [IDBKey, LiveRecord];
type ReactiveFunction = (obj: any) => any;

// --- MemoryStore ---

/** Simple in-memory record store with subscriber notification. */
export class MemoryStore {
  private records = new Map<IDBKey, Record<string, unknown>>();
  private listeners = new Set<() => void>();

  put(key: IDBKey, value: Record<string, unknown>): void {
    this.records.set(key, value);
  }

  delete(key: IDBKey): void {
    this.records.delete(key);
  }

  clear(): void {
    this.records.clear();
  }

  entries(): Array<{ key: IDBKey; value: Record<string, unknown> }> {
    const result: Array<{ key: IDBKey; value: Record<string, unknown> }> = [];
    for (const [key, value] of this.records) {
      result.push({ key, value });
    }
    return result;
  }

  /** Notify subscribers that data changed. Call after a batch of writes. */
  notify(): void {
    for (const cb of this.listeners) cb();
  }

  subscribe(callback: () => void): () => void {
    this.listeners.add(callback);
    return () => this.listeners.delete(callback);
  }
}

// --- MemorySource ---

export function createMemorySource(
  store: MemoryStore,
  callerReactive: ReactiveFunction,
): [LiveSource, () => void] {
  const live = callerReactive({
    status: 'connected' as string,
    connected: true,

    query(): [LiveQuery, () => void] {
      return createMemoryQuery(store, callerReactive);
    },

    frame(opts: { size: number }): [LiveFrame, () => void] {
      const [q, disposeQuery] = createMemoryQuery(store, callerReactive);
      const [f, disposeFrame] = q.frame(opts);
      return [f, () => { disposeFrame(); disposeQuery(); }];
    },

    fold(aggregators: Record<string, AggregatorDescriptor>): [LiveFold, () => void] {
      const [q, disposeQuery] = createMemoryQuery(store, callerReactive);
      const [f, disposeFold] = q.fold(aggregators);
      return [f, () => { disposeFold(); disposeQuery(); }];
    },
  });

  return [live as LiveSource, () => {}];
}

// --- MemoryQuery ---

export function createMemoryQuery(
  store: MemoryStore,
  callerReactive?: ReactiveFunction,
): [LiveQuery, () => void] {
  let currentFilters: Predicate[] = [];
  let currentSorts: SortSpec[] = [];
  let cachedRecords: TupleRecord[] = [];
  let cachedMap = new Map<IDBKey, LiveRecord>();
  const slotMap = new Map<IDBKey, Record<string, unknown>>();

  const indexListeners = new Set<() => void>();

  /** Update getOr slots with current record data or defaults. */
  function updateSlots(): void {
    for (const [key, slot] of slotMap) {
      const record = cachedMap.get(key);
      if (record) {
        // Copy record fields into slot in-place
        for (const k of Object.keys(record)) {
          if (k !== '$') (slot as any)[k] = (record as any)[k];
        }
      }
    }
  }

  /** Recompute records from store, applying filters and sorts. */
  function recompute(): void {
    const all = store.entries();

    let filtered = currentFilters.length > 0
      ? all.filter(({ value }) => matchRecord(value, currentFilters))
      : all;

    let tuples = filtered.map(({ key, value }) => [key, value as unknown as LiveRecord] as TupleRecord);

    if (currentSorts.length > 0) {
      tuples = sortRecords(tuples, currentSorts);
    }

    cachedRecords = tuples;
    cachedMap = new Map(tuples);
    updateSlots();
    for (const cb of indexListeners) cb();
  }

  const unsubStore = store.subscribe(() => recompute());
  recompute();

  const query: LiveQuery = {
    apply(spec: QuerySpec): void {
      if (spec.filter !== undefined) {
        currentFilters = normalizeFilter(spec.filter);
      }
      if (spec.sort !== undefined) {
        currentSorts = normalizeSort(spec.sort);
      }
      recompute();
    },

    filter(spec): void {
      query.apply({ filter: spec });
    },

    sort(spec): void {
      query.apply({ sort: spec });
    },

    all(): ReadonlyArray<TupleRecord> {
      return cachedRecords;
    },

    count(): number {
      return cachedRecords.length;
    },

    window(key: IDBKey, count: number): ReadonlyArray<TupleRecord> {
      // Find key and return records after it (exclusive).
      // If key not found, find the next key that would follow it
      // (same behavior as IDB cursor above(key) on a missing key).
      const idx = cachedRecords.findIndex(([k]) => k === key);
      if (idx === -1) {
        // Key deleted — find insertion point (first key > cursor)
        const insertIdx = cachedRecords.findIndex(([k]) => String(k) > String(key));
        return insertIdx === -1 ? [] : cachedRecords.slice(insertIdx, insertIdx + count);
      }
      return cachedRecords.slice(idx + 1, idx + 1 + count);
    },

    range(startKey: IDBKey, endKey: IDBKey): ReadonlyArray<TupleRecord> {
      const startIdx = cachedRecords.findIndex(([k]) => k === startKey);
      const endIdx = cachedRecords.findIndex(([k]) => k === endKey);
      if (startIdx === -1 || endIdx === -1) return [];
      return cachedRecords.slice(startIdx, endIdx + 1);
    },

    slice(offset: number, count: number): ReadonlyArray<TupleRecord> {
      return cachedRecords.slice(offset, offset + count);
    },

    any(): boolean {
      return cachedRecords.length > 0;
    },

    has(key: IDBKey): boolean {
      return cachedMap.has(key);
    },

    get(key: IDBKey): LiveRecord | undefined {
      return cachedMap.get(key);
    },

    getOr(key: IDBKey, defaults: Record<string, unknown>): LiveRecord {
      if (!slotMap.has(key)) {
        // Create persistent reactive slot — populated from record if available, else defaults
        const record = cachedMap.get(key);
        const initial = { ...defaults };
        if (record) {
          for (const k of Object.keys(record)) {
            if (k !== '$') initial[k] = (record as any)[k];
          }
        }
        const slot = callerReactive ? callerReactive(initial) : initial;
        slotMap.set(key, slot);
      }
      return slotMap.get(key)! as unknown as LiveRecord;
    },

    onIndexChange(callback: () => void): () => void {
      indexListeners.add(callback);
      return () => indexListeners.delete(callback);
    },

    frame(opts: { size: number }): [LiveFrame, () => void] {
      return createFrame(query, opts, callerReactive);
    },

    fold(aggregators: Record<string, AggregatorDescriptor>): [LiveFold, () => void] {
      return createFold(query, aggregators, callerReactive);
    },
  };

  function dispose(): void {
    unsubStore();
    indexListeners.clear();
    cachedRecords = [];
  }

  return [query, dispose];
}


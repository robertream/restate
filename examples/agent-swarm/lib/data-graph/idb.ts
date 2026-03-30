/**
 * IDB-backed LiveSource and LiveQuery via Dexie.
 *
 * Push-down model: window functions create targeted Dexie liveQuery
 * subscriptions that only fetch the requested records. The frame pulls
 * what it needs, the query is a thin pass-through to IDB.
 */
import Dexie, { liveQuery } from 'dexie';
import type { IDBKey, LiveRecord, LiveSource, LiveQuery, LiveFrame, LiveFold, AggregatorDescriptor, Predicate, SortSpec, QuerySpec } from './types.js';
import { normalizeFilter, normalizeSort, matchRecord, sortRecords } from './predicates.js';
import { createFrame } from './frame.js';
import { createFold } from './fold.js';

type TupleRecord = [IDBKey, LiveRecord];
type ReactiveFunction = (obj: any) => any;
type KeyedRecord = { key: IDBKey; value: Record<string, unknown> };

// --- IDBStore ---

/** Dexie-backed record store with out-of-line keys and targeted query methods. */
export class IDBStore {
  private db: Dexie;
  private tableName = 'records';
  private indexes: string[] = [];
  private version = 1;

  constructor(name: string) {
    this.db = new Dexie(name);
    this.db.version(1).stores({ [this.tableName]: '' });
  }

  /** Declare a field index. Must be called before any data access (before db opens). */
  declareIndex(field: string): void {
    if (this.db.isOpen()) {
      throw new Error(`Cannot declareIndex('${field}') after database is open. Declare indexes before any data access.`);
    }
    if (!this.indexes.includes(field)) {
      this.indexes.push(field);
      this.version++;
      this.db.version(this.version).stores({
        [this.tableName]: ', ' + this.indexes.join(', '),
      });
    }
  }

  /** Check if a field has a declared index. */
  hasIndex(field: string): boolean {
    return this.indexes.includes(field);
  }

  table() { return this.db.table(this.tableName); }

  async put(key: IDBKey, value: Record<string, unknown>): Promise<void> {
    await this.table().put(value, key as any);
  }

  async delete(key: IDBKey): Promise<void> {
    await this.table().delete(key as any);
  }

  async bulkDelete(keys: IDBKey[]): Promise<void> {
    await this.table().bulkDelete(keys as any[]);
  }

  async clear(): Promise<void> {
    await this.table().clear();
  }

  close(): void {
    this.db.close();
  }
}

// --- IDBSource ---

export function createIDBSource(
  store: IDBStore,
  callerReactive: ReactiveFunction,
): [LiveSource, () => void] {
  const live = callerReactive({
    status: 'connecting' as string,
    connected: false,

    query(): [LiveQuery, () => void] {
      return createIDBQuery(store, callerReactive);
    },

    frame(opts: { size: number }): [LiveFrame, () => void] {
      const [q, disposeQuery] = createIDBQuery(store, callerReactive);
      const [f, disposeFrame] = q.frame(opts);
      return [f, () => { disposeFrame(); disposeQuery(); }];
    },

    fold(aggregators: Record<string, AggregatorDescriptor>): [LiveFold, () => void] {
      const [q, disposeQuery] = createIDBQuery(store, callerReactive);
      const [f, disposeFold] = q.fold(aggregators);
      return [f, () => { disposeFold(); disposeQuery(); }];
    },
  });

  return [live as LiveSource, () => store.close()];
}

// --- IDBQuery ---

/**
 * IDB query with filter-first push-down.
 *
 * apply() creates a single Dexie liveQuery that:
 * 1. Pushes the first filter predicate to Dexie where() (if pushable)
 * 2. Applies remaining filters via .and() (JS)
 * 3. Caches full filtered result set
 * 4. Sorts in JS
 *
 * Window functions (window/slice/range/has) operate on cachedRecords.
 */
export function createIDBQuery(
  store: IDBStore,
  callerReactive?: ReactiveFunction,
): [LiveQuery, () => void] {
  let currentFilters: Predicate[] = [];
  let currentSorts: SortSpec[] = [];
  let cachedRecords: TupleRecord[] = [];
  let cachedMap = new Map<IDBKey, LiveRecord>();
  const slotMap = new Map<IDBKey, Record<string, unknown>>();
  let unsub: (() => void) | null = null;

  const indexListeners = new Set<() => void>();

  function updateSlots(): void {
    for (const [key, slot] of slotMap) {
      const record = cachedMap.get(key);
      if (record) {
        for (const k of Object.keys(record)) {
          if (k !== '$') (slot as any)[k] = (record as any)[k];
        }
      }
    }
  }

  function resubscribe(): void {
    if (unsub) unsub();

    // Build Dexie query: push first indexed predicate to where(), rest to JS filter.
    const filters = currentFilters;
    const firstIndexed = filters.find(p => p.indexed && store.hasIndex(p.field));
    const jsFilters = firstIndexed ? filters.filter(p => p !== firstIndexed) : filters;

    const sub = liveQuery(async () => {
      let collection: any;

      if (firstIndexed) {
        // Push down to Dexie where()
        const wc = store.table().where(firstIndexed.field);
        switch (firstIndexed.op) {
          case 'eq': collection = wc.equals(firstIndexed.value as any); break;
          case 'not': collection = wc.notEqual(firstIndexed.value as any); break;
          case 'in': collection = wc.anyOf(firstIndexed.value as any[]); break;
          default: collection = store.table().toCollection();
        }
      } else {
        collection = store.table().toCollection();
      }

      // Apply remaining filters as JS predicates
      if (jsFilters.length > 0) {
        collection = collection.and((rec: Record<string, unknown>) =>
          matchRecord(rec, jsFilters)
        );
      }

      const results: KeyedRecord[] = [];
      await collection.each((value: any, cursor: any) => {
        results.push({ key: cursor.primaryKey, value: value as Record<string, unknown> });
      });
      return results;
    }).subscribe({
      next: (records) => {
        let tuples = records.map(({ key, value }) =>
          [key, value as unknown as LiveRecord] as TupleRecord
        );
        if (currentSorts.length > 0) {
          tuples = sortRecords(tuples, currentSorts);
        }
        cachedRecords = tuples;
        cachedMap = new Map(tuples);
        updateSlots();
        for (const cb of indexListeners) cb();
      },
    });

    unsub = () => sub.unsubscribe();
  }

  // Initial subscription — all records, no filter
  resubscribe();

  const query: LiveQuery = {
    apply(spec: QuerySpec): void {
      if (spec.filter !== undefined) {
        currentFilters = normalizeFilter(spec.filter);
      }
      if (spec.sort !== undefined) {
        currentSorts = normalizeSort(spec.sort);
      }
      resubscribe();
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

    any(): boolean {
      return cachedRecords.length > 0;
    },

    window(key: IDBKey, count: number): ReadonlyArray<TupleRecord> {
      const idx = cachedRecords.findIndex(([k]) => k === key);
      if (idx === -1) {
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

    has(key: IDBKey): boolean {
      return cachedMap.has(key);
    },

    get(key: IDBKey): LiveRecord | undefined {
      return cachedMap.get(key);
    },

    getOr(key: IDBKey, defaults: Record<string, unknown>): LiveRecord {
      if (!slotMap.has(key)) {
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
    if (unsub) { unsub(); unsub = null; }
    indexListeners.clear();
    cachedRecords = [];
  }

  return [query, dispose];
}


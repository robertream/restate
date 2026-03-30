/**
 * Shared interfaces for the next-gen component API.
 * Backend-agnostic — Memory and IDB implement these differently.
 */

export type IDBKey = string | number | Date | ArrayBuffer | IDBKey[];

/** A reactive record with field access + $() traversal. */
export interface LiveRecord {
  readonly [key: string]: unknown;
  $(ref: unknown): LiveRecord | LiveRecord[] | undefined;
}

import type { Predicate, SortSpec, QuerySpec } from './predicates.js';
export type { Predicate, SortSpec, QuerySpec } from './predicates.js';

// --- LiveSource ---

/** Aggregator descriptor — matches core/aggregators.ts. */
export type AggregatorDescriptor =
  | { agg: 'count' }
  | { agg: 'sum'; field: string }
  | { agg: 'avg'; field: string }
  | { agg: 'min'; field: string }
  | { agg: 'max'; field: string };

/** A reactive aggregate result — keys are aggregator names, values are computed results. */
export interface LiveFold {
  readonly [key: string]: unknown;
}

export interface LiveSource {
  readonly status: string;
  readonly connected: boolean;

  /** Create a stateful query over this source. Returns [LiveQuery, dispose]. */
  query(): [LiveQuery, () => void];

  /** Shorthand: creates a query + frame. Returns [LiveFrame, dispose]. */
  frame(opts: { size: number }): [LiveFrame, () => void];

  /** Shorthand: creates a query + fold. Returns [LiveFold, dispose]. */
  fold(aggregators: Record<string, AggregatorDescriptor>): [LiveFold, () => void];
}

// --- LiveQuery ---

export interface LiveQuery {
  /** Replace filter and/or sort. Only replaces what's specified. */
  apply(spec: QuerySpec): void;

  /** Convenience: apply({ filter: spec }). */
  filter(spec: Predicate | Predicate[] | Record<string, unknown>): void;

  /** Convenience: apply({ sort: spec }). */
  sort(spec: SortSpec | SortSpec[]): void;

  /** Return all matched records. */
  all(): ReadonlyArray<[IDBKey, LiveRecord]>;

  /** Count of matched records. */
  count(): number;

  /** Key-based: records starting after key, up to count. */
  window(key: IDBKey, count: number): ReadonlyArray<[IDBKey, LiveRecord]>;

  /** Key-based: records between two keys (inclusive). */
  range(startKey: IDBKey, endKey: IDBKey): ReadonlyArray<[IDBKey, LiveRecord]>;

  /** Ordinal: records at offset, up to count. */
  slice(offset: number, count: number): ReadonlyArray<[IDBKey, LiveRecord]>;

  /** Are there any matched records? */
  any(): boolean;

  /** Does a key exist in the current result set? */
  has(key: IDBKey): boolean;

  /** Look up a single record by key. Returns undefined if not found. */
  get(key: IDBKey): LiveRecord | undefined;

  /** Look up a single record by key. Returns a persistent reactive record populated with defaults if key not found, updated in-place when the record arrives. */
  getOr(key: IDBKey, defaults: Record<string, unknown>): LiveRecord;

  /** Subscribe to index changes (key set add/remove/reorder). */
  onIndexChange(callback: () => void): () => void;

  /** Wrap with pagination. Returns [LiveFrame, dispose]. */
  frame(opts: { size: number }): [LiveFrame, () => void];

  /** Aggregate matched records. Returns [LiveFold, dispose]. */
  fold(aggregators: Record<string, AggregatorDescriptor>): [LiveFold, () => void];
}

// --- LiveFrame ---

export interface LiveFrame {
  /** The underlying query. Use page.query.apply(spec) to change predicates. */
  readonly query: LiveQuery;

  /** Current page of records. */
  readonly records: ReadonlyArray<[IDBKey, LiveRecord]>;

  readonly size: number;
  readonly atStart: boolean;
  readonly atEnd: boolean;

  first(): void;
  prev(): void;
  next(): void;
  last(): void;
}

// --- KV-sync ---

export type KvInstruction =
  | { op: 'ASN'; data: Record<string, unknown> }
  | { op: 'DEL'; data: string[] }
  | { op: 'CLR' }
  | { op: 'RPL'; data: Record<string, unknown> };

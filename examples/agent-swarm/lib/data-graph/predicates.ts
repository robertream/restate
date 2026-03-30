/**
 * Shared predicate and sort primitives.
 * Backend-agnostic — used by both Memory and IDB query implementations.
 */

// --- Types ---

export interface Predicate {
  field: string;
  op: string;
  value: unknown;
  indexed?: boolean;
}

export interface SortSpec {
  field: string;
  dir: 'asc' | 'desc';
}

export interface QuerySpec {
  filter?: Predicate | Predicate[] | Record<string, unknown>;
  sort?: SortSpec | SortSpec[];
}

// --- Field builder ---

export interface FieldBuilder {
  eq(value: unknown): Predicate;
  not(value: unknown): Predicate;
  in(values: unknown[]): Predicate;
  where(fn: (value: unknown) => boolean): Predicate;
}

export function Field(path: string): FieldBuilder {
  return {
    eq: (value) => ({ field: path, op: 'eq', value }),
    not: (value) => ({ field: path, op: 'not', value }),
    in: (values) => ({ field: path, op: 'in', value: values }),
    where: (fn) => ({ field: path, op: 'where', value: fn }),
  };
}

// --- Sort descriptors ---

export function Asc(field: string): SortSpec {
  return { field, dir: 'asc' };
}

export function Desc(field: string): SortSpec {
  return { field, dir: 'desc' };
}

// --- Predicate detection ---

export function isPredicate(v: unknown): v is Predicate {
  return v != null && typeof v === 'object' && 'field' in v && 'op' in v;
}

// --- Normalize filter input ---

/** Convert any filter input form to a Predicate array. */
export function normalizeFilter(
  spec: Predicate | Predicate[] | Record<string, unknown>,
): Predicate[] {
  if (Array.isArray(spec)) return spec;
  if (isPredicate(spec)) return [spec];
  // Plain object — each key is a field, value is equality
  return Object.entries(spec).map(([field, value]) => ({
    field,
    op: 'eq',
    value,
  }));
}

/** Convert sort input to array. */
export function normalizeSort(
  spec: SortSpec | SortSpec[],
): SortSpec[] {
  return Array.isArray(spec) ? spec : [spec];
}

// --- Field value access (dot-notation) ---

export function getField(record: Record<string, unknown>, path: string): unknown {
  let val: unknown = record;
  for (const part of path.split('.')) {
    if (val == null || typeof val !== 'object') return undefined;
    val = (val as Record<string, unknown>)[part];
  }
  return val;
}

// --- Evaluate predicates ---

/** Test a single predicate against a record. */
export function evalPredicate(record: Record<string, unknown>, pred: Predicate): boolean {
  const val = getField(record, pred.field);
  switch (pred.op) {
    case 'eq': return val === pred.value;
    case 'not': return val !== pred.value;
    case 'in': return Array.isArray(pred.value) && (pred.value as unknown[]).includes(val);
    case 'where': return typeof pred.value === 'function' && (pred.value as (v: unknown) => boolean)(val);
    default: throw new Error(`Unknown predicate op: "${pred.op}"`);
  }
}

/** Test all predicates against a record (AND). */
export function matchRecord(
  record: Record<string, unknown>,
  predicates: Predicate[],
): boolean {
  return predicates.every(pred => evalPredicate(record, pred));
}

// --- Sort ---

/** Sort records by sort spec. Returns new array. */
export function sortRecords<T extends [unknown, Record<string, unknown>]>(
  records: ReadonlyArray<T>,
  sorts: SortSpec[],
): T[] {
  return [...records].sort((a, b) => {
    for (const { field, dir } of sorts) {
      const av = getField(a[1], field);
      const bv = getField(b[1], field);
      if (av === bv || (av == null && bv == null)) continue;
      if (av == null) return dir === 'asc' ? 1 : -1;
      if (bv == null) return dir === 'asc' ? -1 : 1;
      const cmp = av < bv ? -1 : 1;
      return dir === 'asc' ? cmp : -cmp;
    }
    return 0;
  });
}

/**
 * LiveFold — shared implementation, backend-agnostic.
 * Single-pass aggregation over LiveQuery results.
 */
import type { LiveQuery, LiveFold, AggregatorDescriptor } from './types.js';

type ReactiveFunction = (obj: any) => any;

export function createFold(
  query: LiveQuery,
  aggregators: Record<string, AggregatorDescriptor>,
  callerReactive?: ReactiveFunction,
): [LiveFold, () => void] {
  const makeReactive = callerReactive ?? ((obj: any) => obj);
  const result = makeReactive({} as Record<string, unknown>);

  function recompute(): void {
    const records = query.all();
    const accs: Record<string, { sum: number; count: number; min: number; max: number }> = {};
    for (const name of Object.keys(aggregators)) {
      accs[name] = { sum: 0, count: 0, min: Infinity, max: -Infinity };
    }

    for (const [, rec] of records) {
      const record = rec as Record<string, unknown>;
      for (const [name, agg] of Object.entries(aggregators)) {
        const acc = accs[name]!;
        if (agg.agg === 'count') {
          acc.count++;
        } else {
          const val = record[agg.field];
          if (typeof val === 'number') {
            acc.count++;
            acc.sum += val;
            if (val < acc.min) acc.min = val;
            if (val > acc.max) acc.max = val;
          }
        }
      }
    }

    for (const [name, agg] of Object.entries(aggregators)) {
      const acc = accs[name]!;
      switch (agg.agg) {
        case 'count': result[name] = acc.count; break;
        case 'sum': result[name] = acc.sum; break;
        case 'avg': result[name] = acc.count === 0 ? 0 : acc.sum / acc.count; break;
        case 'min': result[name] = acc.count === 0 ? null : acc.min; break;
        case 'max': result[name] = acc.count === 0 ? null : acc.max; break;
      }
    }
  }

  const unsubIndex = query.onIndexChange(() => recompute());
  recompute();

  return [result as LiveFold, () => unsubIndex()];
}

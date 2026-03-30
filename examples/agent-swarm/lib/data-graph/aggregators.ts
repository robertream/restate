/**
 * Aggregator descriptor factory functions for fold expressions.
 *
 * Each function returns a plain descriptor object.
 * `computeAggregates` evaluates these descriptors over a set of records.
 *
 * @example
 * ```ts
 * fleet.fold({ total: count(), avgFuel: avg('fuel_level') }).subscribe();
 * ```
 */

export interface CountAgg { agg: 'count' }
export interface SumAgg { agg: 'sum'; field: string }
export interface AvgAgg { agg: 'avg'; field: string }
export interface MinAgg { agg: 'min'; field: string }
export interface MaxAgg { agg: 'max'; field: string }

export type AggregatorDescriptor = CountAgg | SumAgg | AvgAgg | MinAgg | MaxAgg;

export function count(): CountAgg { return { agg: 'count' }; }
export function sum(field: string): SumAgg { return { agg: 'sum', field }; }
export function avg(field: string): AvgAgg { return { agg: 'avg', field }; }
export function min(field: string): MinAgg { return { agg: 'min', field }; }
export function max(field: string): MaxAgg { return { agg: 'max', field }; }

// PascalCase aliases for use in Data() component expressions
export const Count = count;
export const Sum = sum;
export const Avg = avg;
export const Min = min;
export const Max = max;

const KNOWN_AGGS = new Set(['count', 'sum', 'avg', 'min', 'max']);

export function isAggregatorDescriptor(v: unknown): v is AggregatorDescriptor {
  return (
    typeof v === 'object' &&
    v !== null &&
    'agg' in v &&
    KNOWN_AGGS.has((v as Record<string, unknown>).agg as string)
  );
}

/**
 * Compute all aggregators over `records` in a single pass.
 * Returns an object keyed by the aggregator names.
 *
 * @example
 * computeAggregates(records, { total: count(), avgFuel: avg('fuel_level') })
 * // → { total: 5, avgFuel: 62.3 }
 */
export function computeAggregates(
  records: Array<{ value: unknown }>,
  aggregators: Record<string, AggregatorDescriptor>,
): Record<string, unknown> {
  // Initialize accumulators for each aggregator
  const accumulators: Record<string, { sum: number; count: number; min: number; max: number }> = {};
  for (const name of Object.keys(aggregators)) {
    accumulators[name] = { sum: 0, count: 0, min: Infinity, max: -Infinity };
  }

  // Single pass over all records
  for (const { value } of records) {
    const record = value as Record<string, unknown>;
    for (const [name, agg] of Object.entries(aggregators)) {
      const acc = accumulators[name]!;
      if (agg.agg === 'count') {
        acc.count++;
      } else {
        const fieldVal = record[agg.field];
        if (typeof fieldVal === 'number') {
          acc.count++; // only count records with numeric values for sum/avg/min/max
          acc.sum += fieldVal;
          if (fieldVal < acc.min) acc.min = fieldVal;
          if (fieldVal > acc.max) acc.max = fieldVal;
        }
      }
    }
  }

  // Produce final results
  const result: Record<string, unknown> = {};
  for (const [name, agg] of Object.entries(aggregators)) {
    const acc = accumulators[name]!;
    switch (agg.agg) {
      case 'count':
        result[name] = acc.count;
        break;
      case 'sum':
        result[name] = acc.sum;
        break;
      case 'avg':
        result[name] = acc.count === 0 ? 0 : acc.sum / acc.count;
        break;
      case 'min':
        result[name] = acc.count === 0 ? null : acc.min;
        break;
      case 'max':
        result[name] = acc.count === 0 ? null : acc.max;
        break;
    }
  }
  return result;
}

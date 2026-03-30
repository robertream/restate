/**
 * Using() — stock Alpine.js adapter for the using contract.
 *
 * Works with x-data (no plugin required). Same detection rules as x-using:
 * function, array cleanup tuples, auto-detect cleanup methods, plain values.
 *
 * Usage:
 *   <div x-data="Using(
 *     Data({ customers: Memory('/api/...') }),
 *     { $clock: Clock() },
 *     { $window: Window({ x: 100, y: 50, width: 800, height: 600 }) }
 *   )">
 */

import Alpine from 'alpinejs';

const CLEANUP_METHODS = ['dispose', 'destroy', 'close', 'disconnect', 'abort', 'unsubscribe'] as const;

function findCleanup(obj: unknown): (() => void) | null {
  if (!obj || typeof obj !== 'object') return null;
  for (const method of CLEANUP_METHODS) {
    if (typeof (obj as any)[method] === 'function') return () => (obj as any)[method]();
  }
  return null;
}

export function Using(...args: Record<string, unknown>[]) {
  const scope: Record<string, unknown> = {};
  const factories: Array<{ key: string; fn: (ctx: any) => unknown }> = [];
  const arrayEntries: Array<{ key: string; val: unknown[] }> = [];
  const disposers: Array<() => void> = [];

  for (const arg of args) {
    for (const [key, val] of Object.entries(arg)) {
      // Function — deferred to init (needs $el)
      if (typeof val === 'function') {
        factories.push({ key, fn: val as (ctx: any) => unknown });
        continue;
      }

      // Array — could be cleanup tuple or plain value
      if (Array.isArray(val)) {
        if (val.length === 1 && typeof val[0] === 'function') {
          // [factory] — deferred to init, return is dispose
          factories.push({ key, fn: (ctx) => {
            const dispose = val[0](ctx);
            if (typeof dispose === 'function') disposers.push(dispose);
            return undefined; // no scope binding
          }});
          continue;
        }
        if (val.length === 2 && typeof val[1] === 'function') {
          // [factory|instance, dispose] — deferred if factory
          if (typeof val[0] === 'function') {
            arrayEntries.push({ key, val });
          } else {
            scope[key] = val[0];
            const dispose = val[1] as (instance: unknown) => void;
            const instance = val[0];
            disposers.push(() => dispose(instance));
          }
          continue;
        }
        // Plain array — pass through
        scope[key] = val;
        continue;
      }

      // Object with cleanup method — auto-detect
      const autoCleanup = findCleanup(val);
      if (autoCleanup) {
        scope[key] = val;
        disposers.push(autoCleanup);
        continue;
      }

      // Plain value
      scope[key] = val;
    }
  }

  scope.init = function (this: Record<string, unknown> & { $el: HTMLElement; $refs: Record<string, HTMLElement> }) {
    const ctx = { $el: this.$el, $refs: this.$refs, $reactive: Alpine.reactive };

    // Process [factory, dispose] tuples
    for (const { key, val } of arrayEntries) {
      const instance = (val[0] as Function)(ctx);
      this[key] = instance;
      const dispose = val[1] as (instance: unknown) => void;
      disposers.push(() => dispose(instance));
    }

    // Process function factories
    for (const { key, fn } of factories) {
      const result = fn(ctx);
      // Check if result is a cleanup tuple
      if (Array.isArray(result)) {
        const resolved = resolveArrayForInit(result, ctx, disposers);
        if (resolved !== undefined) this[key] = resolved;
      } else {
        const autoCleanup = findCleanup(result);
        if (autoCleanup) disposers.push(autoCleanup);
        if (result !== undefined) this[key] = result;
      }
    }
  };

  scope.destroy = function () {
    for (const fn of disposers) fn();
  };

  return scope;
}

function resolveArrayForInit(
  val: unknown[],
  ctx: { $el: HTMLElement; $refs: Record<string, HTMLElement>; $reactive: unknown },
  disposers: Array<() => void>,
): unknown | undefined {
  if (val.length === 1 && typeof val[0] === 'function') {
    const dispose = val[0](ctx);
    if (typeof dispose === 'function') disposers.push(dispose);
    return undefined;
  }

  if (val.length === 2 && typeof val[1] === 'function') {
    const dispose = val[1] as (instance: unknown) => void;
    let instance: unknown;
    if (typeof val[0] === 'function') {
      instance = val[0](ctx);
    } else {
      instance = val[0];
    }
    disposers.push(() => dispose(instance));
    return instance;
  }

  return val;
}

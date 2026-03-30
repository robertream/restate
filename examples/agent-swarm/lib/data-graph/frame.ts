/**
 * LiveFrame — shared implementation, backend-agnostic.
 * Works with any LiveQuery via its window functions.
 */
import type { IDBKey, LiveRecord, LiveQuery, LiveFrame } from './types.js';

type ReactiveFunction = (obj: any) => any;

export function createFrame(
  query: LiveQuery,
  opts: { size: number },
  callerReactive?: ReactiveFunction,
): [LiveFrame, () => void] {
  const makeReactive = callerReactive ?? ((obj: any) => obj);

  let cursor: IDBKey | null = null;
  const cursorStack: (IDBKey | null)[] = [];

  // Reactive state so Vue tracks changes
  const state = makeReactive({
    records: [] as ReadonlyArray<[IDBKey, LiveRecord]>,
    atStart: true,
    atEnd: true,
  });

  /** Set window subscription and read results into frame state. */
  function updatePage(): void {
    let fetched: ReadonlyArray<[IDBKey, LiveRecord]>;
    if (cursor === null) {
      fetched = query.slice(0, opts.size + 1);
    } else {
      fetched = query.window(cursor, opts.size + 1);
    }
    const atEnd = fetched.length <= opts.size;
    state.records = atEnd ? fetched : fetched.slice(0, opts.size);
    state.atStart = cursorStack.length === 0;
    state.atEnd = atEnd;
  }

  // When the subscription fires with new data, re-read the page
  const unsubIndex = query.onIndexChange(() => {
    updatePage();
  });

  // Initial page
  updatePage();

  const frame: LiveFrame = {
    query,

    get records(): ReadonlyArray<[IDBKey, LiveRecord]> {
      return state.records;
    },

    get size(): number {
      return opts.size;
    },

    get atStart(): boolean {
      return state.atStart;
    },

    get atEnd(): boolean {
      return state.atEnd;
    },

    first(): void {
      if (state.atStart) return;
      cursor = null;
      cursorStack.length = 0;
      updatePage();
    },

    prev(): void {
      if (cursorStack.length === 0) return;
      cursor = cursorStack.pop() ?? null;
      updatePage();
    },

    next(): void {
      if (state.atEnd || state.records.length === 0) return;
      cursorStack.push(cursor);
      cursor = state.records[state.records.length - 1]![0];
      updatePage();
    },

    last(): void {
      if (state.atEnd) return;
      while (!state.atEnd && state.records.length > 0) {
        cursorStack.push(cursor);
        cursor = state.records[state.records.length - 1]![0];
        updatePage();
      }
    },
  };

  function dispose(): void {
    unsubIndex();
  }

  return [frame, dispose];
}

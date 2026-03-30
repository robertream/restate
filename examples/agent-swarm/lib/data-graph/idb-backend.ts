/**
 * IDB backend for Data() — dynamically imported when IDB() is called.
 * Keeps Dexie out of the main bundle unless IDB sources are used.
 */
import { IDBStore, createIDBSource } from './idb.js';
import type { KvInstruction } from './types.js';

export const idbBackend = {
  create(
    name: string,
    url: string,
    reactive: (obj: any) => any,
    parseKvEvent: (data: string) => KvInstruction[],
  ): [source: unknown, dispose: () => void] {
    const store = new IDBStore(name);
    const [source, disposeSource] = createIDBSource(store, reactive);

    const es = new EventSource(url);
    let pending = Promise.resolve();

    es.onopen = () => {
      (source as any).status = 'connected';
      (source as any).connected = true;
    };
    es.onerror = () => {
      const status = es.readyState === EventSource.CONNECTING ? 'reconnecting' : 'error';
      (source as any).status = status;
      (source as any).connected = false;
    };
    es.onmessage = (event: MessageEvent) => {
      pending = pending.then(async () => {
        const instructions = parseKvEvent(event.data as string);
        for (const instruction of instructions) {
          if (instruction.op === 'ASN') {
            for (const [key, value] of Object.entries(instruction.data)) {
              await store.put(key, value as Record<string, unknown>);
            }
          } else if (instruction.op === 'DEL') {
            for (const key of instruction.data) await store.delete(key);
          } else if (instruction.op === 'CLR') {
            await store.clear();
          } else if (instruction.op === 'RPL') {
            await store.clear();
            for (const [key, value] of Object.entries(instruction.data)) {
              await store.put(key, value as Record<string, unknown>);
            }
          }
        }
      });
    };

    return [source, () => { disposeSource(); es.close(); }];
  },
};

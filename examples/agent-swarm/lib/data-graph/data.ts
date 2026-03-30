/**
 * Data() — config translator for the next-gen component API.
 *
 * Takes descriptor config, creates stores with SSE connections,
 * returns scope object with [LiveSource, dispose] tuples for v-using.
 *
 * Memory backend is built-in. IDB (Dexie) is dynamically imported
 * when IDB() descriptor factory is called — by the time Data()
 * processes the config, the module is already loading.
 */
import { MemoryStore, createMemorySource } from './memory.js';
import { parseKvEvent } from './kv-sync-parser.js';

type ReactiveFunction = (obj: any) => any;

// --- Descriptor types ---

export interface MemoryDescriptor { type: 'memory'; url: string }
export interface IDBDescriptor { type: 'idb'; url: string; backend: Promise<typeof import('./idb-backend.js')> }
export interface IndexDescriptor { type: 'index'; spec: Record<string, string> }
export interface RelationDescriptor { type: 'relation'; spec: Record<string, Record<string, string>> }

export type Descriptor = MemoryDescriptor | IDBDescriptor | IndexDescriptor | RelationDescriptor;

// --- Factories ---

export function Memory(url: string): MemoryDescriptor { return { type: 'memory', url }; }
export function IDB(url: string): IDBDescriptor { return { type: 'idb', url, backend: import('./idb-backend.js') }; }
export function Index(spec: Record<string, string>): IndexDescriptor { return { type: 'index', spec }; }
export function Relation(spec: Record<string, Record<string, string>>): RelationDescriptor { return { type: 'relation', spec }; }

// --- SSE connection for Memory ---

function connectMemorySSE(
  url: string,
  store: MemoryStore,
  onStatusChange: (status: string) => void,
): () => void {
  const es = new EventSource(url);

  es.onopen = () => onStatusChange('connected');
  es.onerror = () => {
    onStatusChange(es.readyState === EventSource.CONNECTING ? 'reconnecting' : 'error');
  };

  es.onmessage = (event: MessageEvent) => {
    const instructions = parseKvEvent(event.data as string);
    for (const instruction of instructions) {
      if (instruction.op === 'ASN') {
        for (const [key, value] of Object.entries(instruction.data)) {
          store.put(key, value as Record<string, unknown>);
        }
      } else if (instruction.op === 'DEL') {
        for (const key of instruction.data) store.delete(key);
      } else if (instruction.op === 'CLR') {
        store.clear();
      } else if (instruction.op === 'RPL') {
        store.clear();
        for (const [key, value] of Object.entries(instruction.data)) {
          store.put(key, value as Record<string, unknown>);
        }
      }
    }
    store.notify();
  };

  return () => es.close();
}

// --- Data() ---

export function Data(
  config: Record<string, Descriptor>,
  options?: { reactive: ReactiveFunction },
): Record<string, unknown> {
  const reactive: ReactiveFunction = options?.reactive ?? ((obj) => obj);
  const scope: Record<string, unknown> = {};

  // Derive IDB database name from page URL
  const dbPrefix = typeof location !== 'undefined'
    ? (location.hostname + location.pathname).replace(/[^a-zA-Z0-9]/g, '-').replace(/-+/g, '-').replace(/^-|-$/g, '') || 'data-graph'
    : 'data-graph';

  for (const [key, descriptor] of Object.entries(config)) {
    if (descriptor.type === 'memory') {
      const store = new MemoryStore();
      const [source, disposeSource] = createMemorySource(store, reactive);
      const closeSSE = connectMemorySSE(descriptor.url, store, (status) => {
        (source as any).status = status;
        (source as any).connected = status === 'connected';
      });
      scope[key] = [source, () => { disposeSource(); closeSSE(); }];
    } else if (descriptor.type === 'idb') {
      // Placeholder — wired up when dynamic import resolves
      const placeholder = reactive({ status: 'loading', connected: false });
      let disposeIDB = () => {};
      descriptor.backend.then(({ idbBackend }) => {
        const [source, dispose] = idbBackend.create(`${dbPrefix}:${key}`, descriptor.url, reactive, parseKvEvent);
        Object.assign(placeholder, source);
        disposeIDB = dispose;
      }).catch((err) => {
        placeholder.status = 'error';
        console.error(`[data-graph] Failed to load IDB backend for "${key}":`, err);
      });
      scope[key] = [placeholder, () => disposeIDB()];
    }
  }

  return scope;
}

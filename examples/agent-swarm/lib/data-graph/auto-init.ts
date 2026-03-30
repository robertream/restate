/**
 * Auto-init for petite-vue.
 * Uses patched petite-vue with v-using support (function, array, auto-detect).
 */
// @ts-ignore — patched petite-vue
import { createApp, reactive } from './petite-vue-patched.js';
import { Data, Memory, IDB, Index, Relation } from './data.js';
import { count as Count, sum as Sum, avg as Avg, min as Min, max as Max } from './aggregators.js';
import { FormComponent, FieldComponent, type LiveForm } from './form.js';
import { WindowComponent } from './window.js';
import { ClockComponent } from './clock.js';

const raw = {} as Record<string, unknown>;
const withHasTrap = new Proxy(raw, {
  has(target, key) {
    if (key in target) return true;
    if (typeof key === 'string' && key in globalThis) return false;
    return true;
  },
});

const scope = reactive(withHasTrap);
const app = createApp(scope);

// Component factories — reactive injected via v-using ctx.$reactive
raw.Clock = (opts?: any) => ClockComponent(opts);
raw.Window = (initial: any) => WindowComponent(initial as Record<string, unknown>);

// Factories that still need reactive at construction
raw.Data = (config: Record<string, unknown>) => Data(config as any, { reactive });
raw.Form = (record: Record<string, unknown> | null | undefined, url: string) => FormComponent(reactive, record, url);
raw.Field = (form: LiveForm | null, fieldName: string, template?: string) => FieldComponent(form, fieldName, template);

// Pure descriptors / aggregators
raw.Memory = Memory;
raw.IDB = IDB;
raw.Index = Index;
raw.Relation = Relation;
raw.Count = Count;
raw.Sum = Sum;
raw.Avg = Avg;
raw.Min = Min;
raw.Max = Max;

const mountTarget = document.getElementById('app') ?? document.querySelector('[v-scope]') ?? undefined;
app.mount(mountTarget);

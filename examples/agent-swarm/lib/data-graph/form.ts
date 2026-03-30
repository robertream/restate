/**
 * Form/Field components for petite-vue v-scope usage.
 * Inline editing via petite-vue v-scope + $template.
 */

export interface LiveField {
  value: unknown;
  readonly existing: unknown;
  readonly dirty: boolean;
  error: string | null;
  revert(): void;
}

export interface LiveForm {
  readonly dirty: boolean;
  readonly saving: boolean;
  readonly errors: boolean;
  submit(): Promise<void>;
  revert(): void;
  submitIfDirty(): Promise<void>;
}

export interface FormScope {
  form: LiveForm | null;
  readonly dirty: boolean;
  readonly saving: boolean;
  readonly errors: boolean;
  submit(): Promise<void>;
  revert(): void;
  submitIfDirty(): Promise<void>;
  Field(fieldName: string, template?: string): { $template: string; field: LiveField };
}

export function createLiveForm(
  getRecord: () => Record<string, unknown> | null,
  url: string,
  reactive: (obj: any) => any,
): LiveForm {
  const record = getRecord();
  if (!record) throw new Error('createLiveForm: record not found');

  // Clone record fields (exclude $ traversal method) for the editable values layer
  const fieldNames: string[] = [];
  const initialValues: Record<string, unknown> = {};
  for (const k of Object.keys(record)) {
    if (k === '$') continue;
    fieldNames.push(k);
    initialValues[k] = record[k];
  }

  const values = reactive({ ...initialValues });
  const errors = reactive({} as Record<string, string | null>);
  const formState = reactive({ saving: false });

  const fieldCache = new Map<string, LiveField>();

  function getField(name: string): LiveField {
    if (!fieldCache.has(name)) {
      const field: LiveField = {
        get value() { return values[name]; },
        set value(v: unknown) { values[name] = v; },
        get existing() { return getRecord()?.[name]; },
        get dirty() { return values[name] !== getRecord()?.[name]; },
        get error() { return errors[name] ?? null; },
        set error(e: string | null) { errors[name] = e; },
        revert() { const r = getRecord(); if (r) values[name] = r[name]; errors[name] = null; },
      };
      fieldCache.set(name, field);
    }
    return fieldCache.get(name)!;
  }

  async function submitFn() {
    if (formState.saving) return;
    const body: Record<string, unknown> = {};
    for (const f of fieldNames) {
      body[f] = values[f];
      errors[f] = null;
    }

    formState.saving = true;
    try {
      const res = await fetch(url, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        if (err.fields) {
          for (const [name, msg] of Object.entries(err.fields)) {
            errors[name] = msg as string;
          }
        }
      }
      // On success: SSE round-trip will update the LiveRecord,
      // which makes field.dirty false naturally via getRecord().
    } catch (_e) {
      // Network error — silent for now
    } finally {
      formState.saving = false;
    }
  }

  function revertFn() {
    const r = getRecord();
    if (!r) return;
    for (const f of fieldNames) {
      values[f] = r[f];
      errors[f] = null;
    }
  }

  async function submitIfDirtyFn() {
    const r = getRecord();
    if (!r) return;
    for (const f of fieldNames) {
      if (values[f] !== r[f]) {
        await submitFn();
        return;
      }
    }
  }

  // Proxy-based LiveForm: named props return form-level state/methods,
  // any other string key returns a LiveField accessor.
  const FORM_PROPS: Record<string, () => unknown> = {
    dirty: () => { const r = getRecord(); return r ? fieldNames.some(f => values[f] !== r[f]) : false; },
    saving: () => formState.saving,
    errors: () => fieldNames.some(f => errors[f]),
    submit: () => submitFn,
    revert: () => revertFn,
    submitIfDirty: () => submitIfDirtyFn,
  };

  return new Proxy({} as LiveForm, {
    has(_target, prop) {
      if (typeof prop !== 'string' || prop.startsWith('__')) return false;
      return prop in FORM_PROPS || fieldNames.includes(prop);
    },
    get(_target, prop) {
      if (typeof prop !== 'string' || prop.startsWith('__')) return undefined;
      if (prop in FORM_PROPS) return FORM_PROPS[prop]!();
      if (fieldNames.includes(prop)) return getField(prop);
      return undefined;
    },
  });
}

const nullField: LiveField = {
  value: null,
  get existing() { return null; },
  get dirty() { return false; },
  error: null,
  revert() {},
};

export function FormComponent(
  reactive: (obj: any) => any,
  record: Record<string, unknown> | null | undefined,
  url: string,
): FormScope {
  const getRecord = () => (record as Record<string, unknown>) ?? null;
  const noop = async () => {};
  if (!record) {
    return {
      form: null,
      dirty: false,
      saving: false,
      errors: false,
      submit: noop,
      revert() {},
      submitIfDirty: noop,
      Field(fieldName: string, template?: string) { return FieldComponent(null, fieldName, template); },
    };
  }
  const form = createLiveForm(getRecord, url, reactive);
  return {
    form,
    get dirty() { return form.dirty; },
    get saving() { return form.saving; },
    get errors() { return form.errors; },
    submit() { return form.submit(); },
    revert() { return form.revert(); },
    submitIfDirty() { return form.submitIfDirty(); },
    Field(fieldName: string, template?: string) {
      return FieldComponent(form, fieldName, template);
    },
  };
}

export function FieldComponent(
  form: LiveForm | null,
  fieldName: string,
  template?: string,
): { $template: string; field: LiveField } {
  return {
    $template: template || '#data-field',
    field: form ? (form as any)[fieldName] as LiveField : nullField,
  };
}

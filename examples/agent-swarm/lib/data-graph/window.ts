/**
 * Window component — framework-agnostic via function form.
 * Manages drag, resize, minimize, maximize for a positioned element.
 * State is local-only (no server persistence).
 *
 * Two orthogonal states:
 *   visibility: 'visible' | 'minimized' | 'closed'
 *   maximized:  boolean (layout when visible — false means normal/floating)
 *
 * Usage:
 *   <div v-using="Window({...})">
 *     <div class="window" v-show="$window.visible">
 *   <div x-using="Window({...})">
 *     <div class="window" x-show="$window.visible">
 *
 * Factory runs after scope injection (like x-data's init), so children
 * with .w-move/.w-resize-* handles are already in the DOM. Mounts immediately.
 */

export interface WindowState {
  x: number;
  y: number;
  width: number;
  height: number;
}

export type WindowVisibility = 'visible' | 'minimized' | 'closed';

export interface WindowApi {
  readonly state: WindowState;
  visibility: WindowVisibility;
  maximized: boolean;
  readonly dragging: boolean;
  readonly minimized: boolean;
  readonly closed: boolean;
  readonly visible: boolean;
  readonly style: Record<string, string>;
  move(e: PointerEvent): void;
  resize(direction: string, e: PointerEvent): void;
  toggleMax(): void;
  toggleMin(): void;
  restore(): void;
  close(): void;
  mount(el: HTMLElement): void;
}

const MIN_WIDTH = 400;
const MIN_HEIGHT = 300;

export function WindowComponent(
  initial: Record<string, unknown>,
) {
  return (ctx: { $el: Element; $reactive: (obj: any) => any }) => {
      const win = ctx.$reactive({
        x: Number(initial.x),
        y: Number(initial.y),
        width: Number(initial.width),
        height: Number(initial.height),
      });

      const uiState = ctx.$reactive({
        dragging: false,
        visibility: 'visible' as WindowVisibility,
        maximized: false,
      });
      let mountedEl: HTMLElement | null = null;
      let activeDragCleanup: (() => void) | null = null;

      function computeStyle(): Record<string, string> {
        if (uiState.maximized) {
          return { position: 'fixed', left: '-3px', top: '-3px', right: '-3px', width: 'auto', height: 'calc(100vh - 34px + 3px - 6px)' };
        }
        return { position: '', left: win.x + 'px', top: win.y + 'px', right: '', width: win.width + 'px', height: win.height + 'px' };
      }

      function applyStyle() {
        if (!mountedEl) return;
        Object.assign(mountedEl.style, computeStyle());
      }

      function syncClasses() {
        if (!mountedEl) return;
        mountedEl.classList.toggle('w-maximized', uiState.maximized);
        mountedEl.classList.toggle('w-minimized', uiState.visibility === 'minimized');
        mountedEl.classList.toggle('w-closed', uiState.visibility === 'closed');
      }

      function mount(el: HTMLElement) {
        mountedEl = el;
        el.classList.add('w-window');
        applyStyle();

        el.querySelectorAll('.w-move, [class*="w-resize-"], .w-minimize, .w-maximize, .w-close').forEach((child) => {
          for (const cls of Array.from(child.classList)) {
            if (cls === 'w-move') {
              child.addEventListener('pointerdown', (e) => api.move(e as PointerEvent));
            } else if (cls === 'w-minimize') {
              child.addEventListener('click', () => api.toggleMin());
            } else if (cls === 'w-maximize') {
              child.addEventListener('click', () => api.toggleMax());
            } else if (cls === 'w-close') {
              child.addEventListener('click', () => api.close());
            } else if (cls.startsWith('w-resize-')) {
              const dir = cls.slice('w-resize-'.length);
              if (dir) {
                child.addEventListener('pointerdown', (e) => api.resize(dir, e as PointerEvent));
              }
            }
          }
        });
      }

      function dispose() {
        if (activeDragCleanup) activeDragCleanup();
        mountedEl = null;
      }

      const api: WindowApi = {
        state: win,

        get visibility() { return uiState.visibility; },
        set visibility(v: WindowVisibility) { uiState.visibility = v; syncClasses(); applyStyle(); },
        get maximized() { return uiState.maximized; },
        set maximized(v: boolean) { uiState.maximized = v; syncClasses(); applyStyle(); },
        get dragging() { return uiState.dragging; },
        get minimized() { return uiState.visibility === 'minimized'; },
        get closed() { return uiState.visibility === 'closed'; },
        get visible() { return uiState.visibility === 'visible'; },

        get style(): Record<string, string> {
          return computeStyle();
        },

        toggleMax() {
          uiState.maximized = !uiState.maximized;
          uiState.visibility = 'visible';
          syncClasses();
          applyStyle();
        },

        toggleMin() {
          if (uiState.visibility === 'closed') return;
          uiState.visibility = uiState.visibility === 'minimized' ? 'visible' : 'minimized';
          syncClasses();
          applyStyle();
        },

        restore() {
          uiState.visibility = 'visible';
          syncClasses();
          applyStyle();
        },

        close() {
          uiState.visibility = 'closed';
          syncClasses();
        },

        move(e: PointerEvent) {
          if (uiState.dragging || uiState.maximized) return;
          e.preventDefault();
          const offsetX = e.clientX - win.x;
          const offsetY = e.clientY - win.y;
          uiState.dragging = true;

          function onMove(ev: PointerEvent) {
            win.x = ev.clientX - offsetX;
            win.y = ev.clientY - offsetY;
            applyStyle();
          }

          function onUp() {
            document.removeEventListener('pointermove', onMove);
            document.removeEventListener('pointerup', onUp);
            activeDragCleanup = null;
            uiState.dragging = false;
          }

          document.addEventListener('pointermove', onMove);
          document.addEventListener('pointerup', onUp);
          activeDragCleanup = onUp;
        },

        resize(direction: string, e: PointerEvent) {
          if (uiState.dragging || uiState.maximized) return;
          e.preventDefault();
          const sx = e.clientX;
          const sy = e.clientY;
          const sl = win.x;
          const st = win.y;
          const sw = win.width;
          const sh = win.height;
          uiState.dragging = true;

          function onMove(ev: PointerEvent) {
            const dx = ev.clientX - sx;
            const dy = ev.clientY - sy;
            let l = sl, t = st, w = sw, h = sh;

            if (direction.includes('e')) w = Math.max(MIN_WIDTH, sw + dx);
            if (direction.includes('w')) { w = Math.max(MIN_WIDTH, sw - dx); l = sl + sw - w; }
            if (direction.includes('s')) h = Math.max(MIN_HEIGHT, sh + dy);
            if (direction.includes('n')) { h = Math.max(MIN_HEIGHT, sh - dy); t = st + sh - h; }

            win.x = l;
            win.y = t;
            win.width = w;
            win.height = h;
            applyStyle();
          }

          function onUp() {
            document.removeEventListener('pointermove', onMove);
            document.removeEventListener('pointerup', onUp);
            activeDragCleanup = null;
            uiState.dragging = false;
          }

          document.addEventListener('pointermove', onMove);
          document.addEventListener('pointerup', onUp);
          activeDragCleanup = onUp;
        },

        mount,
      };

      return [api, dispose];
  };
}

/**
 * Clock component — framework-agnostic via function form.
 * Returns a factory that receives ctx with $reactive for state wrapping.
 *
 * Usage: <div v-using="Clock()">{{ $clock.time }}</div>
 *        <div x-using="Clock()" x-text="$clock.time"></div>
 */

export function ClockComponent(
  opts: { interval?: number; format?: Intl.DateTimeFormatOptions } = {},
) {
  const {
    interval = 30000,
    format = { hour: '2-digit', minute: '2-digit' },
  } = opts;

  return {
    $clock: (ctx: { $el: Element; $reactive: (obj: any) => any }) => {
      const state = ctx.$reactive({ time: new Date().toLocaleTimeString([], format) });
      const id = setInterval(() => {
        state.time = new Date().toLocaleTimeString([], format);
      }, interval);
      return [state, () => clearInterval(id)];
    },
  };
}

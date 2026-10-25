// @ts-check
/**
 * The listeners, timers and frames one core instance owns, released together by `dispose()`;
 * nothing registers once it ran.
 */
export function createViewerLifecycle() {
  let disposed = false;
  /** @type {Array<[EventTarget, string, EventListenerOrEventListenerObject, (boolean|AddEventListenerOptions|undefined)]>} */
  const listeners = [];
  /** @type {Set<ReturnType<typeof setTimeout>>} */
  const timeouts = new Set();
  /** @type {Set<ReturnType<typeof setInterval>>} */
  const intervals = new Set();
  /** @type {Set<number>} */
  const frames = new Set();
  return {
    get disposed() { return disposed; },
    /**
     * @param {EventTarget} target
     * @param {string} type
     * @param {EventListenerOrEventListenerObject} listener
     * @param {boolean|AddEventListenerOptions} [options]
     */
    listen(target, type, listener, options) {
      if (disposed) return;
      target.addEventListener(type, listener, options);
      listeners.push([target, type, listener, options]);
    },
    /** @param {(...args: any[]) => void} callback @param {number} [delay] @param {...any} args */
    timeout(callback, delay, ...args) {
      if (disposed) return null;
      const id = globalThis.setTimeout(() => {
        timeouts.delete(id);
        if (!disposed) callback(...args);
      }, delay);
      timeouts.add(id);
      return id;
    },
    /** @param {(...args: any[]) => void} callback @param {number} [delay] @param {...any} args */
    interval(callback, delay, ...args) {
      if (disposed) return null;
      const id = globalThis.setInterval(() => { if (!disposed) callback(...args); }, delay);
      intervals.add(id);
      return id;
    },
    /** @param {FrameRequestCallback} callback */
    frame(callback) {
      if (disposed) return null;
      const id = globalThis.requestAnimationFrame(time => {
        frames.delete(id);
        if (!disposed) callback(time);
      });
      frames.add(id);
      return id;
    },
    dispose() {
      if (disposed) return;
      disposed = true;
      for (const [target, type, listener, options] of listeners) target.removeEventListener(type, listener, options);
      for (const id of timeouts) globalThis.clearTimeout(id);
      for (const id of intervals) globalThis.clearInterval(id);
      for (const id of frames) globalThis.cancelAnimationFrame(id);
      listeners.length = 0;
      timeouts.clear();
      intervals.clear();
      frames.clear();
    },
  };
}

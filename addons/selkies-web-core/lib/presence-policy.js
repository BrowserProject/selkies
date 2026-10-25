// @ts-check
//
// When is a viewer still "here"? The server has no idle concept any more: it
// only sees whether a workspace socket is connected, and it keeps the session
// alive while one is. So the client decides, and it decides the same way in
// both viewers: a tab that has been hidden for a while, or one nobody has
// touched for a long while, gives the session back (stream cores stopped AND
// workspace socket closed) instead of holding a container open for nobody.
// Page work that runs without input (a file transfer) holds presence meanwhile.
//
// Pure DOM, no framework: the timers, the document and `now` are injectable so
// the policy can be tested in node against a fake document.

/** Continuously hidden for this long: the session is handed back. */
export const HIDDEN_DISCONNECT_MS = 300000;
/** No pointer, key, touch or wheel for this long: the same. */
export const INACTIVE_DISCONNECT_MS = 3600000;
/** At most one inactivity re-arm per this window; pointermove fires constantly. */
export const ACTIVITY_THROTTLE_MS = 1000;
/** Input on the viewer surface. Keys are watched on the WINDOW in the capture
 *  phase instead: `lib/input.js` takes keydown on the window with capture and
 *  stops its propagation, and the mobile controls dispatch synthetic key events
 *  on the window, so nothing keyboard-shaped ever reaches the document. */
export const ACTIVITY_EVENTS = ['pointerdown', 'pointermove', 'touchstart', 'wheel'];
/** Page Lifecycle events that mean the page is off screen even while
 *  `document.hidden` still reads false. `freeze` fires on the document when the
 *  browser suspends a backgrounded tab (Chromium), `pagehide` on the window
 *  when the document is being left. Both are hidden as far as this policy is
 *  concerned: a frozen page paints nothing and its timers do not run, so a tab
 *  that is frozen before `visibilitychange` lands would otherwise sit there
 *  holding the session open with no deadline armed at all. */
/** @type {Array<['doc'|'win', string]>} */
export const HIDE_EVENTS = [['doc', 'freeze'], ['win', 'pagehide']];
/** ... and the way back: `resume` on the document, `pageshow` on the window
 *  (including the bfcache restore, which carries `persisted: true`). */
/** @type {Array<['doc'|'win', string]>} */
export const SHOW_EVENTS = [['doc', 'resume'], ['win', 'pageshow']];
/** Page work that needs the session while nobody touches the stream (a file
 *  upload, a save from the session) dispatches this CustomEvent on the window
 *  with `detail: { key, until }`: `until` is epoch ms (`Date.now()`), and 0
 *  or any past time releases that key. While any hold lasts the person counts
 *  as present. Holders re-dispatch while their work runs: a watcher started
 *  later (a reconnect, a new transport) only learns of a hold from its next
 *  event. */
export const PRESENCE_HOLD_EVENT = 'viewer-presence-hold';
/** The longest delay a browser timer takes (2^31 - 1 ms); a later hold end is
 *  checked again at this distance instead of overflowing to an instant fire. */
const MAX_TIMER_DELAY_MS = 2147483647;

/**
 * Watches presence and reports the two transitions that matter.
 *
 * @param {Object} options
 * @param {EventTarget} [options.target] The viewer surface input is watched on
 *   (defaults to the document).
 * @param {(reason: 'hidden'|'inactive', idleMs: number) => void} [options.onAway]
 *   Fired once, when the first threshold is crossed, with how long the surface
 *   had gone without input. Nothing else fires until `resume()` or a return to
 *   a visible document.
 * @param {(reason: 'hidden'|'inactive') => void} [options.onBack] Fired when
 *   the document becomes visible again after an away. The watcher is already
 *   armed again when it runs.
 * @param {(hidden: boolean) => void} [options.onVisibility] Every CHANGE of the
 *   effective visibility (`visibilitychange`, plus the lifecycle events above),
 *   away or not: this is the viewer's ONE visibility listener, so whatever else
 *   the page does on hidden/visible hangs here.
 * @param {Document} [options.doc]
 * @param {Window} [options.win] Where keys, the page lifecycle and presence
 *   holds are watched.
 * @param {() => number} [options.now] Epoch ms, the clock hold ends are
 *   given in.
 * @param {(fn: () => void, ms: number) => unknown} [options.schedule]
 * @param {(id: unknown) => void} [options.cancel]
 * @param {number} [options.hiddenMs]
 * @param {number} [options.inactiveMs]
 * @param {number} [options.throttleMs]
 * @returns {{stop: () => void, resume: () => void, notifyActivity: () => void,
 *   isAway: () => boolean, getReason: () => ('hidden'|'inactive'|null),
 *   getIdleMs: () => number, isHeld: () => boolean,
 *   setInactiveMs: (ms: number) => void}}
 */
export function watchPresence(options = {}) {
  const {
    onAway = () => {},
    onBack = () => {},
    onVisibility = () => {},
    doc = typeof document !== 'undefined' ? document : null,
    win = typeof window !== 'undefined' ? window : null,
    now = () => Date.now(),
    // The thresholds are minutes long, so these timers must never be the reason
    // a process (a node test, a headless run) stays alive: node's handle knows
    // how to step out of the way, the browser's number has nothing to unref.
    schedule = (fn, ms) => {
      const id = setTimeout(fn, ms);
      const handle = /** @type {{unref?: () => void}} */ (/** @type {unknown} */ (id));
      if (handle && typeof handle.unref === 'function') handle.unref();
      return id;
    },
    cancel = (id) => clearTimeout(/** @type {ReturnType<typeof setTimeout>} */ (id)),
    hiddenMs = HIDDEN_DISCONNECT_MS,
    throttleMs = ACTIVITY_THROTTLE_MS,
  } = options;
  /** Can change while watching: the page may learn the person's window only after the stream started. */
  let inactiveMs = options.inactiveMs ?? INACTIVE_DISCONNECT_MS;
  const target = options.target || doc;

  /** null while the person is here; the reason once they are not. @type {'hidden'|'inactive'|null} */
  let away = null;
  /** @type {unknown} */
  let hiddenTimer = null;
  /** @type {unknown} */
  let inactiveTimer = null;
  let armedAt = -Infinity;
  let stopped = false;
  /** A lifecycle event said the page is off screen; `document.hidden` may not
   *  agree, and the effective answer is the OR of the two. */
  let frozen = false;
  /** The last effective visibility reported, so a freeze on an already hidden
   *  tab is not a second transition. */
  let lastHidden = false;
  /** When the surface last saw input; the idle span reported with an away. */
  let lastActivityAt = now();
  /** The idle span frozen at the moment of the away, so it does not keep
   *  growing while the overlay is up. */
  let awayIdleMs = 0;
  /** Presence holds by key: when each ends, epoch ms. @type {Map<string, number>} */
  const holds = new Map();
  /** A hold is running: neither deadline is armed, and none fires. */
  let held = false;
  /** @type {unknown} */
  let holdTimer = null;
  /** @type {Array<[EventTarget, string, (event: any) => void, AddEventListenerOptions]>} */
  const listeners = [];

  const isHidden = () => frozen || !!(doc && doc.hidden);

  const clearHidden = () => {
    if (hiddenTimer === null) return;
    cancel(hiddenTimer);
    hiddenTimer = null;
  };

  const clearInactive = () => {
    if (inactiveTimer === null) return;
    cancel(inactiveTimer);
    inactiveTimer = null;
  };

  /** @param {'hidden'|'inactive'} reason */
  function goAway(reason) {
    if (away || stopped || held) return;
    away = reason;
    awayIdleMs = Math.max(0, now() - lastActivityAt);
    clearHidden();
    clearInactive();
    onAway(reason, awayIdleMs);
  }

  /** Arms the inactivity deadline `delayMs` from now (the whole window by default). */
  function armInactive(delayMs = inactiveMs) {
    if (away || stopped || held) return;
    clearInactive();
    armedAt = now();
    inactiveTimer = schedule(() => {
      inactiveTimer = null;
      goAway('inactive');
    }, Math.max(0, delayMs));
  }

  /** A new inactivity window, counted from the last input rather than from now. @param {number} ms */
  function setInactiveMs(ms) {
    if (stopped || !Number.isFinite(ms) || ms <= 0 || ms === inactiveMs) return;
    inactiveMs = ms;
    if (inactiveTimer === null) return;
    armInactive(lastActivityAt + inactiveMs - now());
  }

  function armHidden() {
    if (away || stopped || held || hiddenTimer !== null) return;
    hiddenTimer = schedule(() => {
      hiddenTimer = null;
      goAway('hidden');
    }, hiddenMs);
  }

  /** Any input on the surface. Throttled: one re-arm per throttle window is
   *  enough for a threshold measured in minutes. */
  function notifyActivity() {
    if (away || stopped) return;
    lastActivityAt = now();
    if (now() - armedAt < throttleMs) return;
    armInactive();
  }

  /** The one place the effective visibility is acted on: `visibilitychange` and
   *  the lifecycle events all land here. */
  function applyVisibility() {
    if (stopped) return;
    const hidden = isHidden();
    if (hidden !== lastHidden) {
      lastHidden = hidden;
      onVisibility(hidden);
    }
    if (hidden) {
      armHidden();
      return;
    }
    clearHidden();
    // Only a viewer that left because the TAB went away comes back by itself.
    // An inactive one waits for the button: otherwise a visibility round trip
    // (visible, then hidden long enough, then visible) would quietly reconnect
    // a session nobody has touched.
    if (away !== 'hidden') return;
    resume();
    onBack('hidden');
  }

  /** `freeze`, or a `pagehide` that is not a bfcache suspension. A persisted
   *  pagehide is followed by `freeze` where the browser has one, and its timers
   *  are suspended meanwhile, so it needs no deadline of its own.
   * @param {Event & {persisted?: boolean}} event */
  function onHideEvent(event) {
    if (stopped) return;
    if (event && event.type === 'pagehide' && event.persisted !== false) return;
    if (frozen) return;
    frozen = true;
    applyVisibility();
  }

  function onShowEvent() {
    if (stopped || !frozen) return;
    frozen = false;
    applyVisibility();
  }

  /** Drops the holds that have ended; answers when the last running one ends,
   *  or 0 when none runs. */
  function pruneHolds() {
    const at = now();
    let latest = 0;
    for (const [key, until] of holds) {
      if (until > at) latest = Math.max(latest, until);
      else holds.delete(key);
    }
    return latest;
  }

  /** The one place a hold starts or ends. Both deadlines start over from the
   *  moment the last hold ends, not from the input or the hide before it. */
  function applyHolds() {
    if (holdTimer !== null) {
      cancel(holdTimer);
      holdTimer = null;
    }
    if (stopped) return;
    const latest = pruneHolds();
    if (latest > 0) {
      held = true;
      clearHidden();
      clearInactive();
      holdTimer = schedule(() => {
        holdTimer = null;
        applyHolds();
      }, Math.min(latest - now(), MAX_TIMER_DELAY_MS));
      return;
    }
    if (!held) return;
    held = false;
    armInactive();
    if (isHidden()) armHidden();
  }

  /** @param {Event & {detail?: {key?: unknown, until?: unknown}}} event */
  function onHoldEvent(event) {
    if (stopped) return;
    const detail = event && event.detail;
    if (!detail || typeof detail.key !== 'string' || !detail.key) return;
    if (typeof detail.until !== 'number' || !Number.isFinite(detail.until)) return;
    if (detail.until > now()) holds.set(detail.key, detail.until);
    else holds.delete(detail.key);
    applyHolds();
  }

  /** Back in the session: watch from zero again. */
  function resume() {
    if (stopped) return;
    away = null;
    lastActivityAt = now();
    clearHidden();
    armInactive();
    if (isHidden()) armHidden();
  }

  /**
   * @param {EventTarget|null|undefined} node
   * @param {string} type
   * @param {(event: any) => void} handler
   * @param {AddEventListenerOptions} [options]
   */
  function listen(node, type, handler, options = { passive: true }) {
    if (!node || typeof node.addEventListener !== 'function') return;
    node.addEventListener(type, handler, options);
    listeners.push([node, type, handler, options]);
  }

  const nodes = { doc, win };
  if (doc) listen(doc, 'visibilitychange', applyVisibility);
  for (const [which, type] of HIDE_EVENTS) listen(nodes[which], type, onHideEvent);
  for (const [which, type] of SHOW_EVENTS) listen(nodes[which], type, onShowEvent);
  for (const type of ACTIVITY_EVENTS) listen(target, type, notifyActivity);
  if (win) listen(win, 'keydown', notifyActivity, { capture: true, passive: true });
  listen(win, PRESENCE_HOLD_EVENT, onHoldEvent);

  lastHidden = isHidden();
  armInactive();
  if (lastHidden) armHidden();

  return {
    notifyActivity,
    resume,
    isAway: () => away !== null,
    getReason: () => away,
    /** How long the surface has gone without input; frozen at the away. */
    getIdleMs: () => (away ? awayIdleMs : Math.max(0, now() - lastActivityAt)),
    /** Whether a presence hold keeps both deadlines off right now. */
    isHeld: () => held,
    setInactiveMs,
    stop() {
      if (stopped) return;
      stopped = true;
      clearHidden();
      clearInactive();
      if (holdTimer !== null) cancel(holdTimer);
      holdTimer = null;
      holds.clear();
      held = false;
      for (const [node, type, handler, options] of listeners) {
        if (typeof node.removeEventListener === 'function') node.removeEventListener(type, handler, options);
      }
      listeners.length = 0;
    },
  };
}

// @ts-check
//
// The cores' channel to the page logger. What the console alone would swallow
// (a codec this browser cannot decode, a refused token, a server verdict, a
// decoder crash about to reload the page) is posted to the page at its own
// origin as `{ type: 'viewer-core-event', event, reason, detail }`; the page
// turns it into a log event (wiki/docs/10-streaming-and-viewer.md, "Core
// events"). `reason` is a stable cause, `detail` short context: neither ever
// carries a URL or a token.
import { sessionStore } from './web-storage.js';

export const CORE_EVENT_TYPE = 'viewer-core-event';
/** sessionStorage key a core sets right before it reloads the page itself. */
export const CORE_RELOAD_KEY = 'viewer-core-reload';
const TEXT_MAX = 120;
const URL_PATTERN = /[a-z][a-z0-9+.-]*:\/\/\S*/gi;

/**
 * One short line: URLs replaced, whitespace folded, at most TEXT_MAX characters.
 * @param {*} value
 * @returns {string}
 */
export function coreEventText(value) {
  if (value === undefined || value === null) return '';
  return String(value).replace(URL_PATTERN, '[url]').replace(/\s+/g, ' ').trim().slice(0, TEXT_MAX);
}

/**
 * Posts one core event to the page.
 * @param {string} event Kebab-case name, e.g. `unsupported-codec`.
 * @param {string} reason Stable cause or origin, e.g. `websockets`.
 * @param {*} [detail] Short context: a codec, the server's stated reason.
 */
export function postCoreEvent(event, reason, detail) {
  try {
    window.postMessage({
      type: CORE_EVENT_TYPE, event, reason: coreEventText(reason), detail: coreEventText(detail),
    }, window.location.origin);
  } catch (e) { /* no page to tell */ }
}

/**
 * Leaves the event for the page the reload brings up, which logs it once the
 * logger runs again. Called right before `window.location.reload()`.
 * @param {string} event
 * @param {string} reason
 * @param {*} [detail]
 */
export function rememberCoreReload(event, reason, detail) {
  // Where storage is blocked the post before the reload still told the page.
  sessionStore.setItem(CORE_RELOAD_KEY, JSON.stringify({
    event, reason: coreEventText(reason), detail: coreEventText(detail), ts: Date.now(),
  }));
}

// @ts-check
/**
 * The session's one agent/human control state (containers/base/desktop_control.py)
 * between the cores and the page. The server broadcasts `desktop_control`
 * (`owner`, `epoch`, `agent_seen`, `agent_waiting`, `request`, `reason`,
 * `activity`, `activity_label`, `agent_active`) on every change; the cores
 * forward it as the `desktopControl` window message, and turn the page's
 * `desktopControlCommand` (`take`, `release`, `yield`, `status`) into the
 * `ctl,<action>` input message on the channel that carries input.
 * @module
 */

const ACTIONS = ['take', 'release', 'yield', 'status'];
// mirrored from containers/base/desktop_control.py. Keep in sync.
const ACTIVITIES = ['move', 'click', 'drag', 'scroll', 'type', 'key', 'look'];
const ACTIVITY_LABEL_MAX = 60;
const UNSAFE_LABEL = /[\p{Cc}\p{Cf}\p{Zl}\p{Zp}]/u;

/** @param {unknown} value @returns {string|null} */
function text(value) {
  return (typeof value === 'string' && value.trim()) || null;
}

/**
 * The page names the target, so a label with a line break, control or format
 * character (a bidi override could disguise the line it is shown in) is
 * dropped. Counted in code points like the container.
 * @param {unknown} value
 * @returns {string|null}
 */
function label(value) {
  const trimmed = text(value);
  return trimmed && [...trimmed].length <= ACTIVITY_LABEL_MAX && !UNSAFE_LABEL.test(trimmed) ? trimmed : null;
}

/**
 * A validated copy of a broadcast state, or null for anything else.
 * @param {*} raw
 * @returns {?{owner: 'agent'|'human', epoch: number, agent_seen: boolean, agent_waiting: boolean, request: ?string, reason: ?('take'|'ask'), activity: ?string, activity_label: ?string, agent_active: boolean}}
 */
export function readDesktopControl(raw) {
  if (!raw || typeof raw !== 'object' || (raw.owner !== 'agent' && raw.owner !== 'human')) return null;
  return {
    owner: raw.owner,
    epoch: Number.isSafeInteger(raw.epoch) ? raw.epoch : 0,
    agent_seen: raw.agent_seen === true,
    agent_waiting: raw.agent_waiting === true,
    request: text(raw.request),
    reason: raw.reason === 'take' || raw.reason === 'ask' ? raw.reason : null,
    activity: ACTIVITIES.includes(raw.activity) ? raw.activity : null,
    activity_label: label(raw.activity_label),
    agent_active: raw.agent_active !== false,
  };
}

/** Hands a broadcast state to the page. @param {unknown} raw */
export function postDesktopControl(raw) {
  const state = readDesktopControl(raw);
  if (state) window.postMessage({ type: 'desktopControl', state }, window.location.origin);
}

/**
 * The input message for a page command, or null when it is not one.
 * @param {*} message The `desktopControlCommand` window message.
 * @returns {?string}
 */
export function desktopControlInput(message) {
  return message && ACTIONS.includes(message.action) ? `ctl,${message.action}` : null;
}

/** Sent once a connection carries input, so a freshly opened viewer learns the state. */
export const DESKTOP_CONTROL_STATUS = 'ctl,status';

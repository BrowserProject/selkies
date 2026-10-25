// @ts-check
//
// The viewer's read-only / extra-player roles, selected by the URL hash and by
// the verdict the page hands the transport. Both stream cores read this, so it
// is the one place the rule lives.
//
// Why segments and not an equality test: a viewer page rewrites its own URL
// while it loads, and a browser resolves a fragment-only `replace` by APPENDING
// the fragment, which produces `#shared#shared`. A core that compared the hash
// whole then read "no shared segment" and registered as the CONTROLLER, which
// evicts the person who owns the session and hands their input context to a
// read-only guest. Repeats, extra fragments and casing must all read the same.
// (packages/session-react/src/viewer/sharedMode.ts is the page half over this rule.)

/** The hashes the server-side roles are named after. */
export const SHARED_HASHES = ['#shared', '#player2', '#player3', '#player4'];
/** Any `#playerN`, including the ones no role is defined for. */
const PLAYER_PATTERN = /^#player(\d+)$/;

/** Every `#`-separated segment of a hash, lowercased and `#`-prefixed. @param {string|null|undefined} hash @returns {string[]} */
export function hashSegments(hash) {
  if (!hash) return [];
  return String(hash)
    .split('#')
    .filter(Boolean)
    .map((part) => `#${part.trim().toLowerCase()}`);
}

/** The segments that select a non-controlling client, in order. @param {string|null|undefined} hash @returns {string[]} */
export function sharedSegments(hash) {
  return hashSegments(hash).filter((part) => part === '#shared' || PLAYER_PATTERN.test(part));
}

/**
 * Whether a hash selects a non-controlling viewer. Deliberately broader than
 * `SHARED_HASHES`: an unrecognised `#player9` must not fall through to the
 * controller branch, because falling through is the failure that costs the
 * session owner their input.
 * @param {string|null|undefined} hash
 * @returns {boolean}
 */
export function isSharedHash(hash) {
  return sharedSegments(hash).length > 0;
}

/** The `#display2[-position]` segment of a hash, or null. @param {string|null|undefined} hash @returns {string|null} */
export function displaySegment(hash) {
  return hashSegments(hash).find((part) => part === '#display2' || part.startsWith('#display2-')) || null;
}

/**
 * The client role a page should register with.
 *
 * @param {string|null|undefined} hash `window.location.hash`.
 * @param {{forcedShared?: boolean}} [page] The page's own verdict, which wins:
 *   the transport controller is started with `isShared` and publishes it as
 *   `window.SELKIES_SHARED_MODE` before a core loads, so a hash the core reads
 *   at an unlucky moment can only ever make the session MORE restricted.
 * @returns {{role: string, slot: (number|null), sharedType: (string|null), inputTarget: (number|undefined)}}
 */
export function readClientRole(hash, { forcedShared = false } = {}) {
  const segments = sharedSegments(hash);
  const first = segments[0] || (forcedShared ? '#shared' : null);
  if (!first) return { role: 'controller', slot: 1, sharedType: null, inputTarget: 0 };
  if (first === '#shared') {
    return { role: 'viewer', slot: -1, sharedType: 'shared', inputTarget: undefined };
  }
  const player = Number(PLAYER_PATTERN.exec(first)?.[1]);
  return {
    role: 'viewer',
    slot: player || null,
    // Only 2 to 4 name a player role; a stray number stays a plain viewer.
    sharedType: player >= 2 && player <= 4 ? `player${player}` : null,
    inputTarget: player >= 2 && player <= 4 ? player - 1 : undefined,
  };
}

/** The page verdict a core should honour, published by the transport. */
export function pageSharedMode() {
  return typeof window !== 'undefined' && /** @type {{SELKIES_SHARED_MODE?: boolean}} */ (/** @type {unknown} */ (window)).SELKIES_SHARED_MODE === true;
}

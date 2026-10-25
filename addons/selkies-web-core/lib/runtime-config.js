// @ts-check
/**
 * The project's settings for one viewer that are not Selkies' own `SELKIES_*`
 * globals. Only `runtime.ts` writes them (the page never does); the cores read
 * them at the lines marked `Runtime hook (lib/runtime-config.js)` and the
 * policy modules where they need them. One module instance per bundle, shared
 * by the page code and the cores.
 * @module
 */

/**
 * @typedef {{ isEnding(): boolean, isSuperseded(): boolean }} RuntimeTransport
 * @typedef {{
 *   enhancedResources: boolean,
 *   shouldStart: boolean,
 *   transport: RuntimeTransport | null,
 * }} RuntimeConfig
 */

/** @type {RuntimeConfig} */
const config = {
  // The larger automatic stream cap (lib/stream-policy.js).
  enhancedResources: false,
  // False once the page stopped its viewer: a core started late stays down.
  shouldStart: true,
  // The transport controller that owns the cores, asked whether the session is ending or superseded.
  transport: null,
};

/** @returns {Readonly<RuntimeConfig>} */
export function runtimeConfig() {
  return config;
}

/** @param {Partial<RuntimeConfig>} patch */
export function updateRuntimeConfig(patch) {
  Object.assign(config, patch);
}

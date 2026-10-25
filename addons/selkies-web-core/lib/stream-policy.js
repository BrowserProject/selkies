// @ts-check
import { fitProfileSize } from './stream-profile.js';
/**
 * Stream sizing policy of browser.lol and guard.ch. Project file, NOT part of
 * upstream Selkies (identical in both viewer copies; sync by copy).
 *
 * The vendored cores consult this module at three hooks, each marked
 * "Policy hook (lib/stream-policy.js)" in the core:
 *   1. `import { streamDensity as streamDensityOf } from './lib/stream-policy.js'`
 *      instead of `./lib/stream-density.js`.
 *   2. `autoDeriveDpi()` derives the default `scaling_dpi` from `uiDensity()`
 *      instead of `window.devicePixelRatio`.
 *   3. Init applies `smallScreenDefault()` after the manual resolution params
 *      are read.
 *   4. The canvas/video box is placed by `fitStreamBox()` (manual scaled-to-fit
 *      and automatic sizing), so an aligned stream covers the stage instead
 *      of leaving a sub-pixel black seam, and a letterboxed one keeps the
 *      stage's box.
 * On a Selkies upgrade re-vendor the cores and re-apply those hooks; nothing
 * else in the resolution path is project specific (see
 * wiki/docs/10-streaming-and-viewer.md).
 *
 * The policy, carried over from the pre-WebRTC viewer:
 *  - The stream is sized in CSS pixels and never multiplied by
 *    devicePixelRatio. A MacBook at ratio 2 requests the same frame as a 1x
 *    screen and the browser upscales it: cheaper to encode, and the remote
 *    UI and cursor keep the size of the local ones.
 *  - The automatic resolution is capped at 2000x1250, or 3000x2000 when the
 *    page starts the viewer with `stream.enhancedResources` (lib/runtime-config.js).
 *    Above the cap the request shrinks with its aspect ratio kept and the
 *    page scales the smaller stream up.
 *  - The remote DPI defaults to 96, the DPI of a CSS pixel; a stored
 *    `scaling_dpi` pick always wins.
 *  - A phone (a touch-first screen under 800 CSS pixels on its short side) streams
 *    a fixed 800 pixel wide desktop whose height follows the stage's aspect
 *    ratio, so the page fills the stage edge to edge with no letterbox. The
 *    page publishes the stage (`--viewer-stage-width` / `--viewer-stage-height`
 *    on the root element: the viewport minus its own bars) and calls
 *    `smallScreenRefit()` when it changes (rotation), which re-derives the
 *    height. The pick is remembered under `<app>_small_screen_auto`; once the
 *    visitor stores a different manual resolution the policy steps aside, and
 *    on a screen that is no longer small the policy removes its own pick.
 *    The page imports the refit from `@repo/viewer-engine/policy`: the same
 *    module instance the cores use.
 */
import { streamDensity as upstreamStreamDensity } from './stream-density.js';
import { isMobileClient } from './util.js';
import { localStore } from './web-storage.js';
import { runtimeConfig } from './runtime-config.js';

export const STREAM_CAP_DEFAULT = Object.freeze({ maxWidth: 2000, maxHeight: 1250 });
export const STREAM_CAP_ENHANCED = Object.freeze({ maxWidth: 3000, maxHeight: 2000 });
export const SMALL_SCREEN_MAX_WIDTH = 800;
/** The fixed stream width of a small screen; the height follows the stage. */
export const SMALL_SCREEN_WIDTH = 800;
/** The height a small-screen stream may take: a phone in landscape, up to a tall portrait. */
export const SMALL_SCREEN_HEIGHT_RANGE = Object.freeze({ min: 300, max: 2000 });
/** The relative size change a rotation must bring before the stream is re-derived. */
const SMALL_SCREEN_REFIT_TOLERANCE = 0.02;
const SMALL_SCREEN_MARKER = 'small_screen_auto';
/** How far per side the size the server realized (and the core stores) may sit
 *  from the policy's request and still be the policy's own: the display modes
 *  align to 8 or 16 pixels (1204x300 is realized as 1208x300). */
const SMALL_SCREEN_REALIZED_SLACK_PX = 16;
/** The floor a capped density never goes under; a density of 0 would divide by zero downstream. */
const MIN_DENSITY = 0.1;

/**
 * The widest automatic stream this session may request, in stream pixels.
 * @param {boolean} [enhanced] Whether the session streams with enhanced resources.
 * @returns {{maxWidth: number, maxHeight: number}}
 */
export function streamCap(enhanced = runtimeConfig().enhancedResources) {
    return enhanced === true ? STREAM_CAP_ENHANCED : STREAM_CAP_DEFAULT;
}

/**
 * The density the remote desktop's UI is sized for: one stream pixel per CSS
 * pixel, so 96 DPI on the remote side matches the local UI size.
 * @returns {number}
 */
export function uiDensity() {
    return 1;
}

/**
 * Shrinks a density until a page of the given CSS size streams inside the
 * cap; the aspect ratio is kept because one factor applies to both sides.
 * Pure, so the tests can pin the numbers.
 * @param {number} density Stream pixels per CSS pixel before the cap.
 * @param {number} cssWidth
 * @param {number} cssHeight
 * @param {{maxWidth: number, maxHeight: number}} cap
 * @returns {number}
 */
export function fitDensity(density, cssWidth, cssHeight, cap) {
    if (!(density > 0) || !(cssWidth > 0) || !(cssHeight > 0) || !cap) return density;
    const fit = Math.min(1, cap.maxWidth / (cssWidth * density), cap.maxHeight / (cssHeight * density));
    return fit < 1 ? Math.max(MIN_DENSITY, density * fit) : density;
}

/**
 * The CSS size of the stream stage: the `.video-container` box, else the
 * window's inner size.
 * @returns {{width: number, height: number}|null}
 */
export function measureStage() {
    if (typeof document === 'undefined') return null;
    const container = document.querySelector('.video-container');
    const rect = container ? container.getBoundingClientRect() : null;
    const width = (rect && rect.width > 0) ? rect.width : window.innerWidth;
    const height = (rect && rect.height > 0) ? rect.height : window.innerHeight;
    return (width > 0 && height > 0) ? { width, height } : null;
}

/**
 * Whether a core runs a manual resolution (the websockets core publishes
 * `manual_resolution`, the WebRTC core `manualResolution`).
 * @returns {boolean}
 */
function manualResolutionActive() {
    const win = /** @type {{manual_resolution?: boolean, manualResolution?: boolean} | undefined} */ (/** @type {unknown} */ (globalThis.window));
    return !!(win && (win.manual_resolution || win.manualResolution));
}

/**
 * Stream pixels per CSS pixel this page requests and draws at: the upstream
 * rule evaluated as if CSS scaling were on (a primary streams at 1, a
 * secondary at the primary's reported scale, a shared viewer at its own),
 * then fitted to the cap on an automatic primary. A manual resolution is an
 * exact framebuffer and is not fitted.
 * @param {{displayId: string, layouts: (Object|null), useCssScaling: boolean,
 *     shared: boolean}} page The upstream `streamDensity` argument.
 * @param {() => ({width: number, height: number}|null)} [measure]
 * @returns {number}
 */
export function streamDensity(page, measure = measureStage) {
    const base = upstreamStreamDensity({ ...page, useCssScaling: true });
    if (page.shared || manualResolutionActive()) return base;
    const stage = measure();
    if (!stage) return base;
    const capped = fitDensity(base, stage.width, stage.height, streamCap());
    const fitted = fitProfileSize(stage.width * capped, stage.height * capped);
    return Math.min(capped, fitted.width / stage.width, fitted.height / stage.height);
}

/**
 * Whether the device is a small screen: a touch-first device whose short side
 * is narrower than `SMALL_SCREEN_MAX_WIDTH`, whichever way it is held. A
 * desktop with a short screen (1366x768, or 1920x1080 at 150 %) is not one:
 * it streams its own stage automatically.
 * @param {number} screenWidth
 * @param {number} [screenHeight]
 * @param {boolean} [touchFirst] Whether the primary pointer is coarse.
 * @returns {boolean}
 */
export function isSmallScreen(screenWidth, screenHeight, touchFirst = isMobileClient) {
    if (!touchFirst) return false;
    const short = (screenHeight !== undefined && screenHeight > 0) ? Math.min(screenWidth, screenHeight) : screenWidth;
    return short > 0 && short < SMALL_SCREEN_MAX_WIDTH;
}

/**
 * The stream a small screen requests for a stage: the fixed width, the height
 * from the stage's aspect ratio, both even (the encoders want even sizes).
 * Where the height range clamps (a very flat landscape stage), the width
 * follows the clamped height instead, so the stream keeps the stage's aspect
 * ratio and still covers it.
 * @param {{width: number, height: number}|null} stage The stage in CSS pixels.
 * @returns {{width: number, height: number}}
 */
export function smallScreenResolution(stage) {
    const width = SMALL_SCREEN_WIDTH;
    if (!stage || !(stage.width > 0) || !(stage.height > 0)) return { width, height: width };
    const raw = Math.round(width * stage.height / stage.width);
    const clamped = Math.min(SMALL_SCREEN_HEIGHT_RANGE.max, Math.max(SMALL_SCREEN_HEIGHT_RANGE.min, raw));
    const fittedWidth = clamped === raw ? width : Math.max(2, Math.round(clamped * stage.width / stage.height));
    return fitProfileSize(fittedWidth - (fittedWidth % 2), clamped - (clamped % 2));
}

/**
 * The stage a small screen sizes its stream for: the page's published
 * `--viewer-stage-width` / `--viewer-stage-height` (the viewport minus the
 * page's own bars), else the measured stage.
 * @returns {{width: number, height: number}|null}
 */
export function measureSmallScreenStage() {
    if (typeof document === 'undefined') return measureStage();
    const style = document.documentElement && document.documentElement.style;
    const width = style ? parseFloat(style.getPropertyValue('--viewer-stage-width')) : NaN;
    const height = style ? parseFloat(style.getPropertyValue('--viewer-stage-height')) : NaN;
    return (width > 0 && height > 0) ? { width, height } : measureStage();
}

/**
 * @typedef {{width: number, height: number}} Resolution
 * @typedef {{ getItem(key: string): string|null, setItem(key: string, value: string): unknown, removeItem(key: string): void }} PolicyStorage
 * @typedef {{hasManual: boolean, stored: Resolution|null, marker: string|null}} SmallScreenStore
 */

/**
 * The visitor's stored manual resolution and the policy's own marker.
 * @param {PolicyStorage} storage
 * @param {string} storageAppName
 * @returns {SmallScreenStore}
 */
function readSmallScreenStore(storage, storageAppName) {
    /** @param {string} key */
    const read = (key) => { try { return storage.getItem(`${storageAppName}_${key}`); } catch (_) { return null; } };
    const manual = read('manual_resolution');
    const width = parseInt(read('manual_width') ?? '', 10);
    const height = parseInt(read('manual_height') ?? '', 10);
    const marker = read(SMALL_SCREEN_MARKER);
    return {
        hasManual: manual !== null,
        stored: (width > 0 && height > 0) ? { width, height } : null,
        marker,
    };
}

/** @param {Resolution|null} a @param {Resolution|null} b */
function sameResolution(a, b) {
    return !!a && !!b && a.width === b.width && a.height === b.height;
}

/**
 * Whether the stored manual resolution is the policy's own pick, so the
 * policy may replace it. A stored resolution without a marker is the
 * visitor's, except the legacy 800x800 square the policy used to store.
 * @param {SmallScreenStore} state
 */
function smallScreenOwnsStored({ hasManual, stored, marker }) {
    if (!hasManual || !stored) return true;
    if (marker !== null) {
        const picked = /^(\d+)x(\d+)$/.exec(marker);
        return !!picked
            && Math.abs(Number(picked[1]) - stored.width) <= SMALL_SCREEN_REALIZED_SLACK_PX
            && Math.abs(Number(picked[2]) - stored.height) <= SMALL_SCREEN_REALIZED_SLACK_PX;
    }
    return stored.width === SMALL_SCREEN_WIDTH && stored.height === SMALL_SCREEN_WIDTH;
}

/** @param {PolicyStorage} storage @param {string} storageAppName */
function forgetSmallScreen(storage, storageAppName) {
    try {
        for (const key of [SMALL_SCREEN_MARKER, 'manual_width', 'manual_height', 'manual_resolution']) storage.removeItem(`${storageAppName}_${key}`);
    } catch (_) { /* private mode: nothing was stored */ }
}

/** @param {PolicyStorage} storage @param {string} storageAppName @param {Resolution} resolution */
function rememberSmallScreen(storage, storageAppName, resolution) {
    try {
        storage.setItem(`${storageAppName}_${SMALL_SCREEN_MARKER}`, `${resolution.width}x${resolution.height}`);
        storage.setItem(`${storageAppName}_manual_width`, String(resolution.width));
        storage.setItem(`${storageAppName}_manual_height`, String(resolution.height));
        storage.setItem(`${storageAppName}_manual_resolution`, 'true');
    } catch (_) { /* private mode: the pick lives for this page only */ }
}

/**
 * The manual resolution a visit on a small screen starts in, or null when the
 * viewer is shared, the screen is wide enough for an automatic resolution, or
 * the visitor stored a manual resolution of their own. False when a device
 * that is no small screen still carries the policy's own pick (a laptop moved
 * to a large monitor): the pick is removed and the visit runs automatic.
 * @param {{shared: boolean, storageAppName: string, screenWidth: number,
 *     screenHeight?: number, touchFirst?: boolean,
 *     stage?: ({width: number, height: number}|null), storage?: Storage}} visit
 * @returns {{width: number, height: number}|false|null}
 */
export function smallScreenDefault({ shared, storageAppName, screenWidth, screenHeight, touchFirst, stage, storage }) {
    if (shared) return null;
    const store = storage || localStore;
    const state = readSmallScreenStore(store, storageAppName);
    if (!isSmallScreen(screenWidth, screenHeight, touchFirst)) {
        if (!state.hasManual || !state.stored || state.marker === null || !smallScreenOwnsStored(state)) return null;
        forgetSmallScreen(store, storageAppName);
        return false;
    }
    if (!smallScreenOwnsStored(state)) return null;
    const resolution = smallScreenResolution(stage === undefined ? measureSmallScreenStage() : stage);
    rememberSmallScreen(store, storageAppName, resolution);
    return resolution;
}

/**
 * The resolution a small screen switches to after its stage changed
 * (rotation), or null when the policy does not own the current resolution or
 * the change is within tolerance. The new pick is stored before it is
 * returned, so the core's own write is a no-op.
 * @param {{storageAppName: string, screenWidth: number, screenHeight?: number,
 *     touchFirst?: boolean, stage?: ({width: number, height: number}|null),
 *     storage?: Storage}} visit
 * @returns {{width: number, height: number}|null}
 */
export function smallScreenRefit({ storageAppName, screenWidth, screenHeight, touchFirst, stage, storage }) {
    if (!isSmallScreen(screenWidth, screenHeight, touchFirst)) return null;
    const store = storage || localStore;
    const state = readSmallScreenStore(store, storageAppName);
    if (!state.hasManual || !state.stored || !smallScreenOwnsStored(state)) return null;
    const measured = stage === undefined ? measureSmallScreenStage() : stage;
    if (!measured) return null;
    const next = smallScreenResolution(measured);
    if (sameResolution(next, state.stored)) return null;
    const drift = Math.max(Math.abs(next.width - state.stored.width) / state.stored.width, Math.abs(next.height - state.stored.height) / state.stored.height);
    if (drift < SMALL_SCREEN_REFIT_TOLERANCE) return null;
    rememberSmallScreen(store, storageAppName, next);
    return next;
}

/**
 * Relative gap (per axis, of the stage) up to which a stream is stretched
 * over the stage instead of letterboxed. Covers the encoder's even or
 * 16-pixel alignment and a stage measured a pixel off; a real aspect
 * mismatch (a 4:3 manual resolution on a 16:9 stage) stays letterboxed.
 */
export const STREAM_BOX_COVER_TOLERANCE = 0.02;

/**
 * The CSS box a stream is painted into. Scaled to fit, a stream whose aspect
 * ratio is within the tolerance of the stage gets a box of the buffer's
 * aspect ratio that overlaps the stage by up to a pixel per edge (integer
 * size and offsets, so no sub-pixel seam shows the black stage behind it).
 * A stream further off keeps the stage's own box and is letterboxed inside
 * it by `object-fit: contain`, so a remote screen that changes its aspect
 * ratio (the portrait request replacing the landscape boot screen) never
 * moves the element and shifts no layout; input maps through the contained
 * picture (`_sinkBox` in input.js). Unscaled, the box is the buffer's own
 * size, centered.
 * @param {{width: number, height: number}} buffer The frame size in stream pixels.
 * @param {{width: number, height: number}} stage The container in CSS pixels.
 * @param {boolean} scaleToFit
 * @returns {{width: number, height: number, top: number, left: number, objectFit: string}}
 */
export function fitStreamBox(buffer, stage, scaleToFit) {
    const objectFit = 'cover';
    if (!(buffer.width > 0) || !(buffer.height > 0) || !(stage.width > 0) || !(stage.height > 0)) {
        return { width: buffer.width || 0, height: buffer.height || 0, top: 0, left: 0, objectFit };
    }
    if (!scaleToFit) {
        return {
            width: buffer.width, height: buffer.height,
            top: (stage.height - buffer.height) / 2, left: (stage.width - buffer.width) / 2, objectFit,
        };
    }
    const contain = Math.min(stage.width / buffer.width, stage.height / buffer.height);
    const gapWidth = stage.width - buffer.width * contain;
    const gapHeight = stage.height - buffer.height * contain;
    if (gapWidth <= stage.width * STREAM_BOX_COVER_TOLERANCE && gapHeight <= stage.height * STREAM_BOX_COVER_TOLERANCE) {
        const cover = Math.max(stage.width / buffer.width, stage.height / buffer.height);
        const width = Math.ceil(buffer.width * cover);
        const height = Math.ceil(buffer.height * cover);
        return { width, height, top: Math.floor((stage.height - height) / 2), left: Math.floor((stage.width - width) / 2), objectFit };
    }
    return { width: stage.width, height: stage.height, top: 0, left: 0, objectFit: 'contain' };
}
/**
 * Decoded frames can catch up with a request after an older frame changed the box.
 * @param {Resolution|null|undefined} realized
 * @param {Resolution|null|undefined} requested
 * @param {Resolution|null|undefined} stage
 * @param {boolean} scaleToFit
 */
export function realizedStreamBox(realized, requested, stage, scaleToFit) {
    if (!realized || !(realized.width > 0) || !(realized.height > 0)) return null;
    if (!stage || !(stage.width > 0) || !(stage.height > 0)) return null;
    return fitStreamBox(realized, stage, scaleToFit);
}

/*!
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

/**
 * Helpers shared by the streaming cores and both dashboards: a small FIFO
 * queue, the human-readable labels for wire values, the decodability checks
 * that keep a client from asking for a stream it cannot play, and the route
 * prefix and localStorage namespace every caller derives the same way.
 * @module
 */

/** A FIFO queue over an array. */
export class Queue {
    /** @param {...*} elements Initial items, enqueued in order. */
    constructor(...elements) {
        /** @type {Array} */
        this.items = [];

        this.enqueue(...elements);
    }

    /** @param {...*} elements Items appended in order. */
    enqueue(...elements) {
        elements.forEach(element => this.items.push(element));
    }

    /**
     * Removes the oldest `count` items.
     * @param {number} [count=1] How many items to drop.
     * @returns {*} The oldest of the removed items.
     */
    dequeue(count=1) {
        return this.items.splice(0, count)[0];
    }

    /** @returns {number} */
    size() {
        return this.items.length;
    }

    /** @returns {boolean} */
    isEmpty() {
        return this.items.length===0;
    }

    /** @returns {Array} A copy of the items, oldest first. */
    toArray() {
        return [...this.items]
    }

    /** Removes the first occurrence of `element`. */
    remove(element) {
        var index = this.items.indexOf(element)
        this.items.splice(index, 1)
    }

    /** @returns {boolean} Whether `element` is queued. */
    find(element) {
        return this.items.indexOf(element) == -1 ? false: true;
    }

    /** Drops every item. */
    clear(){
        this.items.length = 0;
    }
}

/**
 * Human-readable names for the wire values surfaced in UIs (transport modes,
 * encoders, rate-control modes). The raw values are what the server APIs
 * speak and stay untouched; unknown values fall through unchanged so new wire
 * values render as-is. Locale-invariant technical terms, so they live here
 * once rather than in every dashboard's translation dictionaries.
 */
export const DISPLAY_LABELS = {
    websockets: "WebSockets",
    webrtc: "WebRTC",
    h264enc: "H.264 (Full Frame)",
    "h264enc-striped": "H.264 (Striped Frame)",
    jpeg: "JPEG (Striped Frame)",
    cbr: "CBR (Constant Bitrate)",
    crf: "CRF (Constant Quality)",
    auto: "Auto",
    h264: "H.264",
    vp8: "VP8",
    mjpeg: "MJPEG",
};

/**
 * @param {string} value A wire value.
 * @returns {string} Its display label, or the value itself when it has none.
 */
export const displayLabel = (value) => DISPLAY_LABELS[value] ?? value;

/** Cached answer of `canDecodeFullColor`, so the probe runs once. */
let fullColorProbe = null;

/**
 * Whether this engine's `VideoDecoder` will take H.264 full colour (4:4:4).
 *
 * Engines differ on High 4:4:4 Predictive, and one whose decoder lacks it
 * cannot show the stream at all rather than showing it worse, so full colour
 * is asked of the decoder before it is asked of the server. Which engines have
 * it changes with their releases, which is why this probes instead of naming
 * them.
 *
 * The profile is the whole question, so it is asked in the least demanding
 * form that can carry it: `F4` with no constraint bits set, which is what the
 * encoders emit and the only spelling engines accept -- setting them names the
 * Intra-only variant and is refused -- at the smallest frame the level allows,
 * since a level is a ceiling and a size beyond it is a pair an implementation
 * may reject on its own. What a real stream is decoded with comes from its
 * keyframe's SPS instead.
 * @returns {Promise<boolean>} False where there is no `VideoDecoder` at all.
 */
export function canDecodeFullColor() {
    if (!fullColorProbe) {
        fullColorProbe = (async () => {
            if (typeof VideoDecoder === "undefined") return false;
            try {
                const support = await VideoDecoder.isConfigSupported({
                    codec: "avc1.F4001E", codedWidth: 320, codedHeight: 240,
                });
                return !!(support && support.supported);
            } catch (err) {
                return false;
            }
        })();
    }
    return fullColorProbe;
}

/**
 * Directory this document is served from, without a trailing slash (`''` at
 * the server root). Every request the client builds hangs off it, so a
 * deployment reverse-proxied under a subfolder reaches its own routes, and an
 * iframed client reads its own path instead of the frame's.
 * @returns {string} The path prefix, e.g. `/desk`.
 */
export function getRoutePrefix() {
    const pathname = window.location.pathname;
    const dirPath = pathname.substring(0, pathname.lastIndexOf('/') + 1);
    return dirPath.replace(/\/$/, '');
}

/**
 * The localStorage namespace every stored key is prefixed with.
 *
 * Origin and pathname only, not the full URL: a per-session `?token=` must
 * not mint a new namespace on each connect. Cores and dashboards share one
 * prefix, so this derivation is the single one they all call.
 * @returns {string} Sanitized namespace, empty outside a browser.
 */
export function getStorageAppName() {
    if (typeof window === 'undefined') return '';
    const urlForKey = window.location.origin + window.location.pathname;
    return urlForKey.replace(/[^a-zA-Z0-9._-]/g, '_');
}

/**
 * Whether the client is touch-first: its primary pointer is coarse. True on
 * phones and tablets, false on desktops -- including touch-screen laptops,
 * whose primary pointer is still the pointing device. The form factor is
 * fixed for the life of the document, so it is resolved once and available
 * to every first render; a touch-capable device this misses is still caught
 * by the first touchstart.
 * @type {boolean}
 */
export const isMobileClient =
    typeof window !== 'undefined' &&
    typeof window.matchMedia === 'function' &&
    window.matchMedia('(pointer: coarse)').matches;

/**
 * Absolute URL of one Selkies endpoint (`api/websockets`, `api/ws`,
 * `api/turn`, `api/switch`).
 *
 * Our client is served by the product frontend, not by the container, so the
 * route prefix derived from `window.location` names the frontend and not the
 * session. `window.SELKIES_BASE_URL` is the per-session base the integration
 * publishes (`https://<backend host>/selkies/<vmId>`), and only when it is
 * absent does this fall back to the upstream same-origin derivation.
 * @param {string} path Endpoint path, with or without a leading slash.
 * @returns {URL}
 */
export function selkiesEndpoint(path) {
    const suffix = String(path).replace(/^\//, '');
    const base = (typeof window !== 'undefined' && window.SELKIES_BASE_URL) || '';
    if (base) {
        return new URL(suffix, base.endsWith('/') ? base : `${base}/`);
    }
    const url = new URL(window.location.href);
    url.pathname = `${getRoutePrefix()}/${suffix}`;
    url.search = '';
    url.hash = '';
    return url;
}

/**
 * `selkiesEndpoint` as a WebSocket URL: same origin, `ws`/`wss` scheme.
 * @param {string} path
 * @returns {URL}
 */
export function selkiesSocketEndpoint(path) {
    const url = selkiesEndpoint(path);
    url.protocol = url.protocol === 'http:' ? 'ws:' : 'wss:';
    return url;
}

/**
 * The viewer's session peer id, published by `transport.ts` before either
 * core loads.
 *
 * Both wire endpoints carry it: the `/api/websockets` query and the signaling
 * `HELLO` metadata. A container that serves the WebSocket sink from its WebRTC
 * pipeline uses it to recognise a socket client and a peer connection as ONE
 * consumer, so a viewer upgrading from the socket to WebRTC hands its consumer
 * entry over instead of being superseded (close 4001). Older containers ignore
 * the extra query parameter and the extra metadata key.
 * @returns {?string}
 */
export function sessionPeerId() {
    try {
        const id = typeof window !== 'undefined' ? window.SELKIES_SESSION_PEER_ID : null;
        return typeof id === 'string' && id ? id : null;
    } catch (e) {
        return null;
    }
}

/**
 * Adds `session_peer_id` to a socket URL, after whatever query the caller built.
 *
 * Appended as a string rather than through `searchParams.set`, which
 * re-serialises the WHOLE query in the form-urlencoded style (`+` for a space,
 * different escaping of `:` and `/`). The query it is appended to carries the
 * session token, which the container compares verbatim, so the pre-existing
 * bytes have to survive untouched.
 * @param {URL} url
 * @returns {URL} The same url, for chaining.
 */
export function withSessionPeerId(url) {
    const query = String(url.search || '').replace(/^\?/, '');
    const pair = `session_peer_id=${encodeURIComponent(sessionPeerId())}`;
    url.search = query ? `${query}&${pair}` : pair;
    return url;
}

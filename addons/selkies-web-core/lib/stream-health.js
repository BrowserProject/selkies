// @ts-check
//
// Whether a connected WebRTC peer still delivers a picture. A peer stays
// `connected` for seconds after the container closed it or the link stopped
// carrying media (Chromium says `disconnected` after about 5 s, Firefox after
// up to 30 s), and a decoder that stopped producing frames is never reported
// at all. A healthy peer decodes frames all the time, a still screen included
// (the static hold sends keepalive frames), so a decoded-frame count that does
// not move for STREAM_STALL_MS while the picture is wanted is a dead stream.

/** How often the peer's decoded frames are read. */
export const STREAM_HEALTH_POLL_MS = 1000;
/** Watched time without one new decoded frame after which the stream counts as dead. */
export const STREAM_STALL_MS = 5000;
/** A sample this long after the one before ran in a suspended or throttled page: it starts over. */
const SAMPLE_GAP_MAX_MS = 3 * STREAM_HEALTH_POLL_MS;

/**
 * @param {number} [stallMs]
 * @returns {{ sample(input: { watching: boolean, frames: number|null|undefined, at: number }): 'ok'|'stalled' }}
 *   `watching` is false while no picture is expected (hidden tab, video
 *   stopped, a resume being verified, the peer not connected); `frames` is the
 *   inbound video's `framesDecoded`, null where the browser has none.
 */
export function createStreamHealth(stallMs = STREAM_STALL_MS) {
  /** @type {number|null} */
  let since = null;
  /** @type {number|null} */
  let count = null;
  /** @type {number|null} */
  let lastAt = null;
  return {
    sample({ watching, frames, at }) {
      const gap = lastAt === null ? 0 : at - lastAt;
      lastAt = at;
      if (!watching || typeof frames !== 'number' || !Number.isFinite(frames) || gap > SAMPLE_GAP_MAX_MS) {
        since = null;
        count = null;
        return 'ok';
      }
      if (since === null || frames !== count) {
        since = at;
        count = frames;
        return 'ok';
      }
      return at - since >= stallMs ? 'stalled' : 'ok';
    },
  };
}

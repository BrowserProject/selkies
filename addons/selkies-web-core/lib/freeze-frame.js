// @ts-check
//
// The last painted frame, kept on screen after the media behind it dies.
//
// When another tab takes the session, this page's peer connection goes
// disconnected and then failed; the `<video>` collapses to 0x0 a few seconds
// later and the page is left showing a blank stage under its "take control"
// prompt. Copying the current frame into a canvas at that moment keeps the
// session visible until the user decides. The canvas is attached to `body`, not
// to the stream container, so a core teardown does not take it with it.

const FROZEN_ID = 'selkies-frozen-frame';
/** Under the page's own overlays (which sit at 2147483000), over the stage. */
const FROZEN_Z_INDEX = '2147482000';

/** Removes the frozen frame, if one is up. */
export function clearFrozenFrame() {
  if (typeof document === 'undefined') return;
  const existing = document.getElementById(FROZEN_ID);
  if (existing && typeof existing.remove === 'function') existing.remove();
}

/**
 * Drops the frozen frame once the element actually presents again, so a
 * recovery does not swap a still picture for a black gap of its own. Falls back
 * to dropping it at once where per-frame callbacks are unavailable, and always
 * within a few seconds, so a connection that comes back without ever painting
 * cannot leave a stale picture over the stage.
 * @param {HTMLVideoElement|null} video
 */
export function clearFrozenFrameOnNextFrame(video) {
  if (typeof document === 'undefined') return;
  if (!document.getElementById(FROZEN_ID)) return;
  if (!video || typeof video.requestVideoFrameCallback !== 'function') {
    clearFrozenFrame();
    return;
  }
  try {
    video.requestVideoFrameCallback(() => clearFrozenFrame());
  } catch (e) {
    clearFrozenFrame();
    return;
  }
  const timer = setTimeout(clearFrozenFrame, 5000);
  if (timer && typeof timer.unref === 'function') timer.unref();
}

/**
 * Copies the video's current frame into a canvas positioned exactly over it,
 * then stops the media so no failed peer connection is kept alive for it.
 *
 * @param {HTMLVideoElement|HTMLCanvasElement|null} video The `<video>`, or the
 *   `<canvas>` the websockets core paints into.
 * @param {{stopMedia?: boolean}} [options] `stopMedia` false keeps the element
 *   playable: a peer connection that merely failed may still be recovered by an
 *   ICE restart onto the SAME element, and a paused element with stopped tracks
 *   would not come back on its own.
 * @returns {HTMLCanvasElement|null} The canvas, or null when there was nothing
 *   to freeze (no decoded frame, a zero-sized box, no 2D context).
 */
export function freezeVideoFrame(video, { stopMedia = true } = {}) {
  if (typeof document === 'undefined') return null;
  clearFrozenFrame();
  if (!video) return null;
  const media = /** @type {Partial<HTMLVideoElement>} */ (video);
  const sourceWidth = media.videoWidth || video.width;
  const sourceHeight = media.videoHeight || video.height;
  if (!sourceWidth || !sourceHeight) return null;
  let rect = null;
  try { rect = video.getBoundingClientRect(); } catch (e) { rect = null; }
  if (!rect || !(rect.width > 0) || !(rect.height > 0)) return null;

  let canvas = null;
  try {
    canvas = document.createElement('canvas');
    canvas.id = FROZEN_ID;
    // The buffer is the stream's own size and the box is the video's, so
    // `object-fit: contain` reproduces the letterboxing pixel for pixel.
    canvas.width = sourceWidth;
    canvas.height = sourceHeight;
    const context = canvas.getContext('2d');
    if (!context) return null;
    context.drawImage(video, 0, 0, canvas.width, canvas.height);
    Object.assign(canvas.style, {
      position: 'fixed',
      left: `${rect.left}px`,
      top: `${rect.top}px`,
      width: `${rect.width}px`,
      height: `${rect.height}px`,
      objectFit: 'contain',
      background: '#000',
      pointerEvents: 'none',
      zIndex: FROZEN_Z_INDEX,
    });
    document.body.appendChild(canvas);
  } catch (e) {
    // A frame that cannot be read (no decoded picture yet) is not worth a throw
    // on a teardown path.
    return null;
  }

  if (!stopMedia) return canvas;
  try {
    if (typeof media.pause === 'function') media.pause();
    const source = /** @type {MediaStream|null|undefined} */ (media.srcObject);
    const tracks = source && typeof source.getTracks === 'function' ? source.getTracks() : [];
    for (const track of tracks) track.stop();
  } catch (e) { /* already gone */ }
  return canvas;
}

// @ts-check
/**
 * The effective hidden state of a core's page: document hidden, frozen, or kept in the
 * back/forward cache. `onChange` hears every change, `immediate` when it must not wait out the
 * navigation guard.
 * @param {ReturnType<typeof import('./viewer-lifecycle.js').createViewerLifecycle>} lifecycle
 * @param {(change: {immediate: boolean}) => void} onChange
 * @param {Document} [doc]
 * @param {Window} [win]
 */
export function watchStreamVisibility(lifecycle, onChange, doc = document, win = window) {
  let frozen = false;
  let cached = false;
  const state = { get hidden() { return doc.hidden || frozen || cached; } };
  const notify = (immediate = false) => onChange({ immediate });
  lifecycle.listen(doc, 'visibilitychange', () => notify());
  lifecycle.listen(doc, 'freeze', () => {
    frozen = true;
    notify(true);
  });
  lifecycle.listen(doc, 'resume', () => {
    frozen = false;
    notify(true);
  });
  lifecycle.listen(win, 'pagehide', /** @param {Event} event */ (event) => {
    // A departing connection must not stop its successor's standalone WS pipeline.
    if (!(/** @type {PageTransitionEvent} */ (event)).persisted) return;
    cached = true;
    notify(true);
  });
  lifecycle.listen(win, 'pageshow', () => {
    cached = false;
    frozen = false;
    notify(true);
  });
  return state;
}

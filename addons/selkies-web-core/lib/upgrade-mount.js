// @ts-check
/**
 * How a WebRTC core instance is mounted while it is only an UPGRADE ATTEMPT
 * running next to a live WebSocket sink (see `transport.ts` and
 * wiki/docs/10-streaming-and-viewer.md, "Hooks in the vendored core").
 *
 * Both cores append a `.video-container` into `#app`, which is a flex column:
 * a second one in the flow squeezes the streaming socket picture to half the
 * height and parks a black box under it for as long as the attempt runs. So
 * the attempt is mounted out of the flow and invisible, and only revealed when
 * the transport completes the upgrade.
 *
 * `position: fixed` (not `display: none`) is what keeps the video decoding: a
 * display-none element presents no frames, and the upgrade is only completed
 * once a frame has actually been painted. Its bounds follow the live stage,
 * not the window, so the upgrade requests the same desktop resolution.
 * @module
 */

/**
 * Marks a container that is mounted but not yet the session the user sees.
 * Namespaced so no page stylesheet matches it: a page rule on the hidden mount
 * resizes the box the pending session measures its desktop size from.
 */
export const UPGRADE_PENDING_CLASS = 'selkies-upgrade-pending';
const pendingMounts = new WeakMap();

/** The rule for that class; both cores paste it into their own stylesheet. */
export const UPGRADE_PENDING_CSS = `
.video-container.${UPGRADE_PENDING_CLASS} {
	position: fixed;
	left: 0;
	top: 0;
	width: 100%;
	height: 100%;
	opacity: 0;
	pointer-events: none;
	z-index: -1;
}
`;

/**
 * Mounts a container off the page: out of the `#app` flex flow, behind
 * everything, transparent and untouchable.
 * @param {?Element} container
 */
export function mountPendingUpgrade(container) {
	container?.classList?.add(UPGRADE_PENDING_CLASS);
	const parent = container?.parentElement;
	if (!parent) return;
	const stage = [...parent.children].find(node => node !== container && node.classList.contains('video-container')) || parent;
	const syncBounds = () => {
		const rect = stage.getBoundingClientRect();
		if (!(rect.width > 0 && rect.height > 0)) return;
		Object.assign(/** @type {HTMLElement} */ (container).style, {
			left: `${rect.left}px`, top: `${rect.top}px`,
			width: `${rect.width}px`, height: `${rect.height}px`,
		});
	};
	pendingMounts.get(container)?.disconnect();
	const observer = new ResizeObserver(syncBounds);
	pendingMounts.set(container, observer);
	observer.observe(stage);
	syncBounds();
}

/**
 * The upgrade won: the container becomes the visible session.
 * @param {?Element} container
 */
export function revealUpgrade(container) {
	const observer = container && pendingMounts.get(container);
	if (observer) {
		observer.disconnect();
		pendingMounts.delete(container);
		for (const property of ['left', 'top', 'width', 'height']) /** @type {HTMLElement} */ (container).style.removeProperty(property);
	}
	container?.classList?.remove(UPGRADE_PENDING_CLASS);
}

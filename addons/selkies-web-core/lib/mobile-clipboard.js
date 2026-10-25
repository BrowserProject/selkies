// @ts-check
export const MOBILE_PASTE_TIMEOUT_MS = 10000;
/** @typedef {{ input: unknown, cancelled: boolean, cancel(): void }} PasteTask */
/** @type {Map<string, PasteTask>} */
const pending = new Map();

/** @param {string} requestId */
export function cancelPanelPaste(requestId) {
    const task = pending.get(requestId);
    if (task) task.cancel();
}

/**
 * Types the page's text into the session through the input core once the core sent it to the
 * remote clipboard, unless the request was cancelled, superseded or ran past its deadline.
 * @param {{ text: unknown, input: { pasteClipboard(): void } | null, requestId: unknown, deadline: number,
 *   sendClipboard: (text: string, mimeType: undefined, onSkip: (reason: unknown, code: unknown) => void) => unknown,
 *   isCurrent: () => boolean, report: (ok: boolean) => void }} request
 */
export async function pasteFromPanel({ text, input, requestId, deadline, sendClipboard, isCurrent, report }) {
    let ok = false;
    /** @type {PasteTask | undefined} */
    let task;
    /** @type {ReturnType<typeof setTimeout> | undefined} */
    let timeout;
    try {
        const now = Date.now();
        if (typeof text !== 'string' || !text || !input || !isCurrent() ||
            typeof requestId !== 'string' || !/^[\w-]{1,80}$/.test(requestId) || pending.has(requestId) ||
            !Number.isFinite(deadline) || deadline <= now || deadline > now + MOBILE_PASTE_TIMEOUT_MS) return;
        for (const other of pending.values()) if (other.input === input) other.cancel();
        /** @type {() => void} */
        let cancel = () => {};
        /** @type {Promise<boolean>} */
        const cancelled = new Promise(resolve => {
            cancel = () => resolve(false);
            timeout = setTimeout(cancel, deadline - Date.now());
        });
        /** @type {PasteTask} */
        const current = { input, cancelled: false, cancel: () => { current.cancelled = true; cancel(); } };
        task = current;
        pending.set(requestId, current);
        let skipped = false;
        const delivered = await Promise.race([
            Promise.resolve(sendClipboard(text, undefined, (_reason, code) => { if (code !== 'clipboardSkipUnchanged') skipped = true; })).then(() => true),
            cancelled,
        ]);
        if (!delivered || skipped || current.cancelled || Date.now() >= deadline || !isCurrent()) return;
        input.pasteClipboard();
        ok = true;
    } catch (_) {
        ok = false;
    } finally {
        clearTimeout(timeout);
        if (task && typeof requestId === 'string' && pending.get(requestId) === task) pending.delete(requestId);
        report(ok);
    }
}

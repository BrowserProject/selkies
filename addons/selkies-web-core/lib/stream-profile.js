// @ts-check
/**
 * @typedef {{ value: string, allowed: string[], pixels?: number, dimension?: number, readonly?: boolean }} StreamProfile
 * @typedef {{ BROWSER_STREAM_PROFILE?: StreamProfile | null }} ProfileWindow
 */

/** @returns {ProfileWindow} */
const profileWindow = () => /** @type {ProfileWindow} */ (/** @type {unknown} */ (window));

/**
 * The server's stream profile for this session, published for the page (`streamProfileState`).
 * @param {StreamProfile | null | undefined} profile
 * @param {boolean} [readonly]
 */
export function applyStreamProfile(profile, readonly) {
    if (!profile || !['latency', 'high'].includes(profile.value) || !Array.isArray(profile.allowed)) {
        profileWindow().BROWSER_STREAM_PROFILE = null;
        window.postMessage({ type: 'streamProfileState', payload: null }, window.location.origin);
        return;
    }
    const previous = profileWindow().BROWSER_STREAM_PROFILE;
    profileWindow().BROWSER_STREAM_PROFILE = { ...profile, readonly: !!readonly };
    publishStreamProfile();
    if (!previous || previous.value !== profile.value || previous.pixels !== profile.pixels) {
        window.dispatchEvent(new Event('resize'));
    }
}

/** Tells the page the current profile again (a page that asks after the core published it). */
export function publishStreamProfile() {
    if (profileWindow().BROWSER_STREAM_PROFILE) {
        window.postMessage({ type: 'streamProfileState', payload: profileWindow().BROWSER_STREAM_PROFILE }, window.location.origin);
    }
}

/**
 * The largest even size inside the profile's pixel and edge budget, with the aspect ratio kept.
 * @param {number} width
 * @param {number} height
 * @param {StreamProfile | null | undefined} [profile]
 * @returns {{width: number, height: number}}
 */
export function fitProfileSize(width, height, profile = typeof window === 'undefined' ? null : profileWindow().BROWSER_STREAM_PROFILE) {
    const pixels = profile?.pixels ?? 0;
    const dimension = profile?.dimension ?? 0;
    if (!(pixels > 0) || !(dimension > 0) || !(width > 0) || !(height > 0)) return { width, height };
    const factor = Math.min(1, dimension / width, dimension / height, Math.sqrt(pixels / (width * height)));
    return { width: Math.max(2, Math.floor(width * factor / 2) * 2), height: Math.max(2, Math.floor(height * factor / 2) * 2) };
}

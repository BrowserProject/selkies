// @ts-check
//
// The viewer's only way to localStorage and sessionStorage. A browser that
// blocks site data (Safari's "Block All Cookies", some private modes and
// embedded views) throws on the bare access to either, and a full store throws
// on a write. The viewer must stream either way, so these never throw: a value
// the browser refuses is kept in memory for this page, and `setItem` answers
// whether it was really stored (so outlives a reload).

/** @param {'localStorage'|'sessionStorage'} name */
function webStore(name) {
  /** @type {Map<string, string>} */
  const unsaved = new Map();
  /** @returns {Storage|null} */
  const area = () => {
    try {
      return globalThis[name] || null;
    } catch (e) {
      return null;
    }
  };
  return Object.freeze({
    /** @param {string} key @returns {string|null} */
    getItem(key) {
      if (unsaved.has(key)) return unsaved.get(key) ?? null;
      try {
        const value = area()?.getItem(key);
        return value === undefined ? null : value;
      } catch (e) {
        return null;
      }
    },
    /** @param {string} key @param {*} value @returns {boolean} Whether the browser stored it. */
    setItem(key, value) {
      const text = String(value);
      try {
        const store = area();
        if (store) {
          store.setItem(key, text);
          unsaved.delete(key);
          return true;
        }
      } catch (e) { /* blocked or full: kept in memory below */ }
      unsaved.set(key, text);
      return false;
    },
    /** @param {string} key */
    removeItem(key) {
      unsaved.delete(key);
      try {
        area()?.removeItem(key);
      } catch (e) { /* nothing stored that could be removed */ }
    },
    /** @returns {string[]} Every key held, stored or in memory. */
    keys() {
      const keys = new Set(unsaved.keys());
      try {
        const store = area();
        for (let i = 0; store && i < store.length; i += 1) {
          const key = store.key(i);
          if (key !== null) keys.add(key);
        }
      } catch (e) { /* only the keys in memory */ }
      return [...keys];
    },
  });
}

export const localStore = webStore('localStorage');
export const sessionStore = webStore('sessionStorage');

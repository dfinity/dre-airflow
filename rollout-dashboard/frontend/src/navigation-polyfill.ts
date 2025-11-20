/**
 * Polyfill for the Navigation API to support Firefox and other browsers
 * that don't yet implement it.
 * 
 * The Navigation API (window.navigation) is currently only supported in
 * Chromium-based browsers. This polyfill provides a minimal implementation
 * to prevent errors in other browsers.
 * 
 * See: https://developer.mozilla.org/en-US/docs/Web/API/Navigation_API
 */

if (typeof window !== 'undefined' && !('navigation' in window)) {
  // Create a minimal polyfill that provides the basic structure
  // Most build tools only check for existence, not full functionality
  (window as any).navigation = {
    addEventListener: () => {},
    removeEventListener: () => {},
    navigate: () => Promise.resolve(),
    reload: () => {},
    back: () => {},
    forward: () => {},
    canGoBack: false,
    canGoForward: false,
    currentEntry: null,
    entries: () => [],
    transition: null,
    updateCurrentEntry: () => {},
  };
}

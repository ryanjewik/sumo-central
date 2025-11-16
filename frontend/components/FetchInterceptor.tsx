"use client";

import { useEffect } from 'react';

export default function FetchInterceptor() {
  useEffect(() => {
    if (typeof window === 'undefined') return;
    // don't install twice
    if ((window as any).__fetchInterceptorInstalled) return;
    (window as any).__fetchInterceptorInstalled = true;

    const orig = window.fetch.bind(window);
    // eslint-disable-next-line @typescript-eslint/ban-types
    (window as any).fetch = async function (input: RequestInfo, init?: RequestInit) {
      try {
        const url = typeof input === 'string' ? input : (input as Request).url;
        // log every fetch for debugging
        console.debug('FetchInterceptor: fetch called', { url, init });

        // if an absolute loopback address is used (127.*) while the page origin
        // is a different hostname (e.g. localhost), rewrite to a root-relative
        // path so the browser talks to the same origin (Next proxy).
        if (typeof url === 'string' && url.match(/^https?:\/\/127\./)) {
          try {
            const u = new URL(url);
            const rel = u.pathname + u.search + u.hash;
            console.warn('FetchInterceptor: rewriting loopback absolute URL to root-relative', { url, rel, stack: new Error().stack });
            return orig(rel, init);
          } catch (e) {
            console.error('FetchInterceptor: failed to rewrite url', e);
          }
        }
      } catch (e) {
        console.error('FetchInterceptor: error in interceptor', e);
      }
      return orig(input, init);
    };

    return () => {
      // restore original if needed
      try {
        if ((window as any).__fetchInterceptorInstalled) {
          (window as any).fetch = orig;
          delete (window as any).__fetchInterceptorInstalled;
        }
      } catch (e) { /* ignore */ }
    };
  }, []);

  return null;
}

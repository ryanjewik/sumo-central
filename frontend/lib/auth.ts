"use client";

let accessToken: string | null = null;

// For browser client requests we want to call our Next.js API proxy under /api/*
// so the browser talks to the Next server (no CORS needed) and the Next server
// forwards requests to the Go backend. Therefore, leave /api/* and /auth/*
// requests as relative URLs so they hit Next.
function resolveUrl(input: RequestInfo): RequestInfo {
  if (typeof input !== 'string') return input;
  // If it's an internal API path, return it unchanged (relative)
  if (input.startsWith('/api/') || input.startsWith('/auth/')) return input;
  return input;
}

export function setAccessToken(token: string | null) {
  accessToken = token;
}

export function getAccessToken(): string | null {
  return accessToken;
}

export function clearAccessToken() {
  accessToken = null;
}

// logout: call backend to revoke refresh token and clear local access token
export async function logout(): Promise<void> {
  try {
    // call backend logout which revokes refresh token and clears cookie
    await fetch(resolveUrl('/api/auth/logout'), { method: 'POST', credentials: 'include' });
  } catch {
    // ignore errors; ensure local state cleared
  }
  clearAccessToken();
}

// tryRefresh: attempt to get a new access token using the refresh cookie.
// If successful, set the in-memory access token and return basic user info
// by calling the `/api/auth/auth` endpoint (which returns JWT claims).
export async function tryRefresh(): Promise<{ id?: string; username?: string } | null> {
  try {
    const r = await fetch(resolveUrl('/api/auth/refresh'), { method: 'POST', credentials: 'include' });
    if (!r.ok) return null;
    const data = await r.json();
    if (data && data.access_token) {
      setAccessToken(data.access_token);
      // Decode JWT payload locally to obtain user id and name without extra roundtrip
      try {
        const parts = data.access_token.split('.');
        if (parts.length >= 2) {
          const payload = parts[1];
          // base64url decode
          const base64 = payload.replace(/-/g, '+').replace(/_/g, '/');
          const padded = base64.padEnd(base64.length + (4 - (base64.length % 4)) % 4, '=');
          const decoded = atob(padded);
          const claims = JSON.parse(decoded);
          return { id: claims.sub, username: claims.name } as { id?: string; username?: string };
        }
      } catch {
        // failed to decode, fall through to clear token below
      }
    }
  } catch {
    // fallthrough
  }
  clearAccessToken();
  return null;
}

// fetchWithAuth: attach Authorization header when access token exists.
// If request returns 401, attempt a single refresh using `/api/auth/refresh` (credentials included)
// and retry the original request one time.
export async function fetchWithAuth(input: RequestInfo, init?: RequestInit): Promise<Response> {
  init = init ? { ...init } : {};
  init.credentials = 'include';

  const makeRequest = async () => {
    const headers = new Headers(init!.headers || {});
    if (accessToken) {
      headers.set('Authorization', `Bearer ${accessToken}`);
    }
    init!.headers = headers;
    return fetch(resolveUrl(input), init);
  };

  let res = await makeRequest();
  if (res.status !== 401) return res;

  // try refresh
  try {
  const r = await fetch(resolveUrl('/api/auth/refresh'), { method: 'POST', credentials: 'include' });
    if (r.ok) {
      const data = await r.json();
      if (data && data.access_token) {
        setAccessToken(data.access_token);
        // retry original request once
        res = await makeRequest();
        return res;
      }
    }
  } catch {
    // fall through to return original 401
  }

  // refresh failed; clear token and return original 401 response
  clearAccessToken();
  return res;
}

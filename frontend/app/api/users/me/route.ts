import { NextResponse, NextRequest } from 'next/server';

const CANDIDATES = [
  process.env.BACKEND_URL,
  process.env.NEXT_PUBLIC_BACKEND_URL,
  process.env.BACKEND_INTERNAL_URL,
  'http://gin-backend:8080',
  'http://host.docker.internal:8080',
  'http://127.0.0.1:8080',
].filter(Boolean) as string[];

async function tryProxy(path: string, request: Request) {
  const incoming = request.headers;
  // build headers to forward
  const forwarded = new Headers();
  for (const [k, v] of incoming) {
    if (k.toLowerCase() === 'host') continue;
    forwarded.set(k, v as string);
  }

  // include body if present
  let body: ArrayBuffer | undefined = undefined;
  try {
    // Some methods have no body; arrayBuffer will throw for streaming bodies in some runtimes, so guard
    if (request.method !== 'GET' && request.method !== 'HEAD') {
      body = await request.arrayBuffer();
    }
  } catch (e) {
    // ignore
  }

  let lastErr: any = null;
  for (const base of CANDIDATES) {
    const url = `${base}${path}`;
    try {
      const res = await fetch(url, {
        method: request.method,
        headers: forwarded,
        body: body,
        // allow cookies to be sent to backend
        credentials: 'include',
      });

      // Copy response headers (including Set-Cookie)
      const respHeaders = new Headers();
      res.headers.forEach((v, k) => {
        if (k.toLowerCase() === 'transfer-encoding') return;
        respHeaders.set(k, v);
      });

      // Stream body back
      return new NextResponse(res.body, { status: res.status, headers: respHeaders });
    } catch (err: any) {
      lastErr = err;
      // try next candidate
    }
  }

  const msg = lastErr && lastErr.message ? lastErr.message : 'backend unreachable';
  return new NextResponse(JSON.stringify({ error: 'backend_unavailable', message: msg, tried: CANDIDATES }), { status: 502, headers: { 'content-type': 'application/json' } });
}

export async function GET(request: NextRequest) {
  return tryProxy('/users/me', request as unknown as Request);
}

export async function PATCH(request: NextRequest) {
  return tryProxy('/users/me', request as unknown as Request);
}

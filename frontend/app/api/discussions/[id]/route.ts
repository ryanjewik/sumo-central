import { NextRequest, NextResponse } from 'next/server'

export const runtime = 'nodejs'

const FALLBACK_CANDIDATES = [
  process.env.BACKEND_URL,
  process.env.NEXT_PUBLIC_BACKEND_URL,
  process.env.BACKEND_INTERNAL_URL,
  // Prefer the compose service DNS name so the proxy from inside the frontend
  // container talks to the backend container directly when available.
  'http://gin-backend:8080',
  'http://host.docker.internal:8080',
  'http://127.0.0.1:8080',
].filter(Boolean) as string[]

async function tryProxy(path: string, incoming: NextRequest) {
  const outHeaders: Record<string, string> = {}
  for (const [k, v] of incoming.headers) {
    if (k.toLowerCase() === 'host') continue
    outHeaders[k] = v
  }

  let lastErr: any = null
  for (const base of FALLBACK_CANDIDATES) {
    const url = `${base}${path}`
    try {
      const res = await fetch(url, { method: incoming.method, headers: outHeaders })
      const body = await res.arrayBuffer()
      const headers: Record<string, string> = {}
      res.headers.forEach((v, k) => { headers[k] = v })
      return new NextResponse(body, { status: res.status, headers })
    } catch (err: any) {
      lastErr = err
      // try next candidate
    }
  }

  const msg = (lastErr && lastErr.message) ? lastErr.message : 'backend fetch error'
  return new NextResponse(JSON.stringify({ error: 'backend_unavailable', message: msg, tried: FALLBACK_CANDIDATES }), { status: 502, headers: { 'content-type': 'application/json' } })
}

export async function GET(req: NextRequest, context: any) {
  const params = await Promise.resolve(context?.params || {})
  const id = String(params?.id || '')
  const path = `/discussions/${encodeURIComponent(id)}`

  // write a short debug record to /tmp for inspection
  // Removed verbose debug logging to /tmp and console to keep server output clean.

  return tryProxy(path, req)
}

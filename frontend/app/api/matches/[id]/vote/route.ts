import { NextRequest, NextResponse } from 'next/server'

const BACKEND_URL = process.env.BACKEND_URL || process.env.NEXT_PUBLIC_BACKEND_URL || 'http://gin-backend:8080'

async function proxy(path: string, incoming: NextRequest) {
  const outHeaders: Record<string,string> = {}
  for (const [k, v] of incoming.headers) {
    if (k.toLowerCase() === 'host') continue
    outHeaders[k] = v
  }

  const candidates = [
    process.env.BACKEND_URL,
    process.env.NEXT_PUBLIC_BACKEND_URL,
    process.env.BACKEND_INTERNAL_URL,
    BACKEND_URL,
    'http://host.docker.internal:8080',
    'http://127.0.0.1:8080',
  ].filter(Boolean) as string[]

  let lastErr: any = null
  for (const base of candidates) {
    const url = `${base}${path}`
    try {
      const bodyBuf = incoming.method === 'GET' ? undefined : await incoming.arrayBuffer()
      const res = await fetch(url, { method: incoming.method, headers: outHeaders, body: bodyBuf })
      const body = await res.arrayBuffer()
      const headers: Record<string,string> = {}
      res.headers.forEach((v,k) => { headers[k] = v })
      return new NextResponse(body, { status: res.status, headers })
    } catch (err: any) {
      lastErr = err
    }
  }

  const msg = (lastErr && lastErr.message) ? lastErr.message : 'backend fetch error'
  return new NextResponse(JSON.stringify({ error: 'backend_unavailable', message: msg, tried: candidates }), { status: 502, headers: { 'content-type': 'application/json' } })
}

export async function POST(req: NextRequest, context: any) {
  // Next 16 may provide params as a Promise in the context; accept any and await if needed.
  const params = context && context.params ? await context.params : undefined
  const id = encodeURIComponent(params?.id ?? '')
  return proxy(`/matches/${id}/vote`, req)
}

export async function OPTIONS(req: NextRequest, context: any) {
  // Let the browser know CORS preflight is allowed when necessary (Next is same-origin
  // but keep parity with backend expectations).
  return new NextResponse(null, { status: 204, headers: { 'Access-Control-Allow-Methods': 'GET,POST,OPTIONS', 'Access-Control-Allow-Headers': 'Content-Type, Authorization' } })
}

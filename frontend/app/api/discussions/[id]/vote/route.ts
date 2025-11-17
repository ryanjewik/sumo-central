import { NextRequest, NextResponse } from 'next/server'

const CANDIDATES = [
  process.env.BACKEND_URL,
  process.env.NEXT_PUBLIC_BACKEND_URL,
  process.env.BACKEND_INTERNAL_URL,
  'http://gin-backend:8080',
  'http://gin-backend:8080',
  'http://host.docker.internal:8080',
  'http://127.0.0.1:8080',
].filter(Boolean) as string[]

async function tryProxy(path: string, req: NextRequest) {
  const outHeaders: Record<string,string> = {}
  for (const [k, v] of req.headers) {
    if (k.toLowerCase() === 'host') continue
    outHeaders[k] = v
  }

  let lastErr: any = null
  for (const base of CANDIDATES) {
    const url = `${base}${path}`
    try {
      const res = await fetch(url, { method: req.method, headers: outHeaders, body: await req.arrayBuffer() })
      const body = await res.arrayBuffer()
      const headers: Record<string,string> = {}
      res.headers.forEach((v,k) => { headers[k] = v })
      return new NextResponse(body, { status: res.status, headers })
    } catch (err: any) {
      lastErr = err
    }
  }

  const msg = (lastErr && lastErr.message) ? lastErr.message : 'backend fetch error'
  return new NextResponse(JSON.stringify({ error: 'backend_unavailable', message: msg, tried: CANDIDATES }), { status: 502, headers: { 'content-type': 'application/json' } })
}

export async function POST(req: NextRequest, context: any) {
  const params = await Promise.resolve(context?.params || {})
  const id = String(params?.id || '')
  const path = `/discussions/${encodeURIComponent(id)}/vote`
  return tryProxy(path, req)
}

export async function GET(req: NextRequest, context: any) {
  const params = await Promise.resolve(context?.params || {})
  const id = String(params?.id || '')
  const path = `/discussions/${encodeURIComponent(id)}/vote`
  return tryProxy(path, req)
}

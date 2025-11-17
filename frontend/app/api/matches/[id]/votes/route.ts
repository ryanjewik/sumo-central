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
      const res = await fetch(url, { method: incoming.method, headers: outHeaders })
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

export async function GET(req: NextRequest, context: any) {
  const params = context && context.params ? await context.params : undefined
  const url = new URL(req.url)
  const qs = url.search
  const id = encodeURIComponent(params?.id ?? '')
  return proxy(`/matches/${id}/votes${qs}`, req)
}

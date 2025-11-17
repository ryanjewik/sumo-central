import { NextResponse } from 'next/server';

export async function GET(req: Request, context: any) {
  // context.params may be a plain object or a Promise depending on Next version
  const rawParams = context?.params ?? {};
  const params = typeof rawParams?.then === 'function' ? await rawParams : rawParams;
  const { id } = params ?? {};
  const backend = process.env.BACKEND_URL ?? process.env.NEXT_PUBLIC_API_URL ?? 'http://gin-backend:8080';
  const base = String(backend).replace(/\/$/, '');
  const url = `${base}/rikishi/${encodeURIComponent(id)}`;

  try {
    // forward cookie/auth headers when available
    const headers: Record<string, string> = {};
  const cookie = req.headers?.get?.('cookie');
  const auth = req.headers?.get?.('authorization');
    if (cookie) headers.cookie = cookie;
    if (auth) headers.authorization = auth;

    const res = await fetch(url, { headers });
    const contentType = res.headers.get('content-type') || '';

    if (contentType.includes('application/json')) {
      const json = await res.json();
      return NextResponse.json(json, { status: res.status });
    }

    // fallback to text
    const text = await res.text();
    return new NextResponse(text, { status: res.status, headers: { 'content-type': contentType || 'text/plain' } });
  } catch (err: unknown) {
    const msg = (err && typeof err === 'object' && 'message' in err) ? String((err as any).message) : String(err);
    return NextResponse.json({ error: 'proxy_error', message: msg }, { status: 502 });
  }
}

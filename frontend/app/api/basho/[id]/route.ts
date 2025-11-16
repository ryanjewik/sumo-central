import { NextResponse, NextRequest } from 'next/server';

// Proxy GET /api/basho/:id  -> backend /basho/:id
export async function GET(request: NextRequest, context: any) {
  const params = context && context.params ? await context.params : undefined;
  const id = encodeURIComponent(params?.id ?? '');
  const candidates = [
    process.env.BACKEND_URL,
    process.env.NEXT_PUBLIC_BACKEND_URL,
    'http://localhost:8080',
    'http://host.docker.internal:8080',
    'http://127.0.0.1:8080',
  ].filter(Boolean) as string[];

  // Build forwarded headers (copy most incoming headers but avoid changing host)
  const incoming = request.headers;
  const url = new URL(request.url);
  const qs = url.search || '';

  for (const base of candidates) {
    try {
      const dest = `${base}/basho/${id}${qs}`;
      const forwarded = new Headers();
      for (const [k, v] of incoming) {
        if (k.toLowerCase() === 'host') continue;
        forwarded.set(k, v as string);
      }

      const res = await fetch(dest, {
        method: 'GET',
        headers: forwarded,
        credentials: 'include',
      });

      const respHeaders = new Headers();
      res.headers.forEach((v, k) => {
        if (k.toLowerCase() === 'transfer-encoding') return;
        respHeaders.set(k, v);
      });

      return new NextResponse(res.body, {
        status: res.status,
        headers: respHeaders,
      });
    } catch (e) {
      // try next candidate
    }
  }

  return NextResponse.json({ error: 'Backend unreachable for basho detail' }, { status: 502 });
}

import { NextResponse } from 'next/server'

// Lightweight middleware to log incoming request paths for debugging
// Why: server-rendered params are empty in SSR; this will confirm what
// pathname Next receives before routing the request.
export function middleware(req: any) {
  try {
    // eslint-disable-next-line no-console
    console.log('[MW] incoming pathname:', req.nextUrl?.pathname)
    // eslint-disable-next-line no-console
    console.log('[MW] full url:', req.nextUrl?.href)
  } catch (e) {
    // ignore
  }
  return NextResponse.next()
}

export const config = {
  // Only run for discussion pages to reduce noise
  matcher: ['/discussions/:path*'],
}

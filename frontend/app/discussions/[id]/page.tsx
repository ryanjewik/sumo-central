import React from 'react'
import { headers } from 'next/headers'
import dynamicImport from 'next/dynamic'

// client components (imported dynamically to avoid SSR issues)
const PostVote = dynamicImport(() => import('../../../components/PostVote'))
const AuthorBadge = dynamicImport(() => import('../../../components/AuthorBadge'))

// Ensure this dynamic route is rendered at request time (avoid static pre-render)
export const dynamic = 'force-dynamic'

type Disc = {
  id?: string
  title?: string
  body?: string
  author_username?: string
  created_at?: string
  comments?: any[]
}

/**
 * Server component for discussion thread. This file now includes extra server-side
 * debug output (headers, env) rendered into the HTML so we can inspect what's
 * actually arriving at the Next server inside the container.
 */
export default async function DiscussionThreadPage({ params }: { params: any }) {
  // `params` may be a Promise in newer Next releases; resolve it before use.
  const resolvedParams = await Promise.resolve(params || {})
  const id = resolvedParams?.id

  // Log params for server-side debugging (visible in Next server logs).
  // This won't ship to the client, it's purely to inspect SSR runtime values.
  try {
    // eslint-disable-next-line no-console
    // (removed verbose debug log to keep server output clean)
  } catch (e) {
    // ignore
  }
  // Collect server-side headers and env for debugging when params are missing
  const hdrs = headers()
  const headersObj: Record<string, string> = {}
  try {
    // headers() can be a ReadonlyHeaders or a Promise resolving to it depending on
    // the Next.js runtime version. Normalize by awaiting and then iterating.
    const resolved = await Promise.resolve(hdrs as any)
    const entries = typeof (resolved as any).entries === 'function' ? (resolved as any).entries() : Object.entries(resolved || {})
    for (const [k, v] of entries as any) {
      headersObj[k] = String(v)
    }
  } catch (e) {
    // swallow — debugging should not crash the page
  }

  // We're going to call the Go backend directly from SSR. Prefer an explicit
  // environment variable if present, otherwise try common in-container and
  // host-local addresses. This keeps SSR hitting the Go service directly
  // (skipping the Next API proxy) which reduces one hop and is fine because
  // the Go handler already returns JSON for `/discussions/:id`.
  const backendEnv = process.env.BACKEND_URL || process.env.NEXT_PUBLIC_BACKEND_URL || ''

  if (!id) {
    return (
      <main className="page-offset">
        <div style={{ padding: 24 }}>
          <h2 className="app-text">Discussion not found</h2>
          <p className="app-text">No discussion id provided.</p>
        </div>
      </main>
    )
  }

  try {
    // Try the same backend we used earlier; include the attempted URL in debug output
  // Fetch via same-origin API route; next.config.js rewrites this to the
  // actual Go backend (BACKEND_URL) at runtime. Keep cache: 'no-store' so
  // we always do a fresh request during SSR.
  // Build an absolute same-origin URL from request headers because
  // `fetch` on the server requires an absolute URL in this runtime.
  const proto = headersObj['x-forwarded-proto'] || 'http'
  // Inside the frontend container the Next server is reachable on the
  // container loopback at the internal port (commonly 3000). The client
  // facing Host header (e.g. localhost:8081) is not reachable from inside
  // the container, so build an internal URL first and try that. If that
  // fails, fall back to the external host header we computed earlier.
  const internalPort = headersObj['x-forwarded-port'] || process.env.PORT || '3000'
  const hostHeader = headersObj['x-forwarded-host'] || headersObj['host'] || `localhost:${internalPort}`
  const internalAttemptedUrl = `${proto}://127.0.0.1:${internalPort}/api/discussions/${encodeURIComponent(id)}`
  const externalAttemptedUrl = `${proto}://${hostHeader}/api/discussions/${encodeURIComponent(id)}`

  // Build a candidate list for the Go backend. Prefer an explicit env var,
  // then the Compose service DNS (gin-backend), then host.docker.internal and
  // finally loopback on 127.0.0.1:8080. We'll try each candidate until one
  // returns a successful response.
  const candidates = [
    backendEnv,
    'http://gin-backend:8080',
    'http://host.docker.internal:8080',
    'http://127.0.0.1:8080'
  ].filter(Boolean) as string[]

  let res: Response | undefined
  let attemptedUrl = ''
  let lastError: any = null
  for (const base of candidates) {
    const url = `${base.replace(/\/$/, '')}/discussions/${encodeURIComponent(id)}`
    try {
      res = await fetch(url, { cache: 'no-store' })
      attemptedUrl = url
      // if we got an HTTP response (even non-ok), stop trying others so we
      // surface the correct status to the user
      break
    } catch (err) {
      lastError = err
      // try next candidate
    }
  }
  if (!res) {
    // none of the candidates produced a Response; rethrow last error to be
    // handled by outer catch and shown in the debug UI
    throw lastError || new Error('No backend candidates available')
  }

    if (!res.ok) {
      const text = await res.text().catch(() => '')
      return (
        <div style={{ maxWidth: 900, margin: '4rem auto', padding: '0 1rem' }}>
          <h2 className="app-text">Discussion not found</h2>
          <p className="app-text">This discussion may have been removed.</p>
          <pre style={{ background: '#f6f6f6', padding: 12, marginTop: 12 }}>HTTP {res.status}: {text}</pre>
        </div>
      )
    }

    const data = (await res.json()) as Disc

    // Render the discussion content server-side, then mount a client-side
    // comments thread component for interactive actions (post, reply, vote).
  const CommentsThread = (await import('../../../components/CommentsThreadClient')).default

    return (
      <main className="page-offset">
        <div style={{ maxWidth: 900, margin: '0 auto', padding: '0 1rem' }}>
          {/* discussion card styled similar to ForumSection; use distinct green background for the whole thread */}
          <div style={{ background: '#A3E0B8', padding: 16, borderRadius: 8, marginBottom: 16, boxShadow: '0 2px 4px rgba(0,0,0,0.05)' }}>
            <h1 style={{ color: '#563861', marginTop: 0 }}>{data.title}</h1>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1rem' }}>
            <div style={{ color: '#563861' }}>
              By <span style={{ fontWeight: 700, padding: '2px 6px', borderRadius: 6, background: 'rgba(255,255,255,0.15)' }}>{data.author_username ?? 'anonymous'}</span> — {data.created_at}
              {/* Render author badge client component (dynamic import) */}
              <div style={{ marginTop: 6 }}>
                {/* @ts-ignore - data may not include author_id in all responses */}
                <AuthorBadge userId={(data as any).author_id || (data as any).authorId} username={data.author_username} />
              </div>
            </div>
            <div>
              {/* Post vote client component */}
              {/* @ts-ignore - initial counts optional */}
              <PostVote discussionId={String(data.id)} initialUp={(data as any).upvotes ?? (data as any).upvote_count ?? 0} initialDown={(data as any).downvotes ?? (data as any).downvote_count ?? 0} />
            </div>
            </div>
            <article style={{ background: 'transparent', padding: 16, borderRadius: 8, border: 'none' }}>
              <div style={{ whiteSpace: 'pre-wrap', color: '#563861' }}>{data.body}</div>
            </article>
          </div>

          {/* keep comments visually inside the same green thread background */}
          <div style={{ background: '#A3E0B8', padding: 12, borderRadius: 8 }}>
            <CommentsThread discussionId={String(data.id)} initialComments={data.comments || []} />
          </div>
        </div>
      </main>
    )
  } catch (err: any) {
    return (
      <main className="page-offset">
        <div style={{ padding: 24 }}>
          <h2 className="app-text">Discussion unavailable</h2>
          <div style={{ marginTop: 12, color: '#555' }}>id: <code>{id}</code></div>
          <pre style={{ background: '#f6f6f6', padding: 12, marginTop: 8, color: '#333' }}>{String(err?.message ?? err)}</pre>
        </div>
      </main>
    )
  }
}

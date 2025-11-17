import React from 'react';
import ForumListClient from './ForumListClient';
import { ForumPost } from './ForumSection';

async function fetchInitial() : Promise<{ posts: ForumPost[]; failed: boolean }> {
  try {
    // Prefer calling the same-origin Next API proxy first. This avoids
    // leaking internal container hostnames (e.g. `gin-backend`) into the
    // browser bundle and avoids making requests that can't be resolved
    // from the user's machine when running inside Docker networks.
    const frontendBase = process.env.FRONTEND_INTERNAL_URL || process.env.NEXT_PUBLIC_FRONTEND_URL || `http://127.0.0.1:${process.env.PORT || '3000'}`;

    // create a short timeout to avoid hanging server renders
    const ctrl = new AbortController();
    const timeout = setTimeout(() => ctrl.abort(), 2000);
    let res: Response | null = null;
    try {
      res = await fetch(`${frontendBase.replace(/\/$/, '')}/api/discussions?skip=0&limit=5`, { cache: 'no-store', signal: ctrl.signal });
    } catch (e) {
      // primary attempt failed; fall back to contacting the backend directly
      // (useful in setups where the Next API proxy isn't available).
      try {
  const backendBase = process.env.BACKEND_INTERNAL_URL || process.env.BACKEND_URL || process.env.NEXT_PUBLIC_BACKEND_URL || 'http://gin-backend:8080';
        // reuse abort controller for fallback, reset timeout
        clearTimeout(timeout);
        const ctrl2 = new AbortController();
        const t2 = setTimeout(() => ctrl2.abort(), 2000);
        res = await fetch(`${backendBase.replace(/\/$/, '')}/discussions?skip=0&limit=5`, { cache: 'no-store', signal: ctrl2.signal });
        clearTimeout(t2);
      } catch (e2) {
        // swallowed - will handle below
      }
    } finally {
      clearTimeout(timeout);
    }

    if (!res || !res.ok) {
      console.error('ForumListServer.fetchInitial: failed to fetch discussions', { frontendBase: process.env.FRONTEND_INTERNAL_URL || process.env.NEXT_PUBLIC_FRONTEND_URL, status: res ? res.status : 'no-response' });
      return { posts: [], failed: true };
    }
    const data = await res.json();
    if (!Array.isArray(data)) return { posts: [], failed: true };
    // normalize
    const posts = data.map((p: any) => ({
      id: p.id || (p._id && p._id.toString && p._id.toString()) || '',
      upvotes: p.upvotes ?? 0,
      downvotes: p.downvotes ?? 0,
      author: p.author_username ?? 'anonymous',
      date_created: p.created_at ?? new Date().toISOString(),
      title: p.title,
      body: p.body,
    }))
    return { posts, failed: false }
  } catch (err) {
    console.error('ForumListServer.fetchInitial error', err)
    return { posts: [], failed: true }
  }
}

export default async function ForumListServer() {
  const { posts: initial, failed } = await fetchInitial();
  return (
    // render a client component that hydrates and continues loading on scroll
    <div>
      {/* initial posts are passed to client for hydration and further infinite scroll */}
      {/* eslint-disable-next-line react/jsx-no-undef */}
      <ForumListClient initial={initial} initialLoadFailed={failed} />
    </div>
  )
}

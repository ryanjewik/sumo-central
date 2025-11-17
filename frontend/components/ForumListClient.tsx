"use client";

import React, { useEffect, useRef, useState, useCallback } from 'react';
import { fetchWithAuth } from '@/lib/auth'
import ForumSection, { ForumPost } from './ForumSection';

type BackendPost = any;
const PAGE_SIZE = 5;

export default function ForumListClient({ initial, initialLoadFailed, maxItems, baseApi }: { initial: ForumPost[], initialLoadFailed?: boolean, maxItems?: number, baseApi?: string }) {
  const [posts, setPosts] = useState<ForumPost[]>(initial ?? []);
  const [loading, setLoading] = useState(false);
  const [hasMore, setHasMore] = useState(!initialLoadFailed);
  const skipRef = useRef(posts.length || 0);
  const sentinelRef = useRef<HTMLDivElement | null>(null);

  const [initialFailed, setInitialFailed] = useState<boolean>(!!initialLoadFailed);

  // ensure derived refs/state reflect server-provided initial props and help
  // with diagnostics when the homepage server fetch fails or returns zero posts.
  useEffect(() => {
    try {
      console.debug('ForumListClient: init props', { initialLength: initial?.length ?? 0, initialLoadFailed });
    } catch (e) {}
    // initialize skipRef from the initial posts length so paging resumes correctly
    skipRef.current = (initial && initial.length) ? initial.length : 0;
    // if server indicated the initial load failed, ensure we don't attempt client paging
    setHasMore(!initialLoadFailed);
    setInitialFailed(!!initialLoadFailed);
  }, [initial, initialLoadFailed]);

  useEffect(() => {
    const onCreated = (e: any) => {
      try {
        const created = e.detail;
        const mapped: ForumPost = {
          id: (created.id) || (created._id && created._id.toString && created._id.toString()) || '',
          upvotes: created.upvotes ?? 0,
          downvotes: created.downvotes ?? 0,
          author: created.author_username ?? 'anonymous',
          date_created: created.created_at ?? new Date().toISOString(),
          title: created.title,
          body: created.body,
        };
        setPosts(prev => [mapped, ...prev]);
        skipRef.current += 1;
      } catch (err) { console.error(err) }
    };
    document.addEventListener('discussion-created', onCreated as EventListener);
    return () => document.removeEventListener('discussion-created', onCreated as EventListener);
  }, []);

  const mapBackend = (p: BackendPost): ForumPost => ({
    id: (p.id) || (p._id && p._id.toString && p._id.toString()) || p.author_id || '',
    upvotes: p.upvotes ?? 0,
    downvotes: p.downvotes ?? 0,
    author: p.author_username ?? 'anonymous',
    date_created: p.created_at ?? new Date().toISOString(),
    title: p.title,
    body: p.body,
  });

  // small ref used for debouncing repeated observer triggers
  const lastLoadAtRef = useRef<number | null>(null);

  const loadMore = useCallback(async () => {
    if (loading || !hasMore) return;
    // debounce / guard against extremely-frequent observer callbacks
    const now = Date.now();
    if ((lastLoadAtRef.current || 0) + 300 > now) {
      // too soon since last load
      console.debug('ForumListClient: loadMore ignored due to debounce', { skip: skipRef.current, loading, hasMore });
      return;
    }
    lastLoadAtRef.current = now;
    setLoading(true);
    try {
      const skip = skipRef.current;
  const apiBase = baseApi ?? '/api/discussions'
  const sep = apiBase.includes('?') ? '&' : '?'
  const res = await fetchWithAuth(`${apiBase}${sep}skip=${skip}&limit=${PAGE_SIZE}`);
      if (!res.ok) throw new Error('failed to load');
      const data = await res.json();
      const items: BackendPost[] = Array.isArray(data) ? data : data.items ?? [];
      const mapped = items.map(mapBackend);
      console.debug('ForumListClient: loadMore result', { skip, returned: items.length, mapped: mapped.length });

      // If a maxItems limit is provided (homepage), enforce it and stop further loads
      if (typeof maxItems === 'number') {
        const remaining = Math.max(0, maxItems - posts.length);
        if (remaining <= 0) {
          setHasMore(false);
        } else {
          const toAppend = mapped.slice(0, remaining);
          setPosts(prev => [...prev, ...toAppend]);
          skipRef.current += toAppend.length;
          // we've reached the configured max, stop further loads
          if (posts.length + toAppend.length >= maxItems) setHasMore(false);
          // if backend returned fewer than requested slice, and it was less than page size,
          // we should stop further loads as well
          if (toAppend.length < Math.min(PAGE_SIZE, remaining)) setHasMore(false);
        }
      } else {
        setPosts(prev => [...prev, ...mapped]);
        skipRef.current += mapped.length;
        // if the backend returned fewer items than page size we reached the end
        if (mapped.length < PAGE_SIZE) setHasMore(false);
        // if backend returned no items at all for this skip, stop further loads to avoid loops
        if (mapped.length === 0) {
          console.warn('ForumListClient: backend returned 0 items for skip', skip, '-- stopping further loads to avoid loop');
          setHasMore(false);
        }
      }
    } catch (err) {
      console.error(err);
        // stop further retries on repeated failures to avoid infinite fetch loops
        setHasMore(false);
    } finally {
      setLoading(false);
    }
  }, [loading, hasMore]);

  

  useEffect(() => {
    // only observe when we actually expect more data; avoids extra observer
    // callbacks on pages where we already know there's nothing to load.
    if (!sentinelRef.current) return;
    if (!hasMore) return;
    const obs = new IntersectionObserver(entries => {
      if (entries[0].isIntersecting) {
        loadMore();
      }
    });
    obs.observe(sentinelRef.current);
    return () => obs.disconnect();
  }, [loadMore, hasMore]);

  return (
    <>
      {initialFailed && posts.length === 0 ? (
        <div style={{ padding: 12, color: '#b02a37' }}>
          <div>Unable to load discussions right now.</div>
          <div style={{ marginTop: 8 }}>
            <button onClick={async () => {
                try {
                setLoading(true);
                const apiBase = baseApi ?? '/api/discussions'
                const sep = apiBase.includes('?') ? '&' : '?'
                const r = await fetchWithAuth(`${apiBase}${sep}skip=0&limit=${PAGE_SIZE}`);
                if (!r.ok) throw new Error('failed');
                const data = await r.json();
                const items: BackendPost[] = Array.isArray(data) ? data : data.items ?? [];
                const mapped = items.map(mapBackend);
                setPosts(mapped);
                skipRef.current = mapped.length;
                setHasMore(mapped.length >= PAGE_SIZE);
                setInitialFailed(false);
              } catch (e) {
                console.error('retry initial load failed', e);
                // keep initialFailed true so user can try again manually
              } finally {
                setLoading(false);
              }
            }} disabled={loading}>
              {loading ? 'Retrying...' : 'Retry'}
            </button>
          </div>
        </div>
      ) : posts.length === 0 ? (
        <div style={{ padding: 12, color: '#563861' }}>no forum posts available</div>
      ) : (
        <ForumSection posts={posts} />
      )}
      <div ref={sentinelRef} style={{ height: 1 }} />
    </>
  );
}

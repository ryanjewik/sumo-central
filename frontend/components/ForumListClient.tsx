"use client";

import React, { useEffect, useRef, useState, useCallback } from 'react';
import { fetchWithAuth } from '@/lib/auth'
import ForumSection, { ForumPost } from './ForumSection';

type BackendPost = any;
const PAGE_SIZE = 20;

export default function ForumListClient({ initial }: { initial: ForumPost[] }) {
  const [posts, setPosts] = useState<ForumPost[]>(initial ?? []);
  const [loading, setLoading] = useState(false);
  const [hasMore, setHasMore] = useState(true);
  const skipRef = useRef(posts.length || 0);
  const sentinelRef = useRef<HTMLDivElement | null>(null);

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

  const loadMore = useCallback(async () => {
    if (loading || !hasMore) return;
    setLoading(true);
    try {
      const skip = skipRef.current;
  const res = await fetchWithAuth(`/api/discussions?skip=${skip}&limit=${PAGE_SIZE}`);
      if (!res.ok) throw new Error('failed to load');
      const data = await res.json();
      const items: BackendPost[] = Array.isArray(data) ? data : data.items ?? [];
      const mapped = items.map(mapBackend);
      setPosts(prev => [...prev, ...mapped]);
      skipRef.current += mapped.length;
      if (mapped.length < PAGE_SIZE) setHasMore(false);
    } catch (err) {
      console.error(err);
    } finally {
      setLoading(false);
    }
  }, [loading, hasMore]);

  useEffect(() => {
    if (!sentinelRef.current) return;
    const obs = new IntersectionObserver(entries => {
      if (entries[0].isIntersecting) {
        loadMore();
      }
    });
    obs.observe(sentinelRef.current);
    return () => obs.disconnect();
  }, [loadMore]);

  return (
    <>
      {posts.length === 0 ? (
        <div style={{ padding: 12, color: '#563861' }}>no forum posts available</div>
      ) : (
        <ForumSection posts={posts} />
      )}
      <div ref={sentinelRef} style={{ height: 1 }} />
      <div style={{ textAlign: 'center', padding: 12 }}>{loading ? 'Loading...' : hasMore ? '' : 'No more posts'}</div>
    </>
  );
}

"use client";

import React, { useEffect, useRef, useState } from 'react';
import Link from 'next/link';
import styles from './page.module.css';

type RikishiItem = { id: string; shikona?: string };

const PAGE_SIZE = 30; // change to 500 if you want large page fetches client-side

function buildAttempts() {
  // Try common backend endpoints (relative paths). Prefer Next proxy /api/* first so
  // browser requests are same-origin and go through the Next server proxy to backend.
  return ['/api/rikishi', '/rikishi', '/api/homepage'];
}

async function tryFetchUrl(url: string, page: number, limit: number) {
  // append pagination params where appropriate
  const sep = url.includes('?') ? '&' : '?';
  const pagedUrl = `${url}${sep}page=${page}&limit=${limit}`;
  const res = await fetch(pagedUrl, { cache: 'no-store' });
  if (!res.ok) throw new Error('not ok');
  return res.json();
}

export default function RikishiIndexPage() {
  const [items, setItems] = useState<RikishiItem[]>([]);
  const [page, setPage] = useState(1);
  const [loading, setLoading] = useState(false);
  const [hasMore, setHasMore] = useState(true);
  const [total, setTotal] = useState<number | null>(null);
  const sentinelRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    let mounted = true;
    const attempts = buildAttempts();

    const fetchOnce = async () => {
      setLoading(true);
      try {
        for (const url of attempts) {
          try {
            const json = await tryFetchUrl(url, 1, PAGE_SIZE);
            if (!mounted) return;

            let newItems: RikishiItem[] = [];
            if (Array.isArray(json)) {
              newItems = json.map((it: any) => ({ id: String(it.id ?? it), shikona: it.shikona ?? it.name ?? '' }));
              setHasMore(false);
            } else if (json && Array.isArray(json.items)) {
              newItems = json.items.map((it: any) => ({ id: String(it.id), shikona: it.shikona ?? it.name ?? '' }));
              if (typeof json.total === 'number') setTotal(json.total);
              if (json.items.length < PAGE_SIZE || (json.total && newItems.length >= json.total)) setHasMore(false);
            } else if (json && typeof json === 'object') {
              // maybe homepage doc contains list
              const possible = json.rikishi_ids || json.rikishi_list || json.all_rikishi || json.rikishi_index || json.items;
              if (Array.isArray(possible)) {
                newItems = possible.map((it: any) => ({ id: String(it.id ?? it), shikona: it.shikona ?? it.name ?? '' }));
                setHasMore(false);
              }
            }

            if (newItems.length > 0) {
              setItems(newItems);
              return;
            }
          } catch (e) {
            // try next endpoint
          }
        }
      } finally {
        if (mounted) setLoading(false);
      }
    };

    fetchOnce();
    return () => {
      mounted = false;
    };
  }, []);

  // fetch subsequent pages when `page` increases
  useEffect(() => {
    if (page === 1) return; // already fetched
    let mounted = true;
    const attempts = buildAttempts();

    const fetchPage = async () => {
      setLoading(true);
      try {
        for (const url of attempts) {
          try {
            const json = await tryFetchUrl(url, page, PAGE_SIZE);
            if (!mounted) return;

            let newItems: RikishiItem[] = [];
            if (Array.isArray(json)) {
              newItems = json.map((it: any) => ({ id: String(it.id ?? it), shikona: it.shikona ?? it.name ?? '' }));
            } else if (json && Array.isArray(json.items)) {
              newItems = json.items.map((it: any) => ({ id: String(it.id), shikona: it.shikona ?? it.name ?? '' }));
              if (typeof json.total === 'number') setTotal(json.total);
            }

            if (newItems.length > 0) {
              setItems((prev) => [...prev, ...newItems]);
              if (newItems.length < PAGE_SIZE) setHasMore(false);
              if (total && items.length + newItems.length >= total) setHasMore(false);
              return;
            }
          } catch (e) {
            // try next
          }
        }
      } finally {
        if (mounted) setLoading(false);
      }
    };

    fetchPage();

    return () => {
      mounted = false;
    };
  }, [page]);

  // sentinel observer
  useEffect(() => {
    const el = sentinelRef.current;
    if (!el) return;
    const obs = new IntersectionObserver(
      (entries) => {
        if (entries[0].isIntersecting && !loading && hasMore) setPage((p) => p + 1);
      },
      { rootMargin: '200px' }
    );
    obs.observe(el);
    return () => obs.disconnect();
  }, [loading, hasMore]);

  return (
    <>
      <div id="background"></div>
      <main className="page-offset">
        <div className="content-box" style={{ padding: '1rem', position: 'relative', zIndex: 1 }}>
          <div style={{ width: '100%' }}>
        <h1 className="app-text" style={{ marginBottom: '1rem' }}>Rikishi — Index</h1>

        {items.length === 0 && !loading && (
          <div className="app-text">No rikishi found or backend endpoint unavailable.</div>
        )}

        {items.length > 0 && (
          <div className={styles.container}>
            {items.map((it) => (
              <Link key={it.id} href={`/rikishi/${encodeURIComponent(String(it.id))}`} className={styles.item}>
                <div className={styles.title}>{it.shikona && it.shikona.length > 0 ? it.shikona : it.id}</div>
                <div className={styles.id}>({it.id})</div>
              </Link>
            ))}
          </div>
        )}

        <div ref={sentinelRef} style={{ height: 1 }} />

        <div className={styles.loading}>
          {loading && <div className="app-text">Loading…</div>}
          {!hasMore && items.length > 0 && <div className={styles.end}>End of list</div>}
          </div>
        </div>
        </div>
      </main>
    </>
  );
}

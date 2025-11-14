"use client";

import React, { useEffect, useRef, useState } from 'react';
import Link from 'next/link';
import styles from './page.module.css';

type BashoItem = { id: string; location?: string; start_date?: string; end_date?: string };

const PAGE_SIZE = 20;

function buildAttempts() {
  return ['/basho', '/api/basho', '/api/homepage'];
}

async function tryFetchUrl(url: string, page: number, limit: number) {
  const sep = url.includes('?') ? '&' : '?';
  const pagedUrl = `${url}${sep}page=${page}&limit=${limit}`;
  const res = await fetch(pagedUrl, { cache: 'no-store' });
  if (!res.ok) throw new Error('not ok');
  return res.json();
}

export default function BashoIndexPage() {
  const [items, setItems] = useState<BashoItem[]>([]);
  const [page, setPage] = useState(1);
  const [loading, setLoading] = useState(true);
  const [hasMore, setHasMore] = useState(true);
  const [total, setTotal] = useState<number | null>(null);
  const sentinelRef = useRef<HTMLDivElement | null>(null);

  // initial fetch
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

            let newItems: BashoItem[] = [];
            if (Array.isArray(json)) {
              newItems = json.map((id: any) => ({ id: String(id) }));
              setHasMore(false);
            } else if (json && Array.isArray(json.items)) {
              newItems = json.items.map((it: any) => ({ id: String(it.id), location: it.location ?? '', start_date: it.start_date ?? '', end_date: it.end_date ?? '' }));
              if (typeof json.total === 'number') setTotal(json.total);
              if (json.items.length < PAGE_SIZE) setHasMore(false);
            } else if (json && typeof json === 'object') {
              const possible = json.basho_ids || json.basho_list || json.all_basho || json.basho_index || json.items;
              if (Array.isArray(possible)) {
                newItems = possible.map((id: any) => ({ id: String(id) }));
                setHasMore(false);
              }
            }

            if (newItems.length > 0) {
              setItems(newItems);
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

    fetchOnce();

    return () => {
      mounted = false;
    };
  }, []);

  // subsequent pages
  useEffect(() => {
    if (page === 1) return;
    let mounted = true;
    const attempts = buildAttempts();

    const fetchPage = async () => {
      setLoading(true);
      try {
        for (const url of attempts) {
          try {
            const json = await tryFetchUrl(url, page, PAGE_SIZE);
            if (!mounted) return;

            let newItems: BashoItem[] = [];
            if (Array.isArray(json)) {
              newItems = json.map((id: any) => ({ id: String(id) }));
            } else if (json && Array.isArray(json.items)) {
              newItems = json.items.map((it: any) => ({ id: String(it.id), location: it.location ?? '', start_date: it.start_date ?? '', end_date: it.end_date ?? '' }));
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
      <div style={{ marginTop: '15rem', padding: '1rem', position: 'relative', zIndex: 1 }} className="content-box">
        <div style={{ width: '100%' }}>
        <h1 className="app-text" style={{ marginBottom: '1rem' }}>Basho — Index</h1>

        {loading && items.length === 0 && <div className="app-text">Loading...</div>}

        {!loading && items.length === 0 && <div className="app-text">No basho found.</div>}

        {items.length > 0 && (
          <div className={styles.container}>
            {items.map((it) => (
              <Link key={it.id} href={`/basho/${encodeURIComponent(String(it.id))}`} className={styles.item}>
                <div className={styles.title}>{it.location ? `${it.location}` : it.id}</div>
                <div className={styles.id}>{it.location ? `${it.start_date ?? ''} — ${it.end_date ?? ''}` : ''} <span style={{ opacity: 0.8 }}>({it.id})</span></div>
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
    </>
  );
}

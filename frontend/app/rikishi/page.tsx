"use client";

import React, { useEffect, useState } from 'react';
import Link from 'next/link';

type RikishiItem = { id: string; shikona?: string };
type ListState = {
  items: RikishiItem[];
  loading: boolean;
  error?: string | null;
};

export default function RikishiIndexPage() {
  const [state, setState] = useState<ListState>({ items: [], loading: true, error: null });

  useEffect(() => {
    let mounted = true;

    const tryFetch = async () => {
      const attempts = ['/rikishi', '/api/rikishi', '/api/homepage'];

      for (const url of attempts) {
        try {
          const res = await fetch(url, { credentials: 'include' });
          if (!mounted) return;
          if (!res.ok) continue;
          const data = await res.json();

          // If endpoint returns items array of objects
          if (data && Array.isArray(data.items)) {
            const items: RikishiItem[] = data.items.map((it: any) => ({ id: String(it.id), shikona: it.shikona ?? it.name ?? '' }));
            setState({ items, loading: false, error: null });
            return;
          }

          // If endpoint returns array of primitives
          if (Array.isArray(data)) {
            const items = data.map((id: any) => ({ id: String(id) }));
            setState({ items, loading: false, error: null });
            return;
          }

          // homepage document: try known keys
          if (url === '/api/homepage' && data && typeof data === 'object') {
            const possible = data.rikishi_ids || data.rikishi_list || data.all_rikishi || data.rikishi_index;
            if (Array.isArray(possible)) {
              const items = possible.map((id: any) => ({ id: String(id) }));
              setState({ items, loading: false, error: null });
              return;
            }
          }
        } catch (err) {
          // try next
        }
      }

      if (mounted) setState({ items: [], loading: false, error: 'No listing endpoint available. Backend does not expose a rikishi index.' });
    };

    tryFetch();
    return () => {
      mounted = false;
    };
  }, []);

  return (
    <div style={{ marginTop: '13rem', padding: '1rem' }} className="content-box">
      <div style={{ width: '100%' }}>
        <h1 className="app-text" style={{ marginBottom: '1rem' }}>Rikishi — Index</h1>

        {state.loading && <div className="app-text">Loading...</div>}

        {!state.loading && state.error && <div style={{ color: 'crimson' }} className="app-text">{state.error}</div>}

        {!state.loading && state.items && state.items.length > 0 && (
          <ul style={{ listStyle: 'none', padding: 0 }}>
            {state.items.map((it) => (
              <li key={it.id} style={{ margin: '0.4rem 0' }}>
                <Link href={`/rikishi/${encodeURIComponent(String(it.id))}`} className="navbar-link">{it.shikona && it.shikona.length > 0 ? `${it.shikona} (${it.id})` : it.id}</Link>
              </li>
            ))}
          </ul>
        )}

        {!state.loading && state.items.length === 0 && !state.error && <div className="app-text">No rikishi found.</div>}
      </div>
    </div>
  );
}

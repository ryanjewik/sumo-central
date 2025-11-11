"use client";

import React, { useEffect, useState } from 'react';
import Link from 'next/link';

type BashoItem = { id: string; location?: string; start_date?: string; end_date?: string };
type ListState = {
  items: BashoItem[];
  loading: boolean;
  error?: string | null;
};

export default function BashoIndexPage() {
  const [state, setState] = useState<ListState>({ items: [], loading: true, error: null });

  useEffect(() => {
    let mounted = true;

    const tryFetch = async () => {
      const attempts = ['/basho', '/api/basho', '/api/homepage'];

      for (const url of attempts) {
        try {
          const res = await fetch(url, { credentials: 'include' });
          if (!mounted) return;
          if (!res.ok) continue;
          const data = await res.json();

          if (data && Array.isArray(data.items)) {
            const items: BashoItem[] = data.items.map((it: any) => ({ id: String(it.id), location: it.location ?? '', start_date: it.start_date ?? '', end_date: it.end_date ?? '' }));
            setState({ items, loading: false, error: null });
            return;
          }

          if (Array.isArray(data)) {
            const items = data.map((id: any) => ({ id: String(id) }));
            setState({ items, loading: false, error: null });
            return;
          }

          if (url === '/api/homepage' && data && typeof data === 'object') {
            const possible = data.basho_ids || data.basho_list || data.all_basho || data.basho_index;
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

      if (mounted) setState({ items: [], loading: false, error: 'No listing endpoint available. Backend does not expose a basho index.' });
    };

    tryFetch();
    return () => {
      mounted = false;
    };
  }, []);

  return (
    <div style={{ marginTop: '13rem', padding: '1rem' }} className="content-box">
      <div style={{ width: '100%' }}>
        <h1 className="app-text" style={{ marginBottom: '1rem' }}>Basho — Index</h1>

        {state.loading && <div className="app-text">Loading...</div>}

        {!state.loading && state.error && <div style={{ color: 'crimson' }} className="app-text">{state.error}</div>}

        {!state.loading && state.items && state.items.length > 0 && (
          <ul style={{ listStyle: 'none', padding: 0 }}>
            {state.items.map((it) => (
              <li key={it.id} style={{ margin: '0.4rem 0' }}>
                <Link href={`/basho/${encodeURIComponent(String(it.id))}`} className="navbar-link">{it.location ? `${it.location} — ${it.start_date ?? ''} to ${it.end_date ?? ''} (${it.id})` : it.id}</Link>
              </li>
            ))}
          </ul>
        )}

        {!state.loading && state.items.length === 0 && !state.error && <div className="app-text">No basho found.</div>}
      </div>
    </div>
  );
}

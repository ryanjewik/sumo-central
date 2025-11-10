"use client";

import React, { useEffect, useState } from 'react';
import Link from 'next/link';

type ListState = {
  ids: string[];
  loading: boolean;
  error?: string | null;
};

export default function BashoIndexPage() {
  const [state, setState] = useState<ListState>({ ids: [], loading: true, error: null });

  useEffect(() => {
    let mounted = true;

    const tryFetch = async () => {
      const attempts = [
        '/api/basho_list',
        '/api/basho',
        '/api/homepage',
      ];

      for (const url of attempts) {
        try {
          const res = await fetch(url, { credentials: 'include' });
          if (!mounted) return;
          if (!res.ok) continue;
          const data = await res.json();

          if (url === '/api/homepage') {
            const possible = (data && (data.basho_ids || data.basho_list || data.all_basho || data.basho_index));
            if (possible && Array.isArray(possible)) {
              setState({ ids: possible.map(String), loading: false, error: null });
              return;
            }
            continue;
          }

          if (Array.isArray(data)) {
            setState({ ids: data.map(String), loading: false, error: null });
            return;
          }

          if (data && typeof data === 'object') {
            const ids = data.ids || data.list || data.basho_ids || data.basho_list;
            if (Array.isArray(ids)) {
              setState({ ids: ids.map(String), loading: false, error: null });
              return;
            }
          }
        } catch (err) {
          // try next
        }
      }

      if (mounted) setState({ ids: [], loading: false, error: 'No listing endpoint available. Backend does not expose a basho index.' });
    };

    tryFetch();
    return () => { mounted = false; };
  }, []);

  return (
    <div style={{ marginTop: '13rem', padding: '1rem' }} className="content-box">
      <div style={{ width: '100%' }}>
        <h1 className="app-text" style={{ marginBottom: '1rem' }}>Basho — Index</h1>

        {state.loading && <div className="app-text">Loading...</div>}

        {!state.loading && state.error && (
          <div style={{ color: 'crimson' }} className="app-text">{state.error}</div>
        )}

        {!state.loading && state.ids && state.ids.length > 0 && (
          <ul style={{ listStyle: 'none', padding: 0 }}>
            {state.ids.map((id) => (
              <li key={id} style={{ margin: '0.4rem 0' }}>
                <Link href={`/basho/${encodeURIComponent(String(id))}`} legacyBehavior>
                  <a className="navbar-link">{id}</a>
                </Link>
              </li>
            ))}
          </ul>
        )}

        {!state.loading && state.ids.length === 0 && !state.error && (
          <div className="app-text">No basho found.</div>
        )}
      </div>
    </div>
  );
}

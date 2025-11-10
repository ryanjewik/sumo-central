"use client";

import React, { useEffect, useState } from 'react';
import Link from 'next/link';

type ListState = {
  ids: string[];
  loading: boolean;
  error?: string | null;
};

export default function RikishiIndexPage() {
  const [state, setState] = useState<ListState>({ ids: [], loading: true, error: null });

  useEffect(() => {
    let mounted = true;

    const tryFetch = async () => {
      const attempts = [
        '/api/rikishi_list',
        '/api/rikishi',
        '/api/homepage',
      ];

      for (const url of attempts) {
        try {
          const res = await fetch(url, { credentials: 'include' });
          if (!mounted) return;
          if (!res.ok) continue;
          const data = await res.json();

          // If this is a homepage document, try to extract known keys
          if (url === '/api/homepage') {
            // check a few likely keys
            const possible = (data && (data.rikishi_ids || data.rikishi_list || data.all_rikishi || data.rikishi_index));
            if (possible && Array.isArray(possible)) {
              setState({ ids: possible.map(String), loading: false, error: null });
              return;
            }
            // sometimes homepage may not include full list
            continue;
          }

          // If the endpoint returns an array
          if (Array.isArray(data)) {
            setState({ ids: data.map(String), loading: false, error: null });
            return;
          }

          // If it's an object with an ids/list field
          if (data && typeof data === 'object') {
            const ids = data.ids || data.list || data.rikishi_ids || data.rikishi_list;
            if (Array.isArray(ids)) {
              setState({ ids: ids.map(String), loading: false, error: null });
              return;
            }
          }
        } catch (err) {
          // try next
        }
      }

      if (mounted) setState({ ids: [], loading: false, error: 'No listing endpoint available. Backend does not expose a rikishi index.' });
    };

    tryFetch();
    return () => { mounted = false; };
  }, []);

  return (
    <div style={{ marginTop: '13rem', padding: '1rem' }} className="content-box">
      <div style={{ width: '100%' }}>
        <h1 className="app-text" style={{ marginBottom: '1rem' }}>Rikishi — Index</h1>

        {state.loading && <div className="app-text">Loading...</div>}

        {!state.loading && state.error && (
          <div style={{ color: 'crimson' }} className="app-text">{state.error}</div>
        )}

        {!state.loading && state.ids && state.ids.length > 0 && (
          <ul style={{ listStyle: 'none', padding: 0 }}>
            {state.ids.map((id) => (
              <li key={id} style={{ margin: '0.4rem 0' }}>
                <Link href={`/rikishi/${encodeURIComponent(String(id))}`} legacyBehavior>
                  <a className="navbar-link">{id}</a>
                </Link>
              </li>
            ))}
          </ul>
        )}

        {!state.loading && state.ids.length === 0 && !state.error && (
          <div className="app-text">No rikishi found.</div>
        )}
      </div>
    </div>
  );
}

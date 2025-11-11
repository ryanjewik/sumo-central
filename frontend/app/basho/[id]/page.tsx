"use client";

import React, { useEffect, useState } from "react";
import { useParams } from "next/navigation";

export default function BashoDetailPage() {
  const params = useParams() as { id?: string };
  const id = params?.id ?? "";
  const [doc, setDoc] = useState<any | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!id) return;
    let mounted = true;
    const fetchDoc = async () => {
      try {
        const res = await fetch(`/api/basho/${encodeURIComponent(id)}`, { credentials: 'include' });
        if (!mounted) return;
        if (!res.ok) {
          const text = await res.text();
          setError(`Failed to load basho: ${res.status} ${text}`);
          setLoading(false);
          return;
        }
        const data = await res.json();
        setDoc(data);
      } catch (err: any) {
        setError(String(err?.message ?? err));
      } finally {
        setLoading(false);
      }
    };

    fetchDoc();
    return () => { mounted = false; };
  }, [id]);

  return (
    <div style={{ marginTop: '13rem', padding: '1rem' }} className="content-box">
      <h1 className="app-text">Basho — {id}</h1>

      {loading && <div className="app-text">Loading...</div>}
      {!loading && error && <div style={{ color: 'crimson' }}>{error}</div>}

      {!loading && !error && doc && (
        <div style={{ maxWidth: 900 }}>
          <h2 className="app-text">{doc.location ?? doc.title ?? `Basho ${id}`}</h2>
          {doc.start_date && <div className="app-text">Start: {String(doc.start_date)}</div>}
          {doc.end_date && <div className="app-text">End: {String(doc.end_date)}</div>}
          {doc.summary && <div style={{ marginTop: '1rem' }} className="app-text">{String(doc.summary)}</div>}

          <pre style={{ marginTop: '1.25rem', padding: '0.75rem', background: '#f5f5f5', borderRadius: 6, overflowX: 'auto' }}>
            {JSON.stringify(doc, null, 2)}
          </pre>
        </div>
      )}

      {!loading && !error && !doc && <div className="app-text">No basho page found.</div>}
    </div>
  );
}

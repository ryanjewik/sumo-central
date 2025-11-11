"use client";

import React, { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import { useParams } from "next/navigation";

export default function RikishiDetailPage() {
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
        const res = await fetch(`/api/rikishi/${encodeURIComponent(id)}`, { credentials: 'include' });
        if (!mounted) return;
        if (!res.ok) {
          const text = await res.text();
          setError(`Failed to load rikishi: ${res.status} ${text}`);
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
      <h1 className="app-text">Rikishi — {id}</h1>

      {loading && <div className="app-text">Loading...</div>}
      {!loading && error && <div style={{ color: 'crimson' }}>{error}</div>}

      {!loading && !error && doc && (
        <div style={{ maxWidth: 900 }}>
          {/* Try to render common fields if present */}
          <h2 className="app-text">{doc.shikona ?? doc.name ?? doc.title ?? `Rikishi ${id}`}</h2>
          {doc.rank && <div className="app-text">Rank: {String(doc.rank)}</div>}
          {doc.birthdate && <div className="app-text">Born: {String(doc.birthdate)}</div>}
          {doc.bio && <div style={{ marginTop: '1rem' }} className="app-text">{String(doc.bio)}</div>}

          {/* Fallback: pretty-print the full document */}
          <pre style={{ marginTop: '1.25rem', padding: '0.75rem', background: '#f5f5f5', borderRadius: 6, overflowX: 'auto' }}>
            {JSON.stringify(doc, null, 2)}
          </pre>
        </div>
      )}

      {!loading && !error && !doc && <div className="app-text">No rikishi page found.</div>}
    </div>
  );
}

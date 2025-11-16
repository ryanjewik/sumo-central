"use client";

import React, { useState } from 'react';
import * as authLib from '@/lib/auth';

export default function DiscussionSubmission() {
  const [title, setTitle] = useState('');
  const [body, setBody] = useState('');
  const [tags, setTags] = useState('');
  const [creating, setCreating] = useState(false);

  const submit = async (e?: React.FormEvent) => {
    e?.preventDefault();
    if (!title.trim() || !body.trim()) return;
    setCreating(true);
    try {
      const payload = { title: title.trim(), body: body.trim(), tags: tags.split(',').map(t => t.trim()).filter(Boolean) };
      // use fetchWithAuth so Authorization header is attached and refresh logic runs
      const res = await authLib.fetchWithAuth('/api/discussions', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      if (!res.ok) {
        const text = await res.text();
        throw new Error(text || `create failed: ${res.status}`);
      }
      const created = await res.json();
      // notify any listening client list to prepend
      try { document.dispatchEvent(new CustomEvent('discussion-created', { detail: created })); } catch {}
      setTitle(''); setBody(''); setTags('');
    } catch (err) {
      console.error(err);
      // user visible
      alert(`Failed to create post: ${err}`);
    } finally {
      setCreating(false);
    }
  };

  return (
    <section style={{ margin: '2rem 0', padding: '1rem', background: '#f7f2f8', borderRadius: 8 }}>
      <h2 style={{ marginTop: 0 }}>Create a discussion</h2>
      <form onSubmit={submit}>
        <div style={{ marginBottom: 8 }}>
          <input aria-label="title" placeholder="Title" value={title} onChange={e => setTitle(e.target.value)} style={{ width: '100%', padding: 8, borderRadius: 6, border: '1px solid #ddd' }} />
        </div>
        <div style={{ marginBottom: 8 }}>
          <textarea aria-label="body" placeholder="Write your post..." value={body} onChange={e => setBody(e.target.value)} rows={6} style={{ width: '100%', padding: 8, borderRadius: 6, border: '1px solid #ddd' }} />
        </div>
        <div style={{ marginBottom: 8 }}>
          <input aria-label="tags" placeholder="tags (comma separated)" value={tags} onChange={e => setTags(e.target.value)} style={{ width: '100%', padding: 8, borderRadius: 6, border: '1px solid #ddd' }} />
        </div>
        <div style={{ display: 'flex', gap: 8 }}>
          <button type="submit" disabled={creating} style={{ background: '#563861', color: 'white', padding: '0.5rem 1rem', borderRadius: 6, border: 'none' }}>{creating ? 'Posting...' : 'Post'}</button>
          <button type="button" onClick={() => { setTitle(''); setBody(''); setTags(''); }} style={{ padding: '0.5rem 1rem', borderRadius: 6 }}>Clear</button>
        </div>
      </form>
    </section>
  );
}

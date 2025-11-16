import React from 'react';
import ForumListClient from './ForumListClient';
import { ForumPost } from './ForumSection';

async function fetchInitial() : Promise<ForumPost[]> {
  try {
    const base = process.env.BACKEND_URL || process.env.NEXT_PUBLIC_BACKEND_URL || 'http://gin-backend:8080'
    const res = await fetch(`${base}/discussions?skip=0&limit=20`, { cache: 'no-store' });
    if (!res.ok) return [];
    const data = await res.json();
    if (!Array.isArray(data)) return [];
    // normalize
    return data.map((p: any) => ({
      id: p.id || (p._id && p._id.toString && p._id.toString()) || '',
      upvotes: p.upvotes ?? 0,
      downvotes: p.downvotes ?? 0,
      author: p.author_username ?? 'anonymous',
      date_created: p.created_at ?? new Date().toISOString(),
      title: p.title,
      body: p.body,
    }))
  } catch (err) {
    console.error(err)
    return []
  }
}

export default async function ForumListServer() {
  const initial = await fetchInitial();
  return (
    // render a client component that hydrates and continues loading on scroll
    <div>
      {/* initial posts are passed to client for hydration and further infinite scroll */}
      {/* eslint-disable-next-line react/jsx-no-undef */}
      <ForumListClient initial={initial} />
    </div>
  )
}

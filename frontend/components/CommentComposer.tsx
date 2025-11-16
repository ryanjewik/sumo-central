"use client"

import React, { useState } from 'react'
import { fetchWithAuth } from '@/lib/auth'

type Props = {
  discussionId: string
  parentId?: string | null
  onPosted?: (comment: any) => void
  placeholder?: string
}

export default function CommentComposer({ discussionId, parentId, onPosted, placeholder }: Props) {
  const [text, setText] = useState('')
  const [loading, setLoading] = useState(false)
  const disabled = loading || text.trim() === ''

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    if (disabled) return
    setLoading(true)
    try {
      const res = await fetchWithAuth(`/api/discussions/${encodeURIComponent(discussionId)}/comments`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ body: text, parent_id: parentId || '' }),
      })
      if (!res.ok) throw new Error('failed')
      const data = await res.json()
      setText('')
      onPosted && onPosted(data)
    } catch (err) {
      // simple error handling
      // eslint-disable-next-line no-console
      console.error('post comment error', err)
      alert('Failed to post comment. Are you signed in?')
    } finally {
      setLoading(false)
    }
  }

  return (
    <form onSubmit={handleSubmit} style={{ marginTop: 8 }}>
      <textarea value={text} onChange={e => setText(e.target.value)} placeholder={placeholder || 'Write a reply...'} rows={3} style={{ width: '100%', padding: 8, borderRadius: 6, background: '#fff' }} />
      <div style={{ marginTop: 6, display: 'flex', gap: 8, justifyContent: 'flex-end' }}>
        <button type="submit" disabled={disabled} style={{ padding: '8px 12px', borderRadius: 6, background: '#563861', color: '#fff', border: 'none' }}>{loading ? 'Posting...' : 'Post'}</button>
      </div>
    </form>
  )
}

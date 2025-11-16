"use client"

import React, { useState } from 'react'
import { fetchWithAuth } from '@/lib/auth'
import CommentComposer from './CommentComposer'

type Comment = any

type Props = {
  comment: Comment
  children?: Comment[]
  discussionId: string
  onReplyPosted?: (c: any) => void
}

export default function CommentItem({ comment, children = [], discussionId, onReplyPosted }: Props) {
  const [showReply, setShowReply] = useState(false)
  const [localUpvotes, setLocalUpvotes] = useState<number>(comment.upvotes || 0)
  const [localDownvotes, setLocalDownvotes] = useState<number>(comment.downvotes || 0)

  async function handleVote(direction: 'up' | 'down') {
    try {
      const res = await fetchWithAuth(`/api/discussions/${encodeURIComponent(discussionId)}/comments/${encodeURIComponent(comment.id)}/vote`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ direction }),
      })
      if (!res.ok) throw new Error('vote failed')
      if (direction === 'up') setLocalUpvotes(v => v + 1)
      else setLocalDownvotes(v => v + 1)
    } catch (err) {
      // eslint-disable-next-line no-console
      console.error('vote error', err)
      alert('Failed to vote. Are you signed in?')
    }
  }

  return (
    <div style={{ borderLeft: '2px solid #eee', paddingLeft: 12, marginTop: 12 }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12 }}>
        <div>
          <strong style={{ color: '#563861' }}>{comment.author_username || 'anonymous'}</strong>
          <div style={{ fontSize: 12, color: '#666' }}>{comment.created_at}</div>
        </div>
        <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
          <button onClick={() => handleVote('up')} style={{ background: 'transparent', border: 'none', cursor: 'pointer' }}>▲ {localUpvotes}</button>
          <button onClick={() => handleVote('down')} style={{ background: 'transparent', border: 'none', cursor: 'pointer' }}>▼ {localDownvotes}</button>
          <button onClick={() => setShowReply(s => !s)} style={{ background: 'transparent', border: 'none', cursor: 'pointer', color: '#563861' }}>Reply</button>
        </div>
      </div>
      <div style={{ marginTop: 8, whiteSpace: 'pre-wrap', color: '#333' }}>{comment.body}</div>

      {showReply && (
        <CommentComposer discussionId={discussionId} parentId={comment.id} onPosted={(c) => {
          setShowReply(false)
          onReplyPosted && onReplyPosted(c)
        }} placeholder={`Reply to ${comment.author_username || 'user'}`} />
      )}

      {children.length > 0 && (
        <div style={{ marginLeft: 8 }}>
          {children.map((ch: any) => (
            <CommentItem key={ch.id} comment={ch} children={ch.children || []} discussionId={discussionId} onReplyPosted={onReplyPosted} />
          ))}
        </div>
      )}
    </div>
  )
}

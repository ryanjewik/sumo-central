"use client"

import React, { useState } from 'react'
import ArrowUpwardIcon from '@mui/icons-material/ArrowUpward'
import ArrowDownwardIcon from '@mui/icons-material/ArrowDownward'
import { fetchWithAuth } from '@/lib/auth'
import CommentComposer from './CommentComposer'
import AuthorBadge from './AuthorBadge'

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
  const [localVote, setLocalVote] = useState<'up' | 'down' | null>(null)

  async function handleVote(direction: 'up' | 'down') {
    try {
      const res = await fetchWithAuth(`/api/discussions/${encodeURIComponent(discussionId)}/comments/${encodeURIComponent(comment.id)}/vote`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ direction }),
      })
      if (!res.ok) throw new Error('vote failed')

      // If backend returns updated counts/myVote use them
      let json: any = null
      try { json = await res.json() } catch (e) { json = null }
      if (json && (typeof json.upvotes === 'number' || typeof json.downvotes === 'number')) {
        if (typeof json.upvotes === 'number') setLocalUpvotes(json.upvotes)
        if (typeof json.downvotes === 'number') setLocalDownvotes(json.downvotes)
        if (json.myVote === 'up' || json.myVote === 'down' || json.myVote === null) setLocalVote(json.myVote)
        return
      }

      // Fallback: modify counts locally and ensure only one vote type is active
      if (direction === 'up') {
        if (localVote === 'up') {
          setLocalUpvotes(u => Math.max(0, u - 1))
          setLocalVote(null)
        } else if (localVote === 'down') {
          setLocalDownvotes(d => Math.max(0, d - 1))
          setLocalUpvotes(u => u + 1)
          setLocalVote('up')
        } else {
          setLocalUpvotes(u => u + 1)
          setLocalVote('up')
        }
      } else {
        if (localVote === 'down') {
          setLocalDownvotes(d => Math.max(0, d - 1))
          setLocalVote(null)
        } else if (localVote === 'up') {
          setLocalUpvotes(u => Math.max(0, u - 1))
          setLocalDownvotes(d => d + 1)
          setLocalVote('down')
        } else {
          setLocalDownvotes(d => d + 1)
          setLocalVote('down')
        }
      }
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
          <AuthorBadge userId={comment.author_id || comment.authorId} username={comment.author_username} />
          <div style={{ fontSize: 12, color: '#666' }}>{comment.created_at}</div>
        </div>
        <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
          <button onClick={() => handleVote('up')} style={{ background: 'transparent', border: 'none', cursor: 'pointer', color: '#388e3c', display: 'flex', alignItems: 'center', gap: 6 }}> <ArrowUpwardIcon fontSize="small" /> {localUpvotes}</button>
          <button onClick={() => handleVote('down')} style={{ background: 'transparent', border: 'none', cursor: 'pointer', color: '#d32f2f', display: 'flex', alignItems: 'center', gap: 6 }}> <ArrowDownwardIcon fontSize="small" /> {localDownvotes}</button>
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

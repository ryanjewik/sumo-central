"use client"

import React, { useEffect, useMemo, useState } from 'react'
import { fetchWithAuth } from '@/lib/auth'
import CommentComposer from './CommentComposer'
import CommentItem from './CommentItem'

type Comment = any

function buildTree(comments: Comment[]) {
  const map = new Map<string, any>()
  const roots: any[] = []
  for (const c of comments) {
    map.set(c.id, { ...c, children: [] })
  }
  for (const c of comments) {
    const node = map.get(c.id)
    if (c.parent_id) {
      const parent = map.get(c.parent_id)
      if (parent) parent.children.push(node)
      else roots.push(node) // orphaned parent -> treat as root
    } else {
      roots.push(node)
    }
  }
  return roots
}

export default function CommentsThreadClient({ discussionId, initialComments }: { discussionId: string, initialComments: Comment[] }) {
  const [comments, setComments] = useState<Comment[]>(initialComments || [])
  const [loading, setLoading] = useState(false)

  const tree = useMemo(() => buildTree(comments), [comments])

  async function handlePosted(comment: any) {
    // append to state
    setComments(c => [...c, comment])
  }

  async function refresh() {
    setLoading(true)
    try {
      const res = await fetchWithAuth(`/api/discussions/${encodeURIComponent(discussionId)}`)
      if (!res.ok) throw new Error('fetch failed')
      const data = await res.json()
      setComments(data.comments || [])
    } catch (err) {
      // ignore
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    // ensure initial comments are in state
    setComments(initialComments || [])
  }, [initialComments])

  return (
    <div style={{ marginTop: 18 }}>
      <h3 style={{ marginBottom: 8 }}>Discussion</h3>
      <CommentComposer discussionId={discussionId} onPosted={handlePosted} placeholder="Write a comment" />

      <div style={{ marginTop: 12 }}>
        {loading ? <div>Loading comments...</div> : (
          tree.length === 0 ? <div style={{ color: '#666' }}>No comments yet</div> : (
            tree.map((c: any) => (
              <CommentItem key={c.id} comment={c} children={c.children || []} discussionId={discussionId} onReplyPosted={handlePosted} />
            ))
          )
        )}
      </div>
    </div>
  )
}

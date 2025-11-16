"use client"

import React, { useEffect, useState } from 'react'
import { fetchWithAuth } from '@/lib/auth'
import ArrowUpwardIcon from '@mui/icons-material/ArrowUpward'
import ArrowDownwardIcon from '@mui/icons-material/ArrowDownward'

export default function PostVote({ discussionId, initialUp = 0, initialDown = 0 }: { discussionId: string, initialUp?: number, initialDown?: number }) {
  const [up, setUp] = useState<number>(initialUp)
  const [down, setDown] = useState<number>(initialDown)
  const [saving, setSaving] = useState(false)
  // localVote: 'up' | 'down' | null
  const [localVote, setLocalVote] = useState<'up' | 'down' | null>(null)

  // Try to fetch current user vote (optional). If backend doesn't support GET, this will fail silently.
  useEffect(() => {
    let mounted = true
    async function load() {
      try {
        const res = await fetchWithAuth(`/api/discussions/${encodeURIComponent(discussionId)}/vote`, { method: 'GET' })
        if (!mounted || !res.ok) return
        const data = await res.json()
        // Expecting shape: { upvotes, downvotes, myVote } where myVote is 'up'|'down'|null
        if (typeof data.upvotes === 'number') setUp(data.upvotes)
        if (typeof data.downvotes === 'number') setDown(data.downvotes)
        if (data.myVote === 'up' || data.myVote === 'down') setLocalVote(data.myVote)
      } catch (e) {
        // ignore; fallback to initial counts
      }
    }
    load()
    return () => { mounted = false }
  }, [discussionId])

  async function vote(type: 'up' | 'down') {
    if (saving) return
    setSaving(true)
    try {
      // backend expects { direction: 'up' | 'down' }
      const res = await fetchWithAuth(`/api/discussions/${encodeURIComponent(discussionId)}/vote`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ direction: type })
      })
      if (!res.ok) throw new Error('vote failed')

      // Try to parse returned counts and myVote
      let json: any = null
      try { json = await res.json() } catch (e) { json = null }
      if (json && (typeof json.upvotes === 'number' || typeof json.downvotes === 'number')) {
        if (typeof json.upvotes === 'number') setUp(json.upvotes)
        if (typeof json.downvotes === 'number') setDown(json.downvotes)
        if (json.myVote === 'up' || json.myVote === 'down' || json.myVote === null) setLocalVote(json.myVote)
      } else {
        // Fallback local adjustments: ensure only one vote type is active
        if (type === 'up') {
          if (localVote === 'up') {
            // unvote
            setUp(u => Math.max(0, u - 1))
            setLocalVote(null)
          } else if (localVote === 'down') {
            setDown(d => Math.max(0, d - 1))
            setUp(u => u + 1)
            setLocalVote('up')
          } else {
            setUp(u => u + 1)
            setLocalVote('up')
          }
        } else {
          if (localVote === 'down') {
            setDown(d => Math.max(0, d - 1))
            setLocalVote(null)
          } else if (localVote === 'up') {
            setUp(u => Math.max(0, u - 1))
            setDown(d => d + 1)
            setLocalVote('down')
          } else {
            setDown(d => d + 1)
            setLocalVote('down')
          }
        }
      }
    } catch (e) {
      // eslint-disable-next-line no-console
      console.error('post vote error', e)
      alert('Failed to submit vote. Are you signed in?')
    } finally {
      setSaving(false)
    }
  }

  return (
    <div style={{ display: 'flex', gap: 12, alignItems: 'center' }}>
      <button aria-pressed={localVote === 'up'} onClick={() => vote('up')} disabled={saving} style={{ color: '#388e3c', fontWeight: '700', background: 'transparent', border: 'none', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: 6 }}>
        <ArrowUpwardIcon fontSize="small" />{up}
      </button>
      <button aria-pressed={localVote === 'down'} onClick={() => vote('down')} disabled={saving} style={{ color: '#d32f2f', fontWeight: '700', background: 'transparent', border: 'none', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: 6 }}>
        <ArrowDownwardIcon fontSize="small" />{down}
      </button>
    </div>
  )
}

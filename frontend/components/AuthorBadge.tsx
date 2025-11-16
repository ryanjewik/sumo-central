"use client"

import React, { useEffect, useState } from 'react'
import { fetchWithAuth } from '@/lib/auth'

type Props = { userId?: string | number | null, username?: string }

export default function AuthorBadge({ userId, username }: Props) {
  const [profile, setProfile] = useState<any | null>(null)

  useEffect(() => {
    let mounted = true
    async function load() {
      if (!userId) return
      try {
        const res = await fetchWithAuth(`/api/users/${encodeURIComponent(String(userId))}`)
        if (!mounted) return
        if (!res.ok) return
        const data = await res.json()
        setProfile(data)
      } catch (e) {
        // ignore
      }
    }
    load()
    return () => { mounted = false }
  }, [userId])

  if (!profile) {
    // If no profile loaded but we have a username, show that; else nothing
    return username ? <span style={{ fontWeight: 700 }}>{username}</span> : null
  }

  const fav = profile.favorite_rikishi
  const favLabel = fav && typeof fav === 'object' ? (fav.shikona || fav.name || String(fav.id)) : (fav || '')
  const country = profile.country ? String(profile.country) : null

  return (
    <span style={{ display: 'inline-flex', gap: 8, alignItems: 'center' }}>
      <strong style={{ fontWeight: 700 }}>{profile.username ?? username ?? profile.id}</strong>
      {favLabel ? <span style={{ fontSize: '0.85rem', color: '#555' }}>• {favLabel}</span> : null}
      {country ? <span style={{ fontSize: '0.85rem', color: '#777' }}>({country})</span> : null}
    </span>
  )
}

import React from 'react'

// Minimal placeholder for a discussion thread page (replacement for forum thread)
export default function DiscussionThreadPlaceholder({ params }: { params: { id: string } }) {
  return (
    <div style={{ padding: 24 }}>
      <h2 className="app-text">Discussion {params?.id ?? ''}</h2>
      <p className="app-text">Placeholder discussion thread page generated to satisfy build-time type resolution.</p>
    </div>
  )
}

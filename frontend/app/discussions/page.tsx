import React from 'react'
import dynamic from 'next/dynamic'
import ForumListServer from '../../components/ForumListServer'
import DiscussionSubmission from '../../components/DiscussionSubmission'

export const metadata = {
  title: 'Discussions - Sumopedia',
};

// DiscussionSubmission is a client component so import normally.
// ForumListServer is a server component - keep it as default import.

export default function DiscussionsPage() {
  return (
    <>
      <div id="background"></div>
      <main className="page-offset" style={{ position: 'relative', zIndex: 1 }}>
        <div style={{ maxWidth: 1100, margin: '0 auto', padding: '0 1rem' }}>
          <DiscussionSubmission />
          <ForumListServer />
        </div>
      </main>
    </>
  );
}

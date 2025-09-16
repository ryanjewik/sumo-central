import React from "react";
import ArrowUpwardIcon from '@mui/icons-material/ArrowUpward';
import ArrowDownwardIcon from '@mui/icons-material/ArrowDownward';

export interface ForumPost {
  id: number;
  upvotes: number;
  downvotes: number;
  author: string;
  date_created: string;
  title: string;
  body: string;
}

interface ForumSectionProps {
  posts: ForumPost[];
}

const ForumSection: React.FC<ForumSectionProps> = ({ posts }) => (
  <div className="forum-area">
    <h2 style={{ fontSize: '1.5rem', fontWeight: 'bold', marginBottom: '1.5rem', color: '#563861' }}>Trending Forum Discussions</h2>
    {posts.map(post => (
      <a
        key={post.id}
        href={"/forum/" + post.id}
        className="forum-post"
        style={{
          display: 'block',
          marginBottom: '1rem',
          padding: '1rem',
          background: '#E4E0BE',
          borderRadius: '8px',
          boxShadow: '0 2px 4px rgba(0,0,0,0.05)',
          textDecoration: 'none',
          color: 'inherit',
          transition: 'box-shadow 0.15s, transform 0.15s',
        }}
        onClick={e => {
          // Prevent default for now if you want to handle navigation in React Router
          // e.preventDefault();
        }}
        onMouseOver={e => {
          (e.currentTarget as HTMLElement).style.boxShadow = '0 4px 16px rgba(86,56,97,0.15)';
          (e.currentTarget as HTMLElement).style.transform = 'scale(1.02)';
        }}
        onMouseOut={e => {
          (e.currentTarget as HTMLElement).style.boxShadow = '0 2px 4px rgba(0,0,0,0.05)';
          (e.currentTarget as HTMLElement).style.transform = 'none';
        }}
      >
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
          <div style={{ flex: 1 }}>
            <div style={{ fontWeight: 'bold', fontSize: '1.2rem', textAlign: 'left' }}>{post.title}</div>
            <div style={{ fontSize: '0.95rem', color: '#555', marginBottom: '0.5rem', textAlign: 'left' }}>By {post.author}</div>
          </div>
          <div style={{ display: 'flex', gap: '1rem', alignItems: 'center' }}>
            <span style={{ color: '#388e3c', fontWeight: 'bold', display: 'flex', alignItems: 'center' }}>
              <ArrowUpwardIcon fontSize="small" style={{ marginRight: '0.25rem' }} />{post.upvotes}
            </span>
            <span style={{ color: '#d32f2f', fontWeight: 'bold', display: 'flex', alignItems: 'center' }}>
              <ArrowDownwardIcon fontSize="small" style={{ marginRight: '0.25rem' }} />{post.downvotes}
            </span>
          </div>
        </div>
        <div style={{ marginBottom: '0.5rem', color: '#563861', fontSize: '1rem', textAlign: 'left' }}>{post.body}</div>
        <div style={{ fontSize: '0.9rem', color: '#555', textAlign: 'right' }}>
          {post.date_created}
        </div>
      </a>
    ))}
  </div>
);

export default ForumSection;
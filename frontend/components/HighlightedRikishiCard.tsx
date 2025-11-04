"use client";

import React from "react";

interface HighlightedRikishiCardProps {
  rikishi?: any;
}

const HighlightedRikishiCard: React.FC<HighlightedRikishiCardProps> = ({ rikishi }) => {
  const r = rikishi ?? {
    shikona: 'Kotonowaka',
    current_rank: 'Komusubi',
    age: 26,
    current_height: 188,
    current_weight: 163,
    heya: 'Sadogatake',
    shusshin: 'Chiba',
    wins: 51,
    losses: 44,
    yusho_count: 1,
    sansho_count: 2,
    // possible image keys used by backend
    image_url: undefined,
    profile_image: undefined,
  };

  // normalize names used across docs
  const name = r.shikona ?? r.name ?? 'Unknown';
  const rank = r.current_rank ?? r.rank ?? '';
  const age = r.age ?? r.current_age ?? '';
  const height = r.current_height ?? r.height ?? '';
  const weight = r.current_weight ?? r.weight ?? '';
  const heya = r.heya ?? r.heya_name ?? '';
  const shusshin = r.shusshin ?? r.shusshin_place ?? '';
  const wins = r.wins ?? r.win_count ?? r.wins_count ?? 0;
  const losses = r.losses ?? r.losses_count ?? 0;

  // profile image: try common keys then fallback to placeholder
  const imageSrc =
  (r.image_url as string) || (r.profile_image as string) || (r.photo as string) || (r.image as string) || (r.imageUrl as string) || '/sumo_logo.png';

  const totalMatches = Number(wins) + Number(losses);
  const winRate = totalMatches > 0 ? Math.round((Number(wins) / totalMatches) * 100) : null;

  return (
    <div
      style={{
        background: '#F5E6C8',
        border: '4px solid #563861',
        borderRadius: '1rem',
        boxShadow: '0 2px 8px rgba(0,0,0,0.08)',
        marginBottom: '1.2rem',
        padding: '1.2rem 1rem',
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        gap: '0.8rem',
        width: '100%',
        fontFamily: `'Courier New', Courier, monospace`,
      }}
    >
      <span
        style={{
          display: 'inline-block',
          fontWeight: 800,
          fontSize: '1.05rem',
          color: '#fff',
          background: 'linear-gradient(90deg,#563861 0%, #7b4a86 100%)',
          borderRadius: '0.5rem',
          padding: '0.18rem 0.7rem',
          letterSpacing: '0.05em',
          marginBottom: '0.5rem',
          alignSelf: 'center',
        }}
      >
        Highlighted Rikishi
      </span>
      <div style={{ display: 'flex', flexDirection: 'row', alignItems: 'center', justifyContent: 'center', width: '100%', gap: '1.2rem' }}>
        <img
          src={imageSrc}
          alt={`${name} profile`}
          onError={(e) => { (e.target as HTMLImageElement).src = '/sumo_logo.png'; }}
          style={{
            width: 170,
            height: 250,
            borderRadius: 18,
            border: '3px solid rgba(224,163,194,0.9)',
            background: '#fff',
            objectFit: 'cover',
            boxShadow: '0 6px 24px rgba(0,0,0,0.12)',
          }}
        />
        <div style={{ textAlign: 'center', minWidth: 200, fontFamily: `'Courier New', Courier, monospace` }}>
          <div style={{ display: 'flex', justifyContent: 'center', gap: 8, alignItems: 'center' }}>
            <div style={{ fontWeight: 800, fontSize: '1.35rem', color: '#563861' }}>{name}</div>
            {rank && (
              <div style={{ background: '#ffd54f', color: '#3b2f1b', padding: '4px 8px', borderRadius: 8, fontWeight: 700, fontSize: '0.95rem' }} aria-label="rank-badge">{rank}</div>
            )}
          </div>
          <div style={{ display: 'flex', gap: 12, justifyContent: 'center', marginTop: 6 }}>
            {age !== '' && <div style={{ fontSize: '0.95rem', color: '#563861' }}>Age: <strong>{age}</strong></div>}
            {height !== '' && <div style={{ fontSize: '0.95rem', color: '#563861' }}>{height} cm</div>}
            {weight !== '' && <div style={{ fontSize: '0.95rem', color: '#563861' }}>{weight} kg</div>}
          </div>
          <div style={{ fontSize: '0.95rem', color: '#563861', marginTop: 6 }}>Heya: <strong>{heya}</strong></div>
          <div style={{ fontSize: '0.95rem', color: '#563861' }}>Shusshin: <strong>{shusshin}</strong></div>
          <div style={{ display: 'flex', gap: 10, justifyContent: 'center', marginTop: 8 }}>
            <div style={{ fontSize: '0.95rem', color: '#388eec' }}><strong>W</strong>: {wins}</div>
            <div style={{ fontSize: '0.95rem', color: '#d32f2f' }}><strong>L</strong>: {losses}</div>
            {winRate !== null && <div style={{ fontSize: '0.95rem', color: '#6b7280' }}>Win%: <strong>{winRate}%</strong></div>}
          </div>
        </div>
      </div>
      {/* Yusho and Special Prizes row below stats */}
      <div style={{ width: '100%', marginTop: 10, display: 'flex', flexDirection: 'row', gap: 18, justifyContent: 'flex-start' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <span role="img" aria-label="Trophy" style={{ fontSize: 22, color: '#f59e0b' }}>🏆</span>
          <div style={{ fontSize: '0.95rem', color: '#563861' }}><strong>Yusho</strong> {r.yusho_count ?? r.yusho ?? 0}</div>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <span role="img" aria-label="Star" style={{ fontSize: 20, color: '#e0a3c2' }}>⭐</span>
          <div style={{ fontSize: '0.95rem', color: '#563861' }}><strong>Sansho</strong> {r.sansho_count ?? r.special_prizes ?? 0}</div>
        </div>
      </div>
    </div>
  );
};

export default HighlightedRikishiCard;
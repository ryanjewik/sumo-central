"use client";

import React from "react";
import Image from 'next/image';

type RikishiInfo = {
  shikona?: string;
  name?: string;
  current_rank?: string;
  rank?: string;
  age?: number | string;
  current_age?: number | string;
  current_height?: number | string;
  height?: number | string;
  current_weight?: number | string;
  weight?: number | string;
  heya?: string;
  heya_name?: string;
  shusshin?: string;
  shusshin_place?: string;
  wins?: number | string;
  losses?: number | string;
  yusho_count?: number;
  sansho_count?: number;
  s3_url?: string;
  pfp_url?: string;
  image_url?: string;
  profile_image?: string;
  photo?: string;
  image?: string;
  imageUrl?: string;
  [k: string]: unknown;
};

interface HighlightedRikishiCardProps {
  rikishi?: RikishiInfo;
}

const HighlightedRikishiCard: React.FC<HighlightedRikishiCardProps> = ({ rikishi }) => {
  const r: RikishiInfo = rikishi ?? {
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

  // normalize names used across docs and coerce to safe string/number types
  const name = String(r.shikona ?? r.name ?? 'Unknown');
  const rank = String(r.current_rank ?? r.rank ?? '');
  const age = (r.age ?? r.current_age ?? '') as string | number;
  const height = (r.current_height ?? r.height ?? '') as string | number;
  const weight = (r.current_weight ?? r.weight ?? '') as string | number;
  const heya = String(r.heya ?? r.heya_name ?? '');
  const shusshin = String(r.shusshin ?? r.shusshin_place ?? '');
  const wins = Number(r.wins ?? (r as Record<string, unknown>)['win_count'] ?? (r as Record<string, unknown>)['wins_count'] ?? 0);
  const losses = Number(r.losses ?? (r as Record<string, unknown>)['losses_count'] ?? 0);

  // profile image: prefer S3-hosted webp if present, then common keys, then placeholder
  const imageSrc = String(r.s3_url ?? r.pfp_url ?? r.image_url ?? r.profile_image ?? r.photo ?? r.image ?? r.imageUrl ?? '/sumo_logo.png');

  const totalMatches = Number(wins) + Number(losses);
  const winRate = totalMatches > 0 ? Math.round((Number(wins) / totalMatches) * 100) : null;

  const yushoCount = Number(r.yusho_count ?? r.yusho ?? 0);
  const sanshoCount = Number(r.sansho_count ?? r.special_prizes ?? 0);

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
        <div style={{ width: 170, height: 250, position: 'relative', borderRadius: 18, overflow: 'hidden', border: '3px solid rgba(224,163,194,0.9)' }}>
          <Image
            src={imageSrc}
            alt={`${name} profile`}
            fill
            style={{ objectFit: 'cover' }}
            sizes="(max-width: 480px) 120px, 170px"
            priority={false}
          />
        </div>
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
          <div style={{ fontSize: '0.95rem', color: '#563861' }}><strong>Yusho</strong> {yushoCount}</div>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <span role="img" aria-label="Star" style={{ fontSize: 20, color: '#e0a3c2' }}>⭐</span>
          <div style={{ fontSize: '0.95rem', color: '#563861' }}><strong>Sansho</strong> {sanshoCount}</div>
        </div>
      </div>
    </div>
  );
};

export default HighlightedRikishiCard;
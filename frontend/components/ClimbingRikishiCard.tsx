"use client";

import React from "react";
import RikishiWinLossSparkline from '../components/sparkline';

interface ClimbingRikishiCardProps {
  rikishi?: any;
}

const ClimbingRikishiCard: React.FC<ClimbingRikishiCardProps> = ({ rikishi }) => {
  const r = rikishi ?? { shikona: 'Kotonowaka', current_rank: 'Komusubi' };
  const name = r.shikona ?? r.name ?? 'Unknown';
  const rank = r.current_rank ?? r.rank ?? '';
  // try to extract a recent form series (array of 0/1) from common keys
  const series = (r.recent_form ?? r.win_series ?? r.trend ?? r.recent_matches ?? r.form) as any;
  let sparkData: number[] | undefined = undefined;
  try {
    if (Array.isArray(series) && series.length > 0) {
      // if array of objects, map known keys
      if (typeof series[0] === 'object') {
        sparkData = (series as any[]).map(s => Number(s.win ?? s.won ?? s.result ?? s.value ?? 0));
      } else {
        sparkData = (series as any[]).map((v: any) => Number(v) || 0);
      }
    }
  } catch (e) {
    sparkData = undefined;
  }

  // compute a simple recent pace metric (wins in last 5)
  let paceLabel: string | null = null;
  if (Array.isArray(sparkData) && sparkData.length > 0) {
    const lastN = sparkData.slice(-5);
    const wins = lastN.reduce((a, b) => a + (Number(b) || 0), 0);
    const pct = Math.round((wins / lastN.length) * 100);
    paceLabel = `${wins}/${lastN.length} wins (${pct}%)`;
  }

  return (
  <div
    className="climbing-rikishi-card"
    style={{
      background: '#F5E6C8',
      borderRadius: '1rem',
      boxShadow: '0 2px 8px rgba(0,0,0,0.08)',
      border: '2px solid #563861',
      padding: '1.2rem 1rem',
      minWidth: 170,
      maxWidth: 220,
      width: '100%',
      display: 'flex',
      flexDirection: 'column',
      alignItems: 'center',
      gap: '1.5rem',
      height: '100%',
      flex: 1,
      transition: 'box-shadow 0.18s',
      cursor: 'pointer',
    }}
    onMouseOver={e => {
      (e.currentTarget as HTMLElement).style.boxShadow = '0 16px 48px 0 rgba(86,56,97,0.32), 0 2px 16px 0 rgba(224,163,194,0.18)';
    }}
    onMouseOut={e => {
      (e.currentTarget as HTMLElement).style.boxShadow = '0 2px 8px rgba(0,0,0,0.08)';
    }}
  >
    <div style={{ width: '100%', marginBottom: '0.5rem' }}>
      <span
        style={{
          display: 'inline-block',
          fontWeight: 'bold',
          fontSize: '1.1rem',
          color: '#fff',
          background: '#563861',
          borderRadius: '0.5rem',
          padding: '0.25rem 1rem',
          letterSpacing: '0.05em',
          margin: '0 auto',
        }}
      >
        Climbing Rikishi
      </span>
    </div>
      <div
        style={{
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          gap: '1.2rem',
          width: '100%',
          justifyContent: 'center',
          flex: 1,
        }}
      >
      <div
        style={{
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          gap: '0.4rem',
          minWidth: 90,
        }}
      >
        {/* profile image: prefer backend-provided keys */}
        <img
          src={r.image_url ?? r.profile_image ?? r.photo ?? '/sumo_logo.png'}
          alt={`${name} profile`}
          onError={(e) => { (e.target as HTMLImageElement).src = '/sumo_logo.png'; }}
          style={{
            width: 70,
            height: 70,
            borderRadius: '50%',
            border: '3px solid #388eec',
            background: '#fff',
            objectFit: 'cover',
          }}
        />
        <div style={{ fontWeight: 800, fontSize: '1.05rem', color: '#563861', textAlign: 'center' }}>{name}</div>
        <div style={{ fontSize: '0.95rem', color: '#388eec', textAlign: 'center' }}>{rank}</div>
        {/* Rikishi Stats removed */}
      </div>
      <div
        style={{
          minWidth: 120,
          maxWidth: 180,
          flex: 1,
          display: 'flex',
          justifyContent: 'center',
          alignItems: 'center',
        }}
      >
        <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
          <RikishiWinLossSparkline data={sparkData} title={undefined} />
          {paceLabel && <div style={{ fontSize: 12, color: '#563861', marginTop: 6 }}>Recent: <strong>{paceLabel}</strong></div>}
        </div>
      </div>
    </div>
  </div>
  );
};

export default ClimbingRikishiCard;
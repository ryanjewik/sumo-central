"use client";

import React from "react";
import Image from 'next/image';

type MatchShape = {
  id?: string | number;
  basho_id?: string | number;
  east_rikishi_id?: string | number;
  west_rikishi_id?: string | number;
  east_shikona?: string;
  west_shikona?: string;
  eastshikona?: string;
  westshikona?: string;
  east?: Record<string, unknown>;
  west?: Record<string, unknown>;
  east_votes?: number | string;
  west_votes?: number | string;
  winner?: string | number | null;
  kimarite?: string;
  [k: string]: unknown;
};

interface HighlightedMatchCardProps {
  match?: MatchShape;
  onOpenLogin?: () => void;
  // When false, voting UI will be hidden (e.g. when using a fallback highlighted match)
  allowVoting?: boolean;
}

import { useAuth } from '../context/AuthContext';

const HighlightedMatchCard: React.FC<HighlightedMatchCardProps> = ({ match, onOpenLogin, allowVoting = true }) => {
  const { user } = useAuth();
  // homepage.highlighted_match shape (example from DB):
  // { id, basho_id, east_rikishi_id, west_rikishi_id, east_rank, west_rank, eastshikona, westshikona, winner, kimarite, day, match_number, division }
  const m: MatchShape = match ?? {};
  const eastName = String(m.east_shikona ?? m.eastshikona ?? (m.east && (m.east as Record<string, unknown>)['shikona']) ?? 'East');
  const westName = String(m.west_shikona ?? m.westshikona ?? (m.west && (m.west as Record<string, unknown>)['shikona']) ?? 'West');
  // base ranks from top-level payload (fallback)
  const eastRank = String(m.east_rank ?? (m as Record<string, unknown>)['eastRank'] ?? (m.east && (m.east as Record<string, unknown>)['rank']) ?? '');
  const westRank = String(m.west_rank ?? (m as Record<string, unknown>)['westRank'] ?? (m.west && (m.west as Record<string, unknown>)['rank']) ?? '');
  const eastVotes = Number(m.east_votes ?? m.eastVotes ?? m.east_votes_count ?? 0) || 0;
  const westVotes = Number(m.west_votes ?? m.westVotes ?? m.west_votes_count ?? 0) || 0;
  const totalVotes = Math.max(1, eastVotes + westVotes);
  const eastPercent = Math.round((eastVotes / totalVotes) * 100);
  const westPercent = 100 - eastPercent;
  const kimarite = String(m.kimarite ?? (m as Record<string, unknown>)['kimarite_name'] ?? (m as Record<string, unknown>)['kimarite_display'] ?? '');
  const winner = m.winner ?? m.result ?? null; // winner might be rikishi id or 'east'/'west'
  const winnerSide = (winner === 'east' || winner === 'west') ? winner : (winner === m.east_rikishi_id || winner === m.east_rikishi_id?.toString() ? 'east' : (winner === m.west_rikishi_id || winner === m.west_rikishi_id?.toString() ? 'west' : null));
  // local cache for rikishi images (s3_url preferred)
  // Server should provide nested rikishi objects (east_rikishi / west_rikishi) when available.
  const eastId = String(m.east_rikishi_id ?? m.eastId ?? '');
  const westId = String(m.west_rikishi_id ?? m.westId ?? '');
  const eastImage = String(m.east_image ?? '');
  const westImage = String(m.west_image ?? '');
  const nestedWest = (m as Record<string, unknown>)['west_rikishi'] as Record<string, unknown> | undefined;
  const nestedEast = (m as Record<string, unknown>)['east_rikishi'] as Record<string, unknown> | undefined;
  // prefer nested rikishi's current_rank when available
  const displayWestRank = nestedWest ? String(nestedWest['current_rank'] ?? nestedWest['rank'] ?? westRank) : westRank;
  const displayEastRank = nestedEast ? String(nestedEast['current_rank'] ?? nestedEast['rank'] ?? eastRank) : eastRank;
  const westImg = String(m.west_image ?? (m as Record<string, unknown>)['west_image_url'] ?? (m as Record<string, unknown>)['west_photo'] ?? (nestedWest ? String(nestedWest['s3_url'] ?? nestedWest['image_url'] ?? nestedWest['pfp_url']) : null) ?? '/sumo_logo.png');
  const eastImg = String(m.east_image ?? (m as Record<string, unknown>)['east_image_url'] ?? (m as Record<string, unknown>)['east_photo'] ?? (nestedEast ? String(nestedEast['s3_url'] ?? nestedEast['image_url'] ?? nestedEast['pfp_url']) : null) ?? '/sumo_logo.png');

  // AI prediction raw value (0/1 or name). Use top-level keys if present.
  const aiRaw = (m as Record<string, unknown>)['ai_prediction'] ?? (m as Record<string, unknown>)['AI_prediction'] ?? (m as Record<string, unknown>)['aiPrediction'];
  let aiPred: number | string | undefined;
  if (typeof aiRaw === 'number') aiPred = aiRaw;
  else if (typeof aiRaw === 'string') {
    const n = Number(aiRaw);
    if (!Number.isNaN(n)) aiPred = n; else aiPred = aiRaw;
  }

  // normalize ai prediction to a side when possible so we can color the pill
  let aiPredSide: 'west' | 'east' | null = null;
  if (typeof aiPred === 'number') {
    aiPredSide = Number(aiPred) === 1 ? 'west' : 'east';
  } else if (typeof aiPred === 'string') {
    const s = aiPred.toLowerCase().trim();
    if (s === westName.toLowerCase()) aiPredSide = 'west';
    else if (s === eastName.toLowerCase()) aiPredSide = 'east';
  }
  const aiNameBg = aiPredSide === 'west' ? '#e0709f' : aiPredSide === 'east' ? '#3ccf9a' : '#fff';
  const aiNameColor = aiPredSide ? '#fff' : '#563861';

  const westDetails: Record<string, any> | null = nestedWest ? {
    height: nestedWest['current_height'] ?? nestedWest['height'] ?? nestedWest['currentHeight'] ?? null,
    weight: nestedWest['current_weight'] ?? nestedWest['weight'] ?? nestedWest['currentWeight'] ?? null,
    wins: nestedWest['wins'] ?? nestedWest['win_count'] ?? null,
    losses: nestedWest['losses'] ?? nestedWest['loss_count'] ?? null,
    yusho: nestedWest['yusho_count'] ?? nestedWest['yusho'] ?? 0,
    sansho: nestedWest['sansho_count'] ?? nestedWest['sansho'] ?? 0,
    birthdate: nestedWest['birthdate'] ?? nestedWest['birth_date'] ?? null,
    heya: nestedWest['heya'] ?? nestedWest['stable'] ?? null,
  } : null;

  const eastDetails: Record<string, any> | null = nestedEast ? {
    height: nestedEast['current_height'] ?? nestedEast['height'] ?? nestedEast['currentHeight'] ?? null,
    weight: nestedEast['current_weight'] ?? nestedEast['weight'] ?? nestedEast['currentWeight'] ?? null,
    wins: nestedEast['wins'] ?? nestedEast['win_count'] ?? null,
    losses: nestedEast['losses'] ?? nestedEast['loss_count'] ?? null,
    yusho: nestedEast['yusho_count'] ?? nestedEast['yusho'] ?? 0,
    sansho: nestedEast['sansho_count'] ?? nestedEast['sansho'] ?? 0,
    birthdate: nestedEast['birthdate'] ?? nestedEast['birth_date'] ?? null,
    heya: nestedEast['heya'] ?? nestedEast['stable'] ?? null,
  } : null;

  const calcAge = (birthdate?: string | null) => {
    if (!birthdate) return undefined;
    const d = new Date(String(birthdate));
    if (isNaN(d.getTime())) return undefined;
    const now = new Date();
    let age = now.getFullYear() - d.getFullYear();
    const mth = now.getMonth() - d.getMonth();
    if (mth < 0 || (mth === 0 && now.getDate() < d.getDate())) age--;
    return age;
  };

  // local voting state for highlighted match
  const [localVotes, setLocalVotes] = React.useState<{ west: number; east: number }>({ west: westVotes, east: eastVotes });
  const [localUserVote, setLocalUserVote] = React.useState<'west' | 'east' | undefined>(undefined);

  const handleVote = (side: 'west' | 'east') => {
    if (!user) return; // user must be signed in; page will show sign-in button otherwise
    setLocalVotes(prev => {
      const next = { ...prev };
      // if user already voted same side, do nothing
      if (localUserVote === side) return prev;
      if (localUserVote) {
        next[localUserVote] = Math.max(0, next[localUserVote] - 1);
      }
      next[side] = (next[side] ?? 0) + 1;
      return next;
    });
    setLocalUserVote(side);
  };

  return (
    <div className="highlighted-match-area" style={{
      background: '#A3E0B8',
      borderRadius: '1rem',
      boxShadow: '0 2px 8px rgba(0,0,0,0.08)',
      padding: '2rem',
      marginBottom: '2rem',
      border: '4px solid #563861',
      display: 'flex',
      flexDirection: 'column',
      alignItems: 'center',
      minWidth: 320,
      width: '100%',
      maxWidth: '100%',
    }}>
      <h2 style={{ fontSize: '1.6rem', fontWeight: 800, color: '#563861', marginBottom: '1rem' }}>Highlighted Match</h2>

  {/* Scoped styles for responsive behavior (uses container queries when available) */}
  <style>{`
        .hme-row { display: flex; }
        /* allow this component's container to be queried */
        .highlighted-match-area { container-type: inline-size; }

        /* If the parent/container becomes narrow, stack the columns to avoid clipping */
        @container (max-width: 700px) {
          /* Stack columns and allow children to stretch full width in narrow containers */
          .hme-row { flex-direction: column; align-items: stretch; gap: 12px; max-width: 100%; }
          .hme-row > div { width: 100%; min-width: 0; margin-left: 0; margin-right: 0; padding: 0 6px; box-sizing: border-box; }
          .rikishi-card { align-items: stretch; }
          .hme-vs { margin: 12px 0 !important; }
          /* Ensure order: west, vs, east when stacked */
          .hme-row > div:nth-child(1) { order: 1; }
          .hme-row > .hme-vs { order: 2; }
          .hme-row > div:nth-child(3) { order: 3; }
          /* Make AI prediction name break to next line on narrow containers */
          .ai-pill { display: inline-flex; flex-wrap: wrap; }
          .ai-pill .ai-name { flex-basis: 100%; margin-top: 6px; }
          /* Reduce pfp and tighten stat grid when stacked to prevent overflow */
          .pfp { width: 130px !important; height: 180px !important; }
          .stat-grid { width: 180px !important; font-size: 15px !important; }
        }

        /* Fallback for older browsers that don't support container queries: use viewport width */
        @media (max-width: 900px) {
          /* Fallback stacking behavior for older browsers */
          .hme-row { flex-direction: column; align-items: stretch; gap: 12px; max-width: 100%; }
          .hme-row > div { width: 100%; min-width: 0; margin-left: 0; margin-right: 0; padding: 0 6px; box-sizing: border-box; }
          .rikishi-card { align-items: stretch; }
          .hme-vs { margin: 12px 0 !important; }
          .hme-row > div:nth-child(1) { order: 1; }
          .hme-row > .hme-vs { order: 2; }
          .hme-row > div:nth-child(3) { order: 3; }
          .ai-pill { display: inline-flex; flex-wrap: wrap; }
          .ai-pill .ai-name { flex-basis: 100%; margin-top: 6px; }
          .pfp { width: 130px !important; height: 180px !important; }
          .stat-grid { width: 180px !important; font-size: 15px !important; }
          /* reduce padding and gap for per-rikishi cards on narrow containers */
          .rikishi-card { padding: 10px !important; }
          .rikishi-card .pfp { margin-right: 16px !important; }
        }
  /* Desktop: ensure a consistent horizontal gap between pfp and stats inside rikishi cards */
  .rikishi-card .pfp { margin-right: clamp(6px, 1vw, 14px); }
      `}</style>

      {/* Inner card wrapper to give a card-like appearance */}
  <div className="inner-card" style={{ width: '100%', boxSizing: 'border-box', background: 'rgba(255,255,255,0.35)', padding: '0.75rem', borderRadius: 12, boxShadow: '0 6px 18px rgba(0,0,0,0.06)', border: '1px solid rgba(86,56,97,0.06)' }}>

  {/* Row 1: AI prediction centered (names stay with each rikishi block below) */}
  <div style={{ width: '100%', display: 'flex', justifyContent: 'center', marginBottom: 8 }}>
    {typeof aiPred !== 'undefined' && (
      <div className="ai-pill" style={{ padding: '8px 14px', borderRadius: 10, background: 'rgba(86,56,97,0.06)', color: '#563861', fontWeight: 700, fontSize: 14, border: '1px solid rgba(86,56,97,0.08)', display: 'inline-flex', alignItems: 'center', flexWrap: 'wrap', margin: '0 auto', textAlign: 'center', justifyContent: 'center' }}>
        <span className="ai-label" style={{ paddingRight: 8, fontWeight: 700 }}>AI prediction:</span>
        <span className="ai-name" style={{ marginLeft: 6, background: aiNameBg, color: aiNameColor, borderRadius: 8, padding: '6px 12px', fontWeight: 900, display: 'inline-block', textAlign: 'center' }}>{typeof aiPred === 'number' ? (Number(aiPred) === 1 ? westName : eastName) : String(aiPred)}</span>
      </div>
    )}
  </div>

  {/* Row 2: images + VS + compact stats (VS vertically centered with pfps and stats) */}
  <div className="hme-row" style={{ display: 'flex', gap: 'clamp(8px, 2vw, 16px)', alignItems: 'flex-start', width: '100%', justifyContent: 'space-between', maxWidth: 980 }}>
        {/* Left: West - image + compact info row below (name shown above so it stays with the block) */}
  <div style={{ minWidth: 220, display: 'flex', flexDirection: 'column', alignItems: 'stretch', flex: '1 1 280px', marginRight: 8 }}>
          <div className="rikishi-card" style={{ width: '100%', boxSizing: 'border-box', borderRadius: 12, padding: 28, background: 'rgba(245,230,200,0.90)', display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
            <div style={{ textAlign: 'center', marginBottom: 8 }}>
              <div style={{ fontWeight: 900, color: '#1e293b', fontSize: 22 }}>{westName}</div>
              <div style={{ fontSize: 15, color: '#475569', fontWeight: 800 }}>{displayWestRank}</div>
              {westDetails && westDetails.heya && (
                <div style={{ fontSize: 14, color: '#6b6b6b', marginTop: 6 }}>{String(westDetails.heya)}</div>
              )}
            </div>
              <div style={{ display: 'flex', gap: 'clamp(12px, 2.5vw, 28px)', alignItems: 'center', marginTop: 4 }}>
              <div className="pfp" style={{ position: 'relative', width: 170, height: 230, flex: '0 0 auto', zIndex: 1, overflow: 'hidden', borderRadius: 10, marginRight: 0 }} aria-hidden>
                <Image src={westImg} alt={`${westName} profile`} width={680} height={920} style={{ width: '100%', height: '100%', objectFit: 'cover', objectPosition: 'top', display: 'block' }} quality={90} />
                <div style={{ position: 'absolute', inset: 0, pointerEvents: 'none', borderRadius: 10, border: winnerSide === 'west' ? '3px solid #2563eb' : '1px solid rgba(37,99,235,0.7)', boxShadow: winnerSide === 'west' ? '0 8px 28px rgba(37,99,235,0.22)' : '0 3px 8px rgba(37,99,235,0.08)'}} />
                {winnerSide === 'west' && (
                  <div style={{ position: 'absolute', top: -6, right: -6, background: '#10b981', color: '#fff', width: 18, height: 18, borderRadius: '50%', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 10, fontWeight: 800 }}>W</div>
                )}
              </div>

              <div style={{ textAlign: 'left', marginTop: 2, zIndex: 2 }}>
                {/* ultra-compact rikishi details */}
                {westDetails && (
                  <div className="stat-grid" style={{ marginTop: 6, color: '#333', fontSize: 17, display: 'grid', gridTemplateColumns: '1fr auto', gap: 4, columnGap: 6, alignItems: 'center', width: 'auto' }}>
                    <div style={{ textAlign: 'left', color: '#7a7a7a' }}>Height</div>
                    <div style={{ textAlign: 'right' }}>{westDetails.height ? `${westDetails.height}cm` : '—'}</div>
                    <div style={{ textAlign: 'left', color: '#7a7a7a' }}>Weight</div>
                    <div style={{ textAlign: 'right' }}>{westDetails.weight ? `${westDetails.weight}kg` : '—'}</div>
                    <div style={{ textAlign: 'left', color: '#7a7a7a' }}>Age</div>
                    <div style={{ textAlign: 'right' }}>{typeof calcAge(westDetails?.birthdate) === 'number' ? `${calcAge(westDetails?.birthdate)}y` : '—'}</div>
                    <div style={{ textAlign: 'left', color: '#7a7a7a' }}>Record</div>
                    <div style={{ textAlign: 'right' }}>{(westDetails.wins ?? '—') + '-' + (westDetails.losses ?? '—')}</div>
                    <div style={{ textAlign: 'left', color: '#7a7a7a' }}>Yusho</div>
                    <div style={{ textAlign: 'right' }}>{westDetails.yusho ?? 0}</div>
                    <div style={{ textAlign: 'left', color: '#7a7a7a' }}>Sansho</div>
                    <div style={{ textAlign: 'right' }}>{westDetails.sansho ?? 0}</div>
                  </div>
                )}
                {/* heya shown above rank now; keep info grid here */}
              </div>
            </div>
          </div>
        </div>

        {/* Center: VS + Kimarite (VS vertically centered with pfps/stats) */}
  <div className="hme-vs" style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', position: 'relative', margin: '0 6px', gap: 8, flexShrink: 0, alignSelf: 'center' }}>
          {/* VS rendered inline so it participates in normal flow and will be vertically centered with the rikishi columns */}
          <div style={{ fontWeight: 900, fontSize: 18, color: '#563861', background: '#A3E0B8', padding: '6px 12px', borderRadius: 12, boxShadow: '0 2px 6px rgba(0,0,0,0.06)' }}>VS</div>
          {kimarite && (
            <div style={{ background: 'linear-gradient(90deg,#fde68a,#fbcfe8)', padding: '6px 12px', borderRadius: 20, color: '#6b2147', fontWeight: 800, fontSize: 13 }}>{kimarite}</div>
          )}
        </div>

        {/* Right: East - image + compact info row below (name shown above so it stays with the block) */}
  <div style={{ minWidth: 220, display: 'flex', flexDirection: 'column', alignItems: 'stretch', flex: '1 1 280px', marginLeft: 8 }}>
  <div className="rikishi-card" style={{ width: '100%', boxSizing: 'border-box', borderRadius: 12, padding: 28, background: 'rgba(245,230,200,0.86)', display: 'flex', flexDirection: 'column', alignItems: 'stretch' }}>
            <div style={{ textAlign: 'center', marginBottom: 8 }}>
              <div style={{ fontWeight: 900, color: '#1e293b', fontSize: 22 }}>{eastName}</div>
              <div style={{ fontSize: 15, color: '#475569', fontWeight: 800 }}>{displayEastRank}</div>
              {eastDetails && eastDetails.heya && (
                <div style={{ fontSize: 14, color: '#6b6b6b', marginTop: 6 }}>{String(eastDetails.heya)}</div>
              )}
            </div>
            <div style={{ display: 'flex', gap: 'clamp(12px, 2.5vw, 28px)', alignItems: 'center', marginTop: 4 }}>
              <div className="pfp" style={{ position: 'relative', width: 170, height: 230, flex: '0 0 auto', zIndex: 1, overflow: 'hidden', borderRadius: 10, marginRight: 0 }} aria-hidden>
                <Image src={eastImg} alt={`${eastName} profile`} width={680} height={920} style={{ width: '100%', height: '100%', objectFit: 'cover', objectPosition: 'top', display: 'block' }} quality={90} />
                <div style={{ position: 'absolute', inset: 0, pointerEvents: 'none', borderRadius: 10, border: winnerSide === 'east' ? '3px solid #e11d48' : '1px solid rgba(225,29,72,0.7)', boxShadow: winnerSide === 'east' ? '0 8px 28px rgba(225,29,72,0.22)' : '0 3px 8px rgba(225,29,72,0.08)'}} />
                {winnerSide === 'east' && (
                  <div style={{ position: 'absolute', top: -6, left: -6, background: '#10b981', color: '#fff', width: 18, height: 18, borderRadius: '50%', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 10, fontWeight: 800 }}>W</div>
                )}
              </div>

              <div style={{ textAlign: 'left', marginTop: 2 }}>
                {eastDetails && (
                  <div className="stat-grid" style={{ marginTop: 6, color: '#333', fontSize: 17, display: 'grid', gridTemplateColumns: '1fr auto', gap: 4, columnGap: 6, alignItems: 'center', width: 'auto' }}>
                    <div style={{ textAlign: 'left', color: '#7a7a7a' }}>Height</div>
                    <div style={{ textAlign: 'right' }}>{eastDetails.height ? `${eastDetails.height}cm` : '—'}</div>
                    <div style={{ textAlign: 'left', color: '#7a7a7a' }}>Weight</div>
                    <div style={{ textAlign: 'right' }}>{eastDetails.weight ? `${eastDetails.weight}kg` : '—'}</div>
                    <div style={{ textAlign: 'left', color: '#7a7a7a' }}>Age</div>
                    <div style={{ textAlign: 'right' }}>{typeof calcAge(eastDetails?.birthdate) === 'number' ? `${calcAge(eastDetails?.birthdate)}y` : '—'}</div>
                    <div style={{ textAlign: 'left', color: '#7a7a7a' }}>Record</div>
                    <div style={{ textAlign: 'right' }}>{(eastDetails.wins ?? '—') + '-' + (eastDetails.losses ?? '—')}</div>
                    <div style={{ textAlign: 'left', color: '#7a7a7a' }}>Yusho</div>
                    <div style={{ textAlign: 'right' }}>{eastDetails.yusho ?? 0}</div>
                    <div style={{ textAlign: 'left', color: '#7a7a7a' }}>Sansho</div>
                    <div style={{ textAlign: 'right' }}>{eastDetails.sansho ?? 0}</div>
                  </div>
                )}
                {/* heya shown above rank now */}
              </div>
            </div>
          </div>
        </div>
      </div>
      {/* close inner card */}
      </div>
      {/* Voting buttons for highlighted match. Hidden when allowVoting is false (fallback mode). */}
      {allowVoting && (
        <div style={{ marginTop: 18, display: 'flex', gap: 20, alignItems: 'center', justifyContent: 'center', width: '100%' }}>
          {user ? (
            <>
              <button
                style={{ background: localUserVote === 'west' ? '#e0709f' : '#e0709f', color: '#fff', border: localUserVote === 'west' ? '2px solid #c45e8b' : '2px solid #c45e8b', borderRadius: 8, width: 150, height: 44, display: 'flex', alignItems: 'center', justifyContent: 'center', boxSizing: 'border-box', whiteSpace: 'nowrap', fontWeight: 700, cursor: 'pointer', fontSize: 14 }}
                onClick={() => handleVote('west')}
              >
                {localUserVote === 'west' ? '✔ Voted' : 'Vote West'}
              </button>

              <div style={{ fontWeight: 700, color: '#444', minWidth: 80, textAlign: 'center' }}>{localVotes.west + localVotes.east} votes</div>

              <button
                style={{ background: localUserVote === 'east' ? '#3ccf9a' : '#3ccf9a', color: '#fff', border: localUserVote === 'east' ? '2px solid #2aa97a' : '2px solid #2aa97a', borderRadius: 8, width: 150, height: 44, display: 'flex', alignItems: 'center', justifyContent: 'center', boxSizing: 'border-box', whiteSpace: 'nowrap', fontWeight: 700, cursor: 'pointer', fontSize: 14 }}
                onClick={() => handleVote('east')}
              >
                {localUserVote === 'east' ? '✔ Voted' : 'Vote East'}
              </button>
            </>
          ) : (
            <button
              style={{ background: '#563861', color: '#fff', border: '2px solid #563861', borderRadius: 8, padding: '10px 16px', fontWeight: 700, cursor: 'pointer' }}
              onClick={() => { if (onOpenLogin) onOpenLogin(); else try { (window as any).location.href = '/'; } catch {} }}
            >
              Sign in to vote
            </button>
          )}
        </div>
      )}

      {/* Progress Bar */}
          <div style={{ width: '100%', marginTop: '2rem', display: 'flex', justifyContent: 'center' }}>
        <div style={{ width: '80%', maxWidth: 600 }}>
          <div style={{ display: 'flex', alignItems: 'center', width: '100%' }}>
            <span style={{ fontWeight: 'bold', color: '#388e3c', minWidth: 40, textAlign: 'right', marginRight: 8 }}>{westPercent}%</span>
            <div style={{ flex: 1, height: 18, background: '#F5E6C8', borderRadius: '0.75rem', position: 'relative', display: 'flex', overflow: 'hidden' }}>
              <div style={{ background: '#388eec', width: `${westPercent}%`, height: '100%', borderRadius: '0.75rem 0 0 0.75rem' }}></div>
              <div style={{ background: '#d32f2f', width: `${eastPercent}%`, height: '100%', borderRadius: '0 0.75rem 0.75rem 0' }}></div>
            </div>
            <span style={{ fontWeight: 'bold', color: '#d32f2f', minWidth: 40, textAlign: 'left', marginLeft: 8 }}>{eastPercent}%</span>
          </div>
        </div>
      </div>

      {kimarite && (
        <div style={{ marginTop: 12, color: '#563861', fontWeight: 600 }}>
          Winning Technique: <span style={{ color: '#388eec' }}>{kimarite}</span>
        </div>
      )}
    </div>
  );
};

export default HighlightedMatchCard;
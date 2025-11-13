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
  const westImg = String(m.west_image ?? (m as Record<string, unknown>)['west_image_url'] ?? (m as Record<string, unknown>)['west_photo'] ?? (nestedWest ? String(nestedWest['s3_url'] ?? nestedWest['image_url'] ?? nestedWest['pfp_url']) : null) ?? '/sumo_logo.png');
  const eastImg = String(m.east_image ?? (m as Record<string, unknown>)['east_image_url'] ?? (m as Record<string, unknown>)['east_photo'] ?? (nestedEast ? String(nestedEast['s3_url'] ?? nestedEast['image_url'] ?? nestedEast['pfp_url']) : null) ?? '/sumo_logo.png');

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

      <div style={{ display: 'flex', gap: 18, alignItems: 'center', width: '100%', flexWrap: 'wrap', justifyContent: 'center' }}>
        {/* Left: West */}
        <div style={{ minWidth: 180, display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
          <div style={{ position: 'relative', width: 120, height: 160 }} aria-hidden>
              <Image src={westImg} alt={`${westName} profile`} fill style={{ objectFit: 'cover', borderRadius: 12, objectPosition: 'top' }} sizes="120px" />
              <div style={{ position: 'absolute', inset: 0, pointerEvents: 'none', borderRadius: 12, border: winnerSide === 'west' ? '3px solid #2563eb' : '2px solid rgba(37,99,235,0.9)', boxShadow: winnerSide === 'west' ? '0 8px 28px rgba(37,99,235,0.28)' : '0 4px 12px rgba(37,99,235,0.12)'}} />
            {winnerSide === 'west' && (
              <div style={{ position: 'absolute', top: -8, right: -8, background: '#10b981', color: '#fff', width: 20, height: 20, borderRadius: '50%', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 11, fontWeight: 800 }}>W</div>
            )}
          </div>
          <div style={{ textAlign: 'center', marginTop: 8 }}>
            <div style={{ fontWeight: 800, color: '#1e293b' }}>{westName}</div>
            <div style={{ fontSize: 13, color: '#475569' }}>{westRank}</div>
            <div style={{ marginTop: 6, fontWeight: 700, color: '#1e40af' }}>{westVotes} votes</div>
            {/* show rikishi details when available */}
            {westDetails && (
              <div style={{ marginTop: 8, color: '#333', fontSize: 12, display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 6, alignItems: 'center', width: 200 }}>
                <div style={{ textAlign: 'left' }}>Height</div>
                <div style={{ textAlign: 'right' }}>{westDetails.height ? `${westDetails.height} cm` : '—'}</div>
                <div style={{ textAlign: 'left' }}>Weight</div>
                <div style={{ textAlign: 'right' }}>{westDetails.weight ? `${westDetails.weight} kg` : '—'}</div>
                <div style={{ textAlign: 'left' }}>Age</div>
                <div style={{ textAlign: 'right' }}>{typeof calcAge(westDetails?.birthdate) === 'number' ? `${calcAge(westDetails?.birthdate)}y` : '—'}</div>
                <div style={{ textAlign: 'left' }}>Record</div>
                <div style={{ textAlign: 'right' }}>{(westDetails.wins ?? '—') + ' - ' + (westDetails.losses ?? '—')}</div>
                <div style={{ textAlign: 'left' }}>Yusho</div>
                <div style={{ textAlign: 'right' }}>{westDetails.yusho ?? 0}</div>
                <div style={{ textAlign: 'left' }}>Sansho</div>
                <div style={{ textAlign: 'right' }}>{westDetails.sansho ?? 0}</div>
                <div style={{ gridColumn: '1 / span 2', textAlign: 'center', marginTop: 6, color: '#475569' }}>Heya: {westDetails.heya ?? '—'}</div>
              </div>
            )}
          </div>
        </div>

        {/* Center: VS + Kimarite */}
        <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 8 }}>
          <div style={{ fontWeight: 900, fontSize: 18, color: '#563861' }}>VS</div>
          {kimarite && (
            <div style={{ background: 'linear-gradient(90deg,#fde68a,#fbcfe8)', padding: '6px 12px', borderRadius: 20, color: '#6b2147', fontWeight: 800, fontSize: 13 }}>{kimarite}</div>
          )}
        </div>

        {/* Right: East */}
        <div style={{ minWidth: 180, display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
          <div style={{ position: 'relative', width: 120, height: 160 }} aria-hidden>
              <Image src={eastImg} alt={`${eastName} profile`} fill style={{ objectFit: 'cover', borderRadius: 12, objectPosition: 'top' }} sizes="120px" />
              <div style={{ position: 'absolute', inset: 0, pointerEvents: 'none', borderRadius: 12, border: '2px solid #ef4444', boxShadow: winnerSide === 'east' ? '0 6px 20px rgba(239,68,68,0.12)' : 'none' }} />
            {winnerSide === 'east' && (
              <div style={{ position: 'absolute', top: -8, right: -8, background: '#10b981', color: '#fff', width: 20, height: 20, borderRadius: '50%', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 11, fontWeight: 800 }}>W</div>
            )}
          </div>
          <div style={{ textAlign: 'center', marginTop: 8 }}>
            <div style={{ fontWeight: 800, color: '#1e293b' }}>{eastName}</div>
            <div style={{ fontSize: 13, color: '#475569' }}>{eastRank}</div>
            <div style={{ marginTop: 6, fontWeight: 700, color: '#dc2626' }}>{eastVotes} votes</div>
            {/* show rikishi details when available */}
            {eastDetails && (
              <div style={{ marginTop: 8, color: '#333', fontSize: 12, display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 6, alignItems: 'center', width: 200 }}>
                <div style={{ textAlign: 'left' }}>Height</div>
                <div style={{ textAlign: 'right' }}>{eastDetails.height ? `${eastDetails.height} cm` : '—'}</div>
                <div style={{ textAlign: 'left' }}>Weight</div>
                <div style={{ textAlign: 'right' }}>{eastDetails.weight ? `${eastDetails.weight} kg` : '—'}</div>
                <div style={{ textAlign: 'left' }}>Age</div>
                <div style={{ textAlign: 'right' }}>{typeof calcAge(eastDetails?.birthdate) === 'number' ? `${calcAge(eastDetails?.birthdate)}y` : '—'}</div>
                <div style={{ textAlign: 'left' }}>Record</div>
                <div style={{ textAlign: 'right' }}>{(eastDetails.wins ?? '—') + ' - ' + (eastDetails.losses ?? '—')}</div>
                <div style={{ textAlign: 'left' }}>Yusho</div>
                <div style={{ textAlign: 'right' }}>{eastDetails.yusho ?? 0}</div>
                <div style={{ textAlign: 'left' }}>Sansho</div>
                <div style={{ textAlign: 'right' }}>{eastDetails.sansho ?? 0}</div>
                <div style={{ gridColumn: '1 / span 2', textAlign: 'center', marginTop: 6, color: '#475569' }}>Heya: {eastDetails.heya ?? '—'}</div>
              </div>
            )}
          </div>
        </div>
      </div>
      {/* Voting buttons for highlighted match. Hidden when allowVoting is false (fallback mode). */}
      {allowVoting && (
        <div style={{ marginTop: 16, display: 'flex', gap: 12, alignItems: 'center', justifyContent: 'center', width: '100%' }}>
          {user ? (
            <>
              <button
                style={{ background: localUserVote === 'west' ? '#e0709f' : '#e0709f', color: '#fff', border: localUserVote === 'west' ? '2px solid #c45e8b' : '2px solid #c45e8b', borderRadius: 8, padding: '8px 14px', fontWeight: 700, cursor: 'pointer' }}
                onClick={() => handleVote('west')}
              >
                {localUserVote === 'west' ? '✔ Voted' : 'Vote West'}
              </button>

              <div style={{ fontWeight: 700, color: '#444' }}>{localVotes.west + localVotes.east} votes</div>

              <button
                style={{ background: localUserVote === 'east' ? '#3ccf9a' : '#3ccf9a', color: '#fff', border: localUserVote === 'east' ? '2px solid #2aa97a' : '2px solid #2aa97a', borderRadius: 8, padding: '8px 14px', fontWeight: 700, cursor: 'pointer' }}
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
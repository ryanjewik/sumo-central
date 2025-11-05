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
}

const HighlightedMatchCard: React.FC<HighlightedMatchCardProps> = ({ match }) => {
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
  const [rikishiImages, setRikishiImages] = React.useState<Record<string,string>>({});
  const eastId = String(m.east_rikishi_id ?? m.eastId ?? '');
  const westId = String(m.west_rikishi_id ?? m.westId ?? '');
  const eastImage = String(m.east_image ?? '');
  const westImage = String(m.west_image ?? '');
  const serializedRikishiImages = React.useMemo(() => JSON.stringify(rikishiImages), [rikishiImages]);

  React.useEffect(() => {
    let mounted = true;
    try {
      const cached: Record<string, string> = JSON.parse(serializedRikishiImages || '{}');
      const toFetch: string[] = [];
      if (westId && !westImage && !cached[String(westId)]) toFetch.push(String(westId));
      if (eastId && !eastImage && !cached[String(eastId)]) toFetch.push(String(eastId));
      if (toFetch.length === 0) return () => { mounted = false };
      (async () => {
        const out: Record<string,string> = {};
        await Promise.all(toFetch.map(async (id) => {
          try {
            const res = await fetch(`/api/rikishi/${encodeURIComponent(id)}`);
            if (!mounted) return;
            if (!res.ok) return;
            const doc = await res.json();
            const s = doc?.rikishi?.s3_url ?? doc?.s3_url ?? doc?.rikishi?.pfp_url ?? doc?.pfp_url ?? doc?.rikishi?.image_url ?? doc?.image_url ?? null;
            if (s) out[id] = s;
          } catch {
            // ignore fetch error
          }
        }));
        if (!mounted) return;
        if (Object.keys(out).length > 0) setRikishiImages(prev => ({ ...prev, ...out }));
      })();
    } catch {
      // parsing failed; bail
    }
    return () => { mounted = false };
  // include simple scalar deps to satisfy hooks linting
  // prefer serializedRikishiImages instead of the object reference rikishiImages
  }, [eastId, westId, eastImage, westImage, serializedRikishiImages]);

  const westImg = String(m.west_image ?? (m as Record<string, unknown>)['west_image_url'] ?? (m as Record<string, unknown>)['west_photo'] ?? rikishiImages[String(m.west_rikishi_id)] ?? '/sumo_logo.png');
  const eastImg = String(m.east_image ?? (m as Record<string, unknown>)['east_image_url'] ?? (m as Record<string, unknown>)['east_photo'] ?? rikishiImages[String(m.east_rikishi_id)] ?? '/sumo_logo.png');

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
              <Image src={westImg} alt={`${westName} profile`} fill style={{ objectFit: 'cover', borderRadius: 12 }} sizes="120px" />
              <div style={{ position: 'absolute', inset: 0, pointerEvents: 'none', borderRadius: 12, border: winnerSide === 'west' ? '3px solid #2563eb' : '2px solid rgba(37,99,235,0.9)', boxShadow: winnerSide === 'west' ? '0 8px 28px rgba(37,99,235,0.28)' : '0 4px 12px rgba(37,99,235,0.12)'}} />
            {winnerSide === 'west' && (
              <div style={{ position: 'absolute', top: -8, right: -8, background: '#10b981', color: '#fff', width: 20, height: 20, borderRadius: '50%', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 11, fontWeight: 800 }}>W</div>
            )}
          </div>
          <div style={{ textAlign: 'center', marginTop: 8 }}>
            <div style={{ fontWeight: 800, color: '#1e293b' }}>{westName}</div>
            <div style={{ fontSize: 13, color: '#475569' }}>{westRank}</div>
            <div style={{ marginTop: 6, fontWeight: 700, color: '#1e40af' }}>{westVotes} votes</div>
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
              <Image src={eastImg} alt={`${eastName} profile`} fill style={{ objectFit: 'cover', borderRadius: 12 }} sizes="120px" />
              <div style={{ position: 'absolute', inset: 0, pointerEvents: 'none', borderRadius: 12, border: winnerSide === 'east' ? '3px solid #ef4444' : '2px solid rgba(0,0,0,0.06)', boxShadow: winnerSide === 'east' ? '0 6px 20px rgba(239,68,68,0.12)' : 'none' }} />
            {winnerSide === 'east' && (
              <div style={{ position: 'absolute', top: -8, right: -8, background: '#10b981', color: '#fff', width: 20, height: 20, borderRadius: '50%', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 11, fontWeight: 800 }}>W</div>
            )}
          </div>
          <div style={{ textAlign: 'center', marginTop: 8 }}>
            <div style={{ fontWeight: 800, color: '#1e293b' }}>{eastName}</div>
            <div style={{ fontSize: 13, color: '#475569' }}>{eastRank}</div>
            <div style={{ marginTop: 6, fontWeight: 700, color: '#dc2626' }}>{eastVotes} votes</div>
          </div>
        </div>
      </div>

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
"use client";

import React from "react";
import Image from 'next/image';
import Link from 'next/link';

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
import { fetchWithAuth } from '../lib/auth';

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
  const kimarite = String(m.kimarite ?? (m as Record<string, unknown>)['kimarite_name'] ?? (m as Record<string, unknown>)['kimarite_display'] ?? '');
  const winner = m.winner ?? m.result ?? null; // winner might be rikishi id or 'east'/'west'
  const winnerSide = (winner === 'east' || winner === 'west') ? winner : (winner === m.east_rikishi_id || winner === m.east_rikishi_id?.toString() ? 'east' : (winner === m.west_rikishi_id || winner === m.west_rikishi_id?.toString() ? 'west' : null));
  // local cache for rikishi images (s3_url preferred)
  // Server should provide nested rikishi objects (east_rikishi / west_rikishi) when available.
  
  const eastImage = String(m.east_image ?? '');
  const westImage = String(m.west_image ?? '');
  const nestedWest = (m as Record<string, unknown>)['west_rikishi'] as Record<string, unknown> | undefined;
  const nestedEast = (m as Record<string, unknown>)['east_rikishi'] as Record<string, unknown> | undefined;
  // prefer nested rikishi's current_rank when available
  const displayWestRank = nestedWest ? String(nestedWest['current_rank'] ?? nestedWest['rank'] ?? westRank) : westRank;
  const displayEastRank = nestedEast ? String(nestedEast['current_rank'] ?? nestedEast['rank'] ?? eastRank) : eastRank;
  // helper to pick the first usable image string or fall back to the bundled placeholder
  const chooseImage = (...cands: any[]) => {
    for (const c of cands) {
      if (c === null || typeof c === 'undefined') continue;
      const s = String(c).trim();
      if (!s) continue;
      const lower = s.toLowerCase();
      if (lower === 'undefined' || lower === 'null') continue;
      return s;
    }
    return '/sumo_logo.png';
  };

  const westImg = chooseImage(
    m.west_image,
    (m as Record<string, unknown>)['west_image_url'],
    (m as Record<string, unknown>)['west_photo'],
    nestedWest && (nestedWest['s3_url'] ?? nestedWest['image_url'] ?? nestedWest['pfp_url']),
  );

  const eastImg = chooseImage(
    m.east_image,
    (m as Record<string, unknown>)['east_image_url'],
    (m as Record<string, unknown>)['east_photo'],
    nestedEast && (nestedEast['s3_url'] ?? nestedEast['image_url'] ?? nestedEast['pfp_url']),
  );

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

  // voting state: seeded from backend and updated via websocket/pubsub
  // derive explicit id string from common possible fields (matches upcoming_matches_list.tsx behavior)
  function getExplicitId(obj: any): string | undefined {
    const keys = ['id', 'matchId'];
    for (const k of keys) {
      const v = obj && (obj as any)[k];
      if (typeof v === 'number') return String(v);
      if (typeof v === 'string' && v.trim() !== '') return v;
    }
    return undefined;
  }
  const matchIdStr = String(getExplicitId(m) ?? '');
  // If a canonical string id isn't provided by backend, construct it from components
  // Rule: concat basho_id + day + match_number + east_rikishi_id + west_rikishi_id and cast to string
  // helper to robustly read possible id field names
  function getIdString(obj: any, ...keys: string[]): string | undefined {
    if (!obj) return undefined;
    for (const k of keys) {
      const v = (obj as any)[k];
      if (typeof v === 'number') return String(v);
      if (typeof v === 'string' && v.trim() !== '') return v;
    }
    return undefined;
  }

  const buildCanonicalMatchId = (mm: MatchShape) => {
    // Build a strict composite canonical id only from explicit components.
    // Do NOT accept a short numeric `id` (like "1") as canonical — many payloads
    // use that field for local match numbering rather than the composite key.
    const basho = (mm as any).basho_id ?? (mm as any).bashoId ?? (mm as any).basho ?? (mm as any).tournament_id;
    const day = (mm as any).day ?? (mm as any).Day ?? (mm as any).match_day;
    const matchNo = (mm as any).match_number ?? (mm as any).matchNo ?? (mm as any).matchNumber ?? (mm as any).num;
    const east = getIdString(mm, 'east_rikishi_id', 'eastId', 'east_id');
    const west = getIdString(mm, 'west_rikishi_id', 'westId', 'west_id');

    // Build strict canonical id only when all core components are present.
    if ([basho, day, matchNo, east, west].some(p => typeof p === 'undefined' || p === null || String(p).trim() === '')) {
      return '';
    }

    return String(basho) + String(day) + String(matchNo) + String(east) + String(west);
  };
  const canonicalMatchId = buildCanonicalMatchId(m);
  // For voting and websocket seeding: require a strict canonical id. Do NOT
  // fall back to short/explicit ids like "1" to avoid writing to legacy keys.
  const effectiveMatchId = canonicalMatchId || matchIdStr;
  const apiMatchId = (canonicalMatchId && /^\d+$/.test(String(canonicalMatchId))) ? String(Number(canonicalMatchId)) : '';
  // note: seed/websocket should use canonicalMatchId when available; otherwise skip seeding.
  try { console.debug('highlighted.matchIds', { canonicalMatchId, matchIdStr, apiMatchId }); } catch {}
  // start empty and let the seed fetch populate real counts to avoid accidental empty-string keys
  const [counts, setCounts] = React.useState<Record<string, number>>({});
  const [localUserVote, setLocalUserVote] = React.useState<'west' | 'east' | undefined>(undefined);

  // hydrate client-side vote hint so the checkmark persists across refresh
  React.useEffect(() => {
    try {
      const key = 'sumo_voted_matches_v1';
      const raw = localStorage.getItem(key);
      if (raw) {
        const parsed = JSON.parse(raw) as Record<string, 'west' | 'east'>;
        const hint = parsed[String(effectiveMatchId)];
        if (hint === 'west' || hint === 'east') setLocalUserVote(hint);
      }
    } catch (e) {
      // ignore storage parse errors
    }
  }, [effectiveMatchId]);

  // helper to robustly read possible id field names
  

  const eastId = getIdString(m, 'east_rikishi_id', 'eastId', 'east_id') ?? (nestedEast && (String(nestedEast['id'] ?? nestedEast['rikishi_id'])) ) ?? '';
  const westId = getIdString(m, 'west_rikishi_id', 'westId', 'west_id') ?? (nestedWest && (String(nestedWest['id'] ?? nestedWest['rikishi_id'])) ) ?? '';

  // Shared pfp container style to ensure identical sizing and border behavior for both sides
  // center the pfp inside its column
  const pfpStyle: React.CSSProperties = { position: 'relative', width: 170, height: 230, flex: '0 0 auto', zIndex: 1, overflow: 'hidden', borderRadius: 10, margin: '0 auto' };

  // Render helper to keep the two rikishi cards identical in layout
  const renderRikishi = (side: 'west' | 'east') => {
    const isWest = side === 'west';
    const id = isWest ? westId : eastId;
    const name = isWest ? westName : eastName;
    const img = isWest ? westImg : eastImg;
    const displayRankLocal = isWest ? displayWestRank : displayEastRank;
    const detailsLocal = isWest ? westDetails : eastDetails;
    const winnerBorder = isWest ? (winnerSide === 'west' ? '3px solid #2563eb' : '1px solid rgba(37,99,235,0.7)') : (winnerSide === 'east' ? '3px solid #e11d48' : '1px solid rgba(225,29,72,0.7)');
    const winnerShadow = isWest ? (winnerSide === 'west' ? '0 8px 28px rgba(37,99,235,0.22)' : '0 3px 8px rgba(37,99,235,0.08)') : (winnerSide === 'east' ? '0 8px 28px rgba(225,29,72,0.22)' : '0 3px 8px rgba(225,29,72,0.08)');
    const badgePos = isWest ? { top: -6, right: -6 } : { top: -6, left: -6 };

    return (
      <div style={{ minWidth: 220, display: 'flex', flexDirection: 'column', alignItems: 'center', flex: '1 1 280px' }}>
        <div className="rikishi-card" style={{ width: '100%', boxSizing: 'border-box', borderRadius: 12, padding: 28, background: 'rgba(245,230,200,0.90)', display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
          <div style={{ textAlign: 'center', marginBottom: 8 }}>
            {id ? (
              <Link href={`/rikishi/${id}`}>
                <a title={`View ${name} profile`} style={{ textDecoration: 'none', color: 'inherit' }}>
                  <div style={{ fontWeight: 900, color: '#1e293b', fontSize: 22 }}>{name}</div>
                  <div className="pfp" style={pfpStyle} aria-hidden>
                    <Image src={img} alt={`${name} profile`} width={680} height={920} style={{ width: '100%', height: '100%', objectFit: 'cover', objectPosition: 'top', display: 'block' }} quality={90} />
                    <div style={{ position: 'absolute', inset: 0, pointerEvents: 'none', borderRadius: 10, border: winnerBorder, boxShadow: winnerShadow }} />
                    {winnerSide === side && (
                      <div style={{ position: 'absolute', ...(badgePos as any), background: '#10b981', color: '#fff', width: 18, height: 18, borderRadius: '50%', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 10, fontWeight: 800 }}>W</div>
                    )}
                  </div>
                </a>
              </Link>
            ) : (
              <div className="pfp" style={pfpStyle} aria-hidden>
                <Image src={img} alt={`${name} profile`} width={680} height={920} style={{ width: '100%', height: '100%', objectFit: 'cover', objectPosition: 'top', display: 'block' }} quality={90} />
                <div style={{ position: 'absolute', inset: 0, pointerEvents: 'none', borderRadius: 10, border: winnerBorder, boxShadow: winnerShadow }} />
                {winnerSide === side && (
                  <div style={{ position: 'absolute', ...(badgePos as any), background: '#10b981', color: '#fff', width: 18, height: 18, borderRadius: '50%', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 10, fontWeight: 800 }}>W</div>
                )}
              </div>
            )}

            <div style={{ textAlign: 'left', marginTop: 2, zIndex: 2 }}>
              {detailsLocal && (
                <div className="stat-grid" style={{ marginTop: 6, color: '#333', fontSize: 17, display: 'grid', gridTemplateColumns: '1fr auto', gap: 4, columnGap: 6, alignItems: 'center', width: 'auto' }}>
                  <div style={{ textAlign: 'left', color: '#7a7a7a' }}>Height</div>
                  <div style={{ textAlign: 'right' }}>{detailsLocal.height ? `${detailsLocal.height}cm` : '—'}</div>
                  <div style={{ textAlign: 'left', color: '#7a7a7a' }}>Weight</div>
                  <div style={{ textAlign: 'right' }}>{detailsLocal.weight ? `${detailsLocal.weight}kg` : '—'}</div>
                  <div style={{ textAlign: 'left', color: '#7a7a7a' }}>Age</div>
                  <div style={{ textAlign: 'right' }}>{typeof calcAge(detailsLocal?.birthdate) === 'number' ? `${calcAge(detailsLocal?.birthdate)}y` : '—'}</div>
                  <div style={{ textAlign: 'left', color: '#7a7a7a' }}>Record</div>
                  <div style={{ textAlign: 'right' }}>{(detailsLocal.wins ?? '—') + '-' + (detailsLocal.losses ?? '—')}</div>
                  <div style={{ textAlign: 'left', color: '#7a7a7a' }}>Yusho</div>
                  <div style={{ textAlign: 'right' }}>{detailsLocal.yusho ?? 0}</div>
                  <div style={{ textAlign: 'left', color: '#7a7a7a' }}>Sansho</div>
                  <div style={{ textAlign: 'right' }}>{detailsLocal.sansho ?? 0}</div>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    );
  };
  React.useEffect(() => {
    // Only seed and open websocket when a canonical numeric id exists. Otherwise skip
    // to avoid subscribing/reading/writing to legacy short ids.
    if (!canonicalMatchId || !/^\d+$/.test(String(canonicalMatchId))) return;
    let ws: WebSocket | null = null;

    const seed = async () => {
      try {
        const url = `/api/matches/${encodeURIComponent(canonicalMatchId)}/votes`;
        const res = user ? await fetchWithAuth(url) : await fetch(url, { credentials: 'include' });
        if (res.ok) {
          const data = await res.json();
          if (data && data.counts) {
            const mapped: Record<string, number> = {};
            Object.entries(data.counts).forEach(([k, v]) => { mapped[String(k)] = Number(v || 0); });
            setCounts(prev => ({ ...prev, ...mapped }));
          }
          // authenticated user's existing vote
          if (data && typeof data.user_vote !== 'undefined' && data.user_vote !== null && user) {
            const uv = String(data.user_vote);
            if (uv === westId) setLocalUserVote('west');
            else if (uv === eastId) setLocalUserVote('east');
          }
        }
      } catch (e) {
        // seed failure is non-fatal
      }

      try {
        const envBackend = process.env.NEXT_PUBLIC_BACKEND_URL && process.env.NEXT_PUBLIC_BACKEND_URL !== '' ? process.env.NEXT_PUBLIC_BACKEND_URL : '';
        let backendBase = envBackend || `${window.location.protocol}//${window.location.host}`;
        if ((window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1') && envBackend && envBackend.includes('gin-backend')) {
          backendBase = `${window.location.protocol}//localhost:8080`;
        }
        const wsProto = backendBase.startsWith('https') ? 'wss' : 'ws';
        const hostNoProto = backendBase.replace(/^https?:\/\//, '').replace(/\/$/, '');
        ws = new WebSocket(`${wsProto}://${hostNoProto}/matches/${encodeURIComponent(canonicalMatchId)}/ws`);
        ws.onopen = () => { try { console.debug(`WS open for highlighted match ${canonicalMatchId} -> ${wsProto}://${hostNoProto}`); } catch {} };
        ws.onmessage = (ev) => {
          try {
            const payload = JSON.parse(ev.data as string);
            if (payload && payload.counts) {
              const mapped: Record<string, number> = {};
              Object.entries(payload.counts).forEach(([k, v]) => { mapped[String(k)] = Number(v || 0); });
              setCounts(prev => ({ ...prev, ...mapped }));
            }
            if (payload && payload.user && payload.new && user) {
              if (String(payload.user) === String(user.id)) {
                const chosen = String(payload.new);
                if (chosen === westId) setLocalUserVote('west');
                else if (chosen === eastId) setLocalUserVote('east');
              }
            }
          } catch (e) {
            // ignore parse errors
          }
        };
      } catch (e) {
        // websocket failure is non-fatal
      }
    };

    seed();

    return () => { try { if (ws) ws.close(); } catch {} };
  }, [canonicalMatchId, user]);

  const handleVote = async (side: 'west' | 'east') => {
    // Only allow voting when we have a strict canonical id. Otherwise skip and warn.
    if (!user) return;
    if (!apiMatchId) { try { console.warn('highlighted.vote skipped: missing canonicalMatchId', { canonicalMatchId, matchIdStr }); } catch {} ; return; }
      const rikishiId = side === 'west' ? westId : eastId;
      try {
      try { console.debug('highlighted.vote', { matchId: apiMatchId, side, rikishiId }); } catch {}
      const res = await fetchWithAuth(`/api/matches/${encodeURIComponent(apiMatchId)}/vote`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ winner: Number(rikishiId) }) });
      if (!res.ok) return;
      const data = await res.json();
      if (data && data.result && data.result.counts) {
        const mapped: Record<string, number> = {};
        Object.entries(data.result.counts).forEach(([k, v]) => { mapped[String(k)] = Number(v || 0); });
        setCounts(prev => ({ ...prev, ...mapped }));
        if (data.result.user && String(data.result.user) === String(user.id)) {
          const chosen = String(data.result.new);
          if (chosen === westId) setLocalUserVote('west');
          else if (chosen === eastId) setLocalUserVote('east');
          try {
            const key = 'sumo_voted_matches_v1';
            const raw = localStorage.getItem(key);
            const obj = raw ? JSON.parse(raw) : {};
            obj[String(apiMatchId || effectiveMatchId)] = (chosen === westId) ? 'west' : 'east';
            localStorage.setItem(key, JSON.stringify(obj));
          } catch (e) {
            // ignore storage errors
          }
        }
      }
    } catch (e) {
      // ignore for now
    }
  };

  const eastVotesNum = counts[eastId] ?? eastVotes;
  const westVotesNum = counts[westId] ?? westVotes;
  const totalVotesNum = (Number(eastVotesNum) || 0) + (Number(westVotesNum) || 0);
  let eastPercent = 50;
  let westPercent = 50;
  if (totalVotesNum > 0) {
    eastPercent = Math.round((Number(eastVotesNum) / totalVotesNum) * 100);
    westPercent = 100 - eastPercent;
  }

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
  <div className="inner-card" style={{ width: '100%', boxSizing: 'border-box', background: 'rgba(255,255,255,0.35)', padding: '0.75rem', borderRadius: 12, boxShadow: '0 6px 18px rgba(0,0,0,0.06)', border: '1px solid rgba(86,56,97,0.06)', display: 'flex', justifyContent: 'center' }}>

  {/* Row 1: (AI prediction moved into center VS column) */}

  {/* Row 2: images + VS + compact stats (VS vertically centered with pfps and stats) */}
  <div className="hme-row" style={{ display: 'flex', gap: 'clamp(8px, 2vw, 16px)', alignItems: 'center', width: 'min(980px,100%)', justifyContent: 'center', maxWidth: 980, margin: '0 auto' }}>
        {renderRikishi('west')}

        {/* Center: VS + Kimarite + AI prediction (all vertically centered with pfps/stats) */}
  <div className="hme-vs" style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', position: 'relative', margin: '0 6px', gap: 8, flexShrink: 0, alignSelf: 'center' }}>
          {/* AI prediction rendered in the center column so it's part of the same horizontal group */}
          {typeof aiPred !== 'undefined' && (
            <div className="ai-pill" style={{ padding: '8px 14px', borderRadius: 10, background: 'rgba(86,56,97,0.06)', color: '#563861', fontWeight: 700, fontSize: 14, border: '1px solid rgba(86,56,97,0.08)', display: 'inline-flex', alignItems: 'center', flexWrap: 'wrap', textAlign: 'center', justifyContent: 'center' }}>
              <span className="ai-label" style={{ paddingRight: 8, fontWeight: 700 }}>AI prediction:</span>
              <span className="ai-name" style={{ marginLeft: 6, background: aiNameBg, color: aiNameColor, borderRadius: 8, padding: '6px 12px', fontWeight: 900, display: 'inline-block', textAlign: 'center' }}>{typeof aiPred === 'number' ? (Number(aiPred) === 1 ? westName : eastName) : String(aiPred)}</span>
            </div>
          )}
          {/* VS rendered inline so it participates in normal flow and will be vertically centered with the rikishi columns */}
          <div style={{ fontWeight: 900, fontSize: 18, color: '#563861', background: '#A3E0B8', padding: '6px 12px', borderRadius: 12, boxShadow: '0 2px 6px rgba(0,0,0,0.06)' }}>VS</div>
          {kimarite && (
            <div style={{ background: 'linear-gradient(90deg,#fde68a,#fbcfe8)', padding: '6px 12px', borderRadius: 20, color: '#6b2147', fontWeight: 800, fontSize: 13 }}>{kimarite}</div>
          )}
        </div>

        {renderRikishi('east')}
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

              <div style={{ fontWeight: 700, color: '#444', minWidth: 80, textAlign: 'center' }}>{totalVotesNum} votes</div>

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
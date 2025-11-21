"use client";

import * as React from 'react';
import { useState, useEffect } from 'react';
import Box from '@mui/joy/Box';
import Typography from '@mui/joy/Typography';
import { fetchWithAuth } from '../lib/auth';
import { useAuth } from '../context/AuthContext';
import { ProgressBar } from './base/progress-indicators/progress-indicators';

interface VoteControlsProps {
  match: Record<string, unknown>;
  matchIdStrLocal: string; // used for localStorage and liveCounts map key
  apiMatchId: string; // canonical numeric string used for POST API (may be '')
  westId?: string | null;
  eastId?: string | null;
  onOpenLogin?: () => void;
  // optional AI prediction display info (VoteControls will render it where appropriate)
  aiPred?: number | undefined;
  rikishi1?: string;
  rikishi2?: string;
}

export default function VoteControls({ match, matchIdStrLocal, apiMatchId, westId, eastId, onOpenLogin, aiPred, rikishi1, rikishi2 }: VoteControlsProps) {
  const { user } = useAuth();
  const [liveCounts, setLiveCounts] = useState<Record<string, Record<string, number>>>({});
  const [userVotes, setUserVotes] = useState<Record<string, 'west' | 'east' | undefined>>({});

  // helper to read flexible fields
  const getString = (obj: Record<string, unknown> | undefined, ...keys: string[]) => {
    if (!obj) return undefined;
    for (const k of keys) {
      const v = (obj as Record<string, unknown>)[k];
      if (typeof v === 'string' && v.trim() !== '') return v;
      if (typeof v === 'number') return String(v);
    }
    return undefined;
  };
  const getNumber = (obj: Record<string, unknown> | undefined, ...keys: string[]) => {
    if (!obj) return undefined;
    for (const k of keys) {
      const v = (obj as Record<string, unknown>)[k];
      if (typeof v === 'number') return v;
      if (typeof v === 'string' && v.trim() !== '') {
        const n = Number(v);
        if (!Number.isNaN(n)) return n;
      }
    }
    return undefined;
  };

  // seed counts from match payload when available
  useEffect(() => {
    try {
      const canonical = String(match['canonical_id'] ?? match['canonicalId'] ?? '');
      const matchKey = matchIdStrLocal;
      // if match payload contains counts, seed them
      const counts = (match as any)['counts'] ?? (match as any)['vote_counts'] ?? null;
      if (counts && typeof counts === 'object') {
        const mapped: Record<string, number> = {};
        Object.entries(counts).forEach(([k, v]) => { mapped[String(k)] = Number(v || 0); });
        setLiveCounts(prev => ({ ...prev, [matchKey]: mapped }));
      } else {
        // fallback: use explicit west_votes/east_votes fields
        const westCount = getNumber(match, 'west_votes', 'westVotes') ?? 0;
        const eastCount = getNumber(match, 'east_votes', 'eastVotes') ?? 0;
        if (westCount || eastCount) setLiveCounts(prev => ({ ...prev, [matchKey]: { [String(westId ?? '')]: westCount, [String(eastId ?? '')]: eastCount } }));
      }

      // hydrate saved client-side votes for UI hint
      try {
        const raw = localStorage.getItem('sumo_voted_matches_v1');
        if (raw) {
          const parsed = JSON.parse(raw) as Record<string, 'west' | 'east'>;
          if (parsed && typeof parsed === 'object') setUserVotes(prev => ({ ...prev, ...parsed }));
        }
      } catch (e) { /* ignore */ }
    } catch (e) { /* ignore */ }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [JSON.stringify(match), matchIdStrLocal]);

  // subscribe to a backend websocket for live updates on mount (use canonical id when available)
  useEffect(() => {
    // prefer the canonical/api id when available; fall back to the local match id string
    const canonicalFromMatch = String(match['canonical_id'] ?? match['canonicalId'] ?? '');
    const matchKey = (apiMatchId && String(apiMatchId).trim() !== '') ? String(apiMatchId) : (canonicalFromMatch || matchIdStrLocal);
    if (!matchKey) return;
    let sockets: WebSocket[] = [];
    try {
      const backendBase = (process.env.NEXT_PUBLIC_BACKEND_URL && process.env.NEXT_PUBLIC_BACKEND_URL !== '') ? process.env.NEXT_PUBLIC_BACKEND_URL : `${window.location.protocol}//${window.location.host}`;
      const wsProto = backendBase.startsWith('https') ? 'wss' : 'ws';
      const hostNoProto = backendBase.replace(/^https?:\/\//, '').replace(/\/$/, '');
      // use the canonical matchKey for the websocket path so the backend subscribes to the same channel we publish to
      const ws = new WebSocket(`${wsProto}://${hostNoProto}/matches/${encodeURIComponent(matchKey)}/ws`);
      ws.onmessage = (ev) => {
        try {
          const payload = JSON.parse(ev.data as string);
          if (payload && payload.counts) {
            const mapped: Record<string, number> = {};
            Object.entries(payload.counts).forEach(([k, v]) => { mapped[String(k)] = Number(v || 0); });
            setLiveCounts(prev => ({ ...prev, [matchKey]: mapped }));
          }
          if (payload && payload.user && typeof payload.new !== 'undefined' && user) {
            if (String(payload.user) === String((user as any).id)) {
              const chosen = String(payload.new);
              if (String(westId) === chosen) setUserVotes(prev => ({ ...prev, [matchKey]: 'west' }));
              else if (String(eastId) === chosen) setUserVotes(prev => ({ ...prev, [matchKey]: 'east' }));
            }
          }
        } catch (e) {}
      };
      ws.onopen = () => { /* noop */ };
      sockets.push(ws);
      // fetch initial counts immediately so the UI shows current votes even
      // before any websocket message arrives (backend supports GET /votes)
      (async () => {
        try {
          const resp = await fetch(`/api/matches/${encodeURIComponent(matchKey)}/votes`);
          if (!resp.ok) return;
          const body = await resp.json();
          const countsObj = body?.result?.counts ?? body?.counts ?? body;
          if (countsObj && typeof countsObj === 'object') {
            const mapped: Record<string, number> = {};
            Object.entries(countsObj).forEach(([k, v]) => { mapped[String(k)] = Number((v as any) || 0); });
            setLiveCounts(prev => ({ ...prev, [matchKey]: mapped }));
          }
        } catch (e) {
          /* ignore fetch errors */
        }
      })();
    } catch (e) { /* ignore websocket errors */ }

    return () => { sockets.forEach(s => { try { s.close(); } catch {} }); };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [apiMatchId, JSON.stringify(match), matchIdStrLocal, user]);

  const handleVote = async (matchId: string, side: 'west' | 'east', rikishiId?: string) => {
    try { console.debug('vote', { matchId, side, rikishiId }); } catch (e) {}
    if (!user) return;
    if (!rikishiId) return;
    try {
      const res = await fetchWithAuth(`/api/matches/${encodeURIComponent(String(matchId))}/vote`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ winner: Number(rikishiId) })
      });
      if (!res.ok) return;
      const data = await res.json();
      if (data && data.result && data.result.counts) {
        const mapped: Record<string, number> = {};
        Object.entries(data.result.counts).forEach(([k, v]) => { mapped[String(k)] = Number(v || 0); });
        setLiveCounts(prev => ({ ...prev, [String(matchId)]: mapped }));
        setUserVotes(prev => ({ ...prev, [String(matchId)]: side }));
        try {
          const key = 'sumo_voted_matches_v1';
          const raw = localStorage.getItem(key);
          const obj = raw ? JSON.parse(raw) : {};
          obj[String(matchId)] = side;
          localStorage.setItem(key, JSON.stringify(obj));
        } catch (e) { /* ignore storage errors */ }
      }
    } catch (e) { /* ignore */ }
  };

  // Render the exact same vote UI markup used previously to avoid layout shifts.
  const countsForMatch = liveCounts[matchIdStrLocal];
  const westCount = (countsForMatch && westId) ? (countsForMatch[String(westId)] ?? 0) : (getNumber(match, 'west_votes', 'westVotes') ?? 0);
  const eastCount = (countsForMatch && eastId) ? (countsForMatch[String(eastId)] ?? 0) : (getNumber(match, 'east_votes', 'eastVotes') ?? 0);
  const total = Math.max(1, westCount + eastCount);
  const progressValue = Math.round((westCount / total) * 100);

  return (
    <>
      <Typography sx={{ fontSize: '1rem', color: '#563861', opacity: 0.95, mb: 0.3, fontWeight: 600, fontFamily: 'inherit' }}>
        {`${westCount + eastCount} vote${(westCount + eastCount) === 1 ? '' : 's'}`}
      </Typography>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, width: '100%', justifyContent: 'center' }}>
        <button
          style={{
            background: userVotes[matchIdStrLocal] === 'west' ? '#e0709f' : '#e0709f',
            color: '#fff',
            border: userVotes[matchIdStrLocal] === 'west' ? '2px solid #c45e8b' : '2px solid #c45e8b',
            borderRadius: 8,
            width: 84,
            height: 34,
            fontWeight: 600,
            cursor: 'pointer',
            fontSize: '0.92rem',
            transition: 'background 0.18s, border 0.18s',
            display: 'inline-block',
            position: 'relative',
            boxShadow: userVotes[matchIdStrLocal] === 'west' ? '0 2px 6px #e0709faa' : '0 1px 2px #e0709f66',
          }}
          onClick={e => { e.stopPropagation(); if (!apiMatchId) { try { console.warn('vote skipped: missing canonical match id for', match); } catch{}; return; } handleVote(apiMatchId, 'west', String(westId)); }}
        >
          {userVotes[matchIdStrLocal] === 'west' ? '✔' : 'Vote'}
        </button>

        <Box sx={{ width: 180, maxWidth: '60%' }}>
          <ProgressBar value={progressValue} className="h-3 bg-red-500" progressClassName="bg-blue-500" />
        </Box>

        <button
          style={{
            background: userVotes[matchIdStrLocal] === 'east' ? '#3ccf9a' : '#3ccf9a',
            color: '#fff',
            border: userVotes[matchIdStrLocal] === 'east' ? '2px solid #2aa97a' : '2px solid #2aa97a',
            borderRadius: 8,
            width: 84,
            height: 34,
            fontWeight: 600,
            cursor: 'pointer',
            fontSize: '0.92rem',
            transition: 'background 0.18s, border 0.18s',
            display: 'inline-block',
            position: 'relative',
            boxShadow: userVotes[matchIdStrLocal] === 'east' ? '0 2px 6px #3ccf9aaa' : '0 1px 2px #3ccf9a66',
          }}
          onClick={e => { e.stopPropagation(); if (!apiMatchId) { try { console.warn('vote skipped: missing canonical match id for', match); } catch{}; return; } handleVote(apiMatchId, 'east', String(eastId)); }}
        >
          {userVotes[matchIdStrLocal] === 'east' ? '✔' : 'Vote'}
        </button>
      </Box>
      {/* AI prediction shown directly below the votes row when provided */}
      {typeof aiPred !== 'undefined' && rikishi1 && rikishi2 && (
        (() => {
          const predictedName = (Number(aiPred) === 1) ? rikishi1 : rikishi2;
          const aiSide = (Number(aiPred) === 1) ? 'west' : 'east';
          const colors = {
            west: { bg: '#e0709f', border: '#c45e8b', text: '#ffffff' },
            east: { bg: '#3ccf9a', border: '#2aa97a', text: '#ffffff' },
          } as const;
          const chosen = aiSide === 'west' ? colors.west : colors.east;
          return (
            <Box sx={{ mt: 0.6, background: 'rgba(86,56,97,0.04)', color: '#563861', borderRadius: 8, px: '0.5rem', py: '0.08rem', fontSize: '0.78rem', fontWeight: 700, border: '1px solid rgba(86,56,97,0.06)' }}>
              <span style={{ paddingRight: 6, fontWeight: 600 }}>AI prediction:</span>
              <span style={{ paddingLeft: 6, background: chosen.bg, color: chosen.text, borderRadius: 6, padding: '0 6px', fontWeight: 700, border: `1px solid ${chosen.border}` }}>{String(predictedName)}</span>
            </Box>
          );
        })()
      )}
    </>
  );
}

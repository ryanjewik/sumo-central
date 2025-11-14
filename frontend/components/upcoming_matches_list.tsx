"use client";
import * as React from 'react';
import Box from '@mui/joy/Box';
import List from '@mui/joy/List';
import ListItem from '@mui/joy/ListItem';
import ListItemButton from '@mui/joy/ListItemButton';
import Typography from '@mui/joy/Typography';
import Image from 'next/image';
import { ProgressBar } from "./base/progress-indicators/progress-indicators";




interface UpcomingMatchesListProps {
  // Matches can be in varied shapes (some backends use west_rikishi_id, others use rikishi1_id, etc.)
  // Use a loose-but-typed incoming shape and helper accessors below to avoid `any`.
  matches: Record<string, unknown>[];
  date?: string;
  // optional callback to open the login dialog in the parent (used when user is not signed in)
  onOpenLogin?: () => void;
}



import { useState } from 'react';

// Auto-fit a single-line text by shrinking font size until it fits within its container.
const AutoFitText: React.FC<{ text: string; maxPx?: number; minPx?: number; sx?: any }> = ({ text, maxPx = 14, minPx = 10, sx }) => {
  const ref = React.useRef<HTMLDivElement | null>(null);
  const [fontPx, setFontPx] = React.useState<number>(maxPx);

  React.useLayoutEffect(() => {
    const el = ref.current;
    if (!el) return;
    // start at maxPx and shrink until fits or minPx reached
    let current = maxPx;
    // apply immediately to measure
    el.style.fontSize = `${current}px`;
    el.style.whiteSpace = 'nowrap';
    // loop a few times; limit iterations for safety
    let iterations = 0;
    while (el.scrollWidth > el.clientWidth && current > minPx && iterations < 40) {
      current = Math.max(minPx, current - 0.5);
      el.style.fontSize = `${current}px`;
      iterations += 1;
    }
    setFontPx(current);
  }, [text, maxPx, minPx]);

  return (
    <Box ref={ref} component="div" sx={{ overflow: 'hidden', textAlign: 'center', ...sx }} style={{ fontSize: `${fontPx}px`, whiteSpace: 'nowrap' }}>
      {text}
    </Box>
  );
};

import { useAuth } from '../context/AuthContext';

const UpcomingMatchesList: React.FC<UpcomingMatchesListProps> = ({ matches, date, onOpenLogin }) => {
  const { user } = useAuth();
  // Track votes for each match: { [matchId]: { west: number; east: number } }
  const [votes, setVotes] = useState<Record<number, { west: number; east: number }>>({});
  // Track user's vote for each match: { [matchId]: 'west' | 'east' | undefined }
  const [userVotes, setUserVotes] = useState<Record<number, 'west' | 'east' | undefined>>({});

  const handleVote = (matchId: number, side: 'west' | 'east') => {
    setVotes(prev => {
      const matchVotes = prev[matchId] || { west: 0, east: 0 };
      const prevUserVote = userVotes[matchId];
      // If user already voted for this side, do nothing
      if (prevUserVote === side) return prev;
      // Remove previous vote if exists, add new vote
  const newVotes = { ...matchVotes };
      if (prevUserVote) {
        newVotes[prevUserVote] = Math.max(0, newVotes[prevUserVote] - 1);
      }
      newVotes[side] = newVotes[side] + 1;
      return { ...prev, [matchId]: newVotes };
    });
    setUserVotes(prev => ({ ...prev, [matchId]: side }));
  };

  // rikishi id -> image cache (s3_url preferred)
  // Server now supplies rikishi images/ranks nested in the match payload when available.

  // helper accessors to safely read possibly-unknown incoming object shapes
  const getString = (obj: Record<string, unknown>, ...keys: string[]): string | undefined => {
    for (const k of keys) {
      const v = obj[k];
      if (typeof v === 'string' && v.trim() !== '') return v;
      if (typeof v === 'number') return String(v);
    }
    return undefined;
  };

  const getNumber = (obj: Record<string, unknown>, ...keys: string[]): number | undefined => {
    for (const k of keys) {
      const v = obj[k];
      if (typeof v === 'number') return v;
      if (typeof v === 'string' && v.trim() !== '') {
        const n = Number(v);
        if (!Number.isNaN(n)) return n;
      }
    }
    return undefined;
  };

  const getIdString = (obj: Record<string, unknown>, ...keys: string[]): string | undefined => {
    for (const k of keys) {
      const v = obj[k];
      if (typeof v === 'number') return String(v);
      if (typeof v === 'string' && v.trim() !== '') return v;
    }
    return undefined;
  };

  // Populate initial random votes for matches (non-destructive)
  const serializedMatches = React.useMemo(() => JSON.stringify(matches), [matches]);

  React.useEffect(() => {
    const initial: Record<number, { west: number; east: number }> = {};
    matches.forEach(m => {
      const id = getNumber(m as Record<string, unknown>, 'id', 'matchId');
      if (!id) return;
      const west = Math.floor(Math.random() * 20) + 1;
      const east = Math.floor(Math.random() * 20) + 1;
      initial[id] = { west, east };
    });
    if (Object.keys(initial).length > 0) setVotes(prev => ({ ...initial, ...prev }));
  }, [serializedMatches, matches]);

  // No client-side rikishi fetches: prefer server-provided nested `west_rikishi` / `east_rikishi` or top-level fields.
  // Small inline Image component that falls back to a local placeholder when the remote image fails
  const ImageWithFallback: React.FC<{ src?: string | null; alt?: string; width?: number; height?: number; quality?: number; style?: React.CSSProperties }> = ({ src, alt, width, height, quality, style }) => {
    const placeholder = '/sumo_logo.png';
    const initial = (typeof src === 'string' && src.trim() !== '') ? src : placeholder;
    const [current, setCurrent] = React.useState<string>(initial);
    React.useEffect(() => { setCurrent((typeof src === 'string' && src.trim() !== '') ? src as string : placeholder); }, [src]);
    return (
      <Image
        src={current}
        alt={alt ?? ''}
        width={width}
        height={height}
        onError={() => { if (current !== placeholder) setCurrent(placeholder); }}
        style={style}
        quality={quality}
      />
    );
  };

  return (
    <Box sx={{ display: 'flex', flexWrap: 'wrap', justifyContent: 'center', gap: 4, mb: 0 }}>
      <Box
        className="app-text"
        sx={{
          width: '100%',
          background: '#A3E0B8',
          borderRadius: '1rem',
          boxShadow: '0 2px 8px rgba(0,0,0,0.08)',
          p: '0.5rem',
          minWidth: 260,
          border: '4px solid #563861',
          position: 'relative',
          zIndex: 0,
          overflow: 'hidden',
        }}
      >
        <Typography className="app-text" level="title-lg" sx={{ fontWeight: 1000, fontSize: '1.5rem' }}>
          Upcoming Matches
        </Typography>
  {date && (() => {
          // Display only YYYY-MM-DD portion. Incoming date strings sometimes include
          // time (e.g. 2025-09-20T00:00:00). Trim that to just the date for display.
          let display = String(date);
          if (display.includes('T')) display = display.split('T')[0];
          else if (display.includes(' ')) display = display.split(' ')[0];
          else if (display.length > 10) display = display.slice(0, 10);
          return (
            <Typography sx={{ color: '#563861', fontWeight: 600, fontSize: '1.08rem', mb: 2, fontFamily: 'inherit' }}>
              {display}
            </Typography>
          );
        })()}

        <List variant="outlined" sx={{ minWidth: 240, borderRadius: 'sm', p: 0, m: 0 }}>
          {matches.length === 0 ? (
            <ListItem>
              <Typography sx={{ color: '#888', fontSize: '0.95rem' }}>No upcoming matches scheduled.</Typography>
            </ListItem>
          ) : (
            matches.map((match, idx) => {
              // normalize/derive commonly-used fields from the loose incoming shape
              const id = getNumber(match, 'id', 'matchId');
              if (!id) return null;
              const matchVotes = votes[id] || { west: 0, east: 0 };
              const totalVotes = matchVotes.west + matchVotes.east;
              const percent = totalVotes === 0 ? 50 : Math.round((matchVotes.west / totalVotes) * 100);

              const aiRaw = (match as Record<string, unknown>)['ai_prediction'] ?? (match as Record<string, unknown>)['AI_prediction'] ?? (match as Record<string, unknown>)['aiPrediction'];
              let aiPred: number | undefined;
              if (typeof aiRaw === 'number') aiPred = aiRaw;
              else if (typeof aiRaw === 'boolean') aiPred = aiRaw ? 1 : 0;
              else if (typeof aiRaw === 'string') {
                const n = Number(aiRaw);
                if (!Number.isNaN(n)) aiPred = n;
              }

              const westId = getIdString(match, 'west_rikishi_id', 'westId', 'rikishi1_id');
              const eastId = getIdString(match, 'east_rikishi_id', 'eastId', 'rikishi2_id');
              const rikishi1 = getString(match, 'rikishi1') ?? String(westId ?? '');
              const rikishi2 = getString(match, 'rikishi2') ?? String(eastId ?? '');
              const rawR1 = getString(match, 'rikishi1Rank', 'rikishi1_rank');
              const rawR2 = getString(match, 'rikishi2Rank', 'rikishi2_rank');
              const nestedWest = (match as Record<string, unknown>)['west_rikishi'] as Record<string, unknown> | undefined;
              const nestedEast = (match as Record<string, unknown>)['east_rikishi'] as Record<string, unknown> | undefined;
              const rikishi1Rank = rawR1 ?? (nestedWest ? (String(nestedWest['current_rank'] ?? nestedWest['rank'] ?? 'NA')) : 'NA');
              const rikishi2Rank = rawR2 ?? (nestedEast ? (String(nestedEast['current_rank'] ?? nestedEast['rank'] ?? 'NA')) : 'NA');

              return (
                <ListItem key={String(id)} sx={{ p: 0, m: 0, listStyle: 'none', position: 'relative' }}>
                  <ListItemButton
                    className='app-text'
                    sx={{
                      width: '100%',
                      display: 'flex',
                      alignItems: 'center',
                      justifyContent: 'center',
                      gap: 1,
                      background: '#F5E6C8',
                      borderRadius: '0.75rem',
                      minHeight: 48,
                      cursor: 'pointer',
                      transition: 'transform 0.15s, box-shadow 0.15s',
                      position: 'relative',
                      isolation: 'isolate',
                      mb: 1,
                      pb: idx < matches.length - 1 ? 'calc(0.3rem + 1px)' : 0,
                      px: '0.25rem',
                      '&:hover': {
                        transform: 'scale(1.03)',
                        boxShadow: '0 4px 16px rgba(86,56,97,0.15)',
                      },
                    }}
                  >
                    {/* (AI prediction pill moved below the votes row) */}

                    {/* Left and right columns: name + rank above a larger avatar */}
                    <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 2, width: '100%' }}>
                      <Box sx={{ minWidth: 0, display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', gap: 0.5, width: 100 }}>
                        <AutoFitText text={rikishi1} maxPx={18} minPx={10} sx={{ fontWeight: 600, maxWidth: '100px', textAlign: 'center' }} />
                        <AutoFitText text={rikishi1Rank || 'NA'} maxPx={14} minPx={10} sx={{ color: '#7a4b7a', fontWeight: 500, maxWidth: '120px' }} />
                        {/* slightly smaller avatar to avoid bleeding outside tab */}
                        <Box sx={{ width: 44, height: 44, mt: 0.5, overflow: 'hidden', borderRadius: '50%' }}>
                          {(() => {
                            const westId = getIdString(match, 'west_rikishi_id', 'westId', 'rikishi1_id');
                            const nestedWest = (match as Record<string, unknown>)['west_rikishi'] as Record<string, unknown> | undefined;
                            const src = getString(match, 'west_image', 'west_image_url', 'west_photo') ?? (nestedWest ? String(nestedWest['s3_url'] ?? nestedWest['image_url'] ?? nestedWest['pfp_url']) : null) ?? '/sumo_logo.png';
                            return (
                              <ImageWithFallback
                                src={src}
                                alt={`${rikishi1} profile`}
                                width={96}
                                height={96}
                                style={{ width: '100%', height: '100%', objectFit: 'cover', objectPosition: 'top', display: 'block' }}
                                quality={85}
                              />
                            );
                          })()}
                        </Box>
                      </Box>

                      {/* Center: Vote area. If the user is signed in show voting controls; otherwise show Sign in button */}
                      <Box sx={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 0.5, width: '38%', justifyContent: 'center' }}>
                        {user ? (
                          <>
                            {/* votes count above the vote row */}
                            <Typography sx={{ fontSize: '1rem', color: '#563861', opacity: 0.95, mb: 0.3, fontWeight: 600, fontFamily: 'inherit' }}>
                              {matchVotes.west + matchVotes.east} vote{(matchVotes.west + matchVotes.east) === 1 ? '' : 's'}
                            </Typography>
                            <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, width: '100%', justifyContent: 'center' }}>
                              <button
                                style={{
                                  background: userVotes[id] === 'west' ? '#e0709f' : '#e0709f',
                                  color: '#fff',
                                  border: userVotes[id] === 'west'
                                    ? '2px solid #c45e8b'
                                    : '2px solid #c45e8b',
                                  borderRadius: 8,
                                  width: 84,
                                  height: 34,
                                  fontWeight: 600,
                                  cursor: 'pointer',
                                  fontSize: '0.92rem',
                                  transition: 'background 0.18s, border 0.18s',
                                  display: 'inline-block',
                                  position: 'relative',
                                  boxShadow: userVotes[id] === 'west' ? '0 2px 6px #e0709faa' : '0 1px 2px #e0709f66',
                                }}
                                onClick={e => { e.stopPropagation(); handleVote(id, 'west'); }}
                              >
                                {userVotes[id] === 'west' ? '✔' : 'Vote'}
                              </button>

                              <Box sx={{ width: 180, maxWidth: '60%' }}>
                                <ProgressBar value={percent} className="h-3 bg-red-500" progressClassName="bg-blue-500" />
                              </Box>

                              <button
                                style={{
                                  background: userVotes[id] === 'east' ? '#3ccf9a' : '#3ccf9a',
                                  color: '#fff',
                                  border: userVotes[id] === 'east'
                                    ? '2px solid #2aa97a'
                                    : '2px solid #2aa97a',
                                  borderRadius: 8,
                                  width: 84,
                                  height: 34,
                                  fontWeight: 600,
                                  cursor: 'pointer',
                                  fontSize: '0.92rem',
                                  transition: 'background 0.18s, border 0.18s',
                                  display: 'inline-block',
                                  position: 'relative',
                                  boxShadow: userVotes[id] === 'east' ? '0 2px 6px #3ccf9aaa' : '0 1px 2px #3ccf9a66',
                                }}
                                onClick={e => { e.stopPropagation(); handleVote(id, 'east'); }}
                              >
                                {userVotes[id] === 'east' ? '✔' : 'Vote'}
                              </button>
                            </Box>

                            {/* AI prediction shown directly below the votes row */}
                            {typeof aiPred !== 'undefined' && (
                              (() => {
                                const predictedName = (Number(aiPred) === 1) ? rikishi1 : rikishi2;
                                const aiSide = (Number(aiPred) === 1) ? 'west' : 'east';
                                // match the vote button colors
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
                        ) : (
                          <>
                            <Typography sx={{ fontSize: '0.82rem', color: '#888', mb: 0.3 }}>
                              Sign in to vote
                            </Typography>
                            <button
                              style={{
                                background: '#563861',
                                color: '#fff',
                                border: '2px solid rgba(86,56,97,0.9)',
                                borderRadius: 8,
                                width: 160,
                                height: 36,
                                fontWeight: 700,
                                cursor: 'pointer',
                                fontSize: '0.95rem',
                              }}
                              onClick={(e) => { e.stopPropagation(); if (onOpenLogin) onOpenLogin(); else { try { (window as any).location.href = '/'; } catch {} } }}
                            >
                              Sign in to vote
                            </button>
                          </>
                        )}
                      </Box>

                      <Box sx={{ minWidth: 0, display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', gap: 0.5, width: 100 }}>
                        <AutoFitText text={rikishi2} maxPx={18} minPx={10} sx={{ fontWeight: 600, maxWidth: '100px', textAlign: 'center' }} />
                        <AutoFitText text={rikishi2Rank || 'NA'} maxPx={14} minPx={10} sx={{ color: '#7a4b7a', fontWeight: 500, maxWidth: '120px' }} />
                        {/* slightly smaller avatar to avoid bleeding outside tab */}
                        <Box sx={{ width: 44, height: 44, mt: 0.5, overflow: 'hidden', borderRadius: '50%' }}>
                          {(() => {
                            const eastId = getIdString(match, 'east_rikishi_id', 'eastId', 'rikishi2_id');
                            const nestedEast = (match as Record<string, unknown>)['east_rikishi'] as Record<string, unknown> | undefined;
                            const src = getString(match, 'east_image', 'east_image_url', 'east_photo') ?? (nestedEast ? String(nestedEast['s3_url'] ?? nestedEast['image_url'] ?? nestedEast['pfp_url']) : null) ?? '/sumo_logo.png';
                            return (
                              <ImageWithFallback
                                src={src}
                                alt={`${rikishi2} profile`}
                                width={96}
                                height={96}
                                style={{ width: '100%', height: '100%', objectFit: 'cover', objectPosition: 'top', display: 'block' }}
                                quality={85}
                              />
                            );
                          })()}
                        </Box>
                      </Box>

                    {/* vote count moved above the vote row in the center column */}
                    </Box>
                    {/* (removed duplicate small east avatar - larger avatar in the right column above is used) */}
                    {/* Divider drawn INSIDE this item (never overlaps the next row) */}
                    {idx < matches.length - 1 && (
                      <Box
                        sx={{
                          pointerEvents: 'none',
                          position: 'absolute',
                          left: 8,
                          right: 8,
                          bottom: 0,
                          height: 1,
                          backgroundColor: 'var(--joy-palette-divider)',
                        }}
                      />
                    )}
                  </ListItemButton>
                </ListItem>
              );
            })
          )}
        </List>
      </Box>
    </Box>
  );
};

export default UpcomingMatchesList;

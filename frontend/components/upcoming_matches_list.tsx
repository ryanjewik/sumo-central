"use client";
import * as React from 'react';
import Box from '@mui/joy/Box';
import List from '@mui/joy/List';
import ListItem from '@mui/joy/ListItem';
import ListItemButton from '@mui/joy/ListItemButton';
import Typography from '@mui/joy/Typography';
import Avatar from '@mui/joy/Avatar';
import { ProgressBar } from "./base/progress-indicators/progress-indicators";




interface UpcomingMatchesListProps {
  // Matches can be in varied shapes (some backends use west_rikishi_id, others use rikishi1_id, etc.)
  // Use a loose-but-typed incoming shape and helper accessors below to avoid `any`.
  matches: Record<string, unknown>[];
  date?: string;
}



import { useState } from 'react';

const UpcomingMatchesList: React.FC<UpcomingMatchesListProps> = ({ matches, date }) => {
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
  const [rikishiImages, setRikishiImages] = React.useState<Record<string, string>>({});

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

  // detect if incoming matches include rikishi id fields and fetch missing images
  const serializedRikishiImages = React.useMemo(() => JSON.stringify(rikishiImages), [rikishiImages]);

  React.useEffect(() => {
    let mounted = true;
    try {
  const parsedMatches: Record<string, unknown>[] = JSON.parse(serializedMatches || '[]') as Record<string, unknown>[];
      const cached: Record<string, string> = JSON.parse(serializedRikishiImages || '{}');
      const ids = new Set<string>();
      parsedMatches.forEach((m) => {
        const westId = getString(m, 'west_rikishi_id', 'westId', 'rikishi1_id') ?? (getNumber(m, 'west_rikishi_id', 'westId', 'rikishi1_id') ? String(getNumber(m, 'west_rikishi_id', 'westId', 'rikishi1_id')) : undefined);
        const eastId = getString(m, 'east_rikishi_id', 'eastId', 'rikishi2_id') ?? (getNumber(m, 'east_rikishi_id', 'eastId', 'rikishi2_id') ? String(getNumber(m, 'east_rikishi_id', 'eastId', 'rikishi2_id')) : undefined);
        if (westId && !cached[String(westId)]) ids.add(String(westId));
        if (eastId && !cached[String(eastId)]) ids.add(String(eastId));
      });
      if (ids.size === 0) return undefined;
      (async () => {
        const out: Record<string, string> = {};
        await Promise.all(Array.from(ids).map(async (id) => {
          try {
            const res = await fetch(`/api/rikishi/${encodeURIComponent(id)}`);
            if (!mounted) return;
            if (!res.ok) return;
            const doc = await res.json();
            const s = doc?.rikishi?.s3_url ?? doc?.s3_url ?? doc?.rikishi?.pfp_url ?? doc?.pfp_url ?? doc?.rikishi?.image_url ?? doc?.image_url ?? null;
            if (s) out[id] = s;
          } catch {
            // ignore
          }
        }));
        if (!mounted) return;
        if (Object.keys(out).length > 0) setRikishiImages(prev => ({ ...prev, ...out }));
      })();
    } catch {
      // parsing failed; bail
    }
    return () => { mounted = false };
  // include serializedMatches and serializedRikishiImages (strings) only
  }, [serializedMatches, serializedRikishiImages]);

  return (
    <Box sx={{ display: 'flex', flexWrap: 'wrap', justifyContent: 'center', gap: 4, mb: 0 }}>
      <Box
        className="app-text"
        sx={{
          width: '100%',
          background: '#A3E0B8',
          borderRadius: '1rem',
          boxShadow: '0 2px 8px rgba(0,0,0,0.08)',
          p: '1rem',
          minWidth: 260,
          border: '4px solid #563861',
          position: 'relative',
          zIndex: 0,
        }}
      >
        <Typography className="app-text" level="title-lg" sx={{ fontWeight: 1000, fontSize: '1.5rem' }}>
          Upcoming Matches
        </Typography>
        {date && (
          <Typography sx={{ color: '#563861', fontWeight: 500, fontSize: '1.08rem', mb: 2 }}>
            {date}
          </Typography>
        )}

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

              const rikishi1 = getString(match, 'rikishi1') ?? String(getIdString(match, 'rikishi1_id') ?? '');
              const rikishi2 = getString(match, 'rikishi2') ?? String(getIdString(match, 'rikishi2_id') ?? '');
              const rikishi1Rank = getString(match, 'rikishi1Rank', 'rikishi1_rank');
              const rikishi2Rank = getString(match, 'rikishi2Rank', 'rikishi2_rank');

              return (
                <ListItem key={String(id)} sx={{ p: 0, m: 0, listStyle: 'none', position: 'relative' }}>
                  <ListItemButton
                    className='app-text'
                    sx={{
                      width: '100%',
                      display: 'flex',
                      alignItems: 'center',
                      justifyContent: 'center',
                      gap: 2,
                      background: '#F5E6C8',
                      borderRadius: '0.75rem',
                      minHeight: 60,
                      cursor: 'pointer',
                      transition: 'transform 0.15s, box-shadow 0.15s',
                      position: 'relative',
                      isolation: 'isolate',
                      mb: 1.5,
                      pb: idx < matches.length - 1 ? 'calc(0.5rem + 1px)' : 0,
                      '&:hover': {
                        transform: 'scale(1.03)',
                        boxShadow: '0 4px 16px rgba(86,56,97,0.15)',
                      },
                    }}
                  >
                    {/* AI prediction indicator (top-right) */}
                    {typeof aiPred !== 'undefined' && (
                      <Box sx={{ position: 'absolute', top: 6, right: 10, background: '#fff', borderRadius: '0.4rem', px: 0.6, py: 0.15, border: '1px solid rgba(0,0,0,0.06)', fontSize: '0.78rem', fontWeight: 700, color: '#563861' }}>
                        {Number(aiPred) === 1 ? 'AI → West' : 'AI → East'}
                      </Box>
                    )}

                    {/* West avatar */}
                    <Box sx={{ position: 'relative' }}>
                      {
                        (() => {
                          const westId = getIdString(match, 'west_rikishi_id', 'westId', 'rikishi1_id');
                          const src = getString(match, 'west_image', 'west_image_url', 'west_photo') ?? (westId ? rikishiImages[String(westId)] : null) ?? '/sumo_logo.png';
                          return <Avatar size="sm" src={src} />;
                        })()
                      }
                    </Box>
                    {/* Middle content */}
                    <Box sx={{ textAlign: 'center', flex: 1, display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
                      <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 4, width: '100%' }}>
                        <Box sx={{ minWidth: 0, display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center' }}>
                          <Typography className="app-text" level="body-md" sx={{ fontWeight: 600, fontSize: '1.13rem', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis', maxWidth: '100%', textAlign: 'center' }}>{rikishi1}</Typography>
                          <Typography className="app-text" level="body-xs" sx={{ color: '#a06b9a', fontWeight: 500, fontSize: '0.98rem', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis', maxWidth: '100%', mt: 0.2, textAlign: 'center' }}>{rikishi1Rank || 'Rank TBD'}</Typography>
                        </Box>
                        <Typography className="app-text" level="body-md" sx={{ fontWeight: 700, fontSize: '1.13rem', color: '#563861', mx: 1 }}>VS</Typography>
                        <Box sx={{ minWidth: 0, display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center' }}>
                          <Typography className="app-text" level="body-md" sx={{ fontWeight: 600, fontSize: '1.13rem', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis', maxWidth: '100%', textAlign: 'center' }}>{rikishi2}</Typography>
                          <Typography className="app-text" level="body-xs" sx={{ color: '#a06b9a', fontWeight: 500, fontSize: '0.98rem', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis', maxWidth: '100%', mt: 0.2, textAlign: 'center' }}>{rikishi2Rank || 'Rank TBD'}</Typography>
                        </Box>
                      </Box>
                      <Box sx={{ display: 'flex', justifyContent: 'center', mt: 1, width: '100%' }}>
                        <Box sx={{ width: 240, maxWidth: '100%' }}>
                          <ProgressBar value={percent} className="bg-red-500" progressClassName="bg-blue-500" />
                        </Box>
                      </Box>
                      <Box sx={{ display: 'flex', justifyContent: 'center', gap: 1, mt: 1, width: '100%' }}>
                        <button
                          style={{
                            background: userVotes[id] === 'west' ? '#e0a3c2' : '#f5e6c8',
                            color: '#563861',
                            border: userVotes[id] === 'west'
                              ? '2.5px solid #563861'
                              : '2.5px solid #e0a3c2',
                            borderRadius: '0.5rem',
                            width: 110,
                            height: 36,
                            fontWeight: 600,
                            cursor: 'pointer',
                            fontSize: '1rem',
                            transition: 'background 0.18s, border 0.18s',
                            display: 'inline-block',
                            position: 'relative',
                            boxShadow: userVotes[id] === 'west' ? '0 2px 8px #e0a3c2aa' : '0 1px 2px #e0a3c255',
                          }}
                            onClick={e => {
                            e.stopPropagation();
                            handleVote(id, 'west');
                          }}
                        >
                          {userVotes[id] === 'west' ? '✔' : 'Vote'}
                        </button>
                        <button
                          style={{
                            background: userVotes[id] === 'east' ? '#a3e0b8' : '#f5e6c8',
                            color: '#563861',
                            border: userVotes[id] === 'east'
                              ? '2.5px solid #563861'
                              : '2.5px solid #a3e0b8',
                            borderRadius: '0.5rem',
                            width: 110,
                            height: 36,
                            fontWeight: 600,
                            cursor: 'pointer',
                            fontSize: '1rem',
                            transition: 'background 0.18s, border 0.18s',
                            display: 'inline-block',
                            position: 'relative',
                            boxShadow: userVotes[id] === 'east' ? '0 2px 8px #a3e0b8aa' : '0 1px 2px #a3e0b855',
                          }}
                          onClick={e => {
                            e.stopPropagation();
                            handleVote(id, 'east');
                          }}
                        >
                          {userVotes[id] === 'east' ? '✔' : 'Vote'}
                        </button>
                      </Box>
                      <Typography sx={{ fontSize: '0.85rem', color: '#888', mt: 0.5 }}>
                        {matchVotes.west + matchVotes.east} vote{(matchVotes.west + matchVotes.east) === 1 ? '' : 's'}
                      </Typography>
                    </Box>
                    {/* East avatar */}
                    <Box sx={{ position: 'relative' }}>
                      {
                        (() => {
                          const eastId = getIdString(match, 'east_rikishi_id', 'eastId', 'rikishi2_id');
                          const src = getString(match, 'east_image', 'east_image_url', 'east_photo') ?? (eastId ? rikishiImages[String(eastId)] : null) ?? '/sumo_logo.png';
                          return <Avatar size="sm" src={src} />;
                        })()
                      }
                    </Box>
                    {/* Divider drawn INSIDE this item (never overlaps the next row) */}
                    {idx < matches.length - 1 && (
                      <Box
                        sx={{
                          pointerEvents: 'none',
                          position: 'absolute',
                          left: 16,
                          right: 16,
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

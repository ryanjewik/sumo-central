"use client";

import * as React from 'react';
import { useAuth } from '../context/AuthContext';
import Box from '@mui/joy/Box';
import List from '@mui/joy/List';
import ListItem from '@mui/joy/ListItem';
import ListItemButton from '@mui/joy/ListItemButton';
import Typography from '@mui/joy/Typography';
import Image from 'next/image';
import Link from 'next/link';
import { ProgressBar } from "./base/progress-indicators/progress-indicators";
import VoteControls from './VoteControls';




interface UpcomingMatchesListProps {
  // Matches can be in varied shapes (some backends use west_rikishi_id, others use rikishi1_id, etc.)
  // Use a loose-but-typed incoming shape and helper accessors below to avoid `any`.
  matches: Record<string, unknown>[];
  date?: string;
  // optional callback to open the login dialog in the parent (used when user is not signed in)
  onOpenLogin?: () => void;
}



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

const UpcomingMatchesList: React.FC<UpcomingMatchesListProps> = ({ matches, date, onOpenLogin }) => {

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

  // Build canonical match id like HighlightedMatchCard: basho_id + day + match_number + east + west
  const buildCanonicalMatchId = (mm: any): string => {
    // Build a strict composite canonical id only from explicit components.
    // Do NOT treat a short numeric `id` field as canonical since many payloads
    // populate `id` with a simple match number (1,2,3) which is NOT the
    // canonical composite key we need for Redis/Postgres.
    if (!mm) return '';
    const basho = mm.basho_id ?? mm.basho ?? mm.tournament_id ?? mm.bashoId;
    const day = mm.day ?? mm.Day ?? mm.match_day;
    const matchNo = mm.match_number ?? mm.matchNumber ?? mm.matchNo ?? mm.num;
    const east = getIdString(mm as Record<string, unknown>, 'east_rikishi_id', 'eastId', 'east_id', 'rikishi2_id', 'rikishi1_id') ?? (mm && mm.east_rikishi && String((mm.east_rikishi.id || mm.east_rikishi.rikishi_id) ?? ''));
    const west = getIdString(mm as Record<string, unknown>, 'west_rikishi_id', 'westId', 'west_id', 'rikishi1_id', 'rikishi2_id') ?? (mm && mm.west_rikishi && String((mm.west_rikishi.id || mm.west_rikishi.rikishi_id) ?? ''));
    if (!basho || !day || !matchNo || !east || !west) return '';
    return String(basho) + String(day) + String(matchNo) + String(east) + String(west);
  };

  // no client-side vote state in the list component: VoteControls handles live updates + voting

  

  // No client-side rikishi fetches: prefer server-provided nested `west_rikishi` / `east_rikishi` or top-level fields.
  // Small inline Image component that falls back to a local placeholder when the remote image fails
  const ImageWithFallback: React.FC<{ src?: string | null; alt?: string; width?: number; height?: number; quality?: number; style?: React.CSSProperties }> = ({ src, alt, width, height, quality, style }) => {
    const placeholder = '/sumo_logo.webp';
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
                // Accept either numeric or pre-built string ids (canonical) coming from backend
                // Compute an effective match id consistent with the seeding/ws logic above
                const canonical = getIdString(match as Record<string, unknown>, 'canonical_id', 'canonicalId') ?? buildCanonicalMatchId(match as any);
                const explicitId = getIdString(match as Record<string, unknown>, 'id', 'matchId') ?? getString(match as Record<string, unknown>, 'id', 'matchId');
                // Only allow a strict canonical numeric id for voting and websocket seeding.
                // This prevents accidentally writing votes to legacy/short ids like "1".
                const matchIdStrLocal = String(canonical || explicitId || '');
                // Use the canonical digit string directly for API calls. Avoid converting
                // to Number because very large canonical ids can lose precision or be
                // formatted in exponential notation which the backend will reject.
                const apiMatchId = (canonical && /^\d+$/.test(String(canonical))) ? String(canonical) : '';
                // Debug: log canonical vs explicit so we can diagnose why short ids appear
                try { console.debug('upcoming.matchIds', { idx, canonical, explicitId, matchIdStrLocal, apiMatchId }); } catch {}
              
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
                <ListItem key={matchIdStrLocal} sx={{ p: 0, m: 0, listStyle: 'none', position: 'relative' }}>
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
                        {westId ? (
                          <Link href={`/rikishi/${westId}`}>
                            <a style={{ textDecoration: 'none', color: 'inherit' }}>
                              <AutoFitText text={rikishi1} maxPx={18} minPx={10} sx={{ fontWeight: 600, maxWidth: '100px', textAlign: 'center' }} />
                            </a>
                          </Link>
                        ) : (
                          <AutoFitText text={rikishi1} maxPx={18} minPx={10} sx={{ fontWeight: 600, maxWidth: '100px', textAlign: 'center' }} />
                        )}
                        <AutoFitText text={rikishi1Rank || 'NA'} maxPx={14} minPx={10} sx={{ color: '#7a4b7a', fontWeight: 500, maxWidth: '120px' }} />
                        {/* slightly smaller avatar to avoid bleeding outside tab */}
                        <Box sx={{ width: 44, height: 44, mt: 0.5, overflow: 'hidden', borderRadius: '50%' }}>
                          {(() => {
                            const nestedWest = (match as Record<string, unknown>)['west_rikishi'] as Record<string, unknown> | undefined;
                            const src = getString(match, 'west_image', 'west_image_url', 'west_photo') ?? (nestedWest ? String(nestedWest['s3_mini_url'] ?? nestedWest['s3_url'] ?? nestedWest['image_url'] ?? nestedWest['pfp_url']) : null) ?? '/sumo_logo.webp';
                            const href = westId ? `/rikishi/${westId}` : undefined;
                            const img = (
                              <ImageWithFallback
                                src={src}
                                alt={`${rikishi1} profile`}
                                width={44}
                                height={44}
                                style={{ width: '100%', height: '100%', objectFit: 'cover', objectPosition: 'top', display: 'block' }}
                                quality={60}
                              />
                            );
                            return href ? (
                              <Link href={href}>
                                <a style={{ display: 'inline-block', textDecoration: 'none', color: 'inherit' }}>{img}</a>
                              </Link>
                            ) : img;
                          })()}
                        </Box>
                      </Box>

                      {/* Center: Vote area handled by a small client component to avoid making the whole list client-side */}
                      <Box sx={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 0.5, width: '38%', justifyContent: 'center' }}>
                        {(() => {
                          try {
                            // get auth from provider (HomepageClient wraps this list with AuthProvider)
                            const { user } = useAuth();
                            if (user) {
                              return (
                                <VoteControls
                                  match={match as Record<string, unknown>}
                                  matchIdStrLocal={matchIdStrLocal}
                                  apiMatchId={apiMatchId}
                                  westId={westId}
                                  eastId={eastId}
                                  onOpenLogin={onOpenLogin}
                                  aiPred={aiPred}
                                  rikishi1={rikishi1}
                                  rikishi2={rikishi2}
                                />
                              );
                            }
                          } catch (e) {
                            // if AuthContext is not available for any reason, fall through to
                            // render a sign-in fallback so anonymous visitors don't see vote UI.
                          }

                          // Anonymous view: hide vote buttons/progress and show a sign-in action.
                          return (
                            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                              <button
                                onClick={(e) => { e.stopPropagation(); if (onOpenLogin) { onOpenLogin(); } else { /* fallback: navigate to sign-in page */ window.location.href = '/auth/login'; } }}
                                style={{
                                  background: '#563861',
                                  color: '#fff',
                                  border: '2px solid rgba(0,0,0,0.06)',
                                  borderRadius: 8,
                                  padding: '8px 12px',
                                  fontWeight: 700,
                                  cursor: 'pointer',
                                }}
                                aria-label="Sign in to vote"
                              >
                                Sign in to vote
                              </button>
                            </div>
                          );
                        })()}
                      </Box>

                      <Box sx={{ minWidth: 0, display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', gap: 0.5, width: 100 }}>
                        {eastId ? (
                          <Link href={`/rikishi/${eastId}`}>
                            <a style={{ textDecoration: 'none', color: 'inherit' }}>
                              <AutoFitText text={rikishi2} maxPx={18} minPx={10} sx={{ fontWeight: 600, maxWidth: '100px', textAlign: 'center' }} />
                            </a>
                          </Link>
                        ) : (
                          <AutoFitText text={rikishi2} maxPx={18} minPx={10} sx={{ fontWeight: 600, maxWidth: '100px', textAlign: 'center' }} />
                        )}
                        <AutoFitText text={rikishi2Rank || 'NA'} maxPx={14} minPx={10} sx={{ color: '#7a4b7a', fontWeight: 500, maxWidth: '120px' }} />
                        {/* slightly smaller avatar to avoid bleeding outside tab */}
                        <Box sx={{ width: 44, height: 44, mt: 0.5, overflow: 'hidden', borderRadius: '50%' }}>
                          {(() => {
                            const nestedEast = (match as Record<string, unknown>)['east_rikishi'] as Record<string, unknown> | undefined;
                            const src = getString(match, 'east_image', 'east_image_url', 'east_photo') ?? (nestedEast ? String(nestedEast['s3_mini_url'] ?? nestedEast['s3_url'] ?? nestedEast['image_url'] ?? nestedEast['pfp_url']) : null) ?? '/sumo_logo.webp';
                            const href = eastId ? `/rikishi/${eastId}` : undefined;
                            const img = (
                              <ImageWithFallback
                                src={src}
                                alt={`${rikishi2} profile`}
                                width={44}
                                height={44}
                                style={{ width: '100%', height: '100%', objectFit: 'cover', objectPosition: 'top', display: 'block' }}
                                quality={60}
                              />
                            );
                            return href ? (
                              <Link href={href}>
                                <a style={{ display: 'inline-block', textDecoration: 'none', color: 'inherit' }}>{img}</a>
                              </Link>
                            ) : img;
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

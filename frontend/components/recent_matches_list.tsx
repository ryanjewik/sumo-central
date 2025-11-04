"use client";

import * as React from 'react';
import Avatar from '@mui/joy/Avatar';
import Box from '@mui/joy/Box';
import List from '@mui/joy/List';
import ListItem from '@mui/joy/ListItem';
import ListItemButton from '@mui/joy/ListItemButton';
import Typography from '@mui/joy/Typography';
import { ProgressBar } from "../components/base/progress-indicators/progress-indicators";

interface RecentMatchesListProps {
  date?: string;
  matches?: any[] | Record<string, any>;
}

const sampleMatches = [
  { westShikona: 'Hoshoryu', westRank: 'Sekiwake', eastShikona: 'Takakeisho', eastRank: 'Ozeki', winner: 'west' },
  { westShikona: 'Wakatakakage', westRank: 'Komusubi', eastShikona: 'Mitakeumi', eastRank: 'Sekiwake', winner: 'east' },
  { westShikona: 'Shodai', westRank: 'Maegashira 1', eastShikona: 'Meisei', eastRank: 'Maegashira 2', winner: 'west' },
  { westShikona: 'Daieisho', westRank: 'Maegashira 3', eastShikona: 'Kotonowaka', eastRank: 'Maegashira 4', winner: 'east' },
  { westShikona: 'Abi', westRank: 'Maegashira 5', eastShikona: 'Ura', eastRank: 'Maegashira 6', winner: 'west' },
  { westShikona: 'Tamawashi', westRank: 'Maegashira 7', eastShikona: 'Endo', eastRank: 'Maegashira 8', winner: 'east' },
  { westShikona: 'Tobizaru', westRank: 'Maegashira 9', eastShikona: 'Sadanoumi', eastRank: 'Maegashira 10', winner: 'west' },
  { westShikona: 'Kotoshoho', westRank: 'Maegashira 11', eastShikona: 'Chiyoshoma', eastRank: 'Maegashira 12', winner: 'east' },
  { westShikona: 'Takarafuji', westRank: 'Maegashira 13', eastShikona: 'Terutsuyoshi', eastRank: 'Maegashira 14', winner: 'west' },
  { westShikona: 'Ichiyamamoto', westRank: 'Maegashira 15', eastShikona: 'Yutakayama', eastRank: 'Maegashira 16', winner: 'east' },
  { westShikona: 'Kagayaki', westRank: 'Maegashira 17', eastShikona: 'Chiyotairyu', eastRank: 'Maegashira 18', winner: 'west' },
  { westShikona: 'Hokutofuji', westRank: 'Maegashira 19', eastShikona: 'Aoiyama', eastRank: 'Maegashira 20', winner: 'east' },
  { westShikona: 'Ryuden', westRank: 'Maegashira 21', eastShikona: 'Kotoeko', eastRank: 'Maegashira 22', winner: 'west' },
  { westShikona: 'Tochinoshin', westRank: 'Maegashira 23', eastShikona: 'Shohozan', eastRank: 'Maegashira 24', winner: 'east' },
  { westShikona: 'Chiyomaru', westRank: 'Maegashira 25', eastShikona: 'Hidenoumi', eastRank: 'Maegashira 26', winner: 'west' },
  { westShikona: 'Akiseyama', westRank: 'Maegashira 27', eastShikona: 'Tokushoryu', eastRank: 'Maegashira 28', winner: 'east' },
  { westShikona: 'Daiamami', westRank: 'Maegashira 29', eastShikona: 'Kotonowaka', eastRank: 'Maegashira 30', winner: 'west' },
  { westShikona: 'Terunofuji', westRank: 'Yokozuna', eastShikona: 'Asanoyama', eastRank: 'Ozeki', winner: 'west' },
  { westShikona: 'Takanosho', westRank: 'Maegashira 31', eastShikona: 'Wakamotoharu', eastRank: 'Maegashira 32', winner: 'east' },
  { westShikona: 'Kotoshogiku', westRank: 'Maegashira 33', eastShikona: 'Sadanoumi', eastRank: 'Maegashira 34', winner: 'west' },
];

const RecentMatchesList: React.FC<RecentMatchesListProps> = ({ date, matches }) => {
  // normalize matches input: accept array or object map
  let itemsArray: any[] = [];
  if (Array.isArray(matches) && matches.length > 0) itemsArray = matches;
  else if (matches && typeof matches === 'object') itemsArray = Object.values(matches as Record<string, any>);
  else itemsArray = sampleMatches;

  // helper to normalize fields from diverse backend shapes
  const normalize = (m: any) => {
    const eastName = m.east_shikona ?? m.eastshikona ?? m.east?.shikona ?? m.eastName ?? m.eastShikona ?? m.east?.name ?? m.east?.displayName ?? null;
    const westName = m.west_shikona ?? m.westshikona ?? m.west?.shikona ?? m.westName ?? m.westShikona ?? m.west?.name ?? m.west?.displayName ?? null;
    const eastRank = m.east_rank ?? m.eastRank ?? m.east?.rank ?? m.east?.banzuke ?? '';
    const westRank = m.west_rank ?? m.westRank ?? m.west?.rank ?? m.west?.banzuke ?? '';
    const kim = m.kimarite ?? m.kimarite_name ?? m.kimarite_display ?? m.kimariteName ?? m.method ?? undefined;
    const eastVotes = Number(m.east_votes ?? m.eastVotes ?? m.east_votes_count ?? m.east?.votes ?? 0) || 0;
    const westVotes = Number(m.west_votes ?? m.westVotes ?? m.west_votes_count ?? m.west?.votes ?? 0) || 0;
    const winner = (m.winner ?? m.result ?? m.winning_side ?? null);
    const winnerSide = (winner === 'east' || winner === 'west') ? winner : (winner === eastName ? 'east' : (winner === westName ? 'west' : null));

    // match number / bout number
    const matchNumber = Number(m.match_number ?? m.bout ?? m.bout_number ?? m.no ?? m.matchNo ?? NaN);

    // date grouping key: try ISO date, timestamp, or basho day
    let dateVal: string | null = null;
    if (m.date) dateVal = m.date;
    else if (m.timestamp) dateVal = new Date(Number(m.timestamp)).toISOString();
    else if (m.match_time) dateVal = new Date(m.match_time).toISOString();
    else if (m.basho_day) dateVal = `Day ${m.basho_day}`;
    else if (m.day) dateVal = `Day ${m.day}`;

    // friendly day label (use a deterministic YYYY-MM-DD to avoid SSR/CSR locale mismatches)
    let dayLabel = 'Unknown day';
    if (dateVal) {
      const maybeDate = new Date(dateVal);
      if (!isNaN(maybeDate.getTime())) {
        const y = maybeDate.getUTCFullYear();
        const m = String(maybeDate.getUTCMonth() + 1).padStart(2, '0');
        const d = String(maybeDate.getUTCDate()).padStart(2, '0');
        dayLabel = `${y}-${m}-${d}`;
      } else {
        dayLabel = String(dateVal);
      }
    }

    // candidate avatar fields
    const eastImage = m.east?.image ?? m.east_image ?? m.east_photo ?? m.east_profile ?? m.east?.avatar ?? null;
    const westImage = m.west?.image ?? m.west_image ?? m.west_photo ?? m.west_profile ?? m.west?.avatar ?? null;

    // build a safe fallback id only when at least one side has a name
    const constructedId = (m.east_shikona ?? m.eastName ?? m.east?.name) && (m.west_shikona ?? m.westName ?? m.west?.name)
      ? `${m.east_shikona ?? m.eastName ?? m.east?.name}-${m.west_shikona ?? m.westName ?? m.west?.name}-${m.bout ?? m.match_number ?? ''}`
      : undefined;

    return {
      raw: m,
      eastName: eastName ?? westName ?? 'East',
      westName: westName ?? eastName ?? 'West',
      eastRank,
      westRank,
      kim,
      eastVotes,
      westVotes,
      winnerSide,
      matchNumber: isNaN(matchNumber) ? undefined : matchNumber,
      dayLabel,
      timestamp: m.timestamp ? Number(m.timestamp) : (m.match_time ? Date.parse(m.match_time) : undefined),
      eastImage,
      westImage,
      id: m.id ?? m._id ?? m.match_id ?? constructedId,
    };
  };

  const normalized = itemsArray.map(normalize);

  // group by dayLabel and sort
  const groups = new Map<string, any[]>();
  normalized.forEach((nm) => {
    const key = nm.dayLabel || 'Unknown day';
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key)!.push(nm);
  });

  // convert groups to array and sort by date (if parseable) desc (most recent first)
  const groupArray = Array.from(groups.entries()).map(([label, list]) => ({ label, list }));
  groupArray.sort((a, b) => {
    const da = Date.parse(a.label) || 0;
    const db = Date.parse(b.label) || 0;
    return db - da;
  });

  // sort matches inside each group by matchNumber (ascending) or timestamp
  groupArray.forEach(g => {
    g.list.sort((x: any, y: any) => {
      if (x.matchNumber != null && y.matchNumber != null) return (x.matchNumber - y.matchNumber);
      if (x.timestamp && y.timestamp) return (x.timestamp - y.timestamp);
      return 0;
    });
  });

  return (
    <Box sx={{ display: 'flex', flexWrap: 'wrap', justifyContent: 'center', gap: 4 }}>
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
          Recent Matches
        </Typography>
        {date && (
          <Typography sx={{ color: '#563861', fontWeight: 500, fontSize: '1.08rem', mb: 2 }}>
            {date}
          </Typography>
        )}

        <List variant="outlined" sx={{ minWidth: 240, borderRadius: 'sm', p: 0, m: 0 }}>
          {groupArray.map((group, gi) => (
            <Box key={`${String(group.label ?? 'day')}-${gi}`} sx={{ mb: 2 }}>
              <Typography level="body-md" sx={{ fontWeight: 700, mb: 1, color: '#563861' }}>{group.label}</Typography>
              {group.list.map((match: any, idx: number) => {
                const eastName = match.eastName;
                const westName = match.westName;
                const eastRank = match.eastRank ?? match.eastRank;
                const westRank = match.westRank ?? match.westRank;
                const kim = match.kim;
                const eastVotes = match.eastVotes ?? 0;
                const westVotes = match.westVotes ?? 0;
                const total = Math.max(1, eastVotes + westVotes);
                const eastPercent = Math.round(((match.eastVotes ?? 0) / total) * 100);
                const westPercent = Math.round(((match.westVotes ?? 0) / total) * 100);
                const progressValue = Math.max(eastPercent, westPercent);
                const winnerSide = match.winnerSide;
                const avatarEast = match.eastImage ?? null;
                const avatarWest = match.westImage ?? null;
                const initials = (n?: string) => {
                  if (!n) return '';
                  const parts = String(n).trim().split(/\s+/);
                  if (parts.length === 1) return parts[0].slice(0, 2).toUpperCase();
                  return (parts[0][0] + (parts[1][0] ?? '')).toUpperCase();
                };
                // create a deterministic, robust key to avoid duplicates
                // include group index (gi) and item index (idx) as a final fallback so keys are always unique
                const fallbackNum = (match.matchNumber ?? match.timestamp ?? idx ?? 0);
                const safeEast = String(match.eastName ?? '').replace(/\s+/g, '') || 'E';
                const safeWest = String(match.westName ?? '').replace(/\s+/g, '') || 'W';
                const key = match.id ?? `${String(group.label ?? 'day')}-${gi}-${idx}-${String(fallbackNum)}-${safeEast}-${safeWest}`;
                return (
                  <ListItem key={key} sx={{ p: 0, m: 0, listStyle: 'none', position: 'relative' }}>
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
                        '&:hover': {
                          transform: 'scale(1.03)',
                          boxShadow: '0 4px 16px rgba(86,56,97,0.15)'
                        },
                      }}
                    >
                      <Box sx={{ position: 'relative' }}>
                        {avatarWest ? <Avatar size="sm" src={avatarWest} /> : <Avatar size="sm">{initials(westName)}</Avatar>}
                        {winnerSide === 'west' && (
                          <Box
                            sx={{
                              pointerEvents: 'none',
                              position: 'absolute',
                              top: -8,
                              right: -8,
                              background: '#22c55e',
                              color: '#fff',
                              borderRadius: '50%',
                              width: 17,
                              height: 17,
                              display: 'flex',
                              alignItems: 'center',
                              justifyContent: 'center',
                              fontSize: 9,
                              boxShadow: '0 1px 4px rgba(0,0,0,0.12)',
                            }}
                          >
                            W
                          </Box>
                        )}
                      </Box>

                      <Box sx={{ textAlign: 'center', flex: 1 }}>
                        <Typography
                          className="app-text"
                          level="body-md"
                          sx={{
                            fontWeight: 500,
                            fontSize: 'clamp(0.95rem, 2vw, 1.15rem)',
                            whiteSpace: 'nowrap',
                            overflow: 'hidden',
                            textOverflow: 'ellipsis',
                            maxWidth: '100%'
                          }}
                        >
                          {westName} vs {eastName}
                        </Typography>
                        <Typography
                          className="app-text"
                          level="body-xs"
                          sx={{
                            color: 'text.secondary',
                            fontSize: 'clamp(0.8rem, 1.5vw, 1rem)',
                            whiteSpace: 'nowrap',
                            overflow: 'hidden',
                            textOverflow: 'ellipsis',
                            maxWidth: '100%'
                          }}
                        >
                          {match.westRank ?? ''} vs {match.eastRank ?? ''}
                        </Typography>
                        <Box sx={{ display: 'flex', justifyContent: 'center', mt: 1 }}>
                          <Box sx={{ width: 160 }}>
                            <ProgressBar value={progressValue} className="bg-red-500" progressClassName="bg-blue-500" />
                          </Box>
                        </Box>
                      </Box>

                      <Box sx={{ position: 'relative' }}>
                        {avatarEast ? <Avatar size="sm" src={avatarEast} /> : <Avatar size="sm">{initials(eastName)}</Avatar>}
                        {winnerSide === 'east' && (
                          <Box
                            sx={{
                              pointerEvents: 'none',
                              position: 'absolute',
                              top: -8,
                              right: -8,
                              background: '#22c55e',
                              color: '#fff',
                              borderRadius: '50%',
                              width: 17,
                              height: 17,
                              display: 'flex',
                              alignItems: 'center',
                              justifyContent: 'center',
                              fontSize: 9,
                              fontWeight: 700,
                              boxShadow: '0 1px 4px rgba(0,0,0,0.12)',
                            }}
                          >
                            W
                          </Box>
                        )}
                      </Box>
                      {kim && <div style={{ position: 'absolute', left: 8, bottom: 8, fontSize: 11, color: '#563861' }}>{kim}</div>}
                    </ListItemButton>
                  </ListItem>
                );
              })}
            </Box>
          ))}
        </List>
      </Box>
    </Box>
  );
}

export default RecentMatchesList;

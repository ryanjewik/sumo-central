"use client";

import * as React from 'react';
import Avatar from '@mui/joy/Avatar';
import Image from 'next/image';
import Link from 'next/link';
import Box from '@mui/joy/Box';
import List from '@mui/joy/List';
import ListItem from '@mui/joy/ListItem';
import ListItemButton from '@mui/joy/ListItemButton';
import Typography from '@mui/joy/Typography';
import { ProgressBar } from "../components/base/progress-indicators/progress-indicators";

interface RecentMatchesListProps {
  date?: string;
  matches?: unknown[] | Record<string, unknown>;
}

type RawMatch = Record<string, unknown>;

type NormalizedMatch = {
  raw: RawMatch;
  eastName: string;
  westName: string;
  eastRank: string;
  westRank: string;
  kim?: string;
  eastVotes: number;
  westVotes: number;
  winnerSide?: string | null;
  matchNumber?: number;
  dayLabel?: string;
  timestamp?: number;
  eastImage?: string | null;
  westImage?: string | null;
  id?: string | number;
  eastId?: string | number | null;
  westId?: string | number | null;
};

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
  let itemsArray: RawMatch[] = [];
  if (Array.isArray(matches) && matches.length > 0) itemsArray = matches as RawMatch[];
  else if (matches && typeof matches === 'object') itemsArray = Object.values(matches as Record<string, unknown>) as RawMatch[];
  else itemsArray = sampleMatches as RawMatch[];

  // helper to normalize fields from diverse backend shapes
  const normalize = (m: RawMatch): NormalizedMatch => {
    const getString = (o: RawMatch | undefined, ...keys: string[]) => {
      if (!o) return undefined;
      for (const k of keys) {
        const v = (o as RawMatch)[k];
        if (typeof v === 'string') return v;
        if (typeof v === 'number') return String(v);
      }
      return undefined;
    };

    const getNumber = (o: RawMatch | undefined, ...keys: string[]) => {
      const s = getString(o, ...keys);
      if (typeof s === 'string') return Number(s) || 0;
      return 0;
    };

  const eastName = getString(m, 'east_shikona', 'eastshikona', 'eastName') ?? ((m['east'] && typeof m['east'] === 'object') ? getString(m['east'] as RawMatch, 'shikona', 'name', 'displayName') : undefined) ?? ((m['east_rikishi'] && typeof m['east_rikishi'] === 'object') ? getString(m['east_rikishi'] as RawMatch, 'shikona', 'name', 'displayName') : undefined);
  const westName = getString(m, 'west_shikona', 'westshikona', 'westName') ?? ((m['west'] && typeof m['west'] === 'object') ? getString(m['west'] as RawMatch, 'shikona', 'name', 'displayName') : undefined) ?? ((m['west_rikishi'] && typeof m['west_rikishi'] === 'object') ? getString(m['west_rikishi'] as RawMatch, 'shikona', 'name', 'displayName') : undefined);
  const eastRank = getString(m, 'east_rank', 'eastRank') ?? ((m['east'] && typeof m['east'] === 'object') ? getString(m['east'] as RawMatch, 'rank', 'banzuke') : '') ?? ((m['east_rikishi'] && typeof m['east_rikishi'] === 'object') ? getString(m['east_rikishi'] as RawMatch, 'current_rank', 'rank', 'banzuke') : '') ?? '';
  const westRank = getString(m, 'west_rank', 'westRank') ?? ((m['west'] && typeof m['west'] === 'object') ? getString(m['west'] as RawMatch, 'rank', 'banzuke') : '') ?? ((m['west_rikishi'] && typeof m['west_rikishi'] === 'object') ? getString(m['west_rikishi'] as RawMatch, 'current_rank', 'rank', 'banzuke') : '') ?? '';
    const kim = getString(m, 'kimarite', 'kimarite_name', 'kimarite_display', 'kimariteName', 'method');
    const eastVotes = getNumber(m, 'east_votes', 'eastVotes', 'east_votes_count') || ((m['east'] && typeof m['east'] === 'object') ? getNumber(m['east'] as RawMatch, 'votes') : 0);
    const westVotes = getNumber(m, 'west_votes', 'westVotes', 'west_votes_count') || ((m['west'] && typeof m['west'] === 'object') ? getNumber(m['west'] as RawMatch, 'votes') : 0);
    const winner = (m['winner'] ?? m['result'] ?? m['winning_side'] ?? null) as unknown;
    // determine winner by id when possible: compare numeric/string winner to east/west rikishi id fields
    let winnerSide: string | null = null;
    try {
      const rawWinner = winner;
      const eastIds = [m['east_rikishi_id'], m['eastId'], m['east_id'], (m['east'] && typeof m['east'] === 'object') ? (m['east'] as RawMatch)['id'] : undefined];
      const westIds = [m['west_rikishi_id'], m['westId'], m['west_id'], (m['west'] && typeof m['west'] === 'object') ? (m['west'] as RawMatch)['id'] : undefined];
      const normalizeId = (v: unknown) => {
        if (v === null || v === undefined) return null;
        if (typeof v === 'number') return String(v);
        if (typeof v === 'string') return v;
        try { return String(v); } catch { return null; }
      };
      const wNorm = normalizeId(rawWinner);
      if (wNorm !== null) {
        for (const eid of eastIds) {
          if (normalizeId(eid) === wNorm) {
            winnerSide = 'east';
            break;
          }
        }
      }
      if (!winnerSide && wNorm !== null) {
        for (const wid of westIds) {
          if (normalizeId(wid) === wNorm) {
            winnerSide = 'west';
            break;
          }
        }
      }
      // fallback: preserve previous behavior (string name or 'east'/'west')
      if (!winnerSide) {
        if (rawWinner === 'east' || rawWinner === 'west') winnerSide = String(rawWinner);
        else if (String(rawWinner) === String(eastName)) winnerSide = 'east';
        else if (String(rawWinner) === String(westName)) winnerSide = 'west';
        else winnerSide = null;
      }
    } catch (e) {
      winnerSide = null;
    }

    // match number / bout number
    const matchNumber = Number(m['match_number'] ?? m['bout'] ?? m['bout_number'] ?? m['no'] ?? m['matchNo'] ?? NaN);

    // date grouping key: try ISO date, timestamp, or basho day
    let dateVal: string | null = null;
    if (m['date']) dateVal = String(m['date']);
    else if (m['timestamp']) dateVal = new Date(Number(m['timestamp'])).toISOString();
    else if (m['match_time']) dateVal = new Date(String(m['match_time'])).toISOString();
    else if (m['basho_day']) dateVal = `Day ${String(m['basho_day'])}`;
    else if (m['day']) dateVal = `Day ${String(m['day'])}`;

    // friendly day label (use a deterministic YYYY-MM-DD to avoid SSR/CSR locale mismatches)
    let dayLabel = 'Unknown day';
    if (dateVal) {
      const maybeDate = new Date(dateVal);
      if (!isNaN(maybeDate.getTime())) {
        const y = maybeDate.getUTCFullYear();
        const mm = String(maybeDate.getUTCMonth() + 1).padStart(2, '0');
        const d = String(maybeDate.getUTCDate()).padStart(2, '0');
        dayLabel = `${y}-${mm}-${d}`;
      } else {
        dayLabel = String(dateVal);
      }
    }

    // candidate avatar fields
  const eastImage = getString(m['east'] && typeof m['east'] === 'object' ? (m['east'] as RawMatch) : undefined, 'image', 'avatar') ?? getString(m, 'east_image', 'east_photo', 'east_profile') ?? ((m['east_rikishi'] && typeof m['east_rikishi'] === 'object') ? getString(m['east_rikishi'] as RawMatch, 's3_url', 's3', 'image', 'pfp_url', 'image_url') : null) ?? null;
  const westImage = getString(m['west'] && typeof m['west'] === 'object' ? (m['west'] as RawMatch) : undefined, 'image', 'avatar') ?? getString(m, 'west_image', 'west_photo', 'west_profile') ?? ((m['west_rikishi'] && typeof m['west_rikishi'] === 'object') ? getString(m['west_rikishi'] as RawMatch, 's3_url', 's3', 'image', 'pfp_url', 'image_url') : null) ?? null;

  // extract rikishi ids when available so we can link shikonas/pfps
  const eastId = (m['east_rikishi_id'] ?? m['eastId'] ?? m['east_id'] ?? (m['east'] && typeof m['east'] === 'object' ? (m['east'] as RawMatch)['id'] : undefined)) as string | number | undefined;
  const westId = (m['west_rikishi_id'] ?? m['westId'] ?? m['west_id'] ?? (m['west'] && typeof m['west'] === 'object' ? (m['west'] as RawMatch)['id'] : undefined)) as string | number | undefined;

    // build a safe fallback id only when at least one side has a name
    const constructedId = (m['east_shikona'] ?? m['eastName'] ?? (m['east'] && (m['east'] as RawMatch)['name'])) && (m['west_shikona'] ?? m['westName'] ?? (m['west'] && (m['west'] as RawMatch)['name']))
      ? `${m['east_shikona'] ?? m['eastName'] ?? (m['east'] && (m['east'] as RawMatch)['name'])}-${m['west_shikona'] ?? m['westName'] ?? (m['west'] && (m['west'] as RawMatch)['name'])}-${m['bout'] ?? m['match_number'] ?? ''}`
      : undefined;

    const resolvedId = (() => {
      const v = m['id'] ?? m['_id'] ?? m['match_id'] ?? constructedId;
      if (typeof v === 'number' || typeof v === 'string') return v;
      if (v != null) return String(v);
      return undefined;
    })();

    return {
      raw: m,
      eastName: eastName ?? westName ?? 'East',
      westName: westName ?? eastName ?? 'West',
      eastRank,
      westRank,
      kim: kim ?? undefined,
      eastVotes,
      westVotes,
      winnerSide,
      matchNumber: isNaN(matchNumber) ? undefined : matchNumber,
      dayLabel,
      timestamp: m['timestamp'] ? Number(m['timestamp']) : (m['match_time'] ? Date.parse(String(m['match_time'])) : undefined),
      eastImage,
      westImage,
      id: resolvedId,
      eastId: eastId ?? null,
      westId: westId ?? null,
    };
  };

  const normalized = itemsArray.map(normalize);

  // If no explicit `date` prop is provided, try to resolve a single date for these recent matches
  // by fetching the basho document using basho_id found on the match objects. The basho document
  // is expected at /api/basho_pages/:bashoId and to contain `basho.start_date`.
  const [resolvedDate, setResolvedDate] = React.useState<string | null>(null);
  const serializedNormalized = React.useMemo(() => JSON.stringify(normalized), [normalized]);

  React.useEffect(() => {
    // If a caller provided an explicit date prop, prefer that (keeps backwards compatibility).
    if (date) return;
    if (!normalized || normalized.length === 0) return;

    // Find the first match that contains both a basho id and a day number.
    let found: { bashoId: string; dayNum: number } | null = null;
    for (const nm of normalized) {
      const r = nm.raw as RawMatch;
      const maybeBasho = r['basho_id'] ?? (r['basho'] && (r['basho'] as any)['basho_id']) ?? r['bashoId'] ?? ((r['basho'] as any)?.id) ?? r['basho'] ?? null;
      const maybeDay = r['day'] ?? r['basho_day'] ?? r['day_number'] ?? null;
      if (maybeBasho != null && maybeDay != null) {
        found = { bashoId: String(maybeBasho), dayNum: Number(maybeDay) || 1 };
        break;
      }
    }
    if (!found) return;

    const { bashoId, dayNum } = found;
    let aborted = false;

    (async () => {
      try {
        // Try both known endpoints that may contain the basho document
        const endpoints = [`/api/basho_pages/${encodeURIComponent(bashoId)}`, `/api/basho/${encodeURIComponent(bashoId)}`];
        let startRaw: unknown = null;
        for (const ep of endpoints) {
          try {
            const res = await fetch(ep);
            if (!res.ok) continue;
            const j = await res.json();
            // prefer nested j.basho.start_date, then j.start_date, then j.basho.start
            startRaw = (j && ((j.basho && (j.basho.start_date ?? (j.basho.start))) ?? j.start_date ?? j.start)) ?? null;
            if (startRaw) break;
          } catch (err) {
            // try next endpoint
            continue;
          }
        }
        if (!startRaw) return;

        // Parse start date as UTC (avoid local timezone shifting). Ensure it has a time component.
        const startStr = String(startRaw);
        // If the date is a plain yyyy-mm-dd, append T00:00:00Z to force UTC parsing
        const iso = /^[0-9]{4}-[0-9]{2}-[0-9]{2}$/.test(startStr) ? `${startStr}T00:00:00Z` : startStr;
        const sd = new Date(iso);
        if (isNaN(sd.getTime())) return;

        // Compute match date: start_date + (dayNum - 1) days
        const matchDate = new Date(sd);
        matchDate.setUTCDate(sd.getUTCDate() + Math.max(0, (dayNum - 1)));
        const y = matchDate.getUTCFullYear();
        const mm = String(matchDate.getUTCMonth() + 1).padStart(2, '0');
        const dd = String(matchDate.getUTCDate()).padStart(2, '0');
        if (!aborted) setResolvedDate(`${y}-${mm}-${dd}`);
      } catch (e) {
        // ignore and leave resolvedDate null
      }
    })();

    return () => { aborted = true; };
  }, [date, serializedNormalized]);

  // If we were able to resolve a single date for this set of recent matches (either
  // passed via the `date` prop or computed via the basho lookup into `resolvedDate`)
  // prefer showing a single unified header and collapse all matches into one group so
  // we don't print duplicate or conflicting day labels per-row.
  const headerDate = React.useMemo(() => {
    if (resolvedDate) return resolvedDate;
    if (date) {
      // trim to YYYY-MM-DD if a full ISO is passed
      let display = String(date);
      if (display.includes('T')) display = display.split('T')[0];
      else if (display.includes(' ')) display = display.split(' ')[0];
      else if (display.length > 10) display = display.slice(0, 10);
      return display;
    }
    return null;
  }, [resolvedDate, date]);

  let groupArray: { label: string; list: NormalizedMatch[] }[] = [];
  if (headerDate) {
    // collapse everything into a single labeled group (avoids double/incorrect dates)
    groupArray = [{ label: headerDate, list: normalized }];
  } else {
    // group by dayLabel and sort when no single header date is available
    const groups = new Map<string, NormalizedMatch[]>();
    normalized.forEach((nm) => {
      const key = nm.dayLabel || 'Unknown day';
      if (!groups.has(key)) groups.set(key, []);
      groups.get(key)!.push(nm);
    });

    // convert groups to array and sort by date (if parseable) desc (most recent first)
    groupArray = Array.from(groups.entries()).map(([label, list]) => ({ label, list }));
    groupArray.sort((a, b) => {
      const da = Date.parse(a.label) || 0;
      const db = Date.parse(b.label) || 0;
      return db - da;
    });
  }

  // sort matches inside each group by matchNumber (ascending) or timestamp
  groupArray.forEach(g => {
    g.list.sort((x: NormalizedMatch, y: NormalizedMatch) => {
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
        {(resolvedDate || date) && (
          <Typography sx={{ color: '#563861', fontWeight: 500, fontSize: '1.08rem', mb: 2, fontFamily: 'inherit' }}>
            {resolvedDate || date}
          </Typography>
        )}

        <List variant="outlined" sx={{ minWidth: 240, borderRadius: 'sm', p: 0, m: 0 }}>
          {groupArray.map((group, gi) => (
            <Box key={`${String(group.label ?? 'day')}-${gi}`} sx={{ mb: 2 }}>
              {/* group.label removed when a unified header date exists to avoid duplicate dates */}
              {group.list.map((match: NormalizedMatch, idx: number) => {
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
                const rawAvatarEast: unknown = match.eastImage ?? (match.raw && (match.raw['east_rikishi'] as RawMatch)?.s3_url) ?? (match.raw && (match.raw['east_rikishi'] as RawMatch)?.image_url) ?? null;
                const rawAvatarWest: unknown = match.westImage ?? (match.raw && (match.raw['west_rikishi'] as RawMatch)?.s3_url) ?? (match.raw && (match.raw['west_rikishi'] as RawMatch)?.image_url) ?? null;
                const avatarEast: string | null = (typeof rawAvatarEast === 'string' || typeof rawAvatarEast === 'number') ? String(rawAvatarEast) : null;
                const avatarWest: string | null = (typeof rawAvatarWest === 'string' || typeof rawAvatarWest === 'number') ? String(rawAvatarWest) : null;
                const westHref = match.westId ? `/rikishi/${match.westId}` : undefined;
                const eastHref = match.eastId ? `/rikishi/${match.eastId}` : undefined;
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
                        // highlight winner side with a colored vertical accent
                        borderLeft: winnerSide === 'west' ? '6px solid #2563eb' : undefined,
                        borderRight: winnerSide === 'east' ? '6px solid #ef4444' : undefined,
                        boxShadow: winnerSide ? (winnerSide === 'west' ? '0 8px 28px rgba(37,99,235,0.08)' : '0 8px 28px rgba(239,68,68,0.08)') : undefined,
                      }}
                    >
                      {/* West avatar: winner gets a gold glowing outline and trophy, loser gets shadowed black outline */}
                      <Box sx={{ position: 'relative', width: 40, height: 40 }}>
                        {
                          (() => {
                            const wrapperStyle: React.CSSProperties = {
                              width: 40,
                              height: 40,
                              position: 'relative',
                              borderRadius: '50%',
                              overflow: 'hidden',
                              display: 'inline-block',
                            };
                            if (winnerSide === 'west') {
                              // gold glow
                              (wrapperStyle as any).boxShadow = '0 0 0 3px rgba(255,215,0,0.95), 0 8px 24px rgba(255,215,0,0.18)';
                              (wrapperStyle as any).border = '2px solid rgba(255,215,0,0.65)';
                            } else if (winnerSide && winnerSide !== 'west') {
                              // shadowed black outline for loser
                              (wrapperStyle as any).boxShadow = '0 0 0 3px rgba(0,0,0,0.6), 0 4px 10px rgba(0,0,0,0.35)';
                              (wrapperStyle as any).border = '1px solid rgba(0,0,0,0.6)';
                            }
                            if (avatarWest) {
                              const content = (
                                <div style={wrapperStyle}>
                                  <Image src={avatarWest} alt={westName} fill style={{ objectFit: 'cover', objectPosition: 'top', borderRadius: '50%' }} />
                                  {winnerSide === 'west' && (
                                    <Box sx={{ position: 'absolute', top: -6, right: -6, background: 'gold', color: '#222', borderRadius: '50%', width: 22, height: 22, display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 12, boxShadow: '0 2px 6px rgba(0,0,0,0.2)' }}>
                                      <span style={{ lineHeight: 1 }}>🏆</span>
                                    </Box>
                                  )}
                                </div>
                              );
                              return westHref ? (
                                <Link href={westHref}>
                                  <a style={{ display: 'inline-block', textDecoration: 'none', color: 'inherit' }}>{content}</a>
                                </Link>
                              ) : content;
                            }
                            return (
                              <div style={wrapperStyle}>
                                <Avatar size="sm" sx={{ width: 40, height: 40 }}>{initials(westName)}</Avatar>
                                {winnerSide === 'west' && (
                                  <Box sx={{ position: 'absolute', top: -6, right: -6, background: 'gold', color: '#222', borderRadius: '50%', width: 22, height: 22, display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 12, boxShadow: '0 2px 6px rgba(0,0,0,0.2)' }}>
                                    <span style={{ lineHeight: 1 }}>🏆</span>
                                  </Box>
                                )}
                              </div>
                            );
                          })()
                        }
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
                          {match.westId ? (
                            <Link href={`/rikishi/${match.westId}`}>
                              <a style={{ textDecoration: 'none', color: 'inherit', fontWeight: 700 }}>{westName}</a>
                            </Link>
                          ) : (
                            <span style={{ fontWeight: 700 }}>{westName}</span>
                          )} {' '}vs{' '}
                          {match.eastId ? (
                            <Link href={`/rikishi/${match.eastId}`}>
                              <a style={{ textDecoration: 'none', color: 'inherit', fontWeight: 700 }}>{eastName}</a>
                            </Link>
                          ) : (
                            <span style={{ fontWeight: 700 }}>{eastName}</span>
                          )}
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
                          {westRank ?? ''} vs {eastRank ?? ''}
                        </Typography>
                        <Box sx={{ display: 'flex', justifyContent: 'center', mt: 1 }}>
                          <Box sx={{ width: 160 }}>
                            <ProgressBar value={progressValue} className="bg-red-500" progressClassName="bg-blue-500" />
                          </Box>
                        </Box>
                      </Box>

                      {/* East avatar: apply winner/loser outlines similarly */}
                      <Box sx={{ position: 'relative', width: 40, height: 40 }}>
                        {
                          (() => {
                            const wrapperStyle: React.CSSProperties = {
                              width: 40,
                              height: 40,
                              position: 'relative',
                              borderRadius: '50%',
                              overflow: 'hidden',
                              display: 'inline-block',
                            };
                            if (winnerSide === 'east') {
                              (wrapperStyle as any).boxShadow = '0 0 0 3px rgba(255,215,0,0.95), 0 8px 24px rgba(255,215,0,0.18)';
                              (wrapperStyle as any).border = '2px solid rgba(255,215,0,0.65)';
                            } else if (winnerSide && winnerSide !== 'east') {
                              (wrapperStyle as any).boxShadow = '0 0 0 3px rgba(0,0,0,0.6), 0 4px 10px rgba(0,0,0,0.35)';
                              (wrapperStyle as any).border = '1px solid rgba(0,0,0,0.6)';
                            }
                            if (avatarEast) {
                              const content = (
                                <div style={wrapperStyle}>
                                  <Image src={avatarEast} alt={eastName} fill style={{ objectFit: 'cover', objectPosition: 'top', borderRadius: '50%' }} />
                                  {winnerSide === 'east' && (
                                    <Box sx={{ position: 'absolute', top: -6, right: -6, background: 'gold', color: '#222', borderRadius: '50%', width: 22, height: 22, display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 12, boxShadow: '0 2px 6px rgba(0,0,0,0.2)' }}>
                                      <span style={{ lineHeight: 1 }}>🏆</span>
                                    </Box>
                                  )}
                                </div>
                              );
                              return eastHref ? (
                                <Link href={eastHref}>
                                  <a style={{ display: 'inline-block', textDecoration: 'none', color: 'inherit' }}>{content}</a>
                                </Link>
                              ) : content;
                            }
                            return (
                              <div style={wrapperStyle}>
                                <Avatar size="sm" sx={{ width: 40, height: 40 }}>{initials(eastName)}</Avatar>
                                {winnerSide === 'east' && (
                                  <Box sx={{ position: 'absolute', top: -6, right: -6, background: 'gold', color: '#222', borderRadius: '50%', width: 22, height: 22, display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 12, boxShadow: '0 2px 6px rgba(0,0,0,0.2)' }}>
                                    <span style={{ lineHeight: 1 }}>🏆</span>
                                  </Box>
                                )}
                              </div>
                            );
                          })()
                        }
                      </Box>
                      {/* kimarite removed per UX: no need to display method text */}
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

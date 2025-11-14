"use client";

import React, { useEffect, useMemo, useState } from "react";
import { useParams } from "next/navigation";
import RikishiWinLossSparkline from '../../../components/sparkline';
import KimariteRadarChart from '../../../components/application/charts/KimariteRadarChart';
import { fetchWithAuth } from '../../../lib/auth';

// Shared Tailwind classes for table headers/cells (keeps consistent spacing & font)
// Slightly larger padding and base font for better legibility and to fill space
// include box-border to keep header/cell alignment consistent across browsers
const SHARED_TH_CLASS = 'text-left px-4 py-3 border-b font-semibold text-base box-border';
const SHARED_TD_CLASS = 'px-4 py-3 border-b align-top text-base box-border';

function SectionToggle({ title, defaultOpen = false, children }: { title: string; defaultOpen?: boolean; children: React.ReactNode }) {
  const [open, setOpen] = useState(defaultOpen);
  return (
    <div style={{ marginTop: '1rem', border: '1px solid #e6e6e6', borderRadius: 8, overflow: 'hidden' }}>
      <button
        onClick={() => setOpen((s) => !s)}
        style={{ width: '100%', textAlign: 'left', padding: '0.6rem 0.8rem', background: '#fafafa', border: 'none', cursor: 'pointer' }}
      >
        <strong style={{ marginRight: 8 }}>{open ? '▼' : '▶'}</strong>
        <span className="app-text">{title}</span>
      </button>
      {open && <div style={{ padding: '0.75rem' }}>{children}</div>}
    </div>
  );
}

function renderKeyValue(obj: any) {
  if (!obj || typeof obj !== 'object') return <span>{String(obj)}</span>;
  return (
    <table className="w-full border-collapse">
      <tbody>
        {Object.entries(obj).map(([k, v]) => (
          <tr key={k}>
            <td className={`${SHARED_TD_CLASS} w-56 font-semibold`}>{k}</td>
            <td className={SHARED_TD_CLASS}>{typeof v === 'object' ? <pre style={{ margin: 0 }}>{JSON.stringify(v, null, 2)}</pre> : String(v)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function renderArrayTable(arr: any[]) {
  if (!Array.isArray(arr) || arr.length === 0) return <div className="app-text">No entries</div>;
  // Derive headers (ensure string typing)
  const headers = Array.from(
    arr.reduce((set: Set<string>, item: any) => {
      if (item && typeof item === 'object') Object.keys(item).forEach((k) => set.add(k));
      return set;
    }, new Set<string>())
  ) as string[];

  return (
    <div className="overflow-x-auto">
      <table className="w-full border-collapse">
        <thead>
          <tr>
            {headers.map((h: string) => (
              <th key={h} className={SHARED_TH_CLASS}>{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {arr.map((item, idx) => (
            <tr key={idx}>
              {headers.map((h: string) => (
                <td key={`${idx}-${h}`} className={SHARED_TD_CLASS}>
                  {item && typeof item === 'object' ? (((item as any)[h] !== undefined) ? String((item as any)[h]) : '') : String(item)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function normalizeObjectMapToArray(obj: any) {
  const rows: any[] = [];
  if (!obj) return rows;
  if (Array.isArray(obj)) return obj.slice();
  if (typeof obj === 'object') {
    Object.entries(obj).forEach(([k, v]) => {
      if (v && typeof v === 'object') rows.push(Object.assign({ _mapKey: k }, v));
    });
  }
  return rows;
}

function renderRankHistory(h: any) {
  const rows = normalizeObjectMapToArray(h);
  if (!rows || rows.length === 0) return <div className="app-text">No rank history</div>;
  const normalized = rows.map((r: any) => ({
    date: r.rank_date ?? (typeof r._mapKey === 'string' ? r._mapKey.slice(0, 10) : undefined),
    rank: r.rank_name ?? r.rank ?? r.rank_value ?? undefined,
  }));
  normalized.sort((a: any, b: any) => (a.date ?? '') < (b.date ?? '') ? 1 : -1);
  return (
    <div className="overflow-x-auto">
      <table className="w-full border-collapse">
        <thead>
          <tr>
            <th className={`${SHARED_TH_CLASS} bg-[#eaf7ef] border-b-2 text-[#042f21]`}>date</th>
            <th className={`${SHARED_TH_CLASS} bg-[#eaf7ef] border-b-2 text-[#042f21]`}>rank</th>
          </tr>
        </thead>
        <tbody>
          {normalized.map((r: any, i: number) => (
            <tr key={i}>
              <td className={SHARED_TD_CLASS}>{r.date ?? ''}</td>
              <td className={SHARED_TD_CLASS}>{r.rank ?? ''}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function renderMeasurementsHistory(h: any) {
  const rows = normalizeObjectMapToArray(h);
  if (!rows || rows.length === 0) return <div className="app-text">No measurements</div>;
  const normalized = rows.map((r: any) => ({
    date: r.measurement_date ?? r._mapKey?.slice?.(0,10) ?? r.start_date ?? undefined,
    height: r.height_cm ?? r.height ?? r.current_height ?? undefined,
    weight: r.weight_kg ?? r.weight ?? r.current_weight ?? undefined,
  }));
  normalized.sort((a: any, b: any) => (a.date ?? '') < (b.date ?? '') ? 1 : -1);

  return (
    <div className="overflow-x-auto">
      <table className="w-full border-collapse">
        <thead>
          <tr>
            <th className={`${SHARED_TH_CLASS} bg-[#eaf7ef] border-b-2 text-[#042f21]`}>date</th>
            <th className={`${SHARED_TH_CLASS} bg-[#eaf7ef] border-b-2 text-[#042f21]`}>height (cm)</th>
            <th className={`${SHARED_TH_CLASS} bg-[#eaf7ef] border-b-2 text-[#042f21]`}>weight (kg)</th>
          </tr>
        </thead>
        <tbody>
          {normalized.map((r: any, i: number) => (
            <tr key={i}>
              <td className={SHARED_TD_CLASS}>{r.date ?? ''}</td>
              <td className={SHARED_TD_CLASS}>{r.height ?? ''}</td>
              <td className={SHARED_TD_CLASS}>{r.weight ?? ''}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function renderShikonaChanges(h: any) {
  const rows = normalizeObjectMapToArray(h);
  if (!rows || rows.length === 0) return <div className="app-text">No shikona changes</div>;
  const normalized = rows.map((r: any) => ({
    date: r.change_date ?? (typeof r._mapKey === 'string' ? r._mapKey.slice(0, 10) : undefined),
    shikona: r.shikona ?? r.name ?? r.new_shikona ?? '',
  }));
  normalized.sort((a: any, b: any) => (a.date ?? '') < (b.date ?? '') ? 1 : -1);
  return (
    <div className="overflow-x-auto">
      <table className="w-full border-collapse">
        <thead>
          <tr>
            <th className={`${SHARED_TH_CLASS} bg-[#eaf7ef] border-b-2 text-[#042f21]`}>date</th>
            <th className={`${SHARED_TH_CLASS} bg-[#eaf7ef] border-b-2 text-[#042f21]`}>shikona</th>
          </tr>
        </thead>
        <tbody>
          {normalized.map((r: any, i: number) => (
            <tr key={i}>
              <td className={SHARED_TD_CLASS}>{r.date ?? ''}</td>
              <td className={`${SHARED_TD_CLASS} font-bold`}>{r.shikona ?? ''}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function renderSpecialPrizes(h: any) {
  const rows = normalizeObjectMapToArray(h);
  if (!rows || rows.length === 0) return <div className="app-text">No special prizes</div>;
  const normalized = rows.map((r: any) => ({
    date: r.end_date ?? (typeof r._mapKey === 'string' ? r._mapKey.slice(0, 10) : undefined),
    prize_name: r.prize_name ?? r.prize ?? r.name ?? '',
    basho_id: r.basho_id ?? r.basho ?? r.bashoId ?? '',
    location: r.location ?? '',
  }));
  normalized.sort((a: any, b: any) => (a.date ?? '') < (b.date ?? '') ? 1 : -1);
  return (
    <div className="overflow-x-auto">
      <table className="w-full border-collapse">
        <thead>
          <tr>
            <th className={`${SHARED_TH_CLASS} bg-[#eaf7ef] border-b-2 text-[#042f21]`}>date</th>
            <th className={`${SHARED_TH_CLASS} bg-[#eaf7ef] border-b-2 text-[#042f21]`}>prize</th>
            <th className={`${SHARED_TH_CLASS} bg-[#eaf7ef] border-b-2 text-[#042f21]`}>basho_id</th>
            <th className={`${SHARED_TH_CLASS} bg-[#eaf7ef] border-b-2 text-[#042f21]`}>location</th>
          </tr>
        </thead>
        <tbody>
          {normalized.map((r: any, i: number) => (
            <tr key={i}>
              <td className={SHARED_TD_CLASS}>{r.date ?? ''}</td>
              <td className={`${SHARED_TD_CLASS} font-bold`}>{r.prize_name ?? ''}</td>
              <td className={SHARED_TD_CLASS}>{r.basho_id ?? ''}</td>
              <td className={SHARED_TD_CLASS}>{r.location ?? ''}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function renderDivisionWins(h: any) {
  const rows = normalizeObjectMapToArray(h);
  if (!rows || rows.length === 0) return <div className="app-text">No division wins recorded</div>;
  const normalized = rows.map((r: any) => {
    let division = r.division_won ?? r.division ?? r.title ?? '';
    if (typeof division === 'string') division = division.replace(/_yusho$/i, '');
    return {
      date: r.end_date ?? (typeof r._mapKey === 'string' ? r._mapKey.slice(0, 10) : undefined),
      division: division,
      basho_id: r.basho_id ?? r.basho ?? r.bashoId ?? '',
      location: r.location ?? '',
    };
  });
  normalized.sort((a: any, b: any) => (a.date ?? '') < (b.date ?? '') ? 1 : -1);

  return (
    <div className="overflow-x-auto">
      <table className="w-full border-collapse">
        <thead>
          <tr>
            <th className={`${SHARED_TH_CLASS} bg-[#eaf7ef] border-b-2 text-[#042f21]`}>date</th>
            <th className={`${SHARED_TH_CLASS} bg-[#eaf7ef] border-b-2 text-[#042f21]`}>division</th>
            <th className={`${SHARED_TH_CLASS} bg-[#eaf7ef] border-b-2 text-[#042f21]`}>basho_id</th>
            <th className={`${SHARED_TH_CLASS} bg-[#eaf7ef] border-b-2 text-[#042f21]`}>location</th>
          </tr>
        </thead>
        <tbody>
          {normalized.map((r: any, i: number) => (
            <tr key={i}>
              <td className={SHARED_TD_CLASS}>{r.date ?? ''}</td>
              <td className={`${SHARED_TD_CLASS} font-bold`}>{r.division ?? ''}</td>
              <td className={SHARED_TD_CLASS}>{r.basho_id ?? ''}</td>
              <td className={SHARED_TD_CLASS}>{r.location ?? ''}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function renderMatchesTable(matches: any, rikishiId?: any) {
  // normalize matches which can be an array or an object map with keys that include date prefix
  const rows: any[] = [];

  if (!matches) return <div className="app-text">No matches</div>;

  if (Array.isArray(matches)) {
    matches.forEach((m: any) => rows.push(m));
  } else if (typeof matches === 'object') {
    Object.entries(matches).forEach(([key, val]) => {
      if (val && typeof val === 'object') {
        // copy the object and attach the key so we can extract date if needed
        rows.push(Object.assign({ _mapKey: key }, val));
      }
    });
  }

  // Extract date for sorting: prefer start_date, else first 10 chars of _mapKey
  const normalized = rows.map((r: any) => {
    const date = r.start_date ?? (typeof r._mapKey === 'string' ? r._mapKey.slice(0, 10) : undefined);
    return Object.assign({}, r, { _date: date });
  });

  // sort by date descending (newest first) when possible
  normalized.sort((a: any, b: any) => {
    const da = a._date ?? '';
    const db = b._date ?? '';
    if (da === db) return 0;
    return da < db ? 1 : -1;
  });

  const rikishiIdNum = rikishiId ?? undefined;

  // include basho_id as requested and keep other columns
  const cols = ['date', 'basho_id', 'opponent', 'opponent_id', 'our_side', 'result', 'our_rank', 'opponent_rank', 'division', 'kimarite', 'location'];

  // Use homepage green provided by user (#A3E0B8) for prominent table headers; keep cells compact
  const headerExtraClass = 'p-3';

  // No vertical scroll or height limit — allow table to extend naturally
  return (
    <div className="overflow-x-auto">
  <table className="w-full border-collapse box-border bg-white">
        <thead>
          <tr>
            <th className={`${SHARED_TH_CLASS} ${headerExtraClass} bg-[#A3E0B8] text-[#042f21]`}>date</th>
            <th className={`${SHARED_TH_CLASS} ${headerExtraClass} bg-[#A3E0B8] text-[#042f21]`}>basho</th>
            <th className={`${SHARED_TH_CLASS} ${headerExtraClass} bg-[#A3E0B8] text-[#042f21]`}>rikishi</th>
            <th className={`${SHARED_TH_CLASS} ${headerExtraClass} bg-[#A3E0B8] text-[#042f21]`}>rikishi rank</th>
            <th className={`${SHARED_TH_CLASS} ${headerExtraClass} bg-[#A3E0B8] text-[#042f21]`}>result</th>
            <th className={`${SHARED_TH_CLASS} ${headerExtraClass} bg-[#A3E0B8] text-[#042f21]`}>opponent</th>
            <th className={`${SHARED_TH_CLASS} ${headerExtraClass} bg-[#A3E0B8] text-[#042f21]`}>opp rank</th>
            <th className={`${SHARED_TH_CLASS} ${headerExtraClass} bg-[#A3E0B8] text-[#042f21]`}>division</th>
            <th className={`${SHARED_TH_CLASS} ${headerExtraClass} bg-[#A3E0B8] text-[#042f21]`}>kimarite</th>
          </tr>
        </thead>
        <tbody>
          {normalized.map((m: any, idx: number) => {
            // determine row background based on Win/Loss
            // determine side
            const eastId = m.east_rikishi_id ?? m.east_rikishi_id;
            const westId = m.west_rikishi_id ?? m.west_rikishi_id;
            let ourSide = '';
            if (rikishiIdNum !== undefined) {
              if (String(eastId) === String(rikishiIdNum) || String(m.east_rikishi_id) === String(rikishiIdNum)) ourSide = 'east';
              if (String(westId) === String(rikishiIdNum) || String(m.west_rikishi_id) === String(rikishiIdNum)) ourSide = ourSide ? ourSide : 'west';
            }

            const opponentName = ourSide === 'east' ? (m.westshikona ?? m.west_shikona ?? m.westshikona) : (m.eastshikona ?? m.east_shikona ?? m.eastshikona);
            const opponentId = ourSide === 'east' ? (m.west_rikishi_id ?? m.west_rikishi_id) : (m.east_rikishi_id ?? m.east_rikishi_id);
            const ourShikona = ourSide === 'east' ? (m.eastshikona ?? m.east_shikona ?? m.eastshikona) : (m.westshikona ?? m.west_shikona ?? m.westshikona);
            // Normalize winner: winner may be stored as 1/2, 'east'/'west', or the rikishi id
            const rawWinner = m.winner;
            let winnerIdNormalized: string | null = null;
            try {
              if (rawWinner === 1 || rawWinner === '1' || String(rawWinner).toLowerCase() === 'east') {
                winnerIdNormalized = String(m.east_rikishi_id ?? '');
              } else if (rawWinner === 2 || rawWinner === '2' || String(rawWinner).toLowerCase() === 'west') {
                winnerIdNormalized = String(m.west_rikishi_id ?? '');
              } else if (rawWinner != null && rawWinner !== '') {
                // assume stored as a rikishi id or other string; stringify for comparison
                winnerIdNormalized = String(rawWinner);
              } else {
                winnerIdNormalized = null;
              }
            } catch (e) {
              winnerIdNormalized = rawWinner != null ? String(rawWinner) : null;
            }

            const isWin = winnerIdNormalized && rikishiIdNum !== undefined && String(rikishiIdNum) === String(winnerIdNormalized);
            const result = isWin ? 'Win' : (winnerIdNormalized ? (ourSide ? 'Loss' : winnerIdNormalized) : '');

            const ourRank = ourSide === 'east' ? (m.east_rank ?? m.east_rank) : (m.west_rank ?? m.west_rank);
            const oppRank = ourSide === 'east' ? (m.west_rank ?? m.west_rank) : (m.east_rank ?? m.east_rank);

            const even = idx % 2 === 0;
            // alternate neutral rows for readability, but keep win/loss highlights
            const rowBg = result === 'Win' ? '#edf7ee' : (result === 'Loss' ? '#fff5f5' : (even ? '#ffffff' : '#f6f8f6'));
            const rowStyle: React.CSSProperties = { background: rowBg };
            const indicatorColor = result === 'Win' ? '#16a34a' : (result === 'Loss' ? '#dc2626' : 'transparent');

            return (
              <tr key={idx} style={rowStyle}>
                <td className={SHARED_TD_CLASS} style={{ borderLeft: `6px solid ${indicatorColor}`, paddingLeft: '0.8rem' }}>{m._date ?? m.start_date ?? ''}</td>
                <td className={SHARED_TD_CLASS}>{m.basho_id ? <a href={`/basho/${m.basho_id}`} className="underline text-inherit">{m.location ?? m.basho_id}</a> : (m.location ?? '')}</td>
                <td className={`${SHARED_TD_CLASS} font-bold`}>{ourShikona ?? ''}</td>
                <td className={SHARED_TD_CLASS}>{ourRank ?? ''}</td>
                <td className={SHARED_TD_CLASS} style={{ color: result === 'Win' ? '#116530' : (result === 'Loss' ? '#b91c1c' : undefined), fontWeight: 700 }}>{result ?? ''}</td>
                <td className={SHARED_TD_CLASS}>
                  {opponentId ? <a href={`/rikishi/${opponentId}`} className="underline font-semibold text-inherit">{opponentName ?? ''}</a> : (opponentName ?? '')}
                </td>
                <td className={SHARED_TD_CLASS}>{oppRank ?? ''}</td>
                <td className={SHARED_TD_CLASS}>{m.division ?? ''}</td>
                <td className={SHARED_TD_CLASS}>{m.kimarite ?? ''}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

export default function RikishiDetailPage() {
  const params = useParams() as { id?: string };
  const id = params?.id ?? "";
  const [doc, setDoc] = useState<any | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [userFavoriteId, setUserFavoriteId] = useState<string | null>(null);
  const [favLoading, setFavLoading] = useState(false);

  useEffect(() => {
    if (!id) return;
    let mounted = true;
    const fetchDoc = async () => {
      setLoading(true);
      try {
          const res = await fetch(`/api/rikishi/${encodeURIComponent(id)}`, { credentials: 'include' });
        if (!mounted) return;
        if (!res.ok) {
          const text = await res.text();
          setError(`Failed to load rikishi: ${res.status} ${text}`);
          setLoading(false);
          return;
        }
        const data = await res.json();
          // Use the whole document returned by the API (not just the `rikishi` nested key)
          setDoc(data ?? null);
      } catch (err: any) {
        setError(String(err?.message ?? err));
      } finally {
        setLoading(false);
      }
    };

    fetchDoc();
    // also try to get current user's favorite rikishi (if logged in)
    (async () => {
      try {
        const r = await fetchWithAuth('/api/users/me');
        if (r.ok) {
          const me = await r.json();
          const fav = me?.favorite_rikishi;
          // favorite_rikishi may be an object or id string
          if (fav && typeof fav === 'object' && fav.id) setUserFavoriteId(String(fav.id));
          else if (fav) setUserFavoriteId(String(fav));
        }
      } catch (e) {
        // ignore - not logged in or network error
      }
    })();
    return () => { mounted = false; };
  }, [id]);

  const summary = useMemo(() => {
    if (!doc) return null;
    const s = doc.rikishi ?? doc;
    // Basic fields prefer the nested `rikishi` object when present
    return {
      name: s.shikona ?? s.name ?? s.title,
      birthdate: s.birthdate ?? s.born ?? null,
      heya: s.heya ?? null,
      shusshin: s.shusshin ?? s.birthplace ?? null,
      current_rank: s.current_rank ?? s.rank ?? null,
      image: s.image_url ?? s.image ?? s.s3_key ?? null,
    };
  }, [doc]);
  // prefer nested `rikishi` object when present
  const source = doc?.rikishi ?? doc ?? {};

  // Normalize matches into an array for reuse (handles arrays or map objects)
  const matchesArray = useMemo(() => {
    const m = doc?.matches ?? doc?.matches_history ?? doc?.rikishi?.matches ?? null;
    return normalizeObjectMapToArray(m) || [];
  }, [doc]);

  // Compute kimarite counts from matches (top-level counts)
  const kimariteCounts = useMemo(() => {
    const counts: Record<string, number> = {};
    if (!matchesArray || matchesArray.length === 0) return counts;
    matchesArray.forEach((mm: any) => {
      const k = (mm.kimarite ?? mm.kimarite_name ?? '')?.toString?.().trim?.() ?? '';
      if (!k) return;
      counts[k] = (counts[k] || 0) + 1;
    });
    return counts;
  }, [matchesArray]);

  // Build a win/loss series (career-wide) from match objects - include all matches (no timeframe filter)
  const sparklineSeries = useMemo(() => {
    if (!matchesArray || matchesArray.length === 0) return [] as number[];

    // derive rikishi id from doc (server-provided preferred id)
    const rikishiIdNum = doc?.id ?? doc?.rikishi_id ?? doc?._id ?? undefined;

    // collect matches with a parsable date (include all historic matches)
    const rows = matchesArray
      .map((m: any) => {
        const dateStr = m._date ?? m.start_date ?? m.date ?? m.match_date ?? null;
        const d = dateStr ? new Date(String(dateStr)) : null;
        return { raw: m, date: d };
      })
      .filter((r: any) => r.date instanceof Date && !isNaN(r.date.getTime()))
      .sort((a: any, b: any) => (a.date!.getTime() < b.date!.getTime() ? -1 : 1));

    const series: number[] = [];
    rows.forEach((r: any) => {
      const m = r.raw;
      // determine our side same as renderMatchesTable
      const eastId = m.east_rikishi_id ?? m.east_rikishi_id;
      const westId = m.west_rikishi_id ?? m.west_rikishi_id;
      let ourSide = '';
      if (rikishiIdNum !== undefined) {
        if (String(eastId) === String(rikishiIdNum) || String(m.east_rikishi_id) === String(rikishiIdNum)) ourSide = 'east';
        if (String(westId) === String(rikishiIdNum) || String(m.west_rikishi_id) === String(rikishiIdNum)) ourSide = ourSide ? ourSide : 'west';
      }
      // Normalize winner: may be 1/2 (east/west), 'east'/'west', or a rikishi id
      const rawWinner = m.winner;
      let winnerIdNormalized: string | null = null;
      try {
        if (rawWinner === 1 || rawWinner === '1' || String(rawWinner).toLowerCase() === 'east') {
          winnerIdNormalized = String(m.east_rikishi_id ?? '');
        } else if (rawWinner === 2 || rawWinner === '2' || String(rawWinner).toLowerCase() === 'west') {
          winnerIdNormalized = String(m.west_rikishi_id ?? '');
        } else if (rawWinner != null && rawWinner !== '') {
          winnerIdNormalized = String(rawWinner);
        } else {
          winnerIdNormalized = null;
        }
      } catch (e) {
        winnerIdNormalized = rawWinner != null ? String(rawWinner) : null;
      }

      const isWin = winnerIdNormalized && rikishiIdNum !== undefined && String(rikishiIdNum) === String(winnerIdNormalized);
      series.push(isWin ? 1 : 0);
    });

    return series;
  }, [matchesArray, doc]);

  // visual heights (px) for side-by-side charts
  // Keep the kimarite radar equal or slightly taller than before, and
  // reduce the sparkline height so its card bottom meets the radar's bottom.
  const KIMARITE_HEIGHT = 360; // keep as before (was 360 originally)
  const SPARKLINE_HEIGHT = 220; // shorter sparkline to align with radar bottom


  return (
    <>
      <div id="background"></div>
      <main style={{ maxWidth: '100%', margin: '15rem 0 0', padding: '0 1.5rem', position: 'relative', zIndex: 1 }}>
  <div style={{ maxWidth: 'min(2200px, 98%)', margin: '0 auto' }} className="content-box">

        {loading && <div className="app-text">Loading...</div>}
        {!loading && error && <div style={{ color: 'crimson' }}>{error}</div>}

        {!loading && !error && doc && (
          <div style={{ display: 'flex', gap: 32, width: '100%', alignItems: 'flex-start' }}>
            {/* Left column: avatar & summary */}
            <div style={{ width: 260, flex: '0 0 260px' }}>
              <div className="rounded-xl" style={{ border: '4px solid #563861', padding: 4 }}>
                <div className="rounded-lg overflow-hidden bg-[#e0a3c2]">
                  {summary?.image ? (
                    // If value appears to be an s3 key, try to show it raw; user can replace with real url mapping
                    <img src={summary.image} alt={String(summary.name ?? 'rikishi')} className="w-full h-96 object-cover object-top" onError={(e)=>{(e.target as HTMLImageElement).style.display='none'}} />
                  ) : (
                    <div className="w-full h-96 flex items-center justify-center bg-gray-50">
                      <div className="app-text">No image</div>
                    </div>
                  )}
                  <div className="p-4 text-[#14532d] text-left leading-tight" style={{ fontSize: '1rem' }}>
                    <h2 className="app-text m-0">{summary?.name ?? `Rikishi ${id}`}</h2>
                    {/* retirement badge on its own line for clarity */}
                    {source?.retirement_date ? (
                      <div className="mt-2"><span className="bg-red-100 text-red-700 px-2 rounded text-xs">Retired</span></div>
                    ) : (
                      <div className="mt-2"><span className="bg-[#A3E0B8]/20 text-[#14532d] px-2 rounded text-xs">Active</span></div>
                    )}
                    <div className="mt-3">
                      <div className="app-text font-semibold">Rank: <span className="font-normal">{summary?.current_rank ?? 'inactive'}</span></div>
                      {summary?.heya && <div className="app-text font-semibold">Heya: <span className="font-normal">{summary.heya}</span></div>}
                      {summary?.shusshin && <div className="app-text font-semibold">From: <span className="font-normal">{summary.shusshin}</span></div>}
                      {summary?.birthdate && <div className="app-text font-semibold">Born: <span className="font-normal">{String(summary.birthdate)}</span></div>}

                      <div className="grid grid-cols-2 gap-1 mt-2 text-sm leading-snug">
                        <div className="app-text font-semibold">Height</div>
                        <div className="app-text">{source?.current_height ? `${source.current_height} cm` : '—'}</div>
                        <div className="app-text font-semibold">Weight</div>
                        <div className="app-text">{source?.current_weight ? `${source.current_weight} kg` : '—'}</div>
                        <div className="app-text font-semibold">Wins</div>
                        <div className="app-text">{(source?.wins ?? source?.win_count) !== undefined ? `${source.wins ?? source.win_count}` : '—'}</div>
                        <div className="app-text font-semibold">Losses</div>
                        <div className="app-text">{(source?.losses) !== undefined ? `${source.losses}` : '—'}</div>
                      </div>

                      <div className="grid grid-cols-2 gap-1 mt-2 text-sm leading-snug">
                        <div className="app-text font-semibold">Yusho</div>
                        <div className="app-text">{source?.yusho_count ?? source?.yusho ?? 0}</div>
                        <div className="app-text font-semibold">Sansho</div>
                        <div className="app-text">{source?.sansho_count ?? source?.sansho ?? 0}</div>
                      </div>

                      {/* Favorite button */}
                      <div className="mt-3">
                        <button
                          onClick={async () => {
                            if (favLoading) return;
                            // if user already favorited this rikishi
                            if (userFavoriteId && String(userFavoriteId) === String(id)) {
                              alert('This rikishi is already your favorite.');
                              return;
                            }
                            // if user has a different favorite, confirm replacement
                            if (userFavoriteId && String(userFavoriteId) !== String(id)) {
                              const ok = window.confirm('You already have a favorite rikishi. Replace it with this one?');
                              if (!ok) return;
                            }
                            setFavLoading(true);
                            try {
                              const res = await fetchWithAuth('/api/users/me', { method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ favorite_rikishi: String(id) }) });
                              if (!res.ok) {
                                const txt = await res.text();
                                alert('Failed to set favorite: ' + res.status + ' ' + txt);
                              } else {
                                // update local state; server returns updated user on success
                                setUserFavoriteId(String(id));
                                alert('Favorite updated');
                              }
                            } catch (e: any) {
                              alert('Network error: ' + String(e?.message ?? e));
                            } finally {
                              setFavLoading(false);
                            }
                          }}
                          className="px-3 py-2 rounded-md font-semibold"
                          style={{ background: userFavoriteId && String(userFavoriteId) === String(id) ? '#f97316' : '#2563eb', color: 'white' }}
                        >
                          {favLoading ? 'Saving...' : (userFavoriteId && String(userFavoriteId) === String(id) ? 'Favorited' : 'Set as favorite')}
                        </button>
                      </div>

                      <div className="mt-2">
                        <div className="app-text font-semibold">Debut</div>
                        <div className="app-text">{source?.debut ?? '—'}</div>
                        <div className="app-text font-semibold mt-2">Retired</div>
                        <div className="app-text">{source?.retirement_date ?? '—'}</div>
                      </div>
                    </div>
                  </div>
                </div>
              </div>

              {/* Quick key-values: show only less-common fields (avoid duplicating grouped info) */}
              <div style={{ marginTop: '0.75rem' }}>
                {(() => {
                  const excluded = new Set([
                    'rank','current_rank','debut','shikona','heya','birthdate','shusshin','current_height','current_weight','wins','losses','yusho_count','yusho','sansho_count','sansho','retirement_date',
                    // remove noisy / large or non-essential fields
                    'absent_count','attribution_html','commons_source_url','credit_html','height','id','image_url','last_match','license','license_url','mime','s3_key','s3_url','width','basho_count',
                    // also omit big nested collections from the quick list
                    'matches','matches_history','rikishi_rank_history','rikishi_shikona_changes','special_prizes','division_wins','division wins','rikishi_measurements_history','bio'
                  ]);
                  const extras = Object.fromEntries(Object.entries(source).filter(([k]) => !excluded.has(k)).slice(0, 30));
                  return Object.keys(extras).length ? renderKeyValue(extras) : null;
                })()}
              </div>
            </div>

            {/* Right column: details */}
            <div style={{ flex: '1 1 0', minWidth: 0 }}>
              {doc.bio && <div style={{ marginBottom: '0.5rem' }} className="app-text">{doc.bio}</div>}

              {/* Visuals: stats, sparkline, kimarite chart (above dropdowns) */}
              <div style={{ marginBottom: '1rem' }}>
                <div className="flex flex-col md:flex-row gap-4" style={{ alignItems: 'stretch' }}>
                  <div className="flex flex-col gap-3 w-full" style={{ flexBasis: '40%', maxWidth: '40%', minHeight: KIMARITE_HEIGHT }}>
                      <div className="grid grid-cols-2 gap-3">
                        {/* Yusho card with kimarite-like gradient */}
                        <div style={{ border: '4px solid #563861', borderRadius: 8, padding: 4 }}>
                          <div className="rounded-md shadow-sm text-center p-4 app-text" style={{ background: 'linear-gradient(135deg, #f5e6c8 0%, #e0a3c2 100%)', color: '#563861' }}>
                            <div className="text-sm">Yusho</div>
                            <div className="text-3xl font-bold">{source?.yusho_count ?? source?.yusho ?? 0}</div>
                          </div>
                        </div>
                        {/* Sansho card with same gradient */}
                        <div style={{ border: '4px solid #563861', borderRadius: 8, padding: 4 }}>
                          <div className="rounded-md shadow-sm text-center p-4 app-text" style={{ background: 'linear-gradient(135deg, #f5e6c8 0%, #e0a3c2 100%)', color: '#563861' }}>
                            <div className="text-sm">Sansho</div>
                            <div className="text-3xl font-bold">{source?.sansho_count ?? source?.sansho ?? 0}</div>
                          </div>
                        </div>
                      </div>
                      <div style={{ border: '4px solid #563861', borderRadius: 8, padding: 4, marginTop: 8 }}>
                        <div className="rounded-md shadow-sm p-4 app-text" style={{ height: SPARKLINE_HEIGHT, background: 'linear-gradient(135deg, #f5e6c8 0%, #e0a3c2 100%)', color: '#563861' }}>
                          <RikishiWinLossSparkline data={sparklineSeries} rikishiId={doc?.id ?? doc?.rikishi_id ?? doc?._id} />
                        </div>
                      </div>
                    </div>
                    <div className="w-full" style={{ flexBasis: '60%', maxWidth: '60%' }}>
                      {/* KimariteRadarChart renders its own card-style wrapper; use it directly without an extra container */}
                      <KimariteRadarChart kimariteCounts={kimariteCounts} height={KIMARITE_HEIGHT} />
                    </div>
                </div>
              </div>

              {/* Large sections */}
              <SectionToggle title={`Rank History (${Array.isArray(doc.rikishi_rank_history) ? doc.rikishi_rank_history.length : Object.keys(doc.rikishi_rank_history || {}).length})`}>
                {renderRankHistory(doc.rikishi_rank_history ?? doc?.rikishi?.rikishi_rank_history ?? null)}
              </SectionToggle>
              <SectionToggle title={`Shikona Changes (${Array.isArray(doc.rikishi_shikona_changes ?? doc?.rikishi?.rikishi_shikona_changes) ? (doc.rikishi_shikona_changes ?? doc?.rikishi?.rikishi_shikona_changes).length : Object.keys(doc.rikishi_shikona_changes ?? doc?.rikishi?.rikishi_shikona_changes ?? {}).length})`}>
                {renderShikonaChanges(doc.rikishi_shikona_changes ?? doc?.rikishi?.rikishi_shikona_changes ?? null)}
              </SectionToggle>
              <SectionToggle title={`Special Prizes (${Array.isArray(doc.special_prizes ?? doc?.rikishi?.special_prizes) ? (doc.special_prizes ?? doc?.rikishi?.special_prizes).length : Object.keys(doc.special_prizes ?? doc?.rikishi?.special_prizes ?? {}).length})`}>
                {renderSpecialPrizes(doc.special_prizes ?? doc?.rikishi?.special_prizes ?? null)}
              </SectionToggle>
              <SectionToggle title={`Division Wins (${Array.isArray(doc['division wins'] ?? doc.division_wins ?? doc?.rikishi?.['division wins'] ?? doc?.rikishi?.division_wins) ? (doc['division wins'] ?? doc.division_wins ?? doc?.rikishi?.['division wins'] ?? doc?.rikishi?.division_wins).length : Object.keys(doc['division wins'] ?? doc.division_wins ?? doc?.rikishi?.['division wins'] ?? doc?.rikishi?.division_wins ?? {}).length})`}>
                {renderDivisionWins(doc['division wins'] ?? doc.division_wins ?? doc?.rikishi?.['division wins'] ?? doc?.rikishi?.division_wins ?? null)}
              </SectionToggle>
              <SectionToggle title={`Measurements History (${Array.isArray(doc.rikishi_measurements_history ?? doc?.rikishi?.rikishi_measurements_history) ? (doc.rikishi_measurements_history ?? doc?.rikishi?.rikishi_measurements_history).length : Object.keys(doc.rikishi_measurements_history ?? doc?.rikishi?.rikishi_measurements_history ?? {}).length})`}>
                {renderMeasurementsHistory(doc.rikishi_measurements_history ?? doc?.rikishi?.rikishi_measurements_history ?? null)}
              </SectionToggle>
              {/* Matches table always visible at the bottom */}
              <div style={{ marginTop: '1rem' }}>
                <h3 className="app-text">Matches ({Array.isArray(doc.matches) ? doc.matches.length : (doc.matches ? Object.keys(doc.matches).length : 0)})</h3>
                {renderMatchesTable(doc.matches ?? doc.matches_history ?? doc?.rikishi?.matches ?? null, doc?.id ?? doc?.rikishi_id ?? doc?._id)}
              </div>
            </div>
          </div>
        )}

        {!loading && !error && !doc && <div className="app-text">No rikishi page found.</div>}
        </div>
      </main>
    </>
  );
}

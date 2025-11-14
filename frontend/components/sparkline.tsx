"use client";

import * as React from 'react';
import Stack from '@mui/material/Stack';
import Typography from '@mui/material/Typography';
import CircularProgress from '@mui/material/CircularProgress';
import { SparkLineChart } from '@mui/x-charts/SparkLineChart';
import type { SparkLineChartProps } from '@mui/x-charts/SparkLineChart';
import { areaElementClasses, lineElementClasses } from '@mui/x-charts/LineChart';
import { chartsAxisHighlightClasses } from '@mui/x-charts/ChartsAxisHighlight';


interface RikishiSparklineProps {
  data?: number[]; // optional array of 0/1 (loss/win) or numeric metric
  title?: string;
  rikishiId?: string | number;
}

// Deterministic placeholder win/loss data for SSR (avoids hydration mismatch).
// We may replace this with client-side random data after hydration for visual variety.
const defaultMatches = 30;
const defaultWinLossRaw = Array.from({ length: defaultMatches }, (_, i) => (i % 2 === 0 ? 1 : 0));

// Helper: convert binary win/loss series to running win/loss ratio
const computeRunningRatio = (series: number[]) => series.map((_, i) => {
  const arr = series.slice(0, i + 1);
  const wins = arr.reduce((acc: number, v) => acc + (Number(v) || 0), 0);
  const losses = arr.length - wins;
  if (losses === 0) return wins === 0 ? 0 : wins;
  return wins / losses;
});


// Base settings for the sparkline chart (data/xAxis/yAxis applied per-instance)
const baseSettings: Omit<SparkLineChartProps, 'data' | 'xAxis' | 'yAxis'> = {
  baseline: 'min',
  margin: { bottom: 0, top: 5, left: 4, right: 0 },
  sx: {
    [`& .${areaElementClasses.root}`]: { opacity: 0.2 },
    [`& .${lineElementClasses.root}`]: { strokeWidth: 3 },
    [`& .${chartsAxisHighlightClasses.root}`]: {
      stroke: 'rgb(137, 86, 255)',
      strokeDasharray: 'none',
      strokeWidth: 2,
    },
  },
  slotProps: {
    lineHighlight: { r: 4 },
  },
  clipAreaOffset: { top: 2, bottom: 2 },
  axisHighlight: { x: 'line' },
};

export default function RikishiWinLossSparkline({ data, title, rikishiId }: RikishiSparklineProps) {
  const [fetchedSeries, setFetchedSeries] = React.useState<number[] | null>(null);
  const [fetchedLabels, setFetchedLabels] = React.useState<string[] | null>(null);
  const [fetchedIsPlaceholder, setFetchedIsPlaceholder] = React.useState(false);

  const series = React.useMemo(() => {
    if (Array.isArray(data) && data.length > 0) return data.map(d => Number(d) || 0);
    if (fetchedSeries && fetchedSeries.length > 0) return fetchedSeries;
    return defaultWinLossRaw;
  }, [data, fetchedSeries]);

  const matchLabels = React.useMemo(() => {
    if (fetchedLabels && fetchedLabels.length > 0) return fetchedLabels;
    return Array.from({ length: series.length }, (_, i) => `Match ${i + 1}`);
  }, [fetchedLabels, series.length]);

  const winLossRatio = computeRunningRatio(series);
  const minY = Math.min(...winLossRatio);
  const maxY = Math.max(...winLossRatio);
  const yPadding = (maxY - minY) * 0.15 || 0.2;

  const [matchIndex, setMatchIndex] = React.useState<null | number>(null);
  const [isLoading, setIsLoading] = React.useState(false);

  // No client-side fetches to /api/rikishi: the sparkline will only use `data` prop
  // If a rikishiId is provided (and no explicit `data` prop), fetch the
  // rikishi page document from the backend and attempt to extract a recent
  // match series from common fields (recent_form, win_series, trend,
  // recent_matches, form). This runs client-side only and won't affect SSR
  // output because the server-rendered fallback is deterministic.

  // After hydration (client-side) optionally populate a randomized sample series
  // only when no real rikishiId and no explicit `data` was provided. This runs
  // on the client and won't affect SSR output (which uses deterministic
  // `defaultWinLossRaw`), avoiding hydration mismatches.
  React.useEffect(() => {
    if (typeof window === 'undefined') return;
    // eslint-disable-next-line no-console
    console.log('sparkline: effect run — rikishiId', rikishiId, 'has data prop?', Array.isArray(data) && data.length > 0);
  // If a rikishiId is provided, prefer fetching the rikishi doc and
  // extracting a real series. If `data` is supplied by the parent, use it.
  const shouldFetch = rikishiId != null && (!Array.isArray(data) || data.length === 0) && (!fetchedSeries || fetchedSeries.length === 0 || fetchedIsPlaceholder);
  // eslint-disable-next-line no-console
  console.log('sparkline: shouldFetch?', shouldFetch);
  if (shouldFetch) {
      const idStr = String(rikishiId);
      (async () => {
        // eslint-disable-next-line no-console
        console.log('sparkline: initiating backend fetch for rikishiId', idStr);
        setIsLoading(true);
        try {
          // Try a few likely endpoints. Prefer an explicit client-side override
          // when NEXT_PUBLIC_API_URL is set (useful in dev/proxy setups).
          const envBase = typeof process !== 'undefined' && (process.env as any).NEXT_PUBLIC_API_URL
            ? String((process.env as any).NEXT_PUBLIC_API_URL).replace(/\/$/, '')
            : '';
          const candidates: string[] = [];
          if (envBase) candidates.push(`${envBase}/rikishi/${encodeURIComponent(idStr)}`);
          // try Next.js proxy API route (if configured)
          candidates.push(`/api/rikishi/${encodeURIComponent(idStr)}`);
          // same-origin public route
          candidates.push(`/rikishi/${encodeURIComponent(idStr)}`);
          // fallback to assumed Go backend on port 8080 (dev Docker)
          candidates.push(`${window.location.protocol}//${window.location.hostname}:8080/rikishi/${encodeURIComponent(idStr)}`);
          let res: Response | null = null;
          let doc: any = null;
          // eslint-disable-next-line no-console
          console.log('sparkline: candidates', candidates);
            for (const url of candidates) {
            try {
              // eslint-disable-next-line no-console
              console.log('sparkline: trying', url);
              const r = await fetch(url);
              if (!r.ok) {
                // eslint-disable-next-line no-console
                console.log('sparkline: non-ok response', r.status, url);
                continue;
              }
              res = r;
              doc = await r.json();
              // eslint-disable-next-line no-console
              console.log('sparkline: fetched keys', Object.keys(doc || {}).slice(0, 12));
              break;
            } catch (err) {
              // eslint-disable-next-line no-console
              console.log('sparkline: fetch error for', url, err);
              continue;
            }
          }
          if (!res || !doc) {
            setIsLoading(false);
            return;
          }

          // Try several common keys that may contain a recent series
          const candidate = doc.recent_form ?? doc.win_series ?? doc.trend ?? doc.recent_matches ?? doc.form ?? doc.matches ?? doc.rikishi?.matches;
          let arr: any[] | null = null;
          if (Array.isArray(candidate)) arr = candidate as any[];
          else if (candidate && typeof candidate === 'object') {
            // candidate may be an object map (e.g., matches keyed by date:match_number)
            // convert to array of values. If keys look like date:match_number we will
            // sort chronologically for sensible ordering.
            const asRec = candidate as Record<string, any>;
            const keys = Object.keys(asRec || {});
            const looksLikeDateKey = keys.length > 0 && keys[0] && keys[0].includes('match_number');
            if (looksLikeDateKey) {
              // sort by date and match_number extracted from the key
              const entries = keys.map(k => ({ key: k, val: asRec[k] }));
              entries.sort((a, b) => {
                const pa = a.key.split(':');
                const pb = b.key.split(':');
                const da = new Date(pa[0]).getTime() || 0;
                const db = new Date(pb[0]).getTime() || 0;
                if (da !== db) return da - db;
                const ma = Number(pa[pa.length - 1] ?? 0) || 0;
                const mb = Number(pb[pb.length - 1] ?? 0) || 0;
                return ma - mb;
              });
              arr = entries.map(e => e.val);
            } else {
              arr = Object.values(asRec as Record<string, any>);
            }
          }

          if (arr && arr.length > 0) {
            // Map array of objects or primitives into numeric 0/1 series.
            // When items are match objects, prefer checking `winner` against the
            // rikishi id. Otherwise, fall back to common keys like `win`, `won`,
            // `result`, or numeric values.
            const mapped = arr.map((s: any) => {
              if (s == null) return 0;
              if (typeof s === 'object') {
                // winner may be number or string
                if ('winner' in s) {
                  try {
                    const w = Number(s.winner);
                    const idNum = Number(idStr);
                    return w === idNum ? 1 : 0;
                  } catch {
                    // ignore and fallthrough
                  }
                }
                if ('win' in s) return Number(s.win) ? 1 : 0;
                if ('won' in s) return Number(s.won) ? 1 : 0;
                if ('result' in s) {
                  const v = s.result;
                  if (typeof v === 'string') return /win|w/i.test(v) ? 1 : 0;
                  return Number(v) ? 1 : 0;
                }
                // fallback: try numeric-conversion of a common value field
                return Number(s.value ?? s.score ?? 0) ? 1 : 0;
              }
              return Number(s) ? 1 : 0;
            });
            setFetchedSeries(mapped);
            setFetchedIsPlaceholder(false);
            setFetchedLabels(Array.from({ length: mapped.length }, (_, i) => `Match ${i + 1}`));
            setIsLoading(false);
            // eslint-disable-next-line no-console
            console.log('sparkline: mapped series length', mapped.length, 'sample', mapped.slice(0, 8));
            return;
          }

          // Fallback: try to parse a 'recent_matches' array of match objects
          if (Array.isArray(doc.recent_matches)) {
            const parsed = (doc.recent_matches as any[]).map((m) => {
              if (m == null) return 0;
              if (typeof m === 'object') {
                if ('winner' in m) {
                  const w = Number(m.winner);
                  const idNum = Number(idStr);
                  return w === idNum ? 1 : 0;
                }
                if ('win' in m) return Number(m.win) ? 1 : 0;
                if ('won' in m) return Number(m.won) ? 1 : 0;
                if ('result' in m) {
                  const v = m.result;
                  if (typeof v === 'string') return /win|w/i.test(v) ? 1 : 0;
                  return Number(v) ? 1 : 0;
                }
                return Number(m.value ?? m.score ?? 0) ? 1 : 0;
              }
              return Number(m) ? 1 : 0;
            });
            if (parsed.length > 0) {
              setFetchedSeries(parsed);
              setFetchedLabels(Array.from({ length: parsed.length }, (_, i) => `Match ${i + 1}`));
              setFetchedIsPlaceholder(false);
              setIsLoading(false);
              // eslint-disable-next-line no-console
              console.log('sparkline: parsed recent_matches length', parsed.length, 'sample', parsed.slice(0, 8));
              return;
            }
          }
        } catch (e) {
          // eslint-disable-next-line no-console
          console.log('sparkline fetch error', e);
          setIsLoading(false);
        }
        // if we reach here without returning, clear loading so UI can fallback
        setIsLoading(false);
      })();
      return;
    } else {
      // if we don't have an id, surface that so debugging is easier
      if (rikishiId == null) {
        // eslint-disable-next-line no-console
        console.log('sparkline: no rikishiId provided; skipping backend fetch');
      }
    }

    if (Array.isArray(data) && data.length > 0) return; // user-provided data
    if (fetchedSeries && fetchedSeries.length > 0) return; // already set

    // generate a modestly randomized series on the client only (no rikishiId)
    const rnd = Array.from({ length: defaultMatches }, () => (Math.random() > 0.5 ? 1 : 0));
    setFetchedSeries(rnd);
    setFetchedLabels(Array.from({ length: rnd.length }, (_, i) => `Match ${i + 1}`));
    setFetchedIsPlaceholder(true);
  }, [data, rikishiId, fetchedSeries]);

  const settingsLocal: SparkLineChartProps = {
    ...baseSettings,
    data: winLossRatio,
    xAxis: { id: 'match-axis', data: matchLabels },
    yAxis: {
      min: Math.floor(minY - yPadding),
      max: Math.ceil(maxY + yPadding),
      tickMinStep: 0.1,
      tickNumber: 5,
    },
  };

  return (
    <div
      onKeyDown={(event) => {
        switch (event.key) {
          case 'ArrowLeft':
            setMatchIndex((p) =>
              p === null ? matchLabels.length - 1 : (matchLabels.length + p - 1) % matchLabels.length,
            );
            break;
          case 'ArrowRight':
            setMatchIndex((p) => (p === null ? 0 : (p + 1) % matchLabels.length));
            break;
          default:
        }
      }}
      onFocus={() => {
        setMatchIndex((p) => (p === null ? 0 : p));
      }}
      role="button"
      aria-label="Showing win/loss by match"
      tabIndex={0}
    >
  <Stack direction="column" width={220} sx={{ paddingTop: 2, paddingBottom: 1 }}>
        <Typography
          sx={{
            color: 'rgb(117, 117, 117)',
            fontWeight: 500,
            fontSize: '0.9rem',
            pt: 1,
          }}
        >
          {matchIndex === null ? (title ?? `Last ${series.length} Matches`) : matchLabels[matchIndex]}
        </Typography>
        <Stack
          direction="column"
          alignItems="center"
          sx={{ borderBottom: 'solid 2px rgba(137, 86, 255, 0.2)', minHeight: 60, width: '100%' }}
        >
          <Typography
            sx={{
              fontSize: '1.25rem',
              fontWeight: 500,
              mb: 1,
              color: '#563861', // matches app main text color
              fontFamily: 'inherit', // inherit app font
              letterSpacing: '0.01em',
            }}
          >
            {(() => {
              const idx = matchIndex ?? series.length - 1;
              const arr = series.slice(0, idx + 1);
              const wins = arr.reduce((acc: number, v) => acc + (Number(v) || 0), 0);
              const losses = arr.length - wins;
              const winRate = Math.round((wins / arr.length) * 100);
              if (losses === 0) return `Win/Loss Ratio: ${wins}.0 (${winRate}% win)`;
              return `Win/Loss Ratio: ${(wins / losses).toFixed(2)} (${winRate}% win)`;
            })()}
          </Typography>
          {isLoading ? (
            <Stack direction="row" alignItems="center" justifyContent="center" sx={{ height: 60 }}>
              <CircularProgress size={20} thickness={5} />
            </Stack>
          ) : (
            <SparkLineChart
              height={60}
              width={140}
              area
              showHighlight
              color="rgb(137, 86, 255)"
              onHighlightedAxisChange={(axisItems) => {
                setMatchIndex(axisItems[0]?.dataIndex ?? null);
              }}
              highlightedAxis={
                matchIndex === null
                  ? []
                  : [{ axisId: 'match-axis', dataIndex: matchIndex }]
              }
              {...settingsLocal}
            />
          )}
        </Stack>
      </Stack>
    </div>
  );
}

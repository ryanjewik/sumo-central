"use client";

import * as React from 'react';
import Stack from '@mui/material/Stack';
import Typography from '@mui/material/Typography';
import { SparkLineChart } from '@mui/x-charts/SparkLineChart';
import type { SparkLineChartProps } from '@mui/x-charts/SparkLineChart';
import { areaElementClasses, lineElementClasses } from '@mui/x-charts/LineChart';
import { chartsAxisHighlightClasses } from '@mui/x-charts/ChartsAxisHighlight';


interface RikishiSparklineProps {
  data?: number[]; // optional array of 0/1 (loss/win) or numeric metric
  title?: string;
}

// Generate random win/loss data for last 30 matches (1 = win, 0 = loss) if no data provided
const defaultMatches = 30;
const defaultWinLossRaw = Array.from({ length: defaultMatches }, () => Math.random() > 0.5 ? 1 : 0);

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

export default function RikishiWinLossSparkline({ data, title }: RikishiSparklineProps) {
  const series = (Array.isArray(data) && data.length > 0) ? data.map(d => Number(d) || 0) : defaultWinLossRaw;
  const matchLabels = Array.from({ length: series.length }, (_, i) => `Match ${i + 1}`);
  const winLossRatio = computeRunningRatio(series);
  const minY = Math.min(...winLossRatio);
  const maxY = Math.max(...winLossRatio);
  const yPadding = (maxY - minY) * 0.15 || 0.2;

  const [matchIndex, setMatchIndex] = React.useState<null | number>(null);

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
        </Stack>
      </Stack>
    </div>
  );
}

import * as React from 'react';
import Stack from '@mui/material/Stack';
import Typography from '@mui/material/Typography';
import { SparkLineChart } from '@mui/x-charts/SparkLineChart';
import type { SparkLineChartProps } from '@mui/x-charts/SparkLineChart';
import { areaElementClasses, lineElementClasses } from '@mui/x-charts/LineChart';
import { chartsAxisHighlightClasses } from '@mui/x-charts/ChartsAxisHighlight';


// Generate random win/loss data for last 30 matches (1 = win, 0 = loss)
const matches = 30;
const winLossRaw = Array.from({ length: matches }, () => Math.random() > 0.5 ? 1 : 0);
const matchLabels = Array.from({ length: matches }, (_, i) => `Match ${i + 1}`);

// Calculate running win/loss ratio (wins/losses)
const winLossRatio = winLossRaw.map((_, i) => {
  const arr = winLossRaw.slice(0, i + 1);
  const wins = arr.reduce((acc: number, v) => acc + v, 0);
  const losses = arr.length - wins;
  // If no losses yet, show wins (as ratio to 1)
  if (losses === 0) return wins === 0 ? 0 : wins;
  return wins / losses;
});


// Calculate dynamic min/max for y-axis
const minY = Math.min(...winLossRatio);
const maxY = Math.max(...winLossRatio);
const yPadding = (maxY - minY) * 0.15 || 0.2;

const settings: SparkLineChartProps = {
  data: winLossRatio,
  baseline: 'min',
  margin: { bottom: 0, top: 5, left: 4, right: 0 },
  xAxis: { id: 'match-axis', data: matchLabels },
  yAxis: {
    min: Math.floor(minY - yPadding),
    max: Math.ceil(maxY + yPadding),
    tickMinStep: 0.1,
    tickNumber: 5,
  },
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


export default function RikishiWinLossSparkline() {
  const [matchIndex, setMatchIndex] = React.useState<null | number>(null);

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
          {matchIndex === null ? 'Last 30 Matches' : matchLabels[matchIndex]}
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
              const idx = matchIndex ?? winLossRaw.length - 1;
              const arr = winLossRaw.slice(0, idx + 1);
              const wins = arr.reduce((acc: number, v) => acc + v, 0);
              const losses = arr.length - wins;
              if (losses === 0) return `Win/Loss Ratio: ${wins}.0`;
              return `Win/Loss Ratio: ${(wins / losses).toFixed(2)}`;
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
            {...settings}
          />
        </Stack>
      </Stack>
    </div>
  );
}

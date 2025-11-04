"use client"

import * as React from "react"
import { Bar, BarChart, CartesianGrid, XAxis, YAxis, Cell } from "recharts"
// Helper to interpolate color saturation based on rikishi count
function getBarColor(rikishiCount: number, min: number, max: number) {
  // Base color: #E0A3C2 (site pink)
  // Make the saturation shift more drastic: 30% (very pale) to 100% (fully saturated)
  const baseHue = 330; // approx for #E0A3C2
  const baseLight = 75; // lightness for pastel
  const minSat = 30;
  const maxSat = 100;
  const sat = minSat + ((rikishiCount - min) / (max - min)) * (maxSat - minSat);
  return `hsl(${baseHue}, ${sat}%, ${baseLight}%)`;
}

// Heya average rank data (heya: avgRank)
const heyaAvgRank: Record<string, number> = {
  Takasago: 425.7307692307692,
  Isegahama: 419.92105263157896,
  Asakayama: 508,
  Oshiogawa: 487.45454545454544,
  Otowayama: 538.4444444444445,
  Sadogatake: 354.875,
  Oitekaze: 349.1818181818182,
  Hanaregoma: 450.4,
  Arashio: 385.06666666666666,
  Kataonami: 157,
  Takadagawa: 141,
  Isenoumi: 158,
  Tatsunami: 269,
  Shikoroyama: 60,
  Sakaigawa: 66,
  Dewanoumi: 379,
  Hakkaku: 105,
  Kise: 107,
  Nishikido: 33,
  Tokitsukaze: 339,
  Kokonoe: 277,
  Otake: 33,
  Tagonoura: 45,
  Fujishima: 86,
  Tamanoi: 115,
  Futagoyama: 252,
};

// Heya rikishi count data (heya: count)
const heyaRikishiCount: Record<string, number> = {
  Tokiwayama: 14,
  Takasago: 327,
  Isegahama: 295,
  Asakayama: 17,
  Oshiogawa: 142,
  Otowayama: 9,
  Onomatsu: 73,
  Sadogatake: 418,
  Oitekaze: 78,
  Hanaregoma: 141,
  Arashio: 37,
  Kataonami: 157,
  Takadagawa: 141,
  Isenoumi: 158,
  Tamanoi: 115,
  Tatsunami: 269,
  Shikoroyama: 60,
  Kasugano: 371,
  Sakaigawa: 66,
  Dewanoumi: 379,
  Hakkaku: 105,
  Kise: 107,
  Nishikido: 33,
  Tokitsukaze: 339,
  Kokonoe: 277,
  Otake: 33,
  Tagonoura: 45,
  Minato: 78,
  Fujishima: 86,
  Miyagino: 181,
  Futagoyama: 252
};

// Prepare chart data: for each heya in heyaAvgRank, show transformed rank and rikishi count
const chartData = Object.entries(heyaAvgRank).map(([heya, avgRank]) => ({
  heya,
  avgRankTransformed: 3000 - avgRank,
  rikishiCount: heyaRikishiCount[heya] ?? 0,
}));

// Chart config for recharts
const chartConfig = {
  avgRankTransformed: {
    label: "Transformed Avg Rank",
    // Use site pink: #E0A3C2
    color: "#E0A3C2",
  },
};

// Dummy components for Card, CardHeader, CardContent, CardTitle, CardDescription, ChartContainer, ChartTooltip
// Replace these imports with your actual UI library imports
import { Card } from "./ui/card";
import { ChartContainer, ChartTooltip } from "./ui/chart";

interface ChartBarInteractiveProps {
  heyaAvgRank?: Record<string, number>;
  heyaRikishiCount?: Record<string, number>;
}

export function ChartBarInteractive({ heyaAvgRank, heyaRikishiCount }: ChartBarInteractiveProps) {
  // if props provided, compute chartData from them; otherwise use compiled constants
  const computeChartData = () => {
    if (heyaAvgRank && Object.keys(heyaAvgRank).length > 0) {
      return Object.entries(heyaAvgRank).map(([heya, avgRank]) => ({
        heya,
        avgRankTransformed: 3000 - (avgRank as number),
        rikishiCount: (heyaRikishiCount && heyaRikishiCount[heya]) || 0,
      }));
    }
    return chartData;
  };

  const chartDataComputed = computeChartData().sort((a, b) => b.avgRankTransformed - a.avgRankTransformed);
  // Find min/max rikishi count for color scaling
  const rikishiCounts = chartDataComputed.map(d => d.rikishiCount);
  const minCount = Math.min(...rikishiCounts);
  const maxCount = Math.max(...rikishiCounts);
  return (
    <Card
      className="flex flex-col gap-1 bg-[#F5E6C8] border-2  rounded-xl shadow w-full"
      style={{ color: '#563861' }}
    >

      <div className="w-full">
        <ChartContainer
          config={chartConfig}
          className="aspect-auto h-full w-full min-h-[180px]"
        >
          <BarChart
            accessibilityLayer
            data={chartDataComputed}
            margin={{
              left: 12,
              right: 12,
              bottom: 40,
            }}
          >
            <CartesianGrid vertical={false} stroke="#e0a3c2" />
            <XAxis
              dataKey="heya"
              tickLine={false}
              axisLine={false}
              tickMargin={8}
              minTickGap={12}
              angle={-45}
              textAnchor="end"
              interval={0}
              height={60}
              tick={{ fill: '#563861', fontWeight: 600, fontSize: 13 }}
            />
            {/* Add YAxis to start at 2300 */}
            <YAxis domain={[2300, 'dataMax']} tick={{ fill: '#563861', fontWeight: 600, fontSize: 13 }} axisLine={false} tickLine={false} />
            <ChartTooltip
              content={({ active, payload }) => {
                if (active && payload && payload.length) {
                  const d = payload[0].payload;
                  return (
                    <div className="w-[180px] p-2 bg-[#fff7f0] border border-[#563861] rounded shadow" style={{ color: '#563861' }}>
                      <div style={{ fontWeight: 700 }}>{d.heya}</div>
                      <div>Transformed Avg Rank: <b>{d.avgRankTransformed.toFixed(2)}</b></div>
                      <div>Rikishi in Heya: <b>{d.rikishiCount}</b></div>
                    </div>
                  );
                }
                return null;
              }}
            />
            <Bar dataKey="avgRankTransformed">
              {chartDataComputed.map((entry, idx) => (
                <Cell key={`cell-${entry.heya}`} fill={getBarColor(entry.rikishiCount, minCount, maxCount)} />
              ))}
            </Bar>
          </BarChart>
        </ChartContainer>
      </div>
    </Card>
  );
}

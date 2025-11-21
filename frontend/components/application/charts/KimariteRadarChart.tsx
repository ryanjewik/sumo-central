import React from "react";
import { Legend, PolarAngleAxis, PolarGrid, PolarRadiusAxis, Radar, RadarChart as RechartsRadarChart, ResponsiveContainer, Tooltip } from "recharts";
// Chart helper components (not used by this chart) removed to avoid unused-import warnings
import { cx } from "../../../utils/cx";

// Custom legend renderer for 3 items per line
const CustomLegend: React.FC = () => {
    const rikishiNames = [
        "Hoshoryu",
        "Terunofuji",
        "Takakeisho",
        "Asanoyama",
        "Wakatakakage",
        "Shodai",
    ];
    const colors: Record<string, string> = {
        Hoshoryu: "#22c55e", // green
        Terunofuji: "#e75480", // pink
        Takakeisho: "#3b82f6", // blue
        Asanoyama: "#10b981", // teal/green
        Wakatakakage: "#f59e42", // orange
        Shodai: "#a78bfa", // purple
    };
    return (
        <div
            style={{
                display: 'flex',
                flexWrap: 'wrap',
                justifyContent: 'center',
                alignItems: 'center',
                gap: '0.25rem',
                width: '100%',
                maxWidth: '100%',
                margin: '0 auto',
                boxSizing: 'border-box',
            }}
        >
            {rikishiNames.map((name) => (
                <span
                    key={name}
                    style={{
                        display: 'inline-flex',
                        alignItems: 'center',
                        width: '32%',
                        minWidth: 70,
                        textAlign: 'left',
                        whiteSpace: 'normal',
                        wordBreak: 'keep-all',
                        color: 'inherit',
                        fontWeight: 500,
                        fontSize: '0.85rem',
                        marginBottom: '0.25rem',
                        gap: '0.4em',
                    }}
                >
                    <span
                        style={{
                            display: 'inline-block',
                            width: 14,
                            height: 14,
                            borderRadius: 3,
                            backgroundColor: colors[name],
                            marginRight: 6,
                            border: '1.5px solid #563861',
                        }}
                    />
                    {name}
                </span>
            ))}
        </div>
    );
};

// Custom tick component for PolarRadiusAxis
type CustomRadarChartTickProps = {
    x?: number | string;
    y?: number | string;
    payload?: { value?: string | number };
    className?: string;
    [key: string]: unknown;
};

const CustomRadarChartTick: React.FC<CustomRadarChartTickProps> = (props: CustomRadarChartTickProps) => {
    const { x, y, payload } = props;
    return (
        <text
            x={x as number}
            y={y as number}
            textAnchor="middle"
            className="fill-utility-gray-700 text-xs font-medium"
        >
            {String(payload?.value ?? '')}
        </text>
    );
};

type RadarDatum = { technique: string; [key: string]: number | string | undefined };

const radarDataDefault: RadarDatum[] = [
    { technique: "Yorikiri", Hoshoryu: 12, Terunofuji: 18, Takakeisho: 5, Asanoyama: 9, Wakatakakage: 7, Shodai: 6 },
    { technique: "Oshidashi", Hoshoryu: 7, Terunofuji: 3, Takakeisho: 15, Asanoyama: 8, Wakatakakage: 6, Shodai: 5 },
    { technique: "Hatakikomi", Hoshoryu: 4, Terunofuji: 2, Takakeisho: 8, Asanoyama: 3, Wakatakakage: 2, Shodai: 4 },
    { technique: "Tsukiotoshi", Hoshoryu: 2, Terunofuji: 1, Takakeisho: 6, Asanoyama: 2, Wakatakakage: 1, Shodai: 3 },
    { technique: "Uwatenage", Hoshoryu: 6, Terunofuji: 9, Takakeisho: 1, Asanoyama: 4, Wakatakakage: 3, Shodai: 2 },
    { technique: "Kotenage", Hoshoryu: 3, Terunofuji: 4, Takakeisho: 2, Asanoyama: 2, Wakatakakage: 1, Shodai: 1 },
    { technique: "Sukuinage", Hoshoryu: 1, Terunofuji: 5, Takakeisho: 0, Asanoyama: 1, Wakatakakage: 0, Shodai: 0 },
];

interface KimariteRadarChartProps {
    kimariteCounts?: Record<string, number>;
    // optional visual height in pixels
    height?: number;
}

const KimariteRadarChart: React.FC<KimariteRadarChartProps> = ({ kimariteCounts, height }: KimariteRadarChartProps) => {
    // if backend provides `kimarite_usage_most_recent_basho` as an object { technique: count }
    // take the top 6 techniques by count and convert to radar data with a single series named 'Usage'
    let radarData = radarDataDefault;
    if (kimariteCounts && Object.keys(kimariteCounts).length > 0) {
        const entries: { technique: string; Usage: number }[] = Object.entries(kimariteCounts).map(([k, v]) => ({ technique: k, Usage: Number(v ?? 0) }));
        entries.sort((a, b) => b.Usage - a.Usage);
        const top = entries.slice(0, 6);
        radarData = top.map(e => ({ technique: e.technique, Usage: e.Usage } as RadarDatum));
    }
    const colors: Record<string, string> = {
        Hoshoryu: "text-utility-brand-600",
        Terunofuji: "text-utility-pink-500",
        Takakeisho: "text-utility-blue-light-500",
        Asanoyama: "text-utility-green-500",
        Wakatakakage: "text-utility-orange-500",
        Shodai: "text-utility-purple-500",
    };

    // Gradient background and font color for contrast
    const gradientBg = 'linear-gradient(135deg, #f5e6c8 0%, #e0a3c2 100%)';
    const mainTextColor = '#563861';
    const axisTextColor = '#563861';

    const heightFinal = typeof height === 'number' ? height : 500;
    const responsiveInnerHeight = Math.max(200, heightFinal - 80);

    return (
        <div
            className="rounded-xl shadow-lg p-4 flex flex-col items-center justify-center app-text"
            style={{
                width: '100%',
                height: heightFinal,
                border: '4px solid #563861',
                background: gradientBg,
                color: mainTextColor,
            }}
        >
                    <div
                        style={{
                            fontWeight: 800,
                            fontSize: 'clamp(0.95rem, 1.1vw, 1.25rem)',
                            marginBottom: '0.75rem',
                            color: mainTextColor,
                            fontFamily: 'Courier New, Courier, monospace',
                            textAlign: 'center',
                        }}
                    >
                        Top Kimarite Usage
                    </div>
            <ResponsiveContainer width="100%" height={responsiveInnerHeight}>
                <RechartsRadarChart
                    cx="50%"
                    cy="50%"
                    outerRadius="80%"
                    data={radarData}
                    className="size-full font-medium text-tertiary [&_.recharts-polar-grid]:text-utility-gray-100 [&_.recharts-text]:text-sm"
                    margin={{ left: 0, right: 0, top: 0, bottom: 0 }}
                >
                                        {/* show a compact legend only for multi-rikishi demo; for single 'Usage' series we show no legend */}
                                        {!(kimariteCounts && Object.keys(kimariteCounts).length > 0) && (
                                            <Legend
                                                verticalAlign="bottom"
                                                align="center"
                                                layout="horizontal"
                                                content={() => <CustomLegend />}
                                            />
                                        )}
                    <PolarGrid stroke="currentColor" className="text-utility-gray-100" />
                    <PolarAngleAxis
                        dataKey="technique"
                        stroke="currentColor"
                        tick={(tickProps: any) => {
                            const { x, y, textAnchor, index, payload, ...props } = tickProps;
                            // shorten long technique names for radial ticks
                            const label = String(payload.value || '');
                            const short = label.length > 18 ? `${label.slice(0, 16)}…` : label;
                            const yAdj = index === 0 ? Number(y) - 14 : index === 3 || index === 4 ? Number(y) + 10 : Number(y);
                            return (
                                <text
                                    x={x}
                                    y={yAdj}
                                    textAnchor={textAnchor}
                                    {...props}
                                    className={cx("recharts-text recharts-polar-angle-axis-tick-value", props.className)}
                                    fill={axisTextColor}
                                >
                                    <tspan dy="0em" style={{ fill: axisTextColor }} className="text-xs font-medium">
                                        {short}
                                    </tspan>
                                </text>
                            );
                        }}
                        tickLine={false}
                        axisLine={false}
                    />
                    <PolarRadiusAxis
                        textAnchor="middle"
                        tick={(props: any) => <CustomRadarChartTick {...props} fill={axisTextColor} />}
                        axisLine={false}
                        angle={90}
                        domain={[0, 20]}
                    />
                    {/* Custom tooltip: show technique and value formatted for single-series or multi-series */}
                    <Tooltip
                        content={({ active, payload }) => {
                            if (!active || !payload || !payload.length) return null;
                            const p = payload[0];
                            const name = p?.payload?.technique ?? p?.name ?? '';
                            // find the numeric value from available keys
                            const val = p?.payload?.Usage ?? Object.values(p?.payload ?? {}).filter(v => typeof v === 'number')[0] ?? p?.value;
                            return (
                                <div style={{ padding: 8, background: '#fff', border: '1px solid #e0a3c2', color: '#563861', borderRadius: 8 }}>
                                    <div style={{ fontWeight: 800 }}>{String(name)}</div>
                                    <div style={{ fontSize: 13 }}>Count: <strong>{Number(val ?? 0)}</strong></div>
                                </div>
                            );
                        }}
                        cursor={{
                            className: "stroke-utility-brand-600  stroke-2",
                            style: { transform: "translateZ(0)" },
                        }}
                    />
                    {/* If kimariteCounts provided we render a single series named 'Usage' */}
                    {kimariteCounts && Object.keys(kimariteCounts).length > 0 ? (
                        <Radar
                            isAnimationActive={false}
                            dataKey="Usage"
                            name="Usage"
                            stroke="#563861"
                            strokeWidth={2}
                            strokeLinejoin="round"
                            fill="#563861"
                            fillOpacity={0.2}
                        />
                    ) : (
                        <>
                            <Radar
                                isAnimationActive={false}
                                className={colors["Hoshoryu"]}
                                dataKey="Hoshoryu"
                                name="Hoshoryu"
                                stroke="currentColor"
                                strokeWidth={2}
                                strokeLinejoin="round"
                                fill="currentColor"
                                fillOpacity={0.2}
                                activeDot={{ className: "fill-bg-primary stroke-utility-brand-600 stroke-2" }}
                            />
                            <Radar
                                isAnimationActive={false}
                                className={colors["Terunofuji"]}
                                dataKey="Terunofuji"
                                name="Terunofuji"
                                stroke="currentColor"
                                strokeWidth={2}
                                strokeLinejoin="round"
                                fill="currentColor"
                                fillOpacity={0.2}
                                activeDot={{ className: "fill-bg-primary stroke-utility-brand-600 stroke-2" }}
                            />
                            <Radar
                                isAnimationActive={false}
                                className={colors["Takakeisho"]}
                                dataKey="Takakeisho"
                                name="Takakeisho"
                                stroke="currentColor"
                                strokeWidth={2}
                                strokeLinejoin="round"
                                fill="currentColor"
                                fillOpacity={0.2}
                                activeDot={{ className: "fill-bg-primary stroke-utility-brand-600 stroke-2" }}
                            />
                            <Radar
                                isAnimationActive={false}
                                className={colors["Asanoyama"]}
                                dataKey="Asanoyama"
                                name="Asanoyama"
                                stroke="currentColor"
                                strokeWidth={2}
                                strokeLinejoin="round"
                                fill="currentColor"
                                fillOpacity={0.2}
                                activeDot={{ className: "fill-bg-primary stroke-utility-green-500 stroke-2" }}
                            />
                            <Radar
                                isAnimationActive={false}
                                className={colors["Wakatakakage"]}
                                dataKey="Wakatakakage"
                                name="Wakatakakage"
                                stroke="currentColor"
                                strokeWidth={2}
                                strokeLinejoin="round"
                                fill="currentColor"
                                fillOpacity={0.2}
                                activeDot={{ className: "fill-bg-primary stroke-utility-orange-500 stroke-2" }}
                            />
                            <Radar
                                isAnimationActive={false}
                                className={colors["Shodai"]}
                                dataKey="Shodai"
                                name="Shodai"
                                stroke="currentColor"
                                strokeWidth={2}
                                strokeLinejoin="round"
                                fill="currentColor"
                                fillOpacity={0.2}
                                activeDot={{ className: "fill-bg-primary stroke-utility-purple-500 stroke-2" }}
                            />
                        </>
                    )}
                </RechartsRadarChart>
            </ResponsiveContainer>
        </div>
    );
};

export default KimariteRadarChart;

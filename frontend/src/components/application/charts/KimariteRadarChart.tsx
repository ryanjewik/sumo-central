// Custom legend renderer for 3 items per line
const CustomLegend = () => {
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
import React from "react";
import { Legend, PolarAngleAxis, PolarGrid, PolarRadiusAxis, Radar, RadarChart as RechartsRadarChart, ResponsiveContainer, Tooltip } from "recharts";
import { ChartLegendContent, ChartTooltipContent } from "./charts-base";
import { cx } from "../../../utils/cx";

// Custom tick component for PolarRadiusAxis
const CustomRadarChartTick = (props: any) => {
    const { x, y, payload } = props;
    return (
        <text
            x={x}
            y={y}
            textAnchor="middle"
            className="fill-utility-gray-700 text-xs font-medium"
        >
            {payload.value}
        </text>
    );
};

const radarData = [
    { technique: "Yorikiri", Hoshoryu: 12, Terunofuji: 18, Takakeisho: 5, Asanoyama: 9, Wakatakakage: 7, Shodai: 6 },
    { technique: "Oshidashi", Hoshoryu: 7, Terunofuji: 3, Takakeisho: 15, Asanoyama: 8, Wakatakakage: 6, Shodai: 5 },
    { technique: "Hatakikomi", Hoshoryu: 4, Terunofuji: 2, Takakeisho: 8, Asanoyama: 3, Wakatakakage: 2, Shodai: 4 },
    { technique: "Tsukiotoshi", Hoshoryu: 2, Terunofuji: 1, Takakeisho: 6, Asanoyama: 2, Wakatakakage: 1, Shodai: 3 },
    { technique: "Uwatenage", Hoshoryu: 6, Terunofuji: 9, Takakeisho: 1, Asanoyama: 4, Wakatakakage: 3, Shodai: 2 },
    { technique: "Kotenage", Hoshoryu: 3, Terunofuji: 4, Takakeisho: 2, Asanoyama: 2, Wakatakakage: 1, Shodai: 1 },
    { technique: "Sukuinage", Hoshoryu: 1, Terunofuji: 5, Takakeisho: 0, Asanoyama: 1, Wakatakakage: 0, Shodai: 0 },
];

const KimariteRadarChart: React.FC = () => {
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

    const purple = '#563861';
    return (
        <div
            className="rounded-xl shadow-lg p-4 flex flex-col items-center justify-center app-text"
            style={{
                width: '100%',
                height: 500,
                border: '4px solid #563861',
                background: gradientBg,
                color: mainTextColor,
            }}
        >
            <div
                style={{
                    fontWeight: 'bold',
                    fontSize: 'clamp(1rem, 1.2vw, 1.5rem)',
                    marginBottom: '1rem',
                    color: mainTextColor,
                    fontFamily: 'Courier New, Courier, monospace',
                    whiteSpace: 'nowrap',
                    overflow: 'hidden',
                    textOverflow: 'ellipsis',
                    width: '100%',
                    textAlign: 'center',
                }}
            >
                Top Rikishi Kimarite Usage
            </div>
            <ResponsiveContainer width="100%" height="100%">
                <RechartsRadarChart
                    cx="50%"
                    cy="50%"
                    outerRadius="80%"
                    data={radarData}
                    className="size-full font-medium text-tertiary [&_.recharts-polar-grid]:text-utility-gray-100 [&_.recharts-text]:text-sm"
                    margin={{ left: 0, right: 0, top: 0, bottom: 0 }}
                >
                    <Legend
                        verticalAlign="bottom"
                        align="center"
                        layout="horizontal"
                        content={CustomLegend}
                    />
                    <PolarGrid stroke="currentColor" className="text-utility-gray-100" />
                    <PolarAngleAxis
                        dataKey="technique"
                        stroke="currentColor"
                        tick={({ x, y, textAnchor, index, payload, ...props }) => (
                            <text
                                x={x}
                                y={index === 0 ? Number(y) - 14 : index === 3 || index === 4 ? Number(y) + 10 : Number(y)}
                                textAnchor={textAnchor}
                                {...props}
                                className={cx("recharts-text recharts-polar-angle-axis-tick-value", props.className)}
                                fill={axisTextColor}
                            >
                                <tspan dy="0em" style={{ fill: axisTextColor }} className="text-xs font-medium">
                                    {payload.value}
                                </tspan>
                            </text>
                        )}
                        tickLine={false}
                        axisLine={false}
                    />
                    <PolarRadiusAxis
                        textAnchor="middle"
                        tick={props => <CustomRadarChartTick {...props} fill={axisTextColor} />}
                        axisLine={false}
                        angle={90}
                        domain={[0, 20]}
                    />
                    <Tooltip
                        content={<ChartTooltipContent />}
                        cursor={{
                            className: "stroke-utility-brand-600  stroke-2",
                            style: { transform: "translateZ(0)" },
                        }}
                    />
                    {/* All Radar lines use purple for stroke and fill */}
                    <Radar
                        isAnimationActive={false}
                        dataKey="Hoshoryu"
                        name="Hoshoryu"
                        stroke={purple}
                        strokeWidth={2}
                        strokeLinejoin="round"
                        fill={purple}
                        fillOpacity={0.2}
                        activeDot={{ className: "fill-bg-primary stroke-utility-brand-600 stroke-2" }}
                    />
                    <Radar
                        isAnimationActive={false}
                        dataKey="Terunofuji"
                        name="Terunofuji"
                        stroke={purple}
                        strokeWidth={2}
                        strokeLinejoin="round"
                        fill={purple}
                        fillOpacity={0.2}
                        activeDot={{ className: "fill-bg-primary stroke-utility-brand-600 stroke-2" }}
                    />
                    <Radar
                        isAnimationActive={false}
                        dataKey="Takakeisho"
                        name="Takakeisho"
                        stroke={purple}
                        strokeWidth={2}
                        strokeLinejoin="round"
                        fill={purple}
                        fillOpacity={0.2}
                        activeDot={{ className: "fill-bg-primary stroke-utility-brand-600 stroke-2" }}
                    />
                    <Radar
                        isAnimationActive={false}
                        dataKey="Asanoyama"
                        name="Asanoyama"
                        stroke={purple}
                        strokeWidth={2}
                        strokeLinejoin="round"
                        fill={purple}
                        fillOpacity={0.2}
                        activeDot={{ className: "fill-bg-primary stroke-utility-green-500 stroke-2" }}
                    />
                    <Radar
                        isAnimationActive={false}
                        dataKey="Wakatakakage"
                        name="Wakatakakage"
                        stroke={purple}
                        strokeWidth={2}
                        strokeLinejoin="round"
                        fill={purple}
                        fillOpacity={0.2}
                        activeDot={{ className: "fill-bg-primary stroke-utility-orange-500 stroke-2" }}
                    />
                    <Radar
                        isAnimationActive={false}
                        dataKey="Shodai"
                        name="Shodai"
                        stroke={purple}
                        strokeWidth={2}
                        strokeLinejoin="round"
                        fill={purple}
                        fillOpacity={0.2}
                        activeDot={{ className: "fill-bg-primary stroke-utility-purple-500 stroke-2" }}
                    />
                </RechartsRadarChart>
            </ResponsiveContainer>
        </div>
    );
};

export default KimariteRadarChart;

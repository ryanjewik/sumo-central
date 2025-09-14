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
        { technique: "Yorikiri", Hoshoryu: 12, Terunofuji: 18, Takakeisho: 5 },
        { technique: "Oshidashi", Hoshoryu: 7, Terunofuji: 3, Takakeisho: 15 },
        { technique: "Hatakikomi", Hoshoryu: 4, Terunofuji: 2, Takakeisho: 8 },
        { technique: "Tsukiotoshi", Hoshoryu: 2, Terunofuji: 1, Takakeisho: 6 },
        { technique: "Uwatenage", Hoshoryu: 6, Terunofuji: 9, Takakeisho: 1 },
        { technique: "Kotenage", Hoshoryu: 3, Terunofuji: 4, Takakeisho: 2 },
        { technique: "Sukuinage", Hoshoryu: 1, Terunofuji: 5, Takakeisho: 0 },
];

const KimariteRadarChart: React.FC = () => {
    const colors: Record<string, string> = {
        Hoshoryu: "text-utility-brand-600",
        Terunofuji: "text-utility-pink-500",
        Takakeisho: "text-utility-blue-light-500",
    };

    return (
    <div className="rounded-xl bg-utility-gray-200 shadow-lg p-6 flex items-center justify-center" style={{ width: '100%', height: 500, border: '4px solid #563861' }}>
            <ResponsiveContainer width="100%" height="100%">
                <RechartsRadarChart
                    cx="50%"
                    cy="50%"
                    outerRadius="80%"
                    data={radarData}
                    className="size-full font-medium text-tertiary [&_.recharts-polar-grid]:text-utility-gray-100 [&_.recharts-text]:text-sm"
                    margin={{ left: 0, right: 0, top: 0, bottom: 0 }}
                >
                    <Legend verticalAlign="bottom" align="center" layout="horizontal" content={ChartLegendContent} />
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
                            >
                                <tspan dy="0em" className="fill-utility-gray-700 text-xs font-medium">
                                    {payload.value}
                                </tspan>
                            </text>
                        )}
                        tickLine={false}
                        axisLine={false}
                    />
                    <PolarRadiusAxis textAnchor="middle" tick={(props) => <CustomRadarChartTick {...props} />} axisLine={false} angle={90} domain={[0, 20]} />
                    <Tooltip
                        content={<ChartTooltipContent />}
                        cursor={{
                            className: "stroke-utility-brand-600  stroke-2",
                            style: { transform: "translateZ(0)" },
                        }}
                    />
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
                </RechartsRadarChart>
            </ResponsiveContainer>
        </div>
    );
};

export default KimariteRadarChart;

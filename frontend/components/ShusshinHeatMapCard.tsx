"use client";

import { shusshinGeoData } from "./shusshinGeoData";
import { Card } from "../components/ui/card";
// Example data: [ [location, count], ... ]
// Default sample list
const shusshinDataDefault = [
  ["Hyogo-ken, Ashiya-shi", 2],
  ["Toyama-ken, Toyama-shi", 15],
  ["Tottori-ken, Kurayoshi-shi", 5],
  ["Kagoshima-ken, Oshima-gun, Tokunoshima-cho", 8],
  ["Aomori-ken, Goshogawara-shi", 12],
  ["Mongolia, Ulaanbaatar", 54],
  ["Chiba-ken, Kashiwa-shi", 15],
  ["Tokyo-to, Edogawa-ku", 46],
];

// We'll use react-simple-maps (aliased to react19-simple-maps in Vite config)

import { ComposableMap, Geographies, Geography, Marker } from "@vnedyalk0v/react19-simple-maps";
import type { Longitude, Latitude, Coordinates as MapCoordinates } from "@vnedyalk0v/react19-simple-maps";
// Import the world topojson locally (Vite supports importing JSON)
// Make sure countries-110m.json is in src/assets/
// @ts-ignore
import countries110m from "@/lib/data/countries-110m.json";

interface ShusshinHeatMapCardProps {
  shusshinCounts?: Record<string, number>;
}

export function ShusshinHeatMapCard({ shusshinCounts }: ShusshinHeatMapCardProps) {
  // Prepare marker data with lat/lng

  // Use branded types from the map package
  type MarkerType = { name: string; count: number; coordinates: MapCoordinates };
  const sourceData: any[] = shusshinCounts && Object.keys(shusshinCounts).length > 0
    ? (Object.entries(shusshinCounts) as unknown as [string, number][])
    : shusshinDataDefault;

  const markers: MarkerType[] = sourceData
    .map(([loc, count]) => {
      const geo = shusshinGeoData[loc as keyof typeof shusshinGeoData];
      if (!geo) return null;
      // Use branded types for coordinates
      const lng = geo.lng as Longitude;
      const lat = geo.lat as Latitude;
      return { name: loc as string, count: count as number, coordinates: [lng, lat] as MapCoordinates };
    })
    .filter((m): m is MarkerType => Boolean(m));

  // Find max count for scaling (safe fallback)
  const maxCount = markers.length > 0 ? Math.max(...markers.map(m => m.count)) : 1;

  // Prepare top N for legend (top 6)
  const topMarkers = markers.slice().sort((a, b) => b.count - a.count).slice(0, 6);

  return (
    <Card className="flex flex-col p-4 gap-3 bg-[#F5E6C8] border-2 border-[#563861] rounded-xl shadow" style={{ color: '#563861', minWidth: 320 }}>
      <div style={{ width: '100%', marginBottom: '0.5rem' }}>
        <span style={{
          display: 'inline-block',
          fontWeight: 'bold',
          fontSize: '1.1rem',
          color: '#fff',
          background: '#563861',
          borderRadius: '0.5rem',
          padding: '0.25rem 1rem',
          letterSpacing: '0.05em',
          margin: '0 auto',
        }}>
          Rikishi Shusshin Heat Map
        </span>
      </div>
      <div style={{ display: 'flex', gap: 12, alignItems: 'flex-start', flexWrap: 'wrap' }}>
        <div style={{ flex: 1, minWidth: 320 }}>
          <div style={{ width: '100%', height: 320, background: '#f9f6ef', borderRadius: 12, overflow: 'hidden', border: '1px solid #e0a3c2' }}>
            <ComposableMap
              projection="geoMercator"
              projectionConfig={{
                scale: 900, // higher = more zoom
                center: [135, 40] as [Longitude, Latitude] // [longitude, latitude] to focus on Japan/Mongolia
              }}
              style={{ width: '100%', height: '100%' }}
            >
              <Geographies geography={countries110m}>
                {({ geographies }: { geographies: any[] }) =>
                  geographies.map((geo: any, idx: number) => (
                    <Geography
                      key={geo.id || geo.rsmKey || idx}
                      geography={geo}
                      fill="#e0a3c2"
                      stroke="#fff"
                      strokeWidth={0.2}
                    />
                  ))
                }
              </Geographies>
              {markers.length === 0 && (
                <text x="50%" y="50%" textAnchor="middle" style={{ fill: '#563861', fontSize: 14 }}>
                  No shusshin data available
                </text>
              )}
              {markers.map((m) => (
                <Marker key={m.name} coordinates={m.coordinates}>
                  {/* cap radius for readability */}
                  <circle
                    r={Math.min(6 + (m.count / maxCount) * 18, 22)}
                    fill="#563861"
                    fillOpacity={0.25 + 0.65 * (m.count / maxCount)}
                    stroke="#563861"
                    strokeWidth={1.2}
                  />
                  {/* simple title for tooltip on hover */}
                  <title>{`${m.name}: ${m.count}`}</title>
                  <text
                    textAnchor="middle"
                    y={-8 - (m.count / maxCount) * 8}
                    style={{ fontFamily: 'inherit', fontSize: 10, fill: '#563861', fontWeight: 700 }}
                  >
                    {m.count}
                  </text>
                </Marker>
              ))}
            </ComposableMap>
          </div>
        </div>

        <aside style={{ width: 220, minWidth: 180, background: '#fff', borderRadius: 12, padding: 12, border: '1px solid #e0a3c2' }}>
          <div style={{ fontWeight: 800, color: '#563861', marginBottom: 8 }}>Top Birthplaces</div>
          {topMarkers.length === 0 && (
            <div style={{ color: '#6b7280' }}>No data</div>
          )}
          <ul style={{ listStyle: 'none', padding: 0, margin: 0, display: 'flex', flexDirection: 'column', gap: 8 }}>
            {topMarkers.map((t) => (
              <li key={t.name} title={t.name} style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 8 }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                  <span style={{ display: 'inline-block', width: 10, height: 10, borderRadius: 9999, background: '#563861', opacity: 0.6 }} />
                  <span style={{ fontSize: 13, color: '#374151' }}>{t.name.length > 26 ? `${t.name.slice(0, 24)}…` : t.name}</span>
                </div>
                <div style={{ fontWeight: 800, color: '#563861' }}>{t.count}</div>
              </li>
            ))}
          </ul>
        </aside>
      </div>
    </Card>
  );
}

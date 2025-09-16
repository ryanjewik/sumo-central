
import { shusshinGeoData } from "./shusshinGeoData";
import { Card } from "../components/ui/card";
// Example data: [ [location, count], ... ]
const shusshinData = [
  ["Hyogo-ken, Ashiya-shi", 2],
  ["Toyama-ken, Toyama-shi", 15],
  ["Tottori-ken, Kurayoshi-shi", 5],
  ["Kagoshima-ken, Oshima-gun, Tokunoshima-cho", 8],
  ["Aomori-ken, Goshogawara-shi", 12],
  ["Mongolia, Uvs, Ulaangom", 1],
  ["Mongolia, Ulaanbaatar", 54],
  ["Aomori-ken, Kitatsugaru-gun, Nakadomari-machi", 2],
  ["Chiba-ken, Kashiwa-shi", 15],
  ["Saitama-ken, Asaka-shi", 4],
  ["Hokkaido, Iwanai-gun, Iwanai-cho", 2],
  ["Fukushima-ken, Fukushima-shi", 13],
  ["Yamanashi-ken, Kofu-shi", 20],
  ["Iwate-ken, Morioka-shi", 7],
  ["Ishikawa-ken, Hosu-gun, Anamizu-machi", 8],
  ["Mongolia, Ulaanbaatar - Mongolia, Gobi-Altai", 2],
  ["Chiba-ken, Matsudo-shi", 16],
  ["Tokyo-to, Edogawa-ku", 46],
  ["Saitama-ken, Koshigaya-shi", 12],
  ["Bulgaria, Sofia - Bulgaria, Yambol", 1],
  ["Nagasaki-ken, Hirado-shi", 6],
  ["Nagano-ken, Kiso-gun, Agematsu-machi", 3],
];

// We'll use react-simple-maps (aliased to react19-simple-maps in Vite config)

import { ComposableMap, Geographies, Geography, Marker } from "@vnedyalk0v/react19-simple-maps";
import type { Longitude, Latitude, Coordinates as MapCoordinates } from "@vnedyalk0v/react19-simple-maps";
// Import the world topojson locally (Vite supports importing JSON)
// Make sure countries-110m.json is in src/assets/
// @ts-ignore
import countries110m from '../assets/countries-110m.json';

    export function ShusshinHeatMapCard() {
  // Prepare marker data with lat/lng

  // Use branded types from the map package
  type MarkerType = { name: string; count: number; coordinates: MapCoordinates };
  const markers: MarkerType[] = shusshinData
    .map(([loc, count]) => {
      const geo = shusshinGeoData[loc as keyof typeof shusshinGeoData];
      if (!geo) return null;
      // Use branded types for coordinates
      const lng = geo.lng as Longitude;
      const lat = geo.lat as Latitude;
      return { name: loc as string, count: count as number, coordinates: [lng, lat] as MapCoordinates };
    })
    .filter((m): m is MarkerType => Boolean(m));

  // Find max count for scaling
  const maxCount = Math.max(...markers.map(m => m.count));

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
          {markers.map((m) => (
            <Marker key={m.name} coordinates={m.coordinates}>
              <circle
                r={6 + (m.count / maxCount) * 18}
                fill="#563861"
                fillOpacity={0.25 + 0.65 * (m.count / maxCount)}
                stroke="#563861"
                strokeWidth={1.2}
              />
              <text
                textAnchor="middle"
                y={-10 - (m.count / maxCount) * 10}
                style={{ fontFamily: 'inherit', fontSize: 20, fill: '#563861', fontWeight: 700 }}
              >
                {m.count}
              </text>
            </Marker>
          ))}
        </ComposableMap>
      </div>
    </Card>
  );
}

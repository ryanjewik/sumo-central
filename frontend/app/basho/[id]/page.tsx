"use client";

import React, { useEffect, useState, useMemo } from "react";
import { useParams } from "next/navigation";
import Link from 'next/link';

export default function BashoDetailPage() {
  const params = useParams() as { id?: string };
  const id = params?.id ?? "";
  const [doc, setDoc] = useState<any | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!id) return;
    let mounted = true;
    const fetchDoc = async () => {
      try {
        const res = await fetch(`/api/basho/${encodeURIComponent(id)}`, { credentials: 'include' });
        if (!mounted) return;
        if (!res.ok) {
          const text = await res.text();
          setError(`Failed to load basho: ${res.status} ${text}`);
          setLoading(false);
          return;
        }
        const data = await res.json();
        setDoc(data);
      } catch (err: any) {
        setError(String(err?.message ?? err));
      } finally {
        setLoading(false);
      }
    };

    fetchDoc();
    return () => { mounted = false; };
  }, [id]);
  // Helper: small toggle section used for each division
  function SectionToggle({ title, defaultOpen = false, children }: { title: string; defaultOpen?: boolean; children: React.ReactNode }) {
    const [open, setOpen] = useState(defaultOpen);
    return (
      <div style={{ marginTop: '1rem', border: '1px solid #e6e6e6', borderRadius: 8, overflow: 'hidden' }}>
        <button
          onClick={() => setOpen((s) => !s)}
          style={{ width: '100%', textAlign: 'left', padding: '0.6rem 0.8rem', background: '#fafafa', border: 'none', cursor: 'pointer' }}
        >
          <strong style={{ marginRight: 8 }}>{open ? '▾' : '▸'}</strong>
          <span className="app-text text-lg font-semibold">{title}</span>
        </button>
        {open && <div style={{ padding: '0.75rem' }}>{children}</div>}
      </div>
    );
  }

  // Flattens matches from the `days` structure into an array and annotates with date + division
  // Supports structure: days -> division -> date -> [matches]
  const matchesArray = useMemo(() => {
    if (!doc) return [] as any[];
    const days = doc.days ?? {};
    const rows: any[] = [];
    try {
      Object.entries(days).forEach(([maybeDivision, maybeValue]) => {
        // If the value is an object where values are arrays, treat the key as a division
        if (maybeValue && typeof maybeValue === 'object' && !Array.isArray(maybeValue)) {
          const inner = maybeValue as Record<string, any>;
          const hasArrayValues = Object.values(inner).some((v) => Array.isArray(v));
          if (hasArrayValues) {
            Object.entries(inner).forEach(([dateKey, arr]) => {
              if (!Array.isArray(arr)) return;
              arr.forEach((m: any) => rows.push(Object.assign({}, m, { _basho_date: dateKey, _division: maybeDivision })));
            });
            return;
          }
        }

        // Fallback: if value is an array, treat the outer key as date
        if (Array.isArray(maybeValue)) {
          maybeValue.forEach((m: any) => {
            const dateRaw = (m.match_date ?? m.start_date ?? maybeDivision) as string | undefined;
            rows.push(Object.assign({}, m, { _basho_date: dateRaw }));
          });
          return;
        }

        // Another fallback: nested object where arrays sit one level deeper
        if (maybeValue && typeof maybeValue === 'object') {
          Object.entries(maybeValue as Record<string, any>).forEach(([k, v]) => {
            if (Array.isArray(v)) {
              v.forEach((m: any) => rows.push(Object.assign({}, m, { _basho_date: k, _division: maybeDivision })));
            }
          });
        }
      });
    } catch (e) {
      // ignore and return what we collected
    }
    return rows;
  }, [doc]);

  // Group matches by division (prefer annotated _division from parsing)
  const matchesByDivision = useMemo(() => {
    const groups: Record<string, any[]> = {};
    matchesArray.forEach((m: any) => {
      const div = (m._division ?? m.division ?? m.div ?? 'Unknown') as string;
      if (!groups[div]) groups[div] = [];
      groups[div].push(m);
    });
    // sort each group's matches by date (newest first). If dates tie, fallback to match number
    Object.keys(groups).forEach((k) => {
      groups[k].sort((a: any, b: any) => {
        const parseDate = (v: any) => {
          try {
            const s = v ?? '';
            const t = Date.parse(String(s));
            return Number.isNaN(t) ? 0 : t;
          } catch (e) {
            return 0;
          }
        };

        const da = parseDate(a._basho_date ?? a.match_date ?? a.start_date ?? a.start ?? '');
        const db = parseDate(b._basho_date ?? b.match_date ?? b.start_date ?? b.start ?? '');
        if (db !== da) return db - da; // newer (larger timestamp) first

        const na = a.match_number ?? a.match_num ?? a.match ?? 0;
        const nb = b.match_number ?? b.match_num ?? b.match ?? 0;
        return na - nb;
      });
    });
    return groups;
  }, [matchesArray]);

  // Division order to present (top to bottom)
  const DIVISION_ORDER = ['Makuuchi', 'Juryo', 'Makushita', 'Sandanme', 'Jonidan', 'Jonokuchi'];

  // Helper to get yusho values in preferred order; fallback to TBD
  function orderedYushos(src: any) {
    const base = src ?? {};
    const out: { key: string; value: any }[] = [];
    DIVISION_ORDER.forEach((d) => {
      // common key variants
      const k1 = `${d.toLowerCase()}_yusho`;
      const k2 = `${d.toLowerCase()}`;
      const val = base[k1] ?? base[k2] ?? null;
      out.push({ key: d, value: val });
    });
    return out;
  }

  function shortDate(s: string | undefined) {
    if (!s) return '';
    // remove time suffix like T00:00:00
    const idx = s.indexOf('T');
    return idx === -1 ? s : s.slice(0, idx);
  }

  function renderRikishiLink(possible: any, display?: string) {
    // possible may be object or id or string
    if (!possible) return display ?? '';
    let idVal: any = null;
    let label = display ?? '';
    if (typeof possible === 'object') {
      idVal = possible.id ?? possible._id ?? possible.rikishi_id ?? possible.rikishiId ?? possible.id_str ?? null;
      label = label || possible.shikona || possible.name || String(idVal ?? '');
    } else {
      // primitive: could be numeric id or shikona
      // if it looks numeric, treat as id
      const s = String(possible);
      if (/^\d+$/.test(s)) idVal = s;
      label = label || s;
    }

    if (idVal) {
      return <Link href={`/rikishi/${idVal}`} className="app-text" style={{ color: '#0969da' }}>{label}</Link>;
    }
    return <span className="app-text">{label}</span>;
  }

  return (
    <>
      <div id="background"></div>
      {/* Inline page-specific styles to ensure basho card and table theme match site colors */}
      <style>{`
        .basho-card { position: relative; z-index: 0; }
        .basho-card-border { border-color: #563861 !important; }
        .basho-yusho-badge { padding: 0.35rem 0.5rem; background: rgba(86,56,97,0.04); border-radius: 6px; border: 1px solid rgba(86,56,97,0.08); font-family: monospace; }
        .basho-table { border-collapse: collapse; background: rgba(224,163,194,0.04); }
        .basho-table, .basho-table th, .basho-table td { border: 1px solid rgba(224,163,194,0.6); }
        .basho-table th, .basho-table td { padding: 0.5rem 0.75rem; }
        .basho-table thead th { background: rgba(224,163,194,0.14); color: rgba(86,56,97,0.92); }
        .basho-table tbody tr:hover { background: rgba(86,56,97,0.02); }
      `}</style>
  <div className="content-box extra-vertical-pad mt-60 px-6 relative z-0 w-full flex flex-col items-center">

  {loading && <div className="app-text text-center">Loading...</div>}
  {!loading && error && <div className="text-center text-crimson">{error}</div>}

        {!loading && !error && doc && (
          <div className="w-full flex flex-col items-center">
            <div style={{ width: '92%', maxWidth: 1800, margin: '0 auto' }} className="w-full">
            {/* Main basho card - centered and themed */}
            <div className="basho-card border-2 basho-card-border bg-white rounded-lg shadow-md p-6 flex flex-col gap-4 items-center w-full mt-8">
              <div className="flex-1 w-full flex flex-col items-center md:items-start">
                <h2 className="app-text text-xl font-semibold mb-2 text-center md:text-left">{(doc.basho?.basho_name ?? doc.title ?? doc.basho?.name) || `Basho ${id}`}</h2>
                <div className="w-full flex flex-col md:flex-row md:items-center md:gap-6 justify-center md:justify-start mb-2">
                  <div className="text-base"><strong>Basho:</strong> <span className="app-text">{id}</span></div>
                  <div className="text-base"><strong>Location:</strong> <span className="app-text">{(doc.basho?.location ?? doc.location) ? String(doc.basho?.location ?? doc.location) : 'NA'}</span></div>
                  <div className="text-base"><strong>Start:</strong> <span className="app-text">{shortDate(String(doc.basho?.start_date ?? doc.start_date ?? ''))}</span></div>
                  <div className="text-base"><strong>End:</strong> <span className="app-text">{shortDate(String(doc.basho?.end_date ?? doc.end_date ?? ''))}</span></div>
                </div>

                <div className="text-base text-gray-700 font-semibold mb-2">Yusho winners</div>
                <div className="flex gap-2 flex-wrap justify-center md:justify-start w-full">
                  {orderedYushos(doc.basho ?? doc).map(({ key, value }) => (
                    <div key={key} className="basho-yusho-badge">
                      <div className="text-xs text-gray-600 mb-1">{key}</div>
                      <div>
                        {value == null || (Array.isArray(value) && value.length === 0) ? (
                          <span className="app-text">TBD</span>
                        ) : Array.isArray(value) ? (
                          value.map((it: any, idx: number) => <div key={idx}>{renderRikishiLink(it)}</div>)
                        ) : (
                          renderRikishiLink(value)
                        )}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </div>

            {/* Divisions with matches */}
            <div className="mt-6 w-full">
              {Object.keys(matchesByDivision).length === 0 && <div className="app-text text-center py-3 text-[#666]">No matches available for this basho.</div>}
              {(() => {
                // Order divisions by DIVISION_ORDER, append any others found
                const ordered = [] as string[];
                DIVISION_ORDER.forEach((d) => { if (matchesByDivision[d]) ordered.push(d); });
                Object.keys(matchesByDivision).forEach((d) => { if (!ordered.includes(d)) ordered.push(d); });
                return ordered.map((division) => {
                  const arr = matchesByDivision[division] || [];
                  return (
                    <SectionToggle key={division} title={`${division} — ${arr.length} matches`} defaultOpen={false}>
                      <div className="overflow-x-auto">
                        <table className="basho-table w-full" style={{ tableLayout: 'auto' }}>
                          <thead>
                            <tr>
                              <th className="app-text px-3 py-2">Date</th>
                              <th className="app-text px-3 py-2">West</th>
                              <th className="app-text px-3 py-2">East</th>
                              <th className="app-text px-3 py-2">Match #</th>
                              <th className="app-text px-3 py-2">Winner</th>
                              <th className="app-text px-3 py-2">Kimarite</th>
                            </tr>
                          </thead>
                          <tbody>
                            {arr.map((m: any, i: number) => (
                              <tr key={i}>
                                <td className="app-text px-3 py-2">{shortDate(String(m._basho_date ?? m.match_date ?? m.start_date ?? ''))}</td>
                                <td className="app-text px-3 py-2">{renderRikishiLink(m.west_rikishi_id ?? m.west_rikishi ?? (m.west ? m.west : undefined), m.westshikona ?? m.west_shikona ?? m.west_shikona_name ?? (typeof m.west === 'string' ? m.west : undefined))}</td>
                                <td className="app-text px-3 py-2">{renderRikishiLink(m.east_rikishi_id ?? m.east_rikishi ?? (m.east ? m.east : undefined), m.eastshikona ?? m.east_shikona ?? m.east_shikona_name ?? (typeof m.east === 'string' ? m.east : undefined))}</td>
                                <td className="app-text px-3 py-2">{m.match_number ?? m.match_num ?? m.match ?? ''}</td>
                                <td className="app-text px-3 py-2">{(() => {
                                  const winnerId = m.winner_obj ?? m.winner_rikishi ?? m.winner_id ?? m.winner ?? m.winner_name ?? null;
                                  let winnerLabel = m.winner_shikona ?? m.winner_name ?? null;
                                  if (!winnerLabel) {
                                    if (winnerId && String(winnerId) === String(m.east_rikishi_id ?? m.east_rikishi)) winnerLabel = m.eastshikona ?? m.east_shikona ?? undefined;
                                    else if (winnerId && String(winnerId) === String(m.west_rikishi_id ?? m.west_rikishi)) winnerLabel = m.westshikona ?? m.west_shikona ?? undefined;
                                  }
                                  return renderRikishiLink(winnerId, winnerLabel ?? undefined);
                                })()}</td>
                                <td className="app-text px-3 py-2">{m.kimarite ?? m.kimarite_name ?? ''}</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </SectionToggle>
                  );
                });
              })()}
            </div>
            </div>
          </div>
        )}

        {!loading && !error && !doc && <div className="app-text">No basho page found.</div>}
      </div>
    </>
  );
}

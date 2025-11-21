"use client";

import React, { useEffect, useState } from 'react'
import dynamic from 'next/dynamic'
import { AuthProvider, useAuth } from '../context/AuthContext'
import HighlightedRikishiCard from './HighlightedRikishiCard'
import { RikishiTable } from './application/rikishi_table'
import KimariteRadarChart from './application/charts/KimariteRadarChart'
import LeaderboardTable from './leaderboard_table'
import LoginDialog from './login_dialog'
import ClimbingRikishiCard from './ClimbingRikishiCard'
import { ChartBarInteractive } from './heya_bar_chart'
import { ShusshinHeatMapCard } from './ShusshinHeatMapCard'
import ForumListClient from './ForumListClient'

const HighlightedMatchCard = dynamic(() => import('./HighlightedMatchCard'), { ssr: false });
const UpcomingMatchesList = dynamic(() => import('./upcoming_matches_list'), { ssr: false });
const DynamicRecentMatches = dynamic(() => import('./recent_matches_list'), { ssr: false });

interface HomepageClientProps {
  initialHomepage?: any;
  initialUpcoming?: any;
}

function InnerApp({ initialHomepage, initialUpcoming }: HomepageClientProps) {
  // Login dialog state
  const [loginOpen, setLoginOpen] = useState(false);
  // Get auth from provider
  const { user, logout } = useAuth();
  // Leaderboard state (populated from backend). No sample fallback data here.
  const [leaderboard, setLeaderboard] = useState<{ id: string; username: string; correctPredictions: number }[] | null>(null);

  useEffect(() => {
    let mounted = true;
    (async () => {
      try {
        const res = await fetch('/api/leaderboard', { credentials: 'include' });
        if (!mounted) return;
        if (!res.ok) return;
        const data = await res.json();
        if (mounted && data && Array.isArray(data.leaderboard)) {
          setLeaderboard(data.leaderboard);
        }
      } catch {
        // ignore
      }
    })();
    return () => { mounted = false };
  }, []);

  const [homepage, setHomepage] = useState<any | null>(initialHomepage ?? null);
  const [upcomingDoc, setUpcomingDoc] = useState<any | null>(initialUpcoming ?? null);
  const [upcomingDocError, setUpcomingDocError] = useState<string | null>(null);
  const [upcomingDocLoading, setUpcomingDocLoading] = useState(false);
  const [homepageError, setHomepageError] = useState<string | null>(null);
  const [homepageLoading, setHomepageLoading] = useState(false);
  const [bashoUpcomingMatches, setBashoUpcomingMatches] = useState<Record<string, unknown>[] | null>(null);
  const [, setBashoLoading] = useState(false);
  const [, setBashoError] = useState<string | null>(null);

  useEffect(() => {
    let mounted = true;
    (async () => {
      if (homepage) return; // server provided initial
      setHomepageLoading(true);
      try {
        const res = await fetch('/api/homepage', { credentials: 'include' });
        if (!mounted) return;
        if (!res.ok) {
          setHomepageError(`failed to load homepage: ${res.status}`);
          setHomepageLoading(false);
          return;
        }
        const doc = await res.json();
        if (mounted) setHomepage(doc as Record<string, unknown>);
      } catch (_err) {
        if (mounted) setHomepageError((_err as Error)?.message || 'network error');
      } finally {
        if (mounted) setHomepageLoading(false);
      }
    })();
    return () => { mounted = false; };
  }, [homepage]);

  useEffect(() => {
    let mounted = true;
    (async () => {
      setUpcomingDocLoading(true);
      setUpcomingDocError(null);
      try {
        const res = await fetch('/api/upcoming', { credentials: 'include' });
        if (!mounted) return;
        if (!res.ok) {
          setUpcomingDoc(null);
          setUpcomingDocLoading(false);
          return;
        }
        const doc = await res.json();
        if (mounted) setUpcomingDoc(doc as Record<string, unknown>);
      } catch (err: unknown) {
        if (mounted) setUpcomingDoc(null);
        if (mounted) setUpcomingDocError(err && typeof err === 'object' && 'message' in err ? String((err as { message?: unknown }).message) : 'network error');
      } finally {
        if (mounted) setUpcomingDocLoading(false);
      }
    })();
    return () => { mounted = false; };
  }, []);

  // keep the rest of the interactive page logic (animations, derived stats) in the client wrapper
  const [searchBarVisible, setSearchBarVisible] = useState(false);
  const [sideBarsVisible, setSideBarsVisible] = useState(false);
  const [mainContentVisible, setMainContentVisible] = useState(false);

  useEffect(() => {
    const t = setTimeout(() => {
      setSearchBarVisible(true);
      setSideBarsVisible(true);
      setMainContentVisible(true);
    }, 120);
    return () => clearTimeout(t);
  }, []);

  // derive stats from homepage
  const avgStats = homepage?.avg_stats ?? {};
  const avgWeightTotal = Number(avgStats.average_weight_kg ?? avgStats.average_weight ?? 157.42);
  const avgHeightTotal = Number(avgStats.average_height_cm ?? avgStats.average_height ?? 185.27);
  const yushoWeight = Number(avgStats.makuuchi_yusho_avg_weight_kg ?? avgWeightTotal);
  const yushoHeight = Number(avgStats.makuuchi_yusho_avg_height_cm ?? avgHeightTotal);

  // compute bashoUpcomingMatches from homepage or upcomingDoc
  useEffect(() => {
    let mounted = true;
    const rawFromUpcoming = (upcomingDoc as any)?.upcoming_matches;
    const rawFromHomepage = rawFromUpcoming ?? (homepage as any)?.upcoming_matches;
    const getString = (o: Record<string, unknown> | undefined, ...keys: string[]) => {
      if (!o) return undefined;
      for (const k of keys) {
        const v = o[k];
        if (typeof v === 'string') return v;
        if (typeof v === 'number') return String(v);
      }
      return undefined;
    };

    if (Array.isArray(rawFromHomepage) && rawFromHomepage.length > 0) {
      const rawMatches: Record<string, unknown>[] = rawFromHomepage;
      const normalized = rawMatches.map((m: Record<string, unknown>, idx: number) => {
        const id = getString(m, 'match_number') ?? getString(m, 'id') ?? String(idx + 1);
        const rikishiWest = getString(m, 'westshikona', 'west_shikona', 'west_name', 'west', 'west_name_local');
        const rikishiEast = getString(m, 'eastshikona', 'east_shikona', 'east_name', 'east', 'east_name_local');
        return {
          id: String(id),
          rikishi1: rikishiWest || rikishiEast || 'TBD',
          rikishi2: rikishiEast || rikishiWest || 'TBD',
          rikishi1Rank: getString(m, 'west_rank') ?? getString(m, 'west_rank_label'),
          rikishi2Rank: getString(m, 'east_rank') ?? getString(m, 'east_rank_label'),
          date: getString(m, 'match_date') ?? getString(homepage as Record<string, unknown>, 'start_date') ?? undefined,
          venue: getString(m, 'venue') ?? getString(homepage as Record<string, unknown>, 'venue'),
          ai_prediction: getString(m, 'AI_prediction') ?? getString(m, 'ai_prediction') ?? getString(m, 'aiPrediction'),
          rikishi1_id: getString(m, 'west_rikishi_id', 'west_id', 'westId', 'rikishi1_id') ?? undefined,
          rikishi2_id: getString(m, 'east_rikishi_id', 'east_id', 'eastId', 'rikishi2_id') ?? undefined,
          west_rikishi_id: getString(m, 'west_rikishi_id', 'west_id', 'westId') ?? undefined,
          east_rikishi_id: getString(m, 'east_rikishi_id', 'east_id', 'eastId') ?? undefined,
          west_rikishi: (m as any)['west_rikishi'] ?? (m as any)['westRikishi'] ?? undefined,
          east_rikishi: (m as any)['east_rikishi'] ?? (m as any)['eastRikishi'] ?? undefined,
          canonical_id: getString(m, 'canonical_id', 'canonicalId') ?? undefined,
        };
      });

      if (mounted) setBashoUpcomingMatches(normalized);
      return () => { mounted = false; };
    }

    const bashoId = homepage?.most_recent_basho;
    if (!bashoId) {
      setBashoUpcomingMatches(null);
      setBashoError(null);
      setBashoLoading(false);
      return () => { mounted = false; };
    }

    (async () => {
      setBashoLoading(true);
      setBashoError(null);
      try {
        const res = await fetch(`/api/basho/${encodeURIComponent(String(bashoId))}`, { credentials: 'include' });
        if (!mounted) return;
        if (!res.ok) {
          setBashoError(`failed to load basho: ${res.status}`);
          setBashoUpcomingMatches([]);
          setBashoLoading(false);
          return;
        }
        const doc = await res.json();
        const rawMatches: Record<string, unknown>[] = Array.isArray(doc?.upcoming_matches) ? doc.upcoming_matches : [];
        const normalized = rawMatches.map((m: Record<string, unknown>, idx: number) => {
          const id = getString(m, 'match_number') ?? getString(m, 'id') ?? String(idx + 1);
          const rikishiWest = getString(m, 'westshikona', 'west_shikona', 'west_name', 'west', 'west_name_local');
          const rikishiEast = getString(m, 'eastshikona', 'east_shikona', 'east_name', 'east', 'east_name_local');
          return {
            id: String(id),
            rikishi1: rikishiWest || rikishiEast || 'TBD',
            rikishi2: rikishiEast || rikishiWest || 'TBD',
            rikishi1Rank: getString(m, 'west_rank') ?? getString(m, 'west_rank_label'),
            rikishi2Rank: getString(m, 'east_rank') ?? getString(m, 'east_rank_label'),
            date: getString(m, 'match_date') ?? getString(doc as Record<string, unknown>, 'start_date') ?? undefined,
            venue: getString(m, 'venue') ?? getString(doc as Record<string, unknown>, 'venue'),
            ai_prediction: getString(m, 'AI_prediction') ?? getString(m, 'ai_prediction') ?? getString(m, 'aiPrediction'),
            rikishi1_id: getString(m, 'west_rikishi_id', 'west_id', 'westId', 'rikishi1_id') ?? undefined,
            rikishi2_id: getString(m, 'east_rikishi_id', 'east_id', 'eastId', 'rikishi2_id') ?? undefined,
            west_rikishi_id: getString(m, 'west_rikishi_id', 'west_id', 'westId') ?? undefined,
            east_rikishi_id: getString(m, 'east_rikishi_id', 'east_id', 'eastId') ?? undefined,
            west_rikishi: (m as any)['west_rikishi'] ?? (m as any)['westRikishi'] ?? undefined,
            east_rikishi: (m as any)['east_rikishi'] ?? (m as any)['eastRikishi'] ?? undefined,
            canonical_id: getString(m, 'canonical_id', 'canonicalId') ?? undefined,
          };
        });

        if (mounted) setBashoUpcomingMatches(normalized);
      } catch (err: unknown) {
        const msg = (err && typeof err === 'object' && 'message' in err) ? String((err as { message?: unknown }).message) : String(err ?? 'network error');
        if (mounted) setBashoUpcomingMatches([]);
        if (mounted) setBashoError(msg || 'network error');
      } finally {
        if (mounted) setBashoLoading(false);
      }
    })();

    return () => { mounted = false; };
  }, [homepage, upcomingDoc]);

  return (
    <AuthProvider>
      <div id="background">
        <div style={{ maxWidth: 1100, margin: '1rem auto', padding: '0 1rem' }}>
          {homepageLoading && <div style={{ color: '#563861' }}>Loading homepage...</div>}
          {homepageError && <div style={{ color: 'red' }}>{homepageError}</div>}
          {homepage && (
            <div style={{ marginTop: 8, fontSize: '0.9rem', color: '#444' }}>
              <strong>DEV:</strong> upcoming doc: {upcomingDoc ? (Array.isArray((upcomingDoc as any).upcoming_matches) ? (upcomingDoc as any).upcoming_matches.length + ' items' : 'present') : (Array.isArray((homepage as any).upcoming_matches) ? (homepage as any).upcoming_matches.length + ' items (homepage)' : 'none')}
            </div>
          )}
        </div>
        <div className="content-box" style={{ marginTop: '13rem' }}>
          <div className="left-bar" style={{ display: 'flex', flexDirection: 'column', height: '100%', transform: sideBarsVisible ? 'translateX(0)' : 'translateX(-80px)', opacity: sideBarsVisible ? 1 : 0, transition: 'transform 1.1s cubic-bezier(0.77,0,0.175,1), opacity 1.1s cubic-bezier(0.77,0,0.175,1)', pointerEvents: sideBarsVisible ? 'auto' : 'none', zIndex: 20001, position: 'relative' }}>
            <HighlightedRikishiCard rikishi={homepage?.top_rikishi} />
            <div style={{ flex: 1, paddingBottom: '1rem' }}>
              <RikishiTable topRikishiOrdered={homepage?.top_rikishi_ordered} />
            </div>
            <div style={{ flex: 1, gap: '1rem', display: 'flex', flexDirection: 'column' }}>
              <KimariteRadarChart kimariteCounts={homepage?.kimarite_usage_most_recent_basho} />
              <LeaderboardTable leaderboard={leaderboard ?? []} />
              <div style={{ marginTop: '1.5rem' }}>
                {/* SumoTicketsCard is presentational and server-rendered elsewhere */}
              </div>
            </div>
          </div>
          <div className="main-content app-text" style={{ opacity: mainContentVisible ? 1 : 0, transition: 'opacity 1.1s cubic-bezier(0.77,0,0.175,1)', pointerEvents: mainContentVisible ? 'auto' : 'none', zIndex: 20001, position: 'relative' }}>
            {(() => {
              const highlightedFromUpcoming = (upcomingDoc as any)?.upcoming_highlighted_match;
              const highlighted = highlightedFromUpcoming ?? (homepage as any)?.highlighted_match;
              const allowVoting = Boolean(highlightedFromUpcoming);
              return <HighlightedMatchCard match={highlighted} onOpenLogin={() => setLoginOpen(true)} allowVoting={allowVoting} />
            })()}

            <div className="dashboard-section-wrapper" style={{ width: '100%', margin: '2rem 0', border: '4px solid #563861', borderRadius: '1.2rem', background: 'linear-gradient(135deg, #f5e6c8 0%, #e0a3c2 100%)', boxShadow: '0 2px 8px rgba(0,0,0,0.06)', padding: '2.5rem 1.5rem', display: 'flex', flexDirection: 'column', alignItems: 'center', gap: '2rem', flexWrap: 'wrap' }}>
              <div style={{ display: 'flex', flexDirection: 'row', gap: '1rem', flexWrap: 'wrap', justifyContent: 'center', width: '100%', maxWidth: 900, alignItems: 'stretch' }}>
                <div style={{ display: 'flex', flexDirection: 'column', minWidth: 170, maxWidth: 220, width: '100%', flex: 1, alignSelf: 'stretch' }}>
                  <ClimbingRikishiCard rikishi={homepage?.fast_climber} />
                </div>

                <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gridTemplateRows: 'auto 1fr', gap: '1rem', minWidth: 320, maxWidth: 650, flex: 1, height: '100%', alignSelf: 'stretch' }}>
                  <div className="average-weight-card" style={{ background: '#F5E6C8', borderRadius: '1rem', boxShadow: '0 2px 8px rgba(0,0,0,0.08)', border: '2px solid #563861', padding: '1rem 1.2rem', minWidth: 0, width: '100%', display: 'flex', flexDirection: 'column', alignItems: 'center', gap: '1rem', transition: 'box-shadow 0.18s', cursor: 'pointer' }}>
                    <span style={{ display: 'inline-block', fontWeight: 'bold', fontSize: '0.95rem', color: '#fff', background: '#563861', borderRadius: '0.5rem', padding: '0.18rem 0.7rem', letterSpacing: '0.05em', marginBottom: '0.5rem' }}>Average Yusho Weight</span>
                    <span style={{ fontWeight: 700, fontSize: '1.6rem', color: '#563861', fontFamily: 'inherit' }}>{yushoWeight.toFixed(2)}kg</span>
                  </div>

                  <div className="average-height-card" style={{ background: '#F5E6C8', borderRadius: '1rem', boxShadow: '0 2px 8px rgba(0,0,0,0.08)', border: '2px solid #563861', padding: '1rem 1.2rem', minWidth: 0, width: '100%', display: 'flex', flexDirection: 'column', alignItems: 'center', gap: '1rem', transition: 'box-shadow 0.18s', cursor: 'pointer' }}>
                    <span style={{ display: 'inline-block', fontWeight: 'bold', fontSize: '0.95rem', color: '#fff', background: '#563861', borderRadius: '0.5rem', padding: '0.18rem 0.7rem', letterSpacing: '0.05em', marginBottom: '0.5rem' }}>Average Yusho Height</span>
                    <span style={{ fontWeight: 700, fontSize: '1.6rem', color: '#563861', fontFamily: 'inherit' }}>{yushoHeight.toFixed(2)}cm</span>
                  </div>

                  <div style={{ gridColumn: '1 / span 2', background: '#F5E6C8', borderRadius: '1rem', boxShadow: '0 2px 8px rgba(0,0,0,0.08)', border: '2px solid #563861', padding: '0.8rem 1rem', display: 'flex', flexDirection: 'column', height: '100%', transition: 'box-shadow 0.18s', cursor: 'pointer' }}>
                    <span style={{ display: 'inline-block', fontWeight: 'bold', fontSize: '0.95rem', color: '#fff', background: '#563861', borderRadius: '0.5rem', padding: '0.18rem 0.7rem', letterSpacing: '0.05em', marginBottom: '0.5rem', alignSelf: 'flex-start' }}>Heya Average Rank</span>
                    <div style={{ flex: 1, display: 'flex' }}>
                      <div style={{ flex: 1, display: 'flex' }}>
                        <ChartBarInteractive heyaAvgRank={homepage?.heya_avg_rank} heyaRikishiCount={homepage?.heya_counts} />
                      </div>
                    </div>
                  </div>
                </div>
              </div>

              <div style={{ width: '100%', maxWidth: 900, background: '#F5E6C8', borderRadius: '1rem', boxShadow: '0 2px 8px rgba(0,0,0,0.08)', border: '2px solid #563861', padding: '0.8rem 1rem', transition: 'box-shadow 0.18s', cursor: 'pointer' }}>
                <ShusshinHeatMapCard shusshinCounts={homepage?.shusshin_counts} />
              </div>
            </div>

            <ForumListClient initial={[]} initialLoadFailed={false} maxItems={5} />
          </div>
          <div className="right-bar" style={{ display: 'flex', flexDirection: 'column', height: '100%', transform: sideBarsVisible ? 'translateX(0)' : 'translateX(80px)', opacity: sideBarsVisible ? 1 : 0, transition: 'transform 1.1s cubic-bezier(0.77,0,0.175,1), opacity 1.1s cubic-bezier(0.77,0,0.175,1)', pointerEvents: sideBarsVisible ? 'auto' : 'none', zIndex: 20001, position: 'relative' }}>
            <div style={{ marginBottom: '1.5rem' }}>
              <UpcomingMatchesList matches={bashoUpcomingMatches ?? []} date={(bashoUpcomingMatches && bashoUpcomingMatches.length > 0 && typeof bashoUpcomingMatches[0].date === 'string') ? (bashoUpcomingMatches[0].date as string) : undefined} onOpenLogin={() => setLoginOpen(true)} />
            </div>
            <div>
              <React.Suspense fallback={<div>Loading recent matches...</div>}>
                <DynamicRecentMatches matches={homepage?.recent_matches ? Object.values(homepage.recent_matches) : undefined} />
              </React.Suspense>
            </div>
          </div>
          <LoginDialog open={loginOpen} onClose={() => setLoginOpen(false)} />
        </div>
      </div>
    </AuthProvider>
  );
}

export default function HomepageClient(props: HomepageClientProps) {
  return <InnerApp {...props} />;
}

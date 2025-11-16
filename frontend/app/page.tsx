"use client";

import React, { useEffect, useState, Suspense } from 'react'
//import { TableCard, Table } from './components/application/table/table';
import { RikishiTable } from '../components/application/rikishi_table';
import KimariteRadarChart from "../components/application/charts/KimariteRadarChart";
import Image from 'next/image';
import LeaderboardTable from "../components/leaderboard_table";
import LoginDialog from "../components/login_dialog";
import { AuthProvider, useAuth } from '../context/AuthContext';
import ClimbingRikishiCard from '../components/ClimbingRikishiCard';
import { ChartBarInteractive } from '../components/heya_bar_chart';
import { ShusshinHeatMapCard } from '../components/ShusshinHeatMapCard';
import SearchBar from '../components/searchbar';
import RecentMatchesList from '../components/recent_matches_list';
import NavbarSelection from '../components/horizontal_list';
import ForumSection from '../components/ForumSection';
// ForumListServer is a server component and MUST NOT be imported into a
// client component (this page is a client component: "use client").
// Import the client-side list component instead to avoid server->client
// reference errors and infinite fetch loops when the page runs in the
// browser.
import ForumListClient from '../components/ForumListClient';
import HighlightedRikishiCard from '../components/HighlightedRikishiCard';
import nextDynamic from 'next/dynamic';
// Dynamically load large client-only components to avoid importing client-side
// libraries during server prerender. Using ssr: false ensures these modules
// are only evaluated in the browser which avoids initialization/circular
// issues during build prerender.
const HighlightedMatchCard = nextDynamic(() => import('../components/HighlightedMatchCard'), { ssr: false });
import SumoTicketsCard from '../components/SumoTicketsCard';
const UpcomingMatchesList = nextDynamic(() => import('../components/upcoming_matches_list'), { ssr: false });
const DynamicRecentMatches = nextDynamic(() => import('../components/recent_matches_list'), { ssr: false });
// Image intentionally not used in this file directly

function InnerApp() {
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


  // No sample upcoming matches; upcoming matches will be derived from backend documents.
  // recent matches date should be resolved by the RecentMatchesList component
  // via the basho lookup; avoid hardcoded dates here.
  // Animation state for header + page content
  // Header component renders the navbar; keep delayed reveals for other page parts
  const [searchBarVisible, setSearchBarVisible] = useState(false);
  const [sideBarsVisible, setSideBarsVisible] = useState(false);
  const [mainContentVisible, setMainContentVisible] = useState(false);

  useEffect(() => {
    // Reveal search bar, side bars, and main content after header animation completes
    const t = setTimeout(() => {
      setSearchBarVisible(true);
      setSideBarsVisible(true);
      setMainContentVisible(true);
    }, 1400);
    return () => clearTimeout(t);
  }, []);

  // Shared auth button/pill style for profile and logout buttons
  const authPillStyle: React.CSSProperties = {
    minWidth: 88,
    maxWidth: 140,
    padding: '0.45rem 0.9rem',
    borderRadius: '999px',
    border: '2px solid #563861',
    background: '#563861',
    color: '#fff',
    fontWeight: 600,
    fontSize: '1rem',
    fontFamily: 'inherit',
    display: 'inline-flex',
    alignItems: 'center',
    justifyContent: 'center',
    cursor: 'pointer',
  };

  // (Rehydration is handled by AuthProvider)
  interface Homepage {
    top_rikishi?: Record<string, unknown>;
    top_rikishi_ordered?: Record<string, unknown>[] | Record<string, Record<string, unknown>>;
    kimarite_usage_most_recent_basho?: Record<string, number>;
    avg_stats?: Record<string, unknown>;
    highlighted_match?: Record<string, unknown>;
    fast_climber?: Record<string, unknown>;
    heya_avg_rank?: Record<string, number>;
    heya_counts?: Record<string, number>;
    shusshin_counts?: Record<string, number>;
    recent_matches?: Record<string, unknown> | Record<string, Record<string, unknown>>;
    most_recent_basho?: string | number;
  }

  const [homepage, setHomepage] = useState<Homepage | null>(null);
  const [upcomingDoc, setUpcomingDoc] = useState<Record<string, unknown> | null>(null);
  const [upcomingDocError, setUpcomingDocError] = useState<string | null>(null);
  const [upcomingDocLoading, setUpcomingDocLoading] = useState(false);
  const [homepageError, setHomepageError] = useState<string | null>(null);
  const [homepageLoading, setHomepageLoading] = useState(false);
  const [bashoUpcomingMatches, setBashoUpcomingMatches] = useState<Record<string, unknown>[] | null>(null);
  const [, setBashoLoading] = useState(false);
  const [, setBashoError] = useState<string | null>(null);

  // derived stats from homepage
  const avgStats = homepage?.avg_stats ?? {};
  const avgWeightTotal = Number(avgStats.average_weight_kg ?? avgStats.average_weight ?? 157.42);
  const avgHeightTotal = Number(avgStats.average_height_cm ?? avgStats.average_height ?? 185.27);
  const yushoWeight = Number(avgStats.makuuchi_yusho_avg_weight_kg ?? avgWeightTotal);
  const yushoHeight = Number(avgStats.makuuchi_yusho_avg_height_cm ?? avgHeightTotal);

  // load homepage document from backend (Mongo)
  useEffect(() => {
    let mounted = true;
    (async () => {
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
    return () => {
      mounted = false;
    };
  }, []);

  // Try to fetch the dedicated upcoming document. If present, frontend will
  // prefer its `upcoming_matches` / `upcoming_highlighted_match`. If not
  // present, frontend falls back to `homepage.upcoming_matches` / homepage
  // highlighted match and will hide voting UI for that fallback.
  useEffect(() => {
    let mounted = true;
    (async () => {
      setUpcomingDocLoading(true);
      setUpcomingDocError(null);
      try {
        const res = await fetch('/api/upcoming', { credentials: 'include' });
        if (!mounted) return;
        if (!res.ok) {
          // Not found is expected until server writes the doc; treat as fallback
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

  // When homepage loads, fetch the most recent basho document (if present)
  useEffect(() => {
    let mounted = true;

  // Prefer the dedicated upcoming document if it exists
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
        // keep the original id as a string so downstream components can
        // compute canonical ids from other fields. Avoid coercing to a
        // numeric short id which can lead to subscribing to match_updates:<n>.
        const id = getString(m, 'match_number') ?? getString(m, 'id') ?? String(idx + 1);
        const rikishiWest = getString(m, 'westshikona', 'west_shikona', 'west_name', 'west', 'west_name_local');
        const rikishiEast = getString(m, 'eastshikona', 'east_shikona', 'east_name', 'east', 'east_name_local');
        return {
          // preserve id as string to allow canonical reconstruction later
          id: String(id),
          rikishi1: rikishiWest || rikishiEast || 'TBD',
          rikishi2: rikishiEast || rikishiWest || 'TBD',
          rikishi1Rank: getString(m, 'west_rank') ?? getString(m, 'west_rank_label'),
          rikishi2Rank: getString(m, 'east_rank') ?? getString(m, 'east_rank_label'),
          date: getString(m, 'match_date') ?? getString(homepage as Record<string, unknown>, 'start_date') ?? undefined,
          venue: getString(m, 'venue') ?? getString(homepage as Record<string, unknown>, 'venue'),
          ai_prediction: getString(m, 'AI_prediction') ?? getString(m, 'ai_prediction') ?? getString(m, 'aiPrediction'),
            // id fields so UpcomingMatchesList can fetch images by rikishi id
            rikishi1_id: getString(m, 'west_rikishi_id', 'west_id', 'westId', 'rikishi1_id') ?? undefined,
            rikishi2_id: getString(m, 'east_rikishi_id', 'east_id', 'eastId', 'rikishi2_id') ?? undefined,
            west_rikishi_id: getString(m, 'west_rikishi_id', 'west_id', 'westId') ?? undefined,
            east_rikishi_id: getString(m, 'east_rikishi_id', 'east_id', 'eastId') ?? undefined,
            // preserve nested rikishi objects (if enrichment has already run server-side)
            west_rikishi: (m as any)['west_rikishi'] ?? (m as any)['westRikishi'] ?? undefined,
            east_rikishi: (m as any)['east_rikishi'] ?? (m as any)['eastRikishi'] ?? undefined,
            // server may already attach a canonical composite id; preserve it
            canonical_id: getString(m, 'canonical_id', 'canonicalId') ?? undefined,
        };
      });

      if (mounted) setBashoUpcomingMatches(normalized);
      return () => { mounted = false; };
    }

  // Fallback: fetch the basho document via API when neither the dedicated
  // upcoming doc nor homepage include upcoming_matches
  const bashoId = homepage?.most_recent_basho;
    if (!bashoId) {
      // clear any previous
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
          // preserve original id as string instead of coercing to numeric
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
            // id fields so UpcomingMatchesList can fetch images by rikishi id
            rikishi1_id: getString(m, 'west_rikishi_id', 'west_id', 'westId', 'rikishi1_id') ?? undefined,
            rikishi2_id: getString(m, 'east_rikishi_id', 'east_id', 'eastId', 'rikishi2_id') ?? undefined,
            west_rikishi_id: getString(m, 'west_rikishi_id', 'west_id', 'westId') ?? undefined,
            east_rikishi_id: getString(m, 'east_rikishi_id', 'east_id', 'eastId') ?? undefined,
            // preserve nested rikishi objects if provided by the basho document
            west_rikishi: (m as any)['west_rikishi'] ?? (m as any)['westRikishi'] ?? undefined,
            east_rikishi: (m as any)['east_rikishi'] ?? (m as any)['eastRikishi'] ?? undefined,
            // server may already attach a canonical composite id; preserve it
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
  // Sample forum post data
  const sampleForumPosts = [
    {
      id: 1,
      upvotes: 120,
      downvotes: 5,
      author: 'sumofan123',
      date_created: '2025-09-10',
      title: 'Who will win the next basho?',
      body: 'Let’s discuss predictions for the upcoming tournament! Who are your picks and why?'
    },
    {
      id: 2,
      upvotes: 98,
      downvotes: 2,
      author: 'rikishi_enthusiast',
      date_created: '2025-09-12',
      title: 'Best kimarite of the year',
      body: 'Share your favorite kimarite (winning technique) from this year’s matches.'
    },
    {
      id: 3,
      upvotes: 85,
      downvotes: 1,
      author: 'sumoStats',
      date_created: '2025-09-08',
      title: 'Statistical analysis: upsets',
      body: 'Let’s look at the biggest upsets in the last two weeks and what made them possible.'
    },
    {
      id: 4,
      upvotes: 80,
      downvotes: 0,
      author: 'banzukeMaster',
      date_created: '2025-09-05',
      title: 'Banzuke changes explained',
      body: 'A breakdown of the latest banzuke and what it means for the wrestlers.'
    },
    {
      id: 5,
      upvotes: 75,
      downvotes: 3,
      author: 'newbieSumo',
      date_created: '2025-09-13',
      title: 'Sumo for beginners',
      body: 'Ask questions and share tips for those new to sumo fandom!'
    },
  ];

  return (
    <>
      <div id="background">
        {/* simple dev view: show homepage document returned from Mongo */}
        <div style={{ maxWidth: 1100, margin: '1rem auto', padding: '0 1rem' }}>
          {homepageLoading && <div style={{ color: '#563861' }}>Loading homepage...</div>}
          {homepageError && <div style={{ color: 'red' }}>{homepageError}</div>}
          {/* homepage document is loaded into components below — avoid printing raw JSON in production */}
          {/* DEV: quick debug showing whether homepage.upcoming_matches is present */}
          {homepage && (
            <div style={{ marginTop: 8, fontSize: '0.9rem', color: '#444' }}>
              <strong>DEV:</strong> upcoming doc: {upcomingDoc ? (Array.isArray((upcomingDoc as any).upcoming_matches) ? (upcomingDoc as any).upcoming_matches.length + ' items' : 'present') : (Array.isArray((homepage as any).upcoming_matches) ? (homepage as any).upcoming_matches.length + ' items (homepage)' : 'none')}
            </div>
          )}
        </div>
        <div className="content-box" style={{ marginTop: '13rem' }}>
          <div
            className="left-bar"
            style={{
              display: 'flex',
              flexDirection: 'column',
              height: '100%',
              transform: sideBarsVisible ? 'translateX(0)' : 'translateX(-80px)',
              opacity: sideBarsVisible ? 1 : 0,
              transition: 'transform 1.1s cubic-bezier(0.77,0,0.175,1), opacity 1.1s cubic-bezier(0.77,0,0.175,1)',
              // ensure this column can receive pointer events and sits above decorative overlays
              pointerEvents: sideBarsVisible ? 'auto' : 'none',
              zIndex: 20001,
              position: 'relative',
            }}
          >
            {/* Highlighted Rikishi Card */}
            <HighlightedRikishiCard rikishi={homepage?.top_rikishi} />
            {/* End Highlighted Rikishi Card */}
      <div style={{ flex: 1, paddingBottom: '1rem' }}>
        <RikishiTable topRikishiOrdered={homepage?.top_rikishi_ordered} />
      </div>
            <div style={{ flex: 1, gap: '1rem', display: 'flex', flexDirection: 'column' }}>
              <KimariteRadarChart kimariteCounts={homepage?.kimarite_usage_most_recent_basho} />
              <LeaderboardTable leaderboard={leaderboard ?? []} />
              <SumoTicketsCard />
            </div>
          </div>
          <div
            className="main-content app-text"
            style={{
              opacity: mainContentVisible ? 1 : 0,
              transition: 'opacity 1.1s cubic-bezier(0.77,0,0.175,1)',
              // keep main content interactive and above decorative background
              pointerEvents: mainContentVisible ? 'auto' : 'none',
              zIndex: 20001,
              position: 'relative',
            }}
          >
            {/* Prefer highlighted from dedicated upcoming doc, fall back to homepage.highlighted_match.
                When falling back (no upcoming doc present) voting should be disabled. */}
            {
              (() => {
                const highlightedFromUpcoming = (upcomingDoc as any)?.upcoming_highlighted_match;
                const highlighted = highlightedFromUpcoming ?? (homepage as any)?.highlighted_match;
                const allowVoting = Boolean(highlightedFromUpcoming);
                return <HighlightedMatchCard match={highlighted} onOpenLogin={() => setLoginOpen(true)} allowVoting={allowVoting} />
              })()
            }

            {/* Dashboard Section: Climbing Rikishi */}

            <div className="dashboard-section-wrapper" style={{
              width: '100%',
              margin: '2rem 0',
              border: '4px solid #563861',
              borderRadius: '1.2rem',
              background: 'linear-gradient(135deg, #f5e6c8 0%, #e0a3c2 100%)',
              boxShadow: '0 2px 8px rgba(0,0,0,0.06)',
              padding: '2.5rem 1.5rem',
              display: 'flex',
              flexDirection: 'column',
              alignItems: 'center',
              gap: '2rem',
              flexWrap: 'wrap',
            }}>
              <div
                style={{
                  display: 'flex',
                  flexDirection: 'row',
                  gap: '1rem',                 // even gaps
                  flexWrap: 'wrap',
                  justifyContent: 'center',
                  width: '100%',
                  maxWidth: 900,
                  alignItems: 'stretch',       // make both columns the same height
                }}
              >
                {/* Left: Climbing Rikishi Card */}
                <div
                  style={{
                    display: 'flex',
                    flexDirection: 'column',
                    minWidth: 170,
                    maxWidth: 220,
                    width: '100%',
                    flex: 1,
                    alignSelf: 'stretch',
                  }}
                >
                  <ClimbingRikishiCard rikishi={homepage?.fast_climber} />
                </div>

                {/* Right: stats row (two cards) + heya card spanning full width below */}
                <div
                  style={{
                    display: 'grid',
                    gridTemplateColumns: '1fr 1fr',
                    gridTemplateRows: 'auto 1fr', // heya row fills remaining height
                    gap: '1rem',                  // even gaps everywhere
                    minWidth: 320,
                    maxWidth: 650,
                    flex: 1,
                    height: '100%',               // match the left column height
                    alignSelf: 'stretch',
                  }}
                >
                  {/* Average Yusho Weight */}
                  <div
                    className="average-weight-card"
                    style={{
                      background: '#F5E6C8',
                      borderRadius: '1rem',
                      boxShadow: '0 2px 8px rgba(0,0,0,0.08)',
                      border: '2px solid #563861',
                      padding: '1rem 1.2rem',
                      minWidth: 0,
                      width: '100%',
                      display: 'flex',
                      flexDirection: 'column',
                      alignItems: 'center',
                      gap: '1rem',
                      transition: 'box-shadow 0.18s',
                      cursor: 'pointer',
                    }}
                    onMouseOver={e => {
                      (e.currentTarget as HTMLElement).style.boxShadow = '0 16px 48px 0 rgba(86,56,97,0.32), 0 2px 16px 0 rgba(224,163,194,0.18)';
                    }}
                    onMouseOut={e => {
                      (e.currentTarget as HTMLElement).style.boxShadow = '0 2px 8px rgba(0,0,0,0.08)';
                    }}
                  >
                    <span
                      style={{
                        display: 'inline-block',
                        fontWeight: 'bold',
                        fontSize: '0.95rem',
                        color: '#fff',
                        background: '#563861',
                        borderRadius: '0.5rem',
                        padding: '0.18rem 0.7rem',
                        letterSpacing: '0.05em',
                        marginBottom: '0.5rem',
                      }}
                    >
                      Average Yusho Weight
                    </span>
                    <span style={{ fontWeight: 700, fontSize: '1.6rem', color: '#563861', fontFamily: 'inherit' }}>
                      {yushoWeight.toFixed(2)}kg
                    </span>
                  </div>

                  {/* Average Yusho Height */}
                  <div
                    className="average-height-card"
                    style={{
                      background: '#F5E6C8',
                      borderRadius: '1rem',
                      boxShadow: '0 2px 8px rgba(0,0,0,0.08)',
                      border: '2px solid #563861',
                      padding: '1rem 1.2rem',
                      minWidth: 0,
                      width: '100%',
                      display: 'flex',
                      flexDirection: 'column',
                      alignItems: 'center',
                      gap: '1rem',
                      transition: 'box-shadow 0.18s',
                      cursor: 'pointer',
                    }}
                    onMouseOver={e => {
                      (e.currentTarget as HTMLElement).style.boxShadow = '0 16px 48px 0 rgba(86,56,97,0.32), 0 2px 16px 0 rgba(224,163,194,0.18)';
                    }}
                    onMouseOut={e => {
                      (e.currentTarget as HTMLElement).style.boxShadow = '0 2px 8px rgba(0,0,0,0.08)';
                    }}
                  >
                    <span
                      style={{
                        display: 'inline-block',
                        fontWeight: 'bold',
                        fontSize: '0.95rem',
                        color: '#fff',
                        background: '#563861',
                        borderRadius: '0.5rem',
                        padding: '0.18rem 0.7rem',
                        letterSpacing: '0.05em',
                        marginBottom: '0.5rem',
                      }}
                    >
                      Average Yusho Height
                    </span>
                    <span style={{ fontWeight: 700, fontSize: '1.6rem', color: '#563861', fontFamily: 'inherit' }}>
                      {yushoHeight.toFixed(2)}cm
                    </span>
                  </div>

                  {/* Heya Average Rank — spans BOTH columns and fills to the same floor */}
                  <div
                    style={{
                      gridColumn: '1 / span 2',    // full width under the two cards
                      background: '#F5E6C8',
                      borderRadius: '1rem',
                      boxShadow: '0 2px 8px rgba(0,0,0,0.08)',
                      border: '2px solid #563861',
                      padding: '0.8rem 1rem',
                      display: 'flex',
                      flexDirection: 'column',
                      height: '100%',              // fills remaining height of the right column
                      transition: 'box-shadow 0.18s',
                      cursor: 'pointer',
                    }}
                    onMouseOver={e => {
                      (e.currentTarget as HTMLElement).style.boxShadow = '0 16px 48px 0 rgba(86,56,97,0.32), 0 2px 16px 0 rgba(224,163,194,0.18)';
                    }}
                    onMouseOut={e => {
                      (e.currentTarget as HTMLElement).style.boxShadow = '0 2px 8px rgba(0,0,0,0.08)';
                    }}
                  >
                    <span
                      style={{
                        display: 'inline-block',
                        fontWeight: 'bold',
                        fontSize: '0.95rem',
                        color: '#fff',
                        background: '#563861',
                        borderRadius: '0.5rem',
                        padding: '0.18rem 0.7rem',
                        letterSpacing: '0.05em',
                        marginBottom: '0.5rem',
                        alignSelf: 'flex-start',
                      }}
                    >
                      Heya Average Rank
                    </span>
                    <div style={{ flex: 1, display: 'flex' }}>
                      {/* Let the chart stretch */}
                      <div style={{ flex: 1, display: 'flex' }}>
                        <ChartBarInteractive heyaAvgRank={homepage?.heya_avg_rank} heyaRikishiCount={homepage?.heya_counts} />
                      </div>
                    </div>
                  </div>
                </div>
              </div>

              {/* Shusshin Heat Map Card Below the Heya Bar Chart */}
              <div
                style={{
                  width: '100%',
                  maxWidth: 900,
                  background: '#F5E6C8',
                  borderRadius: '1rem',
                  boxShadow: '0 2px 8px rgba(0,0,0,0.08)',
                  border: '2px solid #563861',
                  padding: '0.8rem 1rem',
                  transition: 'box-shadow 0.18s',
                  cursor: 'pointer',
                }}
                onMouseOver={e => {
                  (e.currentTarget as HTMLElement).style.boxShadow = '0 16px 48px 0 rgba(86,56,97,0.32), 0 2px 16px 0 rgba(224,163,194,0.18)';
                }}
                onMouseOut={e => {
                  (e.currentTarget as HTMLElement).style.boxShadow = '0 2px 8px rgba(0,0,0,0.08)';
                }}
              >
                <ShusshinHeatMapCard shusshinCounts={homepage?.shusshin_counts} />
              </div>
            </div>

            {/* Show top 5 newest discussions via client component (homepage is client) */}
            <ForumListClient initial={[]} initialLoadFailed={false} />
          </div>
          <div
            className="right-bar"
            style={{
              display: 'flex',
              flexDirection: 'column',
              height: '100%',
              transform: sideBarsVisible ? 'translateX(0)' : 'translateX(80px)',
              opacity: sideBarsVisible ? 1 : 0,
              transition: 'transform 1.1s cubic-bezier(0.77,0,0.175,1), opacity 1.1s cubic-bezier(0.77,0,0.175,1)',
              // ensure right column also sits above background decorations
              pointerEvents: sideBarsVisible ? 'auto' : 'none',
              zIndex: 20001,
              position: 'relative',
            }}
          >
            <div style={{ marginBottom: '1.5rem' }}>
              <UpcomingMatchesList
                matches={bashoUpcomingMatches ?? []}
                date={
                  // prefer a date derived from the first basho match or leave undefined so the component can decide
                  (bashoUpcomingMatches && bashoUpcomingMatches.length > 0 && typeof bashoUpcomingMatches[0].date === 'string')
                    ? (bashoUpcomingMatches[0].date as string)
                    : undefined
                }
                onOpenLogin={() => setLoginOpen(true)}
              />
            </div>
            {/* Load recent matches client-side too to avoid server-time import of client-only libs */}
            <div>
              {/* recent matches is a fairly large client component; load without SSR */}
              {/**
               * We use a dynamic import above for this component as well (ssr:false)
               * so it will only be evaluated client-side and won't pull its
               * dependencies into server prerender.
               */}
              <React.Suspense fallback={<div>Loading recent matches...</div>}>
                <DynamicRecentMatches matches={homepage?.recent_matches ? Object.values(homepage.recent_matches) : undefined} />
              </React.Suspense>
            </div>
          </div>
          {/* Global login dialog controlled by this page so any child can open it via onOpenLogin */}
          <LoginDialog open={loginOpen} onClose={() => setLoginOpen(false)} />
        </div>
      </div>
    </>
  );
}

export default function App() {
  return (
    <AuthProvider>
      <InnerApp />
    </AuthProvider>
  );
}

// Force dynamic rendering for this page during the build to avoid prerender-time
// circular-initialization issues. This is a small, safe change that unblocks the
// production build while we investigate the root cause (module import cycle)
// that produced "Cannot access 'J' before initialization" during prerender.
export const dynamic = 'force-dynamic';

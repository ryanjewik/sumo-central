"use client";

import { useEffect, useState } from 'react'
//import { TableCard, Table } from './components/application/table/table';
import { RikishiTable } from '../components/application/rikishi_table';
import KimariteRadarChart from "../components/application/charts/KimariteRadarChart";
import LeaderboardTable from "../components/leaderboard_table";
import LoginDialog from "../components/login_dialog";
import ClimbingRikishiCard from '../components/ClimbingRikishiCard';
import { ChartBarInteractive } from '../components/heya_bar_chart';
import { ShusshinHeatMapCard } from '../components/ShusshinHeatMapCard';
import SearchBar from '../components/searchbar';
import RecentMatchesList from '../components/recent_matches_list';
import NavbarSelection from '../components/horizontal_list';
import ForumSection from '../components/ForumSection';
import HighlightedRikishiCard from '../components/HighlightedRikishiCard';
import HighlightedMatchCard from '../components/HighlightedMatchCard';
import SumoTicketsCard from '../components/SumoTicketsCard';
import UpcomingMatchesList from '../components/upcoming_matches_list';
import Image from 'next/image';





function App() {
  // Login dialog state
  const [loginOpen, setLoginOpen] = useState(false);
  // User state (null if not logged in)
  const [user, setUser] = useState<{ id: string; username: string } | null>(null);
  // Sample upcoming matches data
  // All upcoming matches are on the same day
  const upcomingDate = '2025-09-20';

  // Sample leaderboard data (top 10 users)
  const sampleLeaderboard = [
    { username: 'sumofan123', correctPredictions: 42 },
    { username: 'rikishi_enthusiast', correctPredictions: 39 },
    { username: 'banzukeMaster', correctPredictions: 37 },
    { username: 'sumoStats', correctPredictions: 35 },
    { username: 'newbieSumo', correctPredictions: 33 },
    { username: 'yokozunaFan', correctPredictions: 30 },
    { username: 'komusubiKid', correctPredictions: 28 },
    { username: 'heyaHero', correctPredictions: 27 },
    { username: 'bashoboy', correctPredictions: 25 },
    { username: 'kimariteKing', correctPredictions: 24 },
    // ...more users
  ];


  const sampleUpcomingMatches = [
    {
      id: 1,
      rikishi1: 'Terunofuji',
      rikishi2: 'Takakeisho',
      rikishi1Rank: 'Yokozuna',
      rikishi2Rank: 'Ozeki',
      date: upcomingDate,
      venue: 'Ryogoku Kokugikan',
    },
    {
      id: 2,
      rikishi1: 'Hoshoryu',
      rikishi2: 'Wakatakakage',
      rikishi1Rank: 'Sekiwake',
      rikishi2Rank: 'Komusubi',
      date: upcomingDate,
      venue: 'Ryogoku Kokugikan',
    },
    {
      id: 3,
      rikishi1: 'Abi',
      rikishi2: 'Shodai',
      rikishi1Rank: 'Maegashira 1',
      rikishi2Rank: 'Maegashira 2',
      date: upcomingDate,
      venue: 'Ryogoku Kokugikan',
    },
  ];
  // Previous day for recent matches
  const recentMatchesDate = '2025-09-19';
  // Animation state for navbar
  const [navbarVisible, setNavbarVisible] = useState(false);
  const [searchBarVisible, setSearchBarVisible] = useState(false);
  const [sideBarsVisible, setSideBarsVisible] = useState(false);
  const [mainContentVisible, setMainContentVisible] = useState(false);

  useEffect(() => {
    // Trigger navbar animation after mount
    setTimeout(() => setNavbarVisible(true), 100);
    // Reveal search bar, side bars, and main content after navbar animation completes
    setTimeout(() => {
      setSearchBarVisible(true);
      setSideBarsVisible(true);
      setMainContentVisible(true);
    }, 1400);
  }, []);
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
      <nav
        className="navbar"
        style={{
          transform: navbarVisible ? 'translateY(0)' : 'translateY(-80px)',
          opacity: navbarVisible ? 1 : 0,
          transition: 'transform 1.3s cubic-bezier(0.77,0,0.175,1), opacity 1.3s cubic-bezier(0.77,0,0.175,1)',
          zIndex: 10,
        }}
      >
        <div className="navbar-row navbar-row-top">
          <div className="navbar-left">
            <img src="/sumo_logo.png" alt="Sumo Logo" className="navbar-logo" />
            <span className="navbar-title">Sumo App</span>
          </div>
          <div
            style={{
              overflow: 'hidden',
              width: '35vw',
              minHeight: 40,
              display: 'flex',
              alignItems: 'center',
            }}
          >
            <div
              style={{
                width: searchBarVisible ? '100%' : '0%',
                transition: 'width 0.7s cubic-bezier(0.77,0,0.175,1)',
                overflow: 'hidden',
                display: 'flex',
                alignItems: 'center',
              }}
            >
              <SearchBar />
            </div>
          </div>
          <div className="navbar-right">
            <button className="navbar-btn">L</button>
            {/* Login/User button with fixed width */}
            <button
              className="navbar-btn"
              style={{
                minWidth: 100,
                maxWidth: 120,
                width: 110,
                overflow: 'hidden',
                textOverflow: 'ellipsis',
                whiteSpace: 'nowrap',
                display: 'inline-block',
                justifyContent: 'center',
                alignItems: 'center',
                borderRadius: '999px', // pill/rounded rectangle
                border: '2px solid #563861',
                background: '#563861',
                color: '#fff',
                fontWeight: 600,
                fontSize: '1rem',
                fontFamily: 'inherit',
                transition: 'background 0.18s, color 0.18s',
              }}
              onClick={() => {
                if (!user) setLoginOpen(true);
              }}
              disabled={!!user}
            >
              {user ? user.username : 'Sign In'}
            </button>
            <LoginDialog
              open={loginOpen}
              onClose={() => setLoginOpen(false)}
              // Pass setUser to LoginDialog so it can set user on successful login
              setUser={setUser}
            />
          </div>
        </div>
        <div className="navbar-row navbar-row-bottom app-text">
          <NavbarSelection />
        </div>
      </nav>
      <div id="background">
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
            }}
          >
            {/* Highlighted Rikishi Card */}
            <HighlightedRikishiCard />
            {/* End Highlighted Rikishi Card */}
            <div style={{ flex: 1, paddingBottom: '1rem' }}>
                <RikishiTable />
            </div>
            <div style={{ flex: 1, gap: '1rem', display: 'flex', flexDirection: 'column' }}>
              <KimariteRadarChart />
              <LeaderboardTable leaderboard={sampleLeaderboard} />
              <SumoTicketsCard />
            </div>
          </div>
          <div
            className="main-content app-text"
            style={{
              opacity: mainContentVisible ? 1 : 0,
              transition: 'opacity 1.1s cubic-bezier(0.77,0,0.175,1)',
            }}
          >
            <HighlightedMatchCard />

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
                  <ClimbingRikishiCard />
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
                    <span style={{ fontWeight: 600, fontSize: '2rem', color: '#563861', fontFamily: 'inherit' }}>
                      157.42kg
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
                    <span style={{ fontWeight: 600, fontSize: '2rem', color: '#563861', fontFamily: 'inherit' }}>
                      185.27cm
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
                        <ChartBarInteractive />
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
                <ShusshinHeatMapCard />
              </div>
            </div>

            <ForumSection posts={sampleForumPosts} />
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
            }}
          >
            <div style={{ marginBottom: '1.5rem' }}>
              <UpcomingMatchesList matches={sampleUpcomingMatches} date={upcomingDate}/>
            </div>
            <RecentMatchesList date={recentMatchesDate} />
          </div>
        </div>
      </div>
    </>
  );
}

export default App

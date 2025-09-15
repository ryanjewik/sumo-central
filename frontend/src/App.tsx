import { useEffect, useState } from 'react'
import './App.css'
import './styles/globals.css'
//import { TableCard, Table } from './components/application/table/table';
import { RikishiTable } from './components/application/rikishi_table';
import { Button } from "./components/base/buttons/button";
import KimariteRadarChart from "./components/application/charts/KimariteRadarChart";
import SearchBar from './components/searchbar';
import RecentMatchesList from './components/recent_matches_list';
import NavbarSelection from './components/horizontal_list';
import ArrowUpwardIcon from '@mui/icons-material/ArrowUpward';
import ArrowDownwardIcon from '@mui/icons-material/ArrowDownward';


function App() {
  //const [count, setCount] = useState(0)
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
            <button className="navbar-btn">A</button>
          </div>
        </div>
        <div className="navbar-row navbar-row-bottom app-text">
          <NavbarSelection />
        </div>
      </nav>
      <div id="background">
        <div className="content-box">
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
            <div style={{ flex: 1, paddingBottom: '1rem' }}>
                <RikishiTable />
            </div>
            <div style={{ flex: 1, gap: '1rem', display: 'flex', flexDirection: 'column' }}>
              <KimariteRadarChart />
            </div>
            <div style={{ flex: 1, backgroundColor: 'red' }}>train</div>
          </div>
          <div
            className="main-content app-text"
            style={{
              opacity: mainContentVisible ? 1 : 0,
              transition: 'opacity 1.1s cubic-bezier(0.77,0,0.175,1)',
            }}
          >
            <div className="highlighted-match-area" style={{
              background: '#A3E0B8',
              borderRadius: '1rem',
              boxShadow: '0 2px 8px rgba(0,0,0,0.08)',
              padding: '2rem',
              marginBottom: '2rem',
              border: '4px solid #563861',
              display: 'flex',
              flexDirection: 'column',
              alignItems: 'center',
              minWidth: 320,
              width: '100%',
              maxWidth: '100%',
            }}>
              <h2 style={{ fontSize: '2rem', fontWeight: 'bold', color: '#563861', marginBottom: '1.5rem' }}>Highlighted Match</h2>
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', width: '100%', gap: '2rem' }}>
                {/* West Rikishi Stats (left) */}
                <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-end', flex: 1, gap: '0.3rem' }}>
                  <div style={{ fontWeight: 'bold', fontSize: '1.2rem', color: '#388eec' }}>Hoshoryu</div>
                  <div style={{ fontSize: '1rem', color: '#388eec' }}>Sekiwake</div>
                  <div style={{ fontSize: '0.95rem', color: '#388eec' }}>Age: 25</div>
                  <div style={{ fontSize: '0.95rem', color: '#388eec' }}>Height: 187cm</div>
                  <div style={{ fontSize: '0.95rem', color: '#388eec' }}>Weight: 155kg</div>
                  <div style={{ fontSize: '0.95rem', color: '#388eec' }}>Shushin: Mongolia</div>
                  <div style={{ fontSize: '0.95rem', color: '#388eec' }}>Heya: Tatsunami</div>
                  <div style={{ fontWeight: 'bold', fontSize: '1.1rem', color: '#388eec', marginTop: '0.5rem' }}>120 votes</div>
                </div>
                {/* Images and VS (center) */}
                <div style={{ display: 'flex', flexDirection: 'row', alignItems: 'center', gap: '2.5rem' }}>
                  <img src="/sumo_logo.png" alt="West Rikishi" style={{ width: 120, height: 160, borderRadius: '18px', border: '3px solid #388eec', objectFit: 'cover', background: '#fff' }} />
                  <div style={{ fontWeight: 'bold', fontSize: '2.5rem', color: '#563861', margin: '0 0.5rem' }}>VS</div>
                  <img src="/sumo_logo.png" alt="East Rikishi" style={{ width: 120, height: 160, borderRadius: '18px', border: '3px solid #d32f2f', objectFit: 'cover', background: '#fff' }} />
                </div>
                {/* East Rikishi Stats (right) */}
                <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-start', flex: 1, gap: '0.3rem' }}>
                  <div style={{ fontWeight: 'bold', fontSize: '1.2rem', color: '#d32f2f' }}>Takakeisho</div>
                  <div style={{ fontSize: '1rem', color: '#d32f2f' }}>Ozeki</div>
                  <div style={{ fontSize: '0.95rem', color: '#d32f2f' }}>Age: 28</div>
                  <div style={{ fontSize: '0.95rem', color: '#d32f2f' }}>Height: 175cm</div>
                  <div style={{ fontSize: '0.95rem', color: '#d32f2f' }}>Weight: 169kg</div>
                  <div style={{ fontSize: '0.95rem', color: '#d32f2f' }}>Shushin: Hyogo</div>
                  <div style={{ fontSize: '0.95rem', color: '#d32f2f' }}>Heya: Tokiwayama</div>
                  <div style={{ fontWeight: 'bold', fontSize: '1.1rem', color: '#d32f2f', marginTop: '0.5rem' }}>120 votes</div>
                </div>
              </div>
              {/* Progress Bar */}
              <div style={{ width: '100%', marginTop: '2rem', display: 'flex', justifyContent: 'center' }}>
                <div style={{ width: '80%', maxWidth: 600 }}>
                  {/* Calculate percentage for west rikishi */}
                  {(() => {
                    const westVotes = 120;
                    const eastVotes = 120;
                    const totalVotes = westVotes + eastVotes;
                    const westPercent = Math.round((westVotes / totalVotes) * 100);
                    const eastPercent = 100 - westPercent;
                    return (
                      <div style={{ display: 'flex', alignItems: 'center', width: '100%' }}>
                        <span style={{ fontWeight: 'bold', color: '#388e3c', minWidth: 40, textAlign: 'right', marginRight: 8 }}>{westPercent}%</span>
                        <div style={{ flex: 1, height: 18, background: '#F5E6C8', borderRadius: '0.75rem', position: 'relative', display: 'flex', overflow: 'hidden' }}>
                          <div style={{ background: '#388eec', width: `${westPercent}%`, height: '100%', borderRadius: '0.75rem 0 0 0.75rem' }}></div>
                          <div style={{ background: '#d32f2f', width: `${eastPercent}%`, height: '100%', borderRadius: '0 0.75rem 0.75rem 0' }}></div>
                        </div>
                        <span style={{ fontWeight: 'bold', color: '#d32f2f', minWidth: 40, textAlign: 'left', marginLeft: 8 }}>{eastPercent}%</span>
                      </div>
                    );
                  })()}
                </div>
              </div>
            </div>
            <div className="forum-area">
              <h2 style={{ fontSize: '1.5rem', fontWeight: 'bold', marginBottom: '1.5rem', color: '#563861' }}>Trending Forum Discussions</h2>
              {sampleForumPosts.map(post => (
                <a
                  key={post.id}
                  href={"/forum/" + post.id}
                  className="forum-post"
                  style={{
                    display: 'block',
                    marginBottom: '1rem',
                    padding: '1rem',
                    background: '#E4E0BE',
                    borderRadius: '8px',
                    boxShadow: '0 2px 4px rgba(0,0,0,0.05)',
                    textDecoration: 'none',
                    color: 'inherit',
                    transition: 'box-shadow 0.15s, transform 0.15s',
                  }}
                  onClick={e => {
                    // Prevent default for now if you want to handle navigation in React Router
                    // e.preventDefault();
                  }}
                  onMouseOver={e => {
                    (e.currentTarget as HTMLElement).style.boxShadow = '0 4px 16px rgba(86,56,97,0.15)';
                    (e.currentTarget as HTMLElement).style.transform = 'scale(1.02)';
                  }}
                  onMouseOut={e => {
                    (e.currentTarget as HTMLElement).style.boxShadow = '0 2px 4px rgba(0,0,0,0.05)';
                    (e.currentTarget as HTMLElement).style.transform = 'none';
                  }}
                >
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                    <div style={{ flex: 1 }}>
                      <div style={{ fontWeight: 'bold', fontSize: '1.2rem', textAlign: 'left' }}>{post.title}</div>
                      <div style={{ fontSize: '0.95rem', color: '#555', marginBottom: '0.5rem', textAlign: 'left' }}>By {post.author}</div>
                    </div>
                    <div style={{ display: 'flex', gap: '1rem', alignItems: 'center' }}>
                      <span style={{ color: '#388e3c', fontWeight: 'bold', display: 'flex', alignItems: 'center' }}>
                        <ArrowUpwardIcon fontSize="small" style={{ marginRight: '0.25rem' }} />{post.upvotes}
                      </span>
                      <span style={{ color: '#d32f2f', fontWeight: 'bold', display: 'flex', alignItems: 'center' }}>
                        <ArrowDownwardIcon fontSize="small" style={{ marginRight: '0.25rem' }} />{post.downvotes}
                      </span>
                    </div>
                  </div>
                  <div style={{ marginBottom: '0.5rem', color: '#563861', fontSize: '1rem', textAlign: 'left' }}>{post.body}</div>
                  <div style={{ fontSize: '0.9rem', color: '#555', textAlign: 'right' }}>
                    {post.date_created}
                  </div>
                </a>
              ))}
            </div>
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
            <RecentMatchesList />
            
          </div>
        </div>
      </div>
    </>
  )
}

export default App

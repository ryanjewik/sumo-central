"use client";

import React from "react";

const HighlightedMatchCard: React.FC = () => {
  // These values are hardcoded as in App.tsx, but could be made props later
  const westVotes = 120;
  const eastVotes = 120;
  const totalVotes = westVotes + eastVotes;
  const westPercent = Math.round((westVotes / totalVotes) * 100);
  const eastPercent = 100 - westPercent;
  return (
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
      <h2 style={{ fontSize: '2rem', fontWeight: 'bold', color: '#563861', marginBottom: '1.5rem' }}>
        Highlighted Match
      </h2>

      <div className="highlighted-match-flex-group">
        {/* Left: WEST stats only */}
        <div className="highlighted-match-profile-group">
          <div className="highlighted-match-stats west">
            <div style={{ fontWeight: 'bold', fontSize: '1.2rem', color: '#388eec' }}>Hoshoryu</div>
            <div style={{ fontSize: '1rem', color: '#388eec' }}>Sekiwake</div>
            <div style={{ fontSize: '0.95rem', color: '#388eec' }}>Age: 25</div>
            <div style={{ fontSize: '0.95rem', color: '#388eec' }}>Height: 187cm</div>
            <div style={{ fontSize: '0.95rem', color: '#388eec' }}>Weight: 155kg</div>
            <div style={{ fontSize: '0.95rem', color: '#388eec' }}>Shushin: Mongolia</div>
            <div style={{ fontSize: '0.95rem', color: '#388eec' }}>Heya: Tatsunami</div>
            <div style={{ fontWeight: 'bold', fontSize: '1.1rem', color: '#388eec', marginTop: '0.5rem' }}>120 votes</div>
          </div>
        </div>

        {/* Center: BOTH images + VS (this is what we’ll stack early) */}
        <div className="highlighted-match-vs-group">
          <img src="/sumo_logo.png" alt="West Rikishi" className="rikishi-img west" />
          <div className="vs-text">VS</div>
          <img src="/sumo_logo.png" alt="East Rikishi" className="rikishi-img east" />
        </div>

        {/* Right: EAST stats only */}
        <div className="highlighted-match-profile-group">
          <div className="highlighted-match-stats east">
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
      </div>

      {/* Progress Bar */}
      <div style={{ width: '100%', marginTop: '2rem', display: 'flex', justifyContent: 'center' }}>
        <div style={{ width: '80%', maxWidth: 600 }}>
          <div style={{ display: 'flex', alignItems: 'center', width: '100%' }}>
            <span style={{ fontWeight: 'bold', color: '#388e3c', minWidth: 40, textAlign: 'right', marginRight: 8 }}>{westPercent}%</span>
            <div style={{ flex: 1, height: 18, background: '#F5E6C8', borderRadius: '0.75rem', position: 'relative', display: 'flex', overflow: 'hidden' }}>
              <div style={{ background: '#388eec', width: `${westPercent}%`, height: '100%', borderRadius: '0.75rem 0 0 0.75rem' }}></div>
              <div style={{ background: '#d32f2f', width: `${eastPercent}%`, height: '100%', borderRadius: '0 0.75rem 0.75rem 0' }}></div>
            </div>
            <span style={{ fontWeight: 'bold', color: '#d32f2f', minWidth: 40, textAlign: 'left', marginLeft: 8 }}>{eastPercent}%</span>
          </div>
        </div>
      </div>
    </div>
  );
};

export default HighlightedMatchCard;
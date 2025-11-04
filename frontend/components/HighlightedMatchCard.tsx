"use client";

import React from "react";

interface HighlightedMatchCardProps {
  match?: any;
}

const HighlightedMatchCard: React.FC<HighlightedMatchCardProps> = ({ match }) => {
  // homepage.highlighted_match shape (example from DB):
  // { id, basho_id, east_rikishi_id, west_rikishi_id, east_rank, west_rank, eastshikona, westshikona, winner, kimarite, day, match_number, division }
  const m = match ?? {};
  const eastName = m.east_shikona ?? m.eastshikona ?? m.eastShikona ?? m.east?.shikona ?? 'East';
  const westName = m.west_shikona ?? m.westshikona ?? m.westShikona ?? m.west?.shikona ?? 'West';
  const eastRank = m.east_rank ?? m.eastRank ?? m.east?.rank ?? '';
  const westRank = m.west_rank ?? m.westRank ?? m.west?.rank ?? '';
  const eastVotes = Number(m.east_votes ?? m.eastVotes ?? m.east_votes_count ?? 0) || 0;
  const westVotes = Number(m.west_votes ?? m.westVotes ?? m.west_votes_count ?? 0) || 0;
  const totalVotes = Math.max(1, eastVotes + westVotes);
  const eastPercent = Math.round((eastVotes / totalVotes) * 100);
  const westPercent = 100 - eastPercent;
  const kimarite = m.kimarite ?? m.kimarite_name ?? m.kimarite_display ?? '';
  const winner = m.winner ?? m.result ?? null; // winner might be rikishi id or 'east'/'west'
  const winnerSide = (winner === 'east' || winner === 'west') ? winner : (winner === m.east_rikishi_id || winner === m.east_rikishi_id?.toString() ? 'east' : (winner === m.west_rikishi_id || winner === m.west_rikishi_id?.toString() ? 'west' : null));
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
      <h2 style={{ fontSize: '1.6rem', fontWeight: 800, color: '#563861', marginBottom: '1rem' }}>Highlighted Match</h2>

      <div style={{ display: 'flex', gap: 18, alignItems: 'center', width: '100%', flexWrap: 'wrap', justifyContent: 'center' }}>
        {/* Left: West */}
        <div style={{ minWidth: 180, display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
          <div style={{ position: 'relative', width: 120, height: 160 }} aria-hidden>
            <img
              src={m.west_image ?? m.west_image_url ?? m.west_photo ?? '/sumo_logo.png'}
              alt={`${westName} profile`}
              onError={(e) => { (e.target as HTMLImageElement).src = '/sumo_logo.png'; }}
              style={{
                width: '100%',
                height: '100%',
                objectFit: 'cover',
                borderRadius: 12,
                // make the west outline blue and more opaque even when not the winner
                border: winnerSide === 'west' ? '3px solid #2563eb' : '2px solid rgba(37,99,235,0.9)',
                boxShadow: winnerSide === 'west' ? '0 8px 28px rgba(37,99,235,0.28)' : '0 4px 12px rgba(37,99,235,0.12)'
              }}
            />
            {winnerSide === 'west' && (
              <div style={{ position: 'absolute', top: -8, right: -8, background: '#10b981', color: '#fff', width: 20, height: 20, borderRadius: '50%', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 11, fontWeight: 800 }}>W</div>
            )}
          </div>
          <div style={{ textAlign: 'center', marginTop: 8 }}>
            <div style={{ fontWeight: 800, color: '#1e293b' }}>{westName}</div>
            <div style={{ fontSize: 13, color: '#475569' }}>{westRank}</div>
            <div style={{ marginTop: 6, fontWeight: 700, color: '#1e40af' }}>{westVotes} votes</div>
          </div>
        </div>

        {/* Center: VS + Kimarite */}
        <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 8 }}>
          <div style={{ fontWeight: 900, fontSize: 18, color: '#563861' }}>VS</div>
          {kimarite && (
            <div style={{ background: 'linear-gradient(90deg,#fde68a,#fbcfe8)', padding: '6px 12px', borderRadius: 20, color: '#6b2147', fontWeight: 800, fontSize: 13 }}>{kimarite}</div>
          )}
        </div>

        {/* Right: East */}
        <div style={{ minWidth: 180, display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
          <div style={{ position: 'relative', width: 120, height: 160 }} aria-hidden>
            <img
              src={m.east_image ?? m.east_image_url ?? m.east_photo ?? '/sumo_logo.png'}
              alt={`${eastName} profile`}
              onError={(e) => { (e.target as HTMLImageElement).src = '/sumo_logo.png'; }}
              style={{ width: '100%', height: '100%', objectFit: 'cover', borderRadius: 12, border: winnerSide === 'east' ? '3px solid #ef4444' : '2px solid rgba(0,0,0,0.06)', boxShadow: winnerSide === 'east' ? '0 6px 20px rgba(239,68,68,0.12)' : 'none' }}
            />
            {winnerSide === 'east' && (
              <div style={{ position: 'absolute', top: -8, right: -8, background: '#10b981', color: '#fff', width: 20, height: 20, borderRadius: '50%', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 11, fontWeight: 800 }}>W</div>
            )}
          </div>
          <div style={{ textAlign: 'center', marginTop: 8 }}>
            <div style={{ fontWeight: 800, color: '#1e293b' }}>{eastName}</div>
            <div style={{ fontSize: 13, color: '#475569' }}>{eastRank}</div>
            <div style={{ marginTop: 6, fontWeight: 700, color: '#dc2626' }}>{eastVotes} votes</div>
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

      {kimarite && (
        <div style={{ marginTop: 12, color: '#563861', fontWeight: 600 }}>
          Winning Technique: <span style={{ color: '#388eec' }}>{kimarite}</span>
        </div>
      )}
    </div>
  );
};

export default HighlightedMatchCard;
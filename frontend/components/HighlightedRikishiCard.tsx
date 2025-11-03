"use client";

import React from "react";

const HighlightedRikishiCard: React.FC = () => (
  <div
    style={{
      background: '#F5E6C8',
      border: '4px solid #563861',
      borderRadius: '1rem',
      boxShadow: '0 2px 8px rgba(0,0,0,0.08)',
      marginBottom: '1.2rem',
      padding: '1.2rem 1rem',
      display: 'flex',
      flexDirection: 'column',
      alignItems: 'center',
      gap: '0.8rem',
      width: '100%',
      fontFamily: `'Courier New', Courier, monospace`,
    }}
  >
    <span
      style={{
        display: 'inline-block',
        fontWeight: 'bold',
        fontSize: '1.05rem',
        color: '#fff',
        background: '#563861',
        borderRadius: '0.5rem',
        padding: '0.18rem 0.7rem',
        letterSpacing: '0.05em',
        marginBottom: '0.5rem',
        alignSelf: 'center',
        fontFamily: `'Courier New', Courier, monospace`,
      }}
    >
      Highlighted Rikishi
    </span>
    <div style={{ display: 'flex', flexDirection: 'row', alignItems: 'center', justifyContent: 'center', width: '100%', gap: '1.2rem' }}>
      <img
        src="/sumo_logo.png"
        alt="Rikishi Profile"
        style={{
          width: 170,
          height: 250,
          borderRadius: '3rem',
          border: '3px solid #e0a3c2',
          background: '#fff',
          objectFit: 'cover',
          boxShadow: '0 6px 24px rgba(56,142,236,0.18)',
        }}
      />
      <div style={{ textAlign: 'center', minWidth: 200, fontFamily: `'Courier New', Courier, monospace` }}>
        <div style={{ fontWeight: 'bold', fontSize: '1.35rem', color: '#563861', fontFamily: `'Courier New', Courier, monospace` }}>Kotonowaka</div>
        <div style={{ fontSize: '1.1rem', color: '#388eec', marginBottom: 4, fontFamily: `'Courier New', Courier, monospace` }}>Komusubi</div>
        <div style={{ fontSize: '1.05rem', color: '#563861', fontFamily: `'Courier New', Courier, monospace` }}>Age: 26</div>
        <div style={{ fontSize: '1.05rem', color: '#563861', fontFamily: `'Courier New', Courier, monospace` }}>Height: 188cm</div>
        <div style={{ fontSize: '1.05rem', color: '#563861', fontFamily: `'Courier New', Courier, monospace` }}>Weight: 163kg</div>
        <div style={{ fontSize: '1.05rem', color: '#563861', fontFamily: `'Courier New', Courier, monospace` }}>Heya: Sadogatake</div>
        <div style={{ fontSize: '1.05rem', color: '#563861', fontFamily: `'Courier New', Courier, monospace` }}>Shusshin: Chiba</div>
        <div style={{ fontSize: '1.05rem', color: '#388eec', marginTop: 6, fontFamily: `'Courier New', Courier, monospace` }}><b>Wins:</b> 51</div>
        <div style={{ fontSize: '1.05rem', color: '#d32f2f', fontFamily: `'Courier New', Courier, monospace` }}><b>Losses:</b> 44</div>
      </div>
    </div>
    {/* Yusho and Special Prizes row below stats */}
    <div style={{ width: '100%', marginTop: 10, display: 'flex', flexDirection: 'row', gap: 18, justifyContent: 'flex-start', fontFamily: `'Courier New', Courier, monospace` }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 6, fontWeight: 600, color: '#563861', fontSize: '1rem', fontFamily: `'Courier New', Courier, monospace` }}>
        <span role="img" aria-label="Trophy" style={{ fontSize: 22, color: '#388eec', marginRight: 2, fontFamily: `'Courier New', Courier, monospace` }}>🏆</span>
        <span style={{ color: '#388eec', fontWeight: 700, fontFamily: `'Courier New', Courier, monospace` }}>1</span>
        <span style={{ fontSize: '0.95rem', color: '#563861', marginLeft: 2, fontFamily: `'Courier New', Courier, monospace` }}>(Juryo)</span>
      </div>
      <div style={{ display: 'flex', alignItems: 'center', gap: 6, fontWeight: 600, color: '#563861', fontSize: '1rem', fontFamily: `'Courier New', Courier, monospace` }}>
        <span role="img" aria-label="Star" style={{ fontSize: 20, color: '#e0a3c2', marginRight: 2, fontFamily: `'Courier New', Courier, monospace` }}>⭐</span>
        <span style={{ color: '#e0a3c2', fontWeight: 700, fontFamily: `'Courier New', Courier, monospace` }}>2</span>
        <span style={{ fontSize: '0.95rem', color: '#563861', marginLeft: 2, fontFamily: `'Courier New', Courier, monospace` }}>(Fighting Spirit, Outstanding Performance)</span>
      </div>
    </div>
  </div>
);

export default HighlightedRikishiCard;
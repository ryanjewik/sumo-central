"use client";

import React from "react";
import RikishiWinLossSparkline from '../components/sparkline';

const ClimbingRikishiCard: React.FC = () => (
  <div
    className="climbing-rikishi-card"
    style={{
      background: '#F5E6C8',
      borderRadius: '1rem',
      boxShadow: '0 2px 8px rgba(0,0,0,0.08)',
      border: '2px solid #563861',
      padding: '1.2rem 1rem',
      minWidth: 170,
      maxWidth: 220,
      width: '100%',
      display: 'flex',
      flexDirection: 'column',
      alignItems: 'center',
      gap: '1.5rem',
      height: '100%',
      flex: 1,
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
    <div style={{ width: '100%', marginBottom: '0.5rem' }}>
      <span
        style={{
          display: 'inline-block',
          fontWeight: 'bold',
          fontSize: '1.1rem',
          color: '#fff',
          background: '#563861',
          borderRadius: '0.5rem',
          padding: '0.25rem 1rem',
          letterSpacing: '0.05em',
          margin: '0 auto',
        }}
      >
        Climbing Rikishi
      </span>
    </div>
    <div
      style={{
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        gap: '1.2rem',
        width: '100%',
        justifyContent: 'center',
        flex: 1,
      }}
    >
      <div
        style={{
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          gap: '0.4rem',
          minWidth: 90,
        }}
      >
        <img
          src="/sumo_logo.png"
          alt="Rikishi Profile"
          style={{
            width: 70,
            height: 70,
            borderRadius: '50%',
            border: '3px solid #388eec',
            background: '#fff',
          }}
        />
        <div
          style={{
            fontWeight: 'bold',
            fontSize: '1.15rem',
            color: '#563861',
            textAlign: 'center',
          }}
        >
          Kotonowaka
        </div>
        <div style={{ fontSize: '0.95rem', color: '#388eec', textAlign: 'center' }}>
          Komusubi
        </div>
        {/* Rikishi Stats removed */}
      </div>
      <div
        style={{
          minWidth: 120,
          maxWidth: 180,
          flex: 1,
          display: 'flex',
          justifyContent: 'center',
          alignItems: 'center',
        }}
      >
        <RikishiWinLossSparkline />
      </div>
    </div>
  </div>
);

export default ClimbingRikishiCard;
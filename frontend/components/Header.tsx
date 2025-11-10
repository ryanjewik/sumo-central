"use client";

import React, { useEffect, useState } from 'react';
import Image from 'next/image';
import Link from 'next/link';
import SearchBar from './searchbar';
import NavbarSelection from './horizontal_list';
import LoginDialog from './login_dialog';
import { useAuth } from '@/context/AuthContext';

export default function Header() {
  const [loginOpen, setLoginOpen] = useState(false);
  const { user, logout } = useAuth();

  // simple animation flags (start visible)
  const [navbarVisible] = useState(true);
  const [searchBarVisible] = useState(true);

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

  return (
    <nav
      className="navbar"
      style={{
        transform: navbarVisible ? 'translateY(0)' : 'translateY(-80px)',
        opacity: navbarVisible ? 1 : 0,
        transition: 'transform 0.8s cubic-bezier(0.77,0,0.175,1), opacity 0.8s cubic-bezier(0.77,0,0.175,1)',
        zIndex: 10,
      }}
    >
      <div className="navbar-row navbar-row-top">
        <div className="navbar-left">
          <Link href="/" legacyBehavior>
            <a style={{ display: 'flex', alignItems: 'center', gap: 8, textDecoration: 'none' }}>
              <Image src="/sumo_logo.png" alt="Sumopedia Logo" width={40} height={40} className="navbar-logo" />
              <span className="navbar-title">Sumopedia</span>
            </a>
          </Link>
        </div>

        <div style={{ overflow: 'hidden', width: '35vw', minHeight: 40, display: 'flex', alignItems: 'center' }}>
          <div style={{ width: searchBarVisible ? '100%' : '0%', transition: 'width 0.7s', overflow: 'hidden', display: 'flex', alignItems: 'center' }}>
            <SearchBar />
          </div>
        </div>

        <div className="navbar-right">
          <button className="navbar-btn" style={authPillStyle}>L</button>

          {user ? (
            <div style={{ display: 'flex', gap: '0.6rem', alignItems: 'center' }}>
              <span style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                <span style={authPillStyle}>{user.username}</span>
              </span>

              <button
                className="navbar-btn"
                onClick={async () => { try { await logout(); } catch {} }}
                style={authPillStyle}
              >
                Logout
              </button>
            </div>
          ) : (
            <>
              <button
                className="navbar-btn"
                style={{ minWidth: 100, maxWidth: 120, width: 110, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', display: 'inline-block', justifyContent: 'center', alignItems: 'center', borderRadius: '999px', border: '2px solid #563861', background: '#563861', color: '#fff', fontWeight: 600, fontSize: '1rem', fontFamily: 'inherit', transition: 'background 0.18s, color 0.18s' }}
                onClick={() => setLoginOpen(true)}
              >
                Sign In
              </button>

              <LoginDialog open={loginOpen} onClose={() => setLoginOpen(false)} />
            </>
          )}
        </div>
      </div>
      <div className="navbar-row navbar-row-bottom app-text">
        <NavbarSelection />
      </div>
    </nav>
  );
}

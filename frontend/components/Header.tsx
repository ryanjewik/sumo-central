"use client";

import React, { useEffect, useState, useRef } from 'react';
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

  const [menuOpen, setMenuOpen] = useState(false);
  const navbarRightRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    const onDocClick = (e: MouseEvent) => {
      if (!navbarRightRef.current) return;
      // close only if the click is completely outside the hamburger + menu area
      if (!navbarRightRef.current.contains(e.target as Node)) {
        setMenuOpen(false);
      }
    };
    document.addEventListener('mousedown', onDocClick);
    return () => document.removeEventListener('mousedown', onDocClick);
  }, []);


  return (
    <nav
      className={`navbar ${menuOpen ? 'menu-open' : ''}`}
      style={{
        transform: navbarVisible ? 'translateY(0)' : 'translateY(-80px)',
        opacity: navbarVisible ? 1 : 0,
        transition:
          'transform 0.8s cubic-bezier(0.77,0,0.175,1), opacity 0.8s cubic-bezier(0.77,0,0.175,1)',
        zIndex: 10,
      }}
    >
      <div className="navbar-inner">
        <div className="navbar-row navbar-row-top">
          <div className="navbar-left">
            <Link href="/" style={{ display: 'flex', alignItems: 'center', gap: 8, textDecoration: 'none' }}>
              <Image src="/sumo_logo.png" alt="Sumopedia Logo" width={40} height={40} className="navbar-logo" />
              <span className="navbar-title">Sumopedia</span>
            </Link>
          </div>

          <div
            className="navbar-search-wrapper"
            style={{ minHeight: 40, display: 'flex', alignItems: 'center' }}
          >
            <div
              className="navbar-search-inner"
              style={{ width: '100%', transition: 'width 0.45s', display: 'flex', alignItems: 'center' }}
            >
              <SearchBar />
            </div>
          </div>

          <div className="navbar-right">
            {/* Hamburger for small screens */}
            <button
              aria-label={menuOpen ? "Close menu" : "Open menu"}
              className="hamburger-btn"
              onClick={() => setMenuOpen(v => !v)}
              style={{ background: 'transparent', border: '2px solid transparent', padding: '4px', borderRadius: 6, cursor: 'pointer' }}
            >
              <span className="hamburger-line" />
              <span className="hamburger-line" />
              <span className="hamburger-line" />
            </button>

            <div className="auth-desktop" style={{ display: 'flex', gap: '0.6rem', alignItems: 'center' }}>
              {user ? (
                <>
                  <span className="username-pill" style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                    <Link href={`/users/me`} style={{ textDecoration: 'none', color: 'inherit' }}>
                      <span style={authPillStyle}>{user.username}</span>
                    </Link>
                  </span>
                  <button
                    className="navbar-btn logout-btn"
                    onClick={async () => { try { await logout(); } catch {} }}
                    style={authPillStyle}
                  >
                    Logout
                  </button>
                </>
              ) : (
                <>
                  <button
                    className="signin-btn"
                    style={{ minWidth: 100, maxWidth: 120, width: 110, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', display: 'inline-block', justifyContent: 'center', alignItems: 'center', borderRadius: '999px', border: '2px solid #563861', background: '#563861', color: '#fff', fontWeight: 600, fontSize: '1rem', fontFamily: 'inherit', transition: 'background 0.18s, color 0.18s' }}
                    onClick={() => setLoginOpen(true)}
                  >
                    Sign In
                  </button>

                  <LoginDialog open={loginOpen} onClose={() => setLoginOpen(false)} />
                </>
              )}
            </div>
            {/* Mobile overflow menu */}
            <div className={`mobile-menu${menuOpen ? ' open' : ''}`}>
              <div style={{ display: 'flex', flexDirection: 'column', gap: 8, padding: 12 }}>
                {/* compact navigation for mobile */}
                <Link href="/" style={{ textDecoration: 'none', color: 'inherit' }}>
                  <div style={{ padding: '8px 12px', borderRadius: 6, background: '#F5E6C8' }}>Sumopedia</div>
                </Link>
                <Link href="/discussions" style={{ textDecoration: 'none', color: 'inherit' }}>
                  <div style={{ padding: '8px 12px', borderRadius: 6, background: '#F5E6C8' }}>Discussions</div>
                </Link>
                <Link href="/about" style={{ textDecoration: 'none', color: 'inherit' }}>
                  <div style={{ padding: '8px 12px', borderRadius: 6, background: '#F5E6C8' }}>About</div>
                </Link>
                <Link href="/resources" style={{ textDecoration: 'none', color: 'inherit' }}>
                  <div style={{ padding: '8px 12px', borderRadius: 6, background: '#F5E6C8' }}>Resources</div>
                </Link>

                {/* auth area */}
                {user ? (
                  <>
                    <Link href={`/users/me`} style={{ textDecoration: 'none', color: 'inherit' }}>
                      <div style={{ padding: '8px 12px', borderRadius: 6, background: '#FFF6D8' }}>{user.username}</div>
                    </Link>
                    <button onClick={async () => { try { await logout(); } catch {} setMenuOpen(false); }} style={{ padding: '8px 12px', borderRadius: 6, background: '#563861', color: '#fff', border: 'none' }}>Logout</button>
                  </>
                ) : (
                  <button onClick={() => { setLoginOpen(true); setMenuOpen(false); }} style={{ padding: '8px 12px', borderRadius: 6, background: '#563861', color: '#fff', border: 'none' }}>Sign In</button>
                )}
              </div>
            </div>
          </div>
        </div>
        <div className="navbar-row navbar-row-bottom app-text">
          <NavbarSelection />
        </div>
      </div>
    </nav>
  );
}

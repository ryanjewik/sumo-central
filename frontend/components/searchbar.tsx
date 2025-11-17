
import { styled, alpha } from '@mui/material/styles';
import SearchIcon from '@mui/icons-material/Search';
import InputBase from '@mui/material/InputBase';
import React, { useEffect, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import Link from 'next/link';
import { useRouter } from 'next/navigation';

const Search = styled('div')(({ theme }) => ({
  position: 'relative',
  borderRadius: theme.shape.borderRadius,
  backgroundColor: alpha(theme.palette.common.white, 0.15),
  '&:hover': {
    backgroundColor: alpha(theme.palette.common.white, 0.25),
  },
  marginRight: 0,          // no sideways push
  marginLeft: 0,
  width: '100%',           // obey the wrapper width
  maxWidth: '100%',
}));

const SearchIconWrapper = styled('div')(({ theme }) => ({
  padding: theme.spacing(0, 2),
  height: '100%',
  position: 'absolute',
  pointerEvents: 'none',
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'center',
}));

const StyledInputBase = styled(InputBase)(({ theme }) => ({
  color: 'inherit',
  '& .MuiInputBase-input': {
    padding: theme.spacing(1, 1, 1, 0),
    // vertical padding + font size from searchIcon
    paddingLeft: `calc(1em + ${theme.spacing(4)})`,
    transition: theme.transitions.create('width'),
    width: '100%',
    maxWidth: '100%',      // never grow past the container
  },
}));


type Hit = {
  rikishi?: {
    id?: any;
    shikona?: string;
    current_rank?: string;
  };
  score?: number;
};

export default function SearchBar() {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState<any[]>([]);
  const [open, setOpen] = useState(false);
  const [selectedIndex, setSelectedIndex] = useState(-1);
  const timer = useRef<number | null>(null);
  const inputRef = useRef<HTMLInputElement | null>(null);
  const resultsRef = useRef<HTMLUListElement | null>(null);
  const wrapperRef = useRef<HTMLDivElement | null>(null);
  const [portalStyles, setPortalStyles] = useState<{ left: number; top: number; width: number } | null>(null);
  const [mounted, setMounted] = useState(false);
  const router = useRouter();

  useEffect(() => {
    // simple debounce (short so search happens as user types)
    if (timer.current) {
      window.clearTimeout(timer.current);
    }
    // open search as soon as something is typed (min 1 char)
    if (!query || query.trim().length < 1) {
      setResults([]);
      setOpen(false);
      setSelectedIndex(-1);
      return;
    }
    timer.current = window.setTimeout(async () => {
      try {
        const resp = await fetch(`/api/search/rikishi?q=${encodeURIComponent(query)}&limit=10`);
        if (!resp.ok) {
          setResults([]);
          setOpen(false);
          setSelectedIndex(-1);
          return;
        }
        const data = await resp.json();
        const items = (data.items || []).map((it: any) => it.rikishi ? it.rikishi : { id: it.rikishi?.id, shikona: it.rikishi?.shikona });
        setResults(items);
        setSelectedIndex(items.length > 0 ? 0 : -1);
        setOpen(items.length > 0);
      } catch (err) {
        setResults([]);
        setOpen(false);
        setSelectedIndex(-1);
      }
    }, 120);
    return () => {
      if (timer.current) window.clearTimeout(timer.current);
    };
  }, [query]);

  // ensure the selected item is visible in the scrollable list
  useEffect(() => {
    if (!resultsRef.current) return;
    const idx = selectedIndex;
    if (idx < 0) return;
    const child = resultsRef.current.children[idx] as HTMLElement | undefined;
    if (child && typeof child.scrollIntoView === 'function') {
      child.scrollIntoView({ block: 'nearest' });
    }
  }, [selectedIndex]);

  // keep portal mounted flag
  useEffect(() => {
    setMounted(true);
    return () => setMounted(false);
  }, []);

  // compute position for portal dropdown when open or on resize/scroll
  useEffect(() => {
    function compute() {
      if (!wrapperRef.current) return setPortalStyles(null);
      const rect = wrapperRef.current.getBoundingClientRect();
      setPortalStyles({ left: rect.left + window.scrollX, top: rect.bottom + window.scrollY + 8, width: rect.width });
    }
    if (open) compute();
    window.addEventListener('resize', compute);
    window.addEventListener('scroll', compute, true);
    return () => {
      window.removeEventListener('resize', compute);
      window.removeEventListener('scroll', compute, true);
    };
  }, [open]);

  return (
  <div
    ref={wrapperRef}
    style={{ position: 'relative', width: '100%', maxWidth: 'clamp(24rem, 70vw, 120rem)' }}
  >
    <Search
      className="h-14 rounded-2xl shadow-md flex items-center"
      style={{
        backgroundColor: 'rgba(255,255,255,0.7)',
        width: '100%',
        maxWidth: '100%',
        cursor: 'text',                 // <- NEW: show text cursor
      }}
      onClick={() => {
        if (inputRef.current) {
          inputRef.current.focus();     // <- NEW: focus input from anywhere on bar
        }
      }}
    >
        <SearchIconWrapper>
          <SearchIcon />
        </SearchIconWrapper>
        <StyledInputBase
          placeholder="Search rikishi by shikona…"
          inputProps={{ 'aria-label': 'search' }}
          inputRef={(el: any) => (inputRef.current = el)}
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onFocus={() => { if (results.length) setOpen(true); }}
            onBlur={() => setTimeout(() => setOpen(false), 200)}
          onKeyDown={(e: any) => {
            if (e.key === 'ArrowDown') {
              e.preventDefault();
                setSelectedIndex((idx) => {
                  if (results.length === 0) return -1;
                  if (idx < 0) return 0;
                  return Math.min(idx + 1, results.length - 1);
                });
              setOpen(true);
            } else if (e.key === 'ArrowUp') {
              e.preventDefault();
                setSelectedIndex((idx) => {
                  if (results.length === 0) return -1;
                  if (idx <= 0) return results.length - 1;
                  return Math.max(idx - 1, 0);
                });
              setOpen(true);
            } else if (e.key === 'Enter') {
              e.preventDefault();
              if (results.length === 0) return;
              const idx = selectedIndex >= 0 ? selectedIndex : 0;
              const target = results[idx];
              if (target && target.id) {
                setOpen(false);
                router.push(`/rikishi/${target.id}`);
              }
            }
          }}
        />
      </Search>

      {mounted && open && results.length > 0 && portalStyles && createPortal(
        <div style={{ position: 'absolute', left: portalStyles.left, top: portalStyles.top, width: portalStyles.width, background: '#fff', borderRadius: 8, boxShadow: '0 8px 36px rgba(0,0,0,0.12)', zIndex: 99999 }}>
          <ul ref={resultsRef} style={{ listStyle: 'none', margin: 0, padding: 8, maxHeight: 360, overflowY: 'auto' }}>
            {results.map((r: any, i: number) => (
              <li
                key={i}
                onMouseEnter={() => setSelectedIndex(i)}
                style={{
                  padding: '8px 12px',
                  borderRadius: 6,
                  background: i === selectedIndex ? 'rgba(86,56,97,0.06)' : 'transparent',
                  cursor: 'pointer',
                }}
              >
                {r && r.id ? (
                  <a
                    onClick={(ev) => {
                      ev.preventDefault();
                      setOpen(false);
                      router.push(`/rikishi/${r.id}`);
                    }}
                    href={`/rikishi/${r.id}`}
                    style={{ textDecoration: 'none', color: '#222', display: 'block' }}
                  >
                    <div style={{ fontWeight: 700, color: '#563861' }}>{r.shikona ?? `#${r.id}`}</div>
                    {r.current_rank ? <div style={{ color: '#666', fontSize: 13 }}>{r.current_rank}</div> : null}
                  </a>
                ) : (
                  <div style={{ color: '#222' }}>{r.shikona ?? 'Unknown'}</div>
                )}
              </li>
            ))}
          </ul>
        </div>,
        // attach to body so dropdown can overlap other elements
        document.body,
      )}
    </div>
  );
}
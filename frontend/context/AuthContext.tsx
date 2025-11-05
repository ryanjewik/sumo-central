"use client";

import React, { createContext, useContext, useEffect, useState } from 'react';
import * as authLib from '../lib/auth';

type User = { id: string; username: string } | null;

interface AuthContextValue {
  user: User;
  setUser: (u: User) => void;
  logout: () => Promise<void>;
  loading: boolean;
  login: (username: string, password: string) => Promise<{ ok: boolean; error?: string }>;
  register: (username: string, email: string, password: string) => Promise<{ ok: boolean; error?: string }>;
}

const AuthContext = createContext<AuthContextValue | undefined>(undefined);

export const AuthProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [user, setUser] = useState<User>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let mounted = true;
    (async () => {
      try {
        // Try a few times with small backoff. Some browsers behave differently
        // on a plain reload vs reload-with-devtools-open (timing, caching,
        // or request ordering). Retrying briefly often reproduces the
        // "works with DevTools" case and makes auth more robust.
        const delays = [0, 200, 800];
        let info: { id?: string; username?: string } | null = null;
          for (let i = 0; i < delays.length; i++) {
          if (delays[i] > 0) await new Promise((res) => setTimeout(res, delays[i]));
          try {
            // Debug: log attempts so we can correlate with backend logs
            console.debug(`[auth] tryRefresh attempt ${i + 1}`);
            // call the library function
            info = await authLib.tryRefresh();
            if (info && info.id) break;
          } catch {
            // swallow and continue to retry
          }
        }

        if (mounted && info && info.id) {
          setUser({ id: info.id, username: info.username || '' });
        }
      } catch {
        // ignore
      }
      if (mounted) setLoading(false);
    })();
    return () => {
      mounted = false;
    };
  }, []);

  const logout = async () => {
    try {
      await authLib.logout();
    } catch {
      // ignore
    }
    setUser(null);
  };

  const login = async (username: string, password: string): Promise<{ ok: boolean; error?: string }> => {
    try {
      const res = await fetch('/api/auth/login', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({ username, password }),
      });
      const data = await res.json().catch(() => null);
      if (res.ok && data && data.token && data.id && data.username) {
        // set access token and user
        authLib.setAccessToken(data.token);
        setUser({ id: data.id, username: data.username });
        return { ok: true };
      }
      return { ok: false, error: (data && data.error) || 'Login failed' };
    } catch {
      return { ok: false, error: 'Login failed' };
    }
  };

  const register = async (username: string, email: string, password: string): Promise<{ ok: boolean; error?: string }> => {
    try {
      const res = await fetch('/api/auth/register', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({ username, email, password }),
      });
      const data = await res.json().catch(() => null);
      if (res.ok && data && data.success) {
        // Auto-login after successful registration
        const loginResult = await login(username, password);
        if (loginResult.ok) return { ok: true };
        return { ok: true };
      }
      return { ok: false, error: (data && data.error) || 'Registration failed' };
    } catch {
      return { ok: false, error: 'Registration failed' };
    }
  };

  return (
    <AuthContext.Provider value={{ user, setUser, logout, loading, login, register }}>
      {children}
    </AuthContext.Provider>
  );
};

export function useAuth() {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error('useAuth must be used within AuthProvider');
  return ctx;
}

export default AuthContext;

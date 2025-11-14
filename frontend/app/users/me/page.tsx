"use client";

import React, { useEffect, useState } from 'react';
import { useAuth } from '@/context/AuthContext';
import { fetchWithAuth } from '@/lib/auth';
import Link from 'next/link';

const COUNTRY_OPTIONS = [
  { code: '', label: 'Select country' },
  { code: 'US', label: 'United States' },
  { code: 'JP', label: 'Japan' },
  { code: 'GB', label: 'United Kingdom' },
  { code: 'AU', label: 'Australia' },
  { code: 'CA', label: 'Canada' },
  { code: 'DE', label: 'Germany' },
];

export default function MePage() {
  const { user, setUser } = useAuth();
  const [loading, setLoading] = useState(true);
  const [profile, setProfile] = useState<any>(null);
  const [username, setUsername] = useState('');
  // username-change form
  const [unameCurrentPassword, setUnameCurrentPassword] = useState('');
  const [newUsername, setNewUsername] = useState('');
  // password-change form
  const [pwdCurrentUsername, setPwdCurrentUsername] = useState('');
  const [pwdCurrentPassword, setPwdCurrentPassword] = useState('');
  const [newPassword, setNewPassword] = useState('');
  const [newPasswordConfirm, setNewPasswordConfirm] = useState('');
  const [country, setCountry] = useState('');
  const [countryStatus, setCountryStatus] = useState<{ type: 'ok' | 'error'; message: string } | null>(null);
  const [savingCountry, setSavingCountry] = useState(false);
  const [unameStatus, setUnameStatus] = useState<{ type: 'ok' | 'error'; message: string } | null>(null);
  const [pwdStatus, setPwdStatus] = useState<{ type: 'ok' | 'error'; message: string } | null>(null);
  const [savingUname, setSavingUname] = useState(false);
  const [savingPwd, setSavingPwd] = useState(false);

  const [loadStatus, setLoadStatus] = useState<{ type: 'ok' | 'error'; message: string } | null>(null);

  useEffect(() => {
    let mounted = true;
    (async () => {
      setLoading(true);
      try {
        const res = await fetchWithAuth('/api/users/me');
        if (res.ok) {
          const data = await res.json();
          if (!mounted) return;
          setProfile(data);
          setUsername(data.username ?? '');
          setNewUsername(data.username ?? '');
          setPwdCurrentUsername(data.username ?? '');
          setCountry(data.country ?? '');
        } else if (res.status === 401) {
          // Not authenticated - redirect to login or show message
          setLoadStatus({ type: 'error', message: 'Not authenticated. Please sign in.' });
        } else {
          setLoadStatus({ type: 'error', message: `Failed to load profile (${res.status})` });
        }
      } catch (err) {
        setLoadStatus({ type: 'error', message: 'Network error while loading profile.' });
      } finally {
        if (mounted) setLoading(false);
      }
    })();
    return () => { mounted = false; };
  }, []);

  const validatePassword = (p: string) => {
    if (p === '') return { ok: true };
    if (p.length < 10) return { ok: false, message: 'Password must be at least 10 characters.' };
    if (!/[A-Z]/.test(p)) return { ok: false, message: 'Password must contain at least one uppercase letter.' };
    if (!/\d/.test(p)) return { ok: false, message: 'Password must contain at least one number.' };
    return { ok: true };
  };

  const submitUsernameChange = async (e: React.FormEvent) => {
    e.preventDefault();
    setUnameStatus(null);
    if (!newUsername || newUsername.trim() === (profile?.username ?? '')) {
      setUnameStatus({ type: 'error', message: 'Enter a different username to change.' });
      return;
    }
    if (!unameCurrentPassword) {
      setUnameStatus({ type: 'error', message: 'Current password is required to change username.' });
      return;
    }
    setSavingUname(true);
    try {
      const body: any = { username: newUsername.trim(), current_password: unameCurrentPassword, current_username: username, country };
      const res = await fetchWithAuth('/api/users/me', {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      if (res.ok) {
        const data = await res.json();
        setProfile(data);
        setUser && setUser({ id: data.id, username: data.username });
        setUsername(data.username ?? '');
        setNewUsername(data.username ?? '');
        setUnameCurrentPassword('');
        setUnameStatus({ type: 'ok', message: 'Username updated.' });
      } else if (res.status === 400) {
        const d = await res.json().catch(() => null);
        setUnameStatus({ type: 'error', message: (d && d.error) || 'Validation failed' });
      } else if (res.status === 401) {
        setUnameStatus({ type: 'error', message: 'Not authenticated or current password incorrect.' });
      } else {
        setUnameStatus({ type: 'error', message: `Failed to save (${res.status})` });
      }
    } catch (err) {
      setUnameStatus({ type: 'error', message: 'Network error while saving.' });
    } finally {
      setSavingUname(false);
    }
  };

  const submitPasswordChange = async (e: React.FormEvent) => {
    e.preventDefault();
    setPwdStatus(null);
    // validate new password
    const passCheck = validatePassword(newPassword);
    if (!passCheck.ok) {
      setPwdStatus({ type: 'error', message: passCheck.message || 'Invalid password' });
      return;
    }
    if (!newPassword || newPassword !== newPasswordConfirm) {
      setPwdStatus({ type: 'error', message: 'Password and confirmation do not match' });
      return;
    }
    if (!pwdCurrentUsername || !pwdCurrentPassword) {
      setPwdStatus({ type: 'error', message: 'Current username and password are required to change password.' });
      return;
    }
    setSavingPwd(true);
    try {
      const body: any = { current_username: pwdCurrentUsername, current_password: pwdCurrentPassword, password: newPassword, password_confirm: newPasswordConfirm };
      const res = await fetchWithAuth('/api/users/me', {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      if (res.ok) {
        const data = await res.json();
        setProfile(data);
        setNewPassword('');
        setNewPasswordConfirm('');
        setPwdCurrentPassword('');
        setPwdStatus({ type: 'ok', message: 'Password updated.' });
      } else if (res.status === 400) {
        const d = await res.json().catch(() => null);
        setPwdStatus({ type: 'error', message: (d && d.error) || 'Validation failed' });
      } else if (res.status === 401) {
        setPwdStatus({ type: 'error', message: 'Not authenticated or current credentials incorrect.' });
      } else {
        setPwdStatus({ type: 'error', message: `Failed to save (${res.status})` });
      }
    } catch (err) {
      setPwdStatus({ type: 'error', message: 'Network error while saving.' });
    } finally {
      setSavingPwd(false);
    }
  };

  return (
    <main className="page-offset" style={{ maxWidth: '100%' }}>
    <div style={{ maxWidth: 960, margin: '0 auto', padding: '4rem 1rem 4rem', position: 'relative' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 18 }}>
            <h1 style={{ margin: 0 }}>Your profile</h1>
            <div>
            <Link href="/">Back home</Link>
            </div>
        </div>

        {loading ? (
            <div>Loading...</div>
        ) : (
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 340px', gap: 24 }}>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 18 }}>
                {/* Username change card */}
                <form onSubmit={submitUsernameChange} style={{ background: 'rgba(255,255,255,0.02)', padding: 18, borderRadius: 8 }}>
                <h3 style={{ marginTop: 0 }}>Change username</h3>
                <label style={{ display: 'block', marginBottom: 8 }}>Current username</label>
                <input value={username} readOnly style={{ width: '100%', padding: '8px 10px', borderRadius: 6, border: '1px solid #ccc', background: '#f7f7f7' }} />
                <div style={{ height: 8 }} />
                <label style={{ display: 'block', marginBottom: 8 }}>Current password</label>
                <input type="password" value={unameCurrentPassword} onChange={(e) => setUnameCurrentPassword(e.target.value)} style={{ width: '100%', padding: '8px 10px', borderRadius: 6, border: '1px solid #ccc' }} />
                <div style={{ height: 8 }} />
                <label style={{ display: 'block', marginBottom: 8 }}>New username</label>
                <input value={newUsername} onChange={(e) => setNewUsername(e.target.value)} style={{ width: '100%', padding: '8px 10px', borderRadius: 6, border: '1px solid #ccc' }} />
                <div style={{ height: 12 }} />
                <button disabled={savingUname} type="submit" style={{ padding: '10px 14px', background: '#563861', color: '#fff', border: 'none', borderRadius: 8, cursor: 'pointer' }}>
                    {savingUname ? 'Saving…' : 'Save username'}
                </button>
                {unameStatus ? <div style={{ marginTop: 12, color: unameStatus.type === 'ok' ? 'green' : 'crimson' }}>{unameStatus.message}</div> : null}
                </form>

                {/* Password change card */}
                <form onSubmit={submitPasswordChange} style={{ background: 'rgba(255,255,255,0.02)', padding: 18, borderRadius: 8 }}>
                <h3 style={{ marginTop: 0 }}>Change password</h3>
                <label style={{ display: 'block', marginBottom: 8 }}>Current username</label>
                <input value={pwdCurrentUsername} onChange={(e) => setPwdCurrentUsername(e.target.value)} style={{ width: '100%', padding: '8px 10px', borderRadius: 6, border: '1px solid #ccc' }} />
                <div style={{ height: 8 }} />
                <label style={{ display: 'block', marginBottom: 8 }}>Current password</label>
                <input type="password" value={pwdCurrentPassword} onChange={(e) => setPwdCurrentPassword(e.target.value)} style={{ width: '100%', padding: '8px 10px', borderRadius: 6, border: '1px solid #ccc' }} />
                <div style={{ height: 8 }} />
                <label style={{ display: 'block', marginBottom: 8 }}>New password</label>
                <input type="password" value={newPassword} onChange={(e) => setNewPassword(e.target.value)} style={{ width: '100%', padding: '8px 10px', borderRadius: 6, border: '1px solid #ccc' }} />
                <div style={{ height: 8 }} />
                <label style={{ display: 'block', marginBottom: 8 }}>Confirm new password</label>
                <input type="password" value={newPasswordConfirm} onChange={(e) => setNewPasswordConfirm(e.target.value)} style={{ width: '100%', padding: '8px 10px', borderRadius: 6, border: '1px solid #ccc' }} />
                <div style={{ color: '#666', fontSize: 13, marginTop: 6 }}>
                    New password must be at least 10 chars, include an uppercase letter and a number.
                </div>
                <div style={{ height: 12 }} />
                <button disabled={savingPwd} type="submit" style={{ padding: '10px 14px', background: '#563861', color: '#fff', border: 'none', borderRadius: 8, cursor: 'pointer' }}>
                    {savingPwd ? 'Saving…' : 'Save password'}
                </button>
                {pwdStatus ? <div style={{ marginTop: 12, color: pwdStatus.type === 'ok' ? 'green' : 'crimson' }}>{pwdStatus.message}</div> : null}
                </form>
            </div>

            <aside style={{ background: 'rgba(255,255,255,0.02)', padding: 18, borderRadius: 8 }}>
                <div style={{ marginBottom: 12 }}>
                <strong>Email</strong>
                <div style={{ color: '#666' }}>{profile?.email ?? '—'}</div>
                </div>
              <div style={{ marginBottom: 12 }}>
                <label style={{ display: 'block', marginBottom: 6 }}>Country</label>
                <div style={{ display: 'flex', gap: 8 }}>
                  <select value={country} onChange={(e) => setCountry(e.target.value)} style={{ flex: 1, padding: '8px 10px', borderRadius: 6 }}>
                    {COUNTRY_OPTIONS.map((opt) => (
                      <option key={opt.code} value={opt.code}>{opt.label}</option>
                    ))}
                  </select>
                  <button
                    type="button"
                    disabled={savingCountry}
                    onClick={async () => {
                      setCountryStatus(null);
                      // if no change, noop
                      if ((profile?.country ?? '') === country) {
                        setCountryStatus({ type: 'ok', message: 'No change.' });
                        return;
                      }
                      setSavingCountry(true);
                      try {
                        const res = await fetchWithAuth('/api/users/me', {
                          method: 'PATCH',
                          headers: { 'Content-Type': 'application/json' },
                          body: JSON.stringify({ country }),
                        });
                        if (res.ok) {
                          const data = await res.json();
                          setProfile(data);
                          setCountry(data.country ?? '');
                          setCountryStatus({ type: 'ok', message: 'Country saved.' });
                        } else if (res.status === 400) {
                          const d = await res.json().catch(() => null);
                          setCountryStatus({ type: 'error', message: (d && d.error) || 'Validation failed' });
                        } else if (res.status === 401) {
                          setCountryStatus({ type: 'error', message: 'Not authenticated.' });
                        } else {
                          setCountryStatus({ type: 'error', message: `Failed to save (${res.status})` });
                        }
                      } catch (err) {
                        setCountryStatus({ type: 'error', message: 'Network error while saving country.' });
                      } finally {
                        setSavingCountry(false);
                      }
                    }}
                    style={{ padding: '8px 10px', borderRadius: 6, background: '#563861', color: '#fff', border: 'none', cursor: 'pointer' }}
                  >
                    {savingCountry ? 'Saving…' : 'Save'}
                  </button>
                </div>
                {countryStatus ? <div style={{ marginTop: 8, color: countryStatus.type === 'ok' ? 'green' : 'crimson' }}>{countryStatus.message}</div> : null}
              </div>

                <div style={{ marginTop: 8, color: '#999', fontSize: 13 }}>
                Your public profile will show the username and country. Email is only visible to you.
                </div>

                <div style={{ marginTop: 16 }}>
                  <div><strong>Stats</strong></div>
                  <div style={{ color: '#666', marginTop: 8 }}>
                    Posts: {profile?.num_posts ?? 0}<br />
                    Predictions: {profile?.num_predictions ?? 0}<br />
                    Correct predictions: {profile?.correct_predictions ?? 0}
                  </div>
                </div>

                {profile?.favorite_rikishi ? (
                  <div style={{ marginTop: 12 }}>
                    <div><strong>Favorite rikishi</strong></div>
                    <div style={{ color: '#666', marginTop: 6 }}>
                      {typeof profile.favorite_rikishi === 'object' ? (
                        <div>
                          <div><strong>{profile.favorite_rikishi.shikona ?? profile.favorite_rikishi.name ?? `#${profile.favorite_rikishi.id}`}</strong></div>
                          <div>Rank: {profile.favorite_rikishi.current_rank ?? '—'}</div>
                          <div>Heya: {profile.favorite_rikishi.heya ?? '—'}</div>
                          <div>Wins: {profile.favorite_rikishi.wins ?? '—'} / Losses: {profile.favorite_rikishi.losses ?? '—'}</div>
                          <div>Matches: {profile.favorite_rikishi.matches ?? '—'}</div>
                          <div>Yusho: {profile.favorite_rikishi.yusho_count ?? '—'} • Sansho: {profile.favorite_rikishi.sansho_count ?? '—'}</div>
                        </div>
                      ) : (
                        <div>Rikishi id: {String(profile.favorite_rikishi)}</div>
                      )}
                    </div>
                  </div>
                ) : null}

                <div style={{ marginTop: 12 }}>
                  <div><strong>Joined</strong></div>
                  <div style={{ color: '#666', marginTop: 6 }}>{profile?.createdAt ? new Date(profile.createdAt).toLocaleDateString() : '—'}</div>
                </div>
            </aside>
            </div>
        )}
        </div>
    </main>
  );
}

"use client";

import * as React from 'react';
import Box from '@mui/joy/Box';
import List from '@mui/joy/List';
import ListItem from '@mui/joy/ListItem';
import ListItemButton from '@mui/joy/ListItemButton';
import Typography from '@mui/joy/Typography';
import Avatar from '@mui/joy/Avatar';
import { ProgressBar } from "./base/progress-indicators/progress-indicators";


interface Match {
  id: number;
  rikishi1: string;
  rikishi2: string;
  rikishi1Rank?: string;
  rikishi2Rank?: string;
  date: string;
  venue?: string;
}


interface UpcomingMatchesListProps {
  matches: Match[];
  date?: string;
}



import { useState } from 'react';

const UpcomingMatchesList: React.FC<UpcomingMatchesListProps> = ({ matches, date }) => {
  // Track votes for each match: { [matchId]: { west: number; east: number } }
  const [votes, setVotes] = useState(() => {
    // Initialize with random votes for demo
    const initial: Record<number, { west: number; east: number }> = {};
    matches.forEach(m => {
      const west = Math.floor(Math.random() * 20) + 1;
      const east = Math.floor(Math.random() * 20) + 1;
      initial[m.id] = { west, east };
    });
    return initial;
  });
  // Track user's vote for each match: { [matchId]: 'west' | 'east' | undefined }
  const [userVotes, setUserVotes] = useState<Record<number, 'west' | 'east' | undefined>>({});

  const handleVote = (matchId: number, side: 'west' | 'east') => {
    setVotes(prev => {
      const matchVotes = prev[matchId] || { west: 0, east: 0 };
      const prevUserVote = userVotes[matchId];
      // If user already voted for this side, do nothing
      if (prevUserVote === side) return prev;
      // Remove previous vote if exists, add new vote
      let newVotes = { ...matchVotes };
      if (prevUserVote) {
        newVotes[prevUserVote] = Math.max(0, newVotes[prevUserVote] - 1);
      }
      newVotes[side] = newVotes[side] + 1;
      return { ...prev, [matchId]: newVotes };
    });
    setUserVotes(prev => ({ ...prev, [matchId]: side }));
  };

  return (
    <Box sx={{ display: 'flex', flexWrap: 'wrap', justifyContent: 'center', gap: 4, mb: 0 }}>
      <Box
        className="app-text"
        sx={{
          width: '100%',
          background: '#A3E0B8',
          borderRadius: '1rem',
          boxShadow: '0 2px 8px rgba(0,0,0,0.08)',
          p: '1rem',
          minWidth: 260,
          border: '4px solid #563861',
          position: 'relative',
          zIndex: 0,
        }}
      >
        <Typography className="app-text" level="title-lg" sx={{ fontWeight: 1000, fontSize: '1.5rem' }}>
          Upcoming Matches
        </Typography>
        {date && (
          <Typography sx={{ color: '#563861', fontWeight: 500, fontSize: '1.08rem', mb: 2 }}>
            {date}
          </Typography>
        )}

        <List variant="outlined" sx={{ minWidth: 240, borderRadius: 'sm', p: 0, m: 0 }}>
          {matches.length === 0 ? (
            <ListItem>
              <Typography sx={{ color: '#888', fontSize: '0.95rem' }}>No upcoming matches scheduled.</Typography>
            </ListItem>
          ) : (
            matches.map((match, idx) => {
              const matchVotes = votes[match.id] || { west: 0, east: 0 };
              const totalVotes = matchVotes.west + matchVotes.east;
              const percent = totalVotes === 0 ? 50 : Math.round((matchVotes.west / totalVotes) * 100);
              return (
                <ListItem key={match.id} sx={{ p: 0, m: 0, listStyle: 'none', position: 'relative' }}>
                  <ListItemButton
                    className='app-text'
                    sx={{
                      width: '100%',
                      display: 'flex',
                      alignItems: 'center',
                      justifyContent: 'center',
                      gap: 2,
                      background: '#F5E6C8',
                      borderRadius: '0.75rem',
                      minHeight: 60,
                      cursor: 'pointer',
                      transition: 'transform 0.15s, box-shadow 0.15s',
                      position: 'relative',
                      isolation: 'isolate',
                      mb: 1.5,
                      pb: idx < matches.length - 1 ? 'calc(0.5rem + 1px)' : 0,
                      '&:hover': {
                        transform: 'scale(1.03)',
                        boxShadow: '0 4px 16px rgba(86,56,97,0.15)',
                      },
                    }}
                  >
                    {/* West avatar */}
                    <Box sx={{ position: 'relative' }}>
                      <Avatar size="sm" src="/sumo_logo.png" />
                    </Box>
                    {/* Middle content */}
                    <Box sx={{ textAlign: 'center', flex: 1, display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
                      <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 4, width: '100%' }}>
                        <Box sx={{ minWidth: 0, display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center' }}>
                          <Typography className="app-text" level="body-md" sx={{ fontWeight: 600, fontSize: '1.13rem', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis', maxWidth: '100%', textAlign: 'center' }}>{match.rikishi1}</Typography>
                          <Typography className="app-text" level="body-xs" sx={{ color: '#a06b9a', fontWeight: 500, fontSize: '0.98rem', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis', maxWidth: '100%', mt: 0.2, textAlign: 'center' }}>{match.rikishi1Rank || 'Rank TBD'}</Typography>
                        </Box>
                        <Typography className="app-text" level="body-md" sx={{ fontWeight: 700, fontSize: '1.13rem', color: '#563861', mx: 1 }}>VS</Typography>
                        <Box sx={{ minWidth: 0, display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center' }}>
                          <Typography className="app-text" level="body-md" sx={{ fontWeight: 600, fontSize: '1.13rem', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis', maxWidth: '100%', textAlign: 'center' }}>{match.rikishi2}</Typography>
                          <Typography className="app-text" level="body-xs" sx={{ color: '#a06b9a', fontWeight: 500, fontSize: '0.98rem', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis', maxWidth: '100%', mt: 0.2, textAlign: 'center' }}>{match.rikishi2Rank || 'Rank TBD'}</Typography>
                        </Box>
                      </Box>
                      <Box sx={{ display: 'flex', justifyContent: 'center', mt: 1, width: '100%' }}>
                        <Box sx={{ width: 240, maxWidth: '100%' }}>
                          <ProgressBar value={percent} className="bg-red-500" progressClassName="bg-blue-500" />
                        </Box>
                      </Box>
                      <Box sx={{ display: 'flex', justifyContent: 'center', gap: 1, mt: 1, width: '100%' }}>
                        <button
                          style={{
                            background: userVotes[match.id] === 'west' ? '#e0a3c2' : '#f5e6c8',
                            color: '#563861',
                            border: userVotes[match.id] === 'west'
                              ? '2.5px solid #563861'
                              : '2.5px solid #e0a3c2',
                            borderRadius: '0.5rem',
                            width: 110,
                            height: 36,
                            fontWeight: 600,
                            cursor: 'pointer',
                            fontSize: '1rem',
                            transition: 'background 0.18s, border 0.18s',
                            display: 'inline-block',
                            position: 'relative',
                            boxShadow: userVotes[match.id] === 'west' ? '0 2px 8px #e0a3c2aa' : '0 1px 2px #e0a3c255',
                          }}
                          onClick={e => {
                            e.stopPropagation();
                            handleVote(match.id, 'west');
                          }}
                        >
                          {userVotes[match.id] === 'west' ? '✔' : 'Vote'}
                        </button>
                        <button
                          style={{
                            background: userVotes[match.id] === 'east' ? '#a3e0b8' : '#f5e6c8',
                            color: '#563861',
                            border: userVotes[match.id] === 'east'
                              ? '2.5px solid #563861'
                              : '2.5px solid #a3e0b8',
                            borderRadius: '0.5rem',
                            width: 110,
                            height: 36,
                            fontWeight: 600,
                            cursor: 'pointer',
                            fontSize: '1rem',
                            transition: 'background 0.18s, border 0.18s',
                            display: 'inline-block',
                            position: 'relative',
                            boxShadow: userVotes[match.id] === 'east' ? '0 2px 8px #a3e0b8aa' : '0 1px 2px #a3e0b855',
                          }}
                          onClick={e => {
                            e.stopPropagation();
                            handleVote(match.id, 'east');
                          }}
                        >
                          {userVotes[match.id] === 'east' ? '✔' : 'Vote'}
                        </button>
                      </Box>
                      <Typography sx={{ fontSize: '0.85rem', color: '#888', mt: 0.5 }}>
                        {matchVotes.west + matchVotes.east} vote{(matchVotes.west + matchVotes.east) === 1 ? '' : 's'}
                      </Typography>
                    </Box>
                    {/* East avatar */}
                    <Box sx={{ position: 'relative' }}>
                      <Avatar size="sm" src="/sumo_logo.png" />
                    </Box>
                    {/* Divider drawn INSIDE this item (never overlaps the next row) */}
                    {idx < matches.length - 1 && (
                      <Box
                        sx={{
                          pointerEvents: 'none',
                          position: 'absolute',
                          left: 16,
                          right: 16,
                          bottom: 0,
                          height: 1,
                          backgroundColor: 'var(--joy-palette-divider)',
                        }}
                      />
                    )}
                  </ListItemButton>
                </ListItem>
              );
            })
          )}
        </List>
      </Box>
    </Box>
  );
};

export default UpcomingMatchesList;

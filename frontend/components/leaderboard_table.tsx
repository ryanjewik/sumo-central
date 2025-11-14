"use client";

import React from 'react';
import Link from 'next/link';
import { Box, Table, TableBody, TableCell, TableHead, TableRow, Paper } from '@mui/material';

interface LeaderboardEntry {
  id: string;
  username: string;
  correctPredictions: number;
}

interface LeaderboardTableProps {
  leaderboard: LeaderboardEntry[];
}

const LeaderboardTable: React.FC<LeaderboardTableProps> = ({ leaderboard }) => {
  return (
    <Box sx={{ width: '100%', mt: 2, fontFamily: `'Courier New', Courier, monospace` }}>
      <Paper
        elevation={4}
        sx={{
          borderRadius: '1.2rem',
          p: 0,
          background: 'linear-gradient(135deg, #f5e6c8 0%, #e0a3c2 100%)',
          border: '3px solid #563861',
          boxShadow: '0 2px 12px 0 rgba(86,56,97,0.10)',
          overflow: 'hidden',
          fontFamily: `'Courier New', Courier, monospace`,
        }}
      >
        <Box
          sx={{
            background: 'linear-gradient(90deg, #563861 0%, #e0a3c2 100%)',
            color: '#fff',
            py: 1.2,
            px: 2,
            textAlign: 'center',
            fontWeight: 'bold',
            fontSize: '1.18rem',
            letterSpacing: '0.04em',
            fontFamily: `'Courier New', Courier, monospace`,
            borderBottom: '2px solid #e0a3c2',
            boxShadow: '0 2px 8px 0 rgba(86,56,97,0.10)',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            gap: 1,
          }}
        >
          <span style={{ fontSize: '1.3em', marginRight: 8 }}>🏆</span> Leaderboard
        </Box>
        <Box sx={{ p: 2 }}>
          <Table size="small" sx={{ fontFamily: `'Courier New', Courier, monospace` }}>
            <TableHead>
              <TableRow>
                <TableCell sx={{ fontWeight: 'bold', color: '#563861', fontSize: '1.05em', borderBottom: '2px solid #e0a3c2', background: 'rgba(224,163,194,0.13)', fontFamily: `'Courier New', Courier, monospace` }}>#</TableCell>
                <TableCell sx={{ fontWeight: 'bold', color: '#563861', fontSize: '1.05em', borderBottom: '2px solid #e0a3c2', background: 'rgba(224,163,194,0.13)', fontFamily: `'Courier New', Courier, monospace` }}>User</TableCell>
                <TableCell sx={{ fontWeight: 'bold', color: '#563861', fontSize: '1.05em', borderBottom: '2px solid #e0a3c2', background: 'rgba(224,163,194,0.13)', textAlign: 'right', fontFamily: `'Courier New', Courier, monospace` }}>Correct Predictions</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {leaderboard.slice(0, 10).map((entry, idx) => (
                <TableRow
                  key={entry.id}
                  sx={{
                    background: idx % 2 === 0 ? 'rgba(245,230,200,0.85)' : 'rgba(224,163,194,0.10)',
                    transition: 'background 0.2s',
                    '&:hover': {
                      background: 'rgba(224,163,194,0.32)',
                    },
                  }}
                >
                  <TableCell sx={{ color: '#563861', fontWeight: 600, fontFamily: `'Courier New', Courier, monospace` }}>{idx + 1}</TableCell>
                  <TableCell sx={{ color: '#563861', fontWeight: 500, fontFamily: `'Courier New', Courier, monospace` }}>
                    <Link href={`/users/${entry.id}`} style={{ textDecoration: 'none', color: 'inherit' }}>
                      {entry.username}
                    </Link>
                  </TableCell>
                  <TableCell align="right" sx={{ color: '#563861', fontWeight: 600, fontFamily: `'Courier New', Courier, monospace` }}>{entry.correctPredictions}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </Box>
      </Paper>
    </Box>
  );
};

export default LeaderboardTable;

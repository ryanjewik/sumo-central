import * as React from 'react';
import Avatar from '@mui/joy/Avatar';
import Box from '@mui/joy/Box';
import List from '@mui/joy/List';
import ListDivider from '@mui/joy/ListDivider';
import ListItem from '@mui/joy/ListItem';
import ListItemDecorator from '@mui/joy/ListItemDecorator';
import Typography from '@mui/joy/Typography';
import { ProgressBar } from "../components/base/progress-indicators/progress-indicators";

const sampleMatches = [
  { westShikona: 'Hoshoryu', westRank: 'Sekiwake', eastShikona: 'Takakeisho', eastRank: 'Ozeki', winner: 'west' },
  { westShikona: 'Wakatakakage', westRank: 'Komusubi', eastShikona: 'Mitakeumi', eastRank: 'Sekiwake', winner: 'east' },
  { westShikona: 'Shodai', westRank: 'Maegashira 1', eastShikona: 'Meisei', eastRank: 'Maegashira 2', winner: 'west' },
  { westShikona: 'Daieisho', westRank: 'Maegashira 3', eastShikona: 'Kotonowaka', eastRank: 'Maegashira 4', winner: 'east' },
  { westShikona: 'Abi', westRank: 'Maegashira 5', eastShikona: 'Ura', eastRank: 'Maegashira 6', winner: 'west' },
  { westShikona: 'Tamawashi', westRank: 'Maegashira 7', eastShikona: 'Endo', eastRank: 'Maegashira 8', winner: 'east' },
  { westShikona: 'Tobizaru', westRank: 'Maegashira 9', eastShikona: 'Sadanoumi', eastRank: 'Maegashira 10', winner: 'west' },
  { westShikona: 'Kotoshoho', westRank: 'Maegashira 11', eastShikona: 'Chiyoshoma', eastRank: 'Maegashira 12', winner: 'east' },
  { westShikona: 'Takarafuji', westRank: 'Maegashira 13', eastShikona: 'Terutsuyoshi', eastRank: 'Maegashira 14', winner: 'west' },
  { westShikona: 'Ichiyamamoto', westRank: 'Maegashira 15', eastShikona: 'Yutakayama', eastRank: 'Maegashira 16', winner: 'east' },
  { westShikona: 'Kagayaki', westRank: 'Maegashira 17', eastShikona: 'Chiyotairyu', eastRank: 'Maegashira 18', winner: 'west' },
  { westShikona: 'Hokutofuji', westRank: 'Maegashira 19', eastShikona: 'Aoiyama', eastRank: 'Maegashira 20', winner: 'east' },
  { westShikona: 'Ryuden', westRank: 'Maegashira 21', eastShikona: 'Kotoeko', eastRank: 'Maegashira 22', winner: 'west' },
  { westShikona: 'Tochinoshin', westRank: 'Maegashira 23', eastShikona: 'Shohozan', eastRank: 'Maegashira 24', winner: 'east' },
  { westShikona: 'Chiyomaru', westRank: 'Maegashira 25', eastShikona: 'Hidenoumi', eastRank: 'Maegashira 26', winner: 'west' },
  { westShikona: 'Akiseyama', westRank: 'Maegashira 27', eastShikona: 'Tokushoryu', eastRank: 'Maegashira 28', winner: 'east' },
  { westShikona: 'Daiamami', westRank: 'Maegashira 29', eastShikona: 'Kotonowaka', eastRank: 'Maegashira 30', winner: 'west' },
  { westShikona: 'Terunofuji', westRank: 'Yokozuna', eastShikona: 'Asanoyama', eastRank: 'Ozeki', winner: 'west' },
  { westShikona: 'Takanosho', westRank: 'Maegashira 31', eastShikona: 'Wakamotoharu', eastRank: 'Maegashira 32', winner: 'east' },
  { westShikona: 'Kotoshogiku', westRank: 'Maegashira 33', eastShikona: 'Sadanoumi', eastRank: 'Maegashira 34', winner: 'west' },
];

export default function DividedList() {
  return (
    <Box sx={{ display: 'flex', flexWrap: 'wrap', justifyContent: 'center', gap: 4 }}>
      <div className="w-full h-full"
        style={{
          background: '#A3E0B8', // forum post green
          borderRadius: '1rem',
          boxShadow: '0 2px 8px rgba(0,0,0,0.08)',
          padding: '1rem',
          minWidth: 260,
          fontFamily: "'Courier New', Courier, monospace",
        }}
      >
        <Typography level="title-lg" sx={{ mb: 2, fontWeight: 700 }}>
          Recent Matches
        </Typography>
        <List variant="outlined" sx={{ minWidth: 240, borderRadius: 'sm' }}>
          {sampleMatches.map((match, idx) => (
            <React.Fragment key={idx}>
              <ListItem sx={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 2, background: '#F5E6C8', borderRadius: '0.75rem', mb: .5, minHeight: 60 }}>
                {/* West Rikishi Avatar with winner indicator */}
                <Box sx={{ position: 'relative' }}>
                  <Avatar size="sm" src="/static/images/avatar/default.jpg" />
                  {match.winner === 'west' && (
                    <Box sx={{ position: 'absolute', top: -8, right: -8, background: '#22c55e', color: 'white', borderRadius: '50%', width: 17, height: 17, display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 9, fontWeight: 700, boxShadow: '0 1px 4px rgba(0,0,0,0.12)' }}>
                      W
                    </Box>
                  )}
                </Box>
                <Box sx={{ textAlign: 'center', flex: 1 }}>
                  <Typography level="body-md" sx={{ fontWeight: 500 }}>
                    {match.westShikona} vs {match.eastShikona}
                  </Typography>
                  <Typography level="body-xs" sx={{ color: 'text.secondary' }}>
                    {match.westRank} vs {match.eastRank}
                  </Typography>
                    {/* Centered Progress Indicator */}
                    <Box sx={{ display: 'flex', justifyContent: 'center', mt: 1 }}>
                      <Box sx={{ width: 160 }}>
                        <ProgressBar value={50} className="bg-red-500" progressClassName="bg-blue-500" />
                      </Box>
                    </Box>
                </Box>
                {/* East Rikishi Avatar with winner indicator */}
                <Box sx={{ position: 'relative' }}>
                  <Avatar size="sm" src="/static/images/avatar/default.jpg" />
                  {match.winner === 'east' && (
                    <Box sx={{ position: 'absolute', top: -8, left: -8, background: '#22c55e', color: 'white', borderRadius: '50%', width: 17, height: 17, display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 9, fontWeight: 700, boxShadow: '0 1px 4px rgba(0,0,0,0.12)' }}>
                      W
                    </Box>
                  )}
                </Box>
              </ListItem>
              {idx < sampleMatches.length - 1 && <ListDivider inset="gutter" />}
            </React.Fragment>
          ))}
        </List>
      </div>
    </Box>
  );
}

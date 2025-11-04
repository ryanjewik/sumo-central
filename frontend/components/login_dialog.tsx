"use client";

import * as React from 'react';

import Button from '@mui/material/Button';
import Dialog from '@mui/material/Dialog';
import DialogActions from '@mui/material/DialogActions';
import DialogContent from '@mui/material/DialogContent';
import DialogTitle from '@mui/material/DialogTitle';
import Slide from '@mui/material/Slide';
import TextField from '@mui/material/TextField';
import Box from '@mui/material/Box';
import type { TransitionProps } from '@mui/material/transitions';
import RegisterDialog from './register_dialog';

const Transition = React.forwardRef(function Transition(
  props: TransitionProps & { children: React.ReactElement<any, any> },
  ref: React.Ref<unknown>,
) {
  return <Slide direction="up" ref={ref} {...props} />;
});

import { useAuth } from '../context/AuthContext';

interface LoginDialogProps {
  open: boolean;
  onClose: () => void;
}

const LoginDialog: React.FC<LoginDialogProps> = ({ open, onClose }) => {
  const { setUser, login } = useAuth();
  const [username, setUsername] = React.useState('');
  const [password, setPassword] = React.useState('');
  const [registerOpen, setRegisterOpen] = React.useState(false);
  const [error, setError] = React.useState('');
  const [success, setSuccess] = React.useState(false);

  const handleClose = () => {
    setUsername('');
    setPassword('');
    setError('');
    setSuccess(false);
    onClose();
  };

  const handleLogin = async () => {
    setError('');
    setSuccess(false);
    if (!username || !password) {
      setError('Please enter both username and password.');
      return;
    }
    try {
      const result = await login(username, password);
      if (result.ok) {
        setSuccess(true);
        setTimeout(() => {
          setSuccess(false);
          handleClose();
        }, 1200);
      } else {
        setError(result.error || 'Login failed.');
      }
    } catch (err) {
      setError('Login failed.');
    }
  };

  const handleOpenRegister = () => setRegisterOpen(true);
  const handleCloseRegister = () => setRegisterOpen(false);

  return (
    <>
      <Dialog
        open={open}
        slots={{ transition: Transition }}
        keepMounted
        onClose={handleClose}
        aria-describedby="login-dialog-slide-description"
        maxWidth="xs"
        PaperProps={{
          sx: {
            minWidth: 380,
            maxWidth: 440,
            borderRadius: '1.2rem',
            background: 'linear-gradient(135deg, #f5e6c8 0%, #e0a3c2 100%)',
            border: '3px solid #563861',
            boxShadow: '0 2px 16px 0 rgba(86,56,97,0.13)',
            p: { xs: 3, sm: 4 },
            fontFamily: `'Courier New', Courier, monospace`,
          },
        }}
      >
        <DialogTitle sx={{ textAlign: 'center', pb: 0 }}>
          <Box sx={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 1 }}>
            <img src="/sumo_logo.png" alt="Sumo App Logo" style={{ width: 56, height: 56, marginBottom: 2 }} />
            <span style={{ fontWeight: 900, fontSize: '1.6rem', color: '#563861', letterSpacing: '0.04em', fontFamily: `'Courier New', Courier, monospace` }}>
              Sumo App
            </span>
            <span style={{ fontWeight: 700, fontSize: '1.18rem', color: '#563861', opacity: 0.88, fontFamily: `'Courier New', Courier, monospace` }}>
              Login to Your Account
            </span>
          </Box>
        </DialogTitle>

        <DialogContent sx={{ pt: 1.5, pb: 0 }}>
          {success && (
            <Box sx={{ color: 'green', fontSize: '1.05em', textAlign: 'center', mb: 1 }}>
              Login successful!
            </Box>
          )}
          {error && (
            <Box sx={{ color: 'red', fontSize: '0.98em', textAlign: 'center', mb: 1 }}>{error}</Box>
          )}
          <Box
            component="form"
            sx={{
              display: 'flex',
              flexDirection: 'column',
              gap: 2.5,
              mt: 1,
              minWidth: 320,
              maxWidth: 380,
              mx: 'auto',
            }}
            autoComplete="off"
          >
            <TextField
              label="Username"
              variant="outlined"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              autoFocus
              fullWidth
            />
            <TextField
              label="Password"
              variant="outlined"
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              fullWidth
            />
          </Box>
        </DialogContent>

        {/* IMPORTANT: disableSpacing removes DialogActions' default left margin on children */}
        <DialogActions
          disableSpacing
          sx={{
            flexDirection: 'column',
            alignItems: 'stretch',
            gap: 1.5,
            px: 3,
            pb: 2,
            pt: 2,
            width: '100%',
            // extra guard in case theme overrides add margins
            '& > :not(:first-of-type)': { ml: 0 },
          }}
        >
          <Button
            onClick={handleLogin}
            variant="contained"
            color="primary"
            disabled={!username || !password}
            fullWidth
            sx={{
              fontWeight: 600,
              borderRadius: '0.7rem',
              fontSize: '1.08rem',
              fontFamily: 'inherit',
              background: 'linear-gradient(90deg, #563861 0%, #e0a3c2 100%)',
              color: '#fff',
              boxShadow: '0 2px 8px 0 rgba(86,56,97,0.10)',
              border: '2px solid #563861',
              '&:hover': {
                background: 'linear-gradient(90deg, #e0a3c2 0%, #563861 100%)',
                color: '#fff',
              },
            }}
          >
            Login
          </Button>

          <Button
            onClick={handleClose}
            color="secondary"
            fullWidth
            sx={{
              fontWeight: 500,
              borderRadius: '0.7rem',
              fontSize: '1.02rem',
              fontFamily: 'inherit',
              background: '#fff',
              color: '#563861',
              border: '2px solid #563861',
              '&:hover': { background: '#f5e6c8' },
            }}
          >
            Cancel
          </Button>

          <Box sx={{ textAlign: 'center', mt: 1, width: '100%' }}>
            <span style={{ color: '#563861', fontSize: '0.98rem', fontFamily: 'inherit' }}>
              Don&apos;t have an account?
            </span>
            <Button
              onClick={handleOpenRegister}
              sx={{
                ml: 0.5,
                fontWeight: 600,
                fontSize: '1.01rem',
                textTransform: 'none',
                p: 0,
                minWidth: 'unset',
                fontFamily: 'inherit',
                color: '#563861',
                background: 'transparent',
                '&:hover': {
                  background: 'rgba(224,163,194,0.13)',
                  color: '#563861',
                },
              }}
            >
              Register
            </Button>
          </Box>
        </DialogActions>
      </Dialog>

      <RegisterDialog open={registerOpen} onClose={handleCloseRegister} />
    </>
  );
};

export default LoginDialog;

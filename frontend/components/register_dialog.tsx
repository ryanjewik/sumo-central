"use client";

import * as React from 'react';
import Button from '@mui/material/Button';
import Dialog from '@mui/material/Dialog';
import DialogActions from '@mui/material/DialogActions';
import DialogContent from '@mui/material/DialogContent';
import TextField from '@mui/material/TextField';
import Box from '@mui/material/Box';
import DialogTitle from '@mui/material/DialogTitle';
import Slide from '@mui/material/Slide';
import type { TransitionProps } from '@mui/material/transitions';
import Image from 'next/image';


const Transition = React.forwardRef(function Transition(
  props: TransitionProps & {
    children: React.ReactElement;
  },
  ref: React.Ref<unknown>,
) {
  return <Slide direction="up" ref={ref} {...props} />;
});

interface RegisterDialogProps {
  open: boolean;
  onClose: () => void;
}

import { useAuth } from '../context/AuthContext';

const RegisterDialog: React.FC<RegisterDialogProps> = ({ open, onClose }) => {
  const { register } = useAuth();
  const [username, setUsername] = React.useState('');
  const [email, setEmail] = React.useState('');
  const [password, setPassword] = React.useState('');
  const [confirmPassword, setConfirmPassword] = React.useState('');
  const [error, setError] = React.useState('');
  const [success, setSuccess] = React.useState(false);

  const handleRegister = async () => {
    // Basic validation
    if (!username || !email || !password || !confirmPassword) {
      setError('Please fill in all fields.');
      return;
    }
    // Email format validation
    const emailRegex = /^[\w.-]+@[\w.-]+\.\w+$/;
    if (!emailRegex.test(email)) {
      setError('Invalid email format.');
      return;
    }
    // Password strength validation
    if (password.length < 10) {
      setError('Password must be at least 10 characters.');
      return;
    }
    if (!/[A-Z]/.test(password)) {
      setError('Password must contain at least one uppercase letter.');
      return;
    }
    if (!/\d/.test(password)) {
      setError('Password must contain at least one number.');
      return;
    }
    if (password !== confirmPassword) {
      setError('Passwords do not match.');
      return;
    }
    setError('');
    setSuccess(false);
    // Send registration data to backend via provider
    try {
      const result = await register(username, email, password);
      if (result.ok) {
        setSuccess(true);
        setTimeout(() => {
          setSuccess(false);
          handleClose();
        }, 1500);
      } else {
        setError(result.error || 'Registration failed.');
      }
    } catch {
      setError('Registration failed.');
    }
  };

  const handleClose = () => {
    setUsername('');
    setEmail('');
    setPassword('');
    setConfirmPassword('');
    setError('');
    setSuccess(false);
    onClose();
  };

  return (
    <Dialog
      open={open}
      slots={{
        transition: Transition,
      }}
      keepMounted
      onClose={handleClose}
      aria-describedby="register-dialog-slide-description"
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
          <Image src="/sumo_logo.png" alt="Sumo App Logo" width={56} height={56} style={{ width: 56, height: 56, marginBottom: 2 }} />
          <span style={{ fontWeight: 900, fontSize: '1.6rem', color: '#563861', letterSpacing: '0.04em', fontFamily: `'Courier New', Courier, monospace` }}>Sumo App</span>
          <span style={{ fontWeight: 700, fontSize: '1.18rem', color: '#563861', opacity: 0.88, fontFamily: `'Courier New', Courier, monospace` }}>Register for Sumo App</span>
        </Box>
      </DialogTitle>
      <DialogContent sx={{ pt: 1.5, pb: 0 }}>
        {success && (
          <Box sx={{ color: 'green', fontSize: '1.05em', textAlign: 'center', mb: 1 }}>
            Registration successful!
          </Box>
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
            onChange={e => setUsername(e.target.value)}
            fullWidth
          />
          <TextField
            label="Email"
            variant="outlined"
            type="email"
            value={email}
            onChange={e => setEmail(e.target.value)}
            fullWidth
          />
          <TextField
            label="Password"
            variant="outlined"
            type="password"
            value={password}
            onChange={e => setPassword(e.target.value)}
            fullWidth
          />
          <TextField
            label="Confirm Password"
            variant="outlined"
            type="password"
            value={confirmPassword}
            onChange={e => setConfirmPassword(e.target.value)}
            fullWidth
          />
          {error && (
            <Box sx={{ color: 'red', fontSize: '0.98em', textAlign: 'center', mt: -1 }}>{error}</Box>
          )}
        </Box>
      </DialogContent>
      <DialogActions disableSpacing sx={{ flexDirection: 'column', alignItems: 'stretch', gap: 1.5, px: 3, pb: 2, pt: 2, width: '100%' }}>
        <Button
          onClick={handleRegister}
          variant="contained"
          color="primary"
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
          Register
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
      </DialogActions>
    </Dialog>
  );
};

export default RegisterDialog;

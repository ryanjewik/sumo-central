"use client";

import React from 'react';
import { AuthProvider } from '@/context/AuthContext';
import FetchInterceptor from './FetchInterceptor';

export default function ClientProviders({ children }: { children: React.ReactNode }) {
  // Keep client-only providers here (AuthProvider, theme hooks that require client, etc.)
  return (
    <AuthProvider>
      <FetchInterceptor />
      {children}
    </AuthProvider>
  );
}


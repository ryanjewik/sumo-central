
import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { BrowserRouter } from 'react-router-dom';
import { RouteProvider } from './providers/route-provider';
import { ThemeProvider } from './providers/theme-provider';
import './index.css';
import App from './App.tsx';

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <BrowserRouter>
      <RouteProvider>
        <ThemeProvider>
          <App />
        </ThemeProvider>
      </RouteProvider>
    </BrowserRouter>
  </StrictMode>
);

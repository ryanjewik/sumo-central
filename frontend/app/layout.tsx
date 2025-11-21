// app/layout.tsx
import type { Metadata } from "next";
import { Geist, Geist_Mono } from "next/font/google";
import "@/styles/globals.css";

import { ThemeProvider } from "@/providers/theme-provider";
import { RouteProvider } from "@/providers/route-provider";
import ClientProviders from '@/components/ClientProviders';
import Header from '@/components/Header';
import React from 'react';

const geistSans = Geist({ variable: "--font-geist-sans", subsets: ["latin"] });
const geistMono = Geist_Mono({ variable: "--font-geist-mono", subsets: ["latin"] });

export const metadata: Metadata = {
  title: "Sumopedia",
  description: "Sumopedia — live Sumo match analytics, head-to-head stats, community predictions and match voting. Explore rikishi profiles, recent results, and upcoming bouts.",
};

// Small, focused critical CSS for the navbar + highlighted match hero and page offset.
// Keep this minimal — it will be inlined into the <head> so the browser can render
// the header and hero immediately without waiting for the large global CSS file.
const criticalCss = `
nav.navbar { position: fixed; top: 0; left: 0; right: 0; z-index: 10000; background: #E0A3C2; color: #563861; box-shadow: 0 2px 8px rgba(0,0,0,0.04); --navbar-height: 6rem; }
.navbar-inner{ background:#E0A3C2; padding: 2.6rem 0 0 0; margin-inline: clamp(0.5rem,10vw,25rem); }
.navbar-row-top{ display:flex; align-items:center; justify-content:space-between; gap:1rem; padding:0.5rem 0 1.75rem 0; }
.navbar-search{ padding:0.5rem 1.5rem; border-radius:10px; height:2rem; background:#fff; color:#563861; box-shadow:0 1px 4px #A3E0B8; }
.navbar-logo{ width:100px; height:100px; object-fit:contain; }
.navbar-title{ font-size:1.7rem; font-weight:700; color:#563861; }
.page-offset{ margin: calc(var(--navbar-height,6rem) + 8rem) auto 0; padding:0 3.5rem; position:relative; z-index:1; }
.content-box{ margin-top:10rem; padding-block:2rem; padding-inline:clamp(0.5rem,15vw,25rem); display:flex; align-items:flex-start; gap:1rem; }
.extra-vertical-pad{ padding-top: 3rem; }
.highlighted-match-flex-group{ display:flex; align-items:center; justify-content:center; gap:2rem; width:100%; }
.rikishi-img{ width:120px; height:160px; border-radius:18px; border:3px solid #388eec; object-fit:cover; background:#fff; }
.rikishi-img.east{ border-color:#d32f2f; }
.vs-text{ font-weight:700; font-size:2.5rem; color:#563861; }
@media (max-width:900px){ .rikishi-img{ width:80px !important; height:110px !important; } }
`;

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" suppressHydrationWarning className={`${geistSans.variable} ${geistMono.variable}`}>
      <head>
        {/* Inline critical CSS for faster first render of header + hero */}
        <style dangerouslySetInnerHTML={{ __html: criticalCss }} />
        {/* Favicon / tab icon - use sumo_logo.webp as requested */}
        <link rel="icon" href="/sumo_logo.webp" type="image/webp" />
        <link rel="apple-touch-icon" href="/sumo_logo.webp" />
      </head>
      <body className="antialiased">
        <RouteProvider>
          <ThemeProvider>
            <ClientProviders>
              <Header />
              {children}
            </ClientProviders>
          </ThemeProvider>
        </RouteProvider>
      </body>
    </html>
  );
}

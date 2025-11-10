// app/layout.tsx
import type { Metadata } from "next";
import { Geist, Geist_Mono } from "next/font/google";
import "@/styles/globals.css";

import { ThemeProvider } from "@/providers/theme-provider";
import { RouteProvider } from "@/providers/route-provider";
import ClientProviders from '@/components/ClientProviders';
import Header from '@/components/Header';

const geistSans = Geist({ variable: "--font-geist-sans", subsets: ["latin"] });
const geistMono = Geist_Mono({ variable: "--font-geist-mono", subsets: ["latin"] });

export const metadata: Metadata = {
  title: "Sumopedia",
  description: "Sumopedia analytics",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" suppressHydrationWarning className={`${geistSans.variable} ${geistMono.variable}`}>
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

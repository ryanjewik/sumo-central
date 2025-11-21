/** @type {import('next').NextConfig} */
const BACKEND_URL = process.env.BACKEND_URL || 'http://localhost:8080';

// Integrate optional bundle analyzer when ANALYZE=true is set in the env.
// This keeps the analyzer out of normal builds but allows an easy diagnostic
// run: `$env:ANALYZE = 'true'; npm run build` (PowerShell)
let nextConfig = {
  // `appDir` is now controlled by Next.js version; do not set it here to avoid unrecognized-key warnings.
  images: {
    // Use remotePatterns instead of the deprecated `domains` array.
    // This more precisely constrains allowed remote image hostnames and paths.
    remotePatterns: [
      {
        protocol: 'https',
        hostname: 'ryans-sumo-bucket.s3.us-west-2.amazonaws.com',
        pathname: '/**',
      },
      {
        protocol: 'https',
        hostname: 's3.us-west-2.amazonaws.com',
        pathname: '/**',
      },
      {
        protocol: 'https',
        hostname: 's3.amazonaws.com',
        pathname: '/**',
      },
      {
        protocol: 'https',
        hostname: 'upload.wikimedia.org',
        pathname: '/**',
      },
      {
        protocol: 'https',
        hostname: 'sumopedia.net',
        pathname: '/**',
      },
    ],
  },
  // Proxy Next `/api/*` requests to the backend when BACKEND_URL is set.
  // NOTE: rewrites to the backend were removed so that explicit
  // `app/api` route handlers (for example `/api/discussions/[id]`)
  // are executed in the Next runtime and can perform logging/proxying.
  // If you still need a blanket `/api/*` rewrite to the backend,
  // prefer setting BACKEND_URL at build time and be aware rewrites
  // are baked into the build (which prevents per-container runtime
  // overrides). For now we return an empty array so app/api routes run.
  async rewrites() {
    return []
  },
};

try {
  // lazily require to avoid a hard dependency for normal dev/build runs
  const withBundleAnalyzer = require('@next/bundle-analyzer')({ enabled: process.env.ANALYZE === 'true' });
  nextConfig = withBundleAnalyzer(nextConfig);
} catch (e) {
  // if the analyzer isn't installed, silently continue — analysis is optional
}

module.exports = nextConfig;

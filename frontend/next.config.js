/** @type {import('next').NextConfig} */
const BACKEND_URL = process.env.BACKEND_URL || 'http://localhost:8080';

const nextConfig = {
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

module.exports = nextConfig;

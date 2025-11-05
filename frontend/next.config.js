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
  async rewrites() {
    return [
      {
        source: '/api/:path*',
        destination: `${BACKEND_URL}/:path*`,
      },
    ];
  },
};

module.exports = nextConfig;

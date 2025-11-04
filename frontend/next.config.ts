import type { NextConfig } from "next";

// Read backend base URL from environment. When running inside Docker compose
// the frontend service sets BACKEND_URL=http://gin-backend:8080 so the dev
// server will proxy to the backend container. Defaults to localhost:8080 for
// local (non-container) development.
const BACKEND_URL = process.env.BACKEND_URL || "http://localhost:8080";

const nextConfig: NextConfig = {
  reactStrictMode: true,
  async rewrites() {
    return [
      // Forward Next `/api/*` requests to the backend service, stripping the
      // `/api` prefix so the backend route `/auth/register` receives `/auth/register`.
      {
        source: "/api/:path*",
        destination: `${BACKEND_URL}/:path*`,
      },
    ];
  },
};

export default nextConfig;

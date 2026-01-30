import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  async rewrites() {
    return [
      {
        source: '/api/:path*',
        destination: 'http://localhost:5000/api/:path*',
      },
      {
        source: '/fetch_games',
        destination: 'http://localhost:5000/fetch_games',
      },
      {
        source: '/fetch_odds',
        destination: 'http://localhost:5000/fetch_odds',
      },
      {
        source: '/post_discord',
        destination: 'http://localhost:5000/post_discord',
      },
    ];
  },
};

export default nextConfig;

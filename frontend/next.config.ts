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
      {
        source: '/post_discord_window/:window',
        destination: 'http://localhost:5000/post_discord_window/:window',
      },
      {
        source: '/check_results',
        destination: 'http://localhost:5000/check_results',
      },
      {
        source: '/update_result/:id',
        destination: 'http://localhost:5000/update_result/:id',
      },
    ];
  },
};

export default nextConfig;

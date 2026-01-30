self.__BUILD_MANIFEST = {
  "__rewrites": {
    "afterFiles": [
      {
        "source": "/api/:path*"
      },
      {
        "source": "/fetch_games"
      },
      {
        "source": "/fetch_odds"
      },
      {
        "source": "/post_discord"
      },
      {
        "source": "/post_discord_window/:window"
      },
      {
        "source": "/check_results"
      },
      {
        "source": "/update_result/:id"
      }
    ],
    "beforeFiles": [],
    "fallback": []
  },
  "sortedPages": [
    "/_app",
    "/_error"
  ]
};self.__BUILD_MANIFEST_CB && self.__BUILD_MANIFEST_CB()
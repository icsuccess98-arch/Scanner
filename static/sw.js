const CACHE_NAME = '730-locks-v4';
const STATIC_CACHE = '730-locks-static-v4';
const DATA_CACHE = '730-locks-data-v4';

const urlsToCache = [
  '/',
  '/spreads',
  '/props',
  '/bankroll',
  '/history',
  '/static/manifest.json',
  '/static/icon-512.png',
  '/static/icon-192.png',
  '/static/icon-128.png',
  '/static/icon-96.png',
  '/static/icon-64.png',
  '/static/images/730_sports_logo.png',
  '/offline.html',
  '/favicon.ico'
];

const cdnResources = [
  'https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css',
  'https://cdn.jsdelivr.net/npm/bootstrap-icons@1.10.0/font/bootstrap-icons.css',
  'https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap'
];

self.addEventListener('install', (event) => {
  event.waitUntil(
    Promise.all([
      caches.open(CACHE_NAME).then((cache) => cache.addAll(urlsToCache)),
      caches.open(STATIC_CACHE).then((cache) => {
        return Promise.allSettled(cdnResources.map(url => 
          cache.add(url).catch(err => console.log('CDN cache failed:', url))
        ));
      })
    ])
  );
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  const validCaches = [CACHE_NAME, STATIC_CACHE, DATA_CACHE];
  event.waitUntil(
    caches.keys().then((cacheNames) => {
      return Promise.all(
        cacheNames.map((cacheName) => {
          if (!validCaches.includes(cacheName)) {
            return caches.delete(cacheName);
          }
        })
      );
    })
  );
  self.clients.claim();
});

self.addEventListener('fetch', (event) => {
  if (event.request.method !== 'GET') return;
  
  // Skip caching for API mutations
  if (event.request.url.includes('/fetch_') || 
      event.request.url.includes('/post_discord')) {
    return;
  }
  
  // Cache API data responses with network-first, fallback to cache for offline
  if (event.request.url.includes('/api/')) {
    event.respondWith(
      fetch(event.request)
        .then((response) => {
          if (response && response.status === 200) {
            const responseToCache = response.clone();
            caches.open(DATA_CACHE).then((cache) => {
              cache.put(event.request, responseToCache);
            });
          }
          return response;
        })
        .catch(() => {
          return caches.match(event.request).then((cached) => {
            if (cached) {
              return cached;
            }
            // Return empty data if no cache
            return new Response(JSON.stringify({
              success: true,
              props: [],
              games: [],
              message: 'Offline - showing cached data'
            }), {
              headers: { 'Content-Type': 'application/json' }
            });
          });
        })
    );
    return;
  }
  
  // Network-first for HTML pages
  event.respondWith(
    fetch(event.request)
      .then((response) => {
        if (response && response.status === 200) {
          const responseToCache = response.clone();
          caches.open(CACHE_NAME).then((cache) => {
            cache.put(event.request, responseToCache);
          });
        }
        return response;
      })
      .catch(() => {
        return caches.match(event.request)
          .then((response) => {
            if (response) return response;
            const accept = event.request.headers.get('accept') || '';
            if (accept.includes('text/html')) {
              return caches.match('/offline.html');
            }
          });
      })
  );
});

self.addEventListener('push', (event) => {
  const data = event.data ? event.data.json() : {};
  const options = {
    body: data.body || 'New picks available!',
    icon: '/static/icon-512.png',
    badge: '/static/icon-512.png',
    vibrate: [100, 50, 100],
    data: {
      url: data.url || '/'
    }
  };
  
  event.waitUntil(
    self.registration.showNotification(data.title || "730's Locks", options)
  );
});

self.addEventListener('notificationclick', (event) => {
  event.notification.close();
  event.waitUntil(
    clients.openWindow(event.notification.data.url || '/')
  );
});

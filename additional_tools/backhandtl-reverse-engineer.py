#!/usr/bin/env python3
"""
730's Locks - BackhandTL Reverse Engineering Tool
Extract all data sources, APIs, and methodologies from BackhandTL
"""

import requests
import json
import re
from urllib.parse import urljoin, urlparse
import time
from datetime import datetime
from dataclasses import dataclass
from typing import Dict, List, Optional

@dataclass
class BackhandTLIntel:
    supabase_url: str
    api_key: str
    database_tables: List[str]
    rpc_functions: List[str]
    data_fields: List[str]
    api_endpoints: List[str]

class BackhandTLReverseEngineer:
    def __init__(self):
        self.base_url = "https://backhandtl.com"
        self.supabase_url = "https://suoaznisiowoolxilaju.supabase.co"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
        
        # Extracted from JS bundle analysis
        self.database_tables = [
            'players', 'market_odds', 'odds_history', 'scouting_reports',
            'player_skills', 'player_achievements', 'tournaments', 'profiles',
            'favorites', 'support_tickets', 'user_events', 'upvotes',
            'promo_codes', 'feedback_posts', 'fantasy_lineups', 'fantasy_gameweeks',
            'articles'
        ]
        
        self.rpc_functions = [
            'check_and_reset_credits',
            'delete_my_account', 
            'redeem_promo_code',
            'use_matchup_analyzer'
        ]
        
        self.key_fields = [
            'ai_fair_odds', 'fair_odds', 'edge_percent', 'confidence_score',
            'bsi_rating', 'form_rating', 'ovr_rating', 'win_probability',
            'hot_streak', 'hold_pct', 'surface_ratings', 'matches_tracked',
            'player1_name', 'player2_name', 'odds1', 'odds2', 'actual_winner_name',
            'score', 'surface', 'tournament', 'round', 'market_odds'
        ]
    
    def extract_supabase_config(self) -> Dict[str, str]:
        """Extract Supabase configuration from the JS bundle"""
        try:
            print("🔍 Extracting Supabase configuration...")
            
            # Get the main JS bundle
            js_url = f"{self.base_url}/assets/index-B50aJzaH.js"
            response = self.session.get(js_url)
            js_content = response.text
            
            # Extract Supabase URL (already found)
            supabase_url = "https://suoaznisiowoolxilaju.supabase.co"
            
            # Look for Supabase anon key pattern
            anon_key_patterns = [
                r'"(eyJ[A-Za-z0-9_-]{100,})"',  # JWT pattern
                r'supabaseKey["\s:]+["\'](eyJ[^"\']+)["\']',
                r'SUPABASE_ANON_KEY["\s:]+["\'](eyJ[^"\']+)["\']',
                r'anon["\s:]+["\'](eyJ[^"\']+)["\']'
            ]
            
            anon_key = None
            for pattern in anon_key_patterns:
                matches = re.findall(pattern, js_content)
                if matches:
                    # JWT tokens starting with eyJ are likely the anon key
                    for match in matches:
                        if len(match) > 100:  # Supabase keys are long
                            anon_key = match
                            break
                    if anon_key:
                        break
            
            print(f"✅ Supabase URL: {supabase_url}")
            if anon_key:
                print(f"✅ Anon Key Found: {anon_key[:20]}...")
            else:
                print("❌ Anon Key: Not found in JS bundle")
            
            return {
                'url': supabase_url,
                'anon_key': anon_key,
                'service_role': None  # Would need server access
            }
            
        except Exception as e:
            print(f"❌ Error extracting Supabase config: {e}")
            return {}
    
    def probe_supabase_api(self, config: Dict[str, str]) -> Dict:
        """Probe Supabase API endpoints for data access"""
        try:
            print("🔍 Probing Supabase API endpoints...")
            
            if not config.get('anon_key'):
                print("❌ No anon key - cannot probe API")
                return {}
            
            headers = {
                'apikey': config['anon_key'],
                'Authorization': f"Bearer {config['anon_key']}",
                'Content-Type': 'application/json',
                'Prefer': 'return=representation'
            }
            
            results = {}
            
            # Test each table for public access
            for table in self.database_tables:
                try:
                    url = f"{config['url']}/rest/v1/{table}?select=*&limit=1"
                    response = self.session.get(url, headers=headers, timeout=10)
                    
                    if response.status_code == 200:
                        data = response.json()
                        results[table] = {
                            'accessible': True,
                            'sample_count': len(data),
                            'fields': list(data[0].keys()) if data else []
                        }
                        print(f"✅ {table}: Accessible ({len(data)} rows)")
                    elif response.status_code == 401:
                        results[table] = {'accessible': False, 'error': 'Unauthorized'}
                        print(f"❌ {table}: Requires authentication")
                    elif response.status_code == 404:
                        results[table] = {'accessible': False, 'error': 'Not found'}
                        print(f"⚠️  {table}: Table not found")
                    else:
                        results[table] = {'accessible': False, 'error': f'HTTP {response.status_code}'}
                        print(f"❌ {table}: HTTP {response.status_code}")
                        
                except Exception as e:
                    results[table] = {'accessible': False, 'error': str(e)}
                    print(f"❌ {table}: {str(e)}")
                
                time.sleep(0.5)  # Be respectful
            
            return results
            
        except Exception as e:
            print(f"❌ Error probing Supabase API: {e}")
            return {}
    
    def extract_data_models(self, js_content: str) -> Dict:
        """Extract data models and field definitions from JavaScript"""
        try:
            print("🔍 Extracting data models...")
            
            # Look for TypeScript interfaces or data structures
            interface_patterns = [
                r'interface\s+(\w+)\s*\{([^}]+)\}',
                r'type\s+(\w+)\s*=\s*\{([^}]+)\}',
                r'const\s+(\w+Schema)\s*=\s*\{([^}]+)\}'
            ]
            
            models = {}
            
            # Extract field definitions from select statements
            select_patterns = re.findall(r'\.select\("([^"]+)"\)', js_content)
            
            for select in select_patterns:
                fields = [f.strip() for f in select.split(',')]
                if '*' not in fields:
                    # This gives us the actual fields being queried
                    for field in fields:
                        if field not in models:
                            models[field] = {'usage_count': 1, 'tables': []}
                        else:
                            models[field]['usage_count'] += 1
            
            # Look for validation schemas or field definitions
            field_patterns = [
                r'(\w+):\s*string',
                r'(\w+):\s*number',
                r'(\w+):\s*boolean',
                r'(\w+):\s*Date',
            ]
            
            print(f"✅ Found {len(models)} field references")
            return models
            
        except Exception as e:
            print(f"❌ Error extracting data models: {e}")
            return {}
    
    def analyze_calculation_methods(self, js_content: str) -> List[str]:
        """Extract calculation methods for ratings and predictions"""
        try:
            print("🔍 Analyzing calculation methods...")
            
            calculations = []
            
            # Look for mathematical formulas
            formula_patterns = [
                r'edge_percent\s*=\s*([^;]+);',
                r'fair_odds\s*=\s*([^;]+);',
                r'confidence\s*=\s*([^;]+);',
                r'win_probability\s*=\s*([^;]+);',
                r'rating\s*=\s*([^;]+);'
            ]
            
            for pattern in formula_patterns:
                matches = re.findall(pattern, js_content, re.IGNORECASE)
                calculations.extend(matches)
            
            # Look for API calculation endpoints
            calc_endpoints = re.findall(r'/api/calculate/(\w+)', js_content)
            calculations.extend([f"API endpoint: /api/calculate/{ep}" for ep in calc_endpoints])
            
            print(f"✅ Found {len(calculations)} calculation methods")
            return calculations
            
        except Exception as e:
            print(f"❌ Error analyzing calculations: {e}")
            return []
    
    def find_external_data_sources(self, js_content: str) -> List[str]:
        """Find external APIs and data sources"""
        try:
            print("🔍 Finding external data sources...")
            
            # Look for external API calls
            api_patterns = [
                r'https://[a-zA-Z0-9.-]+/api/[^"\s]+',
                r'https://[a-zA-Z0-9.-]+\.com/[^"\s]+',
                r'fetch\(["\']([^"\']+)["\']',
                r'axios\.[get|post]+\(["\']([^"\']+)["\']'
            ]
            
            external_sources = set()
            
            for pattern in api_patterns:
                matches = re.findall(pattern, js_content)
                for match in matches:
                    if isinstance(match, tuple):
                        match = match[0]
                    if 'backhandtl.com' not in match and 'supabase' not in match:
                        external_sources.add(match)
            
            # Look for tennis data providers
            tennis_apis = [
                'api.tennisdata.com', 'flashscore.com', 'sofascore.com',
                'tennisapi.org', 'tennislive.net', 'atp.com', 'wta.com'
            ]
            
            for api in tennis_apis:
                if api in js_content:
                    external_sources.add(f"Tennis API: {api}")
            
            print(f"✅ Found {len(external_sources)} external sources")
            return list(external_sources)
            
        except Exception as e:
            print(f"❌ Error finding external sources: {e}")
            return []
    
    def build_data_extraction_methods(self, config: Dict) -> Dict:
        """Build methods to extract data programmatically"""
        
        methods = {
            'direct_supabase': {
                'description': 'Direct Supabase API access (if public)',
                'url_pattern': f"{config.get('url', '')}/rest/v1/{{table}}?select=*",
                'headers': {
                    'apikey': config.get('anon_key', ''),
                    'Authorization': f"Bearer {config.get('anon_key', '')}"
                }
            },
            'web_scraping': {
                'description': 'Scrape rendered pages',
                'endpoints': [
                    f"{self.base_url}/scout",
                    f"{self.base_url}/scanner", 
                    f"{self.base_url}/matchup"
                ]
            },
            'api_proxy': {
                'description': 'Proxy through their frontend API calls',
                'method': 'Monitor network requests and replicate'
            }
        }
        
        return methods
    
    def generate_extraction_code(self, config: Dict, api_results: Dict) -> str:
        """Generate Python code to extract BackhandTL data"""
        
        code = f'''#!/usr/bin/env python3
"""
730's Locks - BackhandTL Data Extractor
Auto-generated extraction tool
"""

import requests
import json
from datetime import datetime

class BackhandTLExtractor:
    def __init__(self):
        self.supabase_url = "{config.get('url', '')}"
        self.anon_key = "{config.get('anon_key', '')}"
        self.session = requests.Session()
        self.session.headers.update({{
            'apikey': self.anon_key,
            'Authorization': f"Bearer {{self.anon_key}}",
            'Content-Type': 'application/json',
            'User-Agent': 'Mozilla/5.0 (compatible; 730sLocks/1.0)'
        }})
    
    def get_players(self, limit=100):
        """Extract player data"""
        url = f"{{self.supabase_url}}/rest/v1/players?select=*&limit={{limit}}"
        response = self.session.get(url)
        return response.json() if response.status_code == 200 else None
    
    def get_market_odds(self, limit=100):
        """Extract current market odds"""
        url = f"{{self.supabase_url}}/rest/v1/market_odds?select=*&limit={{limit}}"
        response = self.session.get(url)
        return response.json() if response.status_code == 200 else None
    
    def get_scouting_reports(self, player_id=None):
        """Extract scouting reports"""
        url = f"{{self.supabase_url}}/rest/v1/scouting_reports?select=*"
        if player_id:
            url += f"&player_id=eq.{{player_id}}"
        response = self.session.get(url)
        return response.json() if response.status_code == 200 else None
    
    def use_matchup_analyzer(self, player1_id, player2_id):
        """Use their matchup analyzer RPC"""
        url = f"{{self.supabase_url}}/rest/v1/rpc/use_matchup_analyzer"
        data = {{"player1_id": player1_id, "player2_id": player2_id}}
        response = self.session.post(url, json=data)
        return response.json() if response.status_code == 200 else None

# Usage example
extractor = BackhandTLExtractor()
players = extractor.get_players()
odds = extractor.get_market_odds()
'''
        
        return code
    
    def full_reverse_engineer(self):
        """Complete reverse engineering analysis"""
        print("=" * 80)
        print("🎾 730'S LOCKS - BACKHANDTL REVERSE ENGINEERING")
        print("=" * 80)
        
        # Step 1: Get JS content
        print("\n🔍 Step 1: Extracting JavaScript bundle...")
        js_response = self.session.get(f"{self.base_url}/assets/index-B50aJzaH.js")
        js_content = js_response.text
        
        # Step 2: Extract Supabase config
        print("\n🔍 Step 2: Extracting Supabase configuration...")
        config = self.extract_supabase_config()
        
        # Step 3: Probe API access
        print("\n🔍 Step 3: Probing API endpoints...")
        api_results = self.probe_supabase_api(config)
        
        # Step 4: Extract data models
        print("\n🔍 Step 4: Extracting data models...")
        models = self.extract_data_models(js_content)
        
        # Step 5: Analyze calculations
        print("\n🔍 Step 5: Analyzing calculation methods...")
        calculations = self.analyze_calculation_methods(js_content)
        
        # Step 6: Find external sources
        print("\n🔍 Step 6: Finding external data sources...")
        external_sources = self.find_external_data_sources(js_content)
        
        # Step 7: Generate extraction code
        print("\n🔍 Step 7: Generating extraction code...")
        extraction_code = self.generate_extraction_code(config, api_results)
        
        # Save results
        results = {
            'timestamp': datetime.now().isoformat(),
            'supabase_config': config,
            'api_accessibility': api_results,
            'data_models': models,
            'calculations': calculations,
            'external_sources': external_sources,
            'extraction_methods': self.build_data_extraction_methods(config)
        }
        
        return results, extraction_code

def main():
    engineer = BackhandTLReverseEngineer()
    results, code = engineer.full_reverse_engineer()
    
    # Save results
    with open('/home/icsuccess98/.openclaw/workspace/content/research/backhandtl_reverse_engineering.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    with open('/home/icsuccess98/.openclaw/workspace/tools/backhandtl_extractor.py', 'w') as f:
        f.write(code)
    
    print("\n" + "=" * 80)
    print("✅ REVERSE ENGINEERING COMPLETE")
    print("=" * 80)
    print(f"📊 Results saved to: backhandtl_reverse_engineering.json")
    print(f"🛠️  Extractor saved to: backhandtl_extractor.py")

if __name__ == "__main__":
    main()
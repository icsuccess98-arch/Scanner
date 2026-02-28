#!/usr/bin/env python3
"""
Value Scanner Engine - BackhandTL Complete Clone
Extracted from live BackhandTL value scanning functionality

Core Value Detection Logic:
- Compare market odds vs AI fair odds
- Calculate edge percentages 
- Classify value opportunities (WATCH/THIN VALUE/GOOD VALUE)
- Filter and rank matches by value
"""

import json
import requests
from datetime import datetime
from typing import List, Dict, Optional
from dataclasses import dataclass

@dataclass
class ValueOpportunity:
    match_id: str
    player1_name: str
    player2_name: str
    tournament: str
    surface: str
    
    # Market data
    market_odds1: float
    market_odds2: float
    
    # AI predictions
    fair_odds1: float
    fair_odds2: float
    
    # Value calculations
    edge1: float
    edge2: float
    best_edge: float
    best_player: str
    best_odds: float
    
    # Classification
    value_tier: str  # GOOD VALUE, THIN VALUE, WATCH, NO VALUE
    value_icon: str
    confidence: float
    
    # Analysis
    analysis_text: str
    game_simulation: Optional[float]

class ValueScannerEngine:
    def __init__(self, supabase_url: str, api_key: str):
        self.supabase_url = supabase_url
        self.api_key = api_key
        
        # Value classification thresholds (extracted from BackhandTL)
        self.value_thresholds = {
            'GOOD_VALUE': 10.0,    # ✨ GOOD VALUE: 10%+ edge
            'THIN_VALUE': 5.0,     # 📈 THIN VALUE: 5-9.9% edge  
            'WATCH': 1.0,          # 👀 WATCH: 1-4.9% edge
            'NO_VALUE': 0.0        # No alert below 1%
        }
        
        # Value icons (extracted from BackhandTL analysis text)
        self.value_icons = {
            'GOOD_VALUE': '✨',
            'THIN_VALUE': '📈', 
            'WATCH': '👀',
            'NO_VALUE': ''
        }
    
    def calculate_edge_percentage(self, market_odds: float, fair_odds: float) -> float:
        """
        Calculate edge percentage: ((market_odds / fair_odds) - 1) * 100
        
        Extracted from BackhandTL algorithms:
        algorithm_1: ai_fair_odds1-1)*100,b=(w/h.ai_fair_odds2-1)*100
        algorithm_2: ai_fair_odds1&&C.ai_fair_odds2&&B>0&&W>0){const K=(B/C.ai_fair_odds1-1)*100
        """
        if not market_odds or not fair_odds or fair_odds <= 0:
            return 0.0
        
        return ((market_odds / fair_odds) - 1) * 100
    
    def classify_value_opportunity(self, edge: float) -> tuple[str, str]:
        """
        Classify value opportunity based on edge percentage
        Returns (tier, icon) based on BackhandTL classification system
        """
        if edge >= self.value_thresholds['GOOD_VALUE']:
            return ('GOOD_VALUE', self.value_icons['GOOD_VALUE'])
        elif edge >= self.value_thresholds['THIN_VALUE']:
            return ('THIN_VALUE', self.value_icons['THIN_VALUE'])
        elif edge >= self.value_thresholds['WATCH']:
            return ('WATCH', self.value_icons['WATCH'])
        else:
            return ('NO_VALUE', self.value_icons['NO_VALUE'])
    
    def fetch_scanner_matches(self) -> List[Dict]:
        """
        Fetch matches visible in scanner from Supabase
        Based on BackhandTL database query: is_visible_in_scanner=true
        """
        headers = {
            'apikey': self.api_key,
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'
        }
        
        # Query matches visible in scanner (extracted from BackhandTL)
        url = f"{self.supabase_url}/rest/v1/market_odds"
        params = {
            'select': 'id,player1_name,player2_name,odds1,odds2,ai_fair_odds1,ai_fair_odds2,ai_analysis_text,tournament,match_time',
            'is_visible_in_scanner': 'eq.true',
            'order': 'created_at.desc',
            'limit': 50
        }
        
        try:
            response = requests.get(url, headers=headers, params=params, timeout=10)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"API Error: {response.status_code}")
                return []
        except Exception as e:
            print(f"Fetch Error: {e}")
            return []
    
    def analyze_match_for_value(self, match: Dict) -> Optional[ValueOpportunity]:
        """
        Analyze a single match for value opportunities
        Implements BackhandTL value detection logic
        """
        try:
            # Extract match data
            player1 = match['player1_name']
            player2 = match['player2_name']
            market_odds1 = float(match['odds1'])
            market_odds2 = float(match['odds2']) 
            fair_odds1 = float(match['ai_fair_odds1']) if match['ai_fair_odds1'] else 0
            fair_odds2 = float(match['ai_fair_odds2']) if match['ai_fair_odds2'] else 0
            
            # Calculate edge for both players
            edge1 = self.calculate_edge_percentage(market_odds1, fair_odds1)
            edge2 = self.calculate_edge_percentage(market_odds2, fair_odds2)
            
            # Find the best value opportunity
            if edge1 > edge2:
                best_edge = edge1
                best_player = player1
                best_odds = market_odds1
            else:
                best_edge = edge2
                best_player = player2
                best_odds = market_odds2
            
            # Classify value opportunity  
            value_tier, value_icon = self.classify_value_opportunity(best_edge)
            
            # Extract game simulation from analysis text (BackhandTL format)
            analysis_text = match.get('ai_analysis_text', '')
            game_simulation = None
            if analysis_text:
                import re
                sim_match = re.search(r'🎲 SIM: ([0-9.]+) Games', analysis_text)
                if sim_match:
                    game_simulation = float(sim_match.group(1))
            
            # Calculate confidence (basic implementation)
            confidence = min(95, max(50, 70 + (best_edge * 2)))
            
            return ValueOpportunity(
                match_id=match['id'],
                player1_name=player1,
                player2_name=player2,
                tournament=match.get('tournament', 'Unknown'),
                surface='hard',  # Would need to extract from tournament data
                market_odds1=market_odds1,
                market_odds2=market_odds2,
                fair_odds1=fair_odds1,
                fair_odds2=fair_odds2,
                edge1=edge1,
                edge2=edge2,
                best_edge=best_edge,
                best_player=best_player,
                best_odds=best_odds,
                value_tier=value_tier,
                value_icon=value_icon,
                confidence=confidence,
                analysis_text=analysis_text,
                game_simulation=game_simulation
            )
            
        except Exception as e:
            print(f"Error analyzing match {match.get('id', 'unknown')}: {e}")
            return None
    
    def scan_for_value(self, min_edge: float = 1.0, max_odds: float = 10.0) -> List[ValueOpportunity]:
        """
        Main value scanning function
        Returns list of value opportunities sorted by edge percentage
        """
        print("🔍 Scanning for value opportunities...")
        
        # Fetch matches from database
        matches = self.fetch_scanner_matches()
        if not matches:
            print("No matches found in scanner")
            return []
        
        # Analyze each match for value
        opportunities = []
        for match in matches:
            value_opp = self.analyze_match_for_value(match)
            if value_opp and value_opp.best_edge >= min_edge and value_opp.best_odds <= max_odds:
                opportunities.append(value_opp)
        
        # Sort by edge percentage (highest first)
        opportunities.sort(key=lambda x: x.best_edge, reverse=True)
        
        print(f"✅ Found {len(opportunities)} value opportunities")
        return opportunities
    
    def generate_value_alert(self, opportunity: ValueOpportunity) -> str:
        """
        Generate value alert text in BackhandTL format
        Examples from extraction:
        [👀 WATCH: Krueger @ 1.74 | Fair: 1.72 | Edge: 1.2%]
        [📈 THIN VALUE: Sanchez Izquierdo @ 2.69 | Fair: 2.5 | Edge: 7.6%]
        [✨ GOOD VALUE: Bouzige @ 5.63 | Fair: 4.96 | Edge: 13.5%]
        """
        if opportunity.value_tier == 'NO_VALUE':
            return ""
        
        tier_text = opportunity.value_tier.replace('_', ' ')
        fair_odds = opportunity.fair_odds1 if opportunity.best_player == opportunity.player1_name else opportunity.fair_odds2
        
        return f"[{opportunity.value_icon} {tier_text}: {opportunity.best_player} @ {opportunity.best_odds} | Fair: {fair_odds} | Edge: {opportunity.best_edge:.1f}%]"
    
    def display_value_opportunities(self, opportunities: List[ValueOpportunity]):
        """
        Display value opportunities in BackhandTL format
        """
        if not opportunities:
            print("❌ No value opportunities found")
            return
        
        print("\n" + "=" * 80)
        print("🎾 730'S LOCKS VALUE SCANNER - LIVE OPPORTUNITIES")
        print("=" * 80)
        
        for i, opp in enumerate(opportunities, 1):
            print(f"\n🎯 VALUE OPPORTUNITY #{i}")
            print(f"🏆 {opp.player1_name} vs {opp.player2_name}")
            print(f"🏟️  {opp.tournament}")
            
            # Value alert
            alert = self.generate_value_alert(opp)
            if alert:
                print(f"💰 {alert}")
            
            print(f"📊 Market Odds: {opp.market_odds1} vs {opp.market_odds2}")
            print(f"🤖 Fair Odds: {opp.fair_odds1} vs {opp.fair_odds2}")
            print(f"📈 Edge: {opp.best_edge:.1f}% ({opp.value_tier.replace('_', ' ')})")
            print(f"🎯 Confidence: {opp.confidence:.0f}%")
            
            if opp.game_simulation:
                print(f"🎲 Game Simulation: {opp.game_simulation} total games")
            
            if opp.analysis_text:
                # Clean analysis text (remove value alerts)
                clean_analysis = opp.analysis_text
                import re
                clean_analysis = re.sub(r'\[.*?\]', '', clean_analysis).strip()
                if clean_analysis:
                    print(f"📝 Analysis: {clean_analysis}")
            
            print("-" * 60)

def main():
    """
    Test the value scanner with BackhandTL connection
    """
    # BackhandTL credentials (extracted)
    supabase_url = "https://suoaznisiowoolxilaju.supabase.co"
    api_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InN1b2F6bmlzaW93b29seGlsYWp1Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjYzNzAwNjIsImV4cCI6MjA4MTczMDA2Mn0.4fh5Unx9Gkd_NPrPnc5O8B6edkipbGnUeAIATHFnyaE"
    
    # Initialize scanner
    scanner = ValueScannerEngine(supabase_url, api_key)
    
    # Scan for value opportunities
    opportunities = scanner.scan_for_value(min_edge=1.0, max_odds=10.0)
    
    # Display results
    scanner.display_value_opportunities(opportunities)

if __name__ == "__main__":
    main()
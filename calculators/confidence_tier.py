"""
ConfidenceTierCalculator - Confidence tier system based on edge.
ELITE/HIGH/MEDIUM/LOW tiers with color-coded badges.
"""


class ConfidenceTierCalculator:
    """Confidence tier based on edge, history, and EV."""
    
    TIERS = {
        'ELITE': {'edge_min': 12.0, 'color': '#00ff41', 'label': 'ELITE'},
        'HIGH': {'edge_min': 10.0, 'color': '#4ade80', 'label': 'HIGH'},
        'MEDIUM': {'edge_min': 8.0, 'color': '#fbbf24', 'label': 'MEDIUM'},
        'LOW': {'edge_min': 3.0, 'color': '#f87171', 'label': 'LOW'}
    }
    
    UNIT_SIZING = {
        'ELITE': 3.0,   # 3 units
        'HIGH': 2.0,    # 2 units
        'MEDIUM': 1.0,  # 1 unit
        'LOW': 0.5      # 0.5 units
    }
    
    @staticmethod
    def get_tier(edge: float) -> str:
        """
        Get confidence tier based on edge.
        
        Args:
            edge: Edge percentage (e.g., 12.5 for 12.5 point edge)
        
        Returns:
            Tier name: 'ELITE', 'HIGH', 'MEDIUM', 'LOW', or 'NONE'
        """
        if edge >= 12.0:
            return 'ELITE'
        elif edge >= 10.0:
            return 'HIGH'
        elif edge >= 8.0:
            return 'MEDIUM'
        elif edge >= 3.0:
            return 'LOW'
        return 'NONE'
    
    @staticmethod
    def get_tier_color(tier: str) -> str:
        """Get display color for tier."""
        return ConfidenceTierCalculator.TIERS.get(tier, {}).get('color', '#94a3b8')
    
    @staticmethod
    def get_unit_size(tier: str) -> float:
        """Get recommended unit size for tier."""
        return ConfidenceTierCalculator.UNIT_SIZING.get(tier, 0.5)
    
    @staticmethod
    def get_tier_details(edge: float) -> dict:
        """
        Get full tier details for display.
        
        Returns:
            dict with tier, color, units, label
        """
        tier = ConfidenceTierCalculator.get_tier(edge)
        
        if tier == 'NONE':
            return {
                'tier': 'NONE',
                'color': '#94a3b8',
                'units': 0,
                'label': 'NO BET'
            }
        
        tier_info = ConfidenceTierCalculator.TIERS[tier]
        return {
            'tier': tier,
            'color': tier_info['color'],
            'units': ConfidenceTierCalculator.UNIT_SIZING[tier],
            'label': tier_info['label']
        }
    
    @staticmethod
    def get_star_rating(edge: float) -> int:
        """
        Get 5-star rating based on edge.
        5★ = ELITE, 4★ = HIGH, 3★ = MEDIUM, 2★ = LOW, 1★ = minimal
        """
        if edge >= 12.0:
            return 5
        elif edge >= 10.0:
            return 4
        elif edge >= 8.0:
            return 3
        elif edge >= 5.0:
            return 2
        elif edge >= 3.0:
            return 1
        return 0

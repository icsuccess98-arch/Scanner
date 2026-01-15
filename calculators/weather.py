"""
WeatherCalculator - Weather impact for outdoor football games.
Cold, wind, and precipitation all suppress scoring (UNDER lean).
"""


class WeatherCalculator:
    """Weather impact for NFL/CFB outdoor games."""
    
    DOME_STADIUMS = {
        'Cardinals', 'Falcons', 'Cowboys', 'Lions', 'Texans',
        'Colts', 'Raiders', 'Saints', 'Vikings', 'Rams'
    }
    
    @staticmethod
    def calculate_weather_impact(temp: float, wind: float, precip: str, is_dome: bool) -> float:
        """
        Calculate total weather impact on scoring.
        
        Args:
            temp: Temperature in Fahrenheit
            wind: Wind speed in MPH
            precip: Precipitation description (rain, snow, etc.)
            is_dome: Whether game is in dome stadium
        
        Returns:
            Points adjustment (negative = scoring suppression)
        """
        if is_dome:
            return 0.0
        
        impact = 0.0
        
        if temp is not None:
            if temp < 20:
                impact -= 3.0  # Extreme cold
            elif temp < 32:
                impact -= 1.5  # Freezing
            elif temp > 85:
                impact -= 0.5  # Extreme heat fatigue
        
        if wind is not None:
            if wind >= 20:
                impact -= 4.0  # Severe wind (passing game killed)
            elif wind >= 15:
                impact -= 2.0  # Heavy wind
            elif wind >= 10:
                impact -= 1.0  # Moderate wind
        
        if precip:
            precip_lower = precip.lower()
            if 'snow' in precip_lower:
                impact -= 3.0  # Snow game
            elif 'rain' in precip_lower or 'storm' in precip_lower:
                impact -= 2.0  # Rain/storm
        
        return round(impact, 1)
    
    @staticmethod
    def get_weather_grade(temp: float, wind: float, precip: str) -> str:
        """
        Get weather grade for game (A-F).
        A = Perfect, F = Unplayable conditions
        """
        impact = WeatherCalculator.calculate_weather_impact(temp, wind, precip, False)
        
        if impact >= -0.5:
            return 'A'
        elif impact >= -2.0:
            return 'B'
        elif impact >= -4.0:
            return 'C'
        elif impact >= -6.0:
            return 'D'
        else:
            return 'F'
    
    @staticmethod
    def is_weather_game(temp: float, wind: float, precip: str) -> bool:
        """
        Check if weather is significant factor.
        Used to flag games for special consideration.
        """
        impact = WeatherCalculator.calculate_weather_impact(temp, wind, precip, False)
        return abs(impact) >= 3.0

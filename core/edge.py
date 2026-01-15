class SharpThresholds:
    MIN_TRUE_EDGE = {'NBA': 3.5, 'CBB': 4.0, 'NFL': 2.0, 'CFB': 2.5, 'NHL': 0.3}
    MIN_EV = 0.0
    MAX_VIG = 6.0


class SharpEdgeCalculator:
    @staticmethod
    def calculate_vig(over_odds: int, under_odds: int) -> dict:
        def implied_prob(odds: int) -> float:
            if odds > 0:
                return 100 / (odds + 100)
            return abs(odds) / (abs(odds) + 100)

        over_prob = implied_prob(over_odds)
        under_prob = implied_prob(under_odds)
        total_prob = over_prob + under_prob
        vig = (total_prob - 1) * 100

        over_fair = over_prob / total_prob
        under_fair = under_prob / total_prob

        shade = 'BALANCED'
        if over_fair > under_fair + 0.03:
            shade = 'OVER'
        elif under_fair > over_fair + 0.03:
            shade = 'UNDER'

        return {
            'vig_percentage': round(vig, 2),
            'over_implied': round(over_prob * 100, 1),
            'under_implied': round(under_prob * 100, 1),
            'over_fair': round(over_fair * 100, 1),
            'under_fair': round(under_fair * 100, 1),
            'shade': shade
        }

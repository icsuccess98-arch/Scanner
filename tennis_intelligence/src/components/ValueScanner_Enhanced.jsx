// ValueScanner - BackhandTL Complete Clone
// Enhanced with actual value scanning functionality extracted from live data
import React, { useState, useEffect } from 'react';

const ValueScanner = () => {
    const [matches, setMatches] = useState([]);
    const [loading, setLoading] = useState(true);
    const [filters, setFilters] = useState({
        minEdge: 5.0,
        maxOdds: 10.0,
        surface: 'all'
    });

    // Extracted value detection logic from BackhandTL
    const calculateEdge = (marketOdds, fairOdds) => {
        if (!marketOdds || !fairOdds) return 0;
        return ((marketOdds / fairOdds) - 1) * 100;
    };

    const getValueIndicator = (edge) => {
        if (edge >= 10) return { icon: '✨', label: 'GOOD VALUE', color: '#00FF00' };
        if (edge >= 5) return { icon: '📈', label: 'THIN VALUE', color: '#FFA500' };
        if (edge >= 1) return { icon: '👀', label: 'WATCH', color: '#FFFF00' };
        return null;
    };

    const parseAnalysisText = (text) => {
        if (!text) return { summary: '', valueAlert: '', simulation: '' };
        
        const valueMatch = text.match(/\[(👀|📈|✨)\s*(WATCH|THIN VALUE|GOOD VALUE):[^\]]+\]/);
        const simMatch = text.match(/\[🎲 SIM: ([0-9.]+) Games\]/);
        
        const summary = text.replace(/\[.*?\]/g, '').trim();
        const valueAlert = valueMatch ? valueMatch[0] : '';
        const simulation = simMatch ? simMatch[1] : '';
        
        return { summary, valueAlert, simulation };
    };

    // Mock data based on actual BackhandTL structure
    useEffect(() => {
        // TODO: Replace with actual API call to your Supabase
        const mockMatches = [
            {
                id: 1,
                player1_name: "Krueger",
                player2_name: "Bejlek", 
                odds1: 1.73,
                odds2: 2.07,
                ai_fair_odds1: 1.72,
                ai_fair_odds2: 2.39,
                ai_analysis_text: "Krueger's aggressive baseliner style and peak form on a medium-fast surface favor her, but Bejlek's counter-punching ability may neutralize Krueger's pace. [👀 WATCH: Krueger @ 1.74 | Fair: 1.72 | Edge: 1.2%] [🎲 SIM: 24.4 Games]",
                tournament: "Abu Dhabi WTA",
                surface: "hard"
            },
            {
                id: 2,
                player1_name: "Sanchez Izquierdo",
                player2_name: "Collarini",
                odds1: 2.69,
                odds2: 1.42,
                ai_fair_odds1: 2.5,
                ai_fair_odds2: 1.67,
                ai_analysis_text: "Sanchez Izquierdo's counter-punching style and clay-court expertise should give him an edge on this slow surface, but Collarini's average form and fresh bio-load make him a formidable opponent. [📈 THIN VALUE: Sanchez Izquierdo @ 2.69 | Fair: 2.5 | Edge: 7.6%] [🎲 SIM: 23.7 Games]",
                tournament: "Rosario",
                surface: "clay"
            },
            {
                id: 3,
                player1_name: "Schoolkate",
                player2_name: "Bouzige",
                odds1: 1.13,
                odds2: 5.63,
                ai_fair_odds1: 1.25,
                ai_fair_odds2: 4.96,
                ai_analysis_text: "Schoolkate's serve and volley style may struggle with Bouzige's aggressive baselining on fast hard courts, but her fresh bio-load and high BSI favor her. [✨ GOOD VALUE: Bouzige @ 5.63 | Fair: 4.96 | Edge: 13.5%] [🎲 SIM: 24.1 Games]",
                tournament: "Brisbane",
                surface: "hard"
            }
        ];
        
        setMatches(mockMatches);
        setLoading(false);
    }, []);

    const getValueOpportunities = () => {
        return matches.map(match => {
            const edge1 = calculateEdge(match.odds1, match.ai_fair_odds1);
            const edge2 = calculateEdge(match.odds2, match.ai_fair_odds2);
            
            const bestEdge = Math.max(edge1, edge2);
            const bestPlayer = edge1 > edge2 ? match.player1_name : match.player2_name;
            const bestOdds = edge1 > edge2 ? match.odds1 : match.odds2;
            const bestFairOdds = edge1 > edge2 ? match.ai_fair_odds1 : match.ai_fair_odds2;
            
            const valueIndicator = getValueIndicator(bestEdge);
            const analysis = parseAnalysisText(match.ai_analysis_text);
            
            return {
                ...match,
                bestEdge,
                bestPlayer,
                bestOdds,
                bestFairOdds,
                valueIndicator,
                analysis
            };
        }).filter(match => 
            match.bestEdge >= filters.minEdge && 
            match.bestOdds <= filters.maxOdds &&
            (filters.surface === 'all' || match.surface === filters.surface)
        ).sort((a, b) => b.bestEdge - a.bestEdge);
    };

    const valueOpportunities = getValueOpportunities();

    if (loading) {
        return (
            <div className="value-scanner-loading">
                <div className="loading-spinner">
                    🔍 Scanning for value opportunities...
                </div>
            </div>
        );
    }

    return (
        <div className="value-scanner-container">
            <header className="scanner-header">
                <h1>🔍 Value Scanner</h1>
                <p>AI-powered tennis value detection • Live market analysis</p>
            </header>

            <div className="scanner-filters">
                <div className="filter-group">
                    <label>Minimum Edge</label>
                    <select value={filters.minEdge} onChange={e => setFilters({...filters, minEdge: parseFloat(e.target.value)})}>
                        <option value="1">1%+</option>
                        <option value="5">5%+</option>
                        <option value="10">10%+</option>
                    </select>
                </div>
                
                <div className="filter-group">
                    <label>Max Odds</label>
                    <select value={filters.maxOdds} onChange={e => setFilters({...filters, maxOdds: parseFloat(e.target.value)})}>
                        <option value="3">3.00</option>
                        <option value="5">5.00</option>
                        <option value="10">10.00</option>
                    </select>
                </div>
                
                <div className="filter-group">
                    <label>Surface</label>
                    <select value={filters.surface} onChange={e => setFilters({...filters, surface: e.target.value})}>
                        <option value="all">All</option>
                        <option value="hard">Hard</option>
                        <option value="clay">Clay</option>
                        <option value="grass">Grass</option>
                    </select>
                </div>
            </div>

            <div className="value-opportunities">
                {valueOpportunities.length === 0 ? (
                    <div className="no-value">
                        <h3>No value opportunities found</h3>
                        <p>Try adjusting your filters or check back later</p>
                    </div>
                ) : (
                    valueOpportunities.map(match => (
                        <div key={match.id} className="value-match">
                            <div className="match-header">
                                <div className="match-title">
                                    <strong>{match.player1_name}</strong> vs <strong>{match.player2_name}</strong>
                                </div>
                                <div className="tournament-info">
                                    {match.tournament} • {match.surface}
                                </div>
                            </div>
                            
                            {match.valueIndicator && (
                                <div className="value-alert" style={{color: match.valueIndicator.color}}>
                                    {match.valueIndicator.icon} {match.valueIndicator.label}
                                </div>
                            )}
                            
                            <div className="value-details">
                                <div className="best-bet">
                                    <span className="player">{match.bestPlayer}</span>
                                    <span className="odds">@ {match.bestOdds}</span>
                                    <span className="fair">Fair: {match.bestFairOdds}</span>
                                    <span className="edge">Edge: {match.bestEdge.toFixed(1)}%</span>
                                </div>
                            </div>
                            
                            {match.analysis.summary && (
                                <div className="match-analysis">
                                    <p>{match.analysis.summary}</p>
                                    {match.analysis.simulation && (
                                        <div className="simulation">
                                            🎲 Game Simulation: {match.analysis.simulation} total games
                                        </div>
                                    )}
                                </div>
                            )}
                        </div>
                    ))
                )}
            </div>

            <div className="scanner-footer">
                <p>Value opportunities update every 5 minutes • Based on AI fair odds calculations</p>
            </div>
        </div>
    );
};

export default ValueScanner;
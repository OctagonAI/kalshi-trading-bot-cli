export interface BacktestOpts {
  resolvedOnly: boolean;
  unresolvedOnly: boolean;
  from?: string;          // ISO date
  to?: string;            // ISO date
  category?: string;
  minHoursBeforeClose: number;  // default 24
  minEdge: number;              // default 0.05 (5pp)
  exportPath?: string;
}

export interface BacktestSnapshot {
  ticker: string;
  event_ticker: string;
  model_prob: number;       // 0-1
  market_prob: number;      // 0-1
  edge_pp: number;          // percentage points
  hours_before_close: number;
  confidence_score: number;
  series_category: string;
}

export interface ResolvedMarket extends BacktestSnapshot {
  outcome: 0 | 1;          // YES=1, NO=0
  close_time: string;       // ISO 8601
}

export interface UnresolvedEdge {
  ticker: string;
  event_ticker: string;
  model_prob: number;
  market_prob: number;
  edge_pp: number;
  direction: 'YES' | 'NO';
  confidence_score: number;
  closes_at: string;
  series_category: string;
}

export interface ResolvedResult {
  verdict: { summary: string; significant: boolean; profitable: boolean };
  brier_octagon: number;
  brier_market: number;
  skill_score: number;
  skill_ci: [number, number];
  edge_signals: number;
  edge_hit_rate: number;
  hit_rate_ci: [number, number];
  flat_bet_pnl: number;
  flat_bet_roi: number;
  markets_evaluated: number;
  events_evaluated: number;
  coverage: number;       // fraction of settled markets with Octagon history
  markets: ResolvedMarket[];  // per-market detail for CSV export
}

export interface UnresolvedResult {
  edges: UnresolvedEdge[];
  total_open_with_coverage: number;
  total_open: number;
}

export interface BacktestResult {
  resolved: ResolvedResult | null;
  unresolved: UnresolvedResult | null;
  date_range: { from: string; to: string };
}

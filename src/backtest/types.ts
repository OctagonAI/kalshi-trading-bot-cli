export interface BacktestOpts {
  days: number;               // lookback period in days (default 30)
  resolvedOnly: boolean;
  unresolvedOnly: boolean;
  category?: string;
  minEdge: number;            // fractional (0-1 scale), converted to pp by caller (e.g., 0.005 → 0.5pp)
  exportPath?: string;
  /** Where the universe is sourced from. Default 'api'. */
  universe?: 'api' | 'local';
  /** Fee model for net P&L. Default 'none' — output is gross. */
  fees?: 'none' | 'taker' | 'maker';
}

/** A single scored market signal — unified type for both resolved and unresolved. */
export interface ScoredSignal {
  event_ticker: string;
  market_ticker: string;
  series_category: string;
  model_prob: number;         // 0-100 (Octagon model % from N days ago)
  market_then: number;        // 0-100 (Kalshi trading price N days ago, from Octagon snapshot)
  market_now: number;         // 0-100 (settlement for resolved, current price for unresolved)
  resolved: boolean;
  /**
   * Raw, unrounded edge in percentage points: model_prob − market_then.
   * Filtering on |edge| should always use this value; display layers
   * round to 0.1pp or 1pp as appropriate.
   */
  edge_pp: number;
  pnl: number;               // computed P&L for this signal ($ per $1 face value)
  capital: number;           // $ capital deployed per $1 face value: kp/100 for YES edges, (100-kp)/100 for NO edges
  edge_bucket: string;        // absolute-edge bucket label e.g. "0-5%", "5-10%", ..., "90%+"
  confidence_score: number;
  close_time: string;
}

/**
 * Per-leg scorecard: realized P&L on the resolved leg, mark-to-market on the
 * unresolved leg. Computed on the leg's subset of signals.
 */
export interface LegMetrics {
  edge_signals: number;
  edge_hit_rate: number;
  hit_rate_ci: [number, number];
  flat_bet_pnl: number;
  flat_bet_roi: number;
  total_capital: number;
}

export interface BacktestResult {
  verdict: { summary: string; significant: boolean; profitable: boolean };
  days: number;
  events_scored: number;
  markets_resolved: number;
  markets_unresolved: number;
  brier_octagon: number;
  brier_market: number;
  skill_score: number;
  skill_ci: [number, number];
  edge_signals: number;
  edge_hit_rate: number;
  hit_rate_ci: [number, number];
  flat_bet_pnl: number;
  flat_bet_roi: number;       // capital-weighted: sum(pnl) / sum(capital) across edge signals
  total_capital: number;      // sum of capital across edge signals (ROI denominator)
  signals: ScoredSignal[];
  /**
   * Count of candidate signals dropped because the Octagon snapshot had no
   * per-contract volume (older snapshots predate the per-contract field).
   * We deliberately do NOT fall back to Kalshi lifetime volume — that
   * would be a look-ahead bias (lifetime includes post-entry trading).
   * Surfaced so users can see the coverage cost of the strict gate.
   */
  signals_dropped_no_volume: number;
  /**
   * Provenance for the universe — printed in the scorecard header so users
   * (and downstream JSON consumers) can see whether the backtest ran over
   * the systematic Octagon-API universe or the legacy local-DB universe.
   */
  universe_source: 'api' | 'local';
  universe_size: number;
  universe_description: string;
  /**
   * Fee model applied to the P&L. 'none' means the reported P&L is gross
   * (no fees, no spreads). 'taker' charges the Kalshi taker fee per entry.
   * 'maker' assumes free entry. Default 'none' so existing output is
   * unchanged — opt in with --fees taker.
   */
  fee_model: 'none' | 'taker' | 'maker';
  /** P&L net of fees when fee_model != 'none', else equal to flat_bet_pnl. */
  flat_bet_pnl_net: number;
  flat_bet_roi_net: number;
  /**
   * Sub-scorecards computed on the resolved and unresolved legs separately.
   * Resolved settles at 0/100 — realized outcomes. Unresolved is marked to
   * an arbitrary "now" price and may reverse before settlement. Blending
   * them in the top-level fields can hide cases where the paper P&L
   * inflates a weak realized result.
   *
   * The blended top-level fields (`edge_hit_rate`, `flat_bet_roi`, etc.)
   * are kept for backward compatibility with existing consumers.
   */
  resolved_metrics: LegMetrics;
  unresolved_metrics: LegMetrics;
  /**
   * Zero-skill baseline ROIs on the same post-filter universe. Always-NO is
   * the relevant null because Kalshi's universe is structurally NO-heavy:
   * multi-outcome events have one YES and many NOs. A model that consistently
   * beats always-NO has selection skill; one that doesn't is mostly
   * harvesting the favorite-longshot tilt.
   */
  baselines: {
    always_no_roi: number;
    always_no_hit_rate: number;
    always_yes_roi: number;
    always_yes_hit_rate: number;
    /**
     * Model NO-bet ROI minus always-NO ROI, computed in entry-price bands
     * (5-20, 20-40, 40-60, 60-80, 80-95) and capital-weighted across bands.
     * This is the honest "within-band skill" delta: it controls for both
     * the structural NO tilt AND the entry-price mix.
     */
    within_band_skill_pp: number;
    /**
     * Per-band breakdown so users can see where the skill (if any) comes from.
     */
    within_band_breakdown: Array<{
      band: string;            // e.g. "20-40¢"
      model_no_roi: number;    // model NO-bet ROI in this band
      always_no_roi: number;   // always-NO ROI in this band
      skill_delta_pp: number;  // (model - baseline) × 100, percentage points
      n_model: number;         // count of model NO bets in this band
      n_universe: number;      // count of all-NO universe contracts in this band
    }>;
  };
  subscription_notice?: string;
}

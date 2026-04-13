import { computeBrier } from '../eval/brier.js';
import type { ResolvedMarket, ResolvedResult } from './types.js';

/**
 * Skill score: how much better Octagon is vs the market as a forecaster.
 * Positive = model beats market. Negative = market is better.
 */
export function computeSkillScore(brierOctagon: number, brierMarket: number): number {
  if (brierMarket === 0) return 0;
  return 1 - (brierOctagon / brierMarket);
}

/**
 * Bootstrap confidence interval for a statistic.
 * Resamples `data` with replacement `iterations` times, computes `statFn` on each sample.
 * Returns [lower, upper] at the given confidence level (default 95%).
 */
export function bootstrapCI(
  data: number[],
  statFn: (sample: number[]) => number,
  iterations = 10_000,
  alpha = 0.05,
): [number, number] {
  if (data.length === 0) return [0, 0];

  const stats: number[] = [];
  for (let i = 0; i < iterations; i++) {
    const sample: number[] = [];
    for (let j = 0; j < data.length; j++) {
      sample.push(data[Math.floor(Math.random() * data.length)]);
    }
    stats.push(statFn(sample));
  }
  stats.sort((a, b) => a - b);

  const lo = Math.floor((alpha / 2) * stats.length);
  const hi = Math.floor((1 - alpha / 2) * stats.length);
  return [stats[lo], stats[hi]];
}

/**
 * Compute all resolved-market metrics from a list of resolved markets.
 * Each market must have model_prob, market_prob, outcome, edge_pp.
 */
export function computeResolvedMetrics(markets: ResolvedMarket[], minEdgePp = 5): ResolvedResult {
  const n = markets.length;
  if (n === 0) {
    return {
      verdict: { summary: 'No resolved markets with Octagon coverage found.', significant: false, profitable: false },
      brier_octagon: 0,
      brier_market: 0,
      skill_score: 0,
      skill_ci: [0, 0],
      edge_signals: 0,
      edge_hit_rate: 0,
      hit_rate_ci: [0, 0],
      flat_bet_pnl: 0,
      flat_bet_roi: 0,
      markets_evaluated: 0,
      events_evaluated: 0,
      coverage: 0,
      markets: [],
    };
  }

  // Brier scores
  const brierOctagonScores = markets.map(m => computeBrier(m.model_prob, m.outcome));
  const brierMarketScores = markets.map(m => computeBrier(m.market_prob, m.outcome));
  const brierOctagon = brierOctagonScores.reduce((a, b) => a + b, 0) / n;
  const brierMarket = brierMarketScores.reduce((a, b) => a + b, 0) / n;

  // Skill score with bootstrap CI
  const skillScore = computeSkillScore(brierOctagon, brierMarket);

  // Bootstrap skill: resample paired (octagon_brier, market_brier), compute skill on each
  const pairedDiffs = markets.map((_, i) => brierMarketScores[i] - brierOctagonScores[i]);
  const skillCI = bootstrapCI(pairedDiffs, (sample) => {
    const avgDiff = sample.reduce((a, b) => a + b, 0) / sample.length;
    const avgMarket = brierMarket; // approximate
    return avgMarket === 0 ? 0 : avgDiff / avgMarket;
  });

  // Edge signals: where |edge| >= minEdgePp
  const edgeSignals = markets.filter(m => Math.abs(m.edge_pp) >= minEdgePp);
  const edgeCount = edgeSignals.length;

  // Hit rate: edge direction was correct
  const hits = edgeSignals.filter(m => {
    if (m.edge_pp > 0) return m.outcome === 1; // model said YES more likely, YES happened
    return m.outcome === 0; // model said NO more likely, NO happened
  });
  const hitRate = edgeCount > 0 ? hits.length / edgeCount : 0;

  // Bootstrap hit rate CI
  const hitRateData = edgeSignals.map(m => {
    if (m.edge_pp > 0) return m.outcome === 1 ? 1 : 0;
    return m.outcome === 0 ? 1 : 0;
  });
  const hitRateCI = bootstrapCI(hitRateData, (sample) => {
    return sample.reduce((a, b) => a + b, 0) / sample.length;
  });

  // Flat-bet P&L: $1 per edge signal
  let pnl = 0;
  for (const m of edgeSignals) {
    const p = m.market_prob;
    if (m.edge_pp > 0) {
      // BUY YES at price P
      pnl += m.outcome === 1 ? (1 - p) : -p;
    } else {
      // BUY NO at price P
      pnl += m.outcome === 0 ? p : -(1 - p);
    }
  }
  const roi = edgeCount > 0 ? pnl / edgeCount : 0;

  // Unique events
  const uniqueEvents = new Set(markets.map(m => m.event_ticker));

  // Verdict
  const significant = skillCI[0] > 0; // CI excludes zero
  const profitable = pnl > 0;
  let summary: string;
  if (skillScore > 0.05 && significant && profitable) {
    summary = `Model shows edge (Skill +${(skillScore * 100).toFixed(1)}% [CI: +${(skillCI[0] * 100).toFixed(1)}%, +${(skillCI[1] * 100).toFixed(1)}%]; ROI +${(roi * 100).toFixed(1)}%)`;
  } else if (skillScore > 0 && !significant) {
    summary = `Inconclusive — need more data (Skill +${(skillScore * 100).toFixed(1)}%, CI includes zero)`;
  } else {
    summary = `No edge detected (Skill ${(skillScore * 100).toFixed(1)}%)`;
  }

  return {
    verdict: { summary, significant, profitable },
    brier_octagon: brierOctagon,
    brier_market: brierMarket,
    skill_score: skillScore,
    skill_ci: skillCI,
    edge_signals: edgeCount,
    edge_hit_rate: hitRate,
    hit_rate_ci: hitRateCI,
    flat_bet_pnl: pnl,
    flat_bet_roi: roi,
    markets_evaluated: n,
    events_evaluated: uniqueEvents.size,
    coverage: 0, // filled in by caller
    markets,
  };
}

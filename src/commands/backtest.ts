import type { ParsedArgs } from './parse-args.js';
import type { CLIResponse } from './json.js';
import { wrapSuccess } from './json.js';
import { getDb } from '../db/index.js';
import { discoverSettledMarkets, discoverOpenMarkets, parallelMap } from '../backtest/discovery.js';
import { fetchAndCacheHistory, selectSnapshotByDate, SubscriptionRequiredError, type OutcomeProbability } from '../backtest/fetcher.js';
import { computeMetrics } from '../backtest/metrics.js';
import type { BacktestResult, ScoredSignal } from '../backtest/types.js';
import { formatBacktestHuman, exportCSV, type FormatOpts } from '../backtest/renderer.js';

/** Look up the per-contract outcome entry from outcome_probabilities array. */
function findOutcomeProb(
  outcomes: OutcomeProbability[] | null | undefined,
  marketTicker: string,
): OutcomeProbability | null {
  if (!outcomes || !Array.isArray(outcomes)) return null;
  const match = outcomes.find(
    o => o.market_ticker.toUpperCase() === marketTicker.toUpperCase(),
  );
  return match ?? null;
}

/** Absolute-edge bucket label matching the Supabase-methodology buckets. */
function edgeBucketLabel(edgePp: number): string {
  const abs = Math.abs(edgePp);
  if (abs < 5) return '0-5%';
  if (abs < 10) return '5-10%';
  if (abs < 20) return '10-20%';
  if (abs < 30) return '20-30%';
  if (abs < 40) return '30-40%';
  if (abs < 50) return '40-50%';
  if (abs < 60) return '50-60%';
  if (abs < 70) return '60-70%';
  if (abs < 80) return '70-80%';
  if (abs < 90) return '80-90%';
  return '90%+';
}

/**
 * Return the tradeable volume for a contract.
 * Prefers per-contract volume fields from the Octagon snapshot (as the
 * Supabase methodology does); falls back to Kalshi lifetime volume for
 * older cached snapshots that pre-date the API's per-contract volume.
 */
function contractVolume(
  perContract: OutcomeProbability | null,
  fallbackLifetimeVolume: number,
): number {
  if (perContract) {
    const v = typeof perContract.volume === 'number' ? perContract.volume : null;
    const v24 = typeof perContract.volume_24h === 'number' ? perContract.volume_24h : null;
    if (v !== null || v24 !== null) return Math.max(v ?? 0, v24 ?? 0);
  }
  return fallbackLifetimeVolume;
}

export { formatBacktestHuman };
export type { FormatOpts };

export async function handleBacktest(args: ParsedArgs): Promise<CLIResponse<BacktestResult>> {
  const db = getDb();
  const days = args.days ?? 15;
  const maxAgeDays = args.maxAge ?? days;
  // Default 0.5pp matches the Supabase reference methodology — enough to
  // skip near-zero-edge noise without excluding the 0-5% bucket.
  const minEdge = args.minEdge ?? 0.005;
  const minEdgePp = minEdge * 100;
  const minVolume = args.minVolume ?? 1;
  const minPrice = args.minPrice ?? 5;   // 0-100 scale
  const maxPrice = args.maxPrice ?? 95;  // 0-100 scale
  const now = new Date();
  const lookbackDate = new Date(now.getTime() - days * 24 * 60 * 60 * 1000);
  const minPredictionDate = new Date(lookbackDate.getTime() - maxAgeDays * 24 * 60 * 60 * 1000);

  const signals: ScoredSignal[] = [];
  let subscriptionNotice: string | undefined;

  // ─── RESOLVED: settled markets with historical Octagon snapshots ────────
  if (!args.unresolved) {
    try {
      const settled = await discoverSettledMarkets(db, { category: args.category });

      if (settled.length > 0) {
        // Group by event_ticker to batch history fetches
        const byEvent = new Map<string, typeof settled>();
        for (const m of settled) {
          const arr = byEvent.get(m.event_ticker) ?? [];
          arr.push(m);
          byEvent.set(m.event_ticker, arr);
        }

        // Fetch & score events concurrently — Octagon history is I/O-bound and
        // processing them serially made cold-cache backtests take 15+ min.
        const perEvent = await parallelMap([...byEvent.entries()], async ([eventTicker, markets]) => {
          let snapshots;
          try {
            snapshots = await fetchAndCacheHistory(db, eventTicker, { maxAgeDays });
          } catch (err) {
            if (err instanceof SubscriptionRequiredError) throw err;
            return [] as ScoredSignal[];
          }

          // Find the snapshot closest to N days ago, rejecting snapshots
          // older than the prediction-age window so we don't score stale
          // model outputs as if they were recent.
          const snap = selectSnapshotByDate(snapshots, lookbackDate, minPredictionDate);
          if (!snap) return [];

          const out: ScoredSignal[] = [];
          for (const m of markets) {
            // Strict per-contract extraction — no event-level fallback.
            const perMarket = findOutcomeProb(snap.outcome_probabilities, m.ticker);
            if (!perMarket) continue;
            const modelProb = perMarket.model_probability;
            const marketThen = perMarket.market_probability;
            if (!Number.isFinite(modelProb) || !Number.isFinite(marketThen)) continue;
            const marketNow = m.result === 'yes' ? 100 : 0;
            const edgePp = Math.round((modelProb - marketThen) * 10) / 10;

            // Tradeable filter — per-contract volume from the Octagon snapshot
            // (matches Supabase methodology); falls back to Kalshi lifetime
            // volume for pre-API-change cached snapshots.
            const vol = contractVolume(perMarket, m.volume);
            if (vol < minVolume) continue;
            // Price is marketThen (the price you'd transact at for a resolved bet).
            if (marketThen < minPrice || marketThen > maxPrice) continue;

            // P&L and capital per $1 face value.
            let pnl = 0;
            let capital = 0;
            if (edgePp > 0) {
              // Buy YES at marketThen, settles at marketNow
              pnl = (marketNow - marketThen) / 100;
              capital = marketThen / 100;
            } else if (edgePp < 0) {
              // Buy NO at (100 - marketThen), settles at (100 - marketNow)
              pnl = (marketThen - marketNow) / 100;
              capital = (100 - marketThen) / 100;
            } else {
              // Zero edge: capital still reflects the tradeable side implied by sign
              // (use YES side so divide-by-zero checks don't fire on 0-edge signals).
              capital = marketThen / 100;
            }
            if (capital <= 0) continue;

            out.push({
              event_ticker: m.event_ticker,
              market_ticker: m.ticker,
              series_category: m.series_category,
              model_prob: modelProb,
              market_then: marketThen,
              market_now: marketNow,
              resolved: true,
              edge_pp: edgePp,
              pnl: Math.round(pnl * 10000) / 10000,
              capital: Math.round(capital * 10000) / 10000,
              edge_bucket: edgeBucketLabel(edgePp),
              confidence_score: snap.confidence_score ?? 0,
              close_time: m.close_time,
            });
          }
          return out;
        }, 10);
        for (const arr of perEvent) signals.push(...arr);
      }
    } catch (err) {
      if (err instanceof SubscriptionRequiredError) {
        subscriptionNotice = err.message;
      } else {
        throw err;
      }
    }
  }

  // ─── UNRESOLVED: open markets with current Kalshi prices ───────────────
  if (!args.resolved) {
    try {
      const openMarkets = await discoverOpenMarkets(db, { category: args.category });

      // Group by event_ticker to batch history fetches (same as resolved path).
      const openByEvent = new Map<string, typeof openMarkets>();
      for (const m of openMarkets) {
        const arr = openByEvent.get(m.event_ticker) ?? [];
        arr.push(m);
        openByEvent.set(m.event_ticker, arr);
      }

      const perEvent = await parallelMap([...openByEvent.entries()], async ([eventTicker, markets]) => {
        let snapshots;
        try {
          snapshots = await fetchAndCacheHistory(db, eventTicker, { maxAgeDays });
        } catch (err) {
          if (err instanceof SubscriptionRequiredError) throw err;
          return [] as ScoredSignal[];
        }
        const snap = selectSnapshotByDate(snapshots, lookbackDate, minPredictionDate);
        if (!snap) return [];

        const out: ScoredSignal[] = [];
        for (const m of markets) {
          // Strict per-contract extraction — no event-level fallback.
          const perMarket = findOutcomeProb(snap.outcome_probabilities, m.ticker);
          if (!perMarket) continue;
          const modelProb = perMarket.model_probability;
          const marketThen = perMarket.market_probability;
          if (!Number.isFinite(modelProb) || !Number.isFinite(marketThen)) continue;
          const confidenceScore = snap.confidence_score ?? 0;

          const marketNow = m.market_prob * 100; // current Kalshi price (0-100)
          const edgePp = Math.round((modelProb - marketThen) * 10) / 10;

          // Tradeable filter — per-contract volume from the Octagon snapshot.
          const vol = contractVolume(perMarket, m.volume);
          if (vol < minVolume) continue;
          // Price is marketNow (the current transactable price for an open position).
          if (marketNow < minPrice || marketNow > maxPrice) continue;

          // M2M P&L and capital per $1 face value.
          let pnl = 0;
          let capital = 0;
          if (edgePp > 0) {
            pnl = (marketNow - marketThen) / 100;
            capital = marketThen / 100;
          } else if (edgePp < 0) {
            pnl = (marketThen - marketNow) / 100;
            capital = (100 - marketThen) / 100;
          } else {
            capital = marketThen / 100;
          }
          if (capital <= 0) continue;

          out.push({
            event_ticker: m.event_ticker,
            market_ticker: m.ticker,
            series_category: m.series_category,
            model_prob: modelProb,
            market_then: marketThen,
            market_now: marketNow,
            resolved: false,
            edge_pp: edgePp,
            pnl: Math.round(pnl * 10000) / 10000,
            capital: Math.round(capital * 10000) / 10000,
            edge_bucket: edgeBucketLabel(edgePp),
            confidence_score: confidenceScore,
            close_time: m.close_time,
          });
        }
        return out;
      }, 10);
      for (const arr of perEvent) signals.push(...arr);
    } catch (err) {
      // Mirror the resolved block: a subscription wall hit while scoring open
      // markets becomes a notice rather than crashing out before CSV export.
      if (err instanceof SubscriptionRequiredError) {
        subscriptionNotice = subscriptionNotice ?? err.message;
      } else {
        throw err;
      }
    }
  }

  // ─── COMPUTE METRICS ───────────────────────────────────────────────────
  const metrics = computeMetrics(signals, minEdgePp);

  const result: BacktestResult = {
    ...metrics,
    days,
    subscription_notice: subscriptionNotice,
  };

  if (args.exportPath) {
    exportCSV(result, args.exportPath);
  }

  return wrapSuccess('backtest', result);
}

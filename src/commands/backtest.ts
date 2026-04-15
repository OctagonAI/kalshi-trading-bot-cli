import type { ParsedArgs } from './parse-args.js';
import type { CLIResponse } from './json.js';
import { wrapSuccess } from './json.js';
import { getDb } from '../db/index.js';
import { discoverSettledMarkets, discoverOpenMarkets } from '../backtest/discovery.js';
import { fetchAndCacheHistory, selectSnapshotByDate, SubscriptionRequiredError, type OutcomeProbability } from '../backtest/fetcher.js';
import { computeMetrics } from '../backtest/metrics.js';
import type { BacktestResult, ScoredSignal } from '../backtest/types.js';
import { formatBacktestHuman, exportCSV, type FormatOpts } from '../backtest/renderer.js';

/** Look up per-market model/market probability from outcome_probabilities array (0-100 scale). */
function findOutcomeProb(
  outcomes: OutcomeProbability[] | null | undefined,
  marketTicker: string,
): { modelProb: number; marketProb: number } | null {
  if (!outcomes || !Array.isArray(outcomes)) return null;
  const match = outcomes.find(
    o => o.market_ticker.toUpperCase() === marketTicker.toUpperCase(),
  );
  if (!match) return null;
  return { modelProb: match.model_probability, marketProb: match.market_probability };
}

export { formatBacktestHuman };
export type { FormatOpts };

export async function handleBacktest(args: ParsedArgs): Promise<CLIResponse<BacktestResult>> {
  const db = getDb();
  const days = args.days ?? 30;
  const minEdge = args.minEdge ?? 0.05;
  const minEdgePp = minEdge * 100;
  const now = new Date();
  const lookbackDate = new Date(now.getTime() - days * 24 * 60 * 60 * 1000);

  const signals: ScoredSignal[] = [];
  let subscriptionNotice: string | undefined;

  // ─── RESOLVED: settled markets with historical Octagon snapshots ────────
  if (!args.unresolved) {
    try {
      const settled = await discoverSettledMarkets(db, { category: args.category, days });

      if (settled.length > 0) {
        // Group by event_ticker to batch history fetches
        const byEvent = new Map<string, typeof settled>();
        for (const m of settled) {
          const arr = byEvent.get(m.event_ticker) ?? [];
          arr.push(m);
          byEvent.set(m.event_ticker, arr);
        }

        for (const [eventTicker, markets] of byEvent) {
          let snapshots;
          try {
            snapshots = await fetchAndCacheHistory(db, eventTicker);
          } catch (err) {
            if (err instanceof SubscriptionRequiredError) throw err;
            continue;
          }

          // Find the snapshot closest to N days ago
          const snap = selectSnapshotByDate(snapshots, lookbackDate);
          if (!snap) continue;

          for (const m of markets) {
            // Per-market probability from outcome_probabilities (0-100 scale)
            const perMarket = findOutcomeProb(snap.outcome_probabilities, m.ticker);
            const modelProb = perMarket?.modelProb ?? snap.model_probability;
            const marketThen = perMarket?.marketProb ?? snap.market_probability;
            const marketNow = m.result === 'yes' ? 100 : 0;
            const edgePp = Math.round((modelProb - marketThen) * 10) / 10;

            // P&L: Buy YES if edge > 0, Buy NO if edge < 0
            let pnl = 0;
            if (edgePp > 0) {
              pnl = (marketNow - marketThen) / 100;  // bought YES at marketThen, settled at marketNow
            } else if (edgePp < 0) {
              pnl = (marketThen - marketNow) / 100;  // bought NO at (100-marketThen), settled at (100-marketNow)
            }

            signals.push({
              event_ticker: m.event_ticker,
              market_ticker: m.ticker,
              series_category: m.series_category,
              model_prob: modelProb,
              market_then: marketThen,
              market_now: marketNow,
              resolved: true,
              edge_pp: edgePp,
              pnl: Math.round(pnl * 100) / 100,
              confidence_score: snap.confidence_score ?? 0,
              close_time: m.close_time,
            });
          }
        }
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
    const openMarkets = await discoverOpenMarkets(db, { category: args.category });

    for (const m of openMarkets) {
      // Get Octagon report from local cache
      const report = db.query(
        "SELECT model_prob, confidence_score, outcome_probabilities_json FROM octagon_reports WHERE event_ticker = $et AND variant_used = 'events-api' ORDER BY fetched_at DESC LIMIT 1",
      ).get({ $et: m.event_ticker }) as { model_prob: number; confidence_score: number | null; outcome_probabilities_json: string | null } | null;

      if (!report) continue;

      // Try to get historical snapshot from N days ago for market_then
      // Fall back to the prefetched events API market_probability if no history
      let modelProb: number;
      let marketThen: number;
      let confidenceScore = report.confidence_score ?? 0;

      // Check if we have cached history for this event
      const historyCount = db.query(
        'SELECT COUNT(*) as cnt FROM octagon_history WHERE event_ticker = $et',
      ).get({ $et: m.event_ticker }) as { cnt: number };

      if (historyCount.cnt > 0) {
        // Use historical snapshot from N days ago
        const rows = db.query(
          `SELECT model_probability, market_probability, confidence_score, outcome_probabilities_json
           FROM octagon_history WHERE event_ticker = $et AND captured_at <= $cutoff
           ORDER BY captured_at DESC LIMIT 1`,
        ).get({ $et: m.event_ticker, $cutoff: lookbackDate.toISOString() }) as {
          model_probability: number; market_probability: number;
          confidence_score: number | null; outcome_probabilities_json: string | null;
        } | null;

        if (rows) {
          let outcomes: OutcomeProbability[] | null = null;
          if (rows.outcome_probabilities_json) {
            try { outcomes = JSON.parse(rows.outcome_probabilities_json); } catch { /* skip */ }
          }
          const perMarket = findOutcomeProb(outcomes, m.ticker);
          modelProb = perMarket?.modelProb ?? rows.model_probability;
          marketThen = perMarket?.marketProb ?? rows.market_probability;
          confidenceScore = rows.confidence_score ?? confidenceScore;
        } else {
          // No snapshot old enough — use current prefetched data
          let outcomes: OutcomeProbability[] | null = null;
          if (report.outcome_probabilities_json) {
            try { outcomes = JSON.parse(report.outcome_probabilities_json); } catch { /* skip */ }
          }
          const perMarket = findOutcomeProb(outcomes, m.ticker);
          modelProb = perMarket ? perMarket.modelProb : report.model_prob * 100;
          marketThen = perMarket ? perMarket.marketProb : m.market_prob * 100;
        }
      } else {
        // No history — use current prefetched data as both model and market_then
        let outcomes: OutcomeProbability[] | null = null;
        if (report.outcome_probabilities_json) {
          try { outcomes = JSON.parse(report.outcome_probabilities_json); } catch { /* skip */ }
        }
        const perMarket = findOutcomeProb(outcomes, m.ticker);
        modelProb = perMarket ? perMarket.modelProb : report.model_prob * 100;
        marketThen = perMarket ? perMarket.marketProb : m.market_prob * 100;
      }

      const marketNow = m.market_prob * 100; // current Kalshi price (0-100)
      const edgePp = Math.round((modelProb - marketThen) * 10) / 10;

      // M2M P&L
      let pnl = 0;
      if (edgePp > 0) {
        pnl = (marketNow - marketThen) / 100;
      } else if (edgePp < 0) {
        pnl = (marketThen - marketNow) / 100;
      }

      signals.push({
        event_ticker: m.event_ticker,
        market_ticker: m.ticker,
        series_category: m.series_category,
        model_prob: modelProb,
        market_then: marketThen,
        market_now: marketNow,
        resolved: false,
        edge_pp: edgePp,
        pnl: Math.round(pnl * 100) / 100,
        confidence_score: confidenceScore,
        close_time: m.close_time,
      });
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

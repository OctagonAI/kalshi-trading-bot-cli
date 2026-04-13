import type { ParsedArgs } from './parse-args.js';
import type { CLIResponse } from './json.js';
import { wrapSuccess, wrapError } from './json.js';
import { getDb } from '../db/index.js';
import { discoverSettledMarkets, discoverOpenMarkets } from '../backtest/discovery.js';
import { fetchAndCacheHistory, selectSnapshot } from '../backtest/fetcher.js';
import { computeResolvedMetrics } from '../backtest/metrics.js';
import type { BacktestResult, ResolvedMarket, UnresolvedEdge } from '../backtest/types.js';
import { formatBacktestHuman, exportCSV, type FormatOpts } from '../backtest/renderer.js';

export { formatBacktestHuman };
export type { FormatOpts };

export async function handleBacktest(args: ParsedArgs): Promise<CLIResponse<BacktestResult>> {
  const db = getDb();
  const minEdge = args.minEdge ?? 0.05;
  const minHours = args.snapshotLast ? 0 : (args.minHoursBeforeClose ?? 24);
  const now = new Date();

  const dateRange = {
    from: args.from ?? '2025-01-01',
    to: args.to ?? now.toISOString().slice(0, 10),
  };

  let resolvedResult: BacktestResult['resolved'] = null;
  let unresolvedResult: BacktestResult['unresolved'] = null;

  // ─── RESOLVED ──────────────────────────────────────────────────────────
  if (!args.unresolved) {

    const settled = await discoverSettledMarkets(db, {
      category: args.category,
      from: dateRange.from,
      to: dateRange.to,
    });

    if (settled.length > 0) {

      const resolvedMarkets: ResolvedMarket[] = [];

      // Group by event_ticker to batch history fetches
      const byEvent = new Map<string, typeof settled>();
      for (const m of settled) {
        const arr = byEvent.get(m.event_ticker) ?? [];
        arr.push(m);
        byEvent.set(m.event_ticker, arr);
      }

      for (const [eventTicker, markets] of byEvent) {
        try {
          const snapshots = await fetchAndCacheHistory(db, eventTicker, {
            capturedTo: markets[0].close_time,
          });

          for (const m of markets) {
            const snap = selectSnapshot(snapshots, m.close_time, minHours);
            if (!snap) continue;

            const closeEpoch = new Date(m.close_time).getTime();
            const snapEpoch = new Date(snap.captured_at).getTime();
            const hoursBefore = (closeEpoch - snapEpoch) / (3600 * 1000);

            resolvedMarkets.push({
              ticker: m.ticker,
              event_ticker: m.event_ticker,
              model_prob: snap.model_probability / 100,
              market_prob: snap.market_probability / 100,
              edge_pp: Math.round((snap.model_probability - snap.market_probability) * 10) / 10,
              hours_before_close: hoursBefore,
              confidence_score: snap.confidence_score ?? 0,
              series_category: m.series_category,
              outcome: m.result === 'yes' ? 1 : 0,
              close_time: m.close_time,
            });
          }
        } catch (err) {
          // History fetch failed for this event — skip
        }
      }

      if (resolvedMarkets.length > 0) {
        const minEdgePp = minEdge * 100;
        resolvedResult = computeResolvedMetrics(resolvedMarkets, minEdgePp);
        resolvedResult.coverage = settled.length > 0
          ? resolvedMarkets.length / settled.length
          : 0;
      }
    }
  }

  // ─── UNRESOLVED ────────────────────────────────────────────────────────
  if (!args.resolved) {

    const openMarkets = await discoverOpenMarkets(db, { category: args.category });

    const edges: UnresolvedEdge[] = [];
    for (const m of openMarkets) {
      // Get latest Octagon model_prob from local cache
      const report = db.query(
        "SELECT model_prob, market_prob, confidence_score FROM octagon_reports WHERE event_ticker = $et AND variant_used = 'events-api' ORDER BY fetched_at DESC LIMIT 1",
      ).get({ $et: m.event_ticker }) as { model_prob: number; market_prob: number | null; confidence_score: number | null } | null;

      if (!report) continue;

      const modelProb = report.model_prob;
      const edgePp = Math.round((modelProb - m.market_prob) * 1000) / 10;

      if (Math.abs(edgePp) < minEdge * 100) continue;

      edges.push({
        ticker: m.ticker,
        event_ticker: m.event_ticker,
        model_prob: modelProb,
        market_prob: m.market_prob,
        edge_pp: edgePp,
        direction: edgePp > 0 ? 'YES' : 'NO',
        confidence_score: report.confidence_score ?? 0,
        closes_at: m.close_time,
        series_category: m.series_category,
      });
    }

    // Sort by |edge| descending
    edges.sort((a, b) => Math.abs(b.edge_pp) - Math.abs(a.edge_pp));

    unresolvedResult = {
      edges,
      total_open_with_coverage: openMarkets.length,
      total_open: openMarkets.length,
    };
  }

  const result: BacktestResult = {
    resolved: resolvedResult,
    unresolved: unresolvedResult,
    date_range: dateRange,
  };

  // Export CSV if requested
  if (args.exportPath) {
    exportCSV(result, args.exportPath);

  }

  return wrapSuccess('backtest', result);
}

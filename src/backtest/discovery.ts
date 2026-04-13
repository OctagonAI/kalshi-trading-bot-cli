import type { Database } from 'bun:sqlite';
import { callKalshiApi } from '../tools/kalshi/api.js';
import type { KalshiMarket } from '../tools/kalshi/types.js';

const CONCURRENCY = 10;

export interface SettledMarket {
  ticker: string;
  event_ticker: string;
  result: 'yes' | 'no';
  close_time: string;
  series_category: string;
  last_price: number;         // last traded price (0-1)
}

export interface OpenMarket {
  ticker: string;
  event_ticker: string;
  market_prob: number;        // current trading price (0-1)
  close_time: string;
  series_category: string;
}

/** Parse market price from Kalshi response (handles both cents and dollars formats). */
function parsePrice(m: KalshiMarket): number {
  return parseFloat(m.last_price_dollars ?? '') || (typeof m.last_price === 'number' ? m.last_price / 100 : 0);
}

/** Fetch event markets from Kalshi, returning empty array on error. */
async function fetchEventMarkets(eventTicker: string): Promise<KalshiMarket[]> {
  try {
    const response = await callKalshiApi('GET', `/events/${eventTicker}`, {
      params: { with_nested_markets: true },
    });
    if (!response || typeof response !== 'object') return [];
    const obj = response as Record<string, unknown>;
    const event = (obj.event ?? obj) as Record<string, unknown>;
    const markets = event.markets;
    return Array.isArray(markets) ? markets as KalshiMarket[] : [];
  } catch {
    return [];
  }
}

/** Process items in parallel batches of `concurrency`. */
async function parallelMap<T, R>(
  items: T[],
  fn: (item: T) => Promise<R>,
  concurrency: number,
): Promise<R[]> {
  const results: R[] = [];
  for (let i = 0; i < items.length; i += concurrency) {
    const batch = items.slice(i, i + concurrency);
    const batchResults = await Promise.all(batch.map(fn));
    results.push(...batchResults);
  }
  return results;
}

/**
 * Discover settled Kalshi markets that have Octagon coverage with history.
 */
export async function discoverSettledMarkets(
  db: Database,
  opts?: { category?: string; from?: string; to?: string },
): Promise<SettledMarket[]> {
  let query = `SELECT DISTINCT event_ticker, series_category as category
    FROM octagon_reports r WHERE variant_used = 'events-api' AND has_history = 1`;
  const params: Record<string, string> = {};
  if (opts?.category) {
    query += ' AND LOWER(series_category) LIKE $cat';
    params.$cat = `%${opts.category.toLowerCase()}%`;
  }

  const events = db.query(query).all(params) as Array<{ event_ticker: string; category: string | null }>;
  // Normalize date-only strings: fromDate → start of day, toDate → end of day
  const isDateOnly = /^\d{4}-\d{2}-\d{2}$/;
  const fromDate = opts?.from ? new Date(opts.from) : null;
  const toDate = opts?.to
    ? (isDateOnly.test(opts.to)
      ? new Date(opts.to + 'T23:59:59.999Z')
      : new Date(opts.to))
    : null;

  const batchResults = await parallelMap(events, async ({ event_ticker, category: cat }) => {
    const markets = await fetchEventMarkets(event_ticker);
    // Skip multi-market events: event-level model_prob doesn't apply to individual brackets.
    // This catches both mutually_exclusive (elections, sports) and bracket events (BTC daily).
    if (markets.length > 1) return [];

    const settled: SettledMarket[] = [];

    for (const m of markets) {
      const result = (m.result ?? '').toLowerCase();
      if (result !== 'yes' && result !== 'no') continue;

      if (m.close_time) {
        const closeDate = new Date(m.close_time);
        if (fromDate && closeDate < fromDate) continue;
        if (toDate && closeDate > toDate) continue;
      }

      settled.push({
        ticker: m.ticker,
        event_ticker,
        result: result as 'yes' | 'no',
        close_time: m.close_time ?? '',
        series_category: cat ?? '',
        last_price: parsePrice(m),
      });
    }
    return settled;
  }, CONCURRENCY);

  return batchResults.flat();
}

/**
 * Discover open Kalshi markets that have Octagon coverage.
 */
export async function discoverOpenMarkets(
  db: Database,
  opts?: { category?: string },
): Promise<OpenMarket[]> {
  let query2 = `SELECT DISTINCT event_ticker, series_category as category
    FROM octagon_reports r WHERE variant_used = 'events-api'`;
  const params2: Record<string, string> = {};
  if (opts?.category) {
    query2 += ' AND LOWER(series_category) LIKE $cat';
    params2.$cat = `%${opts.category.toLowerCase()}%`;
  }

  const events2 = db.query(query2).all(params2) as Array<{ event_ticker: string; category: string | null }>;

  const batchResults = await parallelMap(events2, async ({ event_ticker, category: cat }) => {
    const markets = await fetchEventMarkets(event_ticker);

    // Skip multi-market events: event-level model_prob doesn't apply to individual brackets.
    // This catches both mutually_exclusive (elections, sports) and bracket events (BTC daily).
    if (markets.length > 1) return [];

    const open: OpenMarket[] = [];

    for (const m of markets) {
      const status = (m.status ?? '').toLowerCase();
      if (status !== 'open' && status !== 'active') continue;

      const marketProb = parsePrice(m);
      if (marketProb <= 0) continue;

      open.push({
        ticker: m.ticker,
        event_ticker,
        market_prob: marketProb,
        close_time: m.close_time ?? '',
        series_category: cat ?? '',
      });
    }
    return open;
  }, CONCURRENCY);

  return batchResults.flat();
}

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
  volume: number;             // lifetime trading volume (used for tradeability gate)
}

export interface OpenMarket {
  ticker: string;
  event_ticker: string;
  market_prob: number;        // current trading price (0-1)
  close_time: string;
  series_category: string;
  volume: number;             // lifetime trading volume (tradeability gate)
  volume_24h: number;         // 24-hour volume (liquidity-now gate)
}

/** Parse market price from Kalshi response (handles both cents and dollars formats). */
function parsePrice(m: KalshiMarket): number {
  const dollars = parseFloat(m.last_price_dollars ?? '');
  if (Number.isFinite(dollars)) return dollars;
  return typeof m.last_price === 'number' ? m.last_price / 100 : 0;
}

/** Parse lifetime volume (prefers volume_fp string from new API). */
function parseVolume(m: KalshiMarket): number {
  const fp = parseFloat(m.volume_fp ?? '');
  if (Number.isFinite(fp)) return fp;
  return typeof m.volume === 'number' ? m.volume : 0;
}

/** Parse 24h volume (prefers volume_24h_fp string from new API). */
function parseVolume24h(m: KalshiMarket): number {
  const fp = parseFloat(m.volume_24h_fp ?? '');
  if (Number.isFinite(fp)) return fp;
  return typeof m.volume_24h === 'number' ? m.volume_24h : 0;
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

/** Build the event discovery query with optional category filter and extra WHERE clauses. */
function buildEventQuery(
  extraWhere: string,
  category?: string,
): { query: string; params: Record<string, string> } {
  let query = `SELECT event_ticker, MAX(series_category) as category
    FROM octagon_reports r WHERE variant_used = 'events-api'${extraWhere}`;
  const params: Record<string, string> = {};
  if (category) {
    query += ' AND LOWER(series_category) LIKE $cat';
    params.$cat = `%${category.toLowerCase()}%`;
  }
  query += ' GROUP BY event_ticker';
  return { query, params };
}

/** Process items in parallel batches of `concurrency`. */
export async function parallelMap<T, R>(
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
 *
 * No close_time filter: the backtest's prediction-age window is enforced
 * downstream via `selectSnapshotByDate`. Filtering on close_time here would
 * wrongly exclude events that closed before the lookback window but whose
 * predictions were still made within the prediction-age window (they'd still
 * have outcomes to score against).
 */
export async function discoverSettledMarkets(
  db: Database,
  opts?: { category?: string },
): Promise<SettledMarket[]> {
  const { query, params } = buildEventQuery('', opts?.category);
  const events = db.query(query).all(params) as Array<{ event_ticker: string; category: string | null }>;

  const batchResults = await parallelMap(events, async ({ event_ticker, category: cat }) => {
    const markets = await fetchEventMarkets(event_ticker);
    const settled: SettledMarket[] = [];

    for (const m of markets) {
      const result = (m.result ?? '').toLowerCase();
      if (result !== 'yes' && result !== 'no') continue;

      settled.push({
        ticker: m.ticker,
        event_ticker,
        result: result as 'yes' | 'no',
        close_time: m.close_time ?? '',
        series_category: cat ?? '',
        last_price: parsePrice(m),
        volume: parseVolume(m),
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
  const { query: q2, params: p2 } = buildEventQuery('', opts?.category);
  const events2 = db.query(q2).all(p2) as Array<{ event_ticker: string; category: string | null }>;

  const batchResults = await parallelMap(events2, async ({ event_ticker, category: cat }) => {
    const markets = await fetchEventMarkets(event_ticker);
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
        volume: parseVolume(m),
        volume_24h: parseVolume24h(m),
      });
    }
    return open;
  }, CONCURRENCY);

  return batchResults.flat();
}
